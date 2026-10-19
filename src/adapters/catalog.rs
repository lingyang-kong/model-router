use serde::Deserialize;
use serde_json::{Map, Value};

use super::codex;
use crate::capabilities::Capabilities;
use crate::error::{Error, Result};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CatalogFormat {
    Codex,
    Openai,
    Anthropic,
}

pub(super) struct CatalogLayout {
    pub models: &'static str,
    pub id: &'static str,
}

impl CatalogFormat {
    /// Wire names for shared settings; values are owned by the provider or user.
    fn metadata_fields(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::Codex => &[],
            Self::Openai | Self::Anthropic => &[
                ("name", "display_name"),
                ("description", "description"),
                ("reasoning_level", "reasoning_level"),
                ("reasoning_levels", "reasoning_levels"),
            ],
        }
    }

    pub fn settings(self, fields: &Map<String, Value>) -> Map<String, Value> {
        if self == Self::Codex {
            return codex::settings(fields);
        }
        let mut fields = fields.clone();
        for &(shared, wire) in self.metadata_fields() {
            rename_field(&mut fields, shared, wire);
        }
        fields
    }

    pub fn shared_metadata(self, model: &Value) -> Map<String, Value> {
        if self == Self::Codex {
            return codex::shared_metadata(model);
        }
        self.metadata_fields()
            .iter()
            .filter_map(|&(shared, wire)| {
                model
                    .get(wire)
                    .or_else(|| model.get(shared))
                    .map(|value| (shared.to_owned(), value.clone()))
            })
            .collect()
    }

    pub fn context_window(self, model: &Value) -> Option<&Value> {
        match self {
            Self::Openai => model.get("max_model_len"),
            Self::Anthropic | Self::Codex => None,
        }
    }

    pub fn empty_catalog(self) -> Value {
        match self {
            Self::Codex => serde_json::json!({"models": []}),
            Self::Openai => serde_json::json!({"object": "list", "data": []}),
            Self::Anthropic => serde_json::json!({"data": [], "has_more": false}),
        }
    }

    pub fn project(
        self,
        model: &Value,
        provider: &str,
        source: Self,
        base: Option<(&Value, Self)>,
        capabilities: &Capabilities,
    ) -> Result<Option<Value>> {
        let base_format = base.map(|(_, format)| format);
        if self == Self::Codex && self != source && base_format != Some(self) {
            return Ok(None);
        }

        if !model.is_object() || base.is_some_and(|(base, _)| !base.is_object()) {
            return Err(Error::bad_gateway("Catalog models must be objects"));
        }
        let local_shared = source.shared_metadata(model);

        let mut result = if source != self && base_format != Some(self) {
            let mut shared = base
                .map(|(model, format)| format.shared_metadata(model))
                .unwrap_or_default();
            shared.extend(local_shared.clone());
            let mut result = Value::Object(self.settings(&shared));
            match self {
                Self::Openai => {
                    result["object"] = Value::String("model".into());
                    result["created"] = Value::from(0);
                    result["owned_by"] = Value::String(provider.into());
                }
                Self::Anthropic => {
                    result["type"] = Value::String("model".into());
                    result["created_at"] = Value::String("1970-01-01T00:00:00Z".into());
                    if !result.get("display_name").is_some_and(Value::is_string) {
                        result["display_name"] = Value::String(source.model_id(model)?.into());
                    }
                }
                Self::Codex => unreachable!(),
            }
            result
        } else {
            match base {
                Some((base_model, base_format)) if base_format == self => {
                    let mut result = base_model.clone();
                    if source == self {
                        crate::catalog::overlay(
                            &mut result,
                            model.as_object().expect("validated model object"),
                        );
                    } else {
                        for (key, value) in self.settings(&local_shared) {
                            result[key] = value;
                        }
                    }
                    result
                }
                base => {
                    let mut result = if source == self {
                        model.clone()
                    } else {
                        Value::Object(Map::new())
                    };
                    if let Some((base_model, base_format)) = base {
                        for (key, value) in self.settings(&base_format.shared_metadata(base_model))
                        {
                            result.as_object_mut().unwrap().entry(key).or_insert(value);
                        }
                    }
                    result
                }
            }
        };

        if self == Self::Codex {
            codex::apply_context_window(&mut result, source, model);
        }

        let target_id = source.model_id(model)?.to_owned();
        if self == Self::Codex {
            codex::apply_capabilities(&mut result, capabilities);
            codex::apply_projection_metadata(
                &mut result,
                base.is_some(),
                &local_shared,
                &target_id,
            );
        }
        let layout_id = self.layout().id;
        result[layout_id] = Value::String(target_id.clone());
        if self == Self::Codex && base_format == Some(Self::Codex) {
            codex::rewrite_inherited_instructions(&mut result, (source == self).then_some(model));
        }
        Ok(Some(result))
    }

    fn layout(self) -> &'static CatalogLayout {
        match self {
            Self::Codex => &codex::CATALOG,
            Self::Openai | Self::Anthropic => &CatalogLayout {
                models: "data",
                id: "id",
            },
        }
    }

    pub fn models(self, catalog: &mut Value) -> Result<&mut Vec<Value>> {
        let key = self.layout().models;
        catalog
            .get_mut(key)
            .and_then(Value::as_array_mut)
            .ok_or_else(|| Error::bad_gateway(format!("{self:?} catalog needs a {key} array")))
    }

    pub fn model_id(self, model: &Value) -> Result<&str> {
        let key = self.layout().id;
        model
            .get(key)
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                Error::bad_gateway(format!(
                    "{self:?} catalog models need a nonempty {key} string"
                ))
            })
    }
}

fn rename_field(fields: &mut Map<String, Value>, from: &str, to: &str) {
    if from != to
        && let Some(value) = fields.remove(from)
    {
        fields.insert(to.to_owned(), value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn inherited_codex_row_keeps_opaque_fields_and_local_identity() {
        let donor = json!({
            "slug":"donor", "display_name":"Donor", "description":"inherited description",
            "default_reasoning_level":"deep", "supported_reasoning_levels":[{"effort":"deep"}],
            "opaque":{"provider_extension":[1,2]}, "context_window":999
        });
        let local = json!({"id":"local", "max_model_len":123});
        let got = CatalogFormat::Codex
            .project(
                &local,
                "provider",
                CatalogFormat::Openai,
                Some((&donor, CatalogFormat::Codex)),
                &Capabilities::default(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(got["slug"], "local");
        assert_eq!(got["display_name"], "local");
        assert_eq!(got["description"], "inherited description");
        assert_eq!(got["default_reasoning_level"], "deep");
        assert_eq!(got["opaque"]["provider_extension"], json!([1, 2]));
        assert_eq!(got["max_context_window"], 123);
        assert!(got.get("context_window").is_none());
    }

    #[test]
    fn local_shared_metadata_wins_on_inherited_row() {
        let donor =
            json!({"slug":"donor","display_name":"donor","description":"old","opaque":true});
        let local = json!({"id":"local","display_name":"Local display","description":"local desc"});
        let got = CatalogFormat::Codex
            .project(
                &local,
                "provider",
                CatalogFormat::Openai,
                Some((&donor, CatalogFormat::Codex)),
                &Capabilities::default(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(got["display_name"], "Local display");
        assert_eq!(got["description"], "local desc");
        assert_eq!(got["opaque"], true);
    }

    #[test]
    fn local_format_keeps_shared_settings_over_cross_format_donor() {
        let donor = json!({
            "slug":"donor", "display_name":"Donor", "description":"Donor description",
            "default_reasoning_level":"high",
            "supported_reasoning_levels":[{"effort":"high","description":"Deep"}],
            "donor_only":true
        });
        for format in [CatalogFormat::Openai, CatalogFormat::Anthropic] {
            let local = json!({
                "id":"local", "display_name":"Local", "description":null,
                "reasoning_level":"low", "local_only":true
            });
            let got = format
                .project(
                    &local,
                    "provider",
                    format,
                    Some((&donor, CatalogFormat::Codex)),
                    &Capabilities::default(),
                )
                .unwrap()
                .unwrap();
            assert_eq!(
                got,
                json!({
                    "id":"local", "display_name":"Local", "description":null,
                    "reasoning_level":"low", "local_only":true,
                    "reasoning_levels":[{"level":"high","description":"Deep"}]
                })
            );
        }
    }

    #[test]
    fn same_format_is_passthrough_and_codex_requires_a_compatible_row() {
        let native = json!({"id":"local","extension":{"x":1}});
        assert_eq!(
            CatalogFormat::Openai
                .project(
                    &native,
                    "provider",
                    CatalogFormat::Openai,
                    None,
                    &Capabilities::default()
                )
                .unwrap(),
            Some(native.clone())
        );
        let codex = json!({"slug":"local","opaque":{"x":1},"base_instructions":"literal"});
        assert_eq!(
            CatalogFormat::Codex
                .project(
                    &codex,
                    "provider",
                    CatalogFormat::Codex,
                    None,
                    &Capabilities::default()
                )
                .unwrap(),
            Some(codex)
        );
        assert_eq!(
            CatalogFormat::Codex
                .project(
                    &native,
                    "provider",
                    CatalogFormat::Openai,
                    None,
                    &Capabilities::default()
                )
                .unwrap(),
            None
        );
    }

    #[test]
    fn generic_rows_use_local_shared_metadata_and_provider_identity() {
        let local = json!({
            "slug":"local", "display_name":"Local", "description":null,
            "opaque":"source secret", "created":123, "owned_by":"wrong"
        });
        for format in [CatalogFormat::Openai, CatalogFormat::Anthropic] {
            let donor = json!({
                "slug":"donor", "display_name":"Donor", "description":"Donor description",
                "default_reasoning_level":"high", "opaque":"donor secret",
                "created":456, "owned_by":"donor", "created_at":"2026-01-01T00:00:00Z"
            });
            let row = format
                .project(
                    &local,
                    "local-provider",
                    CatalogFormat::Codex,
                    Some((&donor, CatalogFormat::Codex)),
                    &Capabilities::default(),
                )
                .unwrap()
                .unwrap();
            assert_eq!(row["id"], "local");
            assert_eq!(row["display_name"], "Local");
            assert_eq!(row["description"], Value::Null);
            assert_eq!(row["reasoning_level"], "high");
            assert!(row.get("opaque").is_none());
            match format {
                CatalogFormat::Openai => {
                    assert_eq!(row["object"], "model");
                    assert_eq!(row["created"], 0);
                    assert_eq!(row["owned_by"], "local-provider");
                    assert!(row.get("created_at").is_none());
                }
                CatalogFormat::Anthropic => {
                    assert_eq!(row["type"], "model");
                    assert_eq!(row["created_at"], "1970-01-01T00:00:00Z");
                    assert!(row.get("owned_by").is_none());
                    assert!(row.get("created").is_none());
                }
                CatalogFormat::Codex => unreachable!(),
            }
        }
    }

    #[test]
    fn generic_rows_without_donors_preserve_shared_settings() {
        let local = json!({"slug":"local","description":"Source description","opaque":true});
        for format in [CatalogFormat::Openai, CatalogFormat::Anthropic] {
            let row = format
                .project(
                    &local,
                    "provider",
                    CatalogFormat::Codex,
                    None,
                    &Capabilities::default(),
                )
                .unwrap()
                .unwrap();
            assert_eq!(row["id"], "local");
            assert_eq!(row["description"], "Source description");
            assert!(row.get("opaque").is_none());
            if format == CatalogFormat::Anthropic {
                assert_eq!(row["display_name"], "local");
            }
        }
    }

    #[test]
    fn explicit_matching_format_donors_keep_native_metadata() {
        let local = json!({"slug":"local","description":"Local description"});
        for format in [CatalogFormat::Openai, CatalogFormat::Anthropic] {
            let donor = json!({"id":"donor","display_name":"Donor","opaque":true});
            let row = format
                .project(
                    &local,
                    "provider",
                    CatalogFormat::Codex,
                    Some((&donor, format)),
                    &Capabilities::default(),
                )
                .unwrap()
                .unwrap();
            assert_eq!(row["id"], "local");
            assert_eq!(row["description"], "Local description");
            assert_eq!(row["opaque"], true);
            assert_eq!(row["display_name"], "Donor");
        }
    }

    #[test]
    fn same_format_link_overlays_local_metadata_and_rewrites_inherited_identity() {
        let donor = json!({
            "slug":"donor", "display_name":"Donor", "opaque":{"donor":true}, "priority":1,
            "model_messages":{"instructions_template":"You are Codex, an agent based on GPT-6. Continue."}
        });
        let local = json!({"slug":"local","opaque":{"local":true}});
        let got = CatalogFormat::Codex
            .project(
                &local,
                "provider",
                CatalogFormat::Codex,
                Some((&donor, CatalogFormat::Codex)),
                &Capabilities::default(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(got["slug"], "local");
        assert_eq!(got["display_name"], "local");
        assert_eq!(got["opaque"]["local"], true);
        assert_eq!(got["priority"], 1);
        assert_eq!(
            got["model_messages"]["instructions_template"],
            "You are Codex, an agent based on local, not GPT. Continue."
        );
    }

    #[test]
    fn explicit_capabilities_update_codex_fields() {
        let local = json!({"slug":"local"});
        let caps = Capabilities {
            responses_lite: Some(false),
            tool_search: Some(false),
            image_detail_original: Some(false),
            images: Some(false),
            tool_inventory: Some(false),
            ..Default::default()
        };
        let got = CatalogFormat::Codex
            .project(&local, "provider", CatalogFormat::Codex, None, &caps)
            .unwrap()
            .unwrap();
        assert!(got.get("use_responses_lite").is_none());
        assert_eq!(got["input_modalities"], json!(["text"]));
        assert!(got.get("tool_inventory").is_none());
        assert!(got.get("supports_search_tool").is_none());
        assert!(got.get("supports_image_detail_original").is_none());
    }

    #[test]
    fn unknown_capabilities_preserve_inherited_values_and_image_subset() {
        let donor = json!({
            "slug":"donor", "supports_search_tool":true,
            "use_responses_lite":true, "input_modalities":["text","image","audio"]
        });
        let local = json!({"id":"local"});
        let got = CatalogFormat::Codex
            .project(
                &local,
                "provider",
                CatalogFormat::Openai,
                Some((&donor, CatalogFormat::Codex)),
                &Capabilities::default(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(got["supports_search_tool"], true);
        assert_eq!(got["use_responses_lite"], true);
        assert_eq!(got["input_modalities"], json!(["text", "image", "audio"]));

        let got = CatalogFormat::Codex
            .project(
                &local,
                "provider",
                CatalogFormat::Openai,
                Some((&donor, CatalogFormat::Codex)),
                &Capabilities {
                    images: Some(false),
                    ..Default::default()
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(got["input_modalities"], json!(["text", "audio"]));
    }

    #[test]
    fn image_true_preserves_known_modalities_and_inventory_does_not_set_search() {
        let local = json!({"slug":"local","input_modalities":["text","audio"]});
        let got = CatalogFormat::Codex
            .project(
                &local,
                "provider",
                CatalogFormat::Codex,
                None,
                &Capabilities {
                    images: Some(true),
                    tool_inventory: Some(false),
                    ..Default::default()
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(got["input_modalities"], json!(["text", "audio", "image"]));
        assert!(got.get("supports_search_tool").is_none());
    }

    #[test]
    fn access_choices_follow_provider_capabilities_not_catalog_format() {
        let donor = json!({
            "slug":"donor", "available_access_programs":{"cyber":["standard"]}
        });
        let local = json!({"id":"local"});
        let project = |capabilities: &Capabilities| {
            CatalogFormat::Codex
                .project(
                    &local,
                    "provider",
                    CatalogFormat::Openai,
                    Some((&donor, CatalogFormat::Codex)),
                    capabilities,
                )
                .unwrap()
                .unwrap()
        };
        assert_eq!(
            project(&Capabilities::default())["available_access_programs"],
            json!({"cyber":["standard"]})
        );
        assert_eq!(
            project(&Capabilities {
                access_programs: Some(false),
                ..Default::default()
            })["available_access_programs"],
            json!({"cyber":[]})
        );
        assert_eq!(
            project(&Capabilities {
                access_programs: Some(true),
                ..Default::default()
            })["available_access_programs"],
            json!({"cyber":["standard"]})
        );

        let native = json!({"slug":"native","available_access_programs":{"cyber":["standard"]}});
        let native_got = CatalogFormat::Codex
            .project(
                &native,
                "provider",
                CatalogFormat::Codex,
                None,
                &Capabilities::default(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            native_got["available_access_programs"],
            json!({"cyber":["standard"]})
        );
    }

    #[test]
    fn explicit_reasoning_settings_override_template() {
        let donor = json!({"slug":"donor", "default_reasoning_level":"old"});
        let local = json!({"id":"local", "reasoning_level":"custom", "reasoning_levels":[{"level":"custom","description":"User choice"}]});
        let row = CatalogFormat::Codex
            .project(
                &local,
                "provider",
                CatalogFormat::Openai,
                Some((&donor, CatalogFormat::Codex)),
                &Capabilities::default(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(row["default_reasoning_level"], "custom");
        assert_eq!(
            row["supported_reasoning_levels"],
            json!([{"effort":"custom","description":"User choice"}])
        );
    }
}
