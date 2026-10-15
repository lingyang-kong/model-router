mod codex;

use axum::http::HeaderMap;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::capabilities::Capabilities;
use crate::error::{Error, Result};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CatalogFormat {
    Codex,
    Openai,
    Anthropic,
}

struct CatalogLayout {
    pub models: &'static str,
    pub id: &'static str,
}

impl CatalogFormat {
    /// Wire names for shared settings; values are owned by the provider or user.
    fn metadata_fields(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::Codex => &[
                ("name", "display_name"),
                ("description", "description"),
                ("reasoning_level", "default_reasoning_level"),
                ("reasoning_levels", "supported_reasoning_levels"),
            ],
            Self::Openai | Self::Anthropic => &[
                ("name", "display_name"),
                ("description", "description"),
                ("reasoning_level", "reasoning_level"),
                ("reasoning_levels", "reasoning_levels"),
            ],
        }
    }

    pub fn settings(self, fields: &Map<String, Value>) -> Map<String, Value> {
        let mut fields = fields.clone();
        for &(shared, wire) in self.metadata_fields() {
            rename_field(&mut fields, shared, wire);
        }
        if self == Self::Codex {
            rename_reasoning_levels(
                fields.get_mut("supported_reasoning_levels"),
                "level",
                "effort",
            );
        }
        fields
    }

    pub fn shared_metadata(self, model: &Value) -> Map<String, Value> {
        let mut fields: Map<String, Value> = self
            .metadata_fields()
            .iter()
            .filter_map(|&(shared, wire)| {
                model
                    .get(wire)
                    .or_else(|| model.get(shared))
                    .map(|value| (shared.to_owned(), value.clone()))
            })
            .collect();
        if self == Self::Codex {
            rename_reasoning_levels(fields.get_mut("reasoning_levels"), "effort", "level");
        }
        fields
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
        source: Self,
        base: Option<(&Value, Self)>,
        capabilities: &Capabilities,
    ) -> Result<Option<Value>> {
        let base_format = base.map(|(_, format)| format);
        if self != source && base_format != Some(self) {
            return Ok(None);
        }

        if !model.is_object() || base.is_some_and(|(base, _)| !base.is_object()) {
            return Err(Error::bad_gateway("Catalog models must be objects"));
        }
        let local_shared = source.shared_metadata(model);

        let mut result = match base {
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
                    for (key, value) in self.settings(&base_format.shared_metadata(base_model)) {
                        result.as_object_mut().unwrap().entry(key).or_insert(value);
                    }
                }
                result
            }
        };

        if self == Self::Codex
            && let Some(context) = source.context_window(model)
        {
            result.as_object_mut().unwrap().remove("context_window");
            result["max_context_window"] = context.clone();
        }

        let target_id = source.model_id(model)?.to_owned();
        if self == Self::Codex && base.is_some() && !local_shared.contains_key("name") {
            result["display_name"] = Value::String(target_id.clone());
        }
        if self == Self::Codex {
            codex::apply_capabilities(&mut result, capabilities);
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

fn rename_reasoning_levels(value: Option<&mut Value>, from: &str, to: &str) {
    if let Some(Value::Array(levels)) = value {
        for level in levels {
            if let Value::Object(fields) = level {
                rename_field(fields, from, to);
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Client {
    Codex,
}

impl Client {
    pub fn from_headers(headers: &HeaderMap) -> Result<Option<Self>> {
        let mut values = headers.get_all("x-router-client").iter();
        let Some(value) = values.next() else {
            return Ok(None);
        };
        if values.any(|other| other != value) {
            return Err(Error::bad_request("Conflicting x-router-client headers"));
        }
        match value.as_bytes() {
            b"codex" => Ok(Some(Self::Codex)),
            _ => Err(Error::bad_request("Unknown x-router-client")),
        }
    }

    pub fn catalog_format(self) -> CatalogFormat {
        match self {
            Self::Codex => CatalogFormat::Codex,
        }
    }

    pub fn prepare_catalog(self, models: &mut [Value]) {
        match self {
            Self::Codex => codex::prepare_catalog(models),
        }
    }

    pub fn model_hint<'a>(self, headers: &'a HeaderMap, hint: &mut ModelHint<'a>) -> Result<()> {
        match self {
            Self::Codex => codex::model_hint(headers, hint),
        }
    }
}

#[derive(Default)]
pub(crate) struct ModelHint<'a>(Option<&'a str>);

impl<'a> ModelHint<'a> {
    pub fn set(&mut self, value: &'a str) -> Result<()> {
        if value.is_empty() || self.0.is_some_and(|previous| previous != value) {
            return Err(Error::bad_request(
                "Empty or conflicting WebSocket model hints",
            ));
        }
        self.0 = Some(value);
        Ok(())
    }

    pub fn require(self) -> Result<&'a str> {
        self.0
            .ok_or_else(|| Error::bad_request("WebSocket model routes require a model hint"))
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
    fn same_format_is_passthrough_and_other_unbased_outputs_are_omitted() {
        let native = json!({"id":"local","extension":{"x":1}});
        assert_eq!(
            CatalogFormat::Openai
                .project(
                    &native,
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
                .project(&codex, CatalogFormat::Codex, None, &Capabilities::default())
                .unwrap(),
            Some(codex)
        );
        assert_eq!(
            CatalogFormat::Anthropic
                .project(
                    &native,
                    CatalogFormat::Openai,
                    None,
                    &Capabilities::default()
                )
                .unwrap(),
            None
        );
    }

    #[test]
    fn same_format_link_retains_donor_fields_and_overlays_local_native_row() {
        let donor =
            json!({"slug":"donor","display_name":"Donor","opaque":{"donor":true},"priority":1});
        let local = json!({"slug":"local","opaque":{"local":true}});
        let got = CatalogFormat::Codex
            .project(
                &local,
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
    }

    #[test]
    fn same_format_codex_link_uses_local_name_and_rewrites_inherited_gpt_opening() {
        let donor = json!({
            "slug":"donor", "display_name":"donor name",
            "model_messages":{"instructions_template":"You are Codex, an agent based on GPT-6. Continue."}
        });
        let local = json!({"slug":"local"});
        let got = CatalogFormat::Codex
            .project(
                &local,
                CatalogFormat::Codex,
                Some((&donor, CatalogFormat::Codex)),
                &Capabilities::default(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(got["display_name"], "local");
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
        };
        let got = CatalogFormat::Codex
            .project(&local, CatalogFormat::Codex, None, &caps)
            .unwrap()
            .unwrap();
        assert_eq!(got["use_responses_lite"], false);
        assert_eq!(got["supports_search_tool"], false);
        assert_eq!(got["supports_image_detail_original"], false);
        assert_eq!(got["input_modalities"], json!(["text"]));
        assert!(got.get("tool_inventory").is_none());
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
}
