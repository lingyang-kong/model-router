use axum::http::HeaderMap;
use serde_json::Value;

use super::{catalog::CatalogLayout, client::ModelHint};
use crate::capabilities::Capabilities;
use crate::error::{Error, Result};
use serde_json::Map;

mod compaction;
mod compaction_transport;
mod multiagent;
mod response;
mod transport;
mod websocket;

pub(super) use transport::{adapt_websocket_response, forward};
pub(super) use websocket::route as websocket_request;

pub(super) fn matches_originator(originator: &str) -> bool {
    originator.to_ascii_lowercase().contains("codex")
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

pub(super) fn settings(fields: &Map<String, Value>) -> Map<String, Value> {
    let mut fields = fields.clone();
    for (shared, wire) in [
        ("name", "display_name"),
        ("description", "description"),
        ("reasoning_level", "default_reasoning_level"),
        ("reasoning_levels", "supported_reasoning_levels"),
    ] {
        rename_field(&mut fields, shared, wire);
    }
    rename_reasoning_levels(
        fields.get_mut("supported_reasoning_levels"),
        "level",
        "effort",
    );
    fields
}

pub(super) fn shared_metadata(model: &Value) -> Map<String, Value> {
    let fields = [
        ("name", "display_name"),
        ("description", "description"),
        ("reasoning_level", "default_reasoning_level"),
        ("reasoning_levels", "supported_reasoning_levels"),
    ];
    let mut result: Map<String, Value> = fields
        .iter()
        .filter_map(|&(shared, wire)| {
            model
                .get(wire)
                .or_else(|| model.get(shared))
                .map(|value| (shared.to_owned(), value.clone()))
        })
        .collect();
    rename_reasoning_levels(result.get_mut("reasoning_levels"), "effort", "level");
    result
}

pub(super) fn apply_context_window(
    result: &mut Value,
    source: super::catalog::CatalogFormat,
    model: &Value,
) {
    if let Some(context) = source.context_window(model) {
        result.as_object_mut().unwrap().remove("context_window");
        result["max_context_window"] = context.clone();
    }
}

pub(super) fn apply_projection_metadata(
    result: &mut Value,
    has_base: bool,
    local_shared: &Map<String, Value>,
    target_id: &str,
) {
    if has_base && !local_shared.contains_key("name") {
        result["display_name"] = Value::String(target_id.to_owned());
    }
}

pub(super) const CATALOG: CatalogLayout = CatalogLayout {
    models: "models",
    id: "slug",
};

/// Apply resolved capability facts to a projected Codex row.
pub(super) fn apply_capabilities(model: &mut Value, capabilities: &Capabilities) {
    for (fact, field) in [
        (capabilities.responses_lite, "use_responses_lite"),
        (capabilities.tool_search, "supports_search_tool"),
        (
            capabilities.image_detail_original,
            "supports_image_detail_original",
        ),
        (capabilities.history_notes, "supports_experimental_context"),
        (
            capabilities.reasoning_summary,
            "supports_reasoning_summary_parameter",
        ),
    ] {
        if let Some(value) = fact {
            model[field] = Value::Bool(value);
        }
    }
    apply_response_metadata(model, capabilities);

    if capabilities.service_tiers == Some(false) || capabilities.service_tier_ids.is_some() {
        for field in [
            "additional_speed_tiers",
            "service_tiers",
            "default_service_tier",
        ] {
            model.as_object_mut().unwrap().remove(field);
        }
    }
    if capabilities.service_tiers != Some(false)
        && let Some(ids) = &capabilities.service_tier_ids
    {
        model["service_tiers"] = Value::Array(
            ids.iter()
                .filter(|id| !matches!(id.as_str(), "auto" | "default"))
                .map(|id| serde_json::json!({"id": id, "name": id, "description": ""}))
                .collect(),
        );
    }
    if capabilities.web_search == Some(false) {
        model
            .as_object_mut()
            .unwrap()
            .remove("web_search_tool_type");
    }
    if capabilities.access_programs == Some(false) {
        model["available_access_programs"] = serde_json::json!({"cyber": []});
    }

    if let Some(images) = capabilities.images {
        let modalities = model.get_mut("input_modalities");
        match (images, modalities) {
            (false, Some(Value::Array(values))) => {
                values.retain(|value| value.as_str() != Some("image"));
            }
            (false, None) => model["input_modalities"] = serde_json::json!(["text"]),
            (true, Some(Value::Array(values))) => {
                if !values.iter().any(|value| value.as_str() == Some("image")) {
                    values.push(Value::String("image".into()));
                }
            }
            // Codex's omission default already includes both text and image.
            (true, None) => {}
            (_, Some(_)) => {}
        }
    }

    prune_defaults(model);
}

fn apply_response_metadata(model: &mut Value, capabilities: &Capabilities) {
    if let Some(default) = &capabilities.reasoning_summary_default {
        if matches!(default.as_str(), "auto" | "concise" | "detailed" | "none") {
            model["default_reasoning_summary"] = Value::String(default.clone());
        } else {
            model
                .as_object_mut()
                .unwrap()
                .remove("default_reasoning_summary");
        }
    }
    if let Some(levels) = &capabilities.reasoning_summary_levels {
        let current = model
            .get("default_reasoning_summary")
            .and_then(Value::as_str)
            .unwrap_or("auto");
        if !levels.iter().any(|level| level == current) {
            if let Some(default) = levels
                .iter()
                .find(|level| matches!(level.as_str(), "auto" | "concise" | "detailed" | "none"))
            {
                model["default_reasoning_summary"] = Value::String(default.clone());
            } else {
                model["supports_reasoning_summary_parameter"] = Value::Bool(false);
                model
                    .as_object_mut()
                    .unwrap()
                    .remove("default_reasoning_summary");
            }
        }
    }
    if let Some(levels) = &capabilities.verbosity_levels {
        model["support_verbosity"] = Value::Bool(
            levels
                .iter()
                .any(|level| matches!(level.as_str(), "low" | "medium" | "high")),
        );
        remove_excluded_default(model, "default_verbosity", levels);
    }
    if let Some(default) = &capabilities.verbosity_default {
        if matches!(default.as_str(), "low" | "medium" | "high") {
            model["default_verbosity"] = Value::String(default.clone());
        } else {
            model.as_object_mut().unwrap().remove("default_verbosity");
        }
    }
}

fn remove_excluded_default(model: &mut Value, field: &str, values: &[String]) {
    if model
        .get(field)
        .and_then(Value::as_str)
        .is_some_and(|value| !values.iter().any(|allowed| allowed == value))
    {
        model.as_object_mut().unwrap().remove(field);
    }
}

/// Keep projected catalogs compact where Codex itself treats omission as the default.
fn prune_defaults(model: &mut Value) {
    let Some(fields) = model.as_object_mut() else {
        return;
    };
    if fields.get("supports_reasoning_summary_parameter") == Some(&Value::Bool(true)) {
        fields.remove("supports_reasoning_summary_parameter");
    }
    if fields
        .get("default_reasoning_summary")
        .and_then(Value::as_str)
        == Some("auto")
    {
        fields.remove("default_reasoning_summary");
    }
    for field in [
        "use_responses_lite",
        "supports_search_tool",
        "supports_image_detail_original",
        "supports_experimental_context",
    ] {
        if fields.get(field) == Some(&Value::Bool(false)) {
            fields.remove(field);
        }
    }
    for field in ["additional_speed_tiers", "service_tiers"] {
        if fields
            .get(field)
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        {
            fields.remove(field);
        }
    }
    if fields
        .get("default_service_tier")
        .is_some_and(Value::is_null)
    {
        fields.remove("default_service_tier");
    }
    if fields
        .get("available_access_programs")
        .is_some_and(Value::is_null)
    {
        fields.remove("available_access_programs");
    }
    if fields.get("web_search_tool_type").and_then(Value::as_str) == Some("text") {
        fields.remove("web_search_tool_type");
    }
}

pub(super) fn rewrite_inherited_instructions(
    model: &mut Value,
    local_native_model: Option<&Value>,
) {
    if local_native_model
        .and_then(|local| local.get("base_instructions"))
        .is_none()
        && let Some(text) = model.get("base_instructions").and_then(Value::as_str)
        && let Some(rewritten) = rewrite_identity(text, model)
    {
        model["base_instructions"] = Value::String(rewritten);
    }
    let explicit = local_native_model
        .and_then(|local| local.get("model_messages"))
        .and_then(|messages| messages.get("instructions_template"))
        .is_some();
    if !explicit
        && let Some(text) = model
            .pointer("/model_messages/instructions_template")
            .and_then(Value::as_str)
        && let Some(rewritten) = rewrite_identity(text, model)
    {
        model["model_messages"]["instructions_template"] = Value::String(rewritten);
    }
}

fn rewrite_identity(template: &str, model: &Value) -> Option<String> {
    let start = template.find("GPT-")?;
    let token = template[start..].split(char::is_whitespace).next()?;
    let name = token.trim_end_matches(|ch: char| {
        ch.is_ascii_punctuation() || matches!(ch, '’' | '”' | '»' | '…' | '—' | '–')
    });
    if name.len() <= "GPT-".len() {
        return None;
    }
    let punctuation = &token[name.len()..];
    let punctuation = if punctuation.is_empty() {
        ","
    } else {
        punctuation
    };
    let suffix = &template[start + token.len()..];
    Some(format!(
        "{}{}, not GPT{punctuation}{suffix}",
        &template[..start],
        model
            .get("display_name")
            .and_then(Value::as_str)
            .or_else(|| model.get("slug").and_then(Value::as_str))
            .unwrap_or("model"),
    ))
}

pub(super) fn model_hint<'a>(headers: &'a HeaderMap, hint: &mut ModelHint<'a>) -> Result<()> {
    for value in headers.get_all("x-codex-routing-hint") {
        let value = value
            .to_str()
            .map_err(|_| Error::bad_request("Invalid Codex routing hint"))?;
        for field in value.split(';') {
            if let Some(value) = field.trim().strip_prefix("model=") {
                hint.set(value)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn inherited_gpt_openings_are_rewritten_and_local_native_openings_stay_verbatim() {
        let mut row = json!({"display_name":"local","base_instructions":"You are Codex, an agent based on GPT-6. Keep going.","model_messages":{"instructions_template":"You are Codex, a coding agent based on GPT-6. Continue."}});
        rewrite_inherited_instructions(&mut row, None);
        assert_eq!(
            row["base_instructions"],
            "You are Codex, an agent based on local, not GPT. Keep going."
        );
        assert_eq!(
            row["model_messages"]["instructions_template"],
            "You are Codex, a coding agent based on local, not GPT. Continue."
        );

        let local = json!({"model_messages":{"instructions_template":"leave exactly"}});
        row["model_messages"]["instructions_template"] = json!("leave exactly");
        rewrite_inherited_instructions(&mut row, Some(&local));
        assert_eq!(
            row["model_messages"]["instructions_template"],
            "leave exactly"
        );
    }

    #[test]
    fn identity_rewrite_consumes_model_suffixes_and_preserves_punctuation() {
        for name in [
            "GPT-4o",
            "GPT-4o-mini",
            "GPT-4.1",
            "GPT-6-luna",
            "GPT-4.beta",
            "GPT-4/vendor:variant",
        ] {
            for punctuation in [
                ".",
                ". Keep going.",
                "...",
                ",",
                ":",
                ";",
                "?!",
                ".\"",
                ".)",
                ".”",
            ] {
                let original = format!("You are {name}{punctuation}");
                assert_eq!(
                    rewrite_identity(&original, &json!({"display_name": "local"})),
                    Some(format!("You are local, not GPT{punctuation}"))
                );
            }
        }
    }

    #[test]
    fn identity_rewrite_separates_following_prose() {
        assert_eq!(
            rewrite_identity("You are GPT-4o", &json!({"display_name": "local"})),
            Some("You are local, not GPT,".into())
        );
        for whitespace in [" ", "\n", "\t"] {
            assert_eq!(
                rewrite_identity(
                    &format!("You are GPT-4o{whitespace}trained to code."),
                    &json!({"display_name": "local"}),
                ),
                Some(format!(
                    "You are local, not GPT,{whitespace}trained to code."
                ))
            );
        }
    }

    #[test]
    fn identity_rewrite_does_not_depend_on_opening_wording() {
        let original = "Custom identity: GPT-6.1. Later GPT-6 stays literal.";
        let mut row = json!({"display_name":"local","base_instructions":original});
        rewrite_inherited_instructions(&mut row, None);
        assert_eq!(
            row["base_instructions"],
            "Custom identity: local, not GPT. Later GPT-6 stays literal."
        );
    }

    #[test]
    fn codex_projection_prunes_omission_defaults_and_keeps_nondefault_values() {
        let mut row = json!({
            "slug":"m", "additional_speed_tiers":[], "service_tiers":[],
            "default_service_tier":null, "available_access_programs":null,
            "web_search_tool_type":"text", "supports_search_tool":false,
            "supports_image_detail_original":false, "supports_experimental_context":false
        });
        apply_capabilities(&mut row, &Capabilities::default());
        for key in [
            "additional_speed_tiers",
            "service_tiers",
            "default_service_tier",
            "available_access_programs",
            "web_search_tool_type",
            "supports_search_tool",
            "supports_image_detail_original",
            "supports_experimental_context",
        ] {
            assert!(row.get(key).is_none(), "{key} should be omitted");
        }

        let mut row = json!({"slug":"m"});
        apply_capabilities(
            &mut row,
            &Capabilities {
                tool_search: Some(true),
                image_detail_original: Some(true),
                history_notes: Some(true),
                web_search: Some(true),
                service_tiers: Some(true),
                access_programs: Some(true),
                ..Default::default()
            },
        );
        assert_eq!(row["supports_search_tool"], true);
        assert_eq!(row["supports_image_detail_original"], true);
        assert_eq!(row["supports_experimental_context"], true);
    }

    #[test]
    fn codex_capability_false_values_and_unknowns_preserve_intended_distinctions() {
        let mut row = json!({
            "slug":"m", "additional_speed_tiers":["fast"],
            "service_tiers":[{"id":"fast"}], "default_service_tier":"fast",
            "web_search_tool_type":"text_and_image", "available_access_programs":{"cyber":["standard"]}
        });
        apply_capabilities(
            &mut row,
            &Capabilities {
                service_tiers: Some(false),
                web_search: Some(false),
                access_programs: Some(false),
                history_notes: Some(false),
                ..Default::default()
            },
        );
        assert!(row.get("additional_speed_tiers").is_none());
        assert!(row.get("service_tiers").is_none());
        assert!(row.get("default_service_tier").is_none());
        assert!(row.get("web_search_tool_type").is_none());
        assert_eq!(row["available_access_programs"], json!({"cyber":[]}));
        assert!(row.get("supports_experimental_context").is_none());

        let mut unknown = json!({"slug":"m","web_search_tool_type":"text_and_image"});
        apply_capabilities(&mut unknown, &Capabilities::default());
        assert_eq!(unknown["web_search_tool_type"], "text_and_image");
    }

    #[test]
    fn schema_tiers_replace_template_choices_and_default() {
        let template = json!({
            "slug":"m", "additional_speed_tiers":["fast"],
            "service_tiers":[{"id":"old","name":"Old","description":"Old pricing"}],
            "default_service_tier":"old"
        });
        let mut row = template.clone();
        let mut facts = Capabilities {
            service_tiers: Some(true),
            service_tier_ids: Some(vec![
                "auto".into(),
                "default".into(),
                "flex".into(),
                "priority".into(),
            ]),
            ..Default::default()
        };
        apply_capabilities(&mut row, &facts);
        assert_eq!(
            row["service_tiers"],
            json!([
                {"id":"flex","name":"flex","description":""},
                {"id":"priority","name":"priority","description":""}
            ])
        );
        assert!(row.get("additional_speed_tiers").is_none());
        assert!(row.get("default_service_tier").is_none());

        facts.service_tiers = Some(false);
        let mut row = template;
        apply_capabilities(&mut row, &facts);
        assert!(row.get("service_tiers").is_none());
    }

    #[test]
    fn excluded_summary_default_uses_an_allowed_mode_not_implicit_auto() {
        let mut row = json!({"default_reasoning_summary":"auto"});
        apply_capabilities(
            &mut row,
            &Capabilities {
                reasoning_summary: Some(true),
                reasoning_summary_levels: Some(vec!["concise".into(), "detailed".into()]),
                ..Default::default()
            },
        );
        assert_eq!(row["default_reasoning_summary"], "concise");
        assert!(row.get("supports_reasoning_summary_parameter").is_none());

        apply_capabilities(
            &mut row,
            &Capabilities {
                reasoning_summary: Some(true),
                reasoning_summary_levels: Some(vec!["unrepresentable".into()]),
                ..Default::default()
            },
        );
        assert_eq!(row["supports_reasoning_summary_parameter"], false);
        assert!(row.get("default_reasoning_summary").is_none());
    }
}
