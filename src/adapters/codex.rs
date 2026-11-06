use axum::http::{HeaderMap, HeaderName};
use serde_json::Value;

use super::{
    catalog::{CatalogLayout, rename_field},
    client::ModelHint,
};
use crate::capabilities::Capabilities;
use crate::error::{Error, Result};
use crate::protocol::{field, pointer, value};
use serde_json::Map;

mod compaction;
mod compaction_transport;
mod multiagent;
mod response;
mod transport;
mod websocket;

pub(super) use transport::{adapt_websocket_response, forward};
pub(super) use websocket::route as websocket_request;

mod event {
    pub const CREATE: &str = "response.create";
    pub const CREATED: &str = "response.created";
    pub const COMPLETED: &str = "response.completed";
    pub const IN_PROGRESS: &str = "response.in_progress";
    pub const FAILED: &str = "response.failed";
    pub const INCOMPLETE: &str = "response.incomplete";
    pub const OUTPUT_ITEM_ADDED: &str = "response.output_item.added";
    pub const OUTPUT_ITEM_DONE: &str = "response.output_item.done";
}

mod collaboration_tool {
    pub const SPAWN_AGENT: &str = "spawn_agent";
    pub const SEND_MESSAGE: &str = "send_message";
    pub const FOLLOWUP_TASK: &str = "followup_task";
}

mod error_code {
    pub const CONTEXT_LENGTH_EXCEEDED: &str = "context_length_exceeded";
    pub const MAX_CONTEXT_LENGTH_EXCEEDED: &str = "max_context_length_exceeded";
    pub const PREVIOUS_RESPONSE_NOT_FOUND: &str = "previous_response_not_found";
    pub const WEBSOCKET_UPGRADE_REJECTED: &str = "websocket_upgrade_rejected";
}

const ROUTING_HINT_HEADER: HeaderName = HeaderName::from_static("x-codex-routing-hint");
const FULL_HISTORY_REQUIRED: &str = "Synthetic compaction requires the complete input history";
const SSE_DATA_PREFIX: &[u8] = b"data: ";
const GPT_IDENTITY_PREFIX: &str = "GPT-";
const REASONING_SUMMARIES: &[&str] = &[value::AUTO, "concise", "detailed", value::NONE];
const VERBOSITIES: &[&str] = &["low", "medium", "high"];

pub(super) fn matches_originator(originator: &str) -> bool {
    originator.to_ascii_lowercase().contains("codex")
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

pub(super) fn postprocess_settings(fields: &mut Map<String, Value>) {
    rename_reasoning_levels(
        fields.get_mut(field::SUPPORTED_REASONING_LEVELS),
        field::LEVEL,
        field::EFFORT,
    );
}

pub(super) fn postprocess_shared_metadata(fields: &mut Map<String, Value>) {
    rename_reasoning_levels(
        fields.get_mut(field::REASONING_LEVELS),
        field::EFFORT,
        field::LEVEL,
    );
}

pub(super) fn apply_context_window(
    result: &mut Value,
    source: super::catalog::CatalogFormat,
    model: &Value,
) {
    if let Some(context) = source.context_window(model) {
        result
            .as_object_mut()
            .unwrap()
            .remove(field::CONTEXT_WINDOW);
        result[field::MAX_CONTEXT_WINDOW] = context.clone();
    }
}

pub(super) fn apply_projection_metadata(
    result: &mut Value,
    has_base: bool,
    local_shared: &Map<String, Value>,
    target_id: &str,
) {
    if has_base && !local_shared.contains_key(field::NAME) {
        result[field::DISPLAY_NAME] = Value::String(target_id.to_owned());
    }
}

pub(super) const CATALOG: CatalogLayout = CatalogLayout {
    models: field::MODELS,
    id: field::SLUG,
};

/// Apply resolved capability facts to a projected Codex row.
pub(super) fn apply_capabilities(model: &mut Value, capabilities: &Capabilities) {
    for (fact, field) in [
        (capabilities.responses_lite, field::USE_RESPONSES_LITE),
        (capabilities.tool_search, field::SUPPORTS_SEARCH_TOOL),
        (
            capabilities.image_detail_original,
            field::SUPPORTS_IMAGE_DETAIL_ORIGINAL,
        ),
        (
            capabilities.history_notes,
            field::SUPPORTS_EXPERIMENTAL_CONTEXT,
        ),
        (
            capabilities.reasoning_summary,
            field::SUPPORTS_REASONING_SUMMARY_PARAMETER,
        ),
    ] {
        if let Some(value) = fact {
            model[field] = Value::Bool(value);
        }
    }
    apply_response_metadata(model, capabilities);

    if capabilities.service_tiers == Some(false) || capabilities.service_tier_ids.is_some() {
        for field in [
            field::ADDITIONAL_SPEED_TIERS,
            field::SERVICE_TIERS,
            field::DEFAULT_SERVICE_TIER,
        ] {
            model.as_object_mut().unwrap().remove(field);
        }
    }
    if capabilities.service_tiers != Some(false)
        && let Some(ids) = &capabilities.service_tier_ids
    {
        model[field::SERVICE_TIERS] = Value::Array(
            ids.iter()
                .filter(|id| !matches!(id.as_str(), value::AUTO | value::DEFAULT))
                .map(|id| serde_json::json!({"id": id, "name": id, "description": ""}))
                .collect(),
        );
    }
    if capabilities.web_search == Some(false) {
        model
            .as_object_mut()
            .unwrap()
            .remove(field::WEB_SEARCH_TOOL_TYPE);
    }
    if capabilities.access_programs == Some(false) {
        model[field::AVAILABLE_ACCESS_PROGRAMS] = serde_json::json!({"cyber": []});
    }

    if let Some(images) = capabilities.images {
        let modalities = model.get_mut(field::INPUT_MODALITIES);
        match (images, modalities) {
            (false, Some(Value::Array(values))) => {
                values.retain(|value| value.as_str() != Some(value::IMAGE));
            }
            (false, None) => model[field::INPUT_MODALITIES] = serde_json::json!(["text"]),
            (true, Some(Value::Array(values))) => {
                if !values
                    .iter()
                    .any(|value| value.as_str() == Some(value::IMAGE))
                {
                    values.push(Value::String(value::IMAGE.into()));
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
        if REASONING_SUMMARIES.contains(&default.as_str()) {
            model[field::DEFAULT_REASONING_SUMMARY] = Value::String(default.clone());
        } else {
            model
                .as_object_mut()
                .unwrap()
                .remove(field::DEFAULT_REASONING_SUMMARY);
        }
    }
    if let Some(levels) = &capabilities.reasoning_summary_levels {
        let current = model
            .get(field::DEFAULT_REASONING_SUMMARY)
            .and_then(Value::as_str)
            .unwrap_or(value::AUTO);
        if !levels.iter().any(|level| level == current) {
            if let Some(default) = levels
                .iter()
                .find(|level| REASONING_SUMMARIES.contains(&level.as_str()))
            {
                model[field::DEFAULT_REASONING_SUMMARY] = Value::String(default.clone());
            } else {
                model[field::SUPPORTS_REASONING_SUMMARY_PARAMETER] = Value::Bool(false);
                model
                    .as_object_mut()
                    .unwrap()
                    .remove(field::DEFAULT_REASONING_SUMMARY);
            }
        }
    }
    if let Some(levels) = &capabilities.verbosity_levels {
        model[field::SUPPORT_VERBOSITY] = Value::Bool(
            levels
                .iter()
                .any(|level| VERBOSITIES.contains(&level.as_str())),
        );
        remove_excluded_default(model, field::DEFAULT_VERBOSITY, levels);
    }
    if let Some(default) = &capabilities.verbosity_default {
        if VERBOSITIES.contains(&default.as_str()) {
            model[field::DEFAULT_VERBOSITY] = Value::String(default.clone());
        } else {
            model
                .as_object_mut()
                .unwrap()
                .remove(field::DEFAULT_VERBOSITY);
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
    if fields.get(field::SUPPORTS_REASONING_SUMMARY_PARAMETER) == Some(&Value::Bool(true)) {
        fields.remove(field::SUPPORTS_REASONING_SUMMARY_PARAMETER);
    }
    if fields
        .get(field::DEFAULT_REASONING_SUMMARY)
        .and_then(Value::as_str)
        == Some(value::AUTO)
    {
        fields.remove(field::DEFAULT_REASONING_SUMMARY);
    }
    for field in [
        field::USE_RESPONSES_LITE,
        field::SUPPORTS_SEARCH_TOOL,
        field::SUPPORTS_IMAGE_DETAIL_ORIGINAL,
        field::SUPPORTS_EXPERIMENTAL_CONTEXT,
    ] {
        if fields.get(field) == Some(&Value::Bool(false)) {
            fields.remove(field);
        }
    }
    for field in [field::ADDITIONAL_SPEED_TIERS, field::SERVICE_TIERS] {
        if fields
            .get(field)
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        {
            fields.remove(field);
        }
    }
    if fields
        .get(field::DEFAULT_SERVICE_TIER)
        .is_some_and(Value::is_null)
    {
        fields.remove(field::DEFAULT_SERVICE_TIER);
    }
    if fields
        .get(field::AVAILABLE_ACCESS_PROGRAMS)
        .is_some_and(Value::is_null)
    {
        fields.remove(field::AVAILABLE_ACCESS_PROGRAMS);
    }
    if fields
        .get(field::WEB_SEARCH_TOOL_TYPE)
        .and_then(Value::as_str)
        == Some(value::TEXT)
    {
        fields.remove(field::WEB_SEARCH_TOOL_TYPE);
    }
}

pub(super) fn rewrite_inherited_instructions(
    model: &mut Value,
    local_native_model: Option<&Value>,
) {
    if local_native_model
        .and_then(|local| local.get(field::BASE_INSTRUCTIONS))
        .is_none()
        && let Some(text) = model.get(field::BASE_INSTRUCTIONS).and_then(Value::as_str)
        && let Some(rewritten) = rewrite_identity(text, model)
    {
        model[field::BASE_INSTRUCTIONS] = Value::String(rewritten);
    }
    let explicit = local_native_model
        .and_then(|local| local.get(field::MODEL_MESSAGES))
        .and_then(|messages| messages.get(field::INSTRUCTIONS_TEMPLATE))
        .is_some();
    if !explicit
        && let Some(text) = model
            .pointer(pointer::INSTRUCTIONS_TEMPLATE)
            .and_then(Value::as_str)
        && let Some(rewritten) = rewrite_identity(text, model)
    {
        model[field::MODEL_MESSAGES][field::INSTRUCTIONS_TEMPLATE] = Value::String(rewritten);
    }
}

fn rewrite_identity(template: &str, model: &Value) -> Option<String> {
    let start = template.find(GPT_IDENTITY_PREFIX)?;
    let token = template[start..].split(char::is_whitespace).next()?;
    let name = token.trim_end_matches(|ch: char| {
        ch.is_ascii_punctuation() || matches!(ch, '’' | '”' | '»' | '…' | '—' | '–')
    });
    if name.len() <= GPT_IDENTITY_PREFIX.len() {
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
            .get(field::DISPLAY_NAME)
            .and_then(Value::as_str)
            .or_else(|| model.get(field::SLUG).and_then(Value::as_str))
            .unwrap_or(value::MODEL),
    ))
}

pub(super) fn model_hint<'a>(headers: &'a HeaderMap, hint: &mut ModelHint<'a>) -> Result<()> {
    for value in headers.get_all(ROUTING_HINT_HEADER) {
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
