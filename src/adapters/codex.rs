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

/// Apply explicit provider capability facts to a projected Codex row.
pub(super) fn apply_capabilities(model: &mut Value, capabilities: &Capabilities) {
    for (fact, field) in [
        (capabilities.responses_lite, "use_responses_lite"),
        (capabilities.tool_search, "supports_search_tool"),
        (
            capabilities.image_detail_original,
            "supports_image_detail_original",
        ),
    ] {
        if let Some(value) = fact {
            model[field] = Value::Bool(value);
        }
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
}
