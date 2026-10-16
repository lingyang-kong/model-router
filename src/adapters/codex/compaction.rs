//! Codex Remote V2 compaction support.
//!
//! The prompt text below follows the Codex 0.162.1 templates at
//! https://github.com/openai/codex/tree/rust-v0.162.1/codex-rs/prompts/templates/compact
//! Those prompt files are Apache-2.0 licensed; see the fetched license in the build output.

use serde::{Deserialize, Serialize};
use serde_json::{Value, value::RawValue};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::error::{Error, Result};

const MARKER: &str = "model-router:compaction:v1:";
const SYNTHETIC_RESPONSE_PREFIX: &str = "resp_model_router_";
const NATIVE_PROMPT: &str = include_str!(concat!(env!("CODEX_PROMPTS_DIR"), "/compact_prompt.txt"));
const NATIVE_SUMMARY_PREFIX: &str =
    include_str!(concat!(env!("CODEX_PROMPTS_DIR"), "/summary_prefix.txt"));
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Default, Copy, Clone, PartialEq)]
pub(super) enum Mode {
    #[default]
    Native,
    Text,
}

pub(super) struct Prepared {
    pub rewritten: Option<Vec<u8>>,
    pub summary: Option<SummaryRequest>,
    pub native_trigger: bool,
}

struct ItemMeta {
    kind: Option<String>,
    encrypted_content: Option<String>,
    has_encrypted_content: bool,
    has_readable_reasoning: bool,
    call_id: Option<String>,
    role: Option<String>,
}

#[derive(Serialize)]
struct UserMessage<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    role: &'static str,
    content: Vec<TextContent<'a>>,
}

#[derive(Serialize)]
struct TextContent<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    text: &'a str,
}

pub(super) struct SummaryRequest {
    model: Option<String>,
    instructions: Option<Box<RawValue>>,
    reasoning: Option<Box<RawValue>>,
    max_output_tokens: Option<Box<RawValue>>,
    history: Vec<Box<RawValue>>,
    pub(super) stream: bool,
}

#[derive(Serialize)]
struct SummaryBody<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    instructions: Option<&'a RawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<&'a RawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_output_tokens: Option<&'a RawValue>,
    input: Vec<&'a RawValue>,
    stream: bool,
    store: bool,
    tools: Vec<Value>,
    tool_choice: &'static str,
}

impl SummaryRequest {
    pub(super) fn body(&self) -> Result<Vec<u8>> {
        let mut input: Vec<&RawValue> = self.history.iter().map(|item| item.as_ref()).collect();
        let prompt = serde_json::value::to_raw_value(&UserMessage {
            kind: "message",
            role: "user",
            content: vec![TextContent {
                kind: "input_text",
                text: NATIVE_PROMPT,
            }],
        })
        .map_err(Error::bad_request)?;
        input.push(&prompt);
        let body = SummaryBody {
            model: self.model.as_deref(),
            instructions: self.instructions.as_deref(),
            reasoning: self.reasoning.as_deref(),
            max_output_tokens: self.max_output_tokens.as_deref(),
            input,
            stream: false,
            store: false,
            tools: Vec::new(),
            tool_choice: "none",
        };
        serde_json::to_vec(&body).map_err(Error::bad_request)
    }

    /// Remove the oldest complete call/result group while preserving instructions and the last
    /// unfinished user segment. Returns false when no safe reduction remains.
    pub(super) fn trim_oldest(&mut self) -> bool {
        if self.history.len() <= 1 {
            return false;
        }
        let last_user = self
            .history
            .iter()
            .rposition(|item| {
                item_meta(item).ok().and_then(|meta| meta.role).as_deref() == Some("user")
            })
            .unwrap_or_else(|| self.history.len().saturating_sub(1));
        let Some(candidate) = self
            .history
            .iter()
            .position(|item| {
                item_meta(item).ok().is_none_or(|meta| {
                    !matches!(meta.role.as_deref(), Some("system" | "developer"))
                        && meta.kind.as_deref() != Some("instructions")
                })
            })
            .filter(|index| *index < last_user)
        else {
            return false;
        };
        let call_id = item_meta(&self.history[candidate])
            .ok()
            .and_then(|meta| meta.call_id);
        if call_id.as_ref().is_some_and(|id| {
            self.history.iter().enumerate().any(|(index, item)| {
                index >= last_user
                    && item_meta(item).ok().and_then(|meta| meta.call_id).as_ref() == Some(id)
            })
        }) {
            return false;
        }
        let mut kept = Vec::with_capacity(self.history.len());
        for (index, item) in self.history.drain(..).enumerate() {
            let paired = call_id.as_ref().is_some_and(|id| {
                index > candidate
                    && index < last_user
                    && item_meta(&item).ok().and_then(|meta| meta.call_id).as_ref() == Some(id)
            });
            if index != candidate && !paired {
                kept.push(item);
            }
        }
        self.history = kept;
        true
    }

    pub(super) fn finish(&self, response_bytes: &[u8]) -> Result<Value> {
        let response: Value = serde_json::from_slice(response_bytes).map_err(Error::bad_gateway)?;
        if response.get("status").and_then(Value::as_str) != Some("completed")
            || response.get("error").is_some_and(|v| !v.is_null())
            || response
                .get("incomplete_details")
                .is_some_and(|v| !v.is_null())
        {
            return Err(Error::bad_gateway("Compaction summary did not complete"));
        }
        let output = response
            .get("output")
            .and_then(Value::as_array)
            .ok_or_else(|| Error::bad_gateway("Compaction response has no output"))?;
        let mut text = String::new();
        for item in output {
            if item.get("type").and_then(Value::as_str) == Some("reasoning") {
                continue;
            }
            if item.get("type").and_then(Value::as_str) != Some("message")
                || item.get("role").and_then(Value::as_str) != Some("assistant")
            {
                return Err(Error::bad_gateway(
                    "Compaction response attempted a tool call",
                ));
            }
            if item
                .get("status")
                .and_then(Value::as_str)
                .is_some_and(|status| status != "completed")
            {
                return Err(Error::bad_gateway(
                    "Compaction summary message did not complete",
                ));
            }
            for content in item
                .get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                match content.get("type").and_then(Value::as_str) {
                    Some("refusal") => {
                        return Err(Error::bad_gateway("Compaction summary was refused"));
                    }
                    Some("output_text") => {
                        if let Some(s) = content.get("text").and_then(Value::as_str) {
                            text.push_str(s);
                        }
                    }
                    _ => {
                        return Err(Error::bad_gateway(
                            "Compaction response contained unsupported output",
                        ));
                    }
                }
            }
        }
        if text.trim().is_empty() {
            return Err(Error::bad_gateway(
                "Compaction response has no summary text",
            ));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let next = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let suffix = format!("{}_{next}", now.as_nanos());
        let id = format!("cmp_model_router_{suffix}");
        let mut compacted = serde_json::json!({
            "id": format!("{SYNTHETIC_RESPONSE_PREFIX}{suffix}"),
            "created_at": now.as_secs(),
            "object":"response", "status":"completed",
            "output":[{"type":"compaction","id":id,"encrypted_content":format!("{MARKER}{text}")}],
            "usage":response.get("usage").cloned().unwrap_or(Value::Null)
        });
        if let Some(model) = response
            .get("model")
            .and_then(Value::as_str)
            .or(self.model.as_deref())
        {
            compacted["model"] = Value::String(model.to_owned());
        }
        Ok(compacted)
    }
}

pub(super) fn prepare(bytes: &[u8], mode: Mode, effective_model: Option<&str>) -> Result<Prepared> {
    if !may_contain(bytes, b"compaction") {
        return Ok(Prepared {
            rewritten: None,
            summary: None,
            native_trigger: false,
        });
    }
    let fields: std::collections::HashMap<String, Box<RawValue>> =
        serde_json::from_slice(bytes).map_err(Error::bad_request)?;
    let Some(input_raw) = fields.get("input") else {
        return Ok(Prepared {
            rewritten: None,
            summary: None,
            native_trigger: false,
        });
    };
    if !input_raw.get().trim_start().starts_with('[') {
        return Ok(Prepared {
            rewritten: None,
            summary: None,
            native_trigger: false,
        });
    }
    let items: Vec<Box<RawValue>> =
        serde_json::from_str(input_raw.get()).map_err(Error::bad_request)?;
    let trigger_positions: Vec<usize> = items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| {
            item_meta(item)
                .ok()
                .filter(|meta| meta.kind.as_deref() == Some("compaction_trigger"))
                .map(|_| index)
        })
        .collect();
    let trigger = !trigger_positions.is_empty();
    let checkpoints: Vec<(usize, Option<String>)> = items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| {
            item_meta(item)
                .ok()
                .filter(|meta| meta.kind.as_deref() == Some("compaction"))
                .map(|meta| (index, meta.encrypted_content))
        })
        .collect();
    let has_synthetic_checkpoint = checkpoints
        .iter()
        .any(|(_, value)| value.as_deref().is_some_and(|v| v.starts_with(MARKER)));
    if mode == Mode::Native && !has_synthetic_checkpoint {
        return Ok(Prepared {
            rewritten: None,
            summary: None,
            native_trigger: trigger,
        });
    }
    if trigger_positions
        .iter()
        .any(|index| *index + 1 != items.len())
    {
        return Err(Error::bad_request(
            "Codex compaction_trigger must be the final input item",
        ));
    }
    if !trigger && checkpoints.is_empty() {
        return Ok(Prepared {
            rewritten: None,
            summary: None,
            native_trigger: false,
        });
    }
    let mut hydrated = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        if checkpoints.iter().any(|(i, _)| *i == index) {
            let value = checkpoints
                .iter()
                .find(|(i, _)| *i == index)
                .and_then(|(_, value)| value.as_deref());
            match value.and_then(|v| v.strip_prefix(MARKER)) {
                Some(summary) => hydrated.push(hydrated_message(summary)?),
                None if mode == Mode::Native => hydrated.push(clone_raw(item)?),
                None => {
                    return Err(Error::bad_request(
                        "Unsupported opaque Codex compaction checkpoint",
                    ));
                }
            }
        } else if trigger && mode == Mode::Text && index + 1 == items.len() {
            continue;
        } else if mode == Mode::Text
            && item_meta(item).ok().is_some_and(|meta| {
                meta.kind.as_deref() == Some("reasoning")
                    && !meta.has_readable_reasoning
                    && (meta.has_encrypted_content
                        && meta
                            .encrypted_content
                            .as_deref()
                            .is_none_or(|value| !value.is_empty()))
            })
        {
            return Err(Error::bad_request(
                "Synthetic compaction cannot process opaque encrypted reasoning items",
            ));
        } else {
            hydrated.push(clone_raw(item)?);
        }
    }
    if mode == Mode::Native {
        return Ok(Prepared {
            rewritten: Some(rebuild(fields, &hydrated, has_synthetic_checkpoint)?),
            summary: None,
            native_trigger: trigger,
        });
    }
    if !trigger {
        return Ok(Prepared {
            rewritten: Some(rebuild(fields, &hydrated, has_synthetic_checkpoint)?),
            summary: None,
            native_trigger: false,
        });
    }
    let model = fields
        .get("model")
        .and_then(|value| serde_json::from_str::<String>(value.get()).ok())
        .or_else(|| effective_model.map(str::to_owned));
    if fields
        .get("previous_response_id")
        .is_some_and(|v| v.get() != "null")
        && !has_synthetic_checkpoint
    {
        return Err(Error::bad_request(
            "Synthetic compaction requires the complete input history",
        ));
    }
    let history = hydrated
        .into_iter()
        .filter(|item| {
            item_meta(item).ok().and_then(|meta| meta.kind).as_deref() != Some("additional_tools")
        })
        .collect();
    let request = SummaryRequest {
        model,
        instructions: fields
            .get("instructions")
            .map(|v| clone_raw(v))
            .transpose()?,
        reasoning: fields
            .get("reasoning")
            .map(|value| clone_raw(value))
            .transpose()?,
        max_output_tokens: fields
            .get("max_output_tokens")
            .map(|value| clone_raw(value))
            .transpose()?,
        history,
        stream: fields
            .get("stream")
            .and_then(|v| serde_json::from_str::<bool>(v.get()).ok())
            .unwrap_or(false),
    };
    Ok(Prepared {
        rewritten: None,
        summary: Some(request),
        native_trigger: false,
    })
}

/// WebSocket callers use this to ask Codex for a full-history replay when their current request
/// depends on router-created state that cannot be recovered from the upstream provider.
pub(super) fn needs_full_history(bytes: &[u8], mode: Mode) -> Result<bool> {
    if !may_contain(bytes, b"previous_response_id") {
        return Ok(false);
    }
    let fields: std::collections::HashMap<String, Box<RawValue>> =
        serde_json::from_slice(bytes).map_err(Error::bad_request)?;
    let Some(previous) = fields.get("previous_response_id") else {
        return Ok(false);
    };
    let previous = serde_json::from_str::<String>(previous.get()).ok();
    if previous
        .as_deref()
        .is_some_and(|id| id.starts_with(SYNTHETIC_RESPONSE_PREFIX))
    {
        return Ok(true);
    }
    if mode != Mode::Text || previous.is_none() {
        return Ok(false);
    }
    let Some(input) = fields.get("input") else {
        return Ok(false);
    };
    if !input.get().trim_start().starts_with('[') {
        return Ok(false);
    }
    let items: Vec<Box<RawValue>> =
        serde_json::from_str(input.get()).map_err(Error::bad_request)?;
    Ok(items
        .last()
        .and_then(|item| item_meta(item).ok())
        .is_some_and(|meta| meta.kind.as_deref() == Some("compaction_trigger")))
}

fn rebuild(
    mut fields: std::collections::HashMap<String, Box<RawValue>>,
    items: &[Box<RawValue>],
    strip_previous_response_id: bool,
) -> Result<Vec<u8>> {
    let input = serde_json::to_string(items).map_err(Error::bad_request)?;
    fields.insert(
        "input".into(),
        RawValue::from_string(input).map_err(Error::bad_request)?,
    );
    if strip_previous_response_id {
        fields.remove("previous_response_id");
    }
    serde_json::to_vec(&fields).map_err(Error::bad_request)
}

fn may_contain(bytes: &[u8], token: &[u8]) -> bool {
    memchr::memchr(b'\\', bytes).is_some() || memchr::memmem::find(bytes, token).is_some()
}

fn hydrated_message(summary: &str) -> Result<Box<RawValue>> {
    let message = serde_json::json!({"type":"message","role":"user","content":[{"type":"input_text","text":format!("{NATIVE_SUMMARY_PREFIX}\n{summary}")}]});
    serde_json::value::to_raw_value(&message).map_err(Error::bad_request)
}

fn clone_raw(raw: &RawValue) -> Result<Box<RawValue>> {
    RawValue::from_string(raw.get().to_owned()).map_err(Error::bad_request)
}

fn item_meta(raw: &RawValue) -> serde_json::Result<ItemMeta> {
    let fields: std::collections::HashMap<String, Box<RawValue>> = serde_json::from_str(raw.get())?;
    let string = |name: &str| {
        fields.get(name).map_or(Ok(None), |value| {
            if value.get() == "null" {
                Ok(None)
            } else {
                serde_json::from_str::<String>(value.get()).map(Some)
            }
        })
    };
    Ok(ItemMeta {
        kind: string("type").unwrap_or(None),
        encrypted_content: string("encrypted_content").unwrap_or(None),
        has_encrypted_content: fields
            .get("encrypted_content")
            .is_some_and(|value| value.get() != "null"),
        has_readable_reasoning: string("type").ok().flatten().as_deref() == Some("reasoning")
            && ["summary", "content"].iter().any(|key| {
                fields
                    .get(*key)
                    .and_then(|value| serde_json::from_str::<Vec<Box<RawValue>>>(value.get()).ok())
                    .is_some_and(|parts| {
                        parts.iter().any(|part| {
                            #[derive(Deserialize)]
                            struct Text {
                                text: Option<String>,
                            }
                            serde_json::from_str::<Text>(part.get())
                                .ok()
                                .and_then(|part| part.text)
                                .is_some_and(|text| !text.trim().is_empty())
                        })
                    })
            }),
        call_id: string("call_id").unwrap_or(None),
        role: string("role").unwrap_or(None),
    })
}

pub(super) fn is_context_overflow(status: u16, body: &[u8]) -> bool {
    if status != 400 {
        return false;
    }
    let Ok(error) = serde_json::from_slice::<Value>(body) else {
        return false;
    };
    if matches!(
        error.pointer("/error/code").and_then(Value::as_str),
        Some("context_length_exceeded" | "max_context_length_exceeded")
    ) {
        return true;
    }
    // Some Responses servers expose a numeric error code with these explicit diagnostics.
    error
        .pointer("/error/message")
        .and_then(Value::as_str)
        .is_some_and(|message| {
            message.starts_with("Requested token count exceeds the model's maximum context length ")
                || (message.starts_with("The input (")
                    && message.contains(" tokens) is longer than the model's context length ("))
        })
}

pub(super) fn events(response: &Value, stream_id: Option<&Value>) -> Vec<Value> {
    let id = response.get("id").cloned().unwrap_or(Value::Null);
    let output = response
        .get("output")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut events = Vec::new();
    let mut seq = 0u64;
    let mut push = |kind: &str, mut data: Value| {
        if let Some(object) = data.as_object_mut() {
            object.insert("type".into(), Value::String(kind.into()));
            object.insert("sequence_number".into(), Value::from(seq));
            if let Some(stream_id) = stream_id {
                object.insert("stream_id".into(), stream_id.clone());
            }
        }
        seq += 1;
        events.push(data);
    };
    let mut created = response.clone();
    if let Some(obj) = created.as_object_mut() {
        obj.insert("status".into(), Value::String("in_progress".into()));
        obj.insert("output".into(), Value::Array(Vec::new()));
    }
    push("response.created", serde_json::json!({"response":created}));
    for (index, item) in output.iter().enumerate() {
        push(
            "response.output_item.added",
            serde_json::json!({"output_index":index,"item":item,"response_id":id}),
        );
        push(
            "response.output_item.done",
            serde_json::json!({"output_index":index,"item":item,"response_id":id}),
        );
    }
    push(
        "response.completed",
        serde_json::json!({"response":response}),
    );
    events
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRIGGER: &str = r#"{"type":"compaction_trigger"}"#;

    fn request(input: &str) -> Vec<u8> {
        format!(r#"{{"model":"test","stream":true,"instructions":"keep","input":[{input}]}}"#)
            .into_bytes()
    }

    fn completed(summary: &str) -> Vec<u8> {
        serde_json::json!({
            "id":"upstream-1", "status":"completed",
            "output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":summary}]}]
        }).to_string().into_bytes()
    }

    #[test]
    fn native_trigger_and_unknown_checkpoint_are_exact_passthrough() {
        let trigger = request(TRIGGER);
        let prepared = prepare(&trigger, Mode::Native, None).unwrap();
        assert_eq!(prepared.rewritten, None);
        assert!(prepared.native_trigger);

        let opaque = request(r#"{"type":"compaction","encrypted_content":"ciphertext"}"#);
        let prepared = prepare(&opaque, Mode::Native, None).unwrap();
        assert_eq!(prepared.rewritten, None);
        assert!(prepared.summary.is_none());

        let misplaced = request(&format!(r#"{TRIGGER},{{"type":"message","role":"user"}}"#));
        let prepared = prepare(&misplaced, Mode::Native, None).unwrap();
        assert_eq!(prepared.rewritten, None);
        assert!(prepared.native_trigger);

        let escaped = request(r#"{"type":"compaction\u005ftrigger"}"#);
        assert!(
            prepare(&escaped, Mode::Native, None)
                .unwrap()
                .native_trigger
        );
    }

    #[test]
    fn synthetic_compaction_hydrates_repeated_checkpoints_and_switches_to_native() {
        let first = request(&format!(
            r#"{{"type":"message","role":"user","content":"work"}},{TRIGGER}"#
        ));
        let prepared = prepare(&first, Mode::Text, None).unwrap();
        let summary = prepared.summary.unwrap();
        assert!(summary.stream);
        let response = summary.finish(&completed("remember the work")).unwrap();
        let checkpoint = response["output"][0].clone();
        let second = serde_json::json!({
            "model":"test", "input":[checkpoint, {"type":"message","role":"user","content":"continue"}, {"type":"compaction_trigger"}]
        }).to_string().into_bytes();
        let prepared = prepare(&second, Mode::Text, None).unwrap();
        let body: Value =
            serde_json::from_slice(&prepared.summary.unwrap().body().unwrap()).unwrap();
        let input = body["input"].as_array().unwrap();
        assert_eq!(input[0]["role"], "user");
        assert!(
            input[0]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("remember the work")
        );

        let native = prepare(&second, Mode::Native, None).unwrap();
        let rewritten: Value = serde_json::from_slice(&native.rewritten.unwrap()).unwrap();
        assert_eq!(rewritten["model"], "test");
        assert_eq!(rewritten["input"][0]["type"], "message");
        let resumed = rewritten["input"][0]["content"][0]["text"]
            .as_str()
            .unwrap();
        assert!(resumed.starts_with("Another language model started to solve this problem"));
        assert_eq!(rewritten["input"][2]["type"], "compaction_trigger");
    }

    #[test]
    fn text_mode_rejects_unrecognized_opaque_checkpoint() {
        let opaque = request(r#"{"type":"compaction","encrypted_content":"ciphertext"}"#);
        assert!(prepare(&opaque, Mode::Text, None).is_err());
    }

    #[test]
    fn summary_drops_additional_tool_declarations_and_rejects_opaque_reasoning() {
        let declaration = r#"{"type":"additional_tools","tools":[{"name":"x"}]}"#;
        let native_bytes = request(&format!("{declaration},{TRIGGER}"));
        assert_eq!(
            prepare(&native_bytes, Mode::Native, None)
                .unwrap()
                .rewritten,
            None
        );

        let bytes = request(&format!(
            r#"{{"type":"additional_tools","tools":[{{"name":"x"}}]}},{{"type":"message","role":"user","content":"do work"}},{TRIGGER}"#
        ));
        let prepared = prepare(&bytes, Mode::Text, None).unwrap();
        let body: Value =
            serde_json::from_slice(&prepared.summary.unwrap().body().unwrap()).unwrap();
        assert_eq!(body["input"].as_array().unwrap().len(), 2);
        assert_eq!(body["input"][0]["role"], "user");
        assert_eq!(body["input"][1]["content"][0]["text"], NATIVE_PROMPT);

        let reasoning = request(&format!(
            r#"{{"type":"reasoning","encrypted_content":"opaque"}},{TRIGGER}"#
        ));
        assert!(prepare(&reasoning, Mode::Text, None).is_err());

        let readable = request(&format!(
            r#"{{"type":"reasoning","encrypted_content":"provider-owned","summary":[{{"type":"summary_text","text":"The result is 45"}}]}},{TRIGGER}"#
        ));
        let summary = prepare(&readable, Mode::Text, None)
            .unwrap()
            .summary
            .unwrap();
        let body: Value = serde_json::from_slice(&summary.body().unwrap()).unwrap();
        assert_eq!(body["input"][0]["encrypted_content"], "provider-owned");
        assert_eq!(body["input"][0]["summary"][0]["text"], "The result is 45");
    }

    #[test]
    fn escaped_compaction_fields_and_explicit_overflow_diagnostics_are_recognized() {
        let bytes = br#"{"model":"test","input":[{"type":"comp\u0061ction_trigger"}]}"#;
        assert!(prepare(bytes, Mode::Text, None).unwrap().summary.is_some());
        let replay = br#"{"previous_response_\u0069d":"resp_model_router_1","input":[]}"#;
        assert!(needs_full_history(replay, Mode::Native).unwrap());
        for message in [
            "The input (100 tokens) is longer than the model's context length (50 tokens).",
            "Requested token count exceeds the model's maximum context length of 50 tokens.",
        ] {
            let error = serde_json::json!({"error":{"code":400,"message":message}}).to_string();
            assert!(is_context_overflow(400, error.as_bytes()));
            assert!(!is_context_overflow(500, error.as_bytes()));
        }
        assert!(!is_context_overflow(
            400,
            br#"{"error":{"message":"Invalid input"}}"#
        ));
    }

    #[test]
    fn text_compaction_rejects_previous_response_id_without_synthetic_checkpoint() {
        let bytes = serde_json::json!({
            "model":"test", "previous_response_id":"upstream-1",
            "input":[{"type":"message","role":"user","content":"partial"},{"type":"compaction_trigger"}]
        }).to_string();
        assert!(prepare(bytes.as_bytes(), Mode::Text, None).is_err());
    }

    #[test]
    fn synthetic_response_ids_and_text_compaction_require_replay() {
        let id = serde_json::json!({"previous_response_id":"resp_model_router_12","input":[]})
            .to_string();
        assert!(needs_full_history(id.as_bytes(), Mode::Native).unwrap());
        let trigger = serde_json::json!({"previous_response_id":"upstream-id","input":[{"type":"compaction_trigger"}]}).to_string();
        assert!(needs_full_history(trigger.as_bytes(), Mode::Text).unwrap());
        assert!(!needs_full_history(trigger.as_bytes(), Mode::Native).unwrap());
    }

    #[test]
    fn only_completed_nonempty_nonrefused_text_summaries_are_accepted() {
        let request = prepare(&request(TRIGGER), Mode::Text, None)
            .unwrap()
            .summary
            .unwrap();
        for bytes in [
            br#"{"status":"incomplete","output":[]}"#.as_slice(),
            br#"{"status":"completed","output":[]}"#.as_slice(),
            br#"{"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"refusal","refusal":"no"}]}]}"#.as_slice(),
            br#"{"status":"completed","output":[{"type":"function_call","call_id":"c"}]}"#.as_slice(),
        ] {
            assert!(request.finish(bytes).is_err());
        }
        assert!(request.finish(&completed("summary")).is_ok());
    }

    #[test]
    fn trim_removes_oldest_call_and_matching_result_but_keeps_latest_user_segment() {
        let bytes = br#"{"model":"test","input":[{"type":"function_call","call_id":"c1"},{"type":"function_call_output","call_id":"c1","output":"done"},{"type":"message","role":"user","content":"older"},{"type":"message","role":"user","content":"latest"},{"type":"compaction_trigger"}]}"#;
        let mut summary = prepare(bytes, Mode::Text, None).unwrap().summary.unwrap();
        assert!(summary.trim_oldest());
        let body: Value = serde_json::from_slice(&summary.body().unwrap()).unwrap();
        let input = body["input"].as_array().unwrap();
        assert_eq!(input[0]["content"], "older");
        assert_eq!(input[1]["content"], "latest");
        assert!(summary.trim_oldest());
        assert!(!summary.trim_oldest());
    }
}
