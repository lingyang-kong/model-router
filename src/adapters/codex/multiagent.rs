use serde_json::Value;

struct NamespacePair {
    client: &'static str,
    upstream: &'static str,
}

const COLLABORATION: NamespacePair = NamespacePair {
    client: "collaboration",
    upstream: "collaboration_compat",
};

const AGENT_MESSAGE: &str = "agent_message";
const ENCRYPTED_CONTENT: &str = "encrypted_content";
const INPUT_TEXT: &str = "input_text";
const SPAWN_AGENT: &str = "spawn_agent";

/// Cheap conservative check used to avoid parsing unrelated request bodies.
pub(crate) fn request_may_need_adaptation(bytes: &[u8]) -> bool {
    memchr::memchr(b'\\', bytes).is_some()
        || contains(bytes, COLLABORATION.client.as_bytes())
        || contains(bytes, COLLABORATION.upstream.as_bytes())
        || contains(bytes, AGENT_MESSAGE.as_bytes())
}

/// Cheap conservative check used to avoid parsing unrelated response events.
pub(crate) fn response_may_need_adaptation(bytes: &[u8]) -> bool {
    memchr::memchr(b'\\', bytes).is_some() || contains(bytes, COLLABORATION.upstream.as_bytes())
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    memchr::memmem::find(haystack, needle).is_some()
}

/// Applies the observed Codex multi-agent-v2 request compatibility transforms.
pub(crate) fn prepare_request(
    body: &mut Value,
    text_agent_messages: bool,
    model_listing: &str,
) -> bool {
    let mut changed = false;
    if let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) {
        changed |= prepare_tools(tools, model_listing);
    }
    if let Some(input) = body.get_mut("input").and_then(Value::as_array_mut) {
        for item in input {
            if item.get("type").and_then(Value::as_str) == Some("additional_tools")
                && let Some(tools) = item.get_mut("tools").and_then(Value::as_array_mut)
            {
                changed |= prepare_tools(tools, model_listing);
            }
            changed |= rewrite_call_identity(item, COLLABORATION.client, COLLABORATION.upstream);
            if item.get("type").and_then(Value::as_str) == Some(AGENT_MESSAGE) {
                changed |= rewrite_agent_message(item, text_agent_messages);
            }
        }
    }
    changed
}

pub(super) fn has_agent_message(bytes: &[u8]) -> serde_json::Result<bool> {
    if !contains(bytes, AGENT_MESSAGE.as_bytes()) && !bytes.contains(&b'\\') {
        return Ok(false);
    }
    let body: Value = serde_json::from_slice(bytes)?;
    Ok(body
        .get("input")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items
                .iter()
                .any(|item| item.get("type").and_then(Value::as_str) == Some(AGENT_MESSAGE))
        }))
}

pub(super) fn prepare_bytes(
    bytes: &[u8],
    text_agent_messages: bool,
    model_listing: &str,
) -> serde_json::Result<Option<Vec<u8>>> {
    if !request_may_need_adaptation(bytes) {
        return Ok(None);
    }
    let mut body: Value = serde_json::from_slice(bytes)?;
    if prepare_request(&mut body, text_agent_messages, model_listing) {
        Ok(Some(serde_json::to_vec(&body)?))
    } else {
        Ok(None)
    }
}

fn prepare_tools(tools: &mut [Value], listing: &str) -> bool {
    let mut changed = false;
    for tool in tools {
        if tool.get("type").and_then(Value::as_str) == Some("namespace") {
            let namespace = tool.get("name").and_then(Value::as_str);
            let collaboration = namespace == Some(COLLABORATION.client)
                || namespace == Some(COLLABORATION.upstream);
            if namespace == Some(COLLABORATION.client) {
                tool["name"] = Value::String(COLLABORATION.upstream.into());
                changed = true;
            }
            if collaboration
                && let Some(nested) = tool.get_mut("tools").and_then(Value::as_array_mut)
            {
                changed |= prepare_collaboration_tools(nested, listing);
            }
            continue;
        }
    }
    changed
}

fn prepare_collaboration_tools(tools: &mut [Value], listing: &str) -> bool {
    let mut changed = false;
    for tool in tools {
        if tool.get("type").and_then(Value::as_str) == Some("namespace") {
            // A nested namespace starts a different tool scope.
            continue;
        }
        let name = tool.get("name").and_then(Value::as_str);
        let spawn = name == Some(SPAWN_AGENT);
        if matches!(name, Some(SPAWN_AGENT | "send_message" | "followup_task"))
            && let Some(message) = tool.pointer_mut("/parameters/properties/message")
        {
            // Schema's message.encrypted annotation lives on the message schema itself.
            if let Some(message) = message.as_object_mut() {
                changed |= message.remove("encrypted").is_some();
            }
        }
        if spawn
            && !listing.is_empty()
            && let Some(description) = tool.get("description").and_then(Value::as_str)
        {
            // Codex puts its model list before this stable tool-description marker.
            let description = description
                .find("Spawns an agent")
                .map_or(description, |start| &description[start..]);
            let combined = format!("{listing}{description}");
            if combined != description {
                tool["description"] = Value::String(combined);
                changed = true;
            }
        }
    }
    changed
}

fn rewrite_agent_message(item: &mut Value, text_messages: bool) -> bool {
    let mut changed = false;
    if let Some(content) = item.get_mut("content").and_then(Value::as_array_mut) {
        for part in content {
            if part.get("type").and_then(Value::as_str) == Some(ENCRYPTED_CONTENT)
                && let Some(text) = part.get(ENCRYPTED_CONTENT).cloned()
            {
                part["type"] = Value::String(INPUT_TEXT.into());
                part["text"] = text;
                part.as_object_mut().unwrap().remove(ENCRYPTED_CONTENT);
                changed = true;
            }
        }
    }
    if text_messages {
        if item.get("type").and_then(Value::as_str) != Some("message") {
            item["type"] = Value::String("message".into());
            changed = true;
        }
        if item.get("role").and_then(Value::as_str) != Some("user") {
            item["role"] = Value::String("user".into());
            changed = true;
        }
        if let Some(object) = item.as_object_mut() {
            for key in [
                "author",
                "recipient",
                "internal_chat_message_metadata_passthrough",
            ] {
                changed |= object.remove(key).is_some();
            }
        }
    }
    changed
}

fn rewrite_call_identity(call: &mut Value, from: &str, to: &str) -> bool {
    if !matches!(
        call.get("type").and_then(Value::as_str),
        Some("function_call" | "custom_tool_call")
    ) {
        return false;
    }
    let mut changed = false;
    if call.get("namespace").and_then(Value::as_str) == Some(from) {
        call["namespace"] = Value::String(to.into());
        changed = true;
    }
    if let Some(name) = call.get("name").and_then(Value::as_str) {
        if let Some(suffix) = name.strip_prefix(&format!("{from}.")) {
            call["name"] = Value::String(suffix.into());
            call["namespace"] = Value::String(to.into());
            changed = true;
        } else if let Some(suffix) = name.strip_prefix(&format!("{from}__")) {
            call["name"] = Value::String(format!("{to}__{suffix}"));
            changed = true;
        }
    }
    changed
}

/// Restores identities only on Responses event item/output envelopes.
pub(crate) fn restore_response(value: &mut Value) -> bool {
    let mut changed = false;
    let event_type = value.get("type").and_then(Value::as_str);
    match event_type {
        None if value.get("object").and_then(Value::as_str) == Some("response") => {
            changed |= restore_output(value.get_mut("output"));
        }
        Some(
            "response.completed"
            | "response.in_progress"
            | "response.created"
            | "response.failed"
            | "response.incomplete",
        ) => {
            if let Some(response) = value.get_mut("response") {
                changed |= restore_output(response.get_mut("output"));
            }
        }
        Some("response.output_item.done" | "response.output_item.added") => {
            if let Some(item) = value.get_mut("item") {
                changed |=
                    rewrite_call_identity(item, COLLABORATION.upstream, COLLABORATION.client);
            }
        }
        _ => {}
    }
    changed
}

pub(super) fn restore_bytes(bytes: &[u8]) -> serde_json::Result<Option<Vec<u8>>> {
    if !response_may_need_adaptation(bytes) {
        return Ok(None);
    }
    let mut event: Value = serde_json::from_slice(bytes)?;
    if restore_response(&mut event) {
        Ok(Some(serde_json::to_vec(&event)?))
    } else {
        Ok(None)
    }
}

fn restore_output(output: Option<&mut Value>) -> bool {
    let Some(items) = output.and_then(Value::as_array_mut) else {
        return false;
    };
    let mut changed = false;
    for item in items {
        changed |= rewrite_call_identity(item, COLLABORATION.upstream, COLLABORATION.client);
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn prepares_only_targeted_tools_and_agent_message_text() {
        let mut body = json!({
            "tools":[{"type":"namespace","name":"collaboration","tools":[
                {"name":"spawn_agent","description":"Spawns an agent.","parameters":{"properties":{"message":{"type":"string","encrypted":true}}}},
                {"name":"send_message","parameters":{"properties":{"message":{"encrypted":true,"x":1}}}},
                {"name":"other","parameters":{"properties":{"message":{"encrypted":true}}}}
            ]},{"type":"namespace","name":"other_ns","tools":[{"name":"send_message","parameters":{"properties":{"message":{"encrypted":true}}}}]},{"type":"function","name":"send_message","description":"collaboration","parameters":{"properties":{"message":{"encrypted":true}}}}],
            "input":[{"type":"agent_message","author":"worker","recipient":"parent","internal_chat_message_metadata_passthrough":{},"content":[{"type":"encrypted_content","encrypted_content":"literal gAAAAA"}]}]
        });
        assert!(prepare_request(&mut body, true, "Model list\n"));
        assert_eq!(body["tools"][0]["name"], "collaboration_compat");
        let tools = body["tools"][0]["tools"].as_array().unwrap();
        assert!(
            tools[0]["parameters"]["properties"]["message"]
                .get("encrypted")
                .is_none()
        );
        assert!(
            tools[1]["parameters"]["properties"]["message"]
                .get("encrypted")
                .is_none()
        );
        assert_eq!(
            tools[2]["parameters"]["properties"]["message"]["encrypted"],
            true
        );
        assert_eq!(
            body["tools"][1]["tools"][0]["parameters"]["properties"]["message"]["encrypted"],
            true
        );
        assert_eq!(
            body["tools"][2]["parameters"]["properties"]["message"]["encrypted"],
            true
        );
        assert!(
            tools[0]["description"]
                .as_str()
                .unwrap()
                .starts_with("Model list\n")
        );
        assert_eq!(body["input"][0]["type"], "message");
        assert_eq!(body["input"][0]["role"], "user");
        assert_eq!(
            body["input"][0]["content"][0],
            json!({"type":"input_text","text":"literal gAAAAA"})
        );
        assert!(body["input"][0].get("author").is_none());
    }

    #[test]
    fn rewrites_replay_and_restores_response_call_identities_without_touching_payloads() {
        let mut request = json!({"input":[
            {"type":"function_call","namespace":"collaboration","name":"spawn_agent","arguments":"collaboration"},
            {"type":"function_call","name":"collaboration__send_message","arguments":"collaboration"},
            {"type":"message","content":[{"type":"input_text","text":"collaboration"}]}
        ]});
        assert!(prepare_request(&mut request, false, ""));
        assert_eq!(request["input"][0]["namespace"], "collaboration_compat");
        assert_eq!(
            request["input"][1]["name"],
            "collaboration_compat__send_message"
        );
        assert_eq!(request["input"][0]["arguments"], "collaboration");
        assert_eq!(request["input"][2]["content"][0]["text"], "collaboration");

        let mut event = json!({"type":"response.output_item.done","item":{"type":"function_call","name":"collaboration_compat.followup_task","arguments":"collaboration_compat"},"unrelated":{"name":"collaboration_compat.x"}});
        assert!(restore_response(&mut event));
        assert_eq!(event["item"]["namespace"], "collaboration");
        assert_eq!(event["item"]["name"], "followup_task");
        assert_eq!(event["item"]["arguments"], "collaboration_compat");
        assert_eq!(event["unrelated"]["name"], "collaboration_compat.x");
    }

    #[test]
    fn restores_completed_snapshot_and_ignores_unrelated_event() {
        let mut snapshot = json!({"type":"response.completed","response":{"output":[{"type":"custom_tool_call","namespace":"collaboration_compat","name":"send_message"}]}});
        assert!(restore_response(&mut snapshot));
        assert_eq!(
            snapshot["response"]["output"][0]["namespace"],
            "collaboration"
        );
        let mut unrelated =
            json!({"type":"response.output_text.delta","delta":"collaboration_compat.foo"});
        assert!(!restore_response(&mut unrelated));

        let mut response = json!({"object":"response","output":[{"type":"function_call","name":"collaboration_compat.spawn_agent"}]});
        assert!(restore_response(&mut response));
        assert_eq!(response["output"][0]["name"], "spawn_agent");

        for event_type in ["response.failed", "response.incomplete"] {
            let mut snapshot = json!({"type":event_type,"response":{"output":[{"type":"function_call","name":"collaboration_compat.spawn_agent"}]}});
            assert!(restore_response(&mut snapshot));
            assert_eq!(snapshot["response"]["output"][0]["name"], "spawn_agent");
        }
    }

    #[test]
    fn adaptation_prefilters_are_conservative() {
        for body in [
            br#"{"input":[{"type":"agent_message"}]}"#.as_slice(),
            br#"{"tools":[{"type":"namespace","name":"collaboration"}]}"#,
            br#"{"input":[{"type":"function_call","namespace":"collaboration_compat"}]}"#,
            br#"{"input":[{"type":"function_call","name":"collaboration__spawn_agent"}]}"#,
            br#"{"input":"\\u0063ollaboration"}"#,
        ] {
            assert!(request_may_need_adaptation(body));
        }
        assert!(response_may_need_adaptation(
            br#"{"type":"response.output_item.done","item":{"name":"collaboration_compat.spawn_agent"}}"#
        ));
        assert!(response_may_need_adaptation(
            br#"{"text":"\\u0063ollaboration_compat"}"#
        ));
        assert!(!request_may_need_adaptation(
            br#"{"input":[{"type":"message","content":[{"type":"input_text","text":"hello"}]}]}"#
        ));
        assert!(!request_may_need_adaptation(
            br#"{"type":"response.output_text.delta","delta":"ordinary text"}"#
        ));
        assert!(!response_may_need_adaptation(
            br#"{"type":"response.output_text.delta","delta":"ordinary text"}"#
        ));
        // A plain occurrence intentionally triggers parsing as a conservative false positive.
        assert!(request_may_need_adaptation(
            br#"{"text":"agent_message appears in prose"}"#
        ));
    }
}
