use std::collections::BTreeMap;

use serde_json::value::RawValue;

use super::{collaboration_tool, event};
use crate::body::has_json_unicode_escape;
use crate::protocol::{field, item_type, role, value};

struct NamespacePair {
    client: &'static str,
    upstream: &'static str,
}
const COLLABORATION: NamespacePair = NamespacePair {
    client: "collaboration",
    upstream: "collaboration_compat",
};

pub(crate) fn request_may_need_adaptation(bytes: &[u8]) -> bool {
    has_json_unicode_escape(bytes)
        || contains(bytes, COLLABORATION.client.as_bytes())
        || contains(bytes, COLLABORATION.upstream.as_bytes())
        || contains(bytes, item_type::AGENT_MESSAGE.as_bytes())
}
pub(crate) fn response_may_need_adaptation(bytes: &[u8]) -> bool {
    has_json_unicode_escape(bytes) || contains(bytes, COLLABORATION.upstream.as_bytes())
}
fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    memchr::memmem::find(haystack, needle).is_some()
}

// Open only the protocol envelopes we edit. Raw values keep arbitrary schema
// and payload depth out of recursive deserialization, serialization, and drop.
// Borrow raw values for inspection; own them in editing paths.
fn parse_object(raw: &RawValue) -> serde_json::Result<Option<BTreeMap<String, Box<RawValue>>>> {
    parse_object_bytes(raw.get().as_bytes())
}

fn parse_object_bytes<'a, R: serde::Deserialize<'a>>(
    bytes: &'a [u8],
) -> serde_json::Result<Option<BTreeMap<String, R>>> {
    if bytes.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{') {
        return Ok(None);
    }
    serde_json::from_slice(bytes).map(Some)
}
fn parse_array<'a, R: serde::Deserialize<'a>>(
    raw: &'a RawValue,
) -> serde_json::Result<Option<Vec<R>>> {
    if !raw.get().starts_with('[') {
        return Ok(None);
    }
    serde_json::from_str(raw.get()).map(Some)
}
fn string(raw: Option<impl AsRef<RawValue>>) -> Option<String> {
    raw.and_then(|value| serde_json::from_str(value.as_ref().get()).ok())
}
fn encode_string(value: &str) -> Box<RawValue> {
    serde_json::value::to_raw_value(value).expect("string serialization cannot fail")
}
fn set_str(object: &mut BTreeMap<String, Box<RawValue>>, key: &str, value: &str) {
    object.insert(key.into(), encode_string(value));
}
fn encode_json<T: serde::Serialize + ?Sized>(value: &T) -> serde_json::Result<Box<RawValue>> {
    serde_json::value::to_raw_value(value)
}

pub(super) fn has_convertible_agent_message(bytes: &[u8]) -> serde_json::Result<bool> {
    if !contains(bytes, item_type::AGENT_MESSAGE.as_bytes()) && !has_json_unicode_escape(bytes) {
        return Ok(false);
    }
    let Some(object) = parse_object_bytes::<&RawValue>(bytes)? else {
        return Ok(false);
    };
    let Some(input) = object.get(field::INPUT) else {
        return Ok(false);
    };
    let Some(items) = parse_array::<&RawValue>(input)? else {
        return Ok(false);
    };
    for item in items {
        if let Some(item) = parse_object_bytes::<&RawValue>(item.get().as_bytes())?
            && item
                .get(field::TYPE)
                .and_then(|value| serde_json::from_str::<String>(value.get()).ok())
                .as_deref()
                == Some(item_type::AGENT_MESSAGE)
            && !has_encrypted_content(item.get(field::CONTENT).copied())?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn prepare_bytes(
    bytes: &[u8],
    text_agent_messages: bool,
    listing: &str,
) -> serde_json::Result<Option<Vec<u8>>> {
    let Some(mut object) = parse_object_bytes::<Box<RawValue>>(bytes)? else {
        return Ok(None);
    };
    let mut changed = false;
    if let Some(tools) = object.get(field::TOOLS)
        && let Some(items) = parse_array(tools)?
    {
        let (items, tools_changed) = prepare_tools(items, listing)?;
        if tools_changed {
            object.insert(field::TOOLS.into(), encode_json(&items)?);
            changed = true;
        }
    }
    if let Some(input) = object.get(field::INPUT)
        && let Some(items) = parse_array(input)?
    {
        let (items, input_changed) = prepare_input(items, text_agent_messages, listing)?;
        if input_changed {
            object.insert(field::INPUT.into(), encode_json(&items)?);
            changed = true;
        }
    }
    if !changed {
        return Ok(None);
    }
    Ok(Some(serde_json::to_vec(&object)?))
}
fn prepare_tools(
    mut tools: Vec<Box<RawValue>>,
    listing: &str,
) -> serde_json::Result<(Vec<Box<RawValue>>, bool)> {
    let mut changed = false;
    for entry in &mut tools {
        let Some(mut tool) = parse_object(entry)? else {
            continue;
        };
        if string(tool.get(field::TYPE)).as_deref() != Some(item_type::NAMESPACE) {
            continue;
        }
        let name = string(tool.get(field::NAME));
        let collab = name.as_deref() == Some(COLLABORATION.client)
            || name.as_deref() == Some(COLLABORATION.upstream);
        let mut tool_changed = false;
        if name.as_deref() == Some(COLLABORATION.client) {
            set_str(&mut tool, field::NAME, COLLABORATION.upstream);
            changed = true;
            tool_changed = true;
        }
        if collab
            && let Some(nested_tools) = tool.get(field::TOOLS)
            && let Some(nested_items) = parse_array(nested_tools)?
        {
            let (nested_items, nested_changed) =
                prepare_collaboration_tools(nested_items, listing)?;
            if nested_changed {
                tool.insert(field::TOOLS.into(), encode_json(&nested_items)?);
                changed = true;
                tool_changed = true;
            }
        }
        if tool_changed {
            *entry = encode_json(&tool)?;
        }
    }
    Ok((tools, changed))
}
fn prepare_collaboration_tools(
    mut tools: Vec<Box<RawValue>>,
    listing: &str,
) -> serde_json::Result<(Vec<Box<RawValue>>, bool)> {
    let mut changed = false;
    for entry in &mut tools {
        let Some(mut tool) = parse_object(entry)? else {
            continue;
        };
        if string(tool.get(field::TYPE)).as_deref() == Some(item_type::NAMESPACE) {
            continue;
        }
        let name = string(tool.get(field::NAME));
        let mut tool_changed = false;
        if matches!(
            name.as_deref(),
            Some(
                collaboration_tool::SPAWN_AGENT
                    | collaboration_tool::SEND_MESSAGE
                    | collaboration_tool::FOLLOWUP_TASK
            )
        ) && let Some(parameters) = tool.get(field::PARAMETERS)
            && let Some(mut parameters) = parse_object(parameters)?
        {
            let mut parameters_changed = false;
            if let Some(properties) = parameters.get(field::PROPERTIES)
                && let Some(mut properties) = parse_object(properties)?
                && let Some(message_raw) = properties.get(field::MESSAGE)
                && let Some(mut message) = parse_object(message_raw)?
                && message.remove(field::ENCRYPTED).is_some()
            {
                properties.insert(field::MESSAGE.into(), encode_json(&message)?);
                parameters.insert(field::PROPERTIES.into(), encode_json(&properties)?);
                parameters_changed = true;
            }
            if parameters_changed {
                tool.insert(field::PARAMETERS.into(), encode_json(&parameters)?);
                changed = true;
                tool_changed = true;
            }
        }
        if name.as_deref() == Some(collaboration_tool::SPAWN_AGENT)
            && !listing.is_empty()
            && let Some(description) = string(tool.get(field::DESCRIPTION))
        {
            // Replace Codex's model-list prefix when the expected description marker is present.
            let description = description
                .find("Spawns an agent")
                .map_or(description.as_str(), |start| &description[start..]);
            let combined = format!("{listing}{description}");
            if combined != description {
                set_str(&mut tool, field::DESCRIPTION, &combined);
                changed = true;
                tool_changed = true;
            }
        }
        if tool_changed {
            *entry = encode_json(&tool)?;
        }
    }
    Ok((tools, changed))
}
fn prepare_input(
    mut items: Vec<Box<RawValue>>,
    text_agent_messages: bool,
    listing: &str,
) -> serde_json::Result<(Vec<Box<RawValue>>, bool)> {
    let mut changed = false;
    for entry in &mut items {
        if !request_may_need_adaptation(entry.get().as_bytes()) {
            continue;
        }
        let Some(mut item) = parse_object(entry)? else {
            continue;
        };
        let kind = string(item.get(field::TYPE));
        let mut item_changed = false;
        if kind.as_deref() == Some(item_type::ADDITIONAL_TOOLS)
            && let Some(tools) = item.get(field::TOOLS)
            && let Some(tools) = parse_array(tools)?
        {
            let (tools, tools_changed) = prepare_tools(tools, listing)?;
            if tools_changed {
                item.insert(field::TOOLS.into(), encode_json(&tools)?);
                changed = true;
                item_changed = true;
            }
        }
        if matches!(
            kind.as_deref(),
            Some(item_type::FUNCTION_CALL | item_type::CUSTOM_TOOL_CALL)
        ) {
            item_changed |=
                rewrite_identity(&mut item, COLLABORATION.client, COLLABORATION.upstream);
        }
        if kind.as_deref() == Some(item_type::AGENT_MESSAGE) {
            item_changed |= rewrite_agent(&mut item, text_agent_messages)?;
        }
        if item_changed {
            changed = true;
            *entry = encode_json(&item)?;
        }
    }
    Ok((items, changed))
}
fn rewrite_agent(
    object: &mut BTreeMap<String, Box<RawValue>>,
    text_messages: bool,
) -> serde_json::Result<bool> {
    if has_encrypted_content(object.get(field::CONTENT).map(Box::as_ref))? {
        return Ok(false);
    }
    let mut changed = false;
    if text_messages {
        if string(object.get(field::TYPE)).as_deref() != Some(item_type::MESSAGE) {
            set_str(object, field::TYPE, item_type::MESSAGE);
            changed = true;
        }
        if string(object.get(field::ROLE)).as_deref() != Some(role::USER) {
            set_str(object, field::ROLE, role::USER);
            changed = true;
        }
        for k in [
            field::AUTHOR,
            field::RECIPIENT,
            field::INTERNAL_CHAT_MESSAGE_METADATA_PASSTHROUGH,
        ] {
            changed |= object.remove(k).is_some();
        }
    }
    Ok(changed)
}
fn has_encrypted_content(content: Option<&RawValue>) -> serde_json::Result<bool> {
    if let Some(content) = content
        && let Some(parts) = parse_array::<&RawValue>(content)?
    {
        for part in parts {
            if let Some(part) = parse_object_bytes::<&RawValue>(part.get().as_bytes())?
                && part
                    .get(field::TYPE)
                    .and_then(|value| serde_json::from_str::<String>(value.get()).ok())
                    .as_deref()
                    == Some(item_type::ENCRYPTED_CONTENT)
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
fn rewrite_identity(object: &mut BTreeMap<String, Box<RawValue>>, from: &str, to: &str) -> bool {
    let mut changed = false;
    if string(object.get(field::NAMESPACE)).as_deref() == Some(from) {
        set_str(object, field::NAMESPACE, to);
        changed = true;
    }
    if let Some(name) = string(object.get(field::NAME)) {
        if let Some(suffix) = name.strip_prefix(&format!("{from}.")) {
            set_str(object, field::NAME, suffix);
            set_str(object, field::NAMESPACE, to);
            changed = true;
        } else if let Some(suffix) = name.strip_prefix(&format!("{from}__")) {
            set_str(object, field::NAME, &format!("{to}__{suffix}"));
            changed = true;
        }
    }
    changed
}

pub(super) fn restore_bytes(bytes: &[u8]) -> serde_json::Result<Option<Vec<u8>>> {
    if !response_may_need_adaptation(bytes) {
        return Ok(None);
    }
    let Some(mut object) = parse_object_bytes(bytes)? else {
        return Ok(None);
    };
    let event_type = string(object.get(field::TYPE));
    let mut changed = false;
    match event_type.as_deref() {
        None if string(object.get(field::OBJECT)).as_deref() == Some(value::RESPONSE) => {
            changed |= restore_output(&mut object, field::OUTPUT)?;
        }
        Some(
            event::COMPLETED
            | event::IN_PROGRESS
            | event::CREATED
            | event::FAILED
            | event::INCOMPLETE,
        ) => {
            if let Some(response_raw) = object.get(field::RESPONSE)
                && let Some(mut response) = parse_object(response_raw)?
                && restore_output(&mut response, field::OUTPUT)?
            {
                object.insert(field::RESPONSE.into(), encode_json(&response)?);
                changed = true;
            }
        }
        Some(event::OUTPUT_ITEM_DONE | event::OUTPUT_ITEM_ADDED) => {
            if let Some(item_raw) = object.get(field::ITEM)
                && let Some(mut item) = parse_object(item_raw)?
                && restore_call(&mut item)?
            {
                object.insert(field::ITEM.into(), encode_json(&item)?);
                changed = true;
            }
        }
        _ => {}
    }
    if changed {
        Ok(Some(serde_json::to_vec(&object)?))
    } else {
        Ok(None)
    }
}
fn restore_call(item: &mut BTreeMap<String, Box<RawValue>>) -> serde_json::Result<bool> {
    let kind = string(item.get(field::TYPE));
    if !matches!(
        kind.as_deref(),
        Some(item_type::FUNCTION_CALL | item_type::CUSTOM_TOOL_CALL)
    ) || !rewrite_identity(item, COLLABORATION.upstream, COLLABORATION.client)
    {
        return Ok(false);
    }
    let name = string(item.get(field::NAME));
    let name = name.as_deref().map(|name| {
        name.strip_prefix(COLLABORATION.client)
            .and_then(|suffix| suffix.strip_prefix("__"))
            .unwrap_or(name)
    });
    if kind.as_deref() == Some(item_type::FUNCTION_CALL)
        && matches!(
            name,
            Some(
                collaboration_tool::SPAWN_AGENT
                    | collaboration_tool::SEND_MESSAGE
                    | collaboration_tool::FOLLOWUP_TASK
            )
        )
        && !item.contains_key(field::ENCRYPTED_FUNCTION_ARGS)
    {
        // Codex treats an absent marker as encrypted, even for plaintext arguments.
        item.insert(
            field::ENCRYPTED_FUNCTION_ARGS.into(),
            encode_json(&[] as &[String])?,
        );
    }
    Ok(true)
}
fn restore_output(
    object: &mut BTreeMap<String, Box<RawValue>>,
    key: &str,
) -> serde_json::Result<bool> {
    let Some(output) = object.get(key) else {
        return Ok(false);
    };
    let Some(mut items) = parse_array::<Box<RawValue>>(output)? else {
        return Ok(false);
    };
    let mut changed = false;
    for item_raw in &mut items {
        let Some(mut item) = parse_object(item_raw)? else {
            continue;
        };
        if restore_call(&mut item)? {
            *item_raw = encode_json(&item)?;
            changed = true;
        }
    }
    if changed {
        object.insert(key.into(), encode_json(&items)?);
    }
    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn encrypted_agent_messages_remain_unchanged_even_when_native_items_are_rejected() {
        let input = br#"{"input":[{"type":"agent_message","author":"worker","recipient":"parent","content":[{"type":"input_text","text":"context"},{"type":"encrypted_content","encrypted_content":"opaque-ciphertext"}]}]}"#;
        for convert in [false, true] {
            assert!(prepare_bytes(input, convert, "").unwrap().is_none());
        }
        assert!(!has_convertible_agent_message(input).unwrap());
    }
    #[test]
    fn probe_eligibility_preserves_encrypted_and_mixed_message_semantics() {
        let encrypted = r#"{"type":"agent_message","content":[{"type":"input_text","text":"visible"},{"type":"encrypted\u005fcontent","encrypted_content":"opaque"}]}"#;
        let plain = r#"{"type":"agent_message","content":[{"type":"input_text","text":"plain"}]}"#;
        for (items, expected) in [
            (encrypted.to_owned(), false),
            (format!("{encrypted},{plain}"), true),
            (format!("{plain},{encrypted}"), true),
            (r#"{"type":"agent_message","content":null}"#.into(), true),
            (r#"{"type":"agent_message","content":{}}"#.into(), true),
            (r#"{"type":"agent_message"}"#.into(), true),
        ] {
            let request = format!(r#"{{"input":[{items}]}}"#);
            assert_eq!(
                has_convertible_agent_message(request.as_bytes()).unwrap(),
                expected
            );
        }
    }
    #[test]
    fn plaintext_marker_is_selective_and_preserves_explicit_encryption() {
        use serde_json::json;
        for name in ["spawn_agent", "send_message", "followup_task"] {
            for call in [
                json!({"type":"function_call", "namespace":"collaboration_compat", "name":name}),
                json!({"type":"function_call", "name":format!("collaboration_compat.{name}")}),
                json!({"type":"function_call", "name":format!("collaboration_compat__{name}")}),
            ] {
                for event in ["response.output_item.added", "response.output_item.done"] {
                    let input = json!({"type":event,"item":call});
                    let restored = restore_bytes(input.to_string().as_bytes())
                        .unwrap()
                        .unwrap();
                    let restored: serde_json::Value = serde_json::from_slice(&restored).unwrap();
                    assert_eq!(restored["item"]["encrypted_function_args"], json!([]));
                    assert!(
                        restore_bytes(&serde_json::to_vec(&restored).unwrap())
                            .unwrap()
                            .is_none()
                    );
                }
            }
        }
        for call in [
            json!({"type":"function_call","namespace":"collaboration_compat","name":"wait_agent"}),
            json!({"type":"custom_tool_call","namespace":"collaboration_compat","name":"spawn_agent"}),
            json!({"type":"function_call","namespace":"collaboration","name":"spawn_agent"}),
            json!({"type":"function_call","namespace":"other","name":"spawn_agent"}),
            json!({"type":"function_call","namespace":"collaboration_compat","name":"spawn_agent","encrypted_function_args":["message"]}),
        ] {
            let input = json!({"object":"response","output":[call]});
            let bytes = serde_json::to_vec(&input).unwrap();
            let restored = restore_bytes(&bytes).unwrap().unwrap_or(bytes);
            let restored: serde_json::Value = serde_json::from_slice(&restored).unwrap();
            assert_eq!(
                restored["output"][0].get("encrypted_function_args"),
                call.get("encrypted_function_args")
            );
        }
    }
    #[test]
    fn targeted_adaptation_and_restore() {
        let input=br#"{"tools":[{"type":"namespace","name":"collaboration","tools":[{"name":"spawn_agent","description":"d","parameters":{"properties":{"message":{"encrypted":true,"deep":{"x":1}}}}}]}],"input":[{"type":"agent_message","author":"a","content":[{"type":"input_text","text":"literal"}]},{"type":"function_call","name":"collaboration__send_message","arguments":"collaboration"}]}"#;
        let out = prepare_bytes(input, true, "listing\n").unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["tools"][0]["name"], COLLABORATION.upstream);
        assert_eq!(v["tools"][0]["tools"][0]["description"], "listing\nd");
        assert!(
            v["tools"][0]["tools"][0]["parameters"]["properties"]["message"]
                .get("encrypted")
                .is_none()
        );
        assert_eq!(
            v["input"][0]["content"][0],
            serde_json::json!({"type":"input_text","text":"literal"})
        );
        assert_eq!(v["input"][1]["name"], "collaboration_compat__send_message");
        let response=br#"{"type":"response.output_item.done","item":{"type":"function_call","name":"collaboration_compat.foo"}}"#;
        let restored = restore_bytes(response).unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_slice(&restored).unwrap();
        assert_eq!(v["item"]["name"], "foo");
        assert_eq!(v["item"]["namespace"], "collaboration");
    }
    #[test]
    fn preserves_unrelated_tool_scopes_and_restores_all_snapshot_types() {
        let request = br#"{"tools":[{"type":"namespace","name":"collaboration","tools":[{"name":"other","parameters":{"properties":{"message":{"encrypted":true}}}},{"type":"namespace","name":"nested","tools":[{"name":"send_message","parameters":{"properties":{"message":{"encrypted":true}}}}]}]},{"type":"namespace","name":"other","tools":[{"name":"send_message","parameters":{"properties":{"message":{"encrypted":true}}}}]},{"type":"function","name":"send_message","parameters":{"properties":{"message":{"encrypted":true}}}}]}"#;
        let prepared = prepare_bytes(request, false, "").unwrap().unwrap();
        let value: serde_json::Value = serde_json::from_slice(&prepared).unwrap();
        assert!(
            value["tools"][0]["tools"][0]["parameters"]["properties"]["message"]
                .get("encrypted")
                .is_some()
        );
        assert!(
            value["tools"][0]["tools"][1]["tools"][0]["parameters"]["properties"]["message"]
                .get("encrypted")
                .is_some()
        );
        assert!(
            value["tools"][1]["tools"][0]["parameters"]["properties"]["message"]
                .get("encrypted")
                .is_some()
        );
        assert!(
            value["tools"][2]["parameters"]["properties"]["message"]
                .get("encrypted")
                .is_some()
        );

        for event_type in ["response.failed", "response.incomplete"] {
            let response = format!(
                r#"{{"type":"{event_type}","response":{{"output":[{{"type":"function_call","name":"collaboration_compat.spawn_agent"}}]}}}}"#
            );
            let restored = restore_bytes(response.as_bytes()).unwrap().unwrap();
            let value: serde_json::Value = serde_json::from_slice(&restored).unwrap();
            assert_eq!(value["response"]["output"][0]["name"], "spawn_agent");
        }
    }
    #[test]
    fn opaque_deep_schema_is_ignored() {
        let deep = format!("{}0{}", "[".repeat(1000), "]".repeat(1000));
        let input = r#"{"tools":[{"type":"namespace","name":"collaboration","tools":[{"name":"send_message","parameters":{"properties":{"message":{"encrypted":true},"schema":"#
        .to_owned()
            + &deep
            + r#"}}}]}],"input":[{"type":"function_call","name":"collaboration__x"}]}"#;
        let out = prepare_bytes(input.as_bytes(), false, "").unwrap().unwrap();
        let output = String::from_utf8(out).unwrap();
        assert!(output.contains("collaboration_compat__x"));
        assert!(output.contains("\"name\":\"collaboration_compat\""));
        assert!(output.contains(&deep));

        let response = format!(
            r#"{{"type":"response.output_item.done","item":{{"type":"function_call","name":"collaboration_compat.x"}},"opaque":{deep}}}"#
        );
        let restored = restore_bytes(response.as_bytes()).unwrap().unwrap();
        let restored = String::from_utf8(restored).unwrap();
        assert!(restored.contains("\"name\":\"x\""));
        assert!(restored.contains(&deep));
    }
    #[test]
    fn agent_message_inspection_preserves_object_and_type_semantics() {
        for (input, expected) in [
            (
                r#"{"in\u0070ut":[{"t\u0079pe":"ag\u0065nt_message"}]}"#,
                true,
            ),
            (
                r#"{"input":[0,null,"agent_message",{"type":5},{"type":"agent_message"}]}"#,
                true,
            ),
            (
                r#"{"input":[{"type":"agent_message","type":"message"}]}"#,
                false,
            ),
            (
                r#"{"input":[{"type":"message","type":"agent_message"}]}"#,
                true,
            ),
            (r#"{"input":[{"type":"agent_message"}],"input":[]}"#, false),
            (r#"{"input":[],"input":[{"type":"agent_message"}]}"#, true),
            (r#"{"input":{"type":"agent_message"}}"#, false),
            (
                r#"{"input":[{"type":"message","content":[{"type":"agent_message"}]}]}"#,
                false,
            ),
        ] {
            assert_eq!(
                has_convertible_agent_message(input.as_bytes()).unwrap(),
                expected,
                "{input}"
            );
        }
        assert!(
            has_convertible_agent_message(br#"{"input":[{"type":"agent_message"}],"unfinished":"#)
                .is_err()
        );

        let deep = format!("{}0{}", "[".repeat(1000), "]".repeat(1000));
        let input = format!(
            r#"{{"input":[{{"type":"message","content":{deep}}},{{"type":"agent_message"}}]}}"#
        );
        assert!(has_convertible_agent_message(input.as_bytes()).unwrap());
    }

    #[test]
    fn ordinary_json_escapes_skip_adaptation_but_unicode_names_are_recognized() {
        let ordinary = br#"{"input":[{"type":"message","content":"\n\r\t\b\f\"\\\/"}]}"#;
        serde_json::from_slice::<serde_json::Value>(ordinary).unwrap();
        assert!(!request_may_need_adaptation(ordinary));
        assert!(!response_may_need_adaptation(ordinary));
        assert!(!has_convertible_agent_message(ordinary).unwrap());
        assert!(restore_bytes(ordinary).unwrap().is_none());

        let escaped = br#"{"input":[{"type":"ag\u0065nt_message","content":[] }]}"#;
        assert!(request_may_need_adaptation(escaped));
        assert!(has_convertible_agent_message(escaped).unwrap());
        let prepared = prepare_bytes(escaped, true, "").unwrap().unwrap();
        let prepared: serde_json::Value = serde_json::from_slice(&prepared).unwrap();
        assert_eq!(prepared["input"][0]["type"], "message");
    }

    #[test]
    fn escaped_relevant_name_is_seen() {
        let input=br#"{"input":[{"type":"function_call","name":"collab\u006fration\u005f\u005fx"}],"text":"a\\nb"}"#;
        assert!(request_may_need_adaptation(input));
        let out = prepare_bytes(input, false, "").unwrap().unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["input"][0]["name"], "collaboration_compat__x");
        assert_eq!(v["text"], r"a\nb");
    }
}
