use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Capabilities {
    pub responses_lite: Option<bool>,
    pub tool_search: Option<bool>,
    /// Provider-hosted tool inventory availability, independent of model capabilities.
    pub tool_inventory: Option<bool>,
    pub image_detail_original: Option<bool>,
    pub images: Option<bool>,
}

impl Capabilities {
    /// Apply facts learned later. Missing facts never clear an earlier value.
    pub(crate) fn merge(&mut self, newer: Self) {
        macro_rules! merge_field {
            ($field:ident) => {
                if newer.$field.is_some() {
                    self.$field = newer.$field;
                }
            };
        }
        merge_field!(responses_lite);
        merge_field!(tool_search);
        merge_field!(tool_inventory);
        merge_field!(image_detail_original);
        merge_field!(images);
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Discovery {
    pub capabilities: Capabilities,
    pub models: BTreeMap<String, Capabilities>,
    /// OpenAPI paths that explicitly declare a GET and look like tool inventory endpoints.
    pub tools_paths: Vec<String>,
}

impl Discovery {
    /// Combine independently fetched discovery documents. Explicit newer facts win.
    pub(crate) fn merge(&mut self, newer: Self) {
        self.capabilities.merge(newer.capabilities);
        for (id, capabilities) in newer.models {
            self.models.entry(id).or_default().merge(capabilities);
        }
        for path in newer.tools_paths {
            if !self.tools_paths.contains(&path) {
                self.tools_paths.push(path);
            }
        }
    }

    /// Return global facts with any facts scoped to the requested model applied.
    pub(crate) fn for_model(&self, id: &str) -> Capabilities {
        let mut capabilities = self.capabilities.clone();
        if let Some(model) = self.models.get(id) {
            capabilities.merge(model.clone());
        }
        capabilities
    }
}

/// Extract only explicit provider facts; absence is represented as `None`.
pub(crate) fn parse(document: &Value) -> Discovery {
    let mut found = Discovery::default();
    parse_tool_inventory(document, &mut found.capabilities);
    let Some(root) = document.as_object() else {
        return found;
    };

    if model_id(document).is_none() {
        found.capabilities.merge(read_flags(document));
    }
    parse_model_info(document, &mut found);
    parse_models(document, &mut found);
    found.tools_paths = declared_tool_paths(document);

    // The schema's closed detail enum is direct evidence that `original` is unsupported.
    let mut visited = BTreeSet::new();
    let mut image_constraints = Vec::new();
    if let Some(paths) = root.get("paths").and_then(Value::as_object) {
        for (path, item) in paths {
            if !path.ends_with("/responses") {
                continue;
            }
            let Some(schema) = item.pointer("/post/requestBody/content/application~1json/schema")
            else {
                continue;
            };
            image_original_from_schema(schema, document, &mut visited, &mut image_constraints, 0);
        }
    }
    if found.capabilities.image_detail_original.is_none()
        && let Some(Some(supported)) = image_constraints.first().copied()
        && image_constraints
            .iter()
            .all(|constraint| *constraint == Some(supported))
    {
        found.capabilities.image_detail_original = Some(supported);
    }
    found
}

fn read_flags(value: &Value) -> Capabilities {
    let mut result = Capabilities::default();
    let object = value.as_object();
    let nested = value.get("capabilities").and_then(Value::as_object);
    let get = |name: &str| {
        nested
            .and_then(|m| m.get(name))
            .or_else(|| object.and_then(|m| m.get(name)))
    };
    result.responses_lite = get("responses_lite")
        .and_then(Value::as_bool)
        .or_else(|| get("supports_responses_lite").and_then(Value::as_bool));
    result.responses_lite = result
        .responses_lite
        .or_else(|| get("use_responses_lite").and_then(Value::as_bool));
    result.tool_search = get("tool_search")
        .and_then(Value::as_bool)
        .or_else(|| get("supports_search_tool").and_then(Value::as_bool));
    result.tool_inventory = get("tool_inventory")
        .and_then(Value::as_bool)
        .or_else(|| get("supports_tool_inventory").and_then(Value::as_bool));
    result.image_detail_original = get("image_detail_original")
        .and_then(Value::as_bool)
        .or_else(|| get("supports_image_detail_original").and_then(Value::as_bool));
    result.images = get("images").and_then(Value::as_bool);
    result
}

fn model_id(model: &Value) -> Option<&str> {
    ["served_model_name", "model_path", "id", "slug"]
        .iter()
        .find_map(|key| model.get(key).and_then(Value::as_str))
}

fn parse_model_info(document: &Value, found: &mut Discovery) {
    for info in [
        Some(document),
        document.get("model_info"),
        document.get("data"),
    ]
    .into_iter()
    .flatten()
    {
        let Some(id) = model_id(info) else {
            continue;
        };
        let mut caps = read_flags(info);
        if let Some(flag) = info.get("has_image_understanding").and_then(Value::as_bool) {
            caps.images = Some(flag);
        }
        if !caps.is_empty() {
            found.models.entry(id.to_owned()).or_default().merge(caps);
        }
    }
}

fn parse_models(document: &Value, found: &mut Discovery) {
    for models in [document.get("data"), document.get("models")]
        .into_iter()
        .flatten()
        .filter_map(Value::as_array)
    {
        for model in models {
            let Some(id) = model_id(model) else {
                continue;
            };
            let mut caps = read_flags(model);
            if let Some(flag) = model
                .get("has_image_understanding")
                .and_then(Value::as_bool)
            {
                caps.images = Some(flag);
            }
            if !caps.is_empty() {
                found.models.entry(id.to_owned()).or_default().merge(caps);
            }
        }
    }
}

fn parse_tool_inventory(document: &Value, caps: &mut Capabilities) {
    let inventory = document
        .as_array()
        .or_else(|| document.get("tools").and_then(Value::as_array));
    let Some(items) = inventory else { return };
    caps.tool_inventory = Some(true);
    if items
        .iter()
        .any(|item| item.get("type").and_then(Value::as_str) == Some("tool_search"))
    {
        caps.tool_search = Some(true);
    }
}

fn declared_tool_paths(document: &Value) -> Vec<String> {
    document
        .get("paths")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .filter_map(|(path, methods)| {
            let normalized = path.trim_end_matches('/');
            let looks_like_tools =
                normalized.ends_with("/tools") || normalized.ends_with("/tools/list");
            (looks_like_tools
                && path.starts_with('/')
                && !path.contains('{')
                && methods.get("get").is_some_and(Value::is_object))
            .then(|| path.clone())
        })
        .collect()
}

fn image_original_from_schema(
    schema: &Value,
    document: &Value,
    visited: &mut BTreeSet<String>,
    constraints: &mut Vec<Option<bool>>,
    depth: usize,
) {
    if depth > 32 {
        return;
    }
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        if !visited.insert(reference.to_owned()) {
            return;
        }
        let Some(target) = reference.strip_prefix("#/components/schemas/") else {
            return;
        };
        let Some(schema) = document.pointer(&format!("/components/schemas/{target}")) else {
            return;
        };
        image_original_from_schema(schema, document, visited, constraints, depth + 1);
        return;
    }
    let props = schema.get("properties").and_then(Value::as_object);
    let is_input_image = props
        .and_then(|p| p.get("type"))
        .and_then(|t| t.get("const"))
        .and_then(Value::as_str)
        == Some("input_image");
    if is_input_image {
        let fact = props
            .and_then(|p| p.get("detail"))
            .and_then(|detail| find_closed_enum(detail, document, visited, depth + 1))
            .map(|values| values.iter().any(|v| v.as_str() == Some("original")));
        constraints.push(fact);
    }
    for key in ["anyOf", "oneOf", "allOf"] {
        if let Some(items) = schema.get(key).and_then(Value::as_array) {
            for item in items {
                image_original_from_schema(item, document, visited, constraints, depth + 1);
            }
        }
    }
    if let Some(item_schema) = schema.get("items") {
        image_original_from_schema(item_schema, document, visited, constraints, depth + 1);
    }
    if let Some(props) = props {
        for child in props.values() {
            image_original_from_schema(child, document, visited, constraints, depth + 1);
        }
    }
}

fn find_closed_enum(
    schema: &Value,
    document: &Value,
    visited: &mut BTreeSet<String>,
    depth: usize,
) -> Option<Vec<Value>> {
    if depth > 32 {
        return None;
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        return Some(values.clone());
    }
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        if !visited.insert(reference.to_owned()) {
            return None;
        }
        let target = reference.strip_prefix("#/components/schemas/")?;
        return find_closed_enum(
            document.pointer(&format!("/components/schemas/{target}"))?,
            document,
            visited,
            depth + 1,
        );
    }
    for key in ["anyOf", "oneOf"] {
        if let Some(items) = schema.get(key).and_then(Value::as_array) {
            let mut values = Vec::new();
            for item in items {
                if item.get("type").and_then(Value::as_str) == Some("null") {
                    continue;
                }
                values.extend(find_closed_enum(item, document, visited, depth + 1)?);
            }
            return (!values.is_empty()).then_some(values);
        }
    }
    None
}

impl Capabilities {
    fn is_empty(&self) -> bool {
        self.responses_lite.is_none()
            && self.tool_search.is_none()
            && self.tool_inventory.is_none()
            && self.image_detail_original.is_none()
            && self.images.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn missing_facts_do_not_erase_inherited_values() {
        let mut caps = Capabilities {
            tool_search: Some(true),
            images: Some(true),
            ..Default::default()
        };
        caps.merge(Capabilities {
            tool_search: Some(false),
            ..Default::default()
        });
        caps.merge(Capabilities::default());
        assert_eq!(caps.tool_search, Some(false));
        assert_eq!(caps.images, Some(true));
    }

    #[test]
    fn openapi_enum_is_not_native_tool_support() {
        let parsed = parse(
            &json!({"components":{"schemas":{"Tool":{"properties":{"type":{"enum":["tool_search"]}}}}}}),
        );
        assert_eq!(parsed.capabilities.tool_search, None);
    }

    #[test]
    fn inventories_require_native_tool_type_and_absence_is_unknown() {
        assert_eq!(parse(&json!({"tools":[]})).capabilities.tool_search, None);
        assert_eq!(
            parse(&json!({"tools":[{"type":"function","name":"tool_search"}]}))
                .capabilities
                .tool_search,
            None
        );
        assert_eq!(
            parse(&json!({"tools":[{"type":"tool_search"}]}))
                .capabilities
                .tool_search,
            Some(true)
        );
    }

    #[test]
    fn explicit_flags_and_model_scope_are_preserved() {
        let d = parse(
            &json!({"capabilities":{"responses_lite":false,"images":true},"model_info":{"served_model_name":"m","has_image_understanding":false}}),
        );
        assert_eq!(d.capabilities.responses_lite, Some(false));
        assert_eq!(d.capabilities.images, Some(true));
        assert_eq!(d.models["m"].images, Some(false));
    }

    #[test]
    fn root_model_info_and_top_level_flags_are_read() {
        let d = parse(&json!({
            "served_model_name":"sglang-model",
            "has_image_understanding":true,
            "supports_responses_lite":false,
            "supports_tool_inventory":true
        }));
        assert_eq!(d.capabilities.images, None);
        assert_eq!(d.capabilities.responses_lite, None);
        assert_eq!(d.capabilities.tool_inventory, None);
        assert_eq!(d.models["sglang-model"].images, Some(true));
        assert_eq!(d.for_model("sglang-model").images, Some(true));
        assert_eq!(d.for_model("other-model").images, None);
        assert_eq!(d.for_model("sglang-model").responses_lite, Some(false));
        assert_eq!(d.for_model("other-model").responses_lite, None);
        assert_eq!(d.for_model("sglang-model").tool_inventory, Some(true));
    }

    #[test]
    fn discovery_merge_keeps_unknown_and_deduplicates_paths() {
        let mut first =
            parse(&json!({"capabilities":{"tool_search":true},"paths":{"/tools":{"get":{}}}}));
        first.merge(parse(&json!({"capabilities":{"images":false},"paths":{"/tools":{"get":{}},"/v1/tools/list":{"get":{}}}})));
        assert_eq!(first.capabilities.tool_search, Some(true));
        assert_eq!(first.capabilities.images, Some(false));
        assert_eq!(first.tools_paths, vec!["/tools", "/v1/tools/list"]);
    }

    #[test]
    fn inventory_response_array_is_reachable_and_empty_inventory_is_known() {
        assert_eq!(
            parse(&json!([{"type":"tool_search"}]))
                .capabilities
                .tool_search,
            Some(true)
        );
        assert_eq!(
            parse(&json!({"tools":[]})).capabilities.tool_inventory,
            Some(true)
        );
        assert_eq!(
            parse(&json!({"tool_server":null}))
                .capabilities
                .tool_inventory,
            None
        );
        let object_inventory = parse(&json!({"tools":[{"type":"tool_search"}]}));
        assert_eq!(object_inventory.capabilities.tool_inventory, Some(true));
        assert_eq!(object_inventory.capabilities.tool_search, Some(true));
    }

    #[test]
    fn closed_detail_enum_can_disprove_original_but_open_schema_cannot() {
        let bounded = parse(
            &json!({"paths":{"/v1/responses":{"post":{"requestBody":{"content":{"application/json":{"schema":{"$ref":"#/components/schemas/Input"}}}}}}},"components":{"schemas":{"Input":{"properties":{"type":{"const":"input_image"},"detail":{"anyOf":[{"type":"string","enum":["auto","low","high"]},{"type":"null"}]}}}}}}),
        );
        assert_eq!(bounded.capabilities.image_detail_original, Some(false));
        let open = parse(
            &json!({"components":{"schemas":{"Input":{"properties":{"type":{"const":"input_image"},"detail":{"type":"string"}}}}}}),
        );
        assert_eq!(open.capabilities.image_detail_original, None);
    }

    #[test]
    fn only_responses_request_schema_is_used_and_open_anyof_stays_unknown() {
        let d = parse(&json!({
            "paths":{"/v1/responses":{"post":{"requestBody":{"content":{"application/json":{"schema":{"$ref":"#/components/schemas/Request"}}}}}}},
            "components":{"schemas":{
                "Request":{"properties":{"input":{"items":{"$ref":"#/components/schemas/Image"}}}},
                "Image":{"properties":{"type":{"const":"input_image"},"detail":{"anyOf":[{"enum":["low","high"]},{"type":"string"}]}}},
                "Unrelated":{"properties":{"type":{"const":"input_image"},"detail":{"enum":["low","high"]}}}
            }}
        }));
        assert_eq!(d.capabilities.image_detail_original, None);
        let unrelated = parse(
            &json!({"components":{"schemas":{"Unrelated":{"properties":{"type":{"const":"input_image"},"detail":{"enum":["low","high"]}}}}}}),
        );
        assert_eq!(unrelated.capabilities.image_detail_original, None);
    }

    #[test]
    fn conflicting_or_partly_unknown_image_variants_leave_fact_unknown() {
        let mixed = |second_detail: Value| {
            json!({
                "paths":{"/v1/responses":{"post":{"requestBody":{"content":{"application/json":{"schema":{"$ref":"#/components/schemas/Request"}}}}}}},
                "components":{"schemas":{
                    "Request":{"properties":{"input":{"items":{"anyOf":[{"$ref":"#/components/schemas/ImageA"},{"$ref":"#/components/schemas/ImageB"}]}}}},
                    "ImageA":{"properties":{"type":{"const":"input_image"},"detail":{"enum":["low","high"]}}},
                    "ImageB":{"properties":{"type":{"const":"input_image"},"detail":second_detail}}
                }}
            })
        };
        assert_eq!(
            parse(&mixed(json!({"type":"string"})))
                .capabilities
                .image_detail_original,
            None
        );
        assert_eq!(
            parse(&mixed(json!({"enum":["low","high","original"]})))
                .capabilities
                .image_detail_original,
            None
        );
    }

    #[test]
    fn tool_paths_must_be_explicit_gets_without_parameters() {
        let d = parse(
            &json!({"paths":{"/tools":{"get":{}},"/v1/tools/list":{"get":{}},"/tools/{id}":{"get":{}},"/tools/list":{"post":{}}}}),
        );
        assert_eq!(d.tools_paths, vec!["/tools", "/v1/tools/list"]);
    }

    #[test]
    fn cyclic_schema_references_terminate_and_stay_unknown() {
        let d = parse(
            &json!({"paths":{"/v1/responses":{"post":{"requestBody":{"content":{"application/json":{"schema":{"$ref":"#/components/schemas/A"}}}}}}},"components":{"schemas":{"A":{"$ref":"#/components/schemas/B"},"B":{"$ref":"#/components/schemas/A"}}}}),
        );
        assert_eq!(d.capabilities.image_detail_original, None);
    }
}
