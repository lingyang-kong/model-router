use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::Value;

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
pub(crate) struct Capabilities {
    pub service_tiers: Option<bool>,
    #[serde(skip)]
    pub service_tier_ids: Option<Vec<String>>,
    pub web_search: Option<bool>,
    pub access_programs: Option<bool>,
    pub history_notes: Option<bool>,
    pub responses_lite: Option<bool>,
    pub tool_search: Option<bool>,
    /// Provider-hosted tool inventory availability, independent of model capabilities.
    pub tool_inventory: Option<bool>,
    pub image_detail_original: Option<bool>,
    pub images: Option<bool>,
    pub reasoning_summary: Option<bool>,
    #[serde(skip)]
    pub reasoning_summary_levels: Option<Vec<String>>,
    #[serde(skip)]
    pub reasoning_summary_default: Option<String>,
    #[serde(skip)]
    pub verbosity_levels: Option<Vec<String>>,
    #[serde(skip)]
    pub verbosity_default: Option<String>,
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
        merge_field!(service_tiers);
        merge_field!(service_tier_ids);
        merge_field!(web_search);
        merge_field!(access_programs);
        merge_field!(history_notes);
        merge_field!(reasoning_summary);
        merge_field!(reasoning_summary_levels);
        merge_field!(reasoning_summary_default);
        merge_field!(verbosity_levels);
        merge_field!(verbosity_default);
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
    /// Later documents overwrite known fields, regardless of evidence source.
    /// Missing fields preserve earlier observations.
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

/// Read declared capabilities and supported schema constraints; unknowns remain `None`.
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

    // Agreeing detail enums establish whether `original` is accepted.
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

    // Only use a deduction when all relevant Responses request schemas agree.
    let mut tier_constraints = Vec::new();
    let mut tier_id_constraints = Vec::new();
    let mut search_constraints = Vec::new();
    let mut summary_constraints = Vec::new();
    let mut verbosity_constraints = Vec::new();
    if let Some(paths) = root.get("paths").and_then(Value::as_object) {
        for (path, item) in paths {
            if !path.ends_with("/responses") {
                continue;
            }
            if !item.get("post").is_some_and(Value::is_object) {
                continue;
            }
            let Some(schema) = item.pointer("/post/requestBody/content/application~1json/schema")
            else {
                tier_constraints.push(None);
                tier_id_constraints.push(None);
                search_constraints.push(None);
                summary_constraints.push(None);
                verbosity_constraints.push(None);
                continue;
            };
            let ids = service_tier_ids_from_schema(schema, document);
            tier_constraints.push(ids.as_ref().map(|ids| {
                ids.iter()
                    .any(|id| !matches!(id.as_str(), "auto" | "default"))
            }));
            tier_id_constraints.push(ids);
            search_constraints.push(web_search_from_schema(schema, document));
            summary_constraints.push(summary_facts_from_schema(schema, document));
            verbosity_constraints.push(verbosity_facts_from_schema(schema, document));
        }
    }
    if tier_id_constraints_agree(&tier_id_constraints) {
        found.capabilities.service_tier_ids = tier_id_constraints[0].clone();
    }
    if found.capabilities.service_tiers.is_none()
        && !tier_constraints.is_empty()
        && constraints_agree(&tier_constraints)
    {
        found.capabilities.service_tiers = tier_constraints[0];
    }
    if found.capabilities.web_search.is_none()
        && !search_constraints.is_empty()
        && constraints_agree(&search_constraints)
    {
        found.capabilities.web_search = search_constraints[0];
    }
    if !summary_constraints.is_empty() && constraints_agree(&summary_constraints) {
        let facts = summary_constraints[0].as_ref().unwrap();
        found.capabilities.reasoning_summary =
            found.capabilities.reasoning_summary.or(facts.summary);
        found.capabilities.reasoning_summary_levels = facts.summary_levels.clone();
        found.capabilities.reasoning_summary_default = facts.summary_default.clone();
    }
    if !verbosity_constraints.is_empty() && constraints_agree(&verbosity_constraints) {
        let facts = verbosity_constraints[0].as_ref().unwrap();
        found.capabilities.verbosity_levels = facts.0.clone();
        found.capabilities.verbosity_default = facts.1.clone();
    }
    found
}

fn constraints_agree<T: PartialEq>(constraints: &[Option<T>]) -> bool {
    constraints
        .first()
        .is_some_and(|first| first.is_some() && constraints.iter().all(|value| value == first))
}

fn tier_id_constraints_agree(constraints: &[Option<Vec<String>>]) -> bool {
    let Some(Some(first)) = constraints.first() else {
        return false;
    };
    let first: BTreeSet<_> = first.iter().collect();
    constraints.iter().all(|constraint| {
        constraint
            .as_ref()
            .is_some_and(|ids| ids.iter().collect::<BTreeSet<_>>() == first)
    })
}

fn service_tier_ids_from_schema(schema: &Value, document: &Value) -> Option<Vec<String>> {
    let mut visited = BTreeSet::new();
    let schema = resolve_simple_schema(schema, document, &mut visited, 0)?;
    let props = schema.get("properties").and_then(Value::as_object);
    if let Some(tier) = props.and_then(|props| props.get("service_tier")) {
        let values = closed_enum(tier, document)?;
        return values
            .iter()
            .map(|value| value.as_str().map(str::to_owned))
            .collect();
    }
    None
}

fn web_search_from_schema(schema: &Value, document: &Value) -> Option<bool> {
    let mut visited = BTreeSet::new();
    let schema = resolve_simple_schema(schema, document, &mut visited, 0)?;
    let tools = schema.get("properties")?.as_object()?.get("tools")?;
    let mut visited = BTreeSet::new();
    let tools = resolve_simple_schema(tools, document, &mut visited, 0)?;
    if tools.get("prefixItems").is_some() {
        return None;
    }
    let mut visited = BTreeSet::new();
    let item = resolve_simple_schema(tools.get("items")?, document, &mut visited, 0)?;
    let types = item.get("properties")?.as_object()?.get("type")?;
    let values = closed_enum(types, document)?;
    Some(values.iter().any(|value| {
        value
            .as_str()
            .is_some_and(|kind| kind == "web_search" || kind.starts_with("web_search_"))
    }))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SummaryFacts {
    summary: Option<bool>,
    summary_levels: Option<Vec<String>>,
    summary_default: Option<String>,
}
type VerbosityFacts = (Option<Vec<String>>, Option<String>);

fn field_schema<'a>(schema: &'a Value, document: &'a Value, name: &str) -> Option<&'a Value> {
    let mut visited = BTreeSet::new();
    let schema = resolve_simple_schema(schema, document, &mut visited, 0)?;
    schema.get("properties")?.as_object()?.get(name)
}

fn summary_facts_from_schema(schema: &Value, document: &Value) -> Option<SummaryFacts> {
    let reasoning = field_schema(schema, document, "reasoning")?;
    let mut visited = BTreeSet::new();
    let reasoning = resolve_nullable_schema(reasoning, document, &mut visited, 0)?;
    let properties = reasoning.get("properties")?.as_object()?;
    let summary_schema = properties.get("summary");
    let summary_levels = summary_schema.and_then(|s| closed_string_enum(s, document));
    let summary = summary_levels
        .as_ref()
        .map(|levels| !levels.is_empty())
        .or_else(|| {
            let schema = summary_schema?;
            if schema == &Value::Bool(false) {
                return Some(false);
            }
            let mut visited = BTreeSet::new();
            let schema = resolve_nullable_schema(schema, document, &mut visited, 0)?;
            (schema.get("type").and_then(Value::as_str) == Some("string")).then_some(true)
        });
    let summary_default = summary_schema.and_then(|s| schema_default_string(s, document));
    Some(SummaryFacts {
        summary,
        summary_levels,
        summary_default,
    })
}

fn verbosity_facts_from_schema(schema: &Value, document: &Value) -> Option<VerbosityFacts> {
    let text = field_schema(schema, document, "text")?;
    let mut visited = BTreeSet::new();
    let text = resolve_nullable_schema(text, document, &mut visited, 0)?;
    let verbosity = text.get("properties")?.as_object()?.get("verbosity")?;
    Some((
        closed_string_enum(verbosity, document),
        schema_default_string(verbosity, document),
    ))
}

fn resolve_nullable_schema<'a>(
    schema: &'a Value,
    document: &'a Value,
    visited: &mut BTreeSet<String>,
    depth: usize,
) -> Option<&'a Value> {
    if depth > 32 {
        return None;
    }
    let schema = resolve_reference(schema, document, visited, depth)?;
    let Some(branches) = schema.get("anyOf").and_then(Value::as_array) else {
        return resolve_simple_schema(schema, document, visited, depth);
    };
    if schema
        .as_object()?
        .keys()
        .any(|key| key != "anyOf" && !schema_annotation(key))
    {
        return None;
    }
    let mut non_null = None;
    let mut saw_null = false;
    for branch in branches {
        let resolved = resolve_reference(branch, document, visited, depth + 1)?;
        if resolved.get("type").and_then(Value::as_str) == Some("null") {
            if saw_null {
                return None;
            }
            saw_null = true;
        } else if non_null.replace(resolved).is_some() {
            return None;
        }
    }
    if saw_null && branches.len() == 2 {
        resolve_simple_schema(non_null?, document, visited, depth + 1)
    } else {
        None
    }
}

fn resolve_reference<'a>(
    schema: &'a Value,
    document: &'a Value,
    visited: &mut BTreeSet<String>,
    depth: usize,
) -> Option<&'a Value> {
    if depth > 32 {
        return None;
    }
    let Some(reference) = schema.get("$ref").and_then(Value::as_str) else {
        return Some(schema);
    };
    if schema
        .as_object()?
        .keys()
        .any(|key| key != "$ref" && !schema_annotation(key))
        || !visited.insert(reference.to_owned())
    {
        return None;
    }
    let target = reference.strip_prefix("#/components/schemas/")?;
    resolve_reference(
        document.pointer(&format!("/components/schemas/{target}"))?,
        document,
        visited,
        depth + 1,
    )
}

fn closed_string_enum(schema: &Value, document: &Value) -> Option<Vec<String>> {
    let mut visited = BTreeSet::new();
    let schema = resolve_nullable_schema(schema, document, &mut visited, 0)?;
    closed_enum(schema, document)?
        .iter()
        .map(|v| v.as_str().map(str::to_owned))
        .collect()
}

fn schema_default_string(schema: &Value, document: &Value) -> Option<String> {
    let wrapper_default = schema.get("default").and_then(Value::as_str);
    let mut visited = BTreeSet::new();
    let schema = resolve_nullable_schema(schema, document, &mut visited, 0)?;
    if let Some(value) = wrapper_default {
        return Some(value.to_owned());
    }
    if let Some(value) = schema.get("default").and_then(Value::as_str) {
        return Some(value.to_owned());
    }
    let mut visited = BTreeSet::new();
    let schema = resolve_simple_schema(schema, document, &mut visited, 0)?;
    schema.get("default")?.as_str().map(str::to_owned)
}

fn resolve_simple_schema<'a>(
    schema: &'a Value,
    document: &'a Value,
    visited: &mut BTreeSet<String>,
    depth: usize,
) -> Option<&'a Value> {
    if depth > 32
        || ["anyOf", "oneOf", "allOf", "not"]
            .iter()
            .any(|key| schema.get(key).is_some())
    {
        return None;
    }
    let Some(reference) = schema.get("$ref").and_then(Value::as_str) else {
        return Some(schema);
    };
    if schema
        .as_object()?
        .keys()
        .any(|key| key != "$ref" && !schema_annotation(key))
    {
        return None;
    }
    if !visited.insert(reference.to_owned()) {
        return None;
    }
    let target = reference.strip_prefix("#/components/schemas/")?;
    resolve_simple_schema(
        document.pointer(&format!("/components/schemas/{target}"))?,
        document,
        visited,
        depth + 1,
    )
}

fn closed_enum(schema: &Value, document: &Value) -> Option<Vec<Value>> {
    let mut visited = BTreeSet::new();
    let schema = resolve_simple_schema(schema, document, &mut visited, 0)?;
    if schema
        .as_object()?
        .keys()
        .any(|key| !matches!(key.as_str(), "enum" | "type") && !schema_annotation(key))
    {
        return None;
    }
    schema.get("enum").and_then(Value::as_array).cloned()
}

fn schema_annotation(key: &str) -> bool {
    matches!(
        key,
        "title"
            | "description"
            | "default"
            | "examples"
            | "example"
            | "deprecated"
            | "readOnly"
            | "writeOnly"
    )
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
    result.service_tiers = get("service_tiers").and_then(Value::as_bool);
    result.web_search = get("web_search").and_then(Value::as_bool);
    result.access_programs = get("access_programs").and_then(Value::as_bool);
    result.history_notes = get("history_notes").and_then(Value::as_bool);
    result.reasoning_summary = get("reasoning_summary")
        .and_then(Value::as_bool)
        .or_else(|| get("supports_reasoning_summary_parameter").and_then(Value::as_bool));
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
        self == &Self::default()
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
    fn provider_settings_and_discovery_share_capability_names() {
        let facts = json!({
            "service_tiers": false, "web_search": false,
            "access_programs": false, "history_notes": false,
            "image_detail_original": true
        });
        let configured: Capabilities = serde_json::from_value(facts.clone()).unwrap();
        assert_eq!(parse(&facts).capabilities, configured);
        assert_eq!(
            parse(&json!({"capabilities": facts})).capabilities,
            configured
        );
        assert_eq!(configured.tool_search, None);
    }

    #[test]
    fn original_image_detail_is_enabled_when_advertised() {
        let document = json!({
            "paths": {"/responses": {"post": {"requestBody": {"content": {
                "application/json": {"schema": {"properties": {
                    "type": {"const": "input_image"},
                    "detail": {"enum": ["auto", "low", "high", "original"]}
                }}}
            }}}}}
        });
        assert_eq!(
            parse(&document).capabilities.image_detail_original,
            Some(true)
        );
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
    fn summary_and_verbosity_use_nullable_closed_schemas_and_defaults() {
        let d = parse(&json!({
            "paths": {"/v1/responses": {"post": {"requestBody": {"content": {
                "application/json": {"schema": {"properties": {
                    "reasoning": {"anyOf": [
                        {"properties": {
                            "effort": {"type":"string"},
                            "summary": {"anyOf":[{"type":"string", "enum":["auto","concise","detailed"]},{"type":"null"}], "default":"auto"}
                        }}, {"type":"null"}
                    ]},
                    "text": {"$ref":"#/components/schemas/TextConfig"}
                }}}
            }}}}},
            "components":{"schemas":{"TextConfig":{"anyOf":[
                {"properties":{"verbosity":{"anyOf":[{"type":"string","enum":["low","medium","high"]},{"type":"null"}],"default":"high"}}},
                {"type":"null"}
            ]}}}
        }));
        assert_eq!(d.capabilities.reasoning_summary, Some(true));
        assert_eq!(
            d.capabilities.reasoning_summary_levels,
            Some(vec!["auto".into(), "concise".into(), "detailed".into()])
        );
        assert_eq!(
            d.capabilities.reasoning_summary_default.as_deref(),
            Some("auto")
        );
        assert_eq!(
            d.capabilities.verbosity_levels,
            Some(vec!["low".into(), "medium".into(), "high".into()])
        );
        assert_eq!(d.capabilities.verbosity_default.as_deref(), Some("high"));
    }

    #[test]
    fn summary_inference_ignores_open_and_ambiguous_unions_and_honors_explicit_flags() {
        let response = |schema| json!({"paths":{"/responses":{"post":{"requestBody":{"content":{"application/json":{"schema":schema}}}}}}});
        let open = parse(&response(json!({"properties": {
            "reasoning":{"properties":{"effort":{"enum":["low"]},"summary":{"type":"string"}}},
            "text":{"properties":{"verbosity":{"type":"string"}}}
        }})));
        assert_eq!(open.capabilities.verbosity_levels, None);
        assert_eq!(open.capabilities.reasoning_summary, Some(true));
        assert_eq!(open.capabilities.reasoning_summary_levels, None);
        assert_eq!(
            parse(&response(json!({"properties": {
                "reasoning":{"properties":{"effort":{"enum":["low"]}}}
            }})))
            .capabilities
            .reasoning_summary,
            None
        );

        let ambiguous = parse(&response(json!({"properties": {
            "reasoning":{"anyOf":[{"properties":{"summary":{"anyOf":[{"enum":["auto"]},{"type":"null"}],"default":"auto"}}},{"type":"string"}]}
        }})));
        assert_eq!(ambiguous.capabilities.reasoning_summary, None);

        let invalid_sibling = parse(&response(json!({"properties": {
            "reasoning":{"properties":{"summary":{"anyOf":[
                {"type":"string","enum":["low"]},{"type":"null"}
            ],"default":"low","pattern":"^low$"}}}
        }})));
        assert_eq!(invalid_sibling.capabilities.reasoning_summary, None);

        let conflict = parse(&json!({
            "paths": {
                "/responses": {"post":{"requestBody":{"content":{"application/json":{"schema":{"properties":{
                    "reasoning":{"properties":{"summary":{"enum":["auto"]}}}
                }}}}}}},
                "/v1/responses": {"post":{"requestBody":{"content":{"application/json":{"schema":{"properties":{
                    "reasoning":{"anyOf":[{"properties":{"summary":{"enum":["detailed"]}}},{"type":"null"}]}
                }}}}}}}
            }
        }));
        assert_eq!(conflict.capabilities.reasoning_summary, None);

        let mut explicit_doc = response(json!({"properties": {
            "reasoning":{"properties":{"summary":{"type":"string"}}}
        }}));
        explicit_doc["capabilities"] = json!({"reasoning_summary":false});
        let explicit = parse(&explicit_doc);
        assert_eq!(explicit.capabilities.reasoning_summary, Some(false));
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
    fn closed_request_schema_can_disprove_tier_and_web_search_support() {
        let d = parse(&json!({
            "paths": {"/v1/responses": {"post": {"requestBody": {"content": {
                "application/json": {"schema": {"$ref": "#/components/schemas/Request"}}
            }}}}},
            "components": {"schemas": {
                "Request": {"properties": {
                    "service_tier": {"enum": ["auto", "default"]},
                    "tools": {"items": {"properties": {
                        "type": {"enum": ["function", "tool_search"]}
                    }}}
                }}
            }}
        }));
        assert_eq!(d.capabilities.service_tiers, Some(false));
        assert_eq!(
            d.capabilities.service_tier_ids,
            Some(vec!["auto".to_owned(), "default".to_owned()])
        );
        assert_eq!(d.capabilities.web_search, Some(false));
    }

    #[test]
    fn open_schemas_stay_unknown_and_closed_enums_are_authoritative() {
        let open = parse(&json!({
            "paths": {"/v1/responses": {"post": {"requestBody": {"content": {
                "application/json": {"schema": {"properties": {
                    "service_tier": {"type": "string"},
                    "tools": {"items": {"properties": {"type": {"type": "string"}}}}
                }}}
            }}}}}
        }));
        assert_eq!(open.capabilities.service_tiers, None);
        assert_eq!(open.capabilities.web_search, None);

        let accepted = parse(&json!({
            "paths": {"/v1/responses": {"post": {"requestBody": {"content": {
                "application/json": {"schema": {"properties": {
                    "service_tier": {"type": "string", "title": "Service Tier", "default": "auto", "enum": ["auto", "default", "flex", "priority"]},
                    "tools": {"items": {"properties": {"type": {"enum": ["function", "web_search_preview_2025_03_11"]}}}}
                }}}
            }}}}}
        }));
        assert_eq!(accepted.capabilities.service_tiers, Some(true));
        assert_eq!(
            accepted.capabilities.service_tier_ids,
            Some(vec![
                "auto".to_owned(),
                "default".to_owned(),
                "flex".to_owned(),
                "priority".to_owned()
            ])
        );
        assert_eq!(accepted.capabilities.web_search, Some(true));
    }

    #[test]
    fn explicit_capability_flags_override_schema_inference() {
        let d = parse(&json!({
            "capabilities": {"service_tiers": true, "web_search": true},
            "paths": {"/v1/responses": {"post": {"requestBody": {"content": {
                "application/json": {"schema": {"additionalProperties": false,
                    "properties": {"tools": {"items": {"properties": {
                        "type": {"enum": ["function"]}
                    }}}}
                }}
            }}}}}
        }));
        assert_eq!(d.capabilities.service_tiers, Some(true));
        assert_eq!(d.capabilities.web_search, Some(true));
    }

    #[test]
    fn open_alternate_branch_and_unrelated_nested_properties_stay_unknown() {
        let alternate = parse(&json!({
            "paths": {"/responses": {"post": {"requestBody": {"content": {
                "application/json": {"schema": {"properties": {
                    "service_tier": {"anyOf": [
                        {"enum": ["auto", "default"]}, {"type": "string"}
                    ]},
                    "tools": {"items": {"properties": {
                        "type": {"anyOf": [{"enum": ["function"]}, {"type": "string"}]}
                    }}}
                }}}
            }}}}}
        }));
        assert_eq!(alternate.capabilities.service_tiers, None);
        assert_eq!(alternate.capabilities.web_search, None);

        let nested = parse(&json!({
            "paths": {"/responses": {"post": {"requestBody": {"content": {
                "application/json": {"schema": {"properties": {
                    "metadata": {"properties": {
                        "service_tier": {"enum": ["auto", "default"]},
                        "tools": {"items": {"properties": {"type": {"enum": ["function"]}}}}
                    }}
                }}}
            }}}}}
        }));
        assert_eq!(nested.capabilities.service_tiers, None);
        assert_eq!(nested.capabilities.web_search, None);
    }

    #[test]
    fn unresolved_refs_and_unknown_response_paths_block_negative_inference() {
        let unresolved = parse(&json!({
            "paths": {"/responses": {"post": {"requestBody": {"content": {
                "application/json": {"schema": {"$ref": "#/components/schemas/Missing"}}
            }}}}}
        }));
        assert_eq!(unresolved.capabilities.service_tiers, None);
        assert_eq!(unresolved.capabilities.web_search, None);

        let multiple = parse(&json!({
            "paths": {
                "/v1/responses": {"post": {"requestBody": {"content": {
                    "application/json": {"schema": {"properties": {
                        "service_tier": {"enum": ["auto", "default"]},
                        "tools": {"items": {"properties": {"type": {"enum": ["function"]}}}}
                    }}}
                }}}},
                "/responses": {"post": {}}
            }
        }));
        assert_eq!(multiple.capabilities.service_tiers, None);
        assert_eq!(multiple.capabilities.web_search, None);
    }

    #[test]
    fn referenced_tool_items_are_resolved_for_closed_type_enums() {
        let d = parse(&json!({
            "paths": {"/responses": {"post": {"requestBody": {"content": {
                "application/json": {"schema": {"properties": {
                    "tools": {"items": {"$ref": "#/components/schemas/Tool"}}
                }}}
            }}}}},
            "components": {"schemas": {"Tool": {"properties": {
                "type": {"enum": ["function", "tool_search"]}
            }}}}
        }));
        assert_eq!(d.capabilities.web_search, Some(false));
    }

    #[test]
    fn unsupported_compositions_prefix_items_and_conflicting_paths_stay_unknown() {
        let root_union = parse(&json!({
            "paths": {"/responses": {"post": {"requestBody": {"content": {
                "application/json": {"schema": {"anyOf": [
                    {"properties": {"service_tier": {"enum": ["auto"]}}},
                    {"properties": {"service_tier": {"enum": ["priority"]}}}
                ]}}
            }}}}}
        }));
        assert_eq!(root_union.capabilities.service_tiers, None);

        let prefix = parse(&json!({
            "paths": {"/responses": {"post": {"requestBody": {"content": {
                "application/json": {"schema": {"properties": {"tools": {
                    "prefixItems": [{"properties": {"type": {"enum": ["function"]}}}],
                    "items": {"properties": {"type": {"enum": ["function"]}}}
                }}}}
            }}}}}
        }));
        assert_eq!(prefix.capabilities.web_search, None);

        let conflict = parse(&json!({
            "paths": {
                "/responses": {"post": {"requestBody": {"content": {
                    "application/json": {"schema": {"properties": {
                        "service_tier": {"enum": ["auto"]},
                        "tools": {"items": {"properties": {"type": {"enum": ["function"]}}}}
                    }}}
                }}}},
                "/v1/responses": {"post": {"requestBody": {"content": {
                    "application/json": {"schema": {"properties": {
                        "service_tier": {"enum": ["priority"]},
                        "tools": {"items": {"properties": {"type": {"enum": ["web_search"]}}}}
                    }}}
                }}}}
            }
        }));
        assert_eq!(conflict.capabilities.service_tiers, None);
        assert_eq!(conflict.capabilities.web_search, None);
    }

    #[test]
    fn tier_ids_require_matching_closed_enums_across_response_paths() {
        let d = parse(&json!({
            "paths": {
                "/responses": {"post": {"requestBody": {"content": {
                    "application/json": {"schema": {"properties": {
                        "service_tier": {"enum": ["auto", "flex"]}
                    }}}
                }}}},
                "/v1/responses": {"post": {"requestBody": {"content": {
                    "application/json": {"schema": {"properties": {
                        "service_tier": {"enum": ["auto", "priority"]}
                    }}}
                }}}}
            }
        }));
        assert_eq!(d.capabilities.service_tiers, Some(true));
        assert_eq!(d.capabilities.service_tier_ids, None);

        let same_set = parse(&json!({
            "paths": {
                "/responses": {"post": {"requestBody": {"content": {
                    "application/json": {"schema": {"properties": {
                        "service_tier": {"enum": ["auto", "flex"]}
                    }}}
                }}}},
                "/v1/responses": {"post": {"requestBody": {"content": {
                    "application/json": {"schema": {"properties": {
                        "service_tier": {"enum": ["flex", "auto"]}
                    }}}
                }}}}
            }
        }));
        assert_eq!(same_set.capabilities.service_tiers, Some(true));
        assert_eq!(
            same_set.capabilities.service_tier_ids,
            Some(vec!["auto".to_owned(), "flex".to_owned()])
        );
    }

    #[test]
    fn ref_siblings_and_enum_validation_constraints_stay_unknown() {
        let d = parse(&json!({
            "paths": {"/responses": {"post": {"requestBody": {"content": {
                "application/json": {"schema": {"$ref": "#/components/schemas/Request"}}
            }}}}},
            "components": {"schemas": {
                "Request": {"properties": {
                    "service_tier": {"enum": ["auto", "default"], "const": "auto"},
                    "tools": {"items": {"properties": {
                        "type": {"$ref": "#/components/schemas/ToolType", "enum": ["function"]}
                    }}}
                }},
                "ToolType": {"enum": ["function", "web_search"]}
            }}
        }));
        assert_eq!(d.capabilities.service_tiers, None);
        assert_eq!(d.capabilities.service_tier_ids, None);
        assert_eq!(d.capabilities.web_search, None);

        let validation = parse(&json!({
            "paths": {"/responses": {"post": {"requestBody": {"content": {
                "application/json": {"schema": {"properties": {
                    "service_tier": {"type": "string", "enum": ["auto", "priority"], "pattern": "^priority$"}
                }}}
            }}}}}
        }));
        assert_eq!(validation.capabilities.service_tiers, None);
        assert_eq!(validation.capabilities.service_tier_ids, None);
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
