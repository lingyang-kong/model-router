use std::sync::{Arc, Mutex};

use axum::{Json, Router, extract::Request, http::StatusCode, response::IntoResponse};
use model_router::router;
use serde_json::{Value, json};

use crate::common::*;

type RecordedRequests = Arc<Mutex<Vec<(String, std::collections::BTreeMap<String, String>)>>>;

type ProbeState = Arc<Mutex<(StatusCode, Value)>>;

#[derive(Clone)]
struct DiscoveryFixture {
    requests: RecordedRequests,
    documents: Arc<Mutex<Value>>,
    probes: Arc<Mutex<Vec<Value>>>,
    probe_reply: ProbeState,
}

async fn discovery_source(
    axum::extract::State(fixture): axum::extract::State<DiscoveryFixture>,
    request: Request,
) -> axum::response::Response {
    let path = request.uri().path().to_owned();
    let headers = request
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_owned(), value.to_owned()))
        })
        .collect();
    fixture
        .requests
        .lock()
        .unwrap()
        .push((path.clone(), headers));
    if path == "/catalog" {
        let catalog = fixture
            .documents
            .lock()
            .unwrap()
            .get("/catalog")
            .cloned()
            .unwrap_or_else(|| json!({"data":[{"id":"worker"}]}));
        return Json(catalog).into_response();
    }
    if path == "/responses" || path == "/v1/responses" {
        let body = axum::body::to_bytes(request.into_body(), 1024 * 1024)
            .await
            .unwrap();
        if let Ok(payload) = serde_json::from_slice::<Value>(&body) {
            fixture.probes.lock().unwrap().push(payload);
        }
        let (status, response) = fixture.probe_reply.lock().unwrap().clone();
        let mut response = Json(response).into_response();
        *response.status_mut() = status;
        return response;
    }
    match fixture.documents.lock().unwrap().get(&path).cloned() {
        Some(Value::Null) | None => StatusCode::NOT_FOUND.into_response(),
        Some(document) => Json(document).into_response(),
    }
}

fn openapi_without_original() -> Value {
    json!({
        "paths": {
            "/v1/responses": {
                "post": {"requestBody": {"required": true, "content": {
                    "application/json": {"schema": {"$ref":"#/components/schemas/Request"}}
                }}}
            },
            "/v1/tools": {"get": {}}
        },
        "components": {"schemas": {
            "Request": {"properties":{"input":{"type":"array","items":{"$ref":"#/components/schemas/Input"}}}},
            "Input": {"properties":{"type":{"const":"input_image"},"detail":{"type":"string","enum":["auto","low","high"]}}}
        }}
    })
}

async fn gateway_with(
    fixture: &Fixture,
    discovery: Value,
    discovery_urls: Vec<String>,
) -> (Server, Server, DiscoveryFixture) {
    gateway_with_provider_options(fixture, discovery, discovery_urls, json!({}), vec![]).await
}

async fn gateway_with_provider_options(
    fixture: &Fixture,
    discovery: Value,
    discovery_urls: Vec<String>,
    capabilities: Value,
    hidden_models: Vec<&str>,
) -> (Server, Server, DiscoveryFixture) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let source = DiscoveryFixture {
        requests: requests.clone(),
        documents: Arc::new(Mutex::new(discovery)),
        probes: Arc::new(Mutex::new(Vec::new())),
        probe_reply: Arc::new(Mutex::new((StatusCode::OK, json!({"ok":true})))),
    };
    let source_server = serve(
        Router::new()
            .fallback(discovery_source)
            .with_state(source.clone()),
    )
    .await;
    let urls = discovery_urls
        .into_iter()
        .map(|path| format!("{}{}", source_server.url, path))
        .collect::<Vec<_>>();
    let config = parse_config(
        &json!({
            "providers": {
                "donor": {"base_url":fixture._manual_server.url,"auth":"none","catalog":{"file":"donor.json","format":"codex"}},
                "custom": {
                    "base_url":source_server.url,"auth":"none",
                    "catalog":{"path":"/catalog","format":"openai"},
                    "http_headers":{"x-provider-secret":"provider-secret"},
                    "discovery_urls":urls,
                    "capabilities":capabilities,
                    "hidden_models":hidden_models,
                    "defaults":{"extends":{"provider":"donor","model":"donor-model"}},
                }
            },
            "routes":[
                {"method":"GET","path":"/models","action":"catalog","provider":"custom","format":"codex"},
                {"action":"model","provider":"custom"}
            ]
        })
        .to_string(),
        fixture.root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    (gateway, source_server, source)
}

fn write_donor(fixture: &Fixture) {
    std::fs::write(
        fixture.root.path().join("donor.json"),
        json!({"models":[{
            "slug":"donor-model", "use_responses_lite":true,
            "supports_search_tool":true, "input_modalities":["text","image"]
        }]})
        .to_string(),
    )
    .unwrap();
}

async fn fetch_catalog(fixture: &Fixture, gateway: &Server) -> Value {
    let response = fixture
        .client
        .get(gateway.url.clone() + "/models")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.json().await.unwrap()
}

#[tokio::test]
async fn summary_and_verbosity_refresh_but_effort_schema_does_not_override_template() {
    let fixture = Fixture::new().await;
    std::fs::write(fixture.root.path().join("donor.json"), json!({"models":[{
        "slug":"donor-model", "supported_reasoning_levels":[{"effort":"old","description":"old"}],
        "default_reasoning_level":"old", "supports_reasoning_summary_parameter":false,
        "default_reasoning_summary":"detailed", "support_verbosity":false,
        "default_verbosity":"high"
    }]}).to_string()).unwrap();
    let schema = json!({"paths":{"/responses":{"post":{"requestBody":{"content":{
        "application/json":{"schema":{"properties":{
            "reasoning":{"anyOf":[{"properties":{
                "effort":{"anyOf":[{"enum":["low","medium","high"]},{"type":"null"}],"default":"medium"},
                "summary":{"enum":["auto","concise"],"default":"auto"}
            }},{"type":"null"}]},
            "text":{"properties":{"verbosity":{"enum":["low","medium"],"default":"low"}}}
        }}}
    }}}}}});
    let (gateway, _upstream, source) = gateway_with(
        &fixture,
        json!({"/openapi.json":schema}),
        vec!["/openapi.json".into()],
    )
    .await;
    let catalog = fetch_catalog(&fixture, &gateway).await;
    let row = catalog["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["slug"] == "worker")
        .unwrap();
    assert_eq!(
        row["supported_reasoning_levels"]
            .as_array()
            .unwrap()
            .iter()
            .map(|level| level["effort"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["old"]
    );
    assert_eq!(row["default_reasoning_level"], "old");
    assert!(row.get("supports_reasoning_summary_parameter").is_none());
    assert!(row.get("default_reasoning_summary").is_none());
    assert_eq!(row["support_verbosity"], true);
    assert_eq!(row["default_verbosity"], "low");
    source.documents.lock().unwrap()["/openapi.json"] = Value::Null;
    let catalog = fetch_catalog(&fixture, &gateway).await;
    let row = catalog["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["slug"] == "worker")
        .unwrap();
    assert_eq!(row["default_reasoning_level"], "old");
    assert_eq!(row["support_verbosity"], false);
}

#[tokio::test]
async fn refresh_queries_explicit_and_advertised_sources_once_then_projects_facts() {
    let fixture = Fixture::new().await;
    write_donor(&fixture);
    let (gateway, _source_server, source) = gateway_with(
        &fixture,
        json!({
            "/model_info":{"served_model_name":"worker","has_image_understanding":true},
            "/server_info":{"capabilities":{"responses_lite":true},"paths":{"/v1/tools":{"get":{}}}},
            "/openapi.json":openapi_without_original(),
            "/v1/tools":{"tools":[{"type":"tool_search"}]}
        }),
        vec![
            "/model_info".into(),
            "/model_info".into(),
            "/server_info".into(),
            "/openapi.json".into(),
        ],
    )
    .await;

    let response = fetch_catalog(&fixture, &gateway).await;
    let worker = response["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "worker")
        .unwrap();
    assert_eq!(worker["use_responses_lite"], true);
    assert_eq!(worker["supports_search_tool"], true);
    assert_eq!(worker["input_modalities"], json!(["text", "image"]));
    assert!(worker.get("supports_image_detail_original").is_none());

    let second = fetch_catalog(&fixture, &gateway).await;
    assert_eq!(second["models"], response["models"]);

    let requests = source.requests.lock().unwrap();
    for path in ["/model_info", "/server_info", "/openapi.json", "/v1/tools"] {
        assert_eq!(
            requests.iter().filter(|(seen, _)| seen == path).count(),
            2,
            "{path}"
        );
    }
    assert!(!requests.iter().any(|(path, _)| path == "/responses"));
    assert!(
        requests
            .iter()
            .all(|(_, headers)| headers.get("x-provider-secret").is_some())
    );
    assert!(
        requests
            .iter()
            .all(|(_, headers)| !headers.contains_key("authorization"))
    );
    let probes = source.probes.lock().unwrap();
    assert!(probes.is_empty());
}

#[tokio::test]
async fn negative_inventory_skips_tool_fetch_without_clearing_inherited_capability() {
    let fixture = Fixture::new().await;
    write_donor(&fixture);
    let (gateway, _source_server, source) = gateway_with(
        &fixture,
        json!({
            "/server_info":{"capabilities":{"tool_inventory":false,"responses_lite":false}},
            "/openapi.json":openapi_without_original(),
            "/v1/tools":{"tools":[{"type":"tool_search"}]}
        }),
        vec!["/server_info".into(), "/openapi.json".into()],
    )
    .await;

    let response = fetch_catalog(&fixture, &gateway).await;
    let worker = response["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "worker")
        .unwrap();
    assert_eq!(worker["supports_search_tool"], true);
    {
        let requests = source.requests.lock().unwrap();
        assert!(!requests.iter().any(|(path, _)| path == "/v1/tools"));
    }

    // A fresh negative scoped to the sole served model also makes the query unnecessary.
    source.documents.lock().unwrap()["/server_info"] = json!({
        "served_model_name":"worker", "capabilities":{"tool_inventory":false,"responses_lite":false}
    });
    fetch_catalog(&fixture, &gateway).await;
    assert!(
        !source
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|(path, _)| path == "/v1/tools")
    );

    // A different model's negative cannot suppress discovery for this model.
    source.documents.lock().unwrap()["/server_info"]["served_model_name"] = json!("other");
    fetch_catalog(&fixture, &gateway).await;
    assert_eq!(
        source
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path == "/v1/tools")
            .count(),
        1
    );
}

#[tokio::test]
async fn codex_slug_model_inventory_false_skips_tool_fetch_and_keeps_inherited_search() {
    let fixture = Fixture::new().await;
    write_donor(&fixture);
    let (gateway, _source_server, source) = gateway_with(
        &fixture,
        json!({
            "/catalog":{"data":[{"id":"worker"},{"id":"second-worker"}]},
            "/server_info":{"models":[
                {"slug":"worker", "capabilities":{"tool_inventory":false}},
                {"slug":"second-worker", "capabilities":{"tool_inventory":false}}
            ]},
            "/openapi.json":openapi_without_original(),
            "/v1/tools":{"tools":[{"type":"tool_search"}]}
        }),
        vec!["/server_info".into(), "/openapi.json".into()],
    )
    .await;

    let response = fetch_catalog(&fixture, &gateway).await;
    let models = response["models"].as_array().unwrap();
    for id in ["worker", "second-worker"] {
        assert!(models.iter().any(|model| model["slug"] == id));
    }
    assert!(
        models
            .iter()
            .all(|model| model["supports_search_tool"] == true)
    );
    assert_eq!(
        source
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path == "/v1/tools")
            .count(),
        0
    );
}

#[tokio::test]
async fn responses_lite_probe_learns_only_a_specific_rejection_and_runs_each_refresh() {
    let fixture = Fixture::new().await;
    write_donor(&fixture);
    let (gateway, _source_server, source) = gateway_with(&fixture, json!({}), vec![]).await;
    *source.probe_reply.lock().unwrap() = (
        StatusCode::UNPROCESSABLE_ENTITY,
        json!({"error":{"message":"Unsupported Responses API input item type: 'additional_tools'"}}),
    );

    let rejected = fetch_catalog(&fixture, &gateway).await;
    let rejected_worker = rejected["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "worker")
        .unwrap();
    assert!(rejected_worker.get("use_responses_lite").is_none());
    let first_probe = source.probes.lock().unwrap()[0].clone();
    assert_eq!(first_probe["input"][0]["type"], "additional_tools");
    assert_eq!(first_probe["input"][0]["role"], "developer");
    assert_eq!(first_probe["input"][0]["tools"], json!([]));
    assert_eq!(
        first_probe["input"][1]["type"],
        "__model_router_capability_check__"
    );
    assert_eq!(first_probe["max_output_tokens"], 1);
    assert_eq!(first_probe["store"], false);

    *source.probe_reply.lock().unwrap() = (
        StatusCode::UNAUTHORIZED,
        json!({"error":{"message":"Invalid API key"}}),
    );
    let unknown = fetch_catalog(&fixture, &gateway).await;
    let unknown_worker = unknown["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "worker")
        .unwrap();
    assert_eq!(unknown_worker["use_responses_lite"], true);
    assert_eq!(source.probes.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn disabled_template_features_do_not_trigger_compatibility_probes() {
    let fixture = Fixture::new().await;
    std::fs::write(
        fixture.root.path().join("donor.json"),
        json!({"models":[{
            "slug":"donor-model", "use_responses_lite":false, "supports_search_tool":false
        }]})
        .to_string(),
    )
    .unwrap();
    let (gateway, _upstream, source) = gateway_with(&fixture, json!({}), vec![]).await;
    let catalog = fetch_catalog(&fixture, &gateway).await;
    let row = catalog["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["slug"] == "worker")
        .unwrap();
    assert!(row.get("use_responses_lite").is_none());
    assert!(row.get("supports_search_tool").is_none());
    assert!(source.probes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn tool_search_probe_learns_a_specific_negative_and_does_not_reuse_it() {
    let fixture = Fixture::new().await;
    write_donor(&fixture);
    let (gateway, _source_server, source) = gateway_with(
        &fixture,
        json!({"/server_info":{"capabilities":{"responses_lite":true}}}),
        vec!["/server_info".into()],
    )
    .await;
    *source.probe_reply.lock().unwrap() = (
        StatusCode::BAD_REQUEST,
        json!({"error":{"message":"Unsupported Responses API input item type: 'tool_search_output' None"}}),
    );

    let rejected = fetch_catalog(&fixture, &gateway).await;
    let rejected_worker = rejected["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "worker")
        .unwrap();
    assert!(rejected_worker.get("supports_search_tool").is_none());
    let first_probe = source.probes.lock().unwrap()[0].clone();
    assert_eq!(first_probe["input"][0]["type"], "tool_search_output");
    assert_eq!(first_probe["input"][0]["call_id"], "model_router_probe");
    assert_eq!(first_probe["input"][0]["status"], "completed");
    assert_eq!(first_probe["input"][0]["execution"], "client");
    assert_eq!(first_probe["input"][0]["tools"], json!([]));
    assert_eq!(
        first_probe["input"][1]["type"],
        "__model_router_capability_check__"
    );
    assert_eq!(first_probe["max_output_tokens"], 1);
    assert_eq!(first_probe["store"], false);

    *source.probe_reply.lock().unwrap() = (
        StatusCode::UNAUTHORIZED,
        json!({"error":{"message":"Invalid API key"}}),
    );
    let unknown = fetch_catalog(&fixture, &gateway).await;
    let unknown_worker = unknown["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "worker")
        .unwrap();
    assert_eq!(unknown_worker["supports_search_tool"], true);
    assert_eq!(source.probes.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn provider_capability_overrides_discovered_and_inherited_search_support() {
    let fixture = Fixture::new().await;
    write_donor(&fixture);
    let (gateway, _source_server, source) = gateway_with_provider_options(
        &fixture,
        json!({
            "/model_info":{"served_model_name":"worker","capabilities":{"tool_search":true,"responses_lite":true}}
        }),
        vec!["/model_info".into()],
        json!({"tool_search":false}),
        vec![],
    )
    .await;

    let response = fetch_catalog(&fixture, &gateway).await;
    let worker = response["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "worker")
        .unwrap();
    assert!(worker.get("supports_search_tool").is_none());
    assert!(source.probes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn hidden_models_skip_search_capability_probes() {
    let fixture = Fixture::new().await;
    write_donor(&fixture);
    let (gateway, _source_server, source) = gateway_with_provider_options(
        &fixture,
        json!({"/server_info":{"capabilities":{"responses_lite":true}}}),
        vec!["/server_info".into()],
        json!({}),
        vec!["worker"],
    )
    .await;

    let response = fetch_catalog(&fixture, &gateway).await;
    assert!(
        !response["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|model| model["slug"] == "worker")
    );
    assert!(source.probes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn provider_tool_inventory_false_overrides_discovery_and_skips_tool_fetch() {
    let fixture = Fixture::new().await;
    write_donor(&fixture);
    let (gateway, _source_server, source) = gateway_with_provider_options(
        &fixture,
        json!({
            "/server_info":{"capabilities":{"tool_inventory":true}},
            "/openapi.json":openapi_without_original(),
            "/v1/tools":{"tools":[{"type":"tool_search"}]}
        }),
        vec!["/server_info".into(), "/openapi.json".into()],
        json!({"tool_inventory":false}),
        vec![],
    )
    .await;

    fetch_catalog(&fixture, &gateway).await;
    assert!(
        !source
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|(path, _)| path == "/v1/tools")
    );
}

#[tokio::test]
async fn discovery_facts_refresh_each_time_and_catalog_inventory_false_short_circuits_tools() {
    let fixture = Fixture::new().await;
    write_donor(&fixture);
    let (gateway, _source_server, source) = gateway_with(
        &fixture,
        json!({
            "/catalog":{"capabilities":{"tool_inventory":false},"data":[{"id":"worker"}]},
            "/server_info":{"capabilities":{"tool_inventory":false}},
            "/openapi.json":openapi_without_original(),
            "/v1/tools":{"tools":[{"type":"tool_search"}]}
        }),
        vec!["/server_info".into(), "/openapi.json".into()],
    )
    .await;

    fetch_catalog(&fixture, &gateway).await;
    {
        let requests = source.requests.lock().unwrap();
        assert!(!requests.iter().any(|(path, _)| path == "/v1/tools"));
    }

    {
        let mut docs = source.documents.lock().unwrap();
        docs["/catalog"] = json!({"data":[{"id":"worker"}]});
        docs["/server_info"] = json!({"capabilities":{"tool_inventory":true}});
    }
    fetch_catalog(&fixture, &gateway).await;
    {
        let requests = source.requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|(path, _)| path == "/server_info")
                .count(),
            2
        );
        assert_eq!(
            requests
                .iter()
                .filter(|(path, _)| path == "/openapi.json")
                .count(),
            2
        );
        assert_eq!(
            requests
                .iter()
                .filter(|(path, _)| path == "/v1/tools")
                .count(),
            1
        );
    }

    {
        let mut docs = source.documents.lock().unwrap();
        docs["/server_info"] = Value::Null;
    }
    fetch_catalog(&fixture, &gateway).await;
    let requests = source.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|(path, _)| path == "/server_info")
            .count(),
        3
    );
    assert_eq!(
        requests
            .iter()
            .filter(|(path, _)| path == "/openapi.json")
            .count(),
        3
    );
    assert_eq!(
        requests
            .iter()
            .filter(|(path, _)| path == "/v1/tools")
            .count(),
        2
    );
}

#[tokio::test]
async fn missing_or_wrong_model_facts_preserve_inherited_values() {
    let fixture = Fixture::new().await;
    write_donor(&fixture);
    let (gateway, _source_server, source) = gateway_with(
        &fixture,
        json!({
            "/model_info":{"served_model_name":"some-other-model","has_image_understanding":false,"capabilities":{"responses_lite":false,"tool_search":false,"images":false}},
            "/openapi.json":null
        }),
        vec!["/model_info".into(), "/openapi.json".into()],
    )
    .await;

    let response = fetch_catalog(&fixture, &gateway).await;
    let worker = response["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "worker")
        .unwrap();
    assert_eq!(worker["input_modalities"], json!(["text", "image"]));
    assert_eq!(worker["supports_search_tool"], true);
    assert_eq!(worker["use_responses_lite"], true);
    assert_eq!(source.probes.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn explicit_external_discovery_url_is_fetched_without_provider_credentials() {
    let fixture = Fixture::new().await;
    write_donor(&fixture);
    *fixture.live.models.lock().unwrap() = vec![json!({"slug":"worker"})];
    let external_requests = Arc::new(Mutex::new(Vec::new()));
    let external_state = DiscoveryFixture {
        requests: external_requests.clone(),
        documents: Arc::new(Mutex::new(json!({
            "/model_info":{"served_model_name":"worker","has_image_understanding":false}
        }))),
        probes: Arc::new(Mutex::new(Vec::new())),
        probe_reply: Arc::new(Mutex::new((StatusCode::OK, json!({"ok":true})))),
    };
    let external = serve(
        Router::new()
            .fallback(discovery_source)
            .with_state(external_state),
    )
    .await;
    let config = parse_config(
        &json!({
            "providers": {
                "donor":{"base_url":fixture._manual_server.url,"auth":"none","catalog":{"file":"donor.json","format":"codex"}},
                "custom":{
                    "base_url":fixture._live_server.url,"auth":"none",
                    "catalog":{"path":"/models","format":"codex"},
                    "http_headers":{"x-provider-secret":"should-not-leak"},
                    "discovery_urls":[format!("{}/model_info", external.url)],
                    "defaults":{"extends":{"provider":"donor","model":"donor-model"}}
                }
            },
            "routes":[
                {"method":"GET","path":"/models","action":"catalog","provider":"custom","format":"codex"},
                {"action":"model","provider":"custom"}
            ]
        })
        .to_string(),
        fixture.root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let response = fetch_catalog(&fixture, &gateway).await;
    let worker = response["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "worker")
        .unwrap();
    assert_eq!(worker["input_modalities"], json!(["text"]));
    let requests = external_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(!requests[0].1.contains_key("x-provider-secret"));
    assert!(!requests[0].1.contains_key("authorization"));
    assert!(!requests[0].1.contains_key("cookie"));
}
