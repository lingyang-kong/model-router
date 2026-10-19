use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU16, Ordering},
};

use axum::{
    Json, Router,
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use model_router::router;
use serde_json::{Value, json};

use crate::common::*;

#[tokio::test]
async fn configured_discovery_refreshes_native_metadata_and_isolates_source_headers() {
    async fn source(State(seen): State<Arc<Mutex<Vec<Value>>>>, request: Request) -> Json<Value> {
        let path = request.uri().path().to_owned();
        seen.lock().unwrap().push(json!({
            "path":path,
            "headers":request.headers().iter().filter_map(|(key,value)| value.to_str().ok().map(|value|(key.as_str(),value))).collect::<std::collections::BTreeMap<_,_>>()
        }));
        if path == "/models" {
            return Json(json!({"object":"list","data":[]}));
        }
        Json(json!({
            "object": "list",
            "data": [
                {"id":"vendor/one", "nested":{"left":1,"replace":[1,2]}, "limit":10},
                {"id":"vendor/two", "nested":{"left":2}, "limit":20}
            ],
            "description":"source envelope",
            "extensions":{"kept":true}
        }))
    }

    let fixture = Fixture::new().await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let remote = serve(Router::new().fallback(source).with_state(seen.clone())).await;
    let config = parse_config(
        &json!({"clients":{"openai":{"provider":"anchor","format":"openai"}},"providers": {
            "anchor": {"base_url":remote.url,"auth":"none","catalog":{"path":"/models","format":"openai"}},
            "custom": {
                "base_url":remote.url,"auth":"none",
                "catalog":{"path":"/custom/models","format":"openai"},
                "http_headers":{"x-source-key":"configured"},
                "defaults":{"nested":{"right":3,"replace":[9]},"limit":99,"added":true},
                "models":{
                    "vendor/one":{"nested":{"left":7,"replace":[8,8]},"limit":100}
                }
            }
        }, "routes":[
            {"method":"GET","path":"/models","action":"catalog"},
            {"action":"model"}
        ]})
        .to_string(),
        fixture.root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;

    let response = fixture
        .client
        .get(gateway.url.clone() + "/models")
        .header("authorization", "Bearer client-secret")
        .header("cookie", "client-cookie")
        .header("x-client-only", "private")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let catalog: Value = response.json().await.unwrap();
    // The client profile anchor owns the response envelope; extra source metadata
    // merges into its model array without replacing or extending that envelope.
    assert!(catalog.get("description").is_none());
    assert!(catalog.get("extensions").is_none());
    let first = catalog["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["id"] == "vendor/one")
        .unwrap();
    assert_eq!(
        first,
        &json!({"id":"vendor/one","nested":{"left":7,"right":3,"replace":[8,8]},"limit":100,"added":true})
    );
    let second = catalog["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["id"] == "vendor/two")
        .unwrap();
    assert_eq!(
        second,
        &json!({"id":"vendor/two","nested":{"left":2,"right":3,"replace":[9]},"limit":20,"added":true})
    );

    let requests = seen.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let custom = requests
        .iter()
        .find(|request| request["path"] == "/custom/models")
        .unwrap();
    let headers = custom["headers"].as_object().unwrap();
    assert_eq!(headers["x-source-key"], "configured");
    for leaked in ["authorization", "cookie", "x-client-only"] {
        assert!(
            !headers.contains_key(leaked),
            "client header {leaked} leaked into discovery request"
        );
    }
}

#[derive(Clone)]
struct CatalogSource {
    models: Arc<Mutex<Vec<Value>>>,
    status: Arc<AtomicU16>,
    seen: Arc<Mutex<Vec<Value>>>,
}

async fn catalog_source(State(source): State<CatalogSource>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    source.seen.lock().unwrap().push(json!({
        "path":path,
        "headers":request.headers().iter().filter_map(|(key,value)| value.to_str().ok().map(|value|(key.as_str(),value))).collect::<std::collections::BTreeMap<_,_>>()
    }));
    let configured_status = if path == "/models" || path == "/custom/models" {
        source.status.load(Ordering::SeqCst)
    } else {
        200
    };
    let status = StatusCode::from_u16(configured_status).unwrap();
    if !status.is_success() {
        return (status, Json(json!({"error":{"message":"source failed"}}))).into_response();
    }
    let value = if path == "/models" {
        json!({"models":[{
            "slug":"gpt-6.1-sol", "display_name":"GPT-6.1 Sol",
            "model_messages":{"instructions_template":"You are Codex, an agent based on GPT-6.1. Follow the user's instructions."},
            "supported_reasoning_levels":[{" effort":"high"}], "shell_type":"shell_command"
        },{
            "slug":"gpt-6-luna", "display_name":"GPT-6 Luna",
            "model_messages":{"instructions_template":"You are Codex, an agent based on GPT-6.0."},
            "supported_reasoning_levels":[{"effort":"low"}], "shell_type":"shell_command"
        }]})
    } else {
        json!({"data":source.models.lock().unwrap().clone()})
    };
    Json(value).into_response()
}

#[tokio::test]
async fn openai_sources_project_to_codex_only_with_explicit_native_inheritance() {
    let fixture = Fixture::new().await;
    let source = CatalogSource {
        models: Arc::new(Mutex::new(vec![
            json!({"id":"vendor/one","display_name":"Vendor One","max_model_len":32000}),
            json!({"id":"vendor/two","display_name":"Vendor Two"}),
        ])),
        status: Arc::new(AtomicU16::new(200)),
        seen: Arc::new(Mutex::new(Vec::new())),
    };
    let server = serve(
        Router::new()
            .fallback(catalog_source)
            .with_state(source.clone()),
    )
    .await;
    let config = parse_config(&json!({"clients":{"openai":{"provider":"anchor","format":"codex"}},"providers":{
        "anchor":{"base_url":server.url,"auth":"none","catalog":{"path":"/models","format":"codex"}},
        "custom":{
            "base_url":server.url,"auth":"none",
            "catalog":{"path":"/custom/models","format":"openai"},
            "defaults":{"extends":{"provider":"anchor","model":"gpt-6-luna"},
            },
            "models":{"vendor/one":{
                "extends":{"provider":"anchor","model":"gpt-6.1-sol"},
                "name":"Configured name","description":"Shared description",
                    "reasoning_level":"unlisted-effort",
                    "reasoning_levels":[{"level":"low","description":"Quick","extra":"retained"}],
            },"vendor/two":{"extends":{"provider":"anchor","model":"gpt-6.1-sol"}},
            "unchecked-limit":{"extends":{"provider":"anchor","model":"gpt-6.1-sol"}}}
        }
    },"routes":[
        {"method":"GET","path":"/models","action":"catalog"},
        {"action":"model"}
    ]}).to_string(),fixture.root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let catalog: Value = fixture
        .client
        .get(gateway.url.clone() + "/models")
        .header("cookie", "client-cookie")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let generated = catalog["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "vendor/one")
        .unwrap();
    assert_eq!(generated["display_name"], "Configured name");
    assert_eq!(generated["description"], "Shared description");
    assert_eq!(generated["default_reasoning_level"], "unlisted-effort");
    assert_eq!(
        generated["supported_reasoning_levels"],
        json!([{"effort":"low","description":"Quick","extra":"retained"}])
    );
    assert_eq!(
        generated["model_messages"]["instructions_template"],
        "You are Codex, an agent based on Configured name, not GPT. Follow the user's instructions."
    );
    assert_eq!(generated["max_context_window"], 32000);
    assert!(generated.get("priority").is_none());
    assert!(generated.get("support_verbosity").is_none());
    assert!(generated.get("id").is_none());
    let refreshed: Value = fixture
        .client
        .get(gateway.url.clone() + "/models")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(refreshed["models"], catalog["models"]);

    let route = |model: &str| {
        fixture
            .client
            .post(gateway.url.clone() + "/responses")
            .json(&json!({"model":model}))
    };
    assert_eq!(
        route("vendor/one").send().await.unwrap().status(),
        StatusCode::OK
    );
    *source.models.lock().unwrap() = vec![json!({"id":"vendor/two","display_name":"Vendor Two"})];
    assert_eq!(
        fixture
            .client
            .get(gateway.url.clone() + "/models")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        route("vendor/two").send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        route("vendor/one").send().await.unwrap().status(),
        StatusCode::OK
    );
    *source.models.lock().unwrap() = vec![json!({"id":7})];
    assert_eq!(
        fixture
            .client
            .get(gateway.url.clone() + "/models")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        route("vendor/two").send().await.unwrap().status(),
        StatusCode::OK
    );
    *source.models.lock().unwrap() = vec![json!({"id":"unchecked-limit","max_model_len":0})];
    let response = fixture
        .client
        .get(gateway.url.clone() + "/models")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let catalog: Value = response.json().await.unwrap();
    let unchecked = catalog["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "unchecked-limit")
        .unwrap();
    assert_eq!(unchecked["display_name"], "unchecked-limit");
    assert!(
        unchecked["model_messages"]["instructions_template"]
            .as_str()
            .unwrap()
            .contains("based on unchecked-limit, not GPT.")
    );
    assert!(unchecked.get("visibility").is_none());
    assert_eq!(
        route("vendor/two").send().await.unwrap().status(),
        StatusCode::OK
    );
    source.status.store(503, Ordering::SeqCst);
    assert_eq!(
        fixture
            .client
            .get(gateway.url.clone() + "/models")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        route("vendor/two").send().await.unwrap().status(),
        StatusCode::OK
    );

    let seen = source.seen.lock().unwrap();
    let custom = seen
        .iter()
        .find(|request| request["path"] == "/custom/models")
        .unwrap();
    assert!(
        !custom["headers"]
            .as_object()
            .unwrap()
            .contains_key("cookie")
    );
}

#[derive(Clone)]
struct OwnershipSource(Arc<Mutex<Vec<Value>>>);

async fn ownership_source(State(models): State<OwnershipSource>, request: Request) -> Json<Value> {
    if request.uri().path() == "/models" {
        Json(json!({"data":[{"id":"anchor-model"}]}))
    } else {
        assert!(request.headers().get("authorization").is_none());
        Json(json!({"data":models.0.lock().unwrap().clone()}))
    }
}

#[tokio::test]
async fn ownership_conflict_rolls_back_entire_refresh_and_cross_provider_passthrough_is_anonymous()
{
    let fixture = Fixture::new().await;
    let models = Arc::new(Mutex::new(vec![json!({"id":"custom-model"})]));
    let server = serve(
        Router::new()
            .fallback(ownership_source)
            .with_state(OwnershipSource(models.clone())),
    )
    .await;
    let config = parse_config(&json!({"clients":{"openai":{"format":"openai"}},"providers":{
        "anchor":{"base_url":server.url,"auth":"none","catalog":{"path":"/models","format":"openai"}},
        "custom":{"base_url":server.url,"auth":"none","catalog":{"path":"/custom/models","format":"openai"}}
    },"routes":[
        {"method":"GET","path":"/models","action":"catalog"},
        {"action":"model"}
    ]}).to_string(),fixture.root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let list = || fixture.client.get(gateway.url.clone() + "/models");
    let route = |model: &str| {
        fixture
            .client
            .post(gateway.url.clone() + "/responses")
            .json(&json!({"model":model}))
    };
    assert_eq!(list().send().await.unwrap().status(), StatusCode::OK);
    assert_eq!(
        route("custom-model").send().await.unwrap().status(),
        StatusCode::OK
    );
    *models.lock().unwrap() = vec![
        json!({"id":"anchor-model"}),
        json!({"id":"uncommitted-model"}),
    ];
    assert_eq!(
        list().send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    for model in ["anchor-model", "custom-model"] {
        assert_eq!(route(model).send().await.unwrap().status(), StatusCode::OK);
    }
    assert_eq!(
        route("uncommitted-model").send().await.unwrap().status(),
        StatusCode::NOT_FOUND
    );

    *models.lock().unwrap() = vec![json!({"id":"custom-model"})];
    let passthrough = parse_config(&json!({"clients":{"openai":{"format":"openai"}},"providers":{
        "anchor":{"base_url":server.url,"auth":"none","catalog":{"path":"/models","format":"openai"}},
        "custom":{"base_url":server.url,"auth":"passthrough","catalog":{"path":"/custom/models","format":"openai"}}
    },"routes":[
        {"method":"GET","path":"/models","action":"catalog"},
        {"action":"model"}
    ]}).to_string(),fixture.root.path()).unwrap();
    let gateway = serve(router(passthrough).unwrap()).await;
    assert_eq!(
        fixture
            .client
            .get(gateway.url.clone() + "/models")
            .header("authorization", "Bearer client-secret")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn explicitly_qualified_inheritance_rejects_a_donor_owned_by_another_provider() {
    let fixture = Fixture::new().await;
    let models = Arc::new(Mutex::new(vec![
        json!({"id":"custom-model"}),
        json!({"id":"alias/model"}),
    ]));
    let server = serve(
        Router::new()
            .fallback(ownership_source)
            .with_state(OwnershipSource(models)),
    )
    .await;
    let config = parse_config(&json!({"clients":{"openai":{"format":"openai"}},"providers":{
        "anchor":{"base_url":server.url,"auth":"none","catalog":{"path":"/models","format":"openai"}},
        "custom":{"base_url":server.url,"auth":"none",
            "catalog":{"path":"/custom/models","format":"openai"},
            "models":{"alias/model":{"extends":{"provider":"anchor","model":"custom-model"}}}}
    },"routes":[
        {"method":"GET","path":"/models","action":"catalog"},
        {"action":"model"}
    ]}).to_string(),fixture.root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    assert_eq!(
        fixture
            .client
            .get(gateway.url.clone() + "/models")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_GATEWAY
    );
    for model in ["anchor-model", "custom-model", "alias/model"] {
        assert_eq!(
            fixture
                .client
                .post(gateway.url.clone() + "/responses")
                .json(&json!({"model":model}))
                .send()
                .await
                .unwrap()
                .status(),
            // Explicit provider ownership is usable even before catalog metadata validates.
            if model == "alias/model" {
                StatusCode::OK
            } else {
                StatusCode::NOT_FOUND
            }
        );
    }
}

#[derive(Clone)]
struct TemplateSource {
    models: Arc<Mutex<Vec<Value>>>,
    donor: Arc<Mutex<Value>>,
}

async fn template_source(State(source): State<TemplateSource>, request: Request) -> Response {
    let path = request.uri().path();
    if path == "/models" {
        return Json(json!({"models":[source.donor.lock().unwrap().clone()]})).into_response();
    }
    if path == "/openai/models" {
        return Json(json!({"data":source.models.lock().unwrap().clone()})).into_response();
    }
    Json(json!({"ok":true})).into_response()
}

#[tokio::test]
async fn explicit_inheritance_tracks_remote_donor_updates_and_missing_donor_rolls_back() {
    let fixture = Fixture::new().await;
    let donor = Arc::new(Mutex::new(json!({
        "slug":"gpt-6-luna", "display_name":"GPT-6 Luna",
        "model_messages":{"instructions_template":"You are Codex, an agent based on GPT-6.0. Origin instructions."},
        "supported_reasoning_levels":[{"effort":"high"}], "shell_type":"shell_command",
        "opaque_extension":{"generation":1,"kept":true}
    })));
    let source = TemplateSource {
        models: Arc::new(Mutex::new(vec![
            json!({"id":"vendor/model","display_name":"Vendor Model"}),
        ])),
        donor: donor.clone(),
    };
    let server = serve(
        Router::new()
            .fallback(template_source)
            .with_state(source.clone()),
    )
    .await;
    let config = parse_config(&json!({"clients":{"openai":{"provider":"anchor","format":"codex"}},"providers":{
        "anchor":{"base_url":server.url,"auth":"none","catalog":{"path":"/models","format":"codex"}},
        "remote":{"base_url":server.url,"auth":"none","catalog":{"path":"/openai/models","format":"openai"},
            "models":{"vendor/model":{"extends":{"provider":"anchor","model":"gpt-6-luna"}},
                "broken/model":{"extends":{"provider":"anchor","model":"missing-template"}}}}
    },"routes":[
        {"method":"GET","path":"/models","action":"catalog"},
        {"action":"model"}
    ]}).to_string(),fixture.root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let list = || fixture.client.get(gateway.url.clone() + "/models");
    let route = |model: &str| {
        fixture
            .client
            .post(gateway.url.clone() + "/responses")
            .json(&json!({"model":model}))
    };

    let catalog: Value = list().send().await.unwrap().json().await.unwrap();
    let inherited = catalog["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "vendor/model")
        .unwrap();
    assert_eq!(
        inherited["model_messages"]["instructions_template"],
        "You are Codex, an agent based on Vendor Model, not GPT. Origin instructions."
    );
    assert_eq!(
        inherited["opaque_extension"],
        json!({"generation":1,"kept":true})
    );
    assert_eq!(inherited["slug"], "vendor/model");

    donor.lock().unwrap()["opaque_extension"]["generation"] = json!(2);
    let updated: Value = list().send().await.unwrap().json().await.unwrap();
    let inherited = updated["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "vendor/model")
        .unwrap();
    assert_eq!(inherited["opaque_extension"]["generation"], 2);

    *source.models.lock().unwrap() = vec![
        json!({"id":"vendor/model","display_name":"Vendor Model"}),
        json!({"id":"broken/model","display_name":"Broken Model"}),
    ];
    assert_eq!(
        list().send().await.unwrap().status(),
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        route("broken/model").send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        route("vendor/model").send().await.unwrap().status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn file_backed_catalog_origin_uses_startup_metadata_without_reading_file_or_network() {
    let fixture = Fixture::new().await;
    let file = fixture.root.path().join("origin.json");
    std::fs::write(
        &file,
        json!({"data":[{"id":"file-origin","name":"Cached model"}],"description":"discarded envelope"}).to_string(),
    ).unwrap();
    let config = parse_config(&json!({"clients":{"openai":{"provider":"origin","format":"openai"}},"providers":{
        "origin":{"base_url":"http://127.0.0.1:1","auth":"none","catalog":{"file":"origin.json","format":"openai"}}
    },"routes":[
        {"method":"GET","path":"/models","action":"catalog"},
        {"action":"model"}
    ]}).to_string(),fixture.root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    std::fs::remove_file(file).unwrap();
    let response = fixture
        .client
        .get(gateway.url.clone() + "/models")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let catalog: Value = response.json().await.unwrap();
    assert_eq!(
        catalog["data"],
        json!([{"id":"file-origin","display_name":"Cached model"}])
    );
    assert_eq!(catalog["object"], "list");
    assert_eq!(catalog.as_object().unwrap().len(), 2);
}

async fn file_donor_source(request: Request) -> Response {
    match request.uri().path() {
        "/codex/models" => Json(json!({"models":[]})).into_response(),
        "/openai/models" => Json(json!({"data":[{"id":"derived/model"}]})).into_response(),
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

#[tokio::test]
async fn file_donor_changes_are_visible_after_restart() {
    let fixture = Fixture::new().await;
    let server = serve(Router::new().fallback(file_donor_source)).await;
    let donor_file = fixture.root.path().join("donor.json");
    let write_donor = |generation| {
        std::fs::write(
            &donor_file,
            json!({"models":[{
                "slug":"file-donor","display_name":"File donor",
                "opaque_extension":{"generation":generation}
            }]})
            .to_string(),
        )
        .unwrap();
    };
    write_donor(1);
    let document = json!({"clients":{"openai":{"provider":"origin","format":"codex"}},"providers":{
        "origin":{"base_url":server.url,"auth":"none","catalog":{"path":"/codex/models","format":"codex"}},
        "remote":{"base_url":server.url,"auth":"none","catalog":{"path":"/openai/models","format":"openai"},
            "models":{"derived/model":{"extends":{"provider":"file","model":"file-donor"}}}},
        "file":{"base_url":"http://127.0.0.1:1","auth":"none","catalog":{"file":"donor.json","format":"codex"}}
    },"routes":[
        {"method":"GET","path":"/models","action":"catalog"},
        {"action":"model"}
    ]});
    let config = || parse_config(&document.to_string(), fixture.root.path()).unwrap();
    let start = || serve(router(config()).unwrap());
    let gateway = start().await;
    let get_catalog = |url: String| async {
        fixture
            .client
            .get(url + "/models")
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()
    };
    let catalog = get_catalog(gateway.url.clone()).await;
    let derived = catalog["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "derived/model")
        .unwrap();
    assert_eq!(derived["opaque_extension"]["generation"], 1);

    write_donor(2);
    let same_process: Value = get_catalog(gateway.url.clone()).await;
    let derived = same_process["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "derived/model")
        .unwrap();
    assert_eq!(derived["opaque_extension"]["generation"], 1);
    let restarted = start().await;
    let after_restart: Value = get_catalog(restarted.url.clone()).await;
    let derived = after_restart["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["slug"] == "derived/model")
        .unwrap();
    assert_eq!(derived["opaque_extension"]["generation"], 2);
}

#[derive(Clone)]
struct RejectedOriginState {
    auxiliary_started: Arc<tokio::sync::Notify>,
    auxiliary_succeeds: bool,
}

async fn rejected_origin_source(
    State(state): State<RejectedOriginState>,
    request: Request,
) -> Response {
    if request.uri().path() == "/responses" {
        return StatusCode::OK.into_response();
    }
    if request.uri().path() == "/custom/models" {
        state.auxiliary_started.notify_one();
        if state.auxiliary_succeeds {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            return Json(json!({"data":[{"id":"auxiliary-model"}],"auxiliary_only":true}))
                .into_response();
        }
        return (StatusCode::SERVICE_UNAVAILABLE, "auxiliary failed").into_response();
    }

    state.auxiliary_started.notified().await;
    let mut response = (StatusCode::TOO_MANY_REQUESTS, "origin rate limited").into_response();
    response
        .headers_mut()
        .insert("retry-after", "17".parse().unwrap());
    response
}

#[tokio::test]
async fn rejected_catalog_origin_uses_successful_auxiliary_or_preserves_its_error() {
    for auxiliary_succeeds in [false, true] {
        let fixture = Fixture::new().await;
        let server = serve(Router::new().fallback(rejected_origin_source).with_state(
            RejectedOriginState {
                auxiliary_started: Arc::new(tokio::sync::Notify::new()),
                auxiliary_succeeds,
            },
        ))
        .await;
        let config = parse_config(
            &json!({"clients":{"openai":{"provider":"origin","format":"openai"}},"providers":{
                "origin":{"base_url":server.url,"auth":"none","catalog":{"path":"/models","format":"openai"}},
                "auxiliary":{"base_url":server.url,"auth":"none","catalog":{"path":"/custom/models","format":"openai"}}
            },"routes":[
                {"method":"GET","path":"/models","action":"catalog"},
                {"action":"model"}
            ]}).to_string(),
            fixture.root.path(),
        )
        .unwrap();
        let gateway = serve(router(config).unwrap()).await;
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            fixture.client.get(gateway.url.clone() + "/models").send(),
        )
        .await
        .expect("catalog did not collect the finite auxiliary response")
        .unwrap();
        if auxiliary_succeeds {
            assert_eq!(response.status(), StatusCode::OK);
            assert!(response.headers().get("retry-after").is_none());
            let document: Value = response.json().await.unwrap();
            assert_eq!(document["data"], json!([{"id":"auxiliary-model"}]));
            assert_eq!(document["auxiliary_only"], true);
            assert_eq!(
                fixture
                    .client
                    .post(gateway.url.clone() + "/responses")
                    .json(&json!({"model":"auxiliary-model"}))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::OK
            );
        } else {
            assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(response.headers()["retry-after"], "17");
            assert_eq!(response.text().await.unwrap(), "origin rate limited");
        }
    }
}

#[derive(Clone)]
struct FailedAuxiliaryGroupState {
    first_started: Arc<tokio::sync::Notify>,
}

async fn failed_auxiliary_group_source(
    State(state): State<FailedAuxiliaryGroupState>,
    request: Request,
) -> Response {
    match request.uri().path() {
        "/models" => Json(json!({"data":[{"id":"origin-model"}]})).into_response(),
        "/first/models" => {
            state.first_started.notify_one();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Json(json!({"data":[{"id":"first-model"}]})).into_response()
        }
        "/second/models" => {
            state.first_started.notified().await;
            (StatusCode::SERVICE_UNAVAILABLE, "second auxiliary failed").into_response()
        }
        "/responses" => StatusCode::OK.into_response(),
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

#[tokio::test]
async fn failed_auxiliary_does_not_cancel_delayed_successful_sources() {
    let fixture = Fixture::new().await;
    let server = serve(
        Router::new()
            .fallback(failed_auxiliary_group_source)
            .with_state(FailedAuxiliaryGroupState {
                first_started: Arc::new(tokio::sync::Notify::new()),
            }),
    )
    .await;
    let config = parse_config(
        &json!({"clients":{"openai":{"provider":"origin","format":"openai"}},"providers":{
            "origin":{"base_url":server.url,"auth":"none","catalog":{"path":"/models","format":"openai"}},
            "first":{"base_url":server.url,"auth":"none","catalog":{"path":"/first/models","format":"openai"}},
            "second":{"base_url":server.url,"auth":"none","catalog":{"path":"/second/models","format":"openai"}}
        },"routes":[
            {"method":"GET","path":"/models","action":"catalog"},
            {"action":"model"}
        ]}).to_string(),
        fixture.root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        fixture.client.get(gateway.url.clone() + "/models").send(),
    )
    .await
    .expect("catalog did not collect the finite successful sources")
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let document: Value = response.json().await.unwrap();
    assert_eq!(
        document["data"],
        json!([{"id":"first-model"},{"id":"origin-model"}])
    );
    for model in ["origin-model", "first-model"] {
        assert_eq!(
            fixture
                .client
                .post(gateway.url.clone() + "/responses")
                .json(&json!({"model":model}))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }
}

#[derive(Clone)]
struct InvalidNestedSource(Arc<Mutex<Value>>);

async fn invalid_nested_source(
    State(source): State<InvalidNestedSource>,
    request: Request,
) -> Json<Value> {
    if request.uri().path() == "/models" {
        Json(json!({"data":[]}))
    } else {
        Json(json!({"data":[source.0.lock().unwrap().clone()]}))
    }
}

#[tokio::test]
async fn metadata_override_survives_invalid_source_values_and_cached_file_removal() {
    let fixture = Fixture::new().await;
    let cached_file = fixture.root.path().join("cached.json");
    std::fs::write(
        &cached_file,
        json!({"data":[
            {"id":"file-null","nested":null},
            {"id":"file-scalar","nested":"source"},
            {"id":"file-array","nested":[1,2]}
        ]})
        .to_string(),
    )
    .unwrap();
    let source_model = Arc::new(Mutex::new(json!({"id":"remote-model","nested":null})));
    let server = serve(
        Router::new()
            .fallback(invalid_nested_source)
            .with_state(InvalidNestedSource(source_model.clone())),
    )
    .await;
    let config = parse_config(
        &json!({"clients":{"openai":{"provider":"origin","format":"openai"}},"providers":{
            "origin":{"base_url":server.url,"auth":"none","catalog":{"path":"/models","format":"openai"}},
            "remote":{"base_url":server.url,"auth":"none","catalog":{"path":"/remote/models","format":"openai"},
                "defaults":{"nested":{"a":1}},"models":{"remote-model":{"nested":{"b":2}}}},
            "cached":{"base_url":"http://127.0.0.1:1","auth":"none","catalog":{"file":"cached.json","format":"openai"},
                "defaults":{"nested":{"a":1}},"models":{
                    "file-null":{"nested":{"b":2}},"file-scalar":{"nested":{"b":2}},"file-array":{"nested":{"b":2}}
                }}
        },"routes":[
            {"method":"GET","path":"/models","action":"catalog"},
            {"action":"model"}
        ]}).to_string(),
        fixture.root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    std::fs::remove_file(cached_file).unwrap();

    for invalid_source in [Value::Null, json!("source"), json!([1, 2])] {
        source_model.lock().unwrap()["nested"] = invalid_source;
        let response = fixture
            .client
            .get(gateway.url.clone() + "/models")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let catalog: Value = response.json().await.unwrap();
        for id in ["remote-model", "file-null", "file-scalar", "file-array"] {
            let model = catalog["data"]
                .as_array()
                .unwrap()
                .iter()
                .find(|model| model["id"] == id)
                .unwrap();
            assert_eq!(model["nested"], json!({"b":2}), "model {id}");
        }
    }
}

#[derive(Clone)]
struct NativeCodexSource(Arc<Mutex<Vec<Value>>>);

async fn native_codex_source(State(source): State<NativeCodexSource>) -> Json<Value> {
    Json(json!({"models":source.0.lock().unwrap().clone()}))
}

#[tokio::test]
async fn native_codex_metadata_values_pass_through_unchanged() {
    let fixture = Fixture::new().await;
    let native_model = |context_window: Value, max_context_window: Value| {
        json!({
            "slug":"native-model","display_name":"Native model",
            "model_messages":{"instructions_template":"You are Codex, an agent based on GPT-6.1."},
            "supported_reasoning_levels":[{"effort":"high","description":"High effort"}],
            "shell_type":"shell_command","visibility":"list","supported_in_api":true,
            "priority":1,"support_verbosity":false,
            "truncation_policy":{"mode":"tokens","limit":1000},
            "experimental_supported_tools":[],
            "context_window":context_window,"max_context_window":max_context_window
        })
    };
    let models = Arc::new(Mutex::new(vec![native_model(Value::Null, Value::Null)]));
    let server = serve(
        Router::new()
            .fallback(native_codex_source)
            .with_state(NativeCodexSource(models.clone())),
    )
    .await;
    let config = parse_config(
        &json!({"clients":{"openai":{"provider":"native","format":"codex"}},"providers":{
            "native":{"base_url":server.url,"auth":"none","catalog":{"path":"/models","format":"codex"}}
        },"routes":[
            {"method":"GET","path":"/models","action":"catalog"},
            {"action":"model"}
        ]}).to_string(),
        fixture.root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let refresh = || fixture.client.get(gateway.url.clone() + "/models");

    for (context, maximum) in [
        (Value::Null, Value::Null),
        (json!(4096), Value::Null),
        (Value::Null, json!(8192)),
    ] {
        models.lock().unwrap().clear();
        models.lock().unwrap().push(native_model(context, maximum));
        let response = refresh().send().await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{}",
            response.text().await.unwrap()
        );
        let catalog: Value = response.json().await.unwrap();
        assert_eq!(catalog["models"][0]["display_name"], "Native model");
        assert!(
            catalog["models"][0]
                .get("default_reasoning_level")
                .is_none()
        );
        assert_eq!(
            catalog["models"][0]["supported_reasoning_levels"],
            json!([{"effort":"high","description":"High effort"}])
        );
    }

    for (context, maximum) in [
        (json!(0), Value::Null),
        (json!(-1), Value::Null),
        (json!("4096"), Value::Null),
        (Value::Null, json!(0)),
        (Value::Null, json!(-1)),
        (Value::Null, json!("8192")),
    ] {
        models.lock().unwrap().clear();
        models
            .lock()
            .unwrap()
            .push(native_model(context.clone(), maximum.clone()));
        let response = refresh().send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let catalog: Value = response.json().await.unwrap();
        assert_eq!(catalog["models"][0]["context_window"], context);
        assert_eq!(catalog["models"][0]["max_context_window"], maximum);
    }
}

#[test]
fn invalid_configured_catalog_sources_are_rejected() {
    let root = tempfile::tempdir().unwrap();
    for source in [
        json!({"file":"models.json","path":"/models","format":"openai"}),
        json!({"path":"/models?version=1","format":"openai"}),
        json!({"path":"relative","format":"openai"}),
        json!({"path":"/models","format":"unsupported"}),
    ] {
        let config = json!({"providers":{"api":{"base_url":"http://localhost","auth":"none","catalog":source}},"routes":[{"action":"forward","provider":"api"}]});
        assert!(
            parse_config(&config.to_string(), root.path()).is_err(),
            "accepted catalog source: {source}"
        );
    }
}
