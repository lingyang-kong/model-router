use std::{io::Cursor, sync::atomic::Ordering};

use axum::{Json, Router, body::to_bytes, extract::Request, http::StatusCode};
use model_router::router;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, BufReader};

use crate::common::*;

#[tokio::test]
async fn hidden_models_remain_routable_and_cannot_mask_ownership_conflicts() {
    for format in ["codex", "openai", "anthropic"] {
        let fixture = Fixture::new().await;
        let (array, id) = if format == "codex" {
            ("models", "slug")
        } else {
            ("data", "id")
        };
        *fixture.live.models.lock().unwrap() =
            vec![json!({id: "visible-live"}), json!({id: "hidden-live"})];
        std::fs::write(
            fixture.root.path().join("models.json"),
            json!({array: [{id: "visible-manual"}, {id: "hidden-manual"}]}).to_string(),
        )
        .unwrap();
        let config = parse_config(
            &json!({"providers": {
                "live": {
                    "base_url": fixture._live_server.url, "auth": "none",
                    "hidden_models": ["hidden-{live,manual}", "visible-manual"]
                },
                "manual": {
                    "base_url": fixture._manual_server.url, "auth": "none",
                    "catalog": {"file": "models.json", "format": format},
                    "hidden_models": ["h?dden-[mn]*", "visible-live"]
                }
            }, "routes": [
                {"path": "/models", "action": "catalog", "format": format, "provider": "live"},
                {"action": "model"}
            ]})
            .to_string(),
            fixture.root.path(),
        )
        .unwrap();
        let gateway = serve(router(config).unwrap()).await;
        let catalog = || {
            let request = fixture.client.get(gateway.url.clone() + "/models");
            if format == "codex" {
                request
            } else {
                request.header("x-catalog-data", "1")
            }
        };
        let post = |model: &str| {
            fixture
                .client
                .post(gateway.url.clone() + "/responses")
                .json(&json!({"model": model}))
        };
        // A manually configured hidden model works before the first refresh.
        assert_eq!(
            post("hidden-manual").send().await.unwrap().status(),
            StatusCode::OK
        );
        let document: Value = catalog().send().await.unwrap().json().await.unwrap();
        assert_eq!(
            document[array],
            json!([{id: "visible-live"}, {id: "visible-manual"}])
        );
        if format != "codex" {
            assert_eq!(document["extensions"], json!({"opaque": [1, 2]}));
        }
        for (model, provider) in [("hidden-live", "service-a"), ("hidden-manual", "service-b")] {
            let response = post(model).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let response: Value = response.json().await.unwrap();
            assert_eq!(response["provider"], provider);
            assert_eq!(response["body"], json!({"model": model}).to_string());
        }
        *fixture.live.models.lock().unwrap() = vec![
            json!({id: "uncommitted-live"}),
            json!({id: "hidden-manual"}),
        ];
        assert_eq!(
            catalog().send().await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            post("uncommitted-live").send().await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            post("hidden-live").send().await.unwrap().status(),
            StatusCode::OK
        );
    }
}

#[tokio::test]
async fn catalog_responses_extend_routing_without_reading_harness_files() {
    let fixture = Fixture::new().await;
    let cache = fixture.root.path().join("models_cache.json");
    let original = std::fs::read(&cache).unwrap();
    *fixture.live.models.lock().unwrap() = vec![json!({"slug":"gpt-new","context_window":1048576})];
    let response = fixture
        .client
        .get(format!("{}/models?client_version=test", fixture.routed.url))
        .bearer_auth("client-token")
        .header("if-none-match", "old-tag")
        .header("if-modified-since", "old-date")
        .header("x-compress", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("etag").is_none());
    let response: Value = response.json().await.unwrap();
    assert_eq!(response.as_object().unwrap().len(), 1);
    assert!(
        response["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|model| model["slug"] == "gpt-new")
    );
    assert!(
        response["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|model| model["slug"] == "publisher/model")
    );
    assert_eq!(std::fs::read(&cache).unwrap(), original);
    let seen = fixture.live.seen.lock().unwrap().last().unwrap().clone();
    assert!(seen["headers"].get("if-none-match").is_none());
    assert!(seen["headers"].get("if-modified-since").is_none());
    assert_eq!(seen["headers"]["authorization"], "Bearer client-token");
    assert_eq!(seen["query"], "client_version=test");
    assert!(
        seen["query"]
            .as_str()
            .unwrap()
            .contains("client_version=test")
    );
    // New live models are usable before the harness writes its own cache.
    assert_eq!(
        fixture.post("service-a", "gpt-new").await.status(),
        StatusCode::OK
    );
    assert_eq!(
        fixture.post("service-a", "gpt-test").await.status(),
        StatusCode::OK
    );
    std::fs::write(
        &cache,
        json!({"identity":"codex-owned","models":response["models"]}).to_string(),
    )
    .unwrap();
    let response: Value = fixture
        .post("service-b", "publisher/model")
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(response["provider"], "service-b");
    let count = fixture.live.seen.lock().unwrap().len();
    assert_eq!(
        fixture.post("service-a", "gpt-new").await.status(),
        StatusCode::OK
    );
    assert_eq!(fixture.live.seen.lock().unwrap().len(), count + 1);
    assert_eq!(
        fixture.post("service-a", "gpt-test").await.status(),
        StatusCode::OK
    );
    std::fs::remove_file(&cache).unwrap();
    assert_eq!(
        fixture.post("service-b", "publisher/model").await.status(),
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .client
            .post(format!("{}/v1/responses", fixture.routed.url))
            .header("x-route-provider", "service-a")
            .json(&json!({"model":"gpt-new"}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .client
            .get(format!("{}/models", fixture.routed.url))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn catalog_refreshes_use_cached_manual_files_without_affecting_request_routing() {
    let fixture = Fixture::new().await;
    let config = test_config(
        json!({
            "live":{"base_url":fixture._live_server.url,"auth":"none"},
            "manual":{"base_url":fixture._manual_server.url,"auth":"none","catalog":{"file":"models.json","format":"codex"}}
        }),
        None,
        Some("live"),
        fixture.root.path(),
    );
    let gateway = serve(router(config).unwrap()).await;
    let post = |model: &str| {
        fixture
            .client
            .post(gateway.url.clone() + "/responses")
            .header(
                "x-route-provider",
                if model == "publisher/model" {
                    "manual"
                } else {
                    "live"
                },
            )
            .json(&json!({"model":model}))
    };
    // The harness file already contains gpt-test, but the router has not seen a live response.
    assert_eq!(
        post("gpt-test").send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        post("publisher/model").send().await.unwrap().status(),
        StatusCode::OK
    );
    std::fs::write(
        fixture.root.path().join("models_cache.json"),
        "invalid harness cache",
    )
    .unwrap();
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
        post("gpt-test").send().await.unwrap().status(),
        StatusCode::OK
    );
    std::fs::remove_file(fixture.root.path().join("models.json")).unwrap();
    assert_eq!(
        post("publisher/model").send().await.unwrap().status(),
        StatusCode::OK
    );
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
        post("gpt-test").send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        post("publisher/model").send().await.unwrap().status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn conflicting_catalogs_are_rejected_without_changing_header_routes() {
    let fixture = Fixture::new().await;
    *fixture.live.models.lock().unwrap() = vec![
        json!({"slug":"new-live-model"}),
        json!({"slug":"publisher/model"}),
    ];
    let response = fixture
        .client
        .get(fixture.routed.url.clone() + "/models")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        response.json::<Value>().await.unwrap()["error"]["message"]
            .as_str()
            .unwrap()
            .contains("publisher/model")
    );
    assert_eq!(
        fixture.post("service-a", "new-live-model").await.status(),
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .post("service-a", "another-manual-model")
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        fixture.post("service-a", "gpt-test").await.status(),
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .post("service-b", "publisher/model")
            .await
            .json::<Value>()
            .await
            .unwrap()["provider"],
        "service-b"
    );
    // Once the conflicting entry is gone, the entire refresh is accepted.
    *fixture.live.models.lock().unwrap() = vec![json!({"slug":"new-live-model"})];
    assert_eq!(
        fixture
            .client
            .get(fixture.routed.url.clone() + "/models")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        fixture
            .post("service-a", "new-live-model")
            .await
            .json::<Value>()
            .await
            .unwrap()["provider"],
        "service-a"
    );
}

#[tokio::test]
async fn buffered_model_routes_use_native_ids_and_learn_catalog_updates_atomically() {
    let fixture = Fixture::new().await;
    let config = parse_config(&json!({"providers":{
        "live":{"base_url":fixture._live_server.url,"auth":"passthrough"},
        "manual":{"base_url":fixture._manual_server.url,"auth":"none","catalog":{"file":"models.json","format":"codex"}}
    },"routes":[
        {"method":"GET","path":"/models","action":"catalog","format":"codex","provider":"live"},
        {"action":"model"}
    ]}).to_string(),fixture.root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let send = |body: &str| {
        fixture
            .client
            .post(gateway.url.clone() + "/responses")
            .bearer_auth("session-token")
            .header("cookie", "session-cookie")
            .body(body.to_owned())
    };
    let original = "{ \"model\" : \"publisher/model\", \"input\" : \"Unicode 🐚\" }\n";
    let response: Value = send(original).send().await.unwrap().json().await.unwrap();
    assert_eq!(response["provider"], "service-b");
    assert_eq!(response["body"], original);
    assert!(response["headers"].get("authorization").is_none());
    assert!(response["headers"].get("cookie").is_none());
    assert_eq!(
        send(r#"{"model":"gpt-test"}"#)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
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
    let response: Value = send(r#"{"model":"gpt-test"}"#)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["provider"], "service-a");
    assert_eq!(response["headers"]["authorization"], "Bearer session-token");
    *fixture.live.models.lock().unwrap() = vec![
        json!({"slug":"new-model"}),
        json!({"slug":"publisher/model"}),
    ];
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
        send(r#"{"model":"new-model"}"#)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    *fixture.live.models.lock().unwrap() = vec![json!({"slug":"new-model"})];
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
        send(r#"{"model":"new-model"}"#)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        send(r#"{"model":"gpt-test"}"#)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        send(r#"{"model":7}"#).send().await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send(r#"{"nested":{"model":"gpt-test"}}"#)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );

    // Remembered routes must not reintroduce stale live metadata into discovery.
    for live in [
        vec![json!({"slug":"new-model","display_name":"Updated live metadata"})],
        vec![],
    ] {
        *fixture.live.models.lock().unwrap() = live.clone();
        let catalog: Value = fixture
            .client
            .get(gateway.url.clone() + "/models")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let entries = catalog["models"].as_array().unwrap();
        assert_eq!(&entries[..live.len()], live.as_slice());
        assert_eq!(entries.len(), live.len() + 1);
        assert_eq!(entries.last().unwrap()["slug"], "publisher/model");
        assert_eq!(entries.last().unwrap()["display_name"], "Manually managed");
        for model in ["gpt-test", "new-model", "publisher/model"] {
            assert_eq!(
                send(&json!({"model":model}).to_string())
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::OK
            );
        }
    }
}

#[tokio::test]
async fn forwards_compressed_catalog_errors_without_changing_routes() {
    let fixture = Fixture::new().await;
    let raw_client = reqwest::Client::builder()
        .no_proxy()
        .no_gzip()
        .build()
        .unwrap();
    for status in [401, 429] {
        fixture.live.status.store(status, Ordering::SeqCst);
        let response = raw_client
            .get(fixture.routed.url.clone() + "/models")
            .header("x-compress", "1")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status);
        assert_eq!(response.headers()["retry-after"], "17");
        assert_eq!(response.headers()["content-encoding"], "gzip");
        let bytes = response.bytes().await.unwrap();
        let mut decoded = Vec::new();
        async_compression::tokio::bufread::GzipDecoder::new(BufReader::new(Cursor::new(bytes)))
            .read_to_end(&mut decoded)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&decoded).unwrap()["error"]["message"],
            "catalog rejected"
        );
        assert_eq!(
            fixture.post("service-a", "gpt-test").await.status(),
            StatusCode::OK
        );
    }
}

#[tokio::test]
async fn manual_catalog_metadata_reloads_only_after_restart() {
    let fixture = Fixture::new().await;
    let replacement = fixture.root.path().join("replacement.json");
    std::fs::write(
        &replacement,
        json!({"models":[
            {"slug":"publisher/model","display_name":"Edited","context_window":1048576},
            {"slug":"changed/model","display_name":"New"}
        ]})
        .to_string(),
    )
    .unwrap();
    std::fs::rename(&replacement, fixture.root.path().join("models.json")).unwrap();
    let response: Value = fixture
        .client
        .get(fixture.routed.url.clone() + "/models")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let manual = response["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["slug"] == "publisher/model")
        .unwrap();
    assert_eq!(manual["display_name"], "Manually managed");
    assert_eq!(manual["context_window"], 262144);
    assert_eq!(
        fixture.post("service-b", "changed/model").await.status(),
        StatusCode::OK
    );
    let config = test_config(
        json!({
            "live":{"base_url":fixture._live_server.url,"auth":"none"},
            "manual":{"base_url":fixture._manual_server.url,"auth":"none","catalog":{"file":"models.json","format":"codex"}}
        }),
        None,
        Some("live"),
        fixture.root.path(),
    );
    let restarted = serve(router(config).unwrap()).await;
    let response: Value = fixture
        .client
        .get(restarted.url.clone() + "/models")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let manual = response["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["slug"] == "publisher/model")
        .unwrap();
    assert_eq!(manual["display_name"], "Edited");
    assert_eq!(manual["context_window"], 1048576);
    let response: Value = fixture
        .client
        .post(restarted.url.clone() + "/responses")
        .header("x-route-provider", "manual")
        .json(&json!({"model":"changed/model"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["provider"], "service-b");
}

#[test]
fn validates_providers_and_allows_independent_catalog_sources() {
    let root = tempfile::tempdir().unwrap();
    let provider = json!({"base_url":"http://localhost","auth":"none"});
    let cases = [
        json!({"providers":{},"routes":[{"action":"forward","provider":"api"}]}),
        json!({"providers":{"api":{"base_url":"ftp://localhost","auth":"none"}},"routes":[{"action":"forward","provider":"api"}]}),
        json!({"providers":{"api":{"base_url":"http://localhost","auth":"invalid"}},"routes":[{"action":"forward","provider":"api"}]}),
        json!({"providers":{"api":{"base_url":"http://localhost?version=1","auth":"none"}},"routes":[{"action":"forward","provider":"api"}]}),
        json!({"providers":{"api":{"base_url":"http://localhost","auth":"none","catalog_url":"models"}},"routes":[{"action":"forward","provider":"api"}]}),
    ];
    for document in cases {
        assert!(
            parse_config(&document.to_string(), root.path()).is_err(),
            "accepted invalid configuration: {document}"
        );
    }
    for document in [
        json!({"providers":{"first":provider,"second":provider},"routes":[
            {"path":"/models","action":"catalog","format":"codex","provider":"first"},
            {"path":"/other","action":"catalog","format":"codex","provider":"second"}
        ]}),
        json!({"providers":{"api":{"base_url":"http://localhost","auth":"none","catalog":{"file":"models.json","format":"codex"}}},"routes":[{"action":"catalog","format":"codex","provider":"api"}]}),
    ] {
        assert!(parse_config(&document.to_string(), root.path()).is_ok());
    }
}

#[test]
fn authentication_settings_require_credentials_only_for_bearer() {
    let root = tempfile::tempdir().unwrap();
    for (settings, valid) in [
        (json!({"auth":"none"}), true),
        (json!({"auth":"passthrough"}), true),
        (json!({"auth":"bearer","env_key":"UPSTREAM_KEY"}), true),
        (json!({"auth":"bearer"}), false),
        (json!({"auth":"bearer","env_key":""}), false),
        (json!({"auth":"bearer","env_key":null}), false),
        (json!({"auth":"none","env_key":"UNUSED_KEY"}), false),
        (json!({"auth":"passthrough","env_key":"UNUSED_KEY"}), false),
        (json!({"auth":"none","unexpected":true}), false),
        (json!({"auth":"passthrough","unexpected":true}), false),
        (
            json!({"auth":"bearer","env_key":"UPSTREAM_KEY","unexpected":true}),
            false,
        ),
    ] {
        let mut provider = settings.clone();
        provider["base_url"] = json!("http://localhost");
        let config = json!({"providers":{"api":provider},
            "routes":[{"action":"forward","provider":"api"}]});
        let result = parse_config(&config.to_string(), root.path());
        assert_eq!(
            result.is_ok(),
            valid,
            "settings: {settings}; error: {:?}",
            result.err()
        );
    }
}

#[tokio::test]
async fn serves_messages_and_token_counting_without_a_catalog_source() {
    let root = tempfile::tempdir().unwrap();
    let provider = Mock::new("message-service", "claude-test");
    let backend = serve(Router::new().fallback(mock).with_state(provider)).await;
    let local = Mock::new("local-service", "publisher/model");
    let local_backend = serve(Router::new().fallback(mock).with_state(local)).await;
    std::fs::write(
        root.path().join("local.json"),
        json!({"data":[{"id":"publisher/model","display_name":"Local"}]}).to_string(),
    )
    .unwrap();
    let config = test_config(
        json!({
            "messages":{"base_url":backend.url,"auth":"passthrough"},
            "inference":{"base_url":local_backend.url,"auth":"none","catalog":{"file":"local.json","format":"anthropic"}}
        }),
        Some("messages"),
        None,
        root.path(),
    );
    let gateway = serve(router(config).unwrap()).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for (model, endpoint, provider_name) in [
        ("claude-new-model", "messages", "message-service"),
        ("publisher/model", "messages", "local-service"),
        ("publisher/model", "messages/count_tokens", "local-service"),
    ] {
        let body = format!(
            "{{ \"model\": \"{model}\", \"max_tokens\": 16, \"messages\":[{{\"role\":\"user\",\"content\":\"test\"}}], \"thinking\":{{\"type\":\"enabled\",\"budget_tokens\":8}} }}"
        );
        let response: Value = client
            .post(format!("{}/v1/{endpoint}", gateway.url))
            .header(
                "x-route-provider",
                if provider_name == "local-service" {
                    "inference"
                } else {
                    "messages"
                },
            )
            .header("x-api-key", "client-secret")
            .header("anthropic-version", "2023-06-01")
            .header("anthropic-beta", "beta-a,beta-b")
            .body(body.clone())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response["provider"], provider_name);
        assert_eq!(response["path"], format!("/v1/{endpoint}"));
        assert_eq!(response["body"], body);
        assert_eq!(response["headers"]["anthropic-version"], "2023-06-01");
        assert_eq!(response["headers"]["anthropic-beta"], "beta-a,beta-b");
        if provider_name == "local-service" {
            assert!(response["headers"].get("x-api-key").is_none());
        } else {
            assert_eq!(response["headers"]["x-api-key"], "client-secret");
        }
    }
    let response = client
        .post(format!("{}/v1/messages", gateway.url))
        .json(&json!({"model":7}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn invalid_catalog_identifiers_do_not_change_header_routing() {
    let fixture = Fixture::new().await;
    *fixture.live.models.lock().unwrap() =
        vec![json!({"slug":"new-live-model"}), json!({"slug":""})];
    assert_eq!(
        fixture
            .client
            .get(fixture.routed.url.clone() + "/models")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        fixture.post("service-a", "new-live-model").await.status(),
        StatusCode::OK
    );
    assert_eq!(
        fixture.post("service-a", "gpt-test").await.status(),
        StatusCode::OK
    );

    // The selected format's identifier is required; there is no alias fallback.
    std::fs::write(
        fixture.root.path().join("models.json"),
        json!({"models":[{"id":"manual-id"}]}).to_string(),
    )
    .unwrap();
    let config = parse_config(
        &json!({"providers":{
        "manual":{"base_url":fixture._manual_server.url,"auth":"none",
            "catalog":{"file":"models.json","format":"codex"}}
    },"routes":[{"action":"model"}]})
        .to_string(),
        fixture.root.path(),
    )
    .unwrap();
    assert!(router(config).is_err());
}

#[tokio::test]
async fn catalog_requests_forward_native_method_path_query_and_body() {
    async fn endpoint(request: Request) -> Json<Value> {
        let (parts, body) = request.into_parts();
        let body = to_bytes(body, usize::MAX).await.unwrap();
        Json(json!({"models":[{"slug":"rpc-model"}],"request":{
            "method":parts.method.as_str(),"path":parts.uri.path(),"query":parts.uri.query(),
            "body":String::from_utf8_lossy(&body)
        }}))
    }
    let backend = serve(Router::new().fallback(endpoint)).await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(
        &json!({"providers":{
            "api":{"base_url":format!("{}/gateway/",backend.url),"auth":"none"}
        },"routes":[
            {"method":"POST","path":"/v1/models","action":"catalog","format":"codex","provider":"api"},
            {"action":"forward","provider":"api"}
        ]})
        .to_string(),
        root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let query = "q=a%20b&tag=one&tag=two&bare&encoded=%2f";
    let body = "{ \"filter\" : \"all\" }\n";
    let response: Value = client
        .post(format!("{}/v1/models?{query}", gateway.url))
        .body(body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        response["request"],
        json!({"method":"POST","path":"/gateway/v1/models","query":query,"body":body})
    );
    assert_eq!(
        client
            .post(gateway.url.clone() + "/responses")
            .json(&json!({"model":"rpc-model"}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn native_catalog_envelopes_and_manual_fields_are_preserved() {
    let fixture = Fixture::new().await;
    *fixture.live.models.lock().unwrap() =
        vec![json!({"id":"native-model","slug":"display-alias","capabilities":{"tools":true}})];
    // The entry's native fields are supplied by its author, not synthesized.
    std::fs::write(fixture.root.path().join("models.json"),
        json!({"data":[{"id":"publisher/model","display_name":"Native manual","extensions":{"opaque":[1,2]}}]}).to_string()).unwrap();
    let config = parse_config(
        &json!({"providers":{
        "source":{"base_url":fixture._live_server.url,"auth":"none"},
        "manual":{"base_url":fixture._manual_server.url,"auth":"none","catalog":{"file":"models.json","format":"openai"}}
    },"routes":[{"path":"/models","action":"catalog","format":"openai","provider":"source"},{"action":"forward","provider":"source"}]})
        .to_string(),
        fixture.root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let response: Value = fixture
        .client
        .get(gateway.url.clone() + "/models")
        .header("x-catalog-data", "1")
        .header("x-compress", "1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(response.get("models").is_none());
    assert_eq!(response["object"], "list");
    assert_eq!(response["description"], "Mock catalog");
    assert_eq!(response["extensions"], json!({"opaque":[1,2]}));
    assert_eq!(
        response["data"],
        json!([
            {"id":"native-model","slug":"display-alias","capabilities":{"tools":true}},
            {"id":"publisher/model","display_name":"Native manual","extensions":{"opaque":[1,2]}}
        ])
    );
}

#[test]
fn rejects_unsupported_catalog_format() {
    let root = tempfile::tempdir().unwrap();
    let mut descriptor = json!({"providers":{"api":{"base_url":"http://localhost","auth":"none"}},"routes":[{"action":"catalog","format":"codex","provider":"api"}]});
    assert!(parse_config(&descriptor.to_string(), root.path()).is_ok());
    descriptor["routes"][0]["format"] = json!("models");
    assert!(parse_config(&descriptor.to_string(), root.path()).is_err());
}

#[tokio::test]
async fn client_adapters_and_catalog_formats_are_isolated_on_the_same_endpoint() {
    let fixture = Fixture::new().await;
    let mut providers = json!({"live":{"base_url":fixture._live_server.url,"auth":"none"}});
    for (format, key, id_key) in [
        ("codex", "models", "slug"),
        ("openai", "data", "id"),
        ("anthropic", "data", "id"),
    ] {
        let file = format!("{format}.json");
        std::fs::write(
            fixture.root.path().join(&file),
            json!({(key):[{
                (id_key):format!("{format}/manual"),"display_name":"Manual"
            }]})
            .to_string(),
        )
        .unwrap();
        let mut provider = json!({"base_url":fixture._manual_server.url,"auth":"none",
            "catalog":{"file":file,"format":format}});
        if format != "codex" {
            provider["models"] = json!({(format.to_owned() + "/manual"):{
                "extends":{"provider":"live","model":"gpt-6-luna"}
            }});
        }
        providers[format] = provider;
    }
    let config = parse_config(
        &json!({"providers":providers,"routes":[
            {"path":"/models","headers":{"x-catalog-format":"openai"},
             "action":"catalog","provider":"live","format":"openai"},
            {"path":"/models","headers":{"x-catalog-format":"anthropic"},
             "action":"catalog","provider":"live","format":"anthropic"},
            {"path":"/models","action":"catalog","provider":"live","format":"codex"},
            {"action":"model"}
        ]})
        .to_string(),
        fixture.root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    // A later generic request must not inherit metadata changed for Codex.
    for client in ["codex", "openai", "anthropic", "generic", "codex"] {
        let (format, key, id_key) = match client {
            "openai" | "anthropic" => (client, "data", "id"),
            _ => ("codex", "models", "slug"),
        };
        let live = json!({(id_key):format!("live/{client}"),"multi_agent_version":"v2"});
        *fixture.live.models.lock().unwrap() = vec![live.clone()];
        let mut donor = json!({(id_key):"gpt-6-luna", "display_name":"GPT-6 Luna"});
        if format == "codex" {
            donor["model_messages"] = json!({
                "instructions_template":"You are Codex, an agent based on GPT-6.\nTest instructions."
            });
        }
        fixture.live.models.lock().unwrap().push(donor.clone());
        let mut request = fixture
            .client
            .get(gateway.url.clone() + "/models")
            .header("x-catalog-format", format);
        if client == "codex" {
            request = request.header("x-router-client", "codex");
        }
        if key == "data" {
            request = request.header("x-catalog-data", "1");
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response: Value = response.json().await.unwrap();
        let manual = json!({(id_key):format!("{format}/manual"),"display_name":"Manual"});
        if format == "codex" {
            let models = response[key].as_array().unwrap();
            assert_eq!(models[0], live);
            assert!(models.contains(&manual));
            assert_eq!(models.len(), 5);
            for converted in ["openai/manual", "anthropic/manual"] {
                let model = models
                    .iter()
                    .find(|model| model["slug"] == converted)
                    .unwrap();
                assert_eq!(model["multi_agent_version"], Value::Null);
                assert!(
                    model["model_messages"]["instructions_template"]
                        .as_str()
                        .unwrap()
                        .contains("based on Manual, not GPT")
                );
            }
        } else {
            let models = response[key].as_array().unwrap();
            assert_eq!(models[0], live);
            assert!(models.contains(&donor));
            assert!(models.contains(&manual));
            let linked_other = if format == "openai" {
                "anthropic/manual"
            } else {
                "openai/manual"
            };
            assert!(models.iter().any(|model| model[id_key] == linked_other));
            assert_eq!(models.len(), 4);
        }
    }
    // Manual entries excluded from a particular catalog still select their provider.
    for format in ["codex", "openai", "anthropic"] {
        let response: Value = fixture
            .client
            .post(gateway.url.clone() + "/responses")
            .json(&json!({"model":format!("{format}/manual")}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response["provider"], "service-b");
        let file: Value = serde_json::from_slice(
            &std::fs::read(fixture.root.path().join(format!("{format}.json"))).unwrap(),
        )
        .unwrap();
        assert!(!file.to_string().contains("multi_agent_version"));
    }
    // A Codex catalog response cannot silently switch to an OpenAI envelope.
    let response = fixture
        .client
        .get(gateway.url.clone() + "/models")
        .header("x-router-client", "codex")
        .header("x-catalog-data", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    // Client/format mismatches are rejected before the upstream is called.
    let seen = fixture.live.seen.lock().unwrap().len();
    let response = fixture
        .client
        .get(gateway.url.clone() + "/models")
        .header("x-router-client", "codex")
        .header("x-catalog-format", "openai")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(fixture.live.seen.lock().unwrap().len(), seen);
}

#[test]
fn rejects_ambiguous_catalog_and_client_configuration() {
    let root = tempfile::tempdir().unwrap();
    for route in [
        json!({"action":"catalog","provider":"api"}),
        json!({"action":"catalog","provider":"api","format":"openai","client":"codex"}),
        json!({"action":"catalog","provider":"api","format":"codex","model_overrides":{}}),
        json!({"action":"model","client":"unknown"}),
    ] {
        let config = json!({"providers":{"api":{"base_url":"http://localhost","auth":"none"}},
            "routes":[route]});
        assert!(parse_config(&config.to_string(), root.path()).is_err());
    }
    for settings in [
        json!({"catalog":{"file":"models.json"}}),
        json!({"catalog":{"format":"openai"}}),
        json!({"catalog":{"file":"models.json","format":"unknown"}}),
        json!({"catalog":{"file":"models.json","format":"openai","unexpected":true}}),
        json!({"catalog_file":"models.json","catalog_format":"openai"}),
    ] {
        let mut provider = json!({"base_url":"http://localhost","auth":"none"});
        provider
            .as_object_mut()
            .unwrap()
            .extend(settings.as_object().unwrap().clone());
        let config = json!({"providers":{"api":provider},"routes":[{"action":"model"}]});
        assert!(parse_config(&config.to_string(), root.path()).is_err());
    }
}
