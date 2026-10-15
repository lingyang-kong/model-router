use std::{io::Cursor, sync::Arc, time::Duration};

use async_compression::tokio::bufread::GzipEncoder;
use axum::{
    Json, Router,
    body::{Bytes, to_bytes},
    extract::{Request, State},
    http::StatusCode,
};
use futures_util::StreamExt;
use model_router::router;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, BufReader},
    sync::oneshot,
    time::timeout,
};

use crate::common::*;

#[tokio::test]
async fn client_markers_are_validated_before_forwarding() {
    let fixture = Fixture::new().await;
    for (markers, status) in [
        (vec![], StatusCode::OK),
        (vec!["codex"], StatusCode::OK),
        (vec!["codex", "codex"], StatusCode::OK),
        (vec![""], StatusCode::BAD_REQUEST),
        (vec!["unknown"], StatusCode::BAD_REQUEST),
        (vec!["codex", "unknown"], StatusCode::BAD_REQUEST),
    ] {
        let seen = fixture.live.seen.lock().unwrap().len();
        let mut request = fixture
            .client
            .post(fixture.routed.url.clone() + "/responses")
            .header("x-route-provider", "service-a")
            .json(&json!({"model":"gpt-test"}));
        for marker in markers {
            request = request.header("x-router-client", marker);
        }
        assert_eq!(request.send().await.unwrap().status(), status);
        assert_eq!(
            fixture.live.seen.lock().unwrap().len(),
            seen + usize::from(status.is_success())
        );
    }
}

#[tokio::test]
async fn providers_sharing_an_endpoint_keep_distinct_credential_scopes() {
    let fixture = Fixture::new().await;
    let config = parse_config(
        &json!({"providers":{
            "first":{"base_url":fixture._live_server.url,"auth":"passthrough"},
            "second":{"base_url":fixture._live_server.url,"auth":"passthrough",
                "catalog":{"file":"models.json","format":"codex"}}
        },"routes":[
            {"path":"/direct","action":"forward","provider":"second"},
            {"action":"model","provider":"first"}
        ]})
        .to_string(),
        fixture.root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let seen = fixture.live.seen.lock().unwrap().len();
    let response = fixture
        .client
        .post(gateway.url.clone() + "/responses")
        .bearer_auth("first-token")
        .json(&json!({"model":"publisher/model"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(fixture.live.seen.lock().unwrap().len(), seen);
    for (path, model) in [("/responses", "unknown"), ("/direct", "publisher/model")] {
        let response: Value = fixture
            .client
            .post(gateway.url.clone() + path)
            .bearer_auth("own-token")
            .json(&json!({"model":model}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response["headers"]["authorization"], "Bearer own-token");
    }
}

#[tokio::test]
async fn routes_opaque_ids_and_keeps_request_bytes_and_credentials() {
    let fixture = Fixture::new().await;
    let original = "{ \"model\" : \"publisher/model\", \"input\" : \"Unicode 🐚\" }\n";
    let response = fixture
        .client
        .post(format!("{}/v1/responses?query=1", fixture.routed.url))
        .header("x-route-provider", "service-b")
        .bearer_auth("client-token")
        .header("cookie", "client-cookie")
        .header("chatgpt-account-id", "client-account")
        .header("connection", "x-remove")
        .header("x-remove", "secret")
        .body(original)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response: Value = response.json().await.unwrap();
    assert_eq!(response["provider"], "service-b");
    assert_eq!(response["body"], original);
    assert_eq!(response["path"], "/v1/responses");
    assert_eq!(response["query"], "query=1");
    assert!(response["headers"].get("authorization").is_none());
    assert!(response["headers"].get("cookie").is_none());
    assert!(response["headers"].get("chatgpt-account-id").is_none());
    assert!(response["headers"].get("x-remove").is_none());
    assert_eq!(response["headers"]["x-api-key"], "configured-key");
    let response: Value = fixture
        .post("service-a", "gpt-test")
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(response["provider"], "service-a");
    assert_eq!(response["headers"]["authorization"], "Bearer client-token");
    assert_eq!(response["headers"]["cookie"], "client-cookie");
    assert_eq!(
        fixture
            .post("unknown-provider", "unknown/model")
            .await
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn one_messages_client_can_route_main_and_workers_without_headers() {
    let root = tempfile::tempdir().unwrap();
    let messages = serve(
        Router::new()
            .fallback(mock)
            .with_state(Mock::new("messages", "claude-native")),
    )
    .await;
    let inference = serve(
        Router::new()
            .fallback(mock)
            .with_state(Mock::new("inference", "vendor/native")),
    )
    .await;
    std::fs::write(
        root.path().join("models.json"),
        json!({"data":[{"id":"vendor/native"}]}).to_string(),
    )
    .unwrap();
    let config = parse_config(
        &json!({"providers":{
        "messages":{"base_url":messages.url,"auth":"passthrough"},
        "inference":{"base_url":inference.url,"auth":"none","catalog":{"file":"models.json","format":"anthropic"}}
    },"routes":[{"path_prefix":"/v1/messages","action":"model","provider":"messages"}]})
        .to_string(),
        root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for (model, owner) in [
        ("claude-native", "messages"),
        ("vendor/native", "inference"),
    ] {
        for path in ["/v1/messages", "/v1/messages/count_tokens"] {
            let original = format!("{{ \"model\":\"{model}\",\"messages\":[] }}\n");
            let response: Value = client
                .post(gateway.url.clone() + path)
                .header("x-api-key", "session-key")
                .body(original.clone())
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(response["provider"], owner);
            assert_eq!(response["body"], original);
            if owner == "inference" {
                assert!(response["headers"].get("x-api-key").is_none());
            } else {
                assert_eq!(response["headers"]["x-api-key"], "session-key");
            }
        }
    }
}

#[tokio::test]
async fn compressed_model_requests_route_without_reencoding_the_body() {
    let root = tempfile::tempdir().unwrap();
    let original = br#"{ "model" : "native", "input" : "preserved" }"#;
    let mut encoded = Vec::new();
    GzipEncoder::new(BufReader::new(Cursor::new(original)))
        .read_to_end(&mut encoded)
        .await
        .unwrap();
    async fn receive(request: Request) -> Json<Value> {
        let (parts, body) = request.into_parts();
        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        Json(
            json!({"encoding":parts.headers.get("content-encoding").unwrap().to_str().unwrap(),"bytes":bytes.to_vec()}),
        )
    }
    let backend = serve(Router::new().fallback(receive)).await;
    std::fs::write(
        root.path().join("models.json"),
        json!({"data":[{"id":"native"}]}).to_string(),
    )
    .unwrap();
    let config=parse_config(&json!({"providers":{"api":{"base_url":backend.url,"auth":"none","catalog":{"file":"models.json","format":"openai"}}},"routes":[{"action":"model"}]}).to_string(),root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let response: Value = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(gateway.url.clone() + "/responses")
        .header("content-encoding", "gzip")
        .body(encoded.clone())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["encoding"], "gzip");
    assert_eq!(response["bytes"], json!(encoded));
}

#[tokio::test]
async fn model_routing_waits_for_the_complete_body_without_rewriting_it() {
    use tokio::sync::Notify;
    let arrived = Arc::new(Notify::new());
    async fn receive(State(arrived): State<Arc<Notify>>, request: Request) -> Json<Value> {
        arrived.notify_one();
        let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
        Json(json!({"body":String::from_utf8_lossy(&bytes)}))
    }
    let backend = serve(Router::new().fallback(receive).with_state(arrived.clone())).await;
    let root = tempfile::tempdir().unwrap();
    let config=parse_config(&json!({"providers":{"api":{"base_url":backend.url,"auth":"none"}},"routes":[{"action":"model","provider":"api"}]}).to_string(),root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let (release_tx, release_rx) = oneshot::channel();
    let (started_tx, started_rx) = oneshot::channel();
    let stream = futures_util::stream::once(async move {
        started_tx.send(()).unwrap();
        Ok::<_, std::io::Error>(Bytes::from_static(br#"{ "model":"native", "input":""#))
    })
    .chain(futures_util::stream::once(async move {
        release_rx.await.unwrap();
        Ok::<_, std::io::Error>(Bytes::from_static(b"unchanged\" }\n"))
    }));
    let task = tokio::spawn(async move {
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(gateway.url.clone() + "/responses")
            .body(reqwest::Body::wrap_stream(stream))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()
    });
    started_rx.await.unwrap();
    assert!(
        timeout(Duration::from_millis(50), arrived.notified())
            .await
            .is_err()
    );
    release_tx.send(()).unwrap();
    let response = timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        response["body"],
        "{ \"model\":\"native\", \"input\":\"unchanged\" }\n"
    );
}

#[tokio::test]
async fn headers_select_provider_without_inspecting_model_values() {
    let fixture = Fixture::new().await;
    for (provider, model) in [("service-a", "publisher/model"), ("service-b", "gpt-test")] {
        let response: Value = fixture.post(provider, model).await.json().await.unwrap();
        assert_eq!(response["provider"], provider);
    }
    let response: Value = fixture
        .client
        .post(fixture.routed.url.clone() + "/responses")
        .header("x-route-provider", "service-b")
        .json(&json!({"model":7,"nested":{"model":"ignored"}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["provider"], "service-b");
    assert_eq!(
        fixture
            .client
            .post(fixture.routed.url.clone() + "/responses")
            .header("x-route-provider", "unconfigured")
            .body("arbitrary body")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        fixture
            .client
            .post(fixture.routed.url.clone() + "/responses")
            .body("arbitrary body")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn bearer_keys_are_provider_scoped_and_missing_keys_return_503() {
    let fixture = Fixture::new().await;
    let descriptor = |variable: &str| {
        json!({
            "live":{"base_url":fixture._live_server.url,"auth":"none"},
            "keyed":{"base_url":fixture._manual_server.url,"auth":"bearer","env_key":variable,"catalog":{"file":"models.json","format":"codex"}}
        })
    };
    // Use an existing non-secret variable so tests never mutate process-wide environment.
    let variable = "USER";
    let expected = std::env::var(variable).unwrap();
    let config = test_config(
        descriptor(variable),
        None,
        Some("live"),
        fixture.root.path(),
    );
    let gateway = serve(router(config).unwrap()).await;
    let response: Value = fixture
        .client
        .post(gateway.url.clone() + "/responses")
        .bearer_auth("incoming-session-token")
        .header("cookie", "incoming-cookie")
        .header("x-route-provider", "keyed")
        .json(&json!({"model":"publisher/model"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        response["headers"]["authorization"],
        format!("Bearer {expected}")
    );
    assert!(response["headers"].get("cookie").is_none());
    let config = test_config(
        descriptor("MODEL_ROUTER_TEST_UNCONFIGURED_SECRET"),
        None,
        Some("live"),
        fixture.root.path(),
    );
    let gateway = serve(router(config).unwrap()).await;
    let response = fixture
        .client
        .post(format!("{}/responses", gateway.url))
        .header("x-route-provider", "keyed")
        .json(&json!({"model":"publisher/model"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn metadata_routes_forward_non_model_requests_and_protect_other_credentials() {
    let fixture = Fixture::new().await;
    let response = fixture
        .client
        .get(format!("{}/v1/models", fixture.routed.url))
        .header("x-api-key", "client-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = fixture
        .client
        .post(format!("{}/v1/responses", fixture.routed.url))
        .bearer_auth("foreign-token")
        .header("x-route-provider", "service-b")
        .json(&json!({"model":"gpt-test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response: Value = response.json().await.unwrap();
    assert_eq!(response["provider"], "service-b");
    assert!(response["headers"].get("authorization").is_none());
}

#[tokio::test]
async fn forwarding_rules_support_arbitrary_paths_methods_and_binary_bodies() {
    async fn capture(request: Request) -> Json<Value> {
        let (parts, body) = request.into_parts();
        let body = to_bytes(body, 4096).await.unwrap();
        Json(
            json!({"method":parts.method.as_str(),"path":parts.uri.path(),"query":parts.uri.query(),"bytes":body.to_vec()}),
        )
    }
    let backend = serve(Router::new().fallback(capture)).await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(
        &json!({"providers":{"service":{"base_url":format!("{}/root/",backend.url),"auth":"none","catalog":{"file":"missing.json","format":"codex"}}},"routes":[{"action":"forward","provider":"service"}]}).to_string(),
        root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let response: Value = client
        .get(format!("{}/health?detail=1", gateway.url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["path"], "/root/health");
    assert_eq!(response["query"], "detail=1");
    let response: Value = client
        .get(format!("{}/custom/a%2Fb", gateway.url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["path"], "/root/custom/a%2Fb");
    let bytes = vec![0u8, 255, 3, 13];
    let response: Value = client
        .put(format!("{}/custom/upload", gateway.url))
        .body(bytes.clone())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["method"], "PUT");
    assert_eq!(response["path"], "/root/custom/upload");
    assert_eq!(response["bytes"], json!(bytes));
}

#[tokio::test]
async fn ordered_rules_match_methods_paths_headers_and_queries() {
    async fn backend(request: Request) -> Json<Value> {
        Json(
            json!({"path":request.uri().path(),"query":request.uri().query(),"method":request.method().as_str()}),
        )
    }
    let server = serve(Router::new().fallback(backend)).await;
    let root = tempfile::tempdir().unwrap();
    let config=parse_config(&json!({"providers":{
        "first":{"base_url":format!("{}/first",server.url),"auth":"none"},
        "second":{"base_url":format!("{}/second",server.url),"auth":"none"}
    },"routes":[
        {"method":"GET","path":"/control","headers":{"x-client":"one"},"query":{"kind":"catalog"},"action":"forward","provider":"first"},
        {"method":"GET","path_prefix":"/control","action":"forward","provider":"second"}
    ]}).to_string(),root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let matched: Value = client
        .get(gateway.url.clone() + "/control?kind=catalog")
        .header("X-Client", "one")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(matched["path"], "/first/control");
    let fallback: Value = client
        .get(gateway.url.clone() + "/control?kind=other")
        .header("x-client", "one")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fallback["path"], "/second/control");
    let repeated: Value = client
        .get(gateway.url.clone() + "/control?kind=one&kind=two")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(repeated["query"], "kind=one&kind=two");
    assert_eq!(
        client
            .post(gateway.url.clone() + "/control?kind=catalog")
            .header("x-client", "one")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let invalid = json!({"providers":{"known":{"base_url":server.url,"auth":"none"}},"routes":[{"action":"forward","provider":"unknown"}]});
    assert!(parse_config(&invalid.to_string(), root.path()).is_err());
}
