use crate::common::{parse_config, serve};
use axum::{Json, Router, body::Body, extract::Request, http::StatusCode, response::Response};
use serde_json::{Value, json};

fn client() -> reqwest::Client {
    reqwest::Client::builder().no_proxy().build().unwrap()
}
#[tokio::test]
async fn explicit_ownership_survives_discovery() {
    let a = serve(Router::new().fallback(|| async { Json(json!({"provider":"a"})) })).await;
    let b = serve(Router::new().fallback(|request: Request| async move {
        if request.uri().path() == "/models" {
            Json(json!({"data":[{"id":"worker"}]}))
        } else {
            Json(json!({"provider":"b"}))
        }
    }))
    .await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(
        &json!({"clients":{"codex":{"provider":"b","format":"openai"},"openai":{"provider":"b","format":"openai"},"anthropic":{"provider":"b","format":"openai"}},"providers":{
            "a":{"base_url":a.url,"auth":"none","models":{"worker":{}}},
            "b":{"base_url":b.url,"auth":"none","catalog":{"path":"/models","format":"openai"}}
        },"routes":[
            {"path":"/models","action":"catalog"},
            {"action":"model"}
        ]})
        .to_string(),
        root.path(),
    )
    .unwrap();
    let gateway = serve(model_router::router(config).unwrap()).await;
    let c = client();
    let first: Value = c
        .post(format!("{}/responses", gateway.url))
        .json(&json!({"model":"worker"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first["provider"], "a");
    let refresh = c
        .get(format!("{}/models", gateway.url))
        .send()
        .await
        .unwrap();
    assert_eq!(refresh.status(), StatusCode::SERVICE_UNAVAILABLE);
    let last: Value = c
        .post(format!("{}/responses", gateway.url))
        .json(&json!({"model":"worker"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        last["provider"],
        "a",
        "refresh status: {}",
        refresh.status()
    );
}

#[tokio::test]
async fn sse_content_type_is_case_insensitive() {
    let event = json!({
        "type": "response.output_item.done",
        "item": {
            "type": "function_call",
            "namespace": "collaboration_compat",
            "name": "collaboration_compat.spawn_agent",
            "arguments": "{\"message\":\"unchanged\"}"
        }
    });
    let body = format!("data: {event}\n\n");
    let backend = serve(Router::new().fallback(move || {
        let body = body.clone();
        async move {
            Response::builder()
                .header("content-type", "Text/Event-Stream")
                .body(Body::from(body))
                .unwrap()
        }
    }))
    .await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(
        &json!({
            "clients":{"codex":{"provider":"a"},"openai":{"provider":"a"},"anthropic":{"provider":"a"}},"providers": {
                "a": {
                    "base_url": backend.url,
                    "auth": "none",
                    "models": {"worker": {}}
                }
            },
            "routes": [{"action": "model"}]
        })
        .to_string(),
        root.path(),
    )
    .unwrap();
    let gateway = serve(model_router::router(config).unwrap()).await;
    let response = client()
        .post(format!("{}/responses", gateway.url))
        .header("originator", "codex_cli_rs")
        .json(&json!({"model": "worker", "input": [], "stream": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "Text/Event-Stream");
    let body = response.text().await.unwrap();
    let restored: Value = serde_json::from_str(
        body.strip_prefix("data: ")
            .unwrap()
            .strip_suffix("\n\n")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(restored["type"], "response.output_item.done");
    assert_eq!(restored["item"]["namespace"], "collaboration");
    assert_eq!(restored["item"]["name"], "spawn_agent");
    assert_eq!(restored["item"]["arguments"], event["item"]["arguments"]);
}
