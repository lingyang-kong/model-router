use crate::common::{parse_config, serve};
use axum::{
    Json, Router,
    body::Body,
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, client::IntoClientRequest},
};

fn client() -> reqwest::Client {
    reqwest::Client::builder().no_proxy().build().unwrap()
}
fn handshake(url: &str, model: &str) -> tungstenite::handshake::client::Request {
    let mut request = format!("{}/responses", url.replace("http:", "ws:"))
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("originator", "codex_cli_rs".parse().unwrap());
    request.headers_mut().insert(
        "x-codex-routing-hint",
        format!("model={model}").parse().unwrap(),
    );
    request
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
        &json!({"providers":{
            "a":{"base_url":a.url,"auth":"none","models":{"worker":{}}},
            "b":{"base_url":b.url,"auth":"none","catalog":{"path":"/models","format":"openai"}}
        },"routes":[
            {"path":"/models","action":"catalog","format":"openai","provider":"b"},
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

async fn http_endpoint(State(mode): State<&'static str>, request: Request) -> Response {
    if request.headers().contains_key("upgrade") {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let first = "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}\n\n";
    match mode {
        "uppercase" => Response::builder()
            .header("content-type", "Text/Event-Stream")
            .body(Body::from(format!(
                "{first}data: {{\"type\":\"response.completed\",\"response\":{{\"id\":\"r\"}}}}\n\n"
            )))
            .unwrap(),
        "broken" => {
            let stream = futures_util::stream::once(async move { Ok::<_, std::io::Error>(first) })
                .chain(futures_util::stream::once(async {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    Err(std::io::Error::other("broken stream"))
                }));
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(stream))
                .unwrap()
        }
        _ => Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from_stream(
                futures_util::stream::once(async move { Ok::<_, std::io::Error>(first) })
                    .chain(futures_util::stream::pending()),
            ))
            .unwrap(),
    }
}

async fn fallback_events(mode: &'static str) -> Vec<Value> {
    let backend = serve(Router::new().fallback(http_endpoint).with_state(mode)).await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(&json!({"providers":{"a":{"base_url":backend.url,"auth":"none","models":{"worker":{}}}},"routes":[{"action":"model","provider":"a"}]}).to_string(),root.path()).unwrap();
    let gateway = serve(model_router::router(config).unwrap()).await;
    let (mut ws, _) = connect_async(handshake(&gateway.url, "worker"))
        .await
        .unwrap();
    ws.send(tungstenite::Message::Text(
        json!({"type":"response.create","model":"worker","input":[],"stream_id":"lane"})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut events = vec![];
        while let Some(Ok(message)) = ws.next().await {
            let event: Value = serde_json::from_slice(&message.into_data()).unwrap();
            let done = event["type"] == "error" || event["type"] == "response.completed";
            events.push(event);
            if done {
                break;
            }
        }
        events
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn sse_content_type_is_case_insensitive() {
    let events = fallback_events("uppercase").await;
    assert_eq!(
        events.last().unwrap()["type"],
        "response.completed",
        "{events:?}"
    );
}

#[tokio::test]
async fn broken_http_stream_keeps_stream_id() {
    let events = fallback_events("broken").await;
    assert_eq!(events.last().unwrap()["type"], "error", "{events:?}");
    assert_eq!(events.last().unwrap()["stream_id"], "lane", "{events:?}");
}
