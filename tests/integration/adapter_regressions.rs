use std::{collections::BTreeMap, sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{FromRequestParts, Request, State, WebSocketUpgrade},
    http::StatusCode,
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use model_router::router;
use serde_json::{Value, json, value::RawValue};
use tokio::{sync::oneshot, time::timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, client::IntoClientRequest},
};

use crate::common::{parse_config, serve};

fn deep_schema(depth: usize) -> String {
    let mut schema = String::from("{\"type\":\"object\",\"properties\":{");
    for _ in 0..depth {
        schema.push_str("\"level\":{\"type\":\"object\",\"properties\":{");
    }
    schema.push_str("\"leaf\":{\"type\":\"string\"}");
    schema.push_str(&"}".repeat(depth * 2 + 2));
    schema
}

fn config_for(upstream: &str, catalog: Option<&str>) -> (tempfile::TempDir, model_router::Config) {
    let root = tempfile::tempdir().unwrap();
    let mut provider = json!({"base_url":upstream,"auth":"none"});
    if let Some(catalog) = catalog {
        std::fs::write(root.path().join("models.json"), catalog).unwrap();
        provider["catalog"] = json!({"file":"models.json","format":"codex"});
    }
    let config = parse_config(
        &json!({"providers":{"api":provider},"routes":[{"action":"model","provider":"api"}]})
            .to_string(),
        root.path(),
    )
    .unwrap();
    (root, config)
}

#[tokio::test]
async fn marked_http_request_with_only_ordinary_deep_schema_forwards_exactly() {
    let (captured_tx, captured_rx) = oneshot::channel::<String>();
    async fn endpoint(
        State(sender): State<Arc<std::sync::Mutex<Option<oneshot::Sender<String>>>>>,
        request: Request,
    ) -> Response {
        let bytes = axum::body::to_bytes(request.into_body(), usize::MAX)
            .await
            .unwrap();
        if let Some(sender) = sender.lock().unwrap().take() {
            let _ = sender.send(String::from_utf8(bytes.to_vec()).unwrap());
        }
        Response::new(Body::from("{}"))
    }
    let upstream = serve(
        Router::new()
            .fallback(endpoint)
            .with_state(Arc::new(std::sync::Mutex::new(Some(captured_tx)))),
    )
    .await;
    let (_root, config) = config_for(&upstream.url, None);
    let gateway = serve(router(config).unwrap()).await;
    let body = format!(
        "{{\"model\":\"gpt-test\",\"input\":\"line\\nvalue\",\"tools\":[{{\"type\":\"function\",\"name\":\"ordinary\",\"parameters\":{}}}]}}",
        deep_schema(140)
    );
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(format!("{}/responses", gateway.url))
        .header("originator", "codex_cli_rs")
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let wire = timeout(Duration::from_secs(2), captured_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(wire, body);
}

#[tokio::test]
async fn marked_http_requests_adapt_collaboration_and_preserve_deep_ordinary_schema() {
    let (captured_tx, captured_rx) = oneshot::channel::<String>();
    async fn endpoint(
        State(sender): State<Arc<std::sync::Mutex<Option<oneshot::Sender<String>>>>>,
        request: Request,
    ) -> Response {
        let bytes = axum::body::to_bytes(request.into_body(), usize::MAX)
            .await
            .unwrap();
        if let Some(sender) = sender.lock().unwrap().take() {
            let _ = sender.send(String::from_utf8(bytes.to_vec()).unwrap());
        }
        Response::new(Body::from("{}"))
    }
    let upstream = serve(
        Router::new()
            .fallback(endpoint)
            .with_state(Arc::new(std::sync::Mutex::new(Some(captured_tx)))),
    )
    .await;
    let (_root, config) = config_for(&upstream.url, None);
    let gateway = serve(router(config).unwrap()).await;
    let deep = deep_schema(140);
    let body = format!(
        "{{\"model\":\"gpt-test\",\"input\":\"line\\nvalue\",\"tools\":[{{\"type\":\"namespace\",\"name\":\"collaboration\",\"tools\":[{{\"type\":\"function\",\"name\":\"spawn_agent\",\"parameters\":{{\"properties\":{{\"message\":{{\"type\":\"string\",\"encrypted\":true}}}}}}}}]}},{{\"type\":\"function\",\"name\":\"ordinary\",\"parameters\":{deep}}}]}}"
    );
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(format!("{}/responses", gateway.url))
        .header("originator", "codex_cli_rs")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let wire = timeout(Duration::from_secs(2), captured_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(wire.contains("\"name\":\"collaboration_compat\""));
    assert!(wire.contains("\"name\":\"ordinary\""));
    assert!(wire.contains("\"input\":\"line\\nvalue\""));
    assert!(wire.contains("\"leaf\":{\"type\":\"string\"}"));
    assert!(!wire.contains("\"encrypted\""));
}

#[derive(Clone)]
struct WsCapture {
    sender: Arc<std::sync::Mutex<Option<oneshot::Sender<String>>>>,
}

async fn websocket_capture(State(state): State<WsCapture>, request: Request) -> Response {
    let (mut parts, _) = request.into_parts();
    let upgrade = WebSocketUpgrade::from_request_parts(&mut parts, &())
        .await
        .unwrap();
    upgrade.on_upgrade(move |mut socket| async move {
        if let Some(Ok(axum::extract::ws::Message::Text(text))) = socket.recv().await {
            if let Some(sender) = state.sender.lock().unwrap().take() {
                let _ = sender.send(text.to_string());
            }
            let _ = socket
                .send(axum::extract::ws::Message::Text("{}".into()))
                .await;
        }
    })
}

#[tokio::test]
async fn marked_websocket_response_create_adapts_collaboration_beside_deep_schema() {
    let (captured_tx, captured_rx) = oneshot::channel::<String>();
    let upstream = serve(
        Router::new()
            .fallback(websocket_capture)
            .with_state(WsCapture {
                sender: Arc::new(std::sync::Mutex::new(Some(captured_tx))),
            }),
    )
    .await;
    let catalog = json!({"models":[{"slug":"publisher/model"}]}).to_string();
    let (_root, config) = config_for(&upstream.url, Some(&catalog));
    let gateway = serve(router(config).unwrap()).await;
    let mut request = (gateway.url.replace("http:", "ws:") + "/responses")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("originator", "codex_cli_rs".parse().unwrap());
    request.headers_mut().insert(
        "x-codex-routing-hint",
        "model=publisher/model".parse().unwrap(),
    );
    let (mut socket, _) = connect_async(request).await.unwrap();
    let deep = deep_schema(140);
    let body = format!(
        "{{\"type\":\"response.create\",\"model\":\"publisher/model\",\"tools\":[{{\"type\":\"function\",\"name\":\"ordinary\",\"parameters\":{deep}}},{{\"type\":\"namespace\",\"name\":\"collaboration\",\"tools\":[{{\"type\":\"function\",\"name\":\"spawn_agent\",\"parameters\":{{\"properties\":{{\"message\":{{\"type\":\"string\",\"encrypted\":true}}}}}}}}]}}]}}"
    );
    socket
        .send(tungstenite::Message::Text(body.into()))
        .await
        .unwrap();
    let wire = timeout(Duration::from_secs(2), captured_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(wire.contains("\"name\":\"collaboration_compat\""));
    assert!(wire.contains("\"name\":\"ordinary\""));
    assert!(wire.contains("\"leaf\":{\"type\":\"string\"}"));
    assert!(!wire.contains("\"encrypted\""));
    socket.close(None).await.unwrap();
}

#[tokio::test]
async fn http_response_restoration_preserves_deep_opaque_output() {
    let deep = format!("{{\"opaque\":{}}}", deep_schema(140));
    let response_body = format!(
        "{{\"object\":\"response\",\"output\":[{{\"type\":\"function_call\",\"namespace\":\"collaboration_compat\",\"name\":\"spawn_agent\",\"call_id\":\"c1\",\"arguments\":\"{{}}\"}},{{\"type\":\"opaque\",\"payload\":{deep}}}]}}"
    );
    async fn endpoint(State(body): State<String>, _request: Request) -> Response {
        Response::builder()
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap()
    }
    let upstream = serve(
        Router::new()
            .fallback(endpoint)
            .with_state(response_body.clone()),
    )
    .await;
    let (_root, config) = config_for(&upstream.url, None);
    let gateway = serve(router(config).unwrap()).await;
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(format!("{}/responses", gateway.url))
        .header("originator", "codex_cli_rs")
        .body(r#"{"model":"gpt-test","input":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let output = response.text().await.unwrap();
    let root: BTreeMap<String, Box<RawValue>> = serde_json::from_str(&output).unwrap();
    let items: Vec<Box<RawValue>> = serde_json::from_str(root["output"].get()).unwrap();
    let function: BTreeMap<String, Box<RawValue>> = serde_json::from_str(items[0].get()).unwrap();
    assert_eq!(
        serde_json::from_str::<String>(function["namespace"].get()).unwrap(),
        "collaboration"
    );
    assert_eq!(
        serde_json::from_str::<String>(function["name"].get()).unwrap(),
        "spawn_agent"
    );
    let opaque: BTreeMap<String, Box<RawValue>> = serde_json::from_str(items[1].get()).unwrap();
    assert_eq!(opaque["payload"].get(), deep);
}

async fn sse_gateway(
    first: impl Into<Bytes>,
    release: oneshot::Receiver<()>,
) -> (crate::common::Server, crate::common::Server) {
    type Release = Arc<tokio::sync::Mutex<Option<oneshot::Receiver<()>>>>;
    let first = first.into();
    let release = Arc::new(tokio::sync::Mutex::new(Some(release)));
    async fn endpoint(
        State((first, release)): State<(Bytes, Release)>,
        _request: Request,
    ) -> Response {
        let stream =
            futures_util::stream::unfold((Some(first), release), |(first, release)| async move {
                if let Some(bytes) = first {
                    return Some((Ok::<_, std::io::Error>(bytes), (None, release)));
                }
                let receiver = release.lock().await.take()?;
                receiver.await.ok()?;
                Some((
                    Ok(Bytes::from_static(b"\n\n")),
                    (None, Arc::new(tokio::sync::Mutex::new(None))),
                ))
            });
        let mut response = Response::new(Body::from_stream(stream));
        response
            .headers_mut()
            .insert("content-type", "text/event-stream".parse().unwrap());
        response
    }
    let upstream = serve(
        Router::new()
            .fallback(endpoint)
            .with_state((first, release)),
    )
    .await;
    let (_root, config) = config_for(&upstream.url, None);
    let gateway = serve(router(config).unwrap()).await;
    (gateway, upstream)
}

#[tokio::test]
async fn sse_comment_and_cr_only_event_are_dispatched_without_waiting_for_lf_or_eof() {
    for comment in [true, false] {
        let first = if comment {
            &b":ping\n"[..]
        } else {
            &b"data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"namespace\":\"collaboration_compat\",\"name\":\"spawn_agent\",\"arguments\":\"{}\"}}\r\r"[..]
        };
        let (release_tx, release_rx) = oneshot::channel();
        let (gateway, _upstream) = sse_gateway(first, release_rx).await;
        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("{}/responses", gateway.url))
            .header("originator", "codex_cli_rs")
            .body(r#"{"model":"gpt-test","input":[]}"#)
            .send()
            .await
            .unwrap();
        let mut stream = response.bytes_stream();
        let mut arrived = Vec::new();
        loop {
            let chunk = timeout(Duration::from_secs(2), stream.next())
                .await
                .expect("SSE item was buffered until more upstream bytes arrived")
                .expect("SSE response ended before dispatch")
                .unwrap();
            arrived.extend_from_slice(&chunk);
            if (comment && arrived.ends_with(b"\n")) || (!comment && arrived.ends_with(b"\n\n")) {
                break;
            }
        }
        if comment {
            assert_eq!(arrived, b":ping\n");
        } else {
            let event = std::str::from_utf8(&arrived).unwrap();
            let data = event
                .strip_prefix("data: ")
                .unwrap()
                .strip_suffix("\n\n")
                .unwrap();
            let value: Value = serde_json::from_str(data).unwrap();
            assert_eq!(value["item"]["namespace"], "collaboration");
        }
        let _ = release_tx.send(());
    }
}

#[tokio::test]
async fn unmarked_sse_preserves_original_line_endings() {
    let first: &[u8] = b"data: one\r: heartbeat\n\ndata: two\n\n";
    let (release_tx, release_rx) = oneshot::channel();
    let (gateway, _upstream) = sse_gateway(first, release_rx).await;
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(format!("{}/responses", gateway.url))
        .body(r#"{"model":"gpt-test","input":[]}"#)
        .send()
        .await
        .unwrap();
    let mut stream = response.bytes_stream();
    let mut arrived = Vec::new();
    while arrived.len() < first.len() {
        let chunk = timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("unmarked SSE bytes were buffered")
            .expect("unmarked SSE response ended early")
            .unwrap();
        arrived.extend_from_slice(&chunk);
    }
    assert_eq!(&arrived[..first.len()], first);
    let _ = release_tx.send(());
}
