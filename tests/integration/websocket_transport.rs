use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use crate::common::{Server, parse_config, serve};
use axum::{
    Router,
    body::to_bytes,
    extract::{FromRequestParts, Request, State, WebSocketUpgrade, ws::Message},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use model_router::router;
use serde_json::{Value, json};
use tokio::time::timeout;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, client::IntoClientRequest},
};

#[derive(Clone)]
struct Backend {
    name: &'static str,
    handshake: StatusCode,
    upgrades: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
}
type RecordedRequest = (bool, Option<String>, Value);

#[tokio::test]
async fn original_socket_close_during_switched_response_allows_reconnection() {
    original_socket_close(true).await;
}

#[tokio::test]
async fn original_socket_close_after_switched_response_allows_reconnection() {
    original_socket_close(false).await;
}

async fn original_socket_close(during_response: bool) {
    let close = Arc::new(tokio::sync::Notify::new());
    let finish = Arc::new(tokio::sync::Notify::new());
    let upgrades = Arc::new(AtomicUsize::new(0));
    let primary = serve(Router::new().fallback({
        let close = Arc::clone(&close);
        let upgrades = Arc::clone(&upgrades);
        move |ws: WebSocketUpgrade| {
            let close = Arc::clone(&close);
            let first = upgrades.fetch_add(1, Ordering::Relaxed) == 0;
            async move {
                ws.on_upgrade(move |mut socket| async move {
                    if first {
                        close.notified().await;
                        socket.send(Message::Close(None)).await.unwrap();
                        let _ = socket.recv().await;
                    } else {
                        let _ = socket.recv().await;
                        socket
                            .send(Message::Text(
                                json!({"type":"response.completed"}).to_string().into(),
                            ))
                            .await
                            .unwrap();
                    }
                })
            }
        }
    }))
    .await;
    let worker = serve(Router::new().fallback({
        let finish = Arc::clone(&finish);
        move |ws: WebSocketUpgrade| {
            let finish = Arc::clone(&finish);
            async move {
                ws.on_upgrade(move |mut socket| async move {
                    let _ = socket.recv().await;
                    socket
                        .send(Message::Text(
                            json!({"type":"response.created"}).to_string().into(),
                        ))
                        .await
                        .unwrap();
                    finish.notified().await;
                    socket
                        .send(Message::Text(
                            json!({"type":"response.completed"}).to_string().into(),
                        ))
                        .await
                        .unwrap();
                })
            }
        }
    }))
    .await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(
        &json!({"clients":{"codex":{"provider":"primary"},"openai":{"provider":"primary"},"anthropic":{"provider":"primary"}},"providers":{
        "primary":{"base_url":primary.url,"auth":"none","models":{"primary":{}}},
        "worker":{"base_url":worker.url,"auth":"none","models":{"worker":{}}}
    },"routes":[{"action":"model"}]})
        .to_string(),
        root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let mut request = format!("{}/responses", gateway.url.replace("http:", "ws:"))
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("originator", "codex_cli_rs".parse().unwrap());
    request
        .headers_mut()
        .insert("x-codex-routing-hint", "model=primary".parse().unwrap());
    let (mut socket, _) = connect_async(request).await.unwrap();
    socket
        .send(tungstenite::Message::Text(
            json!({"type":"response.create","model":"worker"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let first = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(first.is_text());
    if during_response {
        close.notify_one();
        assert!(
            timeout(Duration::from_millis(100), socket.next())
                .await
                .is_err()
        );
        // A late control frame for the closed hop must not cancel the active provider.
        socket
            .send(tungstenite::Message::Pong(Vec::new().into()))
            .await
            .unwrap();
    }
    finish.notify_one();
    let completed = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let completed: Value = serde_json::from_slice(&completed.into_data()).unwrap();
    assert_eq!(completed["type"], "response.completed");
    if !during_response {
        close.notify_one();
        assert!(
            timeout(Duration::from_millis(100), socket.next())
                .await
                .is_err()
        );
    }
    assert_eq!(
        turn(&mut socket, "primary").await.last().unwrap()["type"],
        "response.completed"
    );
    assert_eq!(upgrades.load(Ordering::Relaxed), 2);
}

fn events(name: &str, model: &Value) -> Vec<Value> {
    let response = json!({"id":format!("resp_{name}"),"object":"response","status":"completed","model":model,
        "output":[{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":name}]}]});
    vec![
        json!({"type":"response.created","response":{"id":response["id"],"status":"in_progress","output":[]}}),
        json!({"type":"response.output_text.delta","delta":name}),
        json!({"type":"response.output_item.done","output_index":0,"item":response["output"][0]}),
        json!({"type":"response.completed","response":response}),
    ]
}

async fn endpoint(State(state): State<Backend>, request: Request) -> Response {
    let auth = request
        .headers()
        .get("authorization")
        .map(|value| value.to_str().unwrap().to_owned());
    if request.headers().contains_key("upgrade") {
        state.upgrades.fetch_add(1, Ordering::Relaxed);
        if state.handshake != StatusCode::SWITCHING_PROTOCOLS {
            return (
                state.handshake,
                axum::Json(handshake_error(state.handshake)),
            )
                .into_response();
        }
        let (mut parts, _) = request.into_parts();
        return WebSocketUpgrade::from_request_parts(&mut parts, &())
            .await
            .unwrap()
            .on_upgrade(move |mut socket| async move {
                while let Some(Ok(message)) = socket.recv().await {
                    match message {
                        Message::Text(text) => {
                            let body: Value = serde_json::from_str(&text).unwrap();
                            state
                                .requests
                                .lock()
                                .unwrap()
                                .push((true, auth.clone(), body.clone()));
                            for mut event in events(state.name, &body["model"]) {
                                if let Some(stream_id) = body.get("stream_id") {
                                    event["stream_id"] = stream_id.clone();
                                }
                                if socket
                                    .send(Message::Text(event.to_string().into()))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                        Message::Close(frame) => {
                            let _ = socket.send(Message::Close(frame)).await;
                            return;
                        }
                        _ => {}
                    }
                }
            });
    }
    let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    state
        .requests
        .lock()
        .unwrap()
        .push((false, auth, body.clone()));
    axum::Json(json!({"id":"resp_http_retry"})).into_response()
}

struct Fixture {
    gateway: Server,
    _servers: Vec<Server>,
    a: Backend,
    b: Backend,
}
async fn fixture(second: StatusCode) -> Fixture {
    fixture_with_handshakes(StatusCode::SWITCHING_PROTOCOLS, second).await
}

async fn fixture_with_handshakes(first: StatusCode, second: StatusCode) -> Fixture {
    let backend = |name, handshake| Backend {
        name,
        handshake,
        upgrades: Arc::default(),
        requests: Arc::default(),
    };
    let a = backend("a", first);
    let b = backend("b", second);
    let first = serve(Router::new().fallback(endpoint).with_state(a.clone())).await;
    let second = serve(Router::new().fallback(endpoint).with_state(b.clone())).await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(
        &json!({"clients":{"codex":{"provider":"primary"},"openai":{"provider":"primary"},"anthropic":{"provider":"primary"}},"providers":{
        "primary":{"base_url":first.url,"auth":"passthrough","models":{"gpt-test":{}}},
        "custom":{"base_url":format!("{}/v1",second.url),"auth":"none","models":{"worker":{}}}
    },"routes":[{"path":"/responses","action":"model"}]})
        .to_string(),
        root.path(),
    )
    .unwrap();
    Fixture {
        gateway: serve(router(config).unwrap()).await,
        _servers: vec![first, second],
        a,
        b,
    }
}

fn handshake(fixture: &Fixture, model: Option<&str>) -> tungstenite::handshake::client::Request {
    let mut request = (fixture.gateway.url.replace("http:", "ws:") + "/responses")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("originator", "codex_cli_rs".parse().unwrap());
    request
        .headers_mut()
        .insert("authorization", "Bearer primary-secret".parse().unwrap());
    if let Some(model) = model {
        request.headers_mut().insert(
            "x-codex-routing-hint",
            format!("model={model}").parse().unwrap(),
        );
    } else {
        request
            .headers_mut()
            .insert("originator", "codex_cli_rs".parse().unwrap());
    }
    request
}

async fn turn<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>, model: &str) -> Vec<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    socket
        .send(tungstenite::Message::Text(
            json!({"type":"response.create","model":model,"input":[],"stream_id":"lane"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    timeout(Duration::from_secs(3), async {
        let mut events = Vec::new();
        loop {
            let event: Value =
                serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
            let done = matches!(event["type"].as_str(), Some("response.completed" | "error"));
            events.push(event);
            if done {
                return events;
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn switches_between_websocket_providers_without_sending_worker_to_primary() {
    let f = fixture(StatusCode::SWITCHING_PROTOCOLS).await;
    let (mut socket, _) = connect_async(handshake(&f, Some("gpt-test")))
        .await
        .unwrap();
    for model in ["gpt-test", "worker", "gpt-test", "worker"] {
        let response = turn(&mut socket, model).await;
        assert_eq!(response.last().unwrap()["response"]["model"], model);
        if model == "worker" {
            assert!(response.iter().all(|event| event["stream_id"] == "lane"));
        }
    }
    assert_eq!(f.a.upgrades.load(Ordering::Relaxed), 1);
    let primary = f.a.requests.lock().unwrap();
    assert_eq!(primary.len(), 2);
    assert!(primary.iter().all(|(ws, auth, body)| *ws
        && auth.as_deref() == Some("Bearer primary-secret")
        && body["model"] == "gpt-test"));
    let worker = f.b.requests.lock().unwrap();
    assert_eq!(worker.len(), 2);
    assert!(
        worker
            .iter()
            .all(|(ws, auth, body)| *ws && auth.is_none() && body["model"] == "worker")
    );
}

#[tokio::test]
async fn initial_unsupported_websocket_returns_upgrade_required_without_http_inference() {
    for status in unsupported_statuses() {
        let f = fixture(status).await;
        for adapted in [true, false] {
            let mut request = handshake(&f, Some("worker"));
            if !adapted {
                request.headers_mut().remove("originator");
                request.headers_mut().remove("x-codex-routing-hint");
                request
                    .headers_mut()
                    .insert("x-model", "worker".parse().unwrap());
            }
            let tungstenite::Error::Http(response) = connect_async(request).await.unwrap_err()
            else {
                panic!("Expected HTTP rejection before upgrade for {status}");
            };
            assert_eq!(response.status(), StatusCode::UPGRADE_REQUIRED, "{status}");
        }
        assert_eq!(f.b.upgrades.load(Ordering::Relaxed), 2);
        assert!(f.a.requests.lock().unwrap().is_empty());
        assert!(f.b.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn initial_auth_and_transient_upgrade_failures_preserve_status() {
    for status in [
        StatusCode::UNAUTHORIZED,
        StatusCode::FORBIDDEN,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::BAD_GATEWAY,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        let f = fixture(status).await;
        let tungstenite::Error::Http(response) = connect_async(handshake(&f, Some("worker")))
            .await
            .unwrap_err()
        else {
            panic!("Expected HTTP rejection before upgrade for {status}");
        };
        assert_eq!(response.status(), status);
        assert!(f.b.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn no_hint_rejects_unsupported_route_fallback_before_downstream_upgrade() {
    let f = fixture_with_handshakes(
        StatusCode::METHOD_NOT_ALLOWED,
        StatusCode::SWITCHING_PROTOCOLS,
    )
    .await;
    let tungstenite::Error::Http(response) = connect_async(handshake(&f, None)).await.unwrap_err()
    else {
        panic!("Expected the route fallback's upgrade rejection");
    };
    assert_eq!(response.status(), StatusCode::UPGRADE_REQUIRED);
    assert_eq!(f.a.upgrades.load(Ordering::Relaxed), 1);
    assert_eq!(f.b.upgrades.load(Ordering::Relaxed), 0);
    assert!(f.a.requests.lock().unwrap().is_empty());
    assert!(f.b.requests.lock().unwrap().is_empty());
}

fn unsupported_statuses() -> [StatusCode; 7] {
    [
        StatusCode::OK,
        StatusCode::CREATED,
        StatusCode::NO_CONTENT,
        StatusCode::NOT_FOUND,
        StatusCode::METHOD_NOT_ALLOWED,
        StatusCode::UPGRADE_REQUIRED,
        StatusCode::NOT_IMPLEMENTED,
    ]
}

#[tokio::test]
async fn client_can_explicitly_retry_http_after_websocket_upgrade_is_rejected() {
    let f = fixture(StatusCode::METHOD_NOT_ALLOWED).await;
    let tungstenite::Error::Http(response) = connect_async(handshake(&f, Some("worker")))
        .await
        .unwrap_err()
    else {
        panic!("Expected HTTP rejection before upgrade");
    };
    assert_eq!(response.status(), StatusCode::UPGRADE_REQUIRED);
    assert!(f.b.requests.lock().unwrap().is_empty());

    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(format!("{}/responses", f.gateway.url))
        .header("originator", "codex_cli_rs")
        .json(&json!({"model":"worker","input":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.unwrap()["id"],
        "resp_http_retry"
    );
    let worker = f.b.requests.lock().unwrap();
    assert_eq!(worker.len(), 1);
    assert!(!worker[0].0);
    assert_eq!(worker[0].2["model"], "worker");
    assert!(f.a.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn switched_unsupported_websocket_emits_error_then_close_without_http_inference() {
    for status in unsupported_statuses() {
        let f = fixture(status).await;
        let (mut socket, _) = connect_async(handshake(&f, Some("gpt-test")))
            .await
            .unwrap();
        let response = turn(&mut socket, "worker").await;
        assert_eq!(response.len(), 1);
        assert_eq!(response[0]["type"], "error");
        assert_eq!(response[0]["status"], 426);
        assert_eq!(response[0]["error"]["code"], "websocket_upgrade_rejected");
        assert_eq!(response[0]["stream_id"], "lane");
        assert_closed(&mut socket).await;
        assert_eq!(f.b.upgrades.load(Ordering::Relaxed), 1);
        assert!(f.a.requests.lock().unwrap().is_empty());
        assert!(f.b.requests.lock().unwrap().is_empty());
    }
}

async fn assert_closed<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let close = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(close.is_close());
}

#[tokio::test]
async fn switching_websocket_provider_rejects_previous_response_id_and_accepts_full_input() {
    let f = fixture(StatusCode::SWITCHING_PROTOCOLS).await;
    let (mut socket, _) = connect_async(handshake(&f, Some("gpt-test")))
        .await
        .unwrap();
    socket.send(tungstenite::Message::Text(json!({"type":"response.create","model":"worker","previous_response_id":"gpt-response","input":[]}).to_string().into())).await.unwrap();
    let event: Value =
        serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
    assert_eq!(event["error"]["code"], "previous_response_not_found");
    assert!(f.b.requests.lock().unwrap().is_empty());
    assert_eq!(
        turn(&mut socket, "worker").await.last().unwrap()["response"]["model"],
        "worker"
    );
    assert!(f.b.requests.lock().unwrap()[0].0);
}

#[tokio::test]
async fn switched_auth_and_transient_upgrade_failures_preserve_error_then_close() {
    for status in [
        StatusCode::UNAUTHORIZED,
        StatusCode::FORBIDDEN,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::BAD_GATEWAY,
    ] {
        let f = fixture(status).await;
        let (mut socket, _) = connect_async(handshake(&f, Some("gpt-test")))
            .await
            .unwrap();
        let result = turn(&mut socket, "worker").await;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["type"], "error");
        assert_eq!(result[0]["error"], handshake_error(status)["error"]);
        assert_eq!(result[0]["status"], status.as_u16());
        assert!(f.b.requests.lock().unwrap().is_empty());
        assert_eq!(result[0]["stream_id"], "lane");
        assert_closed(&mut socket).await;
    }
}

fn handshake_error(status: StatusCode) -> Value {
    if status == StatusCode::TOO_MANY_REQUESTS {
        json!({"error":{
            "message":"Quota exhausted",
            "code":"insufficient_quota",
            "type":"usage_limit_reached",
            "param":null,
            "details":{"reset_at":12345}
        }})
    } else {
        json!({"error":{"message":"upgrade rejected","code":"test_rejection"}})
    }
}

#[tokio::test]
async fn no_hint_opens_route_fallback_websocket_and_routes_frames_to_configured_models() {
    let f = fixture(StatusCode::SWITCHING_PROTOCOLS).await;
    let (mut socket, _) = connect_async(handshake(&f, None)).await.unwrap();
    assert_eq!(f.a.upgrades.load(Ordering::Relaxed), 1);
    assert_eq!(
        turn(&mut socket, "worker").await.last().unwrap()["response"]["model"],
        "worker"
    );
    assert!(f.a.requests.lock().unwrap().is_empty());
    let worker = f.b.requests.lock().unwrap();
    assert_eq!(worker.len(), 1);
    assert!(worker[0].0);
}
