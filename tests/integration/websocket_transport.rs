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

#[derive(Clone, Default)]
struct PreparationGate {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    worker_finish: Arc<tokio::sync::Notify>,
    worker_closed: Arc<tokio::sync::Notify>,
    primary_close: Arc<tokio::sync::Notify>,
    primary_closed: Arc<tokio::sync::Notify>,
    probes: Arc<AtomicUsize>,
}

#[derive(Clone, Copy)]
enum DuringPreparation {
    Complete,
    Cancel,
    Reconnect,
}

#[tokio::test]
async fn upstream_close_racing_opaque_and_pong_frames_ends_session() {
    for eof in [false, true] {
        let close = Arc::new(tokio::sync::Notify::new());
        let primary = serve(Router::new().fallback({
            let close = Arc::clone(&close);
            move |ws: WebSocketUpgrade| {
                let close = Arc::clone(&close);
                async move {
                    ws.on_upgrade(move |mut socket| async move {
                        close.notified().await;
                        if !eof {
                            let _ = socket
                                .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                                    code: 1001,
                                    reason: "upstream leaving".into(),
                                })))
                                .await;
                            let _ = socket.recv().await;
                        }
                    })
                }
            }
        }))
        .await;
        let root = tempfile::tempdir().unwrap();
        let config = parse_config(&json!({
            "clients":{"codex":{"provider":"primary"},"openai":{"provider":"primary"},"anthropic":{"provider":"primary"}},
            "providers":{"primary":{"base_url":primary.url,"auth":"none","models":{"primary":{}}}},
            "routes":[{"action":"model"}]
        }).to_string(), root.path()).unwrap();
        let gateway = serve(router(config).unwrap()).await;
        for i in 0..80 {
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
            let frame = if i % 2 == 0 {
                tungstenite::Message::Text("opaque frame".into())
            } else {
                tungstenite::Message::Pong(vec![1, 2].into())
            };
            socket.send(frame).await.unwrap();
            close.notify_one();
            let received = timeout(Duration::from_secs(2), socket.next())
                .await
                .expect("session hung after upstream closed");
            if eof {
                assert!(
                    received.is_none() || matches!(received, Some(Err(_))),
                    "unexpected frame: {received:?}"
                );
            } else {
                let frame = received
                    .expect("missing upstream close")
                    .expect("invalid close");
                let tungstenite::Message::Close(Some(frame)) = frame else {
                    panic!("unexpected frame: {frame:?}")
                };
                assert_eq!(u16::from(frame.code), 1001);
                assert_eq!(frame.reason, "upstream leaving");
            }
        }
    }
}

#[tokio::test]
async fn pending_preparation_allows_active_response_and_preserves_request_order() {
    preparation_during_response(DuringPreparation::Complete).await;
}

#[tokio::test]
async fn downstream_close_cancels_active_response_during_preparation() {
    preparation_during_response(DuringPreparation::Cancel).await;
}

#[tokio::test]
async fn original_close_during_preparation_reconnects_pending_request() {
    preparation_during_response(DuringPreparation::Reconnect).await;
}

async fn preparation_during_response(during: DuringPreparation) {
    let gate = PreparationGate::default();
    let backend = Backend {
        name: "primary",
        handshake: StatusCode::SWITCHING_PROTOCOLS,
        upgrades: Arc::default(),
        requests: Arc::default(),
    };
    let primary = serve(Router::new().fallback({
        let gate = gate.clone();
        let backend = backend.clone();
        move |request: Request| {
            let gate = gate.clone();
            let backend = backend.clone();
            async move {
                if request.headers().contains_key("upgrade") {
                    if matches!(during, DuringPreparation::Reconnect) && backend.upgrades.load(Ordering::Relaxed) == 0 {
                        backend.upgrades.fetch_add(1, Ordering::Relaxed);
                        let (mut parts, _) = request.into_parts();
                        return WebSocketUpgrade::from_request_parts(&mut parts, &()).await.unwrap().on_upgrade(move |mut socket| async move {
                            gate.primary_close.notified().await;
                            socket.send(Message::Close(None)).await.unwrap();
                            let _ = socket.recv().await;
                            gate.primary_closed.notify_one();
                        });
                    }
                    return endpoint(State(backend), request).await;
                }
                if gate.probes.fetch_add(1, Ordering::Relaxed) == 0 {
                    gate.started.notify_one();
                    gate.release.notified().await;
                }
                (StatusCode::UNPROCESSABLE_ENTITY, axum::Json(json!({"error":{"message":"Unsupported Responses API input item type: 'agent_message'"}}))).into_response()
            }
        }
    })).await;
    let worker = serve(Router::new().fallback({
        let gate = gate.clone();
        move |ws: WebSocketUpgrade| {
            let gate = gate.clone();
            async move {
                ws.on_upgrade(move |mut socket| async move {
                    socket.recv().await.unwrap().unwrap();
                    socket.send(Message::Text(json!({"type":"response.created"}).to_string().into())).await.unwrap();
                    tokio::select! {
                        _ = gate.worker_finish.notified() => {
                            let _ = socket.send(Message::Text(json!({"type":"response.completed"}).to_string().into())).await;
                        }
                        _ = socket.recv() => gate.worker_closed.notify_one(),
                    }
                })
            }
        }
    })).await;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("models.json"),
        json!({"models":[{"slug":"primary"}]}).to_string(),
    )
    .unwrap();
    let config = parse_config(&json!({
        "clients":{"codex":{"provider":"primary"},"openai":{"provider":"primary"},"anthropic":{"provider":"primary"}},
        "providers":{
            "primary":{"base_url":primary.url,"auth":"none","catalog":{"file":"models.json","format":"codex"}},
            "worker":{"base_url":worker.url,"auth":"none","models":{"worker":{}}}
        },"routes":[{"action":"model"}]
    }).to_string(),root.path()).unwrap();
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
    if !matches!(during, DuringPreparation::Reconnect) {
        socket
            .send(tungstenite::Message::Text(
                json!({"type":"response.create","model":"worker"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let created = timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&created.into_data()).unwrap()["type"],
            "response.created"
        );
    }
    socket.send(tungstenite::Message::Text(json!({"type":"response.create","model":"primary","stream_id":"first","tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","description":"d"}]}],"input":[{"type":"agent_message","author":"worker","content":[{"type":"input_text","text":"hello"}]}]}).to_string().into())).await.unwrap();
    timeout(Duration::from_secs(2), gate.started.notified())
        .await
        .unwrap();
    socket
        .send(tungstenite::Message::Text(
            json!({"type":"response.create","model":"primary","stream_id":"second","input":[]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    if matches!(during, DuringPreparation::Cancel) {
        socket
            .send(tungstenite::Message::Close(None))
            .await
            .unwrap();
        timeout(Duration::from_secs(2), gate.worker_closed.notified())
            .await
            .expect("active worker was not canceled while preparation was blocked");
        assert!(backend.requests.lock().unwrap().is_empty());
    } else {
        if matches!(during, DuringPreparation::Reconnect) {
            gate.primary_close.notify_one();
            timeout(Duration::from_secs(2), gate.primary_closed.notified())
                .await
                .unwrap();
        }
        if matches!(during, DuringPreparation::Complete) {
            gate.worker_finish.notify_one();
            let completed = timeout(Duration::from_secs(2), socket.next())
                .await
                .expect("active response was starved by preparation")
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&completed.into_data()).unwrap()["type"],
                "response.completed"
            );
        }
        assert!(backend.requests.lock().unwrap().is_empty());
        gate.release.notify_one();
        timeout(Duration::from_secs(2), async {
            let mut completed = 0;
            while completed < 2 {
                let message = socket.next().await.unwrap().unwrap();
                let event: Value = serde_json::from_slice(&message.into_data()).unwrap();
                if event["type"] == "response.completed" {
                    completed += 1;
                }
            }
        })
        .await
        .unwrap();
        let requests = backend.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let first = requests
            .iter()
            .find(|request| request.2["stream_id"] == "first")
            .unwrap();
        let description = first.2["tools"][0]["tools"][0]["description"]
            .as_str()
            .unwrap();
        assert_eq!(description.matches("Available model overrides").count(), 1);
        let mut ids: Vec<_> = requests
            .iter()
            .map(|request| request.2["stream_id"].as_str().unwrap())
            .collect();
        if matches!(during, DuringPreparation::Reconnect) {
            // Reconnected responses use independent sockets; only the original hop
            // guarantees wire order.
            ids.sort_unstable();
            assert_eq!(backend.upgrades.load(Ordering::Relaxed), 3);
        }
        assert_eq!(ids, ["first", "second"]);
    }
}

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
            return crate::common::json_response(
                state.handshake,
                handshake_error(state.handshake),
                state.handshake == StatusCode::UNAUTHORIZED,
            )
            .await;
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
    match status {
        StatusCode::UNAUTHORIZED => json!({"error":{
            "message":"Invalid API key",
            "code":"invalid_api_key",
            "type":"authentication_error"
        }}),
        StatusCode::TOO_MANY_REQUESTS => json!({"error":{
            "message":"Quota exhausted",
            "code":"insufficient_quota",
            "type":"usage_limit_reached",
            "param":null,
            "details":{"reset_at":12345}
        }}),
        _ => json!({"error":{"message":"upgrade rejected","code":"test_rejection"}}),
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
