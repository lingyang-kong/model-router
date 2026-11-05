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
    hints: Arc<Mutex<Vec<Option<String>>>>,
    live: Option<Arc<LiveConnections>>,
}
type RecordedRequest = (bool, Option<String>, Value);

#[derive(Default)]
struct LiveConnections {
    count: AtomicUsize,
    closed: tokio::sync::Notify,
}

struct LiveConnection(Arc<LiveConnections>);

impl Drop for LiveConnection {
    fn drop(&mut self) {
        self.0.count.fetch_sub(1, Ordering::SeqCst);
        self.0.closed.notify_one();
    }
}

#[derive(Clone, Default)]
struct PreparationGate {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    worker_finish: Arc<tokio::sync::Notify>,
    worker_closed: Arc<tokio::sync::Notify>,
    probes: Arc<AtomicUsize>,
}

#[derive(Clone, Copy)]
enum DuringPreparation {
    Complete,
    Cancel,
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

async fn preparation_during_response(during: DuringPreparation) {
    let gate = PreparationGate::default();
    let backend = Backend {
        name: "primary",
        handshake: StatusCode::SWITCHING_PROTOCOLS,
        upgrades: Arc::default(),
        requests: Arc::default(),
        hints: Arc::default(),
        live: None,
    };
    let primary = serve(Router::new().fallback({
        let gate = gate.clone();
        let backend = backend.clone();
        move |request: Request| {
            let gate = gate.clone();
            let backend = backend.clone();
            async move {
                if request.headers().contains_key("upgrade") {
                    backend.upgrades.fetch_add(1, Ordering::Relaxed);
                    let (mut parts, _) = request.into_parts();
                    return WebSocketUpgrade::from_request_parts(&mut parts, &()).await.unwrap().on_upgrade(move |mut socket| async move {
                        socket.recv().await.unwrap().unwrap();
                        socket.send(Message::Text(json!({"type":"response.created"}).to_string().into())).await.unwrap();
                        tokio::select! {
                            _ = gate.worker_finish.notified() => {
                                socket.send(Message::Text(json!({"type":"response.completed"}).to_string().into())).await.unwrap();
                            }
                            _ = socket.recv() => {
                                gate.worker_closed.notify_one();
                                return;
                            }
                        }
                        while let Some(Ok(Message::Text(text))) = socket.recv().await {
                            let body: Value = serde_json::from_str(&text).unwrap();
                            backend.requests.lock().unwrap().push((true, None, body.clone()));
                            for event in events(backend.name, &body["model"]) {
                                if socket.send(Message::Text(event.to_string().into())).await.is_err() {
                                    return;
                                }
                            }
                        }
                    });
                }
                if gate.probes.fetch_add(1, Ordering::Relaxed) == 0 {
                    gate.started.notify_one();
                    gate.release.notified().await;
                }
                (StatusCode::UNPROCESSABLE_ENTITY, axum::Json(json!({"error":{"message":"Unsupported Responses API input item type: 'agent_message'"}}))).into_response()
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
            "primary":{"base_url":primary.url,"auth":"none","catalog":{"file":"models.json","format":"codex"}}
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
    socket
        .send(tungstenite::Message::Text(
            json!({"type":"response.create","model":"primary"})
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
        let ids: Vec<_> = requests
            .iter()
            .map(|request| request.2["stream_id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["first", "second"]);
    }
}

#[tokio::test]
async fn active_original_response_loss_after_local_work_ends_session() {
    for eof in [false, true] {
        for started in [false, true] {
            for concurrent in [false, true] {
                for stream_ids in [false, true] {
                    original_response_loss(eof, started, concurrent, stream_ids).await;
                }
            }
        }
    }
}

async fn original_response_loss(eof: bool, started: bool, concurrent: bool, stream_ids: bool) {
    let primary = serve(Router::new().fallback(move |ws: WebSocketUpgrade| async move {
        ws.on_upgrade(move |mut socket| async move {
            for i in 0..if concurrent { 2 } else { 1 } {
                let request = socket.recv().await.unwrap().unwrap();
                let request: Value = serde_json::from_slice(&request.into_data()).unwrap();
                if started {
                    let mut event = json!({"type":"response.created","response":{"id":format!("resp_{i}"),"status":"in_progress"}});
                    if stream_ids { event["stream_id"] = request["stream_id"].clone(); }
                    socket.send(Message::Text(event.to_string().into())).await.unwrap();
                }
                if concurrent && i == 1 {
                    let mut event = json!({"type":"response.completed","response":{"id":"resp_1","status":"completed"}});
                    if stream_ids { event["stream_id"] = request["stream_id"].clone(); }
                    socket.send(Message::Text(event.to_string().into())).await.unwrap();
                }
            }
            socket.send(Message::Text(json!({"type":"error","stream_id":"unrelated","error":{"message":"another request failed"}}).to_string().into())).await.unwrap();
            if !eof {
                socket.send(Message::Close(Some(axum::extract::ws::CloseFrame {
                    code: 1011, reason: "generation failed".into(),
                }))).await.unwrap();
                let _ = socket.recv().await;
            }
        })
    })).await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(
        &json!({
            "providers":{
                "primary":{"base_url":primary.url,"auth":"none","models":{"primary":{}}}
            },"routes":[{"action":"model"}]
        })
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
    assert_eq!(
        turn(&mut socket, "missing").await.last().unwrap()["type"],
        "error"
    );
    for i in 0..if concurrent { 2 } else { 1 } {
        let mut request = json!({"type":"response.create","model":"primary","input":[]});
        if stream_ids {
            request["stream_id"] = json!(format!("lane_{i}"));
        }
        socket
            .send(tungstenite::Message::Text(request.to_string().into()))
            .await
            .unwrap();
    }
    let mut created = 0;
    let mut completed = 0;
    let mut unrelated_errors = 0;
    timeout(Duration::from_secs(2), async {
        loop {
            match socket.next().await {
                Some(Ok(tungstenite::Message::Text(text))) => {
                    let event: Value = serde_json::from_str(&text).unwrap();
                    match event["type"].as_str().unwrap() {
                        "response.created" => created += 1,
                        "response.completed" => completed += 1,
                        "error" => {
                            assert_eq!(event["stream_id"], "unrelated");
                            unrelated_errors += 1;
                        }
                        other => panic!("unexpected event {other}"),
                    }
                }
                Some(Ok(tungstenite::Message::Close(Some(frame)))) if !eof => {
                    assert_eq!(u16::from(frame.code), 1011);
                    assert_eq!(frame.reason, "generation failed");
                    break;
                }
                None | Some(Err(_)) if eof => break,
                other => panic!("unexpected close {other:?}"),
            }
        }
    })
    .await
    .expect("active original response disappeared after upstream loss");
    assert_eq!(
        created,
        if started {
            if concurrent { 2 } else { 1 }
        } else {
            0
        }
    );
    assert_eq!(completed, usize::from(concurrent));
    assert_eq!(unrelated_errors, 1);
}

#[tokio::test]
async fn idle_upstream_loss_after_local_reply_ends_session() {
    for eof in [false, true] {
        for terminal in [None, Some("response.completed"), Some("error")] {
            let close = Arc::new(tokio::sync::Notify::new());
            let primary = serve(Router::new().fallback({
                let close = close.clone();
                move |ws: WebSocketUpgrade| {
                    let close = close.clone();
                    async move {
                        ws.on_upgrade(move |mut socket| async move {
                            if let Some(terminal) = terminal {
                                socket.recv().await.unwrap().unwrap();
                                socket
                                    .send(Message::Text(
                                        json!({"type":terminal}).to_string().into(),
                                    ))
                                    .await
                                    .unwrap();
                            }
                            close.notified().await;
                            if !eof {
                                socket.send(Message::Close(None)).await.unwrap();
                                let _ = socket.recv().await;
                            }
                        })
                    }
                }
            }))
            .await;
            let root = tempfile::tempdir().unwrap();
            let config = parse_config(&json!({
                "providers":{"primary":{"base_url":primary.url,"auth":"none","models":{"primary":{}}}},
                "routes":[{"action":"model"}]
            }).to_string(), root.path()).unwrap();
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
            if let Some(terminal) = terminal {
                assert_eq!(
                    turn(&mut socket, "primary").await.last().unwrap()["type"],
                    terminal
                );
            }
            // A locally generated reply previously enabled permanent Close suppression,
            // even with only one upstream connection and no inference in progress.
            assert_eq!(
                turn(&mut socket, "missing").await.last().unwrap()["type"],
                "error"
            );
            close.notify_one();
            let received = timeout(Duration::from_secs(2), socket.next())
                .await
                .expect("idle upstream loss was hidden after a local reply");
            if eof {
                assert!(received.is_none() || matches!(received, Some(Err(_))));
            } else {
                assert!(received.unwrap().unwrap().is_close());
            }
        }
    }
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
        if let Some(live) = &state.live {
            timeout(Duration::from_secs(2), async {
                loop {
                    let closed = live.closed.notified();
                    if live.count.load(Ordering::SeqCst) == 0 {
                        break;
                    }
                    closed.await;
                }
            })
            .await
            .expect("old upstream remained open while switching providers");
        }
        state.upgrades.fetch_add(1, Ordering::Relaxed);
        state.hints.lock().unwrap().push(
            request
                .headers()
                .get("x-codex-routing-hint")
                .map(|value| value.to_str().unwrap().to_owned()),
        );
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
                let _live = state.live.clone().map(|live| {
                    assert_eq!(live.count.fetch_add(1, Ordering::SeqCst), 0);
                    LiveConnection(live)
                });
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
    let live = Arc::new(LiveConnections::default());
    let backend = |name, handshake| Backend {
        name,
        handshake,
        upgrades: Arc::default(),
        requests: Arc::default(),
        hints: Arc::default(),
        live: Some(Arc::clone(&live)),
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
    for model in ["gpt-test", "worker", "worker", "gpt-test"] {
        let response = if model == "worker" && f.b.requests.lock().unwrap().len() == 1 {
            socket.send(tungstenite::Message::Text(json!({"type":"response.create","model":"worker","previous_response_id":"resp_b","input":[],"stream_id":"lane"}).to_string().into())).await.unwrap();
            timeout(Duration::from_secs(3), async {
                let mut result = Vec::new();
                loop {
                    let event: Value =
                        serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data())
                            .unwrap();
                    let done = event["type"] == "response.completed" || event["type"] == "error";
                    result.push(event);
                    if done {
                        break result;
                    }
                }
            })
            .await
            .unwrap()
        } else {
            turn(&mut socket, model).await
        };
        assert_eq!(response.last().unwrap()["response"]["model"], model);
        if model == "worker" {
            assert!(response.iter().all(|event| event["stream_id"] == "lane"));
        }
    }
    assert_eq!(f.a.upgrades.load(Ordering::Relaxed), 2);
    assert_eq!(f.b.upgrades.load(Ordering::Relaxed), 1);
    let primary = f.a.requests.lock().unwrap();
    assert_eq!(primary.len(), 2);
    assert!(primary.iter().all(|(ws, auth, body)| *ws
        && auth.as_deref() == Some("Bearer primary-secret")
        && body["model"] == "gpt-test"));
    let worker = f.b.requests.lock().unwrap();
    assert_eq!(worker.len(), 2);
    assert_eq!(worker[1].2["previous_response_id"], "resp_b");
    assert!(
        worker
            .iter()
            .all(|(ws, auth, body)| *ws && auth.is_none() && body["model"] == "worker")
    );
}

#[tokio::test]
async fn missing_model_after_switch_uses_original_handshake_model() {
    let f = fixture(StatusCode::SWITCHING_PROTOCOLS).await;
    let (mut socket, _) = connect_async(handshake(&f, Some("gpt-test")))
        .await
        .unwrap();
    assert_eq!(
        turn(&mut socket, "worker").await.last().unwrap()["response"]["model"],
        "worker"
    );
    socket
        .send(tungstenite::Message::Text(
            json!({"type":"response.create","input":[]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    timeout(Duration::from_secs(3), async {
        loop {
            let event: Value =
                serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
            if event["type"] == "response.completed" {
                break;
            }
            assert_ne!(event["type"], "error");
        }
    })
    .await
    .unwrap();
    assert_eq!(f.a.upgrades.load(Ordering::Relaxed), 2);
    assert_eq!(f.b.requests.lock().unwrap().len(), 1);
    assert_eq!(f.a.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn model_less_return_to_fallback_does_not_reuse_switched_model_hint() {
    let f = fixture(StatusCode::SWITCHING_PROTOCOLS).await;
    let (mut socket, _) = connect_async(handshake(&f, None)).await.unwrap();
    turn(&mut socket, "worker").await;
    socket
        .send(tungstenite::Message::Text(
            json!({"type":"response.create","input":[]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    timeout(Duration::from_secs(3), async {
        loop {
            let event: Value =
                serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
            if event["type"] == "response.completed" {
                break;
            }
            assert_ne!(event["type"], "error");
        }
    })
    .await
    .unwrap();
    assert_eq!(f.a.upgrades.load(Ordering::Relaxed), 2);
    assert_eq!(*f.a.hints.lock().unwrap(), [None, None]);
    assert_eq!(f.a.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn opaque_json_without_model_stays_on_current_provider() {
    let f = fixture(StatusCode::SWITCHING_PROTOCOLS).await;
    let (mut socket, _) = connect_async(handshake(&f, Some("gpt-test")))
        .await
        .unwrap();
    turn(&mut socket, "worker").await;
    let control = json!({"type":"session.custom_control","payload":{"value":42}});
    socket
        .send(tungstenite::Message::Text(control.to_string().into()))
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            let event: Value =
                serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
            if event["type"] == "response.completed" {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(f.a.upgrades.load(Ordering::Relaxed), 1);
    assert_eq!(f.b.upgrades.load(Ordering::Relaxed), 1);
    assert!(f.a.requests.lock().unwrap().is_empty());
    let worker = f.b.requests.lock().unwrap();
    assert_eq!(worker.len(), 2);
    assert_eq!(worker[1].2, control);
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
    assert_eq!(f.b.upgrades.load(Ordering::Relaxed), 0);
    assert_eq!(f.a.live.as_ref().unwrap().count.load(Ordering::SeqCst), 1);
    assert_eq!(
        turn(&mut socket, "worker").await.last().unwrap()["response"]["model"],
        "worker"
    );
    assert!(f.b.requests.lock().unwrap()[0].0);
}

#[tokio::test]
async fn pipelined_local_error_survives_following_provider_switch() {
    let f = fixture(StatusCode::SWITCHING_PROTOCOLS).await;
    let (mut socket, _) = connect_async(handshake(&f, Some("gpt-test")))
        .await
        .unwrap();
    for body in [
        json!({"type":"response.create","model":"worker","previous_response_id":"stale","stream_id":"stale","input":[]}),
        json!({"type":"response.create","model":"worker","stream_id":"replay","input":[]}),
    ] {
        socket
            .send(tungstenite::Message::Text(body.to_string().into()))
            .await
            .unwrap();
    }
    let (error, completed) = timeout(Duration::from_secs(3), async {
        let mut error = None;
        let mut completed = None;
        while error.is_none() || completed.is_none() {
            let event: Value =
                serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
            match event["type"].as_str() {
                Some("error") => error = Some(event),
                Some("response.completed") => completed = Some(event),
                _ => {}
            }
        }
        (error.unwrap(), completed.unwrap())
    })
    .await
    .unwrap();
    assert_eq!(error["error"]["code"], "previous_response_not_found");
    assert_eq!(error["stream_id"], "stale");
    assert_eq!(completed["stream_id"], "replay");
    assert_eq!(completed["response"]["model"], "worker");
    assert_eq!(f.b.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn local_summary_finishes_during_switched_handshake() {
    let summary_started = Arc::new(tokio::sync::Notify::new());
    let summary_release = Arc::new(tokio::sync::Notify::new());
    let switch_started = Arc::new(tokio::sync::Notify::new());
    let switch_release = Arc::new(tokio::sync::Notify::new());
    let primary = serve(Router::new().fallback({
        let summary_started = Arc::clone(&summary_started);
        let summary_release = Arc::clone(&summary_release);
        move |request: Request| {
            let summary_started = Arc::clone(&summary_started);
            let summary_release = Arc::clone(&summary_release);
            async move {
                if request.headers().contains_key("upgrade") {
                    let (mut parts, _) = request.into_parts();
                    return WebSocketUpgrade::from_request_parts(&mut parts, &()).await.unwrap().on_upgrade(|mut socket| async move {
                        while let Some(Ok(message)) = socket.recv().await {
                            if let Message::Close(frame) = message {
                                let _ = socket.send(Message::Close(frame)).await;
                                break;
                            }
                        }
                    });
                }
                let body: Value = serde_json::from_slice(&to_bytes(request.into_body(), usize::MAX).await.unwrap()).unwrap();
                if body["input"].as_array().is_some_and(|items| items.iter().any(|item| item["type"] == "__model_router_capability_check__")) {
                    return (StatusCode::BAD_REQUEST, axum::Json(json!({"error":{"message":"Unsupported Responses API input item type: 'compaction_trigger'"}}))).into_response();
                }
                summary_started.notify_one();
                summary_release.notified().await;
                axum::Json(json!({"id":"upstream-summary","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"remembered state"}]}]})).into_response()
            }
        }
    })).await;
    let worker = serve(Router::new().fallback({
        let switch_started = Arc::clone(&switch_started);
        let switch_release = Arc::clone(&switch_release);
        move |ws: WebSocketUpgrade| {
            let switch_started = Arc::clone(&switch_started);
            let switch_release = Arc::clone(&switch_release);
            async move {
                switch_started.notify_one();
                switch_release.notified().await;
                ws.on_upgrade(|mut socket| async move {
                    if let Some(Ok(Message::Text(text))) = socket.recv().await {
                        let body: Value = serde_json::from_str(&text).unwrap();
                        let response =
                            json!({"type":"response.completed","stream_id":body["stream_id"]});
                        let _ = socket
                            .send(Message::Text(response.to_string().into()))
                            .await;
                    }
                })
            }
        }
    }))
    .await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(&json!({
        "clients":{"codex":{"provider":"primary"},"openai":{"provider":"primary"},"anthropic":{"provider":"primary"}},
        "providers":{
            "primary":{"base_url":primary.url,"auth":"none","models":{"primary":{}}},
            "worker":{"base_url":worker.url,"auth":"none","models":{"worker":{}}}
        },
        "routes":[{"action":"model"}]
    }).to_string(), root.path()).unwrap();
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
    socket.send(tungstenite::Message::Text(json!({"type":"response.create","model":"primary","stream_id":"summary","input":[{"type":"message","role":"user","content":"history"},{"type":"compaction_trigger"}]}).to_string().into())).await.unwrap();
    timeout(Duration::from_secs(2), summary_started.notified())
        .await
        .expect("summary did not start");
    socket
        .send(tungstenite::Message::Text(
            json!({"type":"response.create","model":"worker","stream_id":"switched","input":[]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    timeout(Duration::from_secs(2), switch_started.notified())
        .await
        .expect("switched handshake did not start");
    summary_release.notify_one();
    timeout(Duration::from_secs(2), async {
        loop {
            let event: Value =
                serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
            assert_ne!(event["type"], "error", "{event}");
            if event["type"] == "response.completed" {
                assert_eq!(event["stream_id"], "summary");
                break;
            }
        }
    })
    .await
    .expect("summary was lost or blocked during switched handshake");
    switch_release.notify_one();
    let event: Value = serde_json::from_slice(
        &timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_data(),
    )
    .unwrap();
    assert_eq!(event["type"], "response.completed");
    assert_eq!(event["stream_id"], "switched");
}

#[tokio::test]
async fn requests_buffered_during_switch_are_delivered_once_in_order() {
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let upgrades = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let primary = serve(Router::new().fallback(|ws: WebSocketUpgrade| async move {
        ws.on_upgrade(|mut socket| async move {
            while let Some(Ok(message)) = socket.recv().await {
                if let Message::Close(frame) = message {
                    let _ = socket.send(Message::Close(frame)).await;
                    break;
                }
            }
        })
    }))
    .await;
    let worker = serve(Router::new().fallback({
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        let upgrades = Arc::clone(&upgrades);
        let requests = Arc::clone(&requests);
        move |ws: WebSocketUpgrade| {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            let upgrades = Arc::clone(&upgrades);
            let requests = Arc::clone(&requests);
            async move {
                upgrades.fetch_add(1, Ordering::SeqCst);
                started.notify_one();
                release.notified().await;
                ws.on_upgrade(move |mut socket| async move {
                    while let Some(Ok(Message::Text(text))) = socket.recv().await {
                        let body: Value = serde_json::from_str(&text).unwrap();
                        requests.lock().unwrap().push(body.clone());
                        let response =
                            json!({"type":"response.completed","stream_id":body["stream_id"]});
                        if socket
                            .send(Message::Text(response.to_string().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                })
            }
        }
    }))
    .await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(&json!({
        "clients":{"codex":{"provider":"primary"},"openai":{"provider":"primary"},"anthropic":{"provider":"primary"}},
        "providers":{
            "primary":{"base_url":primary.url,"auth":"none","models":{"primary":{}}},
            "worker":{"base_url":worker.url,"auth":"none","models":{"worker":{}}}
        },
        "routes":[{"action":"model"}]
    }).to_string(), root.path()).unwrap();
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
            json!({"type":"response.create","model":"worker","stream_id":"first","input":[]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("switched handshake did not start");
    for id in ["second", "third"] {
        socket
            .send(tungstenite::Message::Text(
                json!({"type":"response.create","model":"worker","stream_id":id,"input":[]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
    }
    release.notify_one();
    let received = timeout(Duration::from_secs(2), async {
        let mut ids = Vec::new();
        while ids.len() < 3 {
            let event: Value =
                serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
            if event["type"] == "response.completed" {
                ids.push(event["stream_id"].as_str().unwrap().to_owned());
            } else {
                assert_ne!(event["type"], "error");
            }
        }
        ids
    })
    .await
    .unwrap();
    assert_eq!(received, ["first", "second", "third"]);
    assert_eq!(upgrades.load(Ordering::SeqCst), 1);
    let bodies = requests.lock().unwrap();
    assert_eq!(bodies.len(), 3);
    assert_eq!(
        bodies
            .iter()
            .map(|body| body["stream_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["first", "second", "third"]
    );
}

#[tokio::test]
async fn provider_switch_cancels_blocked_old_writes_and_preserves_later_requests() {
    let old_started = Arc::new(tokio::sync::Notify::new());
    let release_old = Arc::new(tokio::sync::Notify::new());
    let worker_started = Arc::new(tokio::sync::Notify::new());
    let release_worker = Arc::new(tokio::sync::Notify::new());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let primary = serve(Router::new().fallback({
        let old_started = Arc::clone(&old_started);
        let release_old = Arc::clone(&release_old);
        move |ws: WebSocketUpgrade| {
            let old_started = Arc::clone(&old_started);
            let release_old = Arc::clone(&release_old);
            async move {
                ws.max_message_size(32 * 1024 * 1024)
                    .max_frame_size(32 * 1024 * 1024)
                    .on_upgrade(move |mut socket| async move {
                        old_started.notify_one();
                        // Do not read while the relay sends large old-provider frames.
                        release_old.notified().await;
                        while socket.recv().await.is_some() {}
                    })
            }
        }
    }))
    .await;
    let worker = serve(Router::new().fallback({
        let worker_started = Arc::clone(&worker_started);
        let release_worker = Arc::clone(&release_worker);
        let requests = Arc::clone(&requests);
        move |ws: WebSocketUpgrade| {
            let worker_started = Arc::clone(&worker_started);
            let release_worker = Arc::clone(&release_worker);
            let requests = Arc::clone(&requests);
            async move {
                worker_started.notify_one();
                release_worker.notified().await;
                ws.on_upgrade(move |mut socket| async move {
                    while let Some(Ok(Message::Text(text))) = socket.recv().await {
                        let request: Value = serde_json::from_str(&text).unwrap();
                        let stream_id = request["stream_id"].clone();
                        requests.lock().unwrap().push(stream_id.clone());
                        socket
                            .send(Message::Text(
                                json!({"type":"response.completed","stream_id":stream_id})
                                    .to_string()
                                    .into(),
                            ))
                            .await
                            .unwrap();
                    }
                })
            }
        }
    }))
    .await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(
        &json!({
            "providers":{
                "primary":{"base_url":primary.url,"auth":"none","models":{"primary":{}}},
                "worker":{"base_url":worker.url,"auth":"none","models":{"worker":{}}}
            },
            "routes":[{"action":"model"}]
        })
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
    timeout(Duration::from_secs(2), old_started.notified())
        .await
        .unwrap();

    let size = 16 * 1024 * 1024;
    timeout(Duration::from_secs(10), async {
        for byte in [1, 2] {
            socket
                .send(tungstenite::Message::Binary(vec![byte; size].into()))
                .await
                .unwrap();
        }
        socket
            .send(tungstenite::Message::Text(
                json!({"type":"response.create","model":"worker","stream_id":"first","input":[]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        timeout(Duration::from_secs(3), worker_started.notified())
            .await
            .expect("provider switch waited for the old upstream to read");

        // This request arrives while the replacement handshake is still pending.
        socket
            .send(tungstenite::Message::Text(
                json!({"type":"response.create","model":"worker","stream_id":"second","input":[]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        release_worker.notify_one();

        let mut completed = Vec::new();
        while completed.len() < 2 {
            let event: Value =
                serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
            if event["type"] == "response.completed" {
                completed.push(event["stream_id"].as_str().unwrap().to_owned());
            }
        }
        assert_eq!(completed, ["first", "second"]);
        assert_eq!(*requests.lock().unwrap(), [json!("first"), json!("second")]);
    })
    .await
    .expect("provider switch did not cancel blocked old writes");

    // The old reader remains gated until the replacement has served both requests.
    release_old.notify_one();
}

#[tokio::test]
async fn downstream_close_cancels_blocked_initial_and_switched_upstream_writes() {
    for switched in [false, true] {
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let primary = serve(Router::new().fallback(|ws: WebSocketUpgrade| async move {
            ws.on_upgrade(|mut socket| async move {
                while let Some(Ok(message)) = socket.recv().await {
                    if message == Message::Close(None) {
                        break;
                    }
                }
            })
        }))
        .await;
        let worker = serve(Router::new().fallback({
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            move |ws: WebSocketUpgrade| {
                let started = Arc::clone(&started);
                let release = Arc::clone(&release);
                async move {
                    ws.on_upgrade(move |mut socket| async move {
                        started.notify_one();
                        // Retain the connection without reading until the client has closed.
                        release.notified().await;
                        let _ = socket.recv().await;
                    })
                }
            }
        }))
        .await;
        let root = tempfile::tempdir().unwrap();
        let config = parse_config(
            &json!({
                "clients":{"codex":{"provider":"primary"}},
                "providers":{
                    "primary":{"base_url":primary.url,"auth":"none","models":{"primary":{}}},
                    "worker":{"base_url":worker.url,"auth":"none","models":{"worker":{}}}
                },
                "routes":[{"action":"model"}]
            })
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
        request.headers_mut().insert(
            "x-codex-routing-hint",
            if switched {
                "model=primary"
            } else {
                "model=worker"
            }
            .parse()
            .unwrap(),
        );
        let (mut socket, _) = connect_async(request).await.unwrap();
        timeout(Duration::from_secs(3), socket.send(tungstenite::Message::Text(
            json!({"type":"response.create","model":"worker","input":[],"padding":"x".repeat(16 * 1024 * 1024)})
                .to_string().into(),
        ))).await.unwrap().unwrap();
        timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        socket
            .send(tungstenite::Message::Close(None))
            .await
            .unwrap();
        let closed = timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("client Close was blocked by upstream write");
        assert!(matches!(closed, Some(Ok(tungstenite::Message::Close(_)))));
        release.notify_one();
    }
}

#[tokio::test]
async fn downstream_close_cancels_pending_switched_handshake() {
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let requests = Arc::new(AtomicUsize::new(0));
    let primary = serve(Router::new().fallback(|ws: WebSocketUpgrade| async move {
        ws.on_upgrade(|mut socket| async move {
            while let Some(Ok(message)) = socket.recv().await {
                if matches!(message, Message::Close(_)) {
                    break;
                }
            }
        })
    }))
    .await;
    let worker = serve(Router::new().fallback({
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        let requests = Arc::clone(&requests);
        move |ws: WebSocketUpgrade| {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            let requests = Arc::clone(&requests);
            async move {
                started.notify_one();
                release.notified().await;
                ws.on_upgrade(move |mut socket| async move {
                    if matches!(socket.recv().await, Some(Ok(Message::Text(_)))) {
                        requests.fetch_add(1, Ordering::SeqCst);
                    }
                })
            }
        }
    }))
    .await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(&json!({
        "clients":{"codex":{"provider":"primary"},"openai":{"provider":"primary"},"anthropic":{"provider":"primary"}},
        "providers":{
            "primary":{"base_url":primary.url,"auth":"none","models":{"primary":{}}},
            "worker":{"base_url":worker.url,"auth":"none","models":{"worker":{}}}
        },
        "routes":[{"action":"model"}]
    }).to_string(), root.path()).unwrap();
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
            json!({"type":"response.create","model":"worker","input":[]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("switched handshake did not start");
    socket
        .send(tungstenite::Message::Close(None))
        .await
        .unwrap();
    let closed = timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("downstream close was blocked by handshake");
    assert!(
        closed.is_none()
            || matches!(
                closed,
                Some(Ok(tungstenite::Message::Close(_))) | Some(Err(_))
            )
    );
    release.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(requests.load(Ordering::SeqCst), 0);
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
