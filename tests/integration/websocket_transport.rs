use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use crate::common::{Server, parse_config, serve};
use axum::{
    Router,
    body::{Body, to_bytes},
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
    cancelled: Arc<AtomicBool>,
}
type RecordedRequest = (bool, Option<String>, Value);

#[tokio::test]
async fn original_socket_close_preserves_fallback_and_allows_reconnection() {
    original_socket_close(true).await;
}

#[tokio::test]
async fn original_socket_close_after_fallback_allows_reconnection() {
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
        move |request: Request| {
            let finish = Arc::clone(&finish);
            async move {
                if request.headers().contains_key("upgrade") {
                    return StatusCode::METHOD_NOT_ALLOWED.into_response();
                }
                let body = futures_util::stream::once(async {
                    Ok::<_, std::io::Error>("data: {\"type\":\"response.created\"}\n\n")
                })
                .chain(futures_util::stream::once(async move {
                    finish.notified().await;
                    Ok::<_, std::io::Error>("data: {\"type\":\"response.completed\"}\n\n")
                }));
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from_stream(body))
                    .unwrap()
            }
        }
    }))
    .await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(
        &json!({"providers":{
        "primary":{"base_url":primary.url,"auth":"none","models":{"primary":{}}},
        "worker":{"base_url":worker.url,"auth":"none","models":{"worker":{}}}
    },"routes":[{"action":"model","provider":"primary"}]})
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
        // A late control frame for the closed hop must not cancel the fallback.
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

#[tokio::test]
async fn http_fallback_streams_before_completion_and_cancels_when_client_closes() {
    let f = fixture(StatusCode::METHOD_NOT_ALLOWED).await;
    let (mut socket, _) = connect_async(handshake(&f, Some("gpt-test")))
        .await
        .unwrap();
    socket
        .send(tungstenite::Message::Text(
            json!({"type":"response.create","model":"worker","input":"hold"})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    for kind in ["response.created", "response.output_text.delta"] {
        let message = timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let event: Value = serde_json::from_slice(&message.into_data()).unwrap();
        assert_eq!(event["type"], kind);
    }
    assert!(!f.b.cancelled.load(Ordering::Relaxed));
    socket.close(None).await.unwrap();
    timeout(Duration::from_secs(2), async {
        while !f.b.cancelled.load(Ordering::Relaxed) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn http_only_prewarm_does_not_run_inference() {
    let f = fixture(StatusCode::METHOD_NOT_ALLOWED).await;
    let (mut socket, _) = connect_async(handshake(&f, Some("worker"))).await.unwrap();
    socket
        .send(tungstenite::Message::Text(
            json!({"type":"response.create","model":"worker","generate":false,"input":[]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let first: Value =
        serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
    let last: Value =
        serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
    assert_eq!(first["type"], "response.created");
    assert_eq!(last["type"], "response.completed");
    assert!(f.b.requests.lock().unwrap().is_empty());
    assert_eq!(
        turn(&mut socket, "worker").await.last().unwrap()["type"],
        "response.completed"
    );
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
                axum::Json(json!({"error":{"message":"upgrade rejected","code":"test_rejection"}})),
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
                            for event in events(state.name, &body["model"]) {
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
    let hold = body["input"] == "hold";
    let frames = events(state.name, &body["model"])
        .into_iter()
        .take(if hold { 2 } else { 4 })
        .map(|event| Ok::<_, std::io::Error>(format!("data: {event}\n\n")));
    struct Cancelled(Arc<AtomicBool>);
    impl Drop for Cancelled {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }
    let stream = futures_util::stream::unfold(
        (frames, Cancelled(state.cancelled), hold),
        |(mut frames, guard, hold)| async move {
            if let Some(frame) = frames.next() {
                return Some((frame, (frames, guard, hold)));
            }
            if hold {
                futures_util::future::pending::<()>().await;
            }
            None
        },
    );
    Response::builder()
        .header("content-type", "text/event-stream")
        .body(Body::from_stream(stream))
        .unwrap()
}

struct Fixture {
    gateway: Server,
    _servers: Vec<Server>,
    a: Backend,
    b: Backend,
}
async fn fixture(second: StatusCode) -> Fixture {
    let backend = |name, handshake| Backend {
        name,
        handshake,
        upgrades: Arc::default(),
        requests: Arc::default(),
        cancelled: Arc::default(),
    };
    let a = backend("a", StatusCode::SWITCHING_PROTOCOLS);
    let b = backend("b", second);
    let first = serve(Router::new().fallback(endpoint).with_state(a.clone())).await;
    let second = serve(Router::new().fallback(endpoint).with_state(b.clone())).await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(
        &json!({"providers":{
        "primary":{"base_url":first.url,"auth":"passthrough","models":{"gpt-test":{}}},
        "custom":{"base_url":format!("{}/v1",second.url),"auth":"none","models":{"worker":{}}}
    },"routes":[{"path":"/responses","action":"model","provider":"primary"}]})
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
async fn switches_gpt_to_http_worker_and_back_without_sending_worker_to_gpt() {
    let f = fixture(StatusCode::METHOD_NOT_ALLOWED).await;
    let (mut socket, _) = connect_async(handshake(&f, Some("gpt-test")))
        .await
        .unwrap();
    assert_eq!(
        turn(&mut socket, "gpt-test").await.last().unwrap()["response"]["model"],
        "gpt-test"
    );
    let worker = turn(&mut socket, "worker").await;
    assert_eq!(worker.last().unwrap()["response"]["model"], "worker");
    assert!(worker.iter().all(|event| event["stream_id"] == "lane"));
    assert_eq!(
        turn(&mut socket, "gpt-test").await.last().unwrap()["response"]["model"],
        "gpt-test"
    );
    assert_eq!(f.a.upgrades.load(Ordering::Relaxed), 1);
    let primary = f.a.requests.lock().unwrap();
    assert_eq!(primary.len(), 2);
    assert!(primary.iter().all(|(ws, auth, body)| *ws
        && auth.as_deref() == Some("Bearer primary-secret")
        && body["model"] == "gpt-test"));
    let worker = f.b.requests.lock().unwrap();
    assert_eq!(worker.len(), 1);
    assert!(!worker[0].0);
    assert_eq!(worker[0].1, None);
    assert!(worker[0].2.get("type").is_none());
    assert_eq!(worker[0].2["stream"], true);
}

#[tokio::test]
async fn initial_http_provider_keeps_client_websocket_and_later_provider_uses_websocket() {
    let f = fixture(StatusCode::NOT_FOUND).await;
    let (mut socket, _) = connect_async(handshake(&f, Some("worker"))).await.unwrap();
    assert_eq!(
        turn(&mut socket, "worker").await.last().unwrap()["response"]["model"],
        "worker"
    );
    assert_eq!(
        turn(&mut socket, "gpt-test").await.last().unwrap()["response"]["model"],
        "gpt-test"
    );
    assert_eq!(f.b.upgrades.load(Ordering::Relaxed), 1);
    assert!(!f.b.requests.lock().unwrap()[0].0);
    assert!(f.a.requests.lock().unwrap()[0].0);
}

#[tokio::test]
async fn websocket_capable_second_provider_is_not_downgraded_and_prior_state_is_replayed() {
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
async fn auth_and_transient_upgrade_failures_do_not_trigger_http_fallback() {
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
        assert_eq!(result[0]["status"], status.as_u16());
        assert!(f.b.requests.lock().unwrap().is_empty());
        assert_eq!(
            turn(&mut socket, "gpt-test").await.last().unwrap()["type"],
            "response.completed"
        );
    }
}

#[tokio::test]
async fn no_hint_defers_provider_selection_and_configured_ids_route_before_catalog_refresh() {
    let f = fixture(StatusCode::METHOD_NOT_ALLOWED).await;
    let (mut socket, _) = connect_async(handshake(&f, None)).await.unwrap();
    assert_eq!(f.a.upgrades.load(Ordering::Relaxed), 0);
    assert_eq!(
        turn(&mut socket, "worker").await.last().unwrap()["response"]["model"],
        "worker"
    );
    assert!(f.a.requests.lock().unwrap().is_empty());
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
    assert!(f.a.requests.lock().unwrap().is_empty());
}
