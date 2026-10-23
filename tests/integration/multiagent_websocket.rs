use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    Router,
    body::to_bytes,
    extract::{FromRequestParts, Request, State, WebSocketUpgrade, ws::Message},
    http::StatusCode,
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use model_router::router;
use serde_json::{Value, json};
use tokio::{sync::Notify, time::timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        self,
        client::IntoClientRequest,
        protocol::frame::{
            Frame,
            coding::{Data, OpCode},
        },
    },
};

use crate::common::{parse_config, serve};

type CloseRecord = Option<(u16, String)>;

#[derive(Clone)]
struct FloodState {
    started: Arc<Notify>,
    finished: Arc<Notify>,
}

struct FinishedOnDrop(Arc<Notify>);

impl Drop for FinishedOnDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

async fn flooding_upstream(State(state): State<FloodState>, request: Request) -> Response {
    let (mut parts, _) = request.into_parts();
    let websocket = WebSocketUpgrade::from_request_parts(&mut parts, &())
        .await
        .unwrap();
    websocket
        .max_message_size(40 * 1024 * 1024)
        .max_frame_size(40 * 1024 * 1024)
        .on_upgrade(move |mut socket| async move {
            let _finished = FinishedOnDrop(state.finished);
            let payload = "x".repeat(16 * 1024 * 1024);
            state.started.notify_one();
            for _ in 0..8 {
                if socket
                    .send(Message::Text(payload.clone().into()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        })
}

#[tokio::test]
async fn adapted_websocket_close_cancels_an_upstream_write_blocked_by_client_backpressure() {
    let state = FloodState {
        started: Arc::new(Notify::new()),
        finished: Arc::new(Notify::new()),
    };
    let upstream_server = serve(
        Router::new()
            .fallback(flooding_upstream)
            .with_state(FloodState {
                started: state.started.clone(),
                finished: state.finished.clone(),
            }),
    )
    .await;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("models.json"),
        json!({"models":[{"slug":"publisher/model"}]}).to_string(),
    )
    .unwrap();
    let config = parse_config(&json!({
        "clients":{"codex":{"provider":"parent"},"openai":{"provider":"parent"},"anthropic":{"provider":"parent"}},"providers":{"parent":{"base_url":upstream_server.url,"auth":"none","catalog":{"file":"models.json","format":"codex"}}},
        "routes":[{"action":"model"}]
    }).to_string(), root.path()).unwrap();
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
    timeout(Duration::from_secs(2), state.started.notified())
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(100), state.finished.notified())
            .await
            .is_err(),
        "upstream write completed without client backpressure"
    );
    socket
        .send(tungstenite::Message::Close(Some(
            tungstenite::protocol::CloseFrame {
                code: tungstenite::protocol::frame::coding::CloseCode::Away,
                reason: "cancel backpressure".into(),
            },
        )))
        .await
        .unwrap();
    timeout(Duration::from_secs(3), state.finished.notified())
        .await
        .expect("upstream WebSocket remained alive after client close");
}

#[derive(Clone, Default)]
struct Capture {
    messages: Arc<Mutex<Vec<Value>>>,
    probes: Arc<Mutex<Vec<Value>>>,
    raw_text: Arc<Mutex<Vec<String>>>,
    binary: Arc<Mutex<Vec<Vec<u8>>>>,
    extensions: Arc<Mutex<Vec<Option<String>>>>,
    closes: Arc<Mutex<Vec<CloseRecord>>>,
}

async fn probe(State(capture): State<Capture>, request: Request) -> Response {
    let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
    let document: Value = serde_json::from_slice(&bytes).unwrap();
    capture.probes.lock().unwrap().push(document.clone());
    let message_probe = document["input"][0]["type"] == "agent_message";
    let (status, body) = if message_probe {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            r#"{"error":{"message":"Unsupported Responses API input item type: 'agent_message'"}}"#,
        )
    } else {
        (
            StatusCode::BAD_REQUEST,
            r#"{"error":{"message":"Unsupported Responses API input item type: '__model_router_capability_check__'"}}"#,
        )
    };
    Response::builder()
        .status(status)
        .body(axum::body::Body::from(body))
        .unwrap()
}

async fn upstream(State(capture): State<Capture>, request: Request) -> Response {
    let (mut parts, _) = request.into_parts();
    capture.extensions.lock().unwrap().push(
        parts
            .headers
            .get("sec-websocket-extensions")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
    );
    let websocket = WebSocketUpgrade::from_request_parts(&mut parts, &())
        .await
        .unwrap();
    websocket.max_message_size(32 * 1024 * 1024).max_frame_size(32 * 1024 * 1024).on_upgrade(move |mut socket| async move {
        while let Some(Ok(message)) = socket.recv().await {
            match message {
                Message::Ping(data) => { if socket.send(Message::Pong(data)).await.is_err() { return; } }
                Message::Close(frame) => {
                    capture.closes.lock().unwrap().push(frame.as_ref().map(|frame| (frame.code, frame.reason.to_string())));
                    let _ = socket.send(Message::Close(frame)).await;
                    return;
                }
                Message::Text(text) => {
                    let Ok(document) = serde_json::from_str::<Value>(&text) else {
                        capture.raw_text.lock().unwrap().push(text.to_string());
                        let _ = socket.send(Message::Text(text)).await;
                        continue;
                    };
                    capture.messages.lock().unwrap().push(document);
                    let events = [
                        json!({"type":"response.output_item.added","item":{"type":"function_call","namespace":"collaboration_compat","name":"spawn_agent","call_id":"c1","arguments":"{}"}}),
                        json!({"type":"response.output_item.done","item":{"type":"function_call","namespace":"collaboration_compat","name":"spawn_agent","call_id":"c1","arguments":"{}"}}),
                        json!({"type":"response.completed","response":{"object":"response","output":[
                            {"type":"function_call","namespace":"collaboration_compat","name":"spawn_agent","call_id":"c1","arguments":"{}"},
                            {"type":"agent_message","author":"worker","content":[{"type":"input_text","text":"native"}]}
                        ]}}),
                    ];
                    for event in events {
                        if socket.send(Message::Text(event.to_string().into())).await.is_err() { return; }
                    }
                }
                Message::Binary(bytes) => {
                    capture.binary.lock().unwrap().push(bytes.to_vec());
                    if socket.send(Message::Binary(bytes)).await.is_err() { return; }
                }
                _ => {}
            }
        }
    })
}

#[tokio::test]
async fn codex_multiagent_websocket_transforms_requests_and_restores_events() {
    let capture = Capture::default();
    let upstream_server = serve(
        Router::new()
            .route("/responses", axum::routing::post(probe).get(upstream))
            .fallback(upstream)
            .with_state(capture.clone()),
    )
    .await;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("models.json"),
        json!({"models":[{"slug":"publisher/model"},{"slug":"alternate/model"}]}).to_string(),
    )
    .unwrap();
    let config = parse_config(&json!({
        "clients":{"codex":{"provider":"parent"},"openai":{"provider":"parent"},"anthropic":{"provider":"parent"}},"providers":{"parent":{"base_url":upstream_server.url,"auth":"none","catalog":{"file":"models.json","format":"codex"}}},
        "routes":[{"action":"model"}]
    }).to_string(), root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let mut request = (gateway.url.replace("http:", "ws:") + "/responses")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("originator", "codex_cli_rs".parse().unwrap());
    request.headers_mut().insert(
        "x-codex-routing-hint",
        "model=alternate/model".parse().unwrap(),
    );
    request.headers_mut().insert(
        "sec-websocket-extensions",
        "permessage-deflate; client_max_window_bits"
            .parse()
            .unwrap(),
    );
    let (mut socket, _) = connect_async(request).await.unwrap();
    let original = json!({"type":"response.create","model":"publisher/model","tools":[{"type":"namespace","name":"collaboration","tools":[
        {"type":"function","name":"spawn_agent","description":"spawn","parameters":{"properties":{"message":{"type":"string","encrypted":true}}}}
    ]}],"input":[
        {"type":"additional_tools","tools":[{"type":"namespace","name":"collaboration","tools":[
            {"type":"function","name":"send_message","parameters":{"properties":{"message":{"type":"string","encrypted":true}}}}
        ]}]},
        {"type":"function_call","namespace":"collaboration","name":"spawn_agent","call_id":"c1","arguments":"{}"},
        {"type":"function_call_output","call_id":"c1","output":"done"},
        {"type":"agent_message","author":"worker","recipient":"parent","internal_chat_message_metadata_passthrough":{"opaque":true},"content":[{"type":"encrypted_content","encrypted_content":"secret text"}]}
    ]});
    let encoded = original
        .to_string()
        .replace("agent_message", r"agent\u005fmessage");
    let split = encoded.len() / 2;
    socket
        .send(tungstenite::Message::Frame(Frame::message(
            encoded[..split].to_owned(),
            OpCode::Data(Data::Text),
            false,
        )))
        .await
        .unwrap();
    socket
        .send(tungstenite::Message::Frame(Frame::message(
            encoded[split..].to_owned(),
            OpCode::Data(Data::Continue),
            true,
        )))
        .await
        .unwrap();
    let mut received = Vec::new();
    for _ in 0..3 {
        let bytes = timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_data();
        received.push(serde_json::from_slice::<Value>(&bytes).unwrap());
    }
    let wire = capture.messages.lock().unwrap()[0].clone();
    assert_eq!(capture.probes.lock().unwrap().len(), 1);
    assert_eq!(
        capture.probes.lock().unwrap()[0]["model"],
        "publisher/model"
    );
    assert_eq!(
        capture.probes.lock().unwrap()[0]["input"][1]["type"],
        "__model_router_capability_check__"
    );
    assert_eq!(wire["tools"][0]["name"], "collaboration_compat");
    assert!(
        wire["tools"][0]["tools"][0]["parameters"]["properties"]["message"]
            .get("encrypted")
            .is_none()
    );
    assert_eq!(wire["input"][0]["tools"][0]["name"], "collaboration_compat");
    assert_eq!(wire["input"][1]["namespace"], "collaboration_compat");
    assert_eq!(wire["input"][1]["name"], "spawn_agent");
    assert_eq!(wire["input"][2]["type"], "function_call_output");
    assert_eq!(wire["input"][3]["type"], "message");
    assert_eq!(wire["input"][3]["role"], "user");
    assert_eq!(
        wire["input"][3]["content"][0],
        json!({"type":"input_text","text":"secret text"})
    );
    for event in &received[..2] {
        assert_eq!(event["item"]["namespace"], "collaboration");
        assert_eq!(event["item"]["encrypted_function_args"], json!([]));
    }
    assert_eq!(
        received[2]["response"]["output"][0]["encrypted_function_args"],
        json!([])
    );
    assert_eq!(
        received[2]["response"]["output"][0]["namespace"],
        "collaboration"
    );
    assert_eq!(
        received[2]["response"]["output"][1]["type"],
        "agent_message"
    );
    assert_eq!(capture.extensions.lock().unwrap()[0], None);
    socket
        .send(tungstenite::Message::Ping(vec![7, 8].into()))
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        tungstenite::Message::Pong(vec![7, 8].into())
    );
    // The frame's model overrides the handshake hint for probing as well as routing.
    assert_eq!(
        capture.probes.lock().unwrap()[0]["model"],
        "publisher/model"
    );
    // A frame without a model falls back to the handshake hint.
    let no_frame_model = json!({"type":"response.create","input":[{"type":"agent_message","author":"worker","content":[{"type":"input_text","text":"fallback"}]}]});
    socket
        .send(tungstenite::Message::Text(
            no_frame_model.to_string().into(),
        ))
        .await
        .unwrap();
    for _ in 0..3 {
        timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    assert_eq!(capture.probes.lock().unwrap().len(), 2);
    assert_eq!(
        capture.probes.lock().unwrap()[1]["model"],
        "alternate/model"
    );
    let plain = "this is not JSON";
    socket
        .send(tungstenite::Message::Text(plain.into()))
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        tungstenite::Message::Text(plain.into())
    );
    let binary = vec![0, 255, 9];
    socket
        .send(tungstenite::Message::Binary(binary.clone().into()))
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        tungstenite::Message::Binary(binary.clone().into())
    );
    assert_eq!(capture.raw_text.lock().unwrap()[0], plain);
    assert_eq!(capture.binary.lock().unwrap()[0], binary);
    let large = format!(
        "{{\"model\":\"publisher/model\",\"padding\":\"{}\"}}",
        "x".repeat(16 * 1024 * 1024)
    );
    socket
        .send(tungstenite::Message::Text(large.into()))
        .await
        .unwrap();
    for _ in 0..3 {
        timeout(Duration::from_secs(5), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    assert_eq!(capture.messages.lock().unwrap().len(), 3);
    socket
        .send(tungstenite::Message::Close(Some(
            tungstenite::protocol::CloseFrame {
                code: tungstenite::protocol::frame::coding::CloseCode::Away,
                reason: "finished".into(),
            },
        )))
        .await
        .unwrap();
    let close = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        close,
        tungstenite::Message::Close(Some(tungstenite::protocol::CloseFrame {
            code: tungstenite::protocol::frame::coding::CloseCode::Away,
            reason: "finished".into(),
        }))
    );
    timeout(Duration::from_secs(2), async {
        loop {
            if !capture.closes.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("upstream did not observe the close frame");
    assert_eq!(
        capture.closes.lock().unwrap()[0],
        Some((1001, "finished".into()))
    );
}
