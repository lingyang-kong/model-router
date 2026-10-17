use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{FromRequestParts, Request, State},
    http::HeaderMap,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use model_router::router;
use serde_json::{Value, json};
use tokio::{sync::Notify, time::timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, client::IntoClientRequest},
};

use crate::common::{Server, parse_config, serve};

#[derive(Clone)]
struct Captured {
    body: Vec<u8>,
    path: String,
    headers: HeaderMap,
}

type Captures = Arc<Mutex<Vec<Captured>>>;

#[derive(Clone)]
struct Upstream {
    seen: Captures,
    calls: Arc<AtomicUsize>,
    mode: Arc<Mutex<String>>,
    entered: Arc<Notify>,
    cancelled: Arc<AtomicBool>,
    probe_mode: Arc<Mutex<String>>,
}

async fn endpoint(State(state): State<Upstream>, request: Request) -> Response {
    if request.headers().get("upgrade").is_some() {
        let (mut parts, _) = request.into_parts();
        let upgrade = axum::extract::WebSocketUpgrade::from_request_parts(&mut parts, &())
            .await
            .unwrap();
        return upgrade.on_upgrade(move |mut socket| async move {
            use axum::extract::ws::Message;
            while let Some(Ok(message)) = socket.recv().await {
                let bytes = match message { Message::Text(t) => t.as_bytes().to_vec(), Message::Binary(b) => b.to_vec(), Message::Close(_) => break, _ => continue };
                state.seen.lock().unwrap().push(Captured { body: bytes.clone(), path: String::new(), headers: HeaderMap::new() });
                let value: Value = serde_json::from_slice(&bytes).unwrap();
                let response = json!({"type":"response.created","stream_id":value.get("stream_id"),"response":{"id":"upstream-response"}});
                if socket.send(Message::Text(response.to_string().into())).await.is_err() { break; }
                let response = json!({"type":"response.completed","response":{"id":"upstream-response","status":"completed"}});
                if socket.send(Message::Text(response.to_string().into())).await.is_err() { break; }
            }
        });
    }
    let (parts, body) = request.into_parts();
    let path = parts.uri.path().to_owned();
    let headers = parts.headers;
    let has_idempotency_key = headers.contains_key("idempotency-key");
    let bytes = to_bytes(body, usize::MAX).await.unwrap().to_vec();
    state.seen.lock().unwrap().push(Captured {
        body: bytes.clone(),
        path,
        headers,
    });
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    let is_probe = value["input"].as_array().is_some_and(|items| {
        items
            .iter()
            .any(|item| item["type"] == "__model_router_capability_check__")
    });
    if is_probe {
        let mode = state.probe_mode.lock().unwrap().clone();
        return match mode.as_str() {
            "custom" => (StatusCode::BAD_REQUEST, Json(json!({"error":{"message":"Unsupported Responses API input item type: 'compaction_trigger'"}}))).into_response(),
            "unknown" => (StatusCode::BAD_REQUEST, Json(json!({"error":{"message":"Invalid input item"}}))).into_response(),
            _ => (StatusCode::BAD_REQUEST, Json(json!({"error":{"message":"Invalid value for max_output_tokens"}}))).into_response(),
        };
    }
    let call = state.calls.fetch_add(1, Ordering::SeqCst);
    let mode = state.mode.lock().unwrap().clone();
    if state.probe_mode.lock().unwrap().as_str() != "custom" {
        let mut response = Response::new(Body::from(bytes));
        response
            .headers_mut()
            .insert("content-type", "application/json".parse().unwrap());
        return response;
    }
    if value["input"].as_array().is_some_and(|a| {
        a.last().is_some_and(|v| {
            v["type"] == "compaction_trigger"
                || v.to_string().contains("Summarize the preceding history")
        })
    }) || (value["store"] == false && value["tools"] == json!([]))
    {
        state.entered.notify_one();
        if mode == "overflow" && has_idempotency_key {
            return (
                StatusCode::CONFLICT,
                Json(json!({"error":{"message":"internal summary reused caller idempotency key"}})),
            )
                .into_response();
        }
        if mode == "delay" {
            struct Guard(Arc<AtomicBool>);
            impl Drop for Guard {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::SeqCst);
                }
            }
            let _guard = Guard(state.cancelled.clone());
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
        if mode == "overflow" && call == 0 {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":{"code":"context_length_exceeded","message":"too long"}})),
            )
                .into_response();
        }
        if mode == "http_error" {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error":{"message":"failed"}})),
            )
                .into_response();
        }
        if mode == "empty" {
            return Json(json!({"status":"completed","output":[]})).into_response();
        }
        if mode == "refusal" {
            return Json(json!({"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"refusal","refusal":"no"}]}]})).into_response();
        }
        if mode == "incomplete" {
            return Json(json!({"status":"incomplete","output":[]})).into_response();
        }
        if mode == "tool" {
            return Json(
                json!({"status":"completed","output":[{"type":"function_call","call_id":"x"}]}),
            )
            .into_response();
        }
        return Json(json!({"id":"upstream-summary","model":"provider-default","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"remembered state"}]}]})).into_response();
    }
    let mut response = Response::new(Body::from(bytes));
    response
        .headers_mut()
        .insert("content-type", "application/json".parse().unwrap());
    response
}

struct Fixture {
    pub server: Server,
    _backend: Server,
    pub upstream: Upstream,
    pub client: reqwest::Client,
}
async fn fixture(mode: &str, probe_mode: &str) -> Fixture {
    fixture_with_headers(
        mode,
        probe_mode,
        json!({"x-provider-secret":"local-secret"}),
    )
    .await
}

async fn fixture_with_headers(mode: &str, probe_mode: &str, headers: Value) -> Fixture {
    let upstream = Upstream {
        seen: Arc::new(Mutex::new(Vec::new())),
        calls: Arc::new(AtomicUsize::new(0)),
        mode: Arc::new(Mutex::new(mode.into())),
        entered: Arc::new(Notify::new()),
        cancelled: Arc::new(AtomicBool::new(false)),
        probe_mode: Arc::new(Mutex::new(probe_mode.into())),
    };
    let backend = serve(
        Router::new()
            .fallback(endpoint)
            .with_state(upstream.clone()),
    )
    .await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(&json!({"providers":{"api":{"base_url":backend.url,"auth":"none","http_headers":headers}},"routes":[{"action":"model","provider":"api"}]}).to_string(),root.path()).unwrap();
    let server = serve(router(config).unwrap()).await;
    Fixture {
        server,
        _backend: backend,
        upstream,
        client: reqwest::Client::builder().no_proxy().build().unwrap(),
    }
}

fn request(input: Value, stream: bool) -> Value {
    json!({"model":"gpt-test","instructions":"retain this","stream":stream,"tools":[{"type":"function","name":"extra"}],"tool_choice":"auto","reasoning":{"effort":"high"},"max_output_tokens":512,"input":input})
}
fn trigger() -> Value {
    json!({"type":"compaction_trigger"})
}

fn summary_requests(seen: &[Captured]) -> Vec<Value> {
    seen.iter()
        .filter_map(|capture| serde_json::from_slice::<Value>(&capture.body).ok())
        .filter(|value| {
            !value["input"].as_array().is_some_and(|items| {
                items
                    .iter()
                    .any(|item| item["type"] == "__model_router_capability_check__")
            })
        })
        .collect()
}

fn is_probe(capture: &Captured) -> bool {
    serde_json::from_slice::<Value>(&capture.body)
        .ok()
        .and_then(|value| value["input"].as_array().cloned())
        .is_some_and(|items| {
            items
                .iter()
                .any(|item| item["type"] == "__model_router_capability_check__")
        })
}

#[tokio::test]
async fn text_trigger_summarizes_on_same_provider_and_streams_one_checkpoint_event() {
    let f = fixture("ok", "custom").await;
    let body = request(
        json!([{"type":"message","role":"user","content":"question"},{"type":"function_call","call_id":"call-1","name":"lookup"},{"type":"function_call_output","call_id":"call-1","output":"found"},trigger()]),
        true,
    );
    let response = f
        .client
        .post(format!("{}/responses", f.server.url))
        .header("originator", "codex_cli_rs")
        .header("idempotency-key", "logical-compaction")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let text = response.text().await.unwrap();
    let events: Vec<Value> = text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    let done: Vec<_> = events
        .iter()
        .filter(|v| v["type"] == "response.output_item.done")
        .collect();
    assert_eq!(done.len(), 1);
    assert_eq!(
        done[0]["item"]["encrypted_content"],
        "model-router:compaction:v1:remembered state"
    );
    assert_eq!(
        events
            .iter()
            .filter(|v| v["type"] == "response.completed")
            .count(),
        1
    );
    let seen = f.upstream.seen.lock().unwrap();
    assert_eq!(summary_requests(&seen).len(), 1);
    let probes: Vec<_> = seen.iter().filter(|capture| is_probe(capture)).collect();
    assert_eq!(probes.len(), 1);
    assert!(!probes[0].headers.contains_key("idempotency-key"));
    let probe: Value = serde_json::from_slice(&probes[0].body).unwrap();
    assert_eq!(probe["max_output_tokens"], 1);
    assert_eq!(probe["store"], false);
    let probe_input = probe["input"].as_array().unwrap();
    assert_eq!(probe_input[0]["type"], "compaction_trigger");
    assert_eq!(probe_input[1]["type"], "__model_router_capability_check__");
    assert_eq!(
        probes[0].headers.get("x-provider-secret").unwrap(),
        "local-secret"
    );
    let summary = summary_requests(&seen)[0].clone();
    assert_eq!(summary["stream"], false);
    assert_eq!(summary["store"], false);
    assert_eq!(summary["tools"], json!([]));
    assert_eq!(summary["tool_choice"], "none");
    assert_eq!(summary["instructions"], "retain this");
    assert_eq!(summary["reasoning"]["effort"], "high");
    assert_eq!(summary["max_output_tokens"], 512);
    let summary_capture = seen.iter().find(|capture| !is_probe(capture)).unwrap();
    assert_eq!(summary_capture.path, "/responses");
    assert!(!summary_capture.headers.contains_key("idempotency-key"));
    assert_eq!(
        summary_capture.headers.get("x-provider-secret").unwrap(),
        "local-secret"
    );
    assert!(summary_capture.headers.get("authorization").is_none());
    let input = summary["input"].as_array().unwrap();
    assert_eq!(input[1]["type"], "function_call");
    assert_eq!(input[2]["type"], "function_call_output");
    assert_eq!(input.last().unwrap()["role"], "user");
    assert!(
        input.last().unwrap()["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("CONTEXT CHECKPOINT COMPACTION")
    );
    assert!(!String::from_utf8_lossy(&summary_capture.body).contains("compaction_trigger"));
}

#[tokio::test]
async fn provider_configured_idempotency_key_is_removed_from_internal_requests() {
    let f = fixture_with_headers(
        "ok",
        "custom",
        json!({"idempotency-key":"provider-static","x-provider-secret":"local-secret"}),
    )
    .await;
    let response = f
        .client
        .post(format!("{}/responses", f.server.url))
        .header("originator", "codex_cli_rs")
        .header("idempotency-key", "caller-key")
        .json(&request(json!([trigger()]), false))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let seen = f.upstream.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert!(
        seen.iter()
            .all(|capture| !capture.headers.contains_key("idempotency-key"))
    );
    assert!(
        seen.iter()
            .all(|capture| capture.headers.get("x-provider-secret").unwrap() == "local-secret")
    );
}

#[tokio::test]
async fn omitted_http_model_probes_and_summarizes_using_provider_default() {
    let f = fixture("ok", "custom").await;
    let mut body = request(
        json!([{"type":"message","role":"user","content":"history"},trigger()]),
        false,
    );
    body.as_object_mut().unwrap().remove("model");
    let response = f
        .client
        .post(format!("{}/responses", f.server.url))
        .header("originator", "codex_cli_rs")
        .header("idempotency-key", "default-model-compaction")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response: Value = response.json().await.unwrap();
    assert_eq!(response["model"], "provider-default");
    assert_eq!(response["output"][0]["type"], "compaction");
    let seen = f.upstream.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert!(is_probe(&seen[0]));
    assert!(!is_probe(&seen[1]));
    for capture in seen.iter() {
        let body: Value = serde_json::from_slice(&capture.body).unwrap();
        assert!(body.get("model").is_none());
        assert!(!capture.headers.contains_key("idempotency-key"));
    }
}

#[tokio::test]
async fn omitted_http_model_native_or_unknown_probe_preserves_bytes_and_caller_key() {
    for probe_mode in ["native", "unknown"] {
        let f = fixture("ok", probe_mode).await;
        let raw = br#"{ "input": [{"type":"compaction_trigger"}], "stream":false }"#;
        let response = f
            .client
            .post(format!("{}/responses", f.server.url))
            .header("originator", "codex_cli_rs")
            .header("idempotency-key", "original-operation")
            .body(raw.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{probe_mode}");
        assert_eq!(
            response.bytes().await.unwrap().as_ref(),
            raw,
            "{probe_mode}"
        );
        let seen = f.upstream.seen.lock().unwrap();
        assert_eq!(seen.len(), 2, "{probe_mode}");
        assert!(is_probe(&seen[0]));
        assert!(!seen[0].headers.contains_key("idempotency-key"));
        let probe: Value = serde_json::from_slice(&seen[0].body).unwrap();
        assert!(probe.get("model").is_none());
        assert_eq!(seen[1].body, raw);
        assert_eq!(
            seen[1].headers.get("idempotency-key").unwrap(),
            "original-operation"
        );
    }
}

#[tokio::test]
async fn native_trigger_and_ordinary_native_request_are_forwarded_unchanged() {
    let f = fixture("ok", "native").await;
    let raw = br#"{ "model":"gpt-test", "input":[{"type":"compaction_trigger"}], "stream":false }"#;
    let response = f
        .client
        .post(format!("{}/responses", f.server.url))
        .header("originator", "codex_cli_rs")
        .body(raw.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(response.bytes().await.unwrap().as_ref(), raw);
    let seen = f.upstream.seen.lock().unwrap();
    assert!(seen.iter().any(is_probe));
    assert_eq!(
        seen.iter().find(|capture| !is_probe(capture)).unwrap().body,
        raw
    );
}

#[tokio::test]
async fn checkpoints_rehydrate_for_repeat_text_compaction_and_native_mode_switch() {
    let f = fixture("ok", "custom").await;
    let first = f
        .client
        .post(format!("{}/responses", f.server.url))
        .header("originator", "codex_cli_rs")
        .json(&request(
            json!([{"type":"message","role":"user","content":"first"},trigger()]),
            false,
        ))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let saved = first["output"][0].clone();
    let repeated = request(
        json!([saved.clone(),{"type":"message","role":"user","content":"second"},trigger()]),
        false,
    );
    f.client
        .post(format!("{}/responses", f.server.url))
        .header("originator", "codex_cli_rs")
        .json(&repeated)
        .send()
        .await
        .unwrap();
    let seen = f.upstream.seen.lock().unwrap().clone();
    let summaries = summary_requests(&seen);
    assert_eq!(summaries.len(), 2);
    let second = summaries[1].clone();
    assert!(
        second["input"][0]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("remembered state")
    );
    let native = fixture("ok", "native").await;
    let response = native.client.post(format!("{}/responses", native.server.url)).header("originator", "codex_cli_rs").json(&json!({"model":"gpt-test","input":[saved,{"type":"message","role":"user","content":"continue"}]})).send().await.unwrap();
    let native_input: Value = summary_requests(&native.upstream.seen.lock().unwrap())[0].clone();
    assert_eq!(native_input["input"][0]["type"], "message");
    assert!(
        native_input["input"][0]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("remembered state")
    );
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn failures_never_emit_a_checkpoint_and_only_context_overflow_retries_after_safe_trim() {
    for mode in ["http_error", "empty", "refusal", "incomplete", "tool"] {
        let f = fixture(mode, "custom").await;
        let response = f
            .client
            .post(format!("{}/responses", f.server.url))
            .header("originator", "codex_cli_rs")
            .json(&request(
                json!([{"type":"message","role":"user","content":"latest"},trigger()]),
                false,
            ))
            .send()
            .await
            .unwrap();
        assert!(
            !response
                .text()
                .await
                .unwrap()
                .contains("model-router:compaction:v1:"),
            "{mode}"
        );
        assert_eq!(
            summary_requests(&f.upstream.seen.lock().unwrap()).len(),
            1,
            "{mode}"
        );
    }
    let f = fixture("overflow", "custom").await;
    let input = json!([{"type":"message","role":"system","content":"system"},{"type":"message","role":"developer","content":"developer"},{"type":"function_call","call_id":"c1","name":"old"},{"type":"message","role":"user","content":"middle"},{"type":"function_call_output","call_id":"c1","output":"old result"},{"type":"message","role":"user","content":"latest"},trigger()]);
    let response = f
        .client
        .post(format!("{}/responses", f.server.url))
        .header("originator", "codex_cli_rs")
        .header("idempotency-key", "overflow-compaction")
        .json(&request(input, false))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let seen = f.upstream.seen.lock().unwrap().clone();
    let summaries = summary_requests(&seen);
    assert_eq!(summaries.len(), 2);
    assert!(
        seen.iter()
            .all(|capture| !capture.headers.contains_key("idempotency-key"))
    );
    let retried = summaries[1].clone();
    let input = retried["input"].as_array().unwrap();
    assert_eq!(input[0]["content"], "system");
    assert_eq!(input[1]["content"], "developer");
    assert_eq!(input.iter().filter(|v| v["call_id"] == "c1").count(), 0);
    assert_eq!(input.iter().filter(|v| v["content"] == "latest").count(), 1);
}

#[tokio::test]
async fn unknown_checkpoint_without_trigger_is_passed_to_provider() {
    let body =
        json!({"model":"gpt-test","input":[{"type":"compaction","encrypted_content":"opaque"}]});
    let custom = fixture("ok", "custom").await;
    let raw = serde_json::to_vec(&body).unwrap();
    let response = custom
        .client
        .post(format!("{}/responses", custom.server.url))
        .header("originator", "codex_cli_rs")
        .body(raw.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(custom.upstream.seen.lock().unwrap()[0].body, raw);
    let native = fixture("ok", "native").await;
    native
        .client
        .post(format!("{}/responses", native.server.url))
        .header("originator", "codex_cli_rs")
        .body(raw.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(native.upstream.seen.lock().unwrap()[0].body, raw);
}

#[tokio::test]
async fn only_exact_compaction_item_error_enables_fallback_and_probe_is_fresh_per_trigger() {
    let f = fixture("ok", "unknown").await;
    let raw = serde_json::to_vec(&request(json!([trigger()]), false)).unwrap();
    let response = f
        .client
        .post(format!("{}/responses", f.server.url))
        .header("originator", "codex_cli_rs")
        .body(raw.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let seen = f.upstream.seen.lock().unwrap().clone();
    assert_eq!(seen.iter().filter(|capture| is_probe(capture)).count(), 1);
    assert_eq!(
        seen.iter().find(|capture| !is_probe(capture)).unwrap().body,
        raw
    );

    let f = fixture("ok", "custom").await;
    let first = f
        .client
        .post(format!("{}/responses", f.server.url))
        .header("originator", "codex_cli_rs")
        .json(&request(json!([trigger()]), false))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    *f.upstream.probe_mode.lock().unwrap() = "unknown".into();
    let raw = serde_json::to_vec(&request(json!([trigger()]), false)).unwrap();
    let second = f
        .client
        .post(format!("{}/responses", f.server.url))
        .header("originator", "codex_cli_rs")
        .body(raw.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let seen = f.upstream.seen.lock().unwrap().clone();
    assert_eq!(seen.iter().filter(|capture| is_probe(capture)).count(), 2);
    assert_eq!(summary_requests(&seen).len(), 2);
    assert_eq!(
        seen.iter()
            .find(|capture| !is_probe(capture) && capture.body.as_slice() == raw.as_slice())
            .unwrap()
            .body,
        raw
    );
}

#[tokio::test]
async fn dropping_sse_cancels_pending_summary_request() {
    let f = fixture("delay", "custom").await;
    let client = f.client.clone();
    let url = format!("{}/responses", f.server.url);
    let task = tokio::spawn(async move {
        client
            .post(url)
            .header("originator", "codex_cli_rs")
            .json(&request(
                json!([{"type":"message","role":"user","content":"x"},trigger()]),
                true,
            ))
            .send()
            .await
    });
    timeout(Duration::from_secs(2), f.upstream.entered.notified())
        .await
        .unwrap();
    // The response begins with a heartbeat while the summary is pending.
    let response = task.await.unwrap().unwrap();
    let mut stream = response.bytes_stream();
    let _ = timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap();
    drop(stream);
    timeout(Duration::from_secs(2), async {
        while !f.upstream.cancelled.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn websocket_summary_uses_handshake_model_unless_body_overrides_it() {
    for body_model in [None, Some("body-model")] {
        let f = fixture("ok", "custom").await;
        let mut handshake = format!("{}/responses", f.server.url.replace("http:", "ws:"))
            .into_client_request()
            .unwrap();
        handshake
            .headers_mut()
            .insert("originator", "codex_cli_rs".parse().unwrap());
        handshake
            .headers_mut()
            .insert("x-model", "handshake-model".parse().unwrap());
        handshake
            .headers_mut()
            .insert("idempotency-key", "websocket-operation".parse().unwrap());
        let (mut socket, _) = timeout(Duration::from_secs(2), connect_async(handshake))
            .await
            .unwrap()
            .unwrap();
        let mut body =
            json!({"input":[{"type":"message","role":"user","content":"history"},trigger()]});
        if let Some(model) = body_model {
            body["model"] = json!(model);
        }
        socket
            .send(tungstenite::Message::Text(body.to_string().into()))
            .await
            .unwrap();
        loop {
            let message = timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let event: Value = serde_json::from_slice(&message.into_data()).unwrap();
            assert_ne!(event["type"], "error", "{event}");
            if event["type"] == "response.completed" {
                break;
            }
        }
        let seen = f.upstream.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 2);
        assert!(is_probe(&seen[0]));
        assert!(!is_probe(&seen[1]));
        for capture in &seen {
            assert_eq!(capture.path, "/responses");
            let body: Value = serde_json::from_slice(&capture.body).unwrap();
            assert_eq!(body["model"], body_model.unwrap_or("handshake-model"));
            assert!(!capture.headers.contains_key("idempotency-key"));
        }
        timeout(Duration::from_secs(2), socket.close(None))
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn websocket_trigger_uses_http_summary_and_native_messages_still_forward() {
    let f = fixture("ok", "custom").await;
    let url = format!("{}/responses", f.server.url.replace("http:", "ws:"));
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer client".parse().unwrap());
    request
        .headers_mut()
        .insert("originator", "codex_cli_rs".parse().unwrap());
    request
        .headers_mut()
        .insert("x-model", "gpt-test".parse().unwrap());
    let (mut socket, _) = connect_async(request).await.unwrap();
    socket.send(tungstenite::Message::Text(json!({"model":"gpt-test","stream_id":"s-1","input":[{"type":"message","role":"user","content":"history"},trigger()]}).to_string().into())).await.unwrap();
    let event: Value =
        serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
    assert_eq!(event["type"], "response.created");
    assert_eq!(event["stream_id"], "s-1");
    let synthetic_id = event["response"]["id"].as_str().unwrap().to_owned();
    let event: Value =
        serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
    assert_eq!(event["type"], "response.output_item.added");
    assert_eq!(event["item"]["type"], "compaction");
    let done: Value =
        serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
    assert_eq!(done["type"], "response.output_item.done");
    let completed: Value =
        serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
    assert_eq!(completed["type"], "response.completed");
    let native = json!({"model":"gpt-test","input":[{"type":"message","role":"user","content":"ordinary native websocket request"}]});
    socket
        .send(tungstenite::Message::Text(native.to_string().into()))
        .await
        .unwrap();
    let reply: Value =
        serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
    assert_eq!(reply["type"], "response.created");
    let seen = f.upstream.seen.lock().unwrap().clone();
    assert_eq!(
        serde_json::from_slice::<Value>(&seen.last().unwrap().body).unwrap(),
        native
    );
    let native_completed: Value =
        serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
    assert_eq!(native_completed["type"], "response.completed");
    socket
        .send(tungstenite::Message::Text(
            json!({"model":"gpt-test","previous_response_id":synthetic_id,"input":[]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let error: Value =
        serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
    assert_eq!(error["error"]["code"], "previous_response_not_found");
    socket.send(tungstenite::Message::Text(json!({"model":"gpt-test","input":[{"type":"message","role":"user","content":"full replay"},trigger()]}).to_string().into())).await.unwrap();
    let replay: Value =
        serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
    assert_eq!(replay["type"], "response.created");
    socket.close(None).await.unwrap();
}

#[tokio::test]
async fn closing_websocket_cancels_pending_http_summary() {
    let f = fixture("delay", "custom").await;
    let url = format!("{}/responses", f.server.url.replace("http:", "ws:"));
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("originator", "codex_cli_rs".parse().unwrap());
    request
        .headers_mut()
        .insert("x-model", "gpt-test".parse().unwrap());
    let (mut socket, _) = connect_async(request).await.unwrap();
    socket.send(tungstenite::Message::Text(json!({"model":"gpt-test","stream_id":"cancel-1","input":[{"type":"message","role":"user","content":"history"},trigger()]}).to_string().into())).await.unwrap();
    timeout(Duration::from_secs(2), f.upstream.entered.notified())
        .await
        .unwrap();
    socket.close(None).await.unwrap();
    timeout(Duration::from_secs(2), async {
        while !f.upstream.cancelled.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
