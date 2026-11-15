use std::{io::Cursor, time::Duration};

use async_compression::tokio::bufread::GzipEncoder;
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{FromRequestParts, Request, WebSocketUpgrade, ws::Message},
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, BufReader},
    time::timeout,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, client::IntoClientRequest},
};

use crate::common::{parse_config, serve};

async fn capture(name: &'static str, request: Request) -> Response {
    let (mut parts, body) = request.into_parts();
    let hint = parts
        .headers
        .get("x-codex-routing-hint")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    if parts.headers.contains_key("upgrade") {
        return WebSocketUpgrade::from_request_parts(&mut parts, &())
            .await
            .unwrap()
            .on_upgrade(move |mut socket| async move {
                while let Some(Ok(Message::Text(text))) = socket.next().await {
                    let body: Value = serde_json::from_str(&text).unwrap();
                    let result = json!({"provider": name, "hint": hint, "body": body});
                    socket
                        .send(Message::Text(result.to_string().into()))
                        .await
                        .unwrap();
                }
            });
    }
    let headers = parts
        .headers
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|v| (k.to_string(), v.to_string())))
        .collect::<std::collections::BTreeMap<_, _>>();
    let body = to_bytes(body, usize::MAX).await.unwrap();
    Json(json!({"provider": name, "headers": headers, "body": String::from_utf8_lossy(&body)}))
        .into_response()
}

async fn fixture() -> (
    crate::common::Server,
    crate::common::Server,
    crate::common::Server,
) {
    fixture_with_overrides(json!({
        "gpt-5.6-*": "gpt-6-*",
        "gpt-6-sol": "gpt-6.1-sol"
    }))
    .await
}

async fn fixture_with_overrides(
    overrides: Value,
) -> (
    crate::common::Server,
    crate::common::Server,
    crate::common::Server,
) {
    let old = serve(Router::new().fallback(|request| capture("old", request))).await;
    let new = serve(Router::new().fallback(|request| capture("new", request))).await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(&json!({
        "model_overrides": overrides,
        "providers": {
            "old": {"base_url": old.url, "auth": "none", "models": {"gpt-5.6-luna": {}}},
            "new": {"base_url": new.url, "auth": "none", "models": {"gpt-6-luna": {}, "gpt-6.1-sol": {}}}
        },
        "clients": {"codex": {"provider": "old"}, "openai": {"provider": "old"}, "anthropic": {"provider": "old"}},
        "routes": [{"path": "/responses", "action": "model"}, {"action": "forward"}]
    }).to_string(), root.path()).unwrap();
    let gateway = serve(model_router::router(config).unwrap()).await;
    (gateway, old, new)
}

#[tokio::test]
async fn http_override_selects_target_and_preserves_unmatched_and_forward_requests() {
    let (gateway, _old, _new) = fixture().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for (header, value) in [
        ("originator", "codex_cli_rs"),
        ("originator", "other"),
        ("anthropic-version", "2023-06-01"),
    ] {
        for (model, target) in [
            ("gpt-5.6-luna", "gpt-6-luna"),
            ("gpt-5.6-sol", "gpt-6.1-sol"),
        ] {
            let response: Value = client
                .post(gateway.url.clone() + "/responses")
                .header(header, value)
                .json(&json!({"model":model,"input":"hello"}))
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(response["provider"], "new");
            let body: Value = serde_json::from_str(response["body"].as_str().unwrap()).unwrap();
            assert_eq!(body["model"], target);
            assert_eq!(body["input"], "hello");
        }
    }
    for (path, input) in [
        ("/responses", "{ \"model\": \"other\", \"input\": 1 }"),
        ("/other", "{ \"model\": \"gpt-5.6-luna\" }"),
    ] {
        let response: Value = client
            .post(gateway.url.clone() + path)
            .body(input)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response["provider"], "old");
        assert_eq!(response["body"], input);
    }
    let input = br#"{"model":"gpt-5.6-luna","input":"compressed"}"#;
    let mut compressed = Vec::new();
    GzipEncoder::new(BufReader::new(Cursor::new(input)))
        .read_to_end(&mut compressed)
        .await
        .unwrap();
    for originator in ["codex_cli_rs", "other"] {
        let response: Value = client
            .post(gateway.url.clone() + "/responses")
            .header("originator", originator)
            .header("content-encoding", "gzip")
            .header("content-md5", "stale")
            .body(compressed.clone())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response["provider"], "new");
        assert!(response["headers"].get("content-encoding").is_none());
        assert!(response["headers"].get("content-md5").is_none());
        let body: Value = serde_json::from_str(response["body"].as_str().unwrap()).unwrap();
        assert_eq!(body["model"], "gpt-6-luna");
    }
}

#[tokio::test]
async fn websocket_override_rewrites_hint_and_frames_and_switches_provider() {
    let (gateway, _old, _new) = fixture().await;
    let mut request = (gateway.url.replace("http:", "ws:") + "/responses")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("originator", "codex_cli_rs".parse().unwrap());
    request.headers_mut().insert(
        "x-codex-routing-hint",
        "model=gpt-5.6-luna".parse().unwrap(),
    );
    let (mut socket, _) = connect_async(request).await.unwrap();
    for (body, expected_provider, expected_model, expected_hint) in [
        (
            json!({"type":"response.create", "model":"gpt-5.6-luna"}),
            "new",
            Some("gpt-6-luna"),
            "model=gpt-6-luna",
        ),
        (
            json!({"type":"response.create"}),
            "new",
            None,
            "model=gpt-6-luna",
        ),
        (
            json!({"type":"response.create", "model":"gpt-5.7-luna"}),
            "old",
            Some("gpt-5.7-luna"),
            "model=gpt-5.7-luna",
        ),
        (
            json!({"type":"response.create", "model":"gpt-5.6-sol"}),
            "new",
            Some("gpt-6.1-sol"),
            "model=gpt-6.1-sol",
        ),
        (
            json!({"type":"response.create", "model":"gpt-5.6-luna"}),
            "new",
            Some("gpt-6-luna"),
            // Both models use this connection; its handshake remains fixed.
            "model=gpt-6.1-sol",
        ),
    ] {
        socket
            .send(tungstenite::Message::Text(body.to_string().into()))
            .await
            .unwrap();
        let response = timeout(Duration::from_secs(5), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let response: Value = serde_json::from_slice(&response.into_data()).unwrap();
        assert_eq!(response["provider"], expected_provider);
        assert_eq!(response["hint"], expected_hint);
        assert_eq!(response["body"]["model"], json!(expected_model));
    }
}

#[tokio::test]
async fn conflicts_outside_startup_analysis_fail_before_http_and_websocket_inference() {
    for (rules, expected_error) in [(json!({"a[0-9]*":"one", "*[0-9]x":"two"}), "Conflicting")] {
        let (gateway, _old, _new) = fixture_with_overrides(rules).await;
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        for originator in ["codex_cli_rs", "other"] {
            let response = client
                .post(gateway.url.clone() + "/responses")
                .header("originator", originator)
                .json(&json!({"model":"a9x"}))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
            assert!(response.text().await.unwrap().contains(expected_error));
        }
        let mut request = (gateway.url.replace("http:", "ws:") + "/responses")
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("originator", "codex_cli_rs".parse().unwrap());
        let (mut socket, _) = connect_async(request.clone()).await.unwrap();
        socket
            .send(tungstenite::Message::Text(
                json!({"type":"response.create", "model":"a9x"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let event = timeout(Duration::from_secs(5), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let event: Value = serde_json::from_slice(&event.into_data()).unwrap();
        assert_eq!(event["type"], "error");
        assert!(
            event["error"]["message"]
                .as_str()
                .unwrap()
                .contains(expected_error)
        );

        // A rejected request leaves the WebSocket usable for valid models.
        socket
            .send(tungstenite::Message::Text(
                json!({"type":"response.create", "model":"safe"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let event = timeout(Duration::from_secs(5), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let event: Value = serde_json::from_slice(&event.into_data()).unwrap();
        assert_eq!(event["provider"], "old");
        assert_eq!(event["body"]["model"], "safe");

        request
            .headers_mut()
            .insert("x-codex-routing-hint", "model=a9x".parse().unwrap());
        let error = connect_async(request).await.unwrap_err();
        let tungstenite::Error::Http(response) = error else {
            panic!("expected an HTTP rejection")
        };
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    }
}
