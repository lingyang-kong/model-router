use std::time::Duration;

use axum::{
    Router,
    extract::{FromRequestParts, Request, WebSocketUpgrade, ws::Message},
    http::StatusCode,
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use model_router::router;
use serde_json::{Value, json};
use tokio::{io::AsyncReadExt, net::TcpListener, sync::oneshot, time::timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, client::IntoClientRequest},
};

use crate::common::*;

#[tokio::test]
async fn catalog_refresh_does_not_change_a_websocket_destination() {
    let fixture = Fixture::new().await;
    let (mut socket, _) = connect_async(ws_request(
        fixture.routed.url.replace("http:", "ws:") + "/responses",
        "service-a",
    ))
    .await
    .unwrap();
    socket
        .send(tungstenite::Message::Text(
            json!({"model":"gpt-test"}).to_string().into(),
        ))
        .await
        .unwrap();
    let response = socket.next().await.unwrap().unwrap().into_data();
    assert_eq!(
        serde_json::from_slice::<Value>(&response).unwrap()["provider"],
        "service-a"
    );
    *fixture.live.models.lock().unwrap() = vec![json!({"slug":"new-websocket-model"})];
    assert_eq!(
        fixture
            .client
            .get(fixture.routed.url.clone() + "/models")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    socket
        .send(tungstenite::Message::Binary(
            json!({"model":"new-websocket-model"})
                .to_string()
                .into_bytes()
                .into(),
        ))
        .await
        .unwrap();
    let response = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data();
    let response: Value = serde_json::from_slice(&response).unwrap();
    assert_eq!(response["provider"], "service-a");
    assert_eq!(response["model"], "new-websocket-model");
}

#[tokio::test]
async fn model_websockets_select_a_provider_from_handshake_hints() {
    let fixture = Fixture::new().await;
    let config=parse_config(&json!({"providers":{
        "live":{"base_url":fixture._live_server.url,"auth":"none"},
        "manual":{"base_url":fixture._manual_server.url,"auth":"none","catalog":{"file":"models.json","format":"codex"}}
    },"routes":[
        {"method":"GET","path":"/models","action":"catalog","format":"codex","provider":"live"},
        {"action":"model"}
    ]}).to_string(),fixture.root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    fixture
        .client
        .get(gateway.url.clone() + "/models")
        .send()
        .await
        .unwrap();
    let url = gateway.url.replace("http:", "ws:") + "/responses";
    for (hint, expected_status) in [
        (None, 400),
        (Some("model=unknown"), 404),
        (Some("model="), 400),
    ] {
        let mut request = url.clone().into_client_request().unwrap();
        request
            .headers_mut()
            .insert("x-router-client", "codex".parse().unwrap());
        if let Some(hint) = hint {
            request
                .headers_mut()
                .insert("x-codex-routing-hint", hint.parse().unwrap());
        }
        let tungstenite::Error::Http(response) = connect_async(request).await.unwrap_err() else {
            panic!("Expected HTTP rejection before upgrade");
        };
        assert_eq!(response.status().as_u16(), expected_status);
    }
    // Without a client marker, the Codex header is not interpreted.
    let mut request = url.clone().into_client_request().unwrap();
    request
        .headers_mut()
        .insert("x-codex-routing-hint", "model=gpt-test".parse().unwrap());
    let tungstenite::Error::Http(response) = connect_async(request).await.unwrap_err() else {
        panic!("Expected missing generic model hint");
    };
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    for (header, value) in [
        ("x-model", "gpt-test"),
        ("x-codex-routing-hint", "model=gpt-test"),
    ] {
        let mut request = url.clone().into_client_request().unwrap();
        request
            .headers_mut()
            .insert("x-router-client", "codex".parse().unwrap());
        request.headers_mut().append(
            "x-codex-routing-hint",
            "model=publisher/model".parse().unwrap(),
        );
        request.headers_mut().append(header, value.parse().unwrap());
        let tungstenite::Error::Http(response) = connect_async(request).await.unwrap_err() else {
            panic!("Expected conflicting model hints");
        };
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    let mut request = url.clone().into_client_request().unwrap();
    request
        .headers_mut()
        .insert("x-router-client", "codex".parse().unwrap());
    request.headers_mut().insert(
        "x-codex-routing-hint",
        "model=publisher/model;tier=priority".parse().unwrap(),
    );
    let (mut socket, _) = connect_async(request).await.unwrap();
    let original = "{ \"model\" : \"publisher/model\", \"input\" : \"preserved\" }";
    socket
        .send(tungstenite::Message::Text(original.into()))
        .await
        .unwrap();
    let reply = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data();
    let reply: Value = serde_json::from_slice(&reply).unwrap();
    assert_eq!(reply["provider"], "service-b");
    assert_eq!(reply["body"], original);
    socket
        .send(tungstenite::Message::Binary(
            br#"{"type":"followup"}"#.to_vec().into(),
        ))
        .await
        .unwrap();
    let reply = socket.next().await.unwrap().unwrap().into_data();
    assert_eq!(
        serde_json::from_slice::<Value>(&reply).unwrap()["provider"],
        "service-b"
    );
    socket
        .send(tungstenite::Message::Text(r#"{"model":"gpt-test"}"#.into()))
        .await
        .unwrap();
    let reply = socket.next().await.unwrap().unwrap().into_data();
    let reply: Value = serde_json::from_slice(&reply).unwrap();
    assert_eq!(reply["provider"], "service-b");
    assert_eq!(reply["model"], "gpt-test");
    socket.close(None).await.unwrap();
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("x-model", "gpt-test".parse().unwrap());
    request.headers_mut().insert(
        "x-codex-routing-hint",
        "model=publisher/model".parse().unwrap(),
    );
    let (mut socket, _) = connect_async(request).await.unwrap();
    socket
        .send(tungstenite::Message::Text(r#"{"model":7}"#.into()))
        .await
        .unwrap();
    let reply = socket.next().await.unwrap().unwrap().into_data();
    let reply: Value = serde_json::from_slice(&reply).unwrap();
    assert_eq!(reply["provider"], "service-a");
    assert_eq!(reply["model"], 7);
}

#[tokio::test]
async fn websocket_destination_is_selected_by_handshake_metadata() {
    let fixture = Fixture::new().await;
    for provider in ["service-a", "service-b"] {
        let mut request = ws_request(
            fixture.routed.url.replace("http:", "ws:") + "/v1/responses",
            provider,
        );
        request
            .headers_mut()
            .insert("authorization", "Bearer client-token".parse().unwrap());
        request
            .headers_mut()
            .insert("cookie", "client-cookie".parse().unwrap());
        let (mut socket, _) = connect_async(request).await.unwrap();
        for (model, binary) in [("gpt-test", false), ("publisher/model", true)] {
            let bytes = format!("{{ \"model\":\"{model}\" }}");
            socket
                .send(if binary {
                    tungstenite::Message::Binary(bytes.clone().into_bytes().into())
                } else {
                    tungstenite::Message::Text(bytes.clone().into())
                })
                .await
                .unwrap();
            let reply = timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .into_data();
            let reply: Value = serde_json::from_slice(&reply).unwrap();
            assert_eq!(reply["provider"], provider);
            assert_eq!(reply["body"], bytes);
            if provider == "service-b" {
                assert!(reply["authorization"].is_null());
                assert!(reply["cookie"].is_null());
            }
        }
        socket.close(None).await.unwrap();
    }
}

#[tokio::test]
async fn rejects_duplicate_manual_ownership_and_unknown_websocket_providers() {
    let fixture = Fixture::new().await;
    let url = fixture.routed.url.replace("http:", "ws:") + "/responses";
    let error = connect_async(ws_request(url, "unknown-provider"))
        .await
        .err()
        .unwrap();
    let tungstenite::Error::Http(response) = error else {
        panic!("expected HTTP rejection");
    };
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let server = serve(
        Router::new()
            .fallback(mock)
            .with_state(Mock::new("service-c", "publisher/model")),
    )
    .await;
    let descriptor = json!({"models":[{"slug":"publisher/model"}]});
    std::fs::write(
        fixture.root.path().join("another.json"),
        descriptor.to_string(),
    )
    .unwrap();
    let config = test_config(
        json!({
            "live":{"base_url":fixture._live_server.url,"auth":"none"},
            "first":{"base_url":fixture._manual_server.url,"auth":"none","catalog":{"file":"models.json","format":"codex"}},
            "second":{"base_url":server.url,"auth":"none","catalog":{"file":"another.json","format":"codex"}}
        }),
        None,
        Some("live"),
        fixture.root.path(),
    );
    let error = router(config).expect_err("conflicting manual owners were accepted");
    assert!(error.to_string().contains("multiple providers"));
}

#[tokio::test]
async fn websocket_preserves_handshake_status_and_retry_headers() {
    let fixture = Fixture::new().await;
    let client = reqwest::Client::builder()
        .no_proxy()
        .no_gzip()
        .build()
        .unwrap();
    for status in [401, 429] {
        let url = fixture.routed.url.replace("http:", "ws:") + "/responses";
        let mut request = ws_request(url, "service-a");
        request
            .headers_mut()
            .insert("x-reject-status", status.to_string().parse().unwrap());
        request
            .headers_mut()
            .insert("x-compress", "1".parse().unwrap());
        let direct = client
            .get(fixture._live_server.url.clone() + "/responses")
            .headers(request.headers().clone())
            .send()
            .await
            .unwrap();
        let response = client
            .get(fixture.routed.url.clone() + "/responses")
            .headers(request.headers().clone())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status);
        assert_eq!(response.headers()["retry-after"], "17");
        assert_eq!(response.headers()["content-encoding"], "gzip");
        assert_eq!(
            response.bytes().await.unwrap(),
            direct.bytes().await.unwrap()
        );
    }
}

#[tokio::test]
async fn client_disconnect_cancels_a_pending_upstream_handshake() {
    let root = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind(("localhost", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            headers.push(socket.read_u8().await.unwrap());
        }
        accepted_tx.send(()).unwrap();
        socket.read_u8().await.is_err()
    });
    let config = test_config(
        json!({"named-service":{"base_url":format!("http://localhost:{port}"),"auth":"none"}}),
        Some("named-service"),
        None,
        root.path(),
    );
    let gateway = serve(router(config).unwrap()).await;
    use tokio::io::AsyncWriteExt;
    let gateway_url: url::Url = gateway.url.parse().unwrap();
    let mut socket = tokio::net::TcpStream::connect(("localhost", gateway_url.port().unwrap()))
        .await
        .unwrap();
    socket.write_all(b"GET /responses HTTP/1.1\r\nHost: localhost\r\nConnection: upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n").await.unwrap();
    timeout(Duration::from_secs(2), accepted_rx)
        .await
        .unwrap()
        .unwrap();
    drop(socket);
    assert!(
        timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
    );
}

#[tokio::test]
async fn forwarding_rule_relays_non_json_websocket_frames() {
    async fn upgrade(request: Request) -> Response {
        let (mut parts, _) = request.into_parts();
        let websocket = WebSocketUpgrade::from_request_parts(&mut parts, &())
            .await
            .unwrap();
        websocket.on_upgrade(|mut socket| async move {
            while let Some(Ok(message)) = socket.recv().await {
                match message {
                    Message::Text(_) | Message::Binary(_) => {
                        if socket.send(message).await.is_err() {
                            break;
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        })
    }
    let backend = serve(Router::new().fallback(upgrade)).await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(
        &json!({"providers":{"frames":{"base_url":backend.url,"auth":"none","catalog":{"file":"missing.json","format":"codex"}}},"routes":[{"action":"forward","provider":"frames"}]})
            .to_string(),
        root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let (mut socket, _) = connect_async(gateway.url.replace("http:", "ws:") + "/custom/ws")
        .await
        .unwrap();
    for frame in [
        tungstenite::Message::Text("raw protocol frame".into()),
        tungstenite::Message::Binary(vec![0u8, 255, 9].into()),
    ] {
        socket.send(frame.clone()).await.unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            frame
        );
    }
    socket.close(None).await.unwrap();
}

#[tokio::test]
async fn websocket_relays_server_first_messages_and_upstream_subprotocol() {
    async fn greet(request: Request) -> Response {
        let (mut parts, _) = request.into_parts();
        let key = parts.headers["sec-websocket-key"].clone();
        let websocket = WebSocketUpgrade::from_request_parts(&mut parts, &())
            .await
            .unwrap();
        let mut response =
            websocket
                .protocols(["provider-stream"])
                .on_upgrade(|mut socket| async move {
                    socket.send(Message::Text("ready".into())).await.unwrap();
                    while let Some(Ok(message)) = socket.recv().await {
                        if matches!(message, Message::Close(_)) {
                            break;
                        }
                        if socket.send(message).await.is_err() {
                            break;
                        }
                    }
                });
        response
            .headers_mut()
            .insert("x-upstream-handshake-key", key);
        response
    }
    let upstream = serve(Router::new().fallback(greet)).await;
    let root = tempfile::tempdir().unwrap();
    let config = test_config(
        json!({"api":{"base_url":upstream.url,"auth":"none"}}),
        Some("api"),
        None,
        root.path(),
    );
    let gateway = serve(router(config).unwrap()).await;
    let mut request = gateway
        .url
        .replace("http:", "ws:")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("sec-websocket-protocol", "provider-stream".parse().unwrap());
    let key = request.headers()["sec-websocket-key"].clone();
    let (mut socket, response) = connect_async(request).await.unwrap();
    assert_eq!(
        response.headers()["sec-websocket-protocol"],
        "provider-stream"
    );
    assert_eq!(response.headers()["x-upstream-handshake-key"], key);
    assert_eq!(
        timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        tungstenite::Message::Text("ready".into()),
    );
    socket
        .send(tungstenite::Message::Binary(vec![0, 255, 9].into()))
        .await
        .unwrap();
    assert_eq!(
        socket.next().await.unwrap().unwrap(),
        tungstenite::Message::Binary(vec![0, 255, 9].into())
    );
    socket.close(None).await.unwrap();
}
