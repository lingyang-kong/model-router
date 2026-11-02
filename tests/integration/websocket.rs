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
async fn websocket_upgrade_accepts_token_lists_and_repeated_headers() {
    async fn accept(mut request: Request) -> Response {
        let values: Vec<_> = request
            .headers()
            .get_all("upgrade")
            .iter()
            .map(|value| value.to_str().unwrap().to_owned())
            .collect();
        assert!(
            values
                .iter()
                .flat_map(|value| value.split(','))
                .any(|token| token.trim().eq_ignore_ascii_case("websocket")),
            "received {values:?}"
        );
        // Axum's extractor requires a single value; inspect original headers first.
        request
            .headers_mut()
            .insert("upgrade", "websocket".parse().unwrap());
        let (mut parts, _) = request.into_parts();
        WebSocketUpgrade::from_request_parts(&mut parts, &())
            .await
            .unwrap()
            .on_upgrade(move |mut socket| async move {
                socket
                    .send(Message::Text(json!(values).to_string().into()))
                    .await
                    .unwrap();
            })
    }
    let backend = serve(Router::new().fallback(accept)).await;
    let root = tempfile::tempdir().unwrap();
    let config = test_config(
        json!({"upstream":{"base_url":backend.url,"auth":"none"}}),
        Some("upstream"),
        None,
        root.path(),
    );
    let gateway = serve(router(config).unwrap()).await;
    for values in [
        vec!["websocket"],
        vec!["h2c, WebSocket"],
        vec!["h2c", "websocket"],
    ] {
        use tokio::io::AsyncWriteExt;
        let port = url::Url::parse(&gateway.url).unwrap().port().unwrap();
        let mut raw = tokio::net::TcpStream::connect(("localhost", port))
            .await
            .unwrap();
        let upgrades = values
            .iter()
            .map(|value| format!("Upgrade: {value}\r\n"))
            .collect::<String>();
        raw.write_all(format!("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\n{upgrades}Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").as_bytes()).await.unwrap();
        let mut response = Vec::new();
        while !response.ends_with(b"\r\n\r\n") {
            response.push(
                timeout(Duration::from_secs(2), raw.read_u8())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert!(response.starts_with(b"HTTP/1.1 101"), "{response:?}");
        let mut socket = tokio_tungstenite::WebSocketStream::from_raw_socket(
            raw,
            tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        let message = socket.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&message.into_data()).unwrap(),
            json!(values)
        );
    }
}

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
    let config=parse_config(&json!({"clients":{"codex":{"format":"codex"},"openai":{"format":"codex"},"anthropic":{"format":"codex"}},"providers":{
        "live":{"base_url":fixture._live_server.url,"auth":"none","catalog":{"path":"/models","format":"codex"}},
        "manual":{"base_url":fixture._manual_server.url,"auth":"none","catalog":{"file":"models.json","format":"codex"}}
    },"routes":[
        {"method":"GET","path":"/models","action":"catalog"},
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
            .insert("originator", "codex_cli_rs".parse().unwrap());
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
    // Originator identifies Codex; a routing hint alone does not identify another client.
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
            .insert("originator", "codex_cli_rs".parse().unwrap());
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
    let mut codex_request = url.clone().into_client_request().unwrap();
    codex_request
        .headers_mut()
        .insert("originator", "codex_cli_rs".parse().unwrap());
    codex_request.headers_mut().insert(
        "x-codex-routing-hint",
        "model=publisher/model;tier=priority".parse().unwrap(),
    );
    let (mut codex_socket, _) = connect_async(codex_request).await.unwrap();
    let codex_body = "{ \"model\" : \"publisher/model\", \"input\" : \"codex\" }";
    codex_socket
        .send(tungstenite::Message::Text(codex_body.into()))
        .await
        .unwrap();
    let reply = timeout(Duration::from_secs(2), codex_socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data();
    let reply: Value = serde_json::from_slice(&reply).unwrap();
    assert_eq!(reply["provider"], "service-b");
    assert_eq!(reply["body"], codex_body);
    codex_socket.close(None).await.unwrap();

    // Generic x-model routing remains available and fixes the destination for the connection.
    let mut request = url.clone().into_client_request().unwrap();
    request
        .headers_mut()
        .insert("x-model", "publisher/model".parse().unwrap());
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
        .insert("originator", "codex_cli_rs".parse().unwrap());
    request
        .headers_mut()
        .insert("x-model", "gpt-test".parse().unwrap());
    request.headers_mut().insert(
        "x-codex-routing-hint",
        "model=publisher/model".parse().unwrap(),
    );
    let tungstenite::Error::Http(response) = connect_async(request).await.unwrap_err() else {
        panic!("Expected conflicting native and generic model hints");
    };
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
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
async fn profile_forward_websockets_use_client_provider_without_model_hints() {
    async fn upgrade(request: Request) -> Response {
        let (mut parts, _) = request.into_parts();
        let path = parts.uri.path().to_owned();
        let websocket = WebSocketUpgrade::from_request_parts(&mut parts, &())
            .await
            .unwrap();
        websocket.on_upgrade(move |mut socket| async move {
            socket.send(Message::Text(path.into())).await.unwrap();
            while let Some(Ok(message)) = socket.recv().await {
                if matches!(message, Message::Text(_) | Message::Binary(_)) {
                    if socket.send(message).await.is_err() {
                        break;
                    }
                } else if matches!(message, Message::Close(_)) {
                    break;
                }
            }
        })
    }
    let backend = serve(Router::new().fallback(upgrade)).await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(&json!({"clients":{"codex":{"provider":"codex"},"openai":{"provider":"openai"}},"providers":{"codex":{"base_url":format!("{}/codex",backend.url),"auth":"none"},"openai":{"base_url":format!("{}/openai",backend.url),"auth":"none"}},"routes":[{"action":"forward"}]}).to_string(), root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let url = gateway.url.replace("http:", "ws:") + "/webtools";
    for (originator, expected) in [
        ("codex_cli_rs", "/codex/webtools"),
        ("opencode", "/openai/webtools"),
    ] {
        let mut request = url.clone().into_client_request().unwrap();
        request
            .headers_mut()
            .insert("originator", originator.parse().unwrap());
        let (mut socket, _) = connect_async(request).await.unwrap();
        assert_eq!(
            socket.next().await.unwrap().unwrap().into_text().unwrap(),
            expected
        );
        for frame in [
            tungstenite::Message::Text("opaque frame".into()),
            tungstenite::Message::Binary(vec![0, 255, 9].into()),
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
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("anthropic-version", "2023-06-01".parse().unwrap());
    let tungstenite::Error::Http(response) = connect_async(request).await.unwrap_err() else {
        panic!("Expected missing provider rejection");
    };
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
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
