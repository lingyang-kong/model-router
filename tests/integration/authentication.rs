use axum::{Router, http::StatusCode};
use futures_util::{SinkExt, StreamExt};
use model_router::router;
use serde_json::{Value, json};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

use crate::common::*;

#[tokio::test]
async fn concurrent_credentials_select_provider_without_catalog_ownership() {
    let root = tempfile::tempdir().unwrap();
    let chat = Mock::new("chat", "shared-native");
    let api = Mock::new("api", "shared-native");
    let local = Mock::new("local", "local-model");
    let chat_server = serve(Router::new().fallback(mock).with_state(chat.clone())).await;
    let api_server = serve(Router::new().fallback(mock).with_state(api.clone())).await;
    let local_server = serve(Router::new().fallback(mock).with_state(local.clone())).await;
    let config = parse_config(&json!({
        "providers": {
            "chatgpt": {"base_url": chat_server.url, "auth":"passthrough", "catalog":{"path":"/models","format":"codex"}},
            "openai": {"base_url": api_server.url, "auth":"passthrough", "catalog":{"path":"/models","format":"codex"}},
            "local": {"base_url":local_server.url,"auth":"none","models":{"local-model":{}}}
        },
        "clients":{"codex":{"provider":{"subscription":"chatgpt","api_key":"openai"}}},
        "routes":[{"method":"GET","path":"/models","action":"catalog"},{"path":"/responses","action":"model"},{"action":"forward"}]
    }).to_string(), root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let a = client
        .post(gateway.url.clone() + "/responses")
        .header("originator", "codex_cli_rs")
        .bearer_auth("chat-token")
        .header("chatgpt-account-id", "account")
        .json(&json!({"model":"unseen"}))
        .send();
    let b = client
        .post(gateway.url.clone() + "/responses")
        .header("originator", "codex_cli_rs")
        .bearer_auth("sk-api-key")
        .json(&json!({"model":"unseen"}))
        .send();
    let (a, b) = tokio::join!(a, b);
    assert_eq!(
        a.unwrap().json::<Value>().await.unwrap()["provider"],
        "chat"
    );
    assert_eq!(b.unwrap().json::<Value>().await.unwrap()["provider"], "api");
    for (key, account, expected) in [
        ("chat-token", Some("account"), "chat"),
        ("sk-api-key", None, "api"),
    ] {
        for path in ["/responses", "/auxiliary"] {
            let mut request = client
                .post(gateway.url.clone() + path)
                .header("originator", "codex_cli_rs")
                .bearer_auth(key)
                .json(&json!({"model":"shared-native"}));
            if let Some(account) = account {
                request = request.header("chatgpt-account-id", account);
            }
            let response: Value = request
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(response["provider"], expected);
        }
        let mut request = client
            .post(gateway.url.clone() + "/responses")
            .header("originator", "codex_cli_rs")
            .bearer_auth(key)
            .json(&json!({"model":"local-model"}));
        if let Some(account) = account {
            request = request.header("chatgpt-account-id", account);
        }
        let response: Value = request
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response["provider"], "local");
        assert!(response["headers"].get("authorization").is_none());
        assert!(response["headers"].get("chatgpt-account-id").is_none());
        let mut request = client
            .get(gateway.url.clone() + "/models")
            .header("originator", "codex_cli_rs")
            .bearer_auth(key);
        if let Some(account) = account {
            request = request.header("chatgpt-account-id", account);
        }
        assert_eq!(request.send().await.unwrap().status(), StatusCode::OK);
    }
    for (key, account, expected) in [
        ("chat-token", Some("account"), "chat"),
        ("sk-api-key", None, "api"),
    ] {
        let mut request = client
            .post(gateway.url.clone() + "/responses")
            .header("originator", "codex_cli_rs")
            .bearer_auth(key)
            .json(&json!({"model":"shared-native"}));
        if let Some(account) = account {
            request = request.header("chatgpt-account-id", account);
        }
        let response: Value = request
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response["provider"], expected);
        let mut request = (gateway.url.replace("http:", "ws:") + "/responses")
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("originator", "codex_cli_rs".parse().unwrap());
        request
            .headers_mut()
            .insert("authorization", format!("Bearer {key}").parse().unwrap());
        if let Some(account) = account {
            request
                .headers_mut()
                .insert("chatgpt-account-id", account.parse().unwrap());
        }
        let (mut socket, _) = connect_async(request).await.unwrap();
        for (model, provider, auth) in [
            ("shared-native", expected, Some(format!("Bearer {key}"))),
            ("local-model", "local", None),
            ("shared-native", expected, Some(format!("Bearer {key}"))),
        ] {
            socket
                .send(Message::Text(json!({"model":model}).to_string().into()))
                .await
                .unwrap();
            let response: Value =
                serde_json::from_slice(&socket.next().await.unwrap().unwrap().into_data()).unwrap();
            assert_eq!(response["provider"], provider);
            assert_eq!(response["authorization"], json!(auth));
        }
    }
    assert_eq!(
        chat.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| entry["path"] == "/models")
            .count(),
        1
    );
    assert_eq!(
        api.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| entry["path"] == "/models")
            .count(),
        1
    );
}

#[tokio::test]
async fn ambiguous_authentication_is_rejected_without_contacting_upstreams() {
    let root = tempfile::tempdir().unwrap();
    let chat = Mock::new("chat", "m");
    let api = Mock::new("api", "m");
    let chat_server = serve(Router::new().fallback(mock).with_state(chat.clone())).await;
    let api_server = serve(Router::new().fallback(mock).with_state(api.clone())).await;
    let config = parse_config(
        &json!({
            "providers":{
                "chatgpt":{"base_url":chat_server.url,"auth":"passthrough","catalog":{"path":"/models","format":"codex"}},
                "openai":{"base_url":api_server.url,"auth":"passthrough"}
            },
            "clients":{"codex":{"provider":{"subscription":"chatgpt","api_key":"openai"}}},
            "routes":[{"path":"/models","action":"catalog"},{"path":"/other","action":"forward","provider":"openai"},{"action":"forward"}]
        })
        .to_string(),
        root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for headers in [
        vec![],
        vec![("authorization", "Bearer unknown")],
        vec![("authorization", "Basic dXNlcjpwYXNz")],
        vec![("authorization", "Bearer ")],
        vec![("authorization", "Bearer sk-one,Bearer sk-two")],
        vec![
            ("authorization", "Bearer sk-api"),
            ("cookie", "session=value"),
        ],
        vec![
            ("authorization", "Bearer sk-api"),
            ("x-api-key", "another-key"),
        ],
        vec![
            ("authorization", "Bearer chat-token"),
            ("chatgpt-account-id", "account"),
            ("openai-project", "project"),
        ],
        vec![
            ("authorization", "Bearer sk-api"),
            ("chatgpt-account-id", "account"),
        ],
        vec![
            ("authorization", "Bearer sk-api"),
            ("authorization", "Bearer sk-other"),
        ],
        vec![
            ("authorization", "Bearer chat-token"),
            ("chatgpt-account-id", "account"),
            ("chatgpt-account-id", "other"),
        ],
    ] {
        let mut request = client
            .post(gateway.url.clone() + "/auxiliary")
            .header("originator", "codex_cli_rs")
            .body("{}");
        for (name, value) in &headers {
            request = request.header(*name, *value);
        }
        assert_eq!(
            request.send().await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
        let mut catalog = client
            .get(gateway.url.clone() + "/models")
            .header("originator", "codex_cli_rs");
        let mut socket = (gateway.url.replace("http:", "ws:") + "/auxiliary")
            .into_client_request()
            .unwrap();
        socket
            .headers_mut()
            .insert("originator", "codex_cli_rs".parse().unwrap());
        for (name, value) in &headers {
            catalog = catalog.header(*name, *value);
            socket.headers_mut().append(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        assert_eq!(
            catalog.send().await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
        match connect_async(socket).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                assert_eq!(response.status(), StatusCode::BAD_REQUEST)
            }
            other => panic!("unexpected handshake result: {other:?}"),
        }
    }
    let response = client
        .post(gateway.url.clone() + "/other")
        .header("originator", "codex_cli_rs")
        .bearer_auth("chat-token")
        .header("chatgpt-account-id", "account")
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(chat.seen.lock().unwrap().is_empty());
    assert!(api.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn rejected_catalog_credentials_are_not_retried_against_another_provider() {
    use std::sync::atomic::Ordering;
    let root = tempfile::tempdir().unwrap();
    let chat = Mock::new("chat", "m");
    let api = Mock::new("api", "m");
    chat.status.store(401, Ordering::SeqCst);
    let chat_server = serve(Router::new().fallback(mock).with_state(chat.clone())).await;
    let api_server = serve(Router::new().fallback(mock).with_state(api.clone())).await;
    let config = parse_config(&json!({
        "providers":{
            "chatgpt":{"base_url":chat_server.url,"auth":"passthrough","catalog":{"path":"/models","format":"codex"}},
            "openai":{"base_url":api_server.url,"auth":"passthrough","catalog":{"path":"/models","format":"codex"}}
        },
        "clients":{"codex":{"provider":{"subscription":"chatgpt","api_key":"openai"}}},
        "routes":[{"action":"catalog"}]
    }).to_string(), root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let response = client
        .get(gateway.url.clone() + "/models")
        .header("originator", "codex_cli_rs")
        .bearer_auth("chat-token")
        .header("chatgpt-account-id", "account")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(chat.seen.lock().unwrap().len(), 1);
    assert!(api.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn partial_authentication_mappings_reject_unmapped_credentials() {
    let root = tempfile::tempdir().unwrap();
    let upstream = Mock::new("selected", "m");
    let backend = serve(Router::new().fallback(mock).with_state(upstream.clone())).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for (mapping, allowed, rejected) in [
        (
            json!({"subscription":"selected"}),
            ("chat-token", Some("account")),
            ("sk-api", None),
        ),
        (
            json!({"api_key":"selected"}),
            ("sk-api", None),
            ("chat-token", Some("account")),
        ),
    ] {
        let config = parse_config(&json!({
            "providers":{"selected":{"base_url":backend.url,"auth":"passthrough"}},
            "clients":{"codex":{"provider":mapping}},
            "routes":[{"path":"/direct","action":"forward","provider":"selected"},{"action":"model"}]
        }).to_string(), root.path()).unwrap();
        let gateway = serve(router(config).unwrap()).await;
        let send = |path: &str, (key, account): (&str, Option<&str>)| {
            let mut request = client
                .post(gateway.url.clone() + path)
                .header("originator", "codex_cli_rs")
                .bearer_auth(key)
                .json(&json!({"model":"m"}));
            if let Some(account) = account {
                request = request.header("chatgpt-account-id", account);
            }
            request
        };
        assert_eq!(
            send("/responses", allowed).send().await.unwrap().status(),
            StatusCode::OK
        );
        let count = upstream.seen.lock().unwrap().len();
        for path in ["/responses", "/direct"] {
            assert_eq!(
                send(path, rejected).send().await.unwrap().status(),
                StatusCode::BAD_REQUEST
            );
        }
        assert_eq!(upstream.seen.lock().unwrap().len(), count);
    }
    let config = parse_config(
        &json!({
            "providers":{"selected":{"base_url":backend.url,"auth":"passthrough"}},
            "clients":{"codex":{"provider":{}}},
            "routes":[{"action":"forward","provider":"selected"}]
        })
        .to_string(),
        root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let count = upstream.seen.lock().unwrap().len();
    for (key, account) in [("sk-api", None), ("chat-token", Some("account"))] {
        let mut request = client
            .post(gateway.url.clone() + "/direct")
            .header("originator", "codex_cli_rs")
            .bearer_auth(key)
            .body("{}");
        if let Some(account) = account {
            request = request.header("chatgpt-account-id", account);
        }
        assert_eq!(
            request.send().await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(upstream.seen.lock().unwrap().len(), count);
}

#[test]
fn authentication_mapping_validates_kinds_and_provider_names() {
    let root = tempfile::tempdir().unwrap();
    for mapping in [json!({"unknown":"selected"}), json!({"api_key":"missing"})] {
        let config = json!({
            "providers":{"selected":{"base_url":"http://localhost","auth":"passthrough"}},
            "clients":{"codex":{"provider":mapping}},
            "routes":[{"action":"forward"}]
        });
        assert!(parse_config(&config.to_string(), root.path()).is_err());
    }
}
