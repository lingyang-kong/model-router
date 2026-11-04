use std::sync::atomic::Ordering;

use axum::http::StatusCode;
use model_router::router;
use serde_json::{Value, json};

use crate::common::*;

const CLIENT_DESCRIPTION: &str =
    "Available model overrides:\n- cached-model (local)\n\nSpawns an agent for delegated work.";

async fn forwarded_description(fixture: &Fixture, gateway: &Server, model: &str) -> String {
    let response: Value = fixture
        .client
        .post(gateway.url.clone() + "/responses")
        .header("originator", "codex_cli_rs")
        .json(&json!({
            "model": model,
            "tools": [{"type":"namespace", "name":"collaboration", "tools":[{
                "type":"function", "name":"spawn_agent", "description":CLIENT_DESCRIPTION,
                "parameters":{"properties":{"message":{"type":"string"}}}
            }]}]
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let forwarded: Value = serde_json::from_str(response["body"].as_str().unwrap()).unwrap();
    forwarded["tools"][0]["tools"][0]["description"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn remote_config(fixture: &Fixture) -> model_router::Config {
    std::fs::write(
        fixture.root.path().join("manual-models.json"),
        json!({"models":[{"slug":"manually-known", "use_responses_lite":false}]}).to_string(),
    )
    .unwrap();
    parse_config(
        &json!({"providers": {
            "remote": {
                "base_url": fixture._live_server.url,
                "auth": "none",
                "catalog": {"path": "/models", "format": "codex"},
                "hidden_models": ["hidden-*"]
            },
            "manual": {
                "base_url": fixture._manual_server.url,
                "auth": "none",
                "catalog": {"file": "manual-models.json", "format": "codex"},
                "hidden_models": ["manually-known"]
            }
        }, "clients":{"codex":{"provider":"remote","format":"codex"}}, "routes": [
            {"method":"GET", "path":"/models", "action":"catalog"},
            {"path_prefix":"/responses", "action":"model"}
        ]})
        .to_string(),
        fixture.root.path(),
    )
    .unwrap()
}

#[tokio::test]
async fn native_spawn_agent_listing_tracks_successful_remote_snapshots() {
    let fixture = Fixture::new().await;
    let gateway = serve(router(remote_config(&fixture)).unwrap()).await;

    fixture.live.status.store(503, Ordering::SeqCst);
    assert_eq!(
        fixture
            .client
            .get(gateway.url.clone() + "/models")
            .header("originator", "codex_cli_rs")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    // The hidden file catalog is a successful empty listing even if the remote fails.
    let initial = forwarded_description(&fixture, &gateway, "manually-known").await;
    assert!(initial.starts_with("Available model overrides"));
    assert!(!initial.contains("cached-model"));
    assert!(!initial.contains("manually-known"));

    *fixture.live.models.lock().unwrap() = vec![
        json!({"slug":"model-a", "use_responses_lite":false}),
        json!({"slug":"model-b", "use_responses_lite":false}),
        json!({"slug":"hidden-secret", "use_responses_lite":false}),
    ];
    fixture.live.status.store(200, Ordering::SeqCst);
    assert!(
        fixture
            .client
            .get(gateway.url.clone() + "/models")
            .header("originator", "codex_cli_rs")
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    let initial = forwarded_description(&fixture, &gateway, "model-a").await;
    assert!(initial.starts_with("Available model overrides"));
    assert!(initial.contains("model-a"));
    assert!(initial.contains("model-b"));
    assert!(!initial.contains("cached-model"));
    assert!(!initial.contains("hidden-secret"));
    assert!(!initial.contains("manually-known"));
    assert!(initial.ends_with("Spawns an agent for delegated work."));

    *fixture.live.models.lock().unwrap() =
        vec![json!({"slug":"model-b", "use_responses_lite":false})];
    fixture
        .client
        .get(gateway.url.clone() + "/models")
        .header("originator", "codex_cli_rs")
        .send()
        .await
        .unwrap();
    let retired = forwarded_description(&fixture, &gateway, "model-a").await;
    assert!(retired.contains("model-b"));
    assert!(!retired.contains("model-a"));
    // Listing membership is independent of routing ownership: model-a still routes.

    fixture.live.status.store(503, Ordering::SeqCst);
    assert_eq!(
        fixture
            .client
            .get(gateway.url.clone() + "/models")
            .header("originator", "codex_cli_rs")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let after_failure = forwarded_description(&fixture, &gateway, "model-a").await;
    assert!(!after_failure.contains("model-b"));
    assert!(!after_failure.contains("model-a"));

    fixture.live.status.store(200, Ordering::SeqCst);
    *fixture.live.models.lock().unwrap() = Vec::new();
    fixture
        .client
        .get(gateway.url.clone() + "/models")
        .header("originator", "codex_cli_rs")
        .send()
        .await
        .unwrap();
    let empty = forwarded_description(&fixture, &gateway, "model-a").await;
    assert!(empty.starts_with("Available model overrides"));
    assert!(!empty.contains("model-a"));
    assert!(!empty.contains("model-b"));
}

#[tokio::test]
async fn total_catalog_outage_preserves_native_listing_and_known_routes() {
    let fixture = Fixture::new().await;
    *fixture.live.models.lock().unwrap() = vec![json!({"slug":"known-model"})];
    let config = parse_config(
        &json!({"providers":{
            "remote":{
                "base_url":fixture._live_server.url,"auth":"none",
                "catalog":{"path":"/models","format":"codex"},
                "models":{"known-model":{"use_responses_lite":false}}
            }
        },"clients":{"codex":{"provider":"remote","format":"codex"}},"routes":[
            {"path":"/models","action":"catalog"},
            {"path_prefix":"/responses","action":"model"}
        ]})
        .to_string(),
        fixture.root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let mut last_description = CLIENT_DESCRIPTION.to_owned();
    for status in [503, 200, 503] {
        fixture.live.status.store(status, Ordering::SeqCst);
        assert_eq!(
            fixture
                .client
                .get(gateway.url.clone() + "/models")
                .header("originator", "codex_cli_rs")
                .send()
                .await
                .unwrap()
                .status()
                .as_u16(),
            status
        );
        let description = forwarded_description(&fixture, &gateway, "known-model").await;
        if status == 200 {
            assert!(description.contains("known-model"));
            assert!(!description.contains("cached-model"));
        } else {
            assert_eq!(description, last_description);
        }
        last_description = description;
    }
}

#[tokio::test]
async fn file_catalog_seeds_native_listing_without_catalog_fetches() {
    let fixture = Fixture::new().await;
    std::fs::write(
        fixture.root.path().join("file-models.json"),
        json!({"models":[{"slug":"file-model", "use_responses_lite":false}]}).to_string(),
    )
    .unwrap();
    let config = parse_config(
        &json!({"providers": {
            "manual": {
                "base_url": fixture._manual_server.url,
                "auth": "none",
                "catalog": {"file": "file-models.json", "format": "codex"}
            },
            "forward-only": {"base_url": fixture._live_server.url, "auth":"none"}
        }, "routes": [
            {"path_prefix":"/responses", "action":"model"}
        ]})
        .to_string(),
        fixture.root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let description = forwarded_description(&fixture, &gateway, "file-model").await;
    assert!(description.contains("file-model"));
    assert!(description.ends_with("Spawns an agent for delegated work."));
}

#[tokio::test]
async fn catalog_listings_are_isolated_by_client_credentials_and_account() {
    use axum::{Json, Router, extract::Request, response::IntoResponse};
    use std::sync::{Arc, atomic::AtomicUsize};

    let fixture = Fixture::new().await;
    let fetches = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&fetches);
    let upstream = serve(Router::new().fallback(move |request: Request| {
        let seen = Arc::clone(&seen);
        async move {
            seen.fetch_add(1, Ordering::SeqCst);
            let token = request
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok());
            let account = request
                .headers()
                .get("chatgpt-account-id")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("default");
            match token {
                Some(token) => Json(json!({"models":[{
                    "slug":"private-model", "description":if request.headers().contains_key("originator") { format!("{token}/{account}") } else { "other-client".to_owned() },
                    "use_responses_lite":false
                }]}))
                .into_response(),
                None => StatusCode::UNAUTHORIZED.into_response(),
            }
        }
    }))
    .await;
    std::fs::write(
        fixture.root.path().join("local.json"),
        json!({"models":[{"slug":"local-model", "use_responses_lite":false}]}).to_string(),
    )
    .unwrap();
    let config = parse_config(&json!({
        "providers":{
            "private":{"base_url":upstream.url,"auth":"passthrough","catalog":{"path":"/models","format":"codex"}},
            "local":{"base_url":fixture._manual_server.url,"auth":"none","catalog":{"file":"local.json","format":"codex"}}
        },
        "clients":{"codex":{"provider":"private"},"openai":{"provider":"private"}},
        "routes":[{"path":"/models","action":"catalog"},{"action":"model"}]
    }).to_string(),fixture.root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let headers = |token: &str, account: &str| {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("originator", "codex_cli_rs".parse().unwrap());
        headers.insert("authorization", token.parse().unwrap());
        headers.insert("chatgpt-account-id", account.parse().unwrap());
        headers
    };
    let first = headers("Bearer first", "account-a");
    let second = headers("Bearer first", "account-b");
    let third = headers("Bearer second", "account-a");
    for credentials in [&first, &second, &third] {
        // A scope that has not refreshed retains the caller's own description.
        assert_eq!(
            scoped_description(&fixture, &gateway, credentials, false).await,
            CLIENT_DESCRIPTION
        );
        let response = fixture
            .client
            .get(gateway.url.clone() + "/models")
            .headers(credentials.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    // Anonymous OpenAI refresh succeeds partially, but cannot change Codex listings.
    assert_eq!(
        fixture
            .client
            .get(gateway.url.clone() + "/models")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    // Even with identical credentials a different client receives its own snapshot.
    let mut openai = first.clone();
    openai.remove("originator");
    fixture
        .client
        .get(gateway.url.clone() + "/models")
        .headers(openai)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    for (credentials, expected) in [
        (&first, "Bearer first/account-a"),
        (&second, "Bearer first/account-b"),
        (&third, "Bearer second/account-a"),
    ] {
        for websocket in [false, true] {
            let description = scoped_description(&fixture, &gateway, credentials, websocket).await;
            assert!(description.contains(expected), "{description}");
            assert_eq!(description.matches("Bearer ").count(), 1);
            assert!(description.contains("local-model"));
            assert!(!description.contains("cached-model"));
        }
    }
    // Refresh again: no catalog result or discovery is borrowed from prior requests.
    fixture
        .client
        .get(gateway.url.clone() + "/models")
        .headers(first)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(fetches.load(Ordering::SeqCst), 6);
}

async fn scoped_description(
    fixture: &Fixture,
    gateway: &Server,
    headers: &axum::http::HeaderMap,
    websocket: bool,
) -> String {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
    // Request-specific metadata must not create a new account scope.
    let mut headers = headers.clone();
    headers.insert("x-openai-request-id", "inference-request".parse().unwrap());
    headers.insert(
        "chatgpt-client-feature",
        "inference-feature".parse().unwrap(),
    );
    let body = json!({"type":"response.create", "model":"local-model", "tools":[{"type":"namespace","name":"collaboration","tools":[{
        "type":"function","name":"spawn_agent","description":CLIENT_DESCRIPTION,"parameters":{"properties":{"message":{"type":"string"}}}
    }]}]});
    let response: Value = if websocket {
        let mut request = (gateway.url.replace("http:", "ws:") + "/responses")
            .into_client_request()
            .unwrap();
        request.headers_mut().extend(headers.clone());
        request
            .headers_mut()
            .insert("x-model", "local-model".parse().unwrap());
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        socket
            .send(Message::Text(body.to_string().into()))
            .await
            .unwrap();
        let reply = socket.next().await.unwrap().unwrap();
        serde_json::from_str(reply.to_text().unwrap()).unwrap()
    } else {
        fixture
            .client
            .post(gateway.url.clone() + "/responses")
            .headers(headers.clone())
            .json(&body)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    };
    let forwarded: Value = serde_json::from_str(response["body"].as_str().unwrap()).unwrap();
    forwarded["tools"][0]["tools"][0]["description"]
        .as_str()
        .unwrap()
        .to_owned()
}
