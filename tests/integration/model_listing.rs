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
        }, "routes": [
            {"method":"GET", "path":"/models", "action":"catalog", "provider":"remote", "format":"codex"},
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
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    // A file-backed model remains routable, while the supplied client prefix is untouched
    // until every remote source has a successful listing snapshot.
    assert_eq!(
        forwarded_description(&fixture, &gateway, "manually-known").await,
        CLIENT_DESCRIPTION
    );

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
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let after_failure = forwarded_description(&fixture, &gateway, "model-a").await;
    assert!(after_failure.contains("model-b"));
    assert!(!after_failure.contains("model-a"));

    fixture.live.status.store(200, Ordering::SeqCst);
    *fixture.live.models.lock().unwrap() = Vec::new();
    fixture
        .client
        .get(gateway.url.clone() + "/models")
        .send()
        .await
        .unwrap();
    let empty = forwarded_description(&fixture, &gateway, "model-a").await;
    assert!(empty.starts_with("Available model overrides"));
    assert!(!empty.contains("model-a"));
    assert!(!empty.contains("model-b"));
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
