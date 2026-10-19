use std::{io, sync::atomic::Ordering, time::Duration};

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use futures_util::{StreamExt, future::join_all, stream};
use model_router::router;
use serde_json::{Value, json};

use crate::common::*;

async fn interrupted_catalog_source(request: Request) -> Response {
    let path = request.uri().path();
    match path {
        "/healthy" => Json(json!({"data":[{"id":"healthy-model"}]})).into_response(),
        "/rejected" => (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "17")],
            "origin rate limited",
        )
            .into_response(),
        "/hang-headers" => std::future::pending().await,
        "/hang-body" | "/reset-body" => {
            let reset = path == "/reset-body";
            let prefix =
                stream::once(async { Ok::<_, io::Error>(Bytes::from_static(b"{\"data\":")) });
            let interrupted = stream::once(async move {
                if reset {
                    // Flush the successful headers and partial body before disconnecting.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "catalog disconnected",
                    ))
                } else {
                    std::future::pending::<Result<Bytes, io::Error>>().await
                }
            });
            Response::new(Body::from_stream(prefix.chain(interrupted)))
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn assert_interrupted_catalog(failure_path: &str, origin: &str, healthy: bool) {
    let root = tempfile::tempdir().unwrap();
    let source = serve(Router::new().fallback(interrupted_catalog_source)).await;
    let config = parse_config(
        &json!({"providers":{
            "healthy":{
                "base_url":source.url,"auth":"none",
                "catalog":{"path":if healthy { "/healthy" } else { "/hang-body" },"format":"openai"}
            },
            "interrupted":{
                "base_url":source.url,"auth":"none",
                "catalog":{"path":failure_path,"format":"openai"}
            }
        },"clients":{"openai":{"provider":origin}},"routes":[{"path":"/models","action":"catalog"}]})
        .to_string(),
        root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        let response = client
            .get(gateway.url.clone() + "/models")
            .send()
            .await
            .unwrap();
        let rejected_origin = origin == "interrupted" && failure_path == "/rejected";
        assert_eq!(
            response.status(),
            match (healthy, rejected_origin) {
                (true, _) => StatusCode::OK,
                (false, true) => StatusCode::TOO_MANY_REQUESTS,
                (false, false) => StatusCode::BAD_GATEWAY,
            },
            "failure path: {failure_path}, origin: {origin}, healthy: {healthy}"
        );
        if healthy {
            assert_eq!(
                response.json::<Value>().await.unwrap()["data"],
                json!([{"id":"healthy-model"}])
            );
        } else if rejected_origin {
            assert_eq!(response.headers()["retry-after"], "17");
            assert_eq!(response.text().await.unwrap(), "origin rate limited");
        } else {
            // The error body must also finish within the deadline.
            response.bytes().await.unwrap();
        }
    })
    .await
    .expect("an unresponsive catalog prevented completion within five seconds");
}

#[tokio::test]
async fn catalog_body_connection_failures_leave_healthy_sources_available() {
    // Confirm this fixture fails after HTTP 200, exercising body-read handling.
    let source = serve(Router::new().fallback(interrupted_catalog_source)).await;
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(source.url.clone() + "/reset-body")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.bytes().await.is_err());
    join_all(
        ["healthy", "interrupted"]
            .map(|origin| assert_interrupted_catalog("/reset-body", origin, true)),
    )
    .await;
}

#[tokio::test]
async fn catalog_deadline_covers_waiting_for_headers_and_body() {
    let cases = ["/hang-headers", "/hang-body"]
        .into_iter()
        .flat_map(|path| ["healthy", "interrupted"].map(move |origin| (path, origin)));
    join_all(cases.map(|(path, origin)| assert_interrupted_catalog(path, origin, true))).await;
}

#[tokio::test]
async fn catalog_deadline_returns_bad_gateway_when_all_sources_hang() {
    assert_interrupted_catalog("/hang-headers", "interrupted", false).await;
}

#[tokio::test]
async fn catalog_deadline_preserves_origin_rejection_while_auxiliary_hangs() {
    assert_interrupted_catalog("/rejected", "interrupted", false).await;
}

#[tokio::test]
async fn catalog_connection_failures_leave_healthy_and_empty_sources_available() {
    // A bound socket reserves the port without listening, so later fixtures
    // cannot reuse the supposedly unavailable address.
    let offline = tokio::net::TcpSocket::new_v4().unwrap();
    offline.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let offline_url = format!("http://{}", offline.local_addr().unwrap());
    for format in ["codex", "openai", "anthropic"] {
        let (array, id) = match format {
            "codex" => ("models", "slug"),
            _ => ("data", "id"),
        };
        for origin in ["healthy", "offline"] {
            for empty in [false, true] {
                let fixture = Fixture::new().await;
                let models = if empty {
                    vec![]
                } else {
                    vec![json!({id:"healthy-model"})]
                };
                *fixture.live.models.lock().unwrap() = models.clone();
                let config = parse_config(
                    &json!({"providers":{
                        "healthy":{
                            "base_url":fixture._live_server.url,"auth":"none",
                            "catalog":{"path":"/models","format":format},
                            "http_headers":if format == "codex" { json!({}) } else { json!({"x-catalog-data":"1"}) }
                        },
                        "offline":{
                            "base_url":offline_url,"auth":"none",
                            "catalog":{"path":"/models","format":format}
                        }
                    },"clients":{"openai":{"provider":origin,"format":format}},"routes":[
                        {"path":"/models","action":"catalog"},
                        {"action":"model"}
                    ]})
                    .to_string(),
                    fixture.root.path(),
                )
                .unwrap();
                let gateway = serve(router(config).unwrap()).await;
                let response = fixture
                    .client
                    .get(gateway.url.clone() + "/models")
                    .send()
                    .await
                    .unwrap();
                assert_eq!(
                    response.status(),
                    StatusCode::OK,
                    "{format}, {origin}, {empty}"
                );
                assert!(response.headers().get("etag").is_none());
                assert_eq!(response.headers()["retry-after"], "17");
                let catalog: Value = response.json().await.unwrap();
                assert_eq!(catalog[array], json!(models));
                if format != "codex" {
                    assert_eq!(catalog["description"], "Mock catalog");
                    assert_eq!(catalog["extensions"], json!({"opaque":[1,2]}));
                }
                if format == "anthropic" {
                    let boundary = if empty {
                        Value::Null
                    } else {
                        json!("healthy-model")
                    };
                    assert_eq!(catalog["first_id"], boundary);
                    assert_eq!(catalog["last_id"], boundary);
                    assert_eq!(catalog["has_more"], false);
                }
                if !empty {
                    assert_eq!(
                        fixture
                            .client
                            .post(gateway.url.clone() + "/responses")
                            .json(&json!({"model":"healthy-model"}))
                            .send()
                            .await
                            .unwrap()
                            .status(),
                        StatusCode::OK
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn catalog_errors_when_all_connections_fail() {
    let fixture = Fixture::new().await;
    // A bound socket reserves the port without listening, so later fixtures
    // cannot reuse the supposedly unavailable address.
    let offline = tokio::net::TcpSocket::new_v4().unwrap();
    offline.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let offline_url = format!("http://{}", offline.local_addr().unwrap());
    let config = parse_config(
        &json!({"providers":{
            "origin":{"base_url":offline_url,"auth":"none","catalog":{"path":"/models","format":"codex"}},
            "auxiliary":{"base_url":offline_url,"auth":"none","catalog":{"path":"/models","format":"codex"}}
        },"clients":{"openai":{"provider":"origin","format":"codex"}},"routes":[{"path":"/models","action":"catalog"}]})
        .to_string(),
        fixture.root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    assert_eq!(
        fixture
            .client
            .get(gateway.url.clone() + "/models")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_GATEWAY
    );
}

#[tokio::test]
async fn unavailable_donor_omits_dependent_rows_until_recovery() {
    let fixture = Fixture::new().await;
    *fixture.live.models.lock().unwrap() = vec![json!({"slug":"donor"})];
    std::fs::write(
        fixture.root.path().join("dependent.json"),
        json!({"models":[{"slug":"dependent"},{"slug":"independent"}]}).to_string(),
    )
    .unwrap();
    let config = parse_config(
        &json!({"providers":{
            "donor":{"base_url":fixture._live_server.url,"auth":"none","catalog":{"path":"/models","format":"codex"}},
            "local":{
                "base_url":fixture._manual_server.url,"auth":"none",
                "catalog":{"file":"dependent.json","format":"codex"},
                "models":{"dependent":{"extends":{"provider":"donor","model":"donor"}}}
            }
        },"clients":{"openai":{"provider":"donor","format":"codex"}},"routes":[
            {"path":"/models","action":"catalog"},
            {"action":"model"}
        ]})
        .to_string(),
        fixture.root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    for status in [503, 200, 503, 200] {
        fixture.live.status.store(status, Ordering::SeqCst);
        let response = fixture
            .client
            .get(gateway.url.clone() + "/models")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let catalog: Value = response.json().await.unwrap();
        let ids: Vec<_> = catalog["models"]
            .as_array()
            .unwrap()
            .iter()
            .map(|model| model["slug"].as_str().unwrap())
            .collect();
        if status == 200 {
            assert_eq!(ids, ["donor", "dependent", "independent"]);
        } else {
            assert_eq!(ids, ["independent"]);
        }
        // An unavailable donor affects listing, while the dependent's route survives.
        assert_eq!(
            fixture
                .client
                .post(gateway.url.clone() + "/responses")
                .json(&json!({"model":"dependent"}))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }
}
