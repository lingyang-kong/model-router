use std::sync::{Arc, Mutex};

use axum::{
    Json, Router,
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use model_router::{Config, router};
use serde_json::{Value, json};

use crate::common::*;

type Requests = Arc<Mutex<Vec<(String, axum::http::HeaderMap)>>>;

async fn catalog_source(State(seen): State<Requests>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    seen.lock()
        .unwrap()
        .push((path.clone(), request.headers().clone()));
    if path == "/chatgpt/models"
        && (!request.headers().contains_key("authorization")
            || request.headers().contains_key("x-reject-chatgpt"))
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let (document, source) = match path.as_str() {
        "/chatgpt/models" => (
            json!({"models":[{
                "slug":"gpt-6-luna", "display_name":"GPT", "description":"Coding model",
                "native_codex":{"keep":true}
            }], "codex_extension":true}),
            "codex",
        ),
        "/v1/models" => (
            json!({"object":"list", "data":[{
                "id":"RadixArk/Qwen3.8-Flash-Next-NVFP4", "object":"model",
                "created":123, "owned_by":"RadixArk", "native_openai":{"keep":true}
            }], "openai_extension":true}),
            "openai",
        ),
        _ => return Json(json!({})).into_response(),
    };
    let mut response = Json(document).into_response();
    response
        .headers_mut()
        .insert("x-catalog-source", source.parse().unwrap());
    response
        .headers_mut()
        .insert("etag", "\"upstream\"".parse().unwrap());
    response
}

#[tokio::test]
async fn example_catalog_serves_each_client_and_keeps_credentials_scoped() {
    let seen = Requests::default();
    let upstream = serve(
        Router::new()
            .fallback(catalog_source)
            .with_state(seen.clone()),
    )
    .await;
    let configuration = include_str!("../../examples/router-config.toml")
        .replace(
            "https://chatgpt.com/backend-api/codex",
            &(upstream.url.clone() + "/chatgpt"),
        )
        .replace("http://localhost:30000", &upstream.url);
    let root = tempfile::tempdir().unwrap();
    let gateway =
        serve(router(Config::from_toml(&configuration, root.path()).unwrap()).unwrap()).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    for format in ["codex", "openai", "anthropic", "codex"] {
        let mut request = client
            .get(gateway.url.clone() + "/models?client_version=test")
            .bearer_auth("chatgpt-credential");
        request = match format {
            "codex" => request.header("originator", "codex_cli_rs"),
            "anthropic" => request.header("anthropic-version", "2023-06-01"),
            _ => request,
        };
        let response = request.send().await.unwrap();
        assert!(response.status().is_success(), "{format}");
        assert!(response.headers().get("etag").is_none());
        let vary: Vec<_> = response
            .headers()
            .get_all("vary")
            .iter()
            .map(|value| value.to_str().unwrap().to_ascii_lowercase())
            .collect();
        for header in ["accept-encoding", "originator", "anthropic-version"] {
            assert!(vary.iter().any(|value| value.contains(header)));
        }
        let expected_source = match format {
            "codex" | "openai" => Some(format),
            _ => None,
        };
        assert_eq!(
            response
                .headers()
                .get("x-catalog-source")
                .map(|value| value.to_str().unwrap()),
            expected_source
        );
        let document: Value = response.json().await.unwrap();
        let (array, id) = match format {
            "codex" => ("models", "slug"),
            _ => ("data", "id"),
        };
        let rows = document[array].as_array().unwrap();
        assert_eq!(
            rows.len(),
            if format == "codex" { 2 } else { 1 },
            "{format}: {document}"
        );
        let qwen = rows
            .iter()
            .find(|row| row[id] == "RadixArk/Qwen3.8-Flash-Next-NVFP4")
            .unwrap();
        match format {
            "codex" => {
                let gpt = rows.iter().find(|row| row[id] == "gpt-6-luna").unwrap();
                assert_eq!(document["codex_extension"], true);
                assert!(document.get("openai_extension").is_none());
                assert_eq!(gpt["native_codex"], json!({"keep":true}));
                assert_eq!(qwen["display_name"], "Qwen-3.8 Flash Next");
            }
            "openai" => {
                assert_eq!(document["object"], "list");
                assert_eq!(document["openai_extension"], true);
                assert!(document.get("codex_extension").is_none());
                assert_eq!(qwen["native_openai"], json!({"keep":true}));
                assert_eq!(qwen["created"], 123);
            }
            "anthropic" => {
                assert_eq!(document["has_more"], false);
                assert_eq!(document["first_id"], rows.first().unwrap()["id"]);
                assert_eq!(document["last_id"], rows.last().unwrap()["id"]);
                for row in rows {
                    assert_eq!(row["type"], "model");
                    assert!(row["display_name"].is_string());
                    assert!(row.get("native_codex").is_none());
                    assert!(row.get("native_openai").is_none());
                }
            }
            _ => unreachable!(),
        }
    }
    // Other clients can still list the local models without a usable ChatGPT
    // session, even though Codex projection needs that provider's donor row.
    for format in ["openai", "anthropic", "codex"] {
        let request = client
            .get(gateway.url.clone() + "/models")
            .bearer_auth("chatgpt-credential")
            .header("x-reject-chatgpt", "1");
        let request = match format {
            "anthropic" => request.header("anthropic-version", "2023-06-01"),
            "codex" => request.header("originator", "codex_cli_rs"),
            _ => request,
        };
        let response = request.send().await.unwrap();
        assert!(response.status().is_success());
        let document: Value = response.json().await.unwrap();
        if format == "codex" {
            assert_eq!(document["models"], json!([]));
        } else {
            assert_eq!(document["data"].as_array().unwrap().len(), 1);
            assert_eq!(
                document["data"][0]["id"],
                "RadixArk/Qwen3.8-Flash-Next-NVFP4"
            );
            assert_eq!(document["data"][0]["display_name"], "Qwen-3.8 Flash Next");
        }
    }
    let requests = seen.lock().unwrap();
    for (path, headers) in requests.iter() {
        if path == "/chatgpt/models" && headers.contains_key("originator") {
            assert_eq!(headers["authorization"], "Bearer chatgpt-credential");
        } else {
            assert!(!headers.contains_key("authorization"), "{path}");
        }
    }
}

#[tokio::test]
async fn catalog_without_a_primary_uses_client_format_and_does_not_forward_client_headers() {
    let seen = Requests::default();
    let upstream = serve(
        Router::new()
            .fallback(catalog_source)
            .with_state(seen.clone()),
    )
    .await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(
        &json!({"providers":{
            "chatgpt":{"base_url":upstream.url,"auth":"passthrough",
                "catalog":{"path":"/chatgpt/models","format":"codex"}},
            "openai":{"base_url":upstream.url,"auth":"none",
                "catalog":{"path":"/v1/models","format":"openai"}}
        }, "routes":[{"path":"/models","action":"catalog"}]})
        .to_string(),
        root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let response = client
        .get(gateway.url.clone() + "/models")
        .header("anthropic-version", "2023-06-01")
        .bearer_auth("private")
        .header("cookie", "private")
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    assert!(response.headers().get("x-catalog-source").is_none());
    let document: Value = response.json().await.unwrap();
    assert_eq!(document["data"].as_array().unwrap().len(), 1);
    assert!(document.get("models").is_none());
    for (_, headers) in seen.lock().unwrap().iter() {
        assert!(!headers.contains_key("authorization"));
        assert!(!headers.contains_key("cookie"));
        assert!(!headers.contains_key("anthropic-version"));
    }
}

#[tokio::test]
async fn client_provider_and_format_apply_independently_of_catalog_endpoint() {
    let seen = Requests::default();
    let upstream = serve(
        Router::new()
            .fallback(catalog_source)
            .with_state(seen.clone()),
    )
    .await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(
        &json!({"providers":{
            "chatgpt":{"base_url":upstream.url,"auth":"passthrough",
                "catalog":{"path":"/chatgpt/models","format":"codex"}},
            "openai":{"base_url":upstream.url,"auth":"none",
                "catalog":{"path":"/v1/models","format":"openai"}}
        }, "clients":{
            "openai":{"provider":"chatgpt","format":"openai"},
            "anthropic":{"provider":"chatgpt","format":"anthropic"}
        }, "routes":[
            {"path":"/models","action":"catalog"},
            {"path":"/another-catalog","action":"catalog"},
            {"path":"/responses","action":"model"}
        ]})
        .to_string(),
        root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for path in ["/models", "/another-catalog"] {
        for format in ["openai", "anthropic"] {
            let request = client
                .get(gateway.url.clone() + path)
                .bearer_auth("client-credential");
            let request = if format == "anthropic" {
                request.header("anthropic-version", "2023-06-01")
            } else {
                request
            };
            let response = request.send().await.unwrap();
            assert!(response.status().is_success());
            let document: Value = response.json().await.unwrap();
            assert!(document.get("models").is_none());
            let models = document["data"].as_array().unwrap();
            assert_eq!(models.len(), 2);
            let gpt = models
                .iter()
                .find(|model| model["id"] == "gpt-6-luna")
                .unwrap();
            assert!(gpt.get("native_codex").is_none());
            match format {
                "openai" => {
                    assert_eq!(document["object"], "list");
                    assert_eq!(gpt["object"], "model");
                    assert_eq!(gpt["owned_by"], "chatgpt");
                }
                "anthropic" => {
                    assert_eq!(document["has_more"], false);
                    assert_eq!(gpt["type"], "model");
                }
                _ => unreachable!(),
            }
        }
    }
    // Unknown IDs use the same client's provider without any endpoint binding.
    let response = client
        .post(gateway.url.clone() + "/responses")
        .bearer_auth("client-credential")
        .json(&json!({"model":"unknown"}))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let requests = seen.lock().unwrap();
    assert!(requests.iter().any(|(path, _)| path == "/responses"));
    for (path, headers) in requests.iter() {
        if matches!(path.as_str(), "/chatgpt/models" | "/responses") {
            assert_eq!(headers["authorization"], "Bearer client-credential");
        } else {
            assert!(!headers.contains_key("authorization"));
        }
    }
}
