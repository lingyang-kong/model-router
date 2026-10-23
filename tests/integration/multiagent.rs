use std::{
    io::Cursor,
    sync::{Arc, Mutex},
};

use async_compression::tokio::bufread::GzipEncoder;
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::State,
    http::{StatusCode, header},
    response::Response,
};
use model_router::router;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, BufReader};

use crate::common::{parse_config, serve};

#[tokio::test]
async fn omitted_model_uses_provider_default_for_probe_and_actual_request() {
    async fn endpoint(
        State(requests): State<Arc<Mutex<Vec<Value>>>>,
        axum::Json(body): axum::Json<Value>,
    ) -> Response {
        requests.lock().unwrap().push(body.clone());
        if body["input"][0]["type"] == "agent_message" {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"error":{"message":
                        "Unsupported Responses API input item type: 'agent_message'"
                    }})
                    .to_string(),
                ))
                .unwrap();
        }
        Response::new(Body::from(body.to_string()))
    }
    let requests = Arc::new(Mutex::new(Vec::new()));
    let upstream = serve(
        Router::new()
            .fallback(endpoint)
            .with_state(requests.clone()),
    )
    .await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(
        &json!({
            "clients":{"codex":{"provider":"api"},"openai":{"provider":"api"},"anthropic":{"provider":"api"}},"providers":{"api":{"base_url":upstream.url,"auth":"none"}},
            "routes":[{"action":"model"}]
        })
        .to_string(),
        root.path(),
    )
    .unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(format!("{}/responses", gateway.url))
        .header("originator", "codex_cli_rs")
        .json(&json!({"input":[{"type":"agent_message","author":"/root",
            "content":[{"type":"input_text","text":"task"}]}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.get("model").is_none())
    );
    assert_eq!(
        requests[0]["input"][1]["type"],
        "__model_router_capability_check__"
    );
    assert_eq!(requests[1]["input"][0]["type"], "message");
    assert_eq!(requests[1]["input"][0]["role"], "user");
}

#[derive(Clone)]
struct Upstream {
    key: &'static str,
    probe: ProbeResponse,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    probes: Arc<Mutex<Vec<CapturedRequest>>>,
    methods: Arc<Mutex<Vec<(String, String)>>>,
}

#[derive(Clone, Copy)]
enum ProbeResponse {
    Native,
    Custom,
    CustomBadRequest,
    Unknown,
    CustomThenUnknown,
}

#[derive(Clone)]
struct CapturedRequest {
    body: Value,
    auth: Option<String>,
    content_encoding: bool,
    digest: bool,
    method: String,
    path: String,
    idempotency_key: Option<String>,
}

async fn capture(State(upstream): State<Upstream>, request: axum::extract::Request) -> Response {
    let method = request.method().to_string();
    let path = request.uri().path().to_owned();
    upstream
        .methods
        .lock()
        .unwrap()
        .push((method.clone(), path.clone()));
    let auth = request
        .headers()
        .get("x-upstream-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let has_encoding = request.headers().contains_key("content-encoding");
    let has_digest = request.headers().contains_key("digest");
    let idempotency_key = request
        .headers()
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    assert_eq!(auth.as_deref(), Some(upstream.key));
    let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
    if bytes.is_empty() {
        return Response::new(Body::from(r#"{"ok":true}"#));
    }
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    let is_probe = value["store"] == false
        && value["max_output_tokens"] == 1
        && value["input"][0]["type"] == "agent_message"
        && value["input"][0]["author"] == "/root"
        && value["input"][0]["recipient"] == "/root/worker"
        && value["input"][0]["content"][0]["text"] == "probe"
        && value["input"][1]["type"] == "__model_router_capability_check__";
    if is_probe {
        let probe_index = upstream.probes.lock().unwrap().len();
        upstream.probes.lock().unwrap().push(CapturedRequest {
            body: value,
            auth,
            content_encoding: has_encoding,
            digest: has_digest,
            method,
            path,
            idempotency_key,
        });
        let probe_response = match upstream.probe {
            ProbeResponse::CustomThenUnknown if probe_index == 0 => ProbeResponse::CustomBadRequest,
            ProbeResponse::CustomThenUnknown => ProbeResponse::Unknown,
            response => response,
        };
        let mut response = match probe_response {
            ProbeResponse::Native => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(r#"{"error":{"message":"Unsupported Responses API input item type: '__model_router_capability_check__'"}}"#))
                .unwrap(),
            ProbeResponse::Custom => Response::builder()
                .status(StatusCode::UNPROCESSABLE_ENTITY)
                .body(Body::from(r#"{"error":{"message":"Unsupported Responses API input item type: 'agent_message'"}}"#))
                .unwrap(),
            ProbeResponse::CustomBadRequest => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(r#"{"error":{"message":"Unsupported Responses API input item type: 'agent_message'"}}"#))
                .unwrap(),
            ProbeResponse::Unknown => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(r#"{"error":{"message":"invalid request"}}"#))
                .unwrap(),
            ProbeResponse::CustomThenUnknown => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(r#"{"error":{"message":"invalid request"}}"#))
                .unwrap(),
        };
        response
            .headers_mut()
            .insert("x-upstream-result", upstream.key.parse().unwrap());
        return response;
    }
    upstream.requests.lock().unwrap().push(CapturedRequest {
        body: value.clone(),
        auth,
        content_encoding: has_encoding,
        digest: has_digest,
        method,
        path,
        idempotency_key,
    });

    let mut response = if value["model"] == "parent-model" {
        let mut encoded = Vec::new();
        GzipEncoder::new(BufReader::new(Cursor::new(json!({
            "object":"response",
            "output":[{"type":"function_call","namespace":"collaboration_compat","name":"spawn_agent","call_id":"call-1","arguments":"{}"}]
        }).to_string().into_bytes())))
        .read_to_end(&mut encoded).await.unwrap();
        let mut response = Response::new(Body::from(encoded));
        *response.status_mut() = StatusCode::ACCEPTED;
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        response
            .headers_mut()
            .insert(header::CONTENT_ENCODING, "gzip".parse().unwrap());
        response
    } else {
        Response::new(Body::from(r#"{"object":"response","output":[]}"#))
    };
    response
        .headers_mut()
        .insert("x-upstream-result", upstream.key.parse().unwrap());
    response
}

async fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    GzipEncoder::new(BufReader::new(Cursor::new(bytes)))
        .read_to_end(&mut encoded)
        .await
        .unwrap();
    encoded
}

#[tokio::test]
async fn codex_multiagent_requests_are_rewritten_and_responses_restored() {
    let parent_requests = Arc::new(Mutex::new(Vec::new()));
    let worker_requests = Arc::new(Mutex::new(Vec::new()));
    let parent_probes = Arc::new(Mutex::new(Vec::new()));
    let worker_probes = Arc::new(Mutex::new(Vec::new()));
    let parent_methods = Arc::new(Mutex::new(Vec::new()));
    let worker_methods = Arc::new(Mutex::new(Vec::new()));
    let parent_server = serve(Router::new().fallback(capture).with_state(Upstream {
        key: "parent-secret",
        probe: ProbeResponse::Native,
        requests: parent_requests.clone(),
        probes: parent_probes.clone(),
        methods: parent_methods.clone(),
    }))
    .await;
    let worker_server = serve(Router::new().fallback(capture).with_state(Upstream {
        key: "worker-secret",
        probe: ProbeResponse::Custom,
        requests: worker_requests.clone(),
        probes: worker_probes.clone(),
        methods: worker_methods,
    }))
    .await;
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("parent.json"),
        json!({"models":[{"slug":"parent-model","supported_reasoning_levels":[{"effort":"high"}]}]}).to_string(),
    )
    .unwrap();
    std::fs::write(
        root.path().join("worker.json"),
        json!({"models":[{"slug":"worker-model"},{"slug":"hidden-worker-model"}]}).to_string(),
    )
    .unwrap();
    std::fs::write(
        root.path().join("unknown.json"),
        json!({"models":[{"slug":"unknown-model"}]}).to_string(),
    )
    .unwrap();
    let unknown_requests = Arc::new(Mutex::new(Vec::new()));
    let unknown_probes = Arc::new(Mutex::new(Vec::new()));
    let unknown_server = serve(Router::new().fallback(capture).with_state(Upstream {
        key: "unknown-secret",
        probe: ProbeResponse::CustomThenUnknown,
        requests: unknown_requests.clone(),
        probes: unknown_probes.clone(),
        methods: Arc::new(Mutex::new(Vec::new())),
    }))
    .await;
    let config = parse_config(&json!({
        "clients":{"codex":{"provider":"parent","format":"codex"},"openai":{"provider":"parent","format":"codex"},"anthropic":{"provider":"parent","format":"codex"}},"providers": {
            "parent":{"base_url":parent_server.url,"auth":"none","http_headers":{"x-upstream-key":"parent-secret"},"catalog":{"file":"parent.json","format":"codex"}},
            "worker":{"base_url":worker_server.url,"auth":"none","http_headers":{"x-upstream-key":"worker-secret"},"catalog":{"file":"worker.json","format":"codex"},"hidden_models":["hidden-*"]},
            "unknown":{"base_url":unknown_server.url,"auth":"none","http_headers":{"x-upstream-key":"unknown-secret"},"catalog":{"file":"unknown.json","format":"codex"}}
        },
        "routes":[{"path":"/models","action":"catalog"},{"path_prefix":"/responses","action":"model"}]
    }).to_string(), root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let client = reqwest::Client::builder()
        .no_proxy()
        .no_gzip()
        .build()
        .unwrap();

    // Loading the catalog exposes all visible routing models to the agent tool.
    let catalog = client
        .get(format!("{}/models", gateway.url))
        .send()
        .await
        .unwrap();
    assert!(catalog.status().is_success());
    let catalog: Value = catalog.json().await.unwrap();
    assert!(
        catalog["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["slug"] == "parent-model")
    );
    assert!(
        catalog["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["slug"] == "worker-model")
    );
    assert!(
        catalog["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["slug"] == "unknown-model")
    );
    assert!(
        !catalog["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["slug"] == "hidden-worker-model")
    );

    let initial = json!({"model":"parent-model","tools":[{"type":"namespace","name":"collaboration","tools":[
        {"type":"function","name":"spawn_agent","description":"Old native model prefix. Spawns an agent for delegated work.","parameters":{"properties":{"message":{"type":"string","encrypted":true}}}}
    ]}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"collaboration","tools":[
        {"type":"function","name":"send_message","parameters":{"properties":{"message":{"type":"string","encrypted":true}}}}
    ]}]}]});
    let response = client
        .post(format!("{}/responses", gateway.url))
        .header("originator", "codex_cli_rs")
        .header("content-encoding", "gzip")
        .header("digest", "sha-256=stale")
        .body(
            gzip(
                initial
                    .to_string()
                    .replace("collaboration", r"collab\u006fration")
                    .as_bytes(),
            )
            .await,
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(response.headers()["x-upstream-result"], "parent-secret");
    assert!(!response.headers().contains_key("content-encoding"));
    let restored: Value = response.json().await.unwrap();
    assert_eq!(restored["output"][0]["namespace"], "collaboration");
    assert_eq!(restored["output"][0]["name"], "spawn_agent");
    assert_eq!(restored["output"][0]["encrypted_function_args"], json!([]));

    let worker_message = json!({"model":"worker-model","input":[{"type":"agent_message","author":"worker-1","recipient":"parent","internal_chat_message_metadata_passthrough":{"opaque":true},"content":[{"type":"input_text","text":"finished"}]}]});
    let response = client
        .post(format!("{}/responses", gateway.url))
        .header("originator", "codex_cli_rs")
        .header("idempotency-key", "retain-on-inference")
        .json(&worker_message)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    let worker_wire = worker_requests.lock().unwrap()[0].body.clone();
    assert_eq!(worker_wire["input"][0]["type"], "message");
    assert_eq!(worker_wire["input"][0]["role"], "user");
    assert!(worker_wire["input"][0].get("author").is_none());
    assert!(
        worker_wire["input"][0]
            .get("internal_chat_message_metadata_passthrough")
            .is_none()
    );

    let replay = json!({"model":"parent-model","input":[{"type":"function_call","namespace":"collaboration","name":"spawn_agent","call_id":"call-1","arguments":"{}"},{"type":"function_call_output","call_id":"call-1","output":"done"}]});
    client
        .post(format!("{}/responses", gateway.url))
        .header("originator", "codex_cli_rs")
        .json(&replay)
        .send()
        .await
        .unwrap();
    let parent_wire = parent_requests.lock().unwrap().clone();
    assert_eq!(parent_wire.len(), 2);
    assert_eq!(
        parent_wire[0].body["tools"][0]["name"],
        "collaboration_compat"
    );
    assert!(
        parent_wire[0].body["tools"][0]["tools"][0]["parameters"]["properties"]["message"]
            .get("encrypted")
            .is_none()
    );
    assert_eq!(
        parent_wire[0].body["input"][0]["tools"][0]["name"],
        "collaboration_compat"
    );
    let agent_description = parent_wire[0].body["tools"][0]["tools"][0]["description"]
        .as_str()
        .unwrap();
    assert!(agent_description.starts_with("Available model overrides"));
    assert!(agent_description.contains("parent-model"));
    assert!(agent_description.contains("worker-model"));
    assert!(agent_description.contains("unknown-model"));
    assert!(agent_description.contains("Reasoning efforts: high"));
    assert!(agent_description.contains("Spawns an agent"));
    assert!(!agent_description.contains("Old native model prefix"));
    assert!(
        parent_wire[0].body["input"][0]["tools"][0]["tools"][0]["parameters"]["properties"]["message"]
            .get("encrypted")
            .is_none()
    );
    assert!(!parent_wire[0].content_encoding);
    assert!(!parent_wire[0].digest);
    assert_eq!(
        parent_wire[1].body["input"][0]["namespace"],
        "collaboration_compat"
    );
    assert_eq!(parent_wire[1].body["input"][0]["name"], "spawn_agent");
    assert!(
        parent_wire
            .iter()
            .all(|request| request.auth.as_deref() == Some("parent-secret"))
    );
    assert_eq!(
        worker_requests.lock().unwrap()[0].auth.as_deref(),
        Some("worker-secret")
    );
    let worker_probe = worker_probes.lock().unwrap()[0].clone();
    assert_eq!(worker_probe.auth.as_deref(), Some("worker-secret"));
    assert_eq!(worker_probe.body["model"], "worker-model");
    assert_eq!(worker_probe.body["store"], false);
    assert_eq!(worker_probe.body["max_output_tokens"], 1);
    assert_eq!(
        worker_probe.body["input"][1]["type"],
        "__model_router_capability_check__"
    );
    assert_eq!(worker_probe.method, "POST");
    assert_eq!(worker_probe.path, "/responses");
    assert!(!worker_probe.content_encoding);
    assert!(!worker_probe.digest);
    assert_eq!(worker_probe.idempotency_key, None);
    assert_eq!(
        worker_requests.lock().unwrap()[0]
            .idempotency_key
            .as_deref(),
        Some("retain-on-inference")
    );
    assert_eq!(parent_probes.lock().unwrap().len(), 0);
    let native = json!({"model":"parent-model","input":[{"type":"agent_message","author":"worker-1","content":[{"type":"encrypted_content","encrypted_content":"plain payload"}]}]});
    client
        .post(format!("{}/responses", gateway.url))
        .header("originator", "codex_cli_rs")
        .json(&native)
        .send()
        .await
        .unwrap();
    let parent_wire = parent_requests.lock().unwrap().clone();
    assert_eq!(parent_wire[2].body["input"][0]["type"], "agent_message");
    assert_eq!(
        parent_wire[2].body["input"][0]["content"][0],
        json!({"type":"input_text","text":"plain payload"})
    );
    assert_eq!(parent_probes.lock().unwrap().len(), 1);

    // Probe behavior is request-time and applies to each actual agent_message request.
    let unmarked = json!({"model":"worker-model","input":[{"type":"agent_message","author":"worker-1","content":[{"type":"input_text","text":"native"}]}]});
    client
        .post(format!("{}/responses", gateway.url))
        .header("originator", "codex_cli_rs")
        .json(&unmarked)
        .send()
        .await
        .unwrap();
    let captured_worker_requests = worker_requests.lock().unwrap().clone();
    let worker_wire = &captured_worker_requests[1].body;
    assert_eq!(worker_wire["input"][0]["type"], "message");
    assert_eq!(worker_wire["input"][0]["role"], "user");
    assert_eq!(worker_probes.lock().unwrap().len(), 2);

    // Prose and nested mentions do not trigger capability probes.
    let ordinary = json!({"model":"worker-model","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"agent_message is just prose"}]},{"type":"function_call_output","output":{"type":"agent_message"}}]});
    client
        .post(format!("{}/responses", gateway.url))
        .header("originator", "codex_cli_rs")
        .json(&ordinary)
        .send()
        .await
        .unwrap();
    assert_eq!(worker_probes.lock().unwrap().len(), 2);
    assert_eq!(worker_requests.lock().unwrap().len(), 3);

    // A change from explicit rejection to an unknown result is detected afresh.
    let unknown_message = json!({"model":"unknown-model","input":[{"type":"agent_message","author":"worker-1","recipient":"root","extra":{"nested":true},"content":[{"type":"input_text","text":"hello"}]}]});
    for _ in 0..2 {
        let response = client
            .post(format!("{}/responses", gateway.url))
            .header("originator", "codex_cli_rs")
            .json(&unknown_message)
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
    }
    assert_eq!(unknown_probes.lock().unwrap().len(), 2);
    assert_eq!(unknown_requests.lock().unwrap().len(), 2);
    assert_eq!(
        unknown_requests.lock().unwrap()[0].body["input"][0]["type"],
        "message"
    );
    assert_eq!(
        unknown_requests.lock().unwrap()[1].body["input"][0],
        unknown_message["input"][0]
    );

    // A plain listing includes native and custom models, independent of probe behavior.
    let listing = client
        .get(format!("{}/models", gateway.url))
        .send()
        .await
        .unwrap();
    assert!(listing.status().is_success());
    let listed = listing.json::<Value>().await.unwrap();
    assert!(
        listed["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["slug"] == "parent-model")
    );
    assert!(
        listed["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["slug"] == "worker-model")
    );
    assert!(
        listed["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["slug"] == "unknown-model")
    );
    assert!(
        !listed["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["slug"] == "hidden-worker-model")
    );

    for (method, path) in [
        (reqwest::Method::GET, "/responses/resp-1"),
        (reqwest::Method::DELETE, "/responses/resp-1"),
        (reqwest::Method::POST, "/responses/resp-1/cancel"),
    ] {
        let response = client
            .request(method, format!("{}{}", gateway.url, path))
            .header("originator", "codex_cli_rs")
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        assert_eq!(response.json::<Value>().await.unwrap(), json!({"ok":true}));
    }
    let methods = parent_methods.lock().unwrap();
    assert!(
        methods
            .iter()
            .any(|(method, path)| method == "GET" && path == "/responses/resp-1")
    );
    assert!(
        methods
            .iter()
            .any(|(method, path)| method == "DELETE" && path == "/responses/resp-1")
    );
    assert!(
        methods
            .iter()
            .any(|(method, path)| method == "POST" && path == "/responses/resp-1/cancel")
    );
}
