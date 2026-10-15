use std::{
    io::Cursor,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU16, Ordering},
    },
    time::Duration,
};

use async_compression::tokio::bufread::GzipEncoder;
use axum::{
    Json, Router,
    body::{Body, Bytes, to_bytes},
    extract::{
        FromRequestParts, Request, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use model_router::{Config, router};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, BufReader},
    net::TcpListener,
    task::JoinHandle,
};
use tokio_tungstenite::tungstenite::{self, client::IntoClientRequest};

pub(crate) struct Server {
    pub(crate) url: String,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(crate) async fn serve(state: Router) -> Server {
    let listener = TcpListener::bind(("localhost", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        axum::serve(listener, state).await.unwrap();
    });
    Server {
        url: format!("http://localhost:{port}"),
        task,
    }
}

#[derive(Clone)]
pub(crate) struct Mock {
    pub(crate) name: &'static str,
    pub(crate) models: Arc<Mutex<Vec<Value>>>,
    pub(crate) status: Arc<AtomicU16>,
    pub(crate) seen: Arc<Mutex<Vec<Value>>>,
    pub(crate) cancelled: Arc<AtomicBool>,
}

impl Mock {
    pub(crate) fn new(name: &'static str, model: &str) -> Self {
        Self {
            name,
            models: Arc::new(Mutex::new(vec![
                json!({"slug": model, "display_name":model, "context_window":262144}),
            ])),
            status: Arc::new(AtomicU16::new(200)),
            seen: Arc::new(Mutex::new(Vec::new())),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }
}

pub(crate) async fn json_response(status: StatusCode, value: Value, compressed: bool) -> Response {
    if !compressed {
        return (status, Json(value)).into_response();
    }
    let mut encoded = Vec::new();
    GzipEncoder::new(BufReader::new(Cursor::new(value.to_string().into_bytes())))
        .read_to_end(&mut encoded)
        .await
        .unwrap();
    let mut response = Response::new(Body::from(encoded));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert("content-encoding", "gzip".parse().unwrap());
    response
        .headers_mut()
        .insert("content-type", "application/json".parse().unwrap());
    response
}

pub(crate) async fn mock(State(state): State<Mock>, request: Request) -> Response {
    let (mut parts, body) = request.into_parts();
    if parts.headers.get(header::UPGRADE).is_some() {
        if let Some(status) = parts
            .headers
            .get("x-reject-status")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u16>().ok())
        {
            let mut response = json_response(
                StatusCode::from_u16(status).unwrap(),
                json!({"error":{"message":"mock rejected"}}),
                parts.headers.contains_key("x-compress"),
            )
            .await;
            response
                .headers_mut()
                .insert("retry-after", "17".parse().unwrap());
            return response;
        }
        let upgrade = WebSocketUpgrade::from_request_parts(&mut parts, &())
            .await
            .unwrap();
        let headers = parts.headers;
        return upgrade.on_upgrade(move |socket| echo(socket, state, headers));
    }
    let bytes = to_bytes(body, usize::MAX).await.unwrap();
    let seen = json!({ "provider":state.name, "path":parts.uri.path(), "query":parts.uri.query(), "body":String::from_utf8_lossy(&bytes), "headers":parts.headers.iter().filter_map(|(name,value)| value.to_str().ok().map(|value|(name.as_str(),value))).collect::<std::collections::BTreeMap<_,_>>() });
    state.seen.lock().unwrap().push(seen.clone());
    if parts.uri.path().ends_with("/models") {
        let status = StatusCode::from_u16(state.status.load(Ordering::SeqCst)).unwrap();
        let value = if status == StatusCode::OK {
            if parts.headers.contains_key("x-catalog-data") {
                json!({"object":"list","data":state.models.lock().unwrap().clone(),"description":"Mock catalog","extensions":{"opaque":[1,2]}})
            } else {
                json!({"models":state.models.lock().unwrap().clone()})
            }
        } else {
            json!({"error":{"message":"catalog rejected"}})
        };
        let mut response =
            json_response(status, value, parts.headers.contains_key("x-compress")).await;
        response
            .headers_mut()
            .insert("etag", "\"catalog-tag\"".parse().unwrap());
        response
            .headers_mut()
            .insert("retry-after", "17".parse().unwrap());
        return response;
    }
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    if value.get("stream").and_then(Value::as_bool) == Some(true) {
        let guard = StreamGuard(state.cancelled.clone());
        let stream = futures_util::stream::unfold((0, guard), |(state, guard)| async move {
            match state {
                0 => Some((
                    Ok::<_, std::io::Error>(Bytes::from_static(b"data: first\n\n")),
                    (1, guard),
                )),
                1 => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    Some((Ok(Bytes::from_static(b"data: last\n\n")), (2, guard)))
                }
                _ => None,
            }
        });
        let mut response = Response::new(Body::from_stream(stream));
        response
            .headers_mut()
            .insert("content-type", "text/event-stream".parse().unwrap());
        return response;
    }
    Json(seen).into_response()
}

async fn echo(mut socket: WebSocket, state: Mock, headers: axum::http::HeaderMap) {
    while let Some(Ok(message)) = socket.recv().await {
        let bytes = match message {
            Message::Text(text) => Bytes::from(text),
            Message::Binary(bytes) => bytes,
            Message::Close(_) => break,
            _ => continue,
        };
        let payload: Value = serde_json::from_slice(&bytes).unwrap();
        let response = json!({"provider":state.name,"model":payload["model"],"body":String::from_utf8_lossy(&bytes),"authorization":headers.get("authorization").and_then(|value|value.to_str().ok()),"cookie":headers.get("cookie").and_then(|value|value.to_str().ok()),"api-key":headers.get("x-api-key").and_then(|value|value.to_str().ok())});
        if socket
            .send(Message::Text(response.to_string().into()))
            .await
            .is_err()
        {
            break;
        }
    }
}

struct StreamGuard(Arc<AtomicBool>);
impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

pub(crate) struct Fixture {
    pub(crate) root: TempDir,
    pub(crate) live: Mock,
    pub(crate) routed: Server,
    pub(crate) _live_server: Server,
    pub(crate) _manual_server: Server,
    pub(crate) client: reqwest::Client,
}

pub(crate) fn test_config(
    providers: Value,
    default_provider: Option<&str>,
    catalog_provider: Option<&str>,
    root: &std::path::Path,
) -> Config {
    let mut routes = Vec::new();
    if let Some(provider) = catalog_provider {
        routes
            .push(json!({"method":"GET","path":"/models","action":"catalog","format":"codex","provider":provider}));
    }
    if providers.get("service-b").is_some() {
        routes.push(
            json!({"method":"GET","path":"/v1/models","action":"forward","provider":"service-b"}),
        );
    }
    for provider in providers.as_object().unwrap().keys() {
        routes.push(
            json!({"headers":{"x-route-provider":provider},"action":"forward","provider":provider}),
        );
    }
    if let Some(provider) = default_provider {
        routes.push(json!({"action":"forward","provider":provider}));
    }
    parse_config(
        &json!({"providers":providers,"routes":routes}).to_string(),
        root,
    )
    .unwrap()
}

/// Keep JSON-shaped fixtures convenient while exercising the TOML configuration parser.
pub(crate) fn parse_config(json: &str, root: &std::path::Path) -> anyhow::Result<Config> {
    let document: Value = serde_json::from_str(json)?;
    Config::from_toml(&toml::to_string(&document)?, root)
}

pub(crate) fn ws_request(url: String, provider: &str) -> tungstenite::handshake::client::Request {
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("x-route-provider", provider.parse().unwrap());
    request
}

impl Fixture {
    pub(crate) async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let live = Mock::new("service-a", "gpt-test");
        let manual = Mock::new("service-b", "publisher/model");
        let live_server = serve(Router::new().fallback(mock).with_state(live.clone())).await;
        let manual_server = serve(Router::new().fallback(mock).with_state(manual)).await;
        std::fs::write(root.path().join("models_cache.json"), json!({"identity":"cache-identity","fetched_at":"2000-01-01T00:00:00Z","models":live.models.lock().unwrap().clone()}).to_string()).unwrap();
        std::fs::write(root.path().join("models.json"), json!({"models":[{"slug":"publisher/model","display_name":"Manually managed","context_window":262144}]}).to_string()).unwrap();
        let config = test_config(
            json!({
                "service-a":{"base_url":format!("{}/backend-api/codex",live_server.url),"auth":"passthrough"},
                "service-b":{"base_url":manual_server.url,"auth":"none","catalog":{"file":"models.json","format":"codex"},"http_headers":{"x-api-key":"configured-key"}}
            }),
            None,
            Some("service-a"),
            root.path(),
        );
        let routed = serve(router(config).unwrap()).await;
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        assert_eq!(
            client
                .get(format!("{}/models", routed.url))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        Self {
            root,
            live,
            routed,
            _live_server: live_server,
            _manual_server: manual_server,
            client,
        }
    }
    pub(crate) async fn post(&self, provider: &str, model: &str) -> reqwest::Response {
        self.client
            .post(format!("{}/v1/responses", self.routed.url))
            .header("x-route-provider", provider)
            .bearer_auth("client-token")
            .header("cookie", "client-cookie")
            .header("chatgpt-account-id", "client-account")
            .json(&json!({"model":model}))
            .send()
            .await
            .unwrap()
    }
}
