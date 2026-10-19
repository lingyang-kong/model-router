use std::{
    io::Cursor,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use async_compression::tokio::bufread::GzipEncoder;
use axum::{
    Json, Router,
    body::{Body, Bytes, to_bytes},
    extract::{Request, State},
    http::StatusCode,
    response::Response,
};
use futures_util::StreamExt;
use model_router::router;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, BufReader},
    time::{Instant, timeout},
};

use crate::common::*;
use axum::http::HeaderMap;
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use std::{io, path::Path};

#[tokio::test]
async fn streams_responses_without_waiting_for_completion() {
    let fixture = Fixture::new().await;
    let started = Instant::now();
    let response = fixture
        .client
        .post(format!("{}/responses", fixture.routed.url))
        .header("x-route-provider", "service-a")
        .json(&json!({"model":"gpt-test","stream":true}))
        .send()
        .await
        .unwrap();
    let mut stream = response.bytes_stream();
    assert_eq!(stream.next().await.unwrap().unwrap(), "data: first\n\n");
    assert!(started.elapsed() < Duration::from_millis(350));
    assert_eq!(stream.next().await.unwrap().unwrap(), "data: last\n\n");
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn dropping_a_streaming_response_cancels_the_upstream_body() {
    let fixture = Fixture::new().await;
    let response = fixture
        .client
        .post(format!("{}/responses", fixture.routed.url))
        .header("x-route-provider", "service-a")
        .json(&json!({"model":"gpt-test","stream":true}))
        .send()
        .await
        .unwrap();
    let mut stream = response.bytes_stream();
    assert!(stream.next().await.unwrap().is_ok());
    drop(stream);
    timeout(Duration::from_millis(350), async {
        while !fixture.live.cancelled.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ordinary_compressed_responses_are_forwarded_byte_for_byte() {
    let mut encoded = Vec::new();
    GzipEncoder::new(BufReader::new(Cursor::new(
        b"data: first\n\ndata: last\n\n",
    )))
    .read_to_end(&mut encoded)
    .await
    .unwrap();
    async fn endpoint(State(encoded): State<Vec<u8>>) -> Response {
        let mut response = Response::new(Body::from(encoded));
        response
            .headers_mut()
            .insert("content-encoding", "gzip".parse().unwrap());
        response
            .headers_mut()
            .insert("content-type", "text/event-stream".parse().unwrap());
        response
    }
    let backend = serve(Router::new().fallback(endpoint).with_state(encoded.clone())).await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(&json!({"providers":{"api":{"base_url":backend.url,"auth":"none"}},"routes":[{"action":"forward","provider":"api"}]}).to_string(),root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let response = reqwest::Client::builder()
        .no_proxy()
        .no_gzip()
        .build()
        .unwrap()
        .get(gateway.url.clone() + "/events")
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["content-encoding"], "gzip");
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    assert_eq!(response.bytes().await.unwrap().as_ref(), encoded.as_slice());
}

#[tokio::test]
async fn opaque_requests_forward_before_the_rest_of_the_body_arrives() {
    use tokio::sync::Notify;
    let arrived = Arc::new(Notify::new());
    async fn receive(State(arrived): State<Arc<Notify>>, request: Request) -> Json<Value> {
        arrived.notify_one();
        let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
        Json(json!({"body":String::from_utf8_lossy(&bytes)}))
    }
    let backend = serve(Router::new().fallback(receive).with_state(arrived.clone())).await;
    let root = tempfile::tempdir().unwrap();
    let config = parse_config(&json!({"providers":{"backend":{"base_url":backend.url,"auth":"none"}},"routes":[{"headers":{"x-target":"backend"},"action":"forward","provider":"backend"}]}).to_string(),root.path()).unwrap();
    let gateway = serve(router(config).unwrap()).await;
    let first = Bytes::from_static(b"opaque-prefix|");
    let remaining = futures_util::stream::once(async move {
        timeout(Duration::from_secs(2), arrived.notified())
            .await
            .expect("router waited for the request body before forwarding");
        Ok::<_, std::io::Error>(Bytes::from_static(b"opaque-suffix"))
    });
    let stream =
        futures_util::stream::once(async move { Ok::<_, std::io::Error>(first) }).chain(remaining);
    let response: Value = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(gateway.url.clone() + "/anything")
        .header("x-target", "backend")
        .body(reqwest::Body::wrap_stream(stream))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["body"], "opaque-prefix|opaque-suffix");
}

async fn gateway(upstream: &Server, routes: Value) -> Server {
    let config = parse_config(
        &json!({
            "providers": {"api": {"base_url": upstream.url, "auth": "none"}},
            "routes": routes,
        })
        .to_string(),
        Path::new("."),
    )
    .unwrap();
    serve(router(config).unwrap()).await
}

fn body_with_trailer(
    bytes: &'static [u8],
    name: &'static str,
    value: &'static str,
) -> reqwest::Body {
    let mut trailers = HeaderMap::new();
    trailers.insert(name, value.parse().unwrap());
    let frames = [
        Ok::<_, io::Error>(Frame::data(Bytes::from_static(bytes))),
        Ok(Frame::trailers(trailers)),
    ];
    reqwest::Body::wrap(StreamBody::new(futures_util::stream::iter(frames)))
}

#[tokio::test]
async fn request_bodies_and_checksums_survive_forward_and_model_routes() {
    async fn inspect(request: Request) -> axum::Json<Value> {
        let collected = request.into_body().collect().await.unwrap();
        let trailer = collected
            .trailers()
            .and_then(|headers| headers.get("x-checksum"))
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        axum::Json(json!({
            "body": String::from_utf8(collected.to_bytes().to_vec()).unwrap(),
            "checksum": trailer,
        }))
    }
    let upstream = serve(Router::new().fallback(inspect)).await;
    let gateway = gateway(
        &upstream,
        json!([
            {"path": "/forward", "action": "forward", "provider": "api"},
            {"path": "/model", "action": "model", "provider": "api"}
        ]),
    )
    .await;
    let client = reqwest::Client::builder()
        .no_proxy()
        .http1_only()
        .build()
        .unwrap();
    let input = br#"{ "model": "native-id", "input": "untouched" }"#;
    for path in ["forward", "model"] {
        let observed: Value = client
            .post(format!("{}/{path}", gateway.url))
            .header("trailer", "x-checksum")
            .body(body_with_trailer(input, "x-checksum", "verified"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(observed["body"], String::from_utf8_lossy(input).as_ref());
        assert_eq!(observed["checksum"], "verified", "{path}");
    }
}

#[tokio::test]
async fn response_body_and_checksum_trailer_survive_forwarding() {
    async fn respond() -> Response {
        let mut response = Response::new(Body::new(StreamBody::new(futures_util::stream::iter([
            Ok::<_, io::Error>(Frame::data(Bytes::from_static(b"opaque payload"))),
            Ok(Frame::trailers({
                let mut trailers = HeaderMap::new();
                trailers.insert("x-checksum", "verified".parse().unwrap());
                trailers
            })),
        ]))));
        response
            .headers_mut()
            .insert("trailer", "x-checksum".parse().unwrap());
        response
    }
    let upstream = serve(Router::new().fallback(respond)).await;
    let gateway = gateway(&upstream, json!([{"action": "forward", "provider": "api"}])).await;
    let client = reqwest::Client::builder()
        .no_proxy()
        .http1_only()
        .build()
        .unwrap();
    let direct = client
        .get(&upstream.url)
        .header("te", "trailers")
        .header("connection", "te")
        .send()
        .await
        .unwrap();
    let direct: axum::http::Response<reqwest::Body> = direct.into();
    let direct = direct.into_body().collect().await.unwrap();
    assert_eq!(direct.trailers().unwrap()["x-checksum"], "verified");
    let response = client
        .get(&gateway.url)
        .header("te", "trailers")
        .header("connection", "te")
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["trailer"], "x-checksum");
    let response: axum::http::Response<reqwest::Body> = response.into();
    let collected = response.into_body().collect().await.unwrap();
    assert_eq!(collected.trailers().unwrap()["x-checksum"], "verified");
    assert_eq!(collected.to_bytes(), Bytes::from_static(b"opaque payload"));
}

#[tokio::test]
async fn every_connection_field_removes_its_named_request_and_response_headers() {
    async fn inspect(request: Request) -> Response {
        assert!(!request.headers().contains_key("x-hop-first"));
        assert!(!request.headers().contains_key("x-hop-second"));
        let mut response = Response::new(Body::from("ok"));
        response
            .headers_mut()
            .append("connection", "x-response-first".parse().unwrap());
        response
            .headers_mut()
            .append("connection", "x-response-second".parse().unwrap());
        response
            .headers_mut()
            .insert("x-response-first", "one".parse().unwrap());
        response
            .headers_mut()
            .insert("x-response-second", "two".parse().unwrap());
        response
            .headers_mut()
            .insert("x-end-to-end", "kept".parse().unwrap());
        response
    }
    let upstream = serve(Router::new().fallback(inspect)).await;
    let gateway = gateway(&upstream, json!([{"action": "forward", "provider": "api"}])).await;
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(&gateway.url)
        .header("connection", "x-hop-first")
        .header("connection", "x-hop-second")
        .header("x-hop-first", "one")
        .header("x-hop-second", "two")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.headers().contains_key("x-response-first"));
    assert!(!response.headers().contains_key("x-response-second"));
    assert_eq!(response.headers()["x-end-to-end"], "kept");
}

#[tokio::test]
async fn rewritten_catalog_drops_stale_integrity_and_trailer_declarations() {
    async fn catalog() -> Response {
        let mut response = Response::new(Body::new(StreamBody::new(futures_util::stream::iter([
            Ok::<_, io::Error>(Frame::data(Bytes::from_static(
                b"{ \"models\": [{\"slug\": \"native-id\"}] }",
            ))),
            Ok(Frame::trailers({
                let mut trailers = HeaderMap::new();
                trailers.insert("x-checksum", "old".parse().unwrap());
                trailers
            })),
        ]))));
        for (name, value) in [
            ("trailer", "x-checksum"),
            ("content-md5", "old"),
            ("digest", "old"),
            ("content-digest", "old"),
            ("repr-digest", "old"),
            ("etag", "\"old\""),
        ] {
            response.headers_mut().insert(name, value.parse().unwrap());
        }
        response
    }
    let upstream = serve(Router::new().fallback(catalog)).await;
    let gateway = gateway(
        &upstream,
        json!([{"path": "/models", "action": "catalog", "format": "codex", "provider": "api"}]),
    )
    .await;
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(format!("{}/models", gateway.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    for name in [
        "trailer",
        "content-md5",
        "digest",
        "content-digest",
        "repr-digest",
        "etag",
    ] {
        assert!(!response.headers().contains_key(name), "{name}");
    }
    let response: axum::http::Response<reqwest::Body> = response.into();
    let collected = response.into_body().collect().await.unwrap();
    assert!(collected.trailers().is_none());
    let value: Value = serde_json::from_slice(&collected.to_bytes()).unwrap();
    assert_eq!(value["models"][0]["slug"], "native-id");
}
