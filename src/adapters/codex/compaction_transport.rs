use std::{io, sync::Arc, time::Duration};

use axum::{
    Json,
    body::{Body, Bytes},
    http::{HeaderMap, Method, StatusCode, request::Parts},
    response::{IntoResponse, Response},
};
use futures_util::{FutureExt, TryStreamExt};
use serde_json::Value;
use tokio_util::io::StreamReader;

use super::{
    compaction::{self, SummaryRequest},
    response::clean_rewritten_headers,
    transport::prepare_multiagent,
};
use crate::{compression::decode_body, config::Provider, error::Error, router::RouterState};

pub(super) async fn summarize(
    state: Arc<RouterState>,
    provider: Arc<Provider>,
    origin: Option<Arc<Provider>>,
    mut parts: Parts,
    mut summary: SummaryRequest,
) -> Result<Value, Box<Response>> {
    parts.method = Method::POST;
    clean_rewritten_headers(&mut parts.headers);
    for header in [
        "sec-websocket-key",
        "sec-websocket-version",
        "sec-websocket-protocol",
        "sec-websocket-extensions",
    ] {
        parts.headers.remove(header);
    }
    parts
        .headers
        .insert("content-type", "application/json".parse().unwrap());
    parts
        .headers
        .insert("accept", "application/json".parse().unwrap());
    loop {
        let body = summary.body().map_err(IntoResponse::into_response)?;
        let model = crate::body::request_model(&body).map_err(IntoResponse::into_response)?;
        let body = prepare_multiagent(
            &state,
            &provider,
            origin.as_ref(),
            &parts,
            &body,
            model.as_deref(),
            "",
        )
        .await
        .map_err(IntoResponse::into_response)?
        .unwrap_or(body);
        let (client, mut request) = state
            .prepare_request(&provider, origin.as_ref(), &parts, body.into())
            .map_err(IntoResponse::into_response)?;
        // Internal summaries and trimmed retries have distinct payloads. Strip
        // caller and provider-configured keys after all headers are applied.
        request.headers_mut().remove("idempotency-key");
        let response = client
            .execute(request)
            .await
            .map_err(|error| Error::bad_gateway(error).into_response())?;
        let status = response.status();
        let headers = response.headers().clone();
        let body: reqwest::Body = response.into();
        let reader =
            StreamReader::new(Body::new(body).into_data_stream().map_err(io::Error::other));
        let bytes = decode_body(reader, &headers)
            .await
            .map_err(|error| Error::bad_gateway(error).into_response())?;
        if !status.is_success() {
            if compaction::is_context_overflow(status.as_u16(), &bytes) && summary.trim_oldest() {
                continue;
            }
            return Err(Box::new(reply(status, headers, bytes)));
        }
        return summary
            .finish(&bytes)
            .map_err(|error| Box::new(error.into_response()));
    }
}

fn reply(status: StatusCode, mut headers: HeaderMap, bytes: Vec<u8>) -> Response {
    clean_rewritten_headers(&mut headers);
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

pub(super) fn sse(
    work: impl std::future::Future<Output = Result<Value, Box<Response>>> + Send + 'static,
) -> Response {
    // Returning the body immediately lets a dropped client cancel summary generation.
    let stream = futures_util::stream::unfold(
        (
            Some(work.boxed()),
            tokio::time::interval(Duration::from_secs(15)),
        ),
        |(mut work, mut heartbeat)| async move {
            let pending = work.as_mut()?;
            let bytes = tokio::select! {
                result = pending => {
                    work = None;
                    match result {
                        Ok(response) => encode_sse(compaction::events(&response, None)),
                        Err(response) => encode_sse(vec![failure_event(*response, None).await]),
                    }
                }
                _ = heartbeat.tick() => Bytes::from_static(b": compacting\n\n"),
            };
            Some((Ok::<_, io::Error>(bytes), (work, heartbeat)))
        },
    );
    Response::builder()
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(Body::from_stream(stream))
        .unwrap()
}

fn encode_sse(events: Vec<Value>) -> Bytes {
    let mut bytes = Vec::new();
    for event in events {
        bytes.extend_from_slice(b"data: ");
        serde_json::to_writer(&mut bytes, &event).expect("JSON event serializes");
        bytes.extend_from_slice(b"\n\n");
    }
    bytes.into()
}

pub(super) async fn failure_event(response: Response, stream_id: Option<&Value>) -> Value {
    let status = response.status().as_u16();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap_or_default();
    let error = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|body| body.get("error").cloned())
        .unwrap_or_else(|| serde_json::json!({"message":String::from_utf8_lossy(&body)}));
    let mut event = serde_json::json!({"type":"error", "status":status, "error":error});
    if let Some(stream_id) = stream_id {
        event["stream_id"] = stream_id.clone();
    }
    event
}

pub(super) fn json(response: Value) -> Response {
    Json(response).into_response()
}
