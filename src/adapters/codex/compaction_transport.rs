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
use crate::protocol::{field, header, media_type};
use crate::{compression::decode_body, config::Provider, error::Error, router::RouterState};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

pub(super) async fn summarize(
    state: Arc<RouterState>,
    provider: Arc<Provider>,
    origin: Option<Arc<Provider>>,
    mut parts: Parts,
    mut summary: SummaryRequest,
) -> Result<Value, Box<Response>> {
    parts.method = Method::POST;
    clean_rewritten_headers(&mut parts.headers);
    crate::config::remove_websocket_headers(&mut parts.headers);
    parts
        .headers
        .insert(header::CONTENT_TYPE, media_type::JSON.parse().unwrap());
    parts
        .headers
        .insert(header::ACCEPT, media_type::JSON.parse().unwrap());
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
        )
        .await
        .map_err(IntoResponse::into_response)?
        .unwrap_or(body);
        let (client, mut request) = state
            .prepare_request(&provider, origin.as_ref(), &parts, body.into())
            .map_err(IntoResponse::into_response)?;
        // Internal summaries and trimmed retries have distinct payloads. Strip
        // caller and provider-configured keys after all headers are applied.
        request.headers_mut().remove(header::IDEMPOTENCY_KEY);
        let response = client
            .execute(request)
            .await
            .map_err(|error| Error::bad_gateway(error).into_response())?;
        let status = response.status();
        let headers = response.headers().clone();
        let reader = crate::body::response_reader(response);
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
            tokio::time::interval(HEARTBEAT_INTERVAL),
        ),
        |(mut work, mut heartbeat)| async move {
            let pending = work.as_mut()?;
            let bytes = tokio::select! {
                result = pending => {
                    work = None;
                    match result {
                        Ok(response) => encode_sse(compaction::events(&response, None)),
                        Err(response) => encode_sse(vec![sse_failure_event(*response).await]),
                    }
                }
                _ = heartbeat.tick() => Bytes::from_static(b": compacting\n\n"),
            };
            Some((Ok::<_, io::Error>(bytes), (work, heartbeat)))
        },
    );
    Response::builder()
        .header(header::CONTENT_TYPE, media_type::EVENT_STREAM)
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(stream))
        .unwrap()
}

fn encode_sse(events: Vec<Value>) -> Bytes {
    let mut bytes = Vec::new();
    for event in events {
        bytes.extend_from_slice(super::SSE_DATA_PREFIX);
        serde_json::to_writer(&mut bytes, &event).expect("JSON event serializes");
        bytes.extend_from_slice(b"\n\n");
    }
    bytes.into()
}

async fn sse_failure_event(response: Response) -> Value {
    let (status, error) = failure_details(response).await;
    serde_json::json!({"type":"response.failed", "status":status, "response":{"status":"failed", "error":error}})
}

pub(super) async fn failure_event(response: Response, stream_id: Option<&Value>) -> Value {
    let (status, error) = failure_details(response).await;
    let mut event = serde_json::json!({"type":"error", "status":status, "error":error});
    if let Some(stream_id) = stream_id {
        event[field::STREAM_ID] = stream_id.clone();
    }
    event
}

async fn failure_details(response: Response) -> (u16, Value) {
    let (parts, body) = response.into_parts();
    let status = parts.status.as_u16();
    let reader = StreamReader::new(body.into_data_stream().map_err(io::Error::other));
    let body = decode_body(reader, &parts.headers)
        .await
        .unwrap_or_else(|error| error.to_string().into_bytes());
    let mut error = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|body| body.get(field::ERROR).cloned())
        .unwrap_or_else(|| serde_json::json!({"message":String::from_utf8_lossy(&body)}));
    // Codex expects string codes; a numeric code makes it ignore the error event.
    if let Some(code @ Value::Number(_)) = error.get_mut(field::CODE) {
        *code = Value::String(code.to_string());
    }
    if !error.is_object() {
        let message = error
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| error.to_string());
        error = serde_json::json!({"message":message});
    }
    if let Some(value) = parts
        .headers
        .get(axum::http::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
    {
        let headers = error
            .as_object_mut()
            .unwrap()
            .entry("headers")
            .or_insert_with(|| serde_json::json!({}));
        if !headers.is_object() {
            *headers = serde_json::json!({});
        }
        headers["retry-after"] = Value::String(value.into());
    }
    (status, error)
}

pub(super) fn json(response: Value) -> Response {
    Json(response).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn transport_errors_preserve_retry_advice() {
        fn response() -> Response {
            Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header("retry-after", "17")
                .body(Body::from(
                    r#"{"error":{"code":"server_is_overloaded","message":"busy"}}"#,
                ))
                .unwrap()
        }
        assert_eq!(
            sse_failure_event(response()).await,
            json!({"type":"response.failed", "status":503, "response":{"status":"failed", "error":{
                "code":"server_is_overloaded", "message":"busy", "headers":{"retry-after":"17"}
            }}})
        );
        assert_eq!(
            failure_event(response(), Some(&json!("lane"))).await,
            json!({"type":"error", "status":503, "stream_id":"lane",
                "error":{"code":"server_is_overloaded", "message":"busy", "headers":{"retry-after":"17"}}})
        );
    }

    #[tokio::test]
    async fn sse_stream_emits_failure_with_http_date_and_existing_metadata() {
        let response = Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("retry-after", "Wed, 21 Oct 2026 07:28:00 GMT")
            .body(Body::from(r#"{"error":{"code":"server_is_overloaded","message":"busy","headers":{"x-request-id":"abc"}}}"#))
            .unwrap();
        let stream = sse(async { Err(Box::new(response)) });
        assert_eq!(
            stream.headers()[header::CONTENT_TYPE],
            media_type::EVENT_STREAM
        );
        let bytes = axum::body::to_bytes(stream.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        let data = text
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .unwrap();
        let event: Value = serde_json::from_str(data).unwrap();
        assert_eq!(event["type"], "response.failed");
        assert_eq!(
            event["response"]["error"],
            json!({
                "code":"server_is_overloaded", "message":"busy", "headers":{
                    "x-request-id":"abc", "retry-after":"Wed, 21 Oct 2026 07:28:00 GMT"
                }
            })
        );
    }

    #[tokio::test]
    async fn compressed_text_errors_use_the_decoded_message() {
        let response = Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .header("content-encoding", "gzip")
            .body(crate::compression::encode_body(
                b"upstream unavailable".to_vec(),
                true,
            ))
            .unwrap();
        assert_eq!(
            failure_event(response, None).await,
            json!({"type":"error", "status":502, "error":{"message":"upstream unavailable"}})
        );
    }

    #[tokio::test]
    async fn failure_events_normalize_numeric_codes_and_preserve_other_fields() {
        for (code, expected) in [
            (json!(400), json!("400")),
            (
                json!("previous_response_not_found"),
                json!("previous_response_not_found"),
            ),
            (Value::Null, Value::Null),
        ] {
            let response = (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":{"code":code,"message":"upstream rejected request","type":"invalid_request_error"}})),
            )
                .into_response();
            let event = tokio::time::timeout(
                Duration::from_secs(1),
                failure_event(response, Some(&json!("lane"))),
            )
            .await
            .expect("upstream errors must be emitted promptly");
            assert_eq!(
                event,
                json!({
                    "type":"error", "status":400, "stream_id":"lane",
                    "error":{"code":expected,"message":"upstream rejected request","type":"invalid_request_error"}
                })
            );
        }
    }
}
