use std::{io, pin::Pin};

use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, header},
    response::Response,
};
use futures_util::{Stream, StreamExt};
use http_body_util::BodyExt;
use serde_json::Value;
use tokio::io::AsyncRead;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::{
    compression::decode_reader,
    error::{Error, Result},
};

pub(crate) async fn restore(response: reqwest::Response) -> Result<Response> {
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    if !response.status().is_success() {
        return Ok(crate::router::stream_response(response));
    }
    if content_type.starts_with("text/event-stream") {
        return stream_sse(response);
    }
    if content_type.split(';').next().unwrap_or("").trim() != "application/json" {
        return Ok(crate::router::stream_response(response));
    }
    let response: axum::http::Response<reqwest::Body> = response.into();
    let (parts, body) = response.into_parts();
    let collected = body.collect().await.map_err(Error::bad_gateway)?;
    let trailers = collected.trailers().cloned();
    let original = collected.to_bytes();
    restore_json(parts.status, parts.headers, original, trailers).await
}

async fn restore_json(
    status: axum::http::StatusCode,
    headers: HeaderMap,
    original: Bytes,
    trailers: Option<HeaderMap>,
) -> Result<Response> {
    let decoded =
        decode_reader(io::Cursor::new(original.as_ref()), &headers).map_err(Error::bad_gateway)?;
    let mut decoded = decoded;
    let mut bytes = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut decoded, &mut bytes)
        .await
        .map_err(Error::bad_gateway)?;
    drop(decoded);
    if super::multiagent::response_may_need_adaptation(&bytes)
        && let Ok(mut value) = serde_json::from_slice::<Value>(&bytes)
        && super::multiagent::restore_response(&mut value)
    {
        let output = serde_json::to_vec(&value).map_err(Error::bad_gateway)?;
        return Ok(response_from_parts(
            status,
            headers,
            Body::from(output),
            true,
        ));
    }
    Ok(response_from_parts(
        status,
        headers,
        Body::new(crate::body::buffered_body(original, trailers)),
        false,
    ))
}

fn response_from_parts(
    status: axum::http::StatusCode,
    mut headers: HeaderMap,
    body: Body,
    rewritten: bool,
) -> Response {
    if rewritten {
        clean_rewritten_headers(&mut headers);
    } else {
        crate::config::sanitize_headers(&mut headers);
    }
    let mut response = Response::new(body);
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

fn clean_rewritten_headers(headers: &mut HeaderMap) {
    for name in [
        "content-encoding",
        "content-length",
        "content-md5",
        "digest",
        "content-digest",
        "repr-digest",
        "trailer",
    ] {
        headers.remove(name);
    }
    crate::config::sanitize_headers(headers);
}

fn stream_sse(response: reqwest::Response) -> Result<Response> {
    let status = response.status();
    let mut headers = response.headers().clone();
    let reader = response_reader(response);
    let reader = decode_reader(reader, &headers).map_err(Error::bad_gateway)?;
    clean_rewritten_headers(&mut headers);
    let stream = transform_sse(ReaderStream::new(reader));
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    Ok(response)
}

fn response_reader(response: reqwest::Response) -> impl AsyncRead + Send + Unpin + 'static {
    let chunks = futures_util::stream::unfold(response, |mut response| async move {
        match response.chunk().await {
            Ok(Some(bytes)) => Some((Ok::<Bytes, io::Error>(bytes), response)),
            Ok(None) => None,
            Err(error) => Some((Err(io::Error::other(error)), response)),
        }
    });
    StreamReader::new(Box::pin(chunks))
}

fn transform_sse<S>(input: S) -> impl Stream<Item = std::result::Result<Bytes, io::Error>> + Send
where
    S: Stream<Item = std::result::Result<Bytes, io::Error>> + Send + 'static,
{
    futures_util::stream::unfold(
        (
            Box::pin(input)
                as Pin<Box<dyn Stream<Item = std::result::Result<Bytes, io::Error>> + Send>>,
            Vec::<u8>::new(),
            false,
        ),
        |(mut input, mut pending, mut ended)| async move {
            loop {
                if let Some((index, width)) = event_boundary(&pending) {
                    let event: Vec<u8> = pending.drain(..index + width).collect();
                    return Some((
                        Ok(Bytes::from(transform_event(&event))),
                        (input, pending, ended),
                    ));
                }
                if ended {
                    if pending.is_empty() {
                        return None;
                    }
                    return Some((
                        Ok(Bytes::from(transform_event(&pending))),
                        (input, Vec::new(), true),
                    ));
                }
                match input.next().await {
                    Some(Ok(chunk)) => pending.extend_from_slice(&chunk),
                    Some(Err(error)) => return Some((Err(error), (input, pending, true))),
                    None => ended = true,
                }
            }
        },
    )
}

fn event_boundary(bytes: &[u8]) -> Option<(usize, usize)> {
    for i in 0..bytes.len() {
        if bytes[i] == b'\n' && i > 0 && bytes[i - 1] == b'\n' {
            return Some((i - 1, 2));
        }
        if bytes[i] == b'\n' && i > 1 && bytes[i - 1] == b'\r' && bytes[i - 2] == b'\n' {
            return Some((i - 2, 3));
        }
    }
    None
}

fn transform_event(event: &[u8]) -> Vec<u8> {
    if !super::multiagent::response_may_need_adaptation(event) {
        return event.to_vec();
    }
    let text = match std::str::from_utf8(event) {
        Ok(text) => text,
        Err(_) => return event.to_vec(),
    };
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    let data: Vec<&str> = lines
        .iter()
        .filter_map(|line| {
            line.trim_end_matches(['\r', '\n'])
                .strip_prefix("data:")
                .map(|v| v.strip_prefix(' ').unwrap_or(v))
        })
        .collect();
    if data.is_empty() {
        return event.to_vec();
    }
    let mut value: Value = match serde_json::from_str(&data.join("\n")) {
        Ok(v) => v,
        Err(_) => return event.to_vec(),
    };
    if !super::multiagent::restore_response(&mut value) {
        return event.to_vec();
    }
    let rewritten = match serde_json::to_string(&value) {
        Ok(v) => v,
        Err(_) => return event.to_vec(),
    };
    let mut out = Vec::new();
    let mut replaced = false;
    for line in lines {
        let ending = if line.ends_with("\r\n") {
            "\r\n"
        } else if line.ends_with('\n') {
            "\n"
        } else {
            ""
        };
        if line.trim_end_matches(['\r', '\n']).starts_with("data:") {
            if !replaced {
                out.extend_from_slice(format!("data: {rewritten}{ending}").as_bytes());
                replaced = true;
            }
        } else {
            out.extend_from_slice(line.as_bytes());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_compression::tokio::bufread::GzipEncoder;
    use tokio::io::{AsyncReadExt, BufReader};

    #[test]
    fn prefilter_preserves_deltas_and_restores_escaped_namespaces() {
        let ordinary = b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n";
        assert_eq!(transform_event(ordinary), ordinary);
        let escaped = br#"data: {"type":"response.output_item.done","item":{"type":"function_call","namespace":"collab\u006fration_compat","name":"spawn_agent","arguments":"unchanged"}}

"#;
        let output = transform_event(escaped);
        let document: Value =
            serde_json::from_slice(output.strip_prefix(b"data: ").unwrap()).unwrap();
        assert_eq!(document["item"]["namespace"], "collaboration");
        assert_eq!(document["item"]["arguments"], "unchanged");
    }

    #[tokio::test]
    async fn completed_event_rewrites_after_arbitrary_chunk_splits() {
        let raw = b"event: response.completed\r\ndata: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"function_call\",\"namespace\":\"collaboration_compat\",\"name\":\"collaboration_compat.spawn_agent\"}]}}\r\n\r\n";
        let chunks = raw
            .iter()
            .map(|byte| Ok::<_, io::Error>(Bytes::copy_from_slice(&[*byte])));
        let output = transform_sse(futures_util::stream::iter(chunks))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|result| result.unwrap())
            .collect::<Vec<_>>();
        let output = output.concat();
        assert!(
            output
                .windows(b"\"namespace\":\"collaboration\"".len())
                .any(|w| w == b"\"namespace\":\"collaboration\"")
        );
        assert!(
            output
                .windows(b"\"name\":\"spawn_agent\"".len())
                .any(|w| w == b"\"name\":\"spawn_agent\"")
        );
        assert!(output.ends_with(b"\r\n\r\n"));
    }

    async fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzipEncoder::new(BufReader::new(io::Cursor::new(bytes)));
        let mut output = Vec::new();
        encoder.read_to_end(&mut output).await.unwrap();
        output
    }

    #[tokio::test]
    async fn unchanged_and_invalid_gzip_json_forward_original_bytes_and_trailers() {
        for original in [br#"{"ok":true}"#.as_slice(), b"not json"] {
            let encoded = gzip(original).await;
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_ENCODING, "gzip".parse().unwrap());
            let mut trailers = HeaderMap::new();
            trailers.insert("x-checksum", "kept".parse().unwrap());
            let response = restore_json(
                axum::http::StatusCode::OK,
                headers,
                Bytes::from(encoded.clone()),
                Some(trailers),
            )
            .await
            .unwrap();
            assert_eq!(response.headers()[header::CONTENT_ENCODING], "gzip");
            let collected = response.into_body().collect().await.unwrap();
            assert_eq!(collected.trailers().unwrap()["x-checksum"], "kept");
            assert_eq!(collected.to_bytes().as_ref(), encoded);
        }
    }

    #[tokio::test]
    async fn item_events_without_event_fields_restore_names_and_keep_arguments() {
        let raw = concat!(
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"namespace\":\"collaboration_compat\",\"name\":\"collaboration_compat.spawn_agent\",\"arguments\":\"{\\\"opaque\\\":true}\"}}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"namespace\":\"collaboration_compat\",\"name\":\"collaboration_compat.send_message\",\"arguments\":\"literal payload\"}}\n\n"
        );
        let output = transform_sse(futures_util::stream::iter(
            raw.as_bytes()
                .chunks(11)
                .map(|chunk| Ok::<_, io::Error>(Bytes::copy_from_slice(chunk))),
        ))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|result| result.unwrap())
        .collect::<Vec<_>>()
        .concat();
        assert!(
            output
                .windows(b"\"namespace\":\"collaboration\"".len())
                .any(|w| w == b"\"namespace\":\"collaboration\"")
        );
        assert!(
            output
                .windows(b"\"name\":\"spawn_agent\"".len())
                .any(|w| w == b"\"name\":\"spawn_agent\"")
        );
        assert!(
            output
                .windows(b"\"name\":\"send_message\"".len())
                .any(|w| w == b"\"name\":\"send_message\"")
        );
        assert!(
            output
                .windows(b"literal payload".len())
                .any(|w| w == b"literal payload")
        );
        assert!(output.windows(b"opaque".len()).any(|w| w == b"opaque"));
    }

    #[tokio::test]
    async fn opaque_events_are_preserved_and_emitted_before_stream_completion() {
        let first = Bytes::from_static(
            ": keepalive\r\n\r\nevent: response.delta\ndata: {\"delta\":\"雪\"}\n\n".as_bytes(),
        );
        let second = futures_util::stream::pending::<std::result::Result<Bytes, io::Error>>();
        let output =
            transform_sse(futures_util::stream::once(async move { Ok(first) }).chain(second));
        tokio::pin!(output);
        let emitted = tokio::time::timeout(std::time::Duration::from_millis(100), output.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(emitted.as_ref(), b": keepalive\r\n\r\n");
        let emitted = output.next().await.unwrap().unwrap();
        assert_eq!(
            emitted.as_ref(),
            "event: response.delta\ndata: {\"delta\":\"雪\"}\n\n".as_bytes()
        );
    }

    #[test]
    fn ordinary_json_events_and_multiline_data_are_left_untouched() {
        for event in [
            b"event: response.delta\ndata: {\"type\":\"response.delta\"}\n\n".as_slice(),
            b"event: response.completed\ndata: {\"type\":\ndata: \"response.completed\"}\n\n",
        ] {
            assert_eq!(transform_event(event), event);
        }
    }
}
