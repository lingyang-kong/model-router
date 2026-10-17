use std::{collections::VecDeque, io, pin::Pin};

use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, header},
    response::Response,
};
use futures_util::{Stream, StreamExt, stream::BoxStream};
use http_body_util::BodyExt;
use serde_json::{Value, value::RawValue};
use tokio::io::AsyncRead;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::{
    compression::decode_reader,
    error::{Error, Result},
};

pub(crate) async fn restore(response: reqwest::Response) -> Result<Response> {
    let content_type = content_type(response.headers());
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

/// HTTP content types are case-insensitive; compare the decoded value in lowercase.
fn content_type(headers: &HeaderMap) -> String {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase()
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
    if let Ok(Some(output)) = super::multiagent::restore_bytes(&bytes) {
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

pub(super) fn clean_rewritten_headers(headers: &mut HeaderMap) {
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
    frame_sse(input).map(|result| result.map(|event| Bytes::from(transform_event(&event))))
}

fn frame_sse<S>(input: S) -> impl Stream<Item = std::result::Result<Bytes, io::Error>> + Send
where
    S: Stream<Item = std::result::Result<Bytes, io::Error>> + Send + 'static,
{
    futures_util::stream::unfold(
        (
            Box::pin(input)
                as Pin<Box<dyn Stream<Item = std::result::Result<Bytes, io::Error>> + Send>>,
            SseParser::default(),
            VecDeque::<Vec<u8>>::new(),
            false,
        ),
        |(mut input, mut parser, mut ready, mut ended)| async move {
            loop {
                if let Some(output) = ready.pop_front() {
                    return Some((Ok(Bytes::from(output)), (input, parser, ready, ended)));
                }
                if ended {
                    parser.finish(&mut ready);
                    return ready
                        .pop_front()
                        .map(|output| (Ok(Bytes::from(output)), (input, parser, ready, true)));
                }
                match input.next().await {
                    Some(Ok(chunk)) => parser.push(&chunk, &mut ready),
                    Some(Err(error)) => return Some((Err(error), (input, parser, ready, true))),
                    None => ended = true,
                }
            }
        },
    )
}

/// Convert the existing HTTP adapter's output to WebSocket events without retaining a stream.
pub(super) fn websocket_response(
    response: Response,
    stream_id: Option<Value>,
) -> BoxStream<'static, Message> {
    futures_util::stream::once(async move {
        if !response.status().is_success() {
            return futures_util::stream::once(async move {
                Message::Text(
                    super::compaction_transport::failure_event(response, stream_id.as_ref())
                        .await
                        .to_string()
                        .into(),
                )
            })
            .boxed();
        }
        let sse = content_type(response.headers()).starts_with("text/event-stream");
        let (parts, body) = response.into_parts();
        let reader = StreamReader::new(
            body.into_data_stream()
                .map(|result| result.map_err(io::Error::other)),
        );
        let reader = match decode_reader(reader, &parts.headers) {
            Ok(reader) => reader,
            Err(error) => {
                return futures_util::stream::iter([super::websocket::error_message(
                    error,
                    stream_id.as_ref(),
                )])
                .boxed();
            }
        };
        if sse {
            let end_stream_id = stream_id.clone();
            let events = frame_sse(ReaderStream::new(reader))
                .filter_map(move |event| {
                    let message = match event {
                        Err(error) => {
                            Some(super::websocket::error_message(error, stream_id.as_ref()))
                        }
                        Ok(event) => {
                            let text = std::str::from_utf8(&event).unwrap_or("");
                            let data: Vec<_> = text
                                .lines()
                                .filter_map(|line| {
                                    line.strip_prefix("data:")
                                        .map(|line| line.strip_prefix(' ').unwrap_or(line))
                                })
                                .collect();
                            if data.is_empty() || data == ["[DONE]"] {
                                None
                            } else {
                                Some(with_stream_id(data.join("\n"), stream_id.as_ref()))
                            }
                        }
                    };
                    futures_util::future::ready(message)
                })
                .boxed();
            return completed_stream(events, end_stream_id);
        }
        let mut reader = reader;
        let mut bytes = Vec::new();
        if let Err(error) = tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut bytes).await {
            return futures_util::stream::iter([super::websocket::error_message(
                error,
                stream_id.as_ref(),
            )])
            .boxed();
        }
        let events = json_websocket_events(&bytes, stream_id.as_ref()).unwrap_or_else(|error| {
            vec![super::websocket::error_message(error, stream_id.as_ref())]
        });
        futures_util::stream::iter(events).boxed()
    })
    .flatten()
    .boxed()
}

fn completed_stream(
    events: BoxStream<'static, Message>,
    stream_id: Option<Value>,
) -> BoxStream<'static, Message> {
    futures_util::stream::unfold(
        (events, false, stream_id),
        |(mut events, done, stream_id)| async move {
            if done {
                return None;
            }
            let message = match events.next().await {
                Some(message) => message,
                None => super::websocket::error_message(
                    "Upstream response ended before completion",
                    stream_id.as_ref(),
                ),
            };
            let done = super::websocket::terminal(&message);
            Some((message, (events, done, stream_id)))
        },
    )
    .boxed()
}

fn with_stream_id(text: String, stream_id: Option<&Value>) -> Message {
    if let Some(stream_id) = stream_id
        && let Ok(mut event) =
            serde_json::from_str::<std::collections::BTreeMap<String, Box<RawValue>>>(&text)
    {
        event.insert(
            "stream_id".into(),
            serde_json::value::to_raw_value(stream_id).unwrap(),
        );
        return Message::Text(serde_json::to_string(&event).unwrap().into());
    }
    Message::Text(text.into())
}

fn json_websocket_events(
    bytes: &[u8],
    stream_id: Option<&Value>,
) -> serde_json::Result<Vec<Message>> {
    type Fields = std::collections::BTreeMap<String, Box<RawValue>>;
    let response: &RawValue = serde_json::from_slice(bytes)?;
    let mut created: Fields = serde_json::from_slice(bytes)?;
    let terminal = match created
        .get("status")
        .and_then(|status| serde_json::from_str::<String>(status.get()).ok())
        .as_deref()
    {
        Some("failed") => "response.failed",
        Some("incomplete") => "response.incomplete",
        _ => "response.completed",
    };
    let output: Vec<&RawValue> = match created.get("output") {
        Some(output) => serde_json::from_str(output.get())?,
        None => Vec::new(),
    };
    #[derive(serde::Serialize)]
    struct Item<'a> {
        #[serde(rename = "type")]
        kind: &'static str,
        output_index: usize,
        item: &'a RawValue,
    }
    let mut events = Vec::new();
    for (output_index, item) in output.into_iter().enumerate() {
        for kind in ["response.output_item.added", "response.output_item.done"] {
            events.push(with_stream_id(
                serde_json::to_string(&Item {
                    kind,
                    output_index,
                    item,
                })?,
                stream_id,
            ));
        }
    }
    created.insert(
        "status".into(),
        serde_json::value::to_raw_value("in_progress")?,
    );
    created.insert(
        "output".into(),
        serde_json::value::to_raw_value(&Vec::<Value>::new())?,
    );
    #[derive(serde::Serialize)]
    struct Envelope<'a, T> {
        #[serde(rename = "type")]
        kind: &'a str,
        response: T,
    }
    events.insert(
        0,
        with_stream_id(
            serde_json::to_string(&Envelope {
                kind: "response.created",
                response: created,
            })?,
            stream_id,
        ),
    );
    events.push(with_stream_id(
        serde_json::to_string(&Envelope {
            kind: terminal,
            response,
        })?,
        stream_id,
    ));
    Ok(events)
}

#[derive(Default)]
struct SseParser {
    line: Vec<u8>,
    event: Vec<u8>,
    after_cr: bool,
    #[cfg(test)]
    scanned: usize,
}

impl SseParser {
    fn push(&mut self, bytes: &[u8], ready: &mut VecDeque<Vec<u8>>) {
        for &byte in bytes {
            #[cfg(test)]
            {
                self.scanned += 1;
            }
            if std::mem::replace(&mut self.after_cr, false) && byte == b'\n' {
                continue;
            }
            match byte {
                b'\r' | b'\n' => {
                    self.after_cr = byte == b'\r';
                    self.complete_line(ready);
                }
                _ => self.line.push(byte),
            }
        }
    }

    fn complete_line(&mut self, ready: &mut VecDeque<Vec<u8>>) {
        let blank = self.line.is_empty();
        // Normalize logical terminators before moving comments. Otherwise a CR
        // field ending and LF blank line can merge into CRLF across a removed comment.
        self.line.push(b'\n');
        if self.line.first() == Some(&b':') {
            // Comments carry no event data; send keepalives before buffered data.
            ready.push_back(std::mem::take(&mut self.line));
            return;
        }
        if blank && self.event.is_empty() {
            if let Some(last) = ready.back_mut() {
                last.append(&mut self.line);
            } else {
                ready.push_back(std::mem::take(&mut self.line));
            }
            return;
        }
        self.event.append(&mut self.line);
        if blank {
            ready.push_back(std::mem::take(&mut self.event));
        }
    }

    fn finish(&mut self, ready: &mut VecDeque<Vec<u8>>) {
        if !self.line.is_empty() {
            if self.line.first() == Some(&b':') {
                ready.push_back(std::mem::take(&mut self.line));
            } else {
                self.event.append(&mut self.line);
            }
        }
        if !self.event.is_empty() {
            ready.push_back(std::mem::take(&mut self.event));
        }
    }
}

/// Event field lines have already been normalized to LF by the streaming parser.
fn transform_event(event: &[u8]) -> Vec<u8> {
    if !super::multiagent::response_may_need_adaptation(event) {
        return event.to_vec();
    }
    let text = match std::str::from_utf8(event) {
        Ok(text) => text,
        Err(_) => return event.to_vec(),
    };
    let lines: Vec<_> = text.split_inclusive('\n').collect();
    let data: Vec<&str> = lines
        .iter()
        .filter_map(|line| {
            line.trim_end_matches('\n')
                .strip_prefix("data:")
                .map(|v| v.strip_prefix(' ').unwrap_or(v))
        })
        .collect();
    if data.is_empty() {
        return event.to_vec();
    }
    let joined = data.join("\n");
    let Ok(Some(mut rewritten)) = super::multiagent::restore_bytes(joined.as_bytes()) else {
        return event.to_vec();
    };
    // Valid JSON cannot contain literal CR/LF inside strings. RawValue may keep
    // them as formatting whitespace; escaped newlines and all string bytes survive.
    rewritten.retain(|byte| !matches!(byte, b'\r' | b'\n'));
    let mut out = Vec::new();
    let mut replaced = false;
    for line in lines {
        let ending = if line.ends_with('\n') { "\n" } else { "" };
        if line.starts_with("data:") {
            if !replaced {
                out.extend_from_slice(b"data: ");
                out.extend_from_slice(&rewritten);
                out.extend_from_slice(ending.as_bytes());
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
    #[tokio::test]
    async fn bridge_errors_preserve_stream_id() {
        use axum::body::Body;
        use futures_util::StreamExt;
        use serde_json::{Value, json};

        for (content_type, encoding, body) in [
            ("application/json", "identity", "invalid json"),
            ("text/event-stream", "unsupported", ""),
            ("text/event-stream", "gzip", "invalid gzip"),
            (
                "text/event-stream",
                "identity",
                "data: {\"type\":\"response.created\"}\n\n",
            ),
        ] {
            let response = axum::response::Response::builder()
                .header("content-type", content_type)
                .header("content-encoding", encoding)
                .body(Body::from(body))
                .unwrap();
            let events: Vec<_> = super::websocket_response(response, Some(json!("lane")))
                .collect()
                .await;
            let error: Value =
                serde_json::from_slice(&events.last().unwrap().clone().into_data()).unwrap();
            assert_eq!(error["type"], "error", "{content_type}, {encoding}");
            assert_eq!(error["stream_id"], "lane");
        }
    }

    use super::*;
    use async_compression::tokio::bufread::GzipEncoder;
    use serde_json::Value;
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
        assert!(output.ends_with(b"\n\n"));
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
        assert_eq!(emitted.as_ref(), b": keepalive\n\n");
        let emitted = output.next().await.unwrap().unwrap();
        assert_eq!(
            emitted.as_ref(),
            "event: response.delta\ndata: {\"delta\":\"雪\"}\n\n".as_bytes()
        );
    }

    #[tokio::test]
    async fn heartbeat_cr_line_is_emitted_without_waiting_for_lf_or_eof() {
        let first = futures_util::stream::once(async {
            Ok::<_, io::Error>(Bytes::from_static(b":ping\r"))
        });
        let input = first.chain(futures_util::stream::pending());
        let mut output = Box::pin(transform_sse(input));
        let emitted = tokio::time::timeout(std::time::Duration::from_millis(100), output.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(emitted.as_ref(), b":ping\n");
    }

    #[tokio::test]
    async fn cr_lines_split_crlf_and_interspersed_comments_are_handled() {
        let chunks = [
            b"data: one\rdata: two\r: between\r\n\r".as_slice(),
            b"\ndata: three\r\r".as_slice(),
        ];
        let output = transform_sse(futures_util::stream::iter(
            chunks
                .into_iter()
                .map(|chunk| Ok::<_, io::Error>(Bytes::copy_from_slice(chunk))),
        ))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|result| result.unwrap())
        .collect::<Vec<_>>()
        .concat();
        assert_eq!(
            output,
            b": between\ndata: one\ndata: two\n\ndata: three\n\n"
        );
    }

    #[test]
    fn parser_scans_each_input_byte_once_across_tiny_chunks() {
        let input = b"event: response.delta\rdata: {\"delta\":\"hello\"}\r\r";
        let mut parser = SseParser::default();
        let mut ready = VecDeque::new();
        for byte in input {
            parser.push(std::slice::from_ref(byte), &mut ready);
        }
        assert_eq!(parser.scanned, input.len());
        assert_eq!(
            ready.into_iter().flatten().collect::<Vec<_>>(),
            b"event: response.delta\ndata: {\"delta\":\"hello\"}\n\n"
        );
    }

    #[test]
    fn extracting_comments_preserves_event_boundaries_for_every_line_ending() {
        for field in ["\n", "\r", "\r\n"] {
            for comment in ["\n", "\r", "\r\n"] {
                for blank in ["\n", "\r", "\r\n"] {
                    let input = format!(
                        "id: first{field}data: one{field}: heartbeat{comment}{blank}id: second\ndata: two\n\n"
                    );
                    // Adjacent CR and LF are one terminator in the input itself:
                    // this combination has no blank line, so it is not a two-event fixture.
                    if comment == "\r" && blank == "\n" {
                        continue;
                    }
                    for chunk_size in [1, 2, 7, input.len()] {
                        let mut parser = SseParser::default();
                        let mut ready = VecDeque::new();
                        let mut output = Vec::new();
                        for chunk in input.as_bytes().chunks(chunk_size) {
                            parser.push(chunk, &mut ready);
                            output.extend(ready.drain(..).flatten());
                        }
                        // Both events must be dispatched without waiting for EOF.
                        assert_eq!(
                            output,
                            b": heartbeat\nid: first\ndata: one\n\nid: second\ndata: two\n\n",
                            "field={field:?}, comment={comment:?}, blank={blank:?}, chunk={chunk_size}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn rewritten_multiline_json_keeps_string_escapes_and_deep_raw_values() {
        let deep = format!("{}\n0\n{}", "[".repeat(1000), "]".repeat(1000));
        let opaque = r#"{ "text": "a\n\r\u000A \" \\", "utf8": "雪" }"#;
        let json = format!(
            r#"{{"type":"response.output_item.done","item":{{"type":"function_call","namespace":"collaboration_compat","name":"spawn_agent"}},"deep":{deep},"opaque":{opaque}}}"#
        );
        let input = format!("data: {}\n\n", json.replace('\n', "\ndata: "));
        let output = transform_event(input.as_bytes());
        let output = std::str::from_utf8(&output).unwrap();
        assert_eq!(
            output
                .lines()
                .filter(|line| line.starts_with("data:"))
                .count(),
            1
        );
        let payload = output
            .strip_prefix("data: ")
            .unwrap()
            .strip_suffix("\n\n")
            .unwrap();
        assert!(!payload.contains(['\r', '\n']));
        let object: std::collections::BTreeMap<String, Box<serde_json::value::RawValue>> =
            serde_json::from_str(payload).unwrap();
        assert_eq!(object["opaque"].get(), opaque);
        assert_eq!(object["deep"].get(), deep.replace('\n', ""));
        assert!(
            object["item"]
                .get()
                .contains(r#""namespace":"collaboration""#)
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

    #[test]
    fn cr_only_json_events_restore_with_normalized_line_endings() {
        let event = b"data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"namespace\":\"collaboration_compat\",\"name\":\"spawn_agent\"}}\r\r";
        let mut parser = SseParser::default();
        let mut ready = VecDeque::new();
        parser.push(event, &mut ready);
        let output: Vec<_> = ready
            .into_iter()
            .flat_map(|event| transform_event(&event))
            .collect();
        assert!(
            output
                .windows(b"\"namespace\":\"collaboration\"".len())
                .any(|w| w == b"\"namespace\":\"collaboration\"")
        );
        assert!(output.ends_with(b"\n\n"));
    }
}
