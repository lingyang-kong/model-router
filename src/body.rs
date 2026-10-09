use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::compression::decode_body;
use crate::error::{Error, Result as RouterResult};
use axum::{
    body::{Body, Bytes},
    http::HeaderMap,
};
use http_body::{Body as HttpBody, Frame, SizeHint};
use http_body_util::{BodyExt, StreamBody};
use serde::Deserialize;
use sync_wrapper::SyncWrapper;

#[derive(Deserialize)]
struct ModelSelector {
    model: Option<String>,
}

pub(crate) fn request_model(bytes: &[u8]) -> RouterResult<Option<String>> {
    let selector: ModelSelector =
        serde_json::from_slice(bytes).map_err(|error| Error::bad_request(error.to_string()))?;
    if selector.model.as_deref() == Some("") {
        return Err(Error::bad_request("The model must be a nonempty string"));
    }
    Ok(selector.model)
}

pub(crate) async fn inspect_body(
    body: Body,
    headers: &HeaderMap,
) -> RouterResult<(Option<String>, reqwest::Body)> {
    let collected = body
        .collect()
        .await
        .map_err(|error| Error::bad_request(error.to_string()))?;
    let trailers = collected.trailers().cloned();
    let bytes = collected.to_bytes();
    let model = if headers.contains_key("content-encoding") {
        let decoded = decode_body(std::io::Cursor::new(&bytes), headers)
            .await
            .map_err(|error| Error::bad_request(error.to_string()))?;
        request_model(&decoded)?
    } else {
        request_model(&bytes)?
    };
    let body = if let Some(trailers) = trailers {
        // An exact size hint makes Hyper send Content-Length and omit the
        // trailing fields on HTTP/1.1 connections.
        let frames = [
            Ok::<_, std::convert::Infallible>(Frame::data(bytes)),
            Ok(Frame::trailers(trailers)),
        ];
        reqwest::Body::wrap(StreamBody::new(futures_util::stream::iter(frames)))
    } else {
        reqwest::Body::from(bytes)
    };
    Ok((model, body))
}

/// Adapts Axum's body to reqwest without inspecting or retaining frames.
struct RequestBody {
    body: SyncWrapper<Body>,
    remaining: Option<u64>,
    ended: bool,
}

impl HttpBody for RequestBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        let body = this.body.get_mut();
        let frame = Pin::new(&mut *body).poll_frame(cx);
        this.remaining = body.size_hint().exact();
        this.ended = matches!(&frame, Poll::Ready(None)) || body.is_end_stream();
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.ended
    }
    fn size_hint(&self) -> SizeHint {
        self.remaining.map(SizeHint::with_exact).unwrap_or_default()
    }
}

pub(crate) fn stream_body(body: Body) -> reqwest::Body {
    let remaining = body.size_hint().exact();
    let ended = body.is_end_stream();
    reqwest::Body::wrap(RequestBody {
        body: SyncWrapper::new(body),
        remaining,
        ended,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[tokio::test]
    async fn original_frames_lengths_and_trailers_are_forwarded() {
        let bytes = Bytes::from_static(b"opaque bytes");
        let pointer = bytes.as_ptr();
        let mut trailers = axum::http::HeaderMap::new();
        trailers.insert("x-checksum", "preserved".parse().unwrap());
        let frames = vec![
            Ok::<_, std::io::Error>(Frame::data(bytes)),
            Ok(Frame::trailers(trailers)),
        ];
        let body = Body::new(http_body_util::StreamBody::new(futures_util::stream::iter(
            frames,
        )));
        let mut forwarded = stream_body(body);
        assert_eq!(
            forwarded
                .frame()
                .await
                .unwrap()
                .unwrap()
                .into_data()
                .unwrap()
                .as_ptr(),
            pointer
        );
        assert_eq!(
            forwarded
                .frame()
                .await
                .unwrap()
                .unwrap()
                .into_trailers()
                .unwrap()["x-checksum"],
            "preserved"
        );
        assert!(forwarded.frame().await.is_none());
        assert!(forwarded.is_end_stream());

        let mut known = stream_body(Body::from("not JSON"));
        assert_eq!(known.size_hint().exact(), Some(8));
        known.frame().await.unwrap().unwrap();
        assert_eq!(known.size_hint().exact(), Some(0));
    }

    #[tokio::test]
    async fn buffered_selection_preserves_original_bytes_and_trailers() {
        let first =
            Bytes::from_static(br#"{ "nested":{"model":"ignored"},"mo\u0064el":"vendor/\u96ea", "#);
        let second = Bytes::from_static(b"\"input\": \"unchanged\" }\n");
        let expected = [first.as_ref(), second.as_ref()].concat();
        let mut trailers = axum::http::HeaderMap::new();
        trailers.insert("x-checksum", "preserved".parse().unwrap());
        let frames = vec![
            Ok::<_, std::io::Error>(Frame::data(first)),
            Ok(Frame::data(second)),
            Ok(Frame::trailers(trailers)),
        ];
        let body = Body::new(http_body_util::StreamBody::new(futures_util::stream::iter(
            frames,
        )));
        let (model, mut forwarded) = inspect_body(body, &HeaderMap::new())
            .await
            .unwrap_or_else(|_| panic!("invalid buffered request"));
        assert_eq!(model.as_deref(), Some("vendor/雪"));
        assert_eq!(forwarded.size_hint().exact(), None);
        assert_eq!(
            forwarded
                .frame()
                .await
                .unwrap()
                .unwrap()
                .into_data()
                .unwrap()
                .as_ref(),
            expected.as_slice()
        );
        assert_eq!(
            forwarded
                .frame()
                .await
                .unwrap()
                .unwrap()
                .into_trailers()
                .unwrap()["x-checksum"],
            "preserved"
        );
    }

    #[test]
    fn model_selector_ignores_deep_payloads_and_rejects_invalid_routing_values() {
        let deep = format!(
            "{{\"input\":{}0{},\"model\":\"native\"}}",
            "[".repeat(256),
            "]".repeat(256)
        );
        assert_eq!(
            request_model(deep.as_bytes())
                .unwrap_or_else(|_| panic!("ignored payload depth became a routing limit")),
            Some("native".into())
        );
        for input in [
            br#"{"model":7}"#.as_slice(),
            br#"{"model":""}"#,
            br#"{"model":"one","model":"two"}"#,
            br#"{"model":"unfinished"#,
        ] {
            assert!(request_model(input).is_err());
        }
    }
}
