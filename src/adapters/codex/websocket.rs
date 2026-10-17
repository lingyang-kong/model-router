//! Route model changes independently of the client connection's initial handshake.
use std::{collections::BTreeMap, sync::Arc};

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    response::IntoResponse,
};
use futures_util::{FutureExt, SinkExt, StreamExt, stream::BoxStream};
use serde::Deserialize;
use serde_json::{Value, value::RawValue};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        protocol::{Role, WebSocketConfig},
    },
};

use super::{compaction_transport, response, transport};
use crate::{
    adapters::websocket::{WebSocketAction, WebSocketContext},
    error::Error,
};

#[derive(Deserialize)]
struct Selector {
    model: Option<String>,
    previous_response_id: Option<String>,
    generate: Option<bool>,
    stream_id: Option<Value>,
}

pub(in crate::adapters) async fn route(
    message: Message,
    context: &WebSocketContext,
) -> WebSocketAction {
    let Message::Text(text) = &message else {
        return WebSocketAction::Forward(message);
    };
    let Ok(selector) = serde_json::from_str::<Selector>(text) else {
        return WebSocketAction::Forward(message);
    };
    let model = selector.model.as_deref().or(context.model.as_deref());
    let provider = match context
        .state
        .resolve_provider(model, context.origin.as_ref())
        .await
    {
        Ok(provider) => provider,
        Err(error) => return failure(error, selector.stream_id),
    };
    if let Err(error) = provider.validate_credential_scope(context.origin.as_ref()) {
        return failure(error, selector.stream_id);
    }
    if context.connected && Arc::ptr_eq(&provider, &context.provider) {
        return transport::websocket_request(message, context).await;
    }
    if selector.previous_response_id.is_some() {
        let mut event = serde_json::json!({"type":"error","status":400,"error":{
            "code":"previous_response_not_found", "message":"Provider connection changed; retry with full input"
        }});
        if let Some(stream_id) = selector.stream_id {
            event["stream_id"] = stream_id;
        }
        return WebSocketAction::Respond(
            async move { vec![Message::Text(event.to_string().into())] }.boxed(),
        );
    }
    let http_only = context.http_only && Arc::ptr_eq(&provider, &context.provider);
    let mut routed = context.clone();
    routed.provider = provider;
    routed.model = model.map(str::to_owned);
    if let Some(model) = &routed.model
        && let Ok(hint) = format!("model={model}").parse()
    {
        routed.headers.insert("x-codex-routing-hint", hint);
    }
    let stream_id = selector.stream_id.clone();
    WebSocketAction::Stream(
        futures_util::stream::once(async move {
            match bridge(message, selector, routed, http_only).await {
                Ok(stream) => stream,
                Err(error) => futures_util::stream::once(async move {
                    Message::Text(
                        compaction_transport::failure_event(
                            error.into_response(),
                            stream_id.as_ref(),
                        )
                        .await
                        .to_string()
                        .into(),
                    )
                })
                .boxed(),
            }
        })
        .flatten()
        .boxed(),
    )
}

fn failure(error: Error, stream_id: Option<Value>) -> WebSocketAction {
    WebSocketAction::Respond(
        async move {
            vec![Message::Text(
                compaction_transport::failure_event(error.into_response(), stream_id.as_ref())
                    .await
                    .to_string()
                    .into(),
            )]
        }
        .boxed(),
    )
}

async fn bridge(
    message: Message,
    selector: Selector,
    context: WebSocketContext,
    http_only: bool,
) -> crate::error::Result<BoxStream<'static, Message>> {
    let mut request = Request::builder()
        .method(Method::GET)
        .uri(context.uri.clone())
        .body(())
        .map_err(Error::bad_request)?;
    *request.headers_mut() = context.headers.clone();
    let (mut parts, _) = request.into_parts();
    if !http_only {
        parts.headers.insert(
            "sec-websocket-key",
            tokio_tungstenite::tungstenite::handshake::client::generate_key()
                .parse()
                .unwrap(),
        );
        let reply = crate::websocket::connect(
            &context.state,
            &context.provider,
            context.origin.as_ref(),
            &parts,
            reqwest::Body::from(Vec::new()),
            true,
        )
        .await?;
        if reply.status() == StatusCode::SWITCHING_PROTOCOLS {
            let connection = reply.upgrade().await.map_err(Error::bad_gateway)?;
            let config = WebSocketConfig::default()
                .max_message_size(None)
                .max_frame_size(None);
            let mut socket =
                WebSocketStream::from_raw_socket(connection, Role::Client, Some(config)).await;
            match transport::websocket_request(message, &context).await {
                WebSocketAction::Forward(message) => {
                    socket.send(message).await.map_err(Error::bad_gateway)?
                }
                WebSocketAction::Respond(job) => {
                    return Ok(futures_util::stream::once(job)
                        .flat_map(futures_util::stream::iter)
                        .boxed());
                }
                WebSocketAction::Stream(stream) => return Ok(stream),
            }
            return Ok(socket_events(socket, selector.stream_id));
        }
        if !crate::websocket::unsupported(reply.status()) {
            return Ok(response::websocket_response(
                crate::router::stream_response(reply),
                selector.stream_id,
            ));
        }
    }
    if selector.generate == Some(false) {
        let reply = serde_json::json!({"id":"resp_router_prewarm","object":"response","status":"completed","output":[]});
        return Ok(futures_util::stream::iter(
            super::compaction::events(&reply, selector.stream_id.as_ref())
                .into_iter()
                .map(|event| Message::Text(event.to_string().into())),
        )
        .boxed());
    }
    let Message::Text(text) = message else {
        unreachable!()
    };
    let mut fields: BTreeMap<String, Box<RawValue>> =
        serde_json::from_str(&text).map_err(Error::bad_request)?;
    for name in ["type", "stream_id", "event_id", "generate"] {
        fields.remove(name);
    }
    fields.insert(
        "stream".into(),
        serde_json::value::to_raw_value(&true).unwrap(),
    );
    parts.method = Method::POST;
    response::clean_rewritten_headers(&mut parts.headers);
    for name in [
        "sec-websocket-key",
        "sec-websocket-version",
        "sec-websocket-protocol",
        "sec-websocket-extensions",
    ] {
        parts.headers.remove(name);
    }
    parts
        .headers
        .insert("content-type", "application/json".parse().unwrap());
    let body = Body::from(serde_json::to_vec(&fields).map_err(Error::bad_request)?);
    let response = transport::forward_to(
        &context.state,
        parts,
        body,
        context.origin.as_ref(),
        Some(Arc::clone(&context.provider)),
    )
    .await?;
    Ok(response::websocket_response(response, selector.stream_id))
}

pub(super) fn terminal(message: &Message) -> bool {
    #[derive(Deserialize)]
    struct Kind {
        #[serde(rename = "type")]
        kind: String,
    }
    matches!(message, Message::Text(text) if serde_json::from_str::<Kind>(text).is_ok_and(|event| matches!(event.kind.as_str(), "response.completed" | "response.failed" | "response.incomplete" | "error")))
}

/// Forward one side WebSocket session, correlating premature-close errors with its request.
fn socket_events<S>(
    socket: WebSocketStream<S>,
    stream_id: Option<Value>,
) -> BoxStream<'static, Message>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    futures_util::stream::unfold(Some(socket), move |socket| {
        let stream_id = stream_id.clone();
        async move {
            let mut socket = socket?;
            loop {
                match socket.next().await {
                    Some(Ok(Message::Ping(_))) => {
                        let _ = socket.flush().await;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        return Some((
                            error_message(
                                "Upstream WebSocket closed before response completion",
                                stream_id.as_ref(),
                            ),
                            None,
                        ));
                    }
                    Some(Err(error)) => {
                        return Some((error_message(error, stream_id.as_ref()), None));
                    }
                    Some(Ok(message)) => {
                        let terminal = terminal(&message);
                        let message = transport::adapt_websocket_response(message);
                        return Some((message, if terminal { None } else { Some(socket) }));
                    }
                }
            }
        }
    })
    .boxed()
}

pub(super) fn error_message(error: impl std::fmt::Display, stream_id: Option<&Value>) -> Message {
    let mut event =
        serde_json::json!({"type":"error","status":502,"error":{"message":error.to_string()}});
    if let Some(stream_id) = stream_id {
        event["stream_id"] = stream_id.clone();
    }
    Message::Text(event.to_string().into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn premature_socket_termination_preserves_stream_id() {
        for clean in [true, false] {
            let (client, server) = tokio::io::duplex(1024);
            let client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
            let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
            if clean {
                server.send(Message::Close(None)).await.unwrap();
            }
            drop(server);
            let events: Vec<_> = socket_events(client, Some(serde_json::json!("lane")))
                .collect()
                .await;
            assert_eq!(events.len(), 1);
            let error: Value = serde_json::from_slice(&events[0].clone().into_data()).unwrap();
            assert_eq!(error["type"], "error");
            assert_eq!(error["stream_id"], "lane");
        }
    }
}
