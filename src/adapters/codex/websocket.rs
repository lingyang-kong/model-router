//! Select the upstream for each request; the relay owns connection replacement.
use std::sync::Arc;

use axum::{http::StatusCode, response::IntoResponse};
use futures_util::FutureExt;
use serde::Deserialize;
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

use super::{ROUTING_HINT_HEADER, compaction_transport, error_code, event, transport};
use crate::protocol::{field, header};
use crate::{
    adapters::websocket::{WebSocketAction, WebSocketContext, WebSocketSwitch},
    error::Error,
};

#[derive(Deserialize)]
struct Selector {
    #[serde(rename = "type")]
    kind: Option<String>,
    model: Option<String>,
    previous_response_id: Option<String>,
    stream_id: Option<Value>,
}

pub(in crate::adapters) async fn route(
    message: Message,
    context: &WebSocketContext,
) -> WebSocketAction {
    let Message::Text(text) = message else {
        return WebSocketAction::Forward(message);
    };
    let Ok(selector) = serde_json::from_str::<Selector>(&text) else {
        return WebSocketAction::Forward(Message::Text(text));
    };
    // Control and unknown protocol events belong to the current connection.
    if selector
        .kind
        .as_deref()
        .is_some_and(|kind| kind != event::CREATE)
    {
        return WebSocketAction::Forward(Message::Text(text));
    }
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
    if Arc::ptr_eq(&provider, &context.provider) {
        return transport::websocket_request(text, context, model, selector.stream_id).await;
    }
    if selector.previous_response_id.is_some() {
        return previous_response_not_found(
            "Provider connection changed; retry with full input",
            selector.stream_id,
        );
    }
    let mut routed = context.clone();
    routed.provider = provider;
    if let Some(model) = model {
        if let Ok(hint) = format!("model={model}").parse() {
            routed.headers.insert(ROUTING_HINT_HEADER, hint);
        }
        if routed.headers.contains_key(header::X_MODEL)
            && let Ok(hint) = model.parse()
        {
            routed.headers.insert(header::X_MODEL, hint);
        }
    } else {
        routed.headers.remove(ROUTING_HINT_HEADER);
        routed.headers.remove(header::X_MODEL);
    }
    let opening_context = routed.clone();
    WebSocketAction::Switch(Box::new(WebSocketSwitch {
        context: routed,
        request: Message::Text(text),
        opening: open(opening_context, selector.stream_id).boxed(),
    }))
}

pub(super) fn failure(error: Error, stream_id: Option<Value>) -> WebSocketAction {
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

pub(super) fn previous_response_not_found(
    message: &'static str,
    stream_id: Option<Value>,
) -> WebSocketAction {
    let mut event = serde_json::json!({"type":"error","status":StatusCode::BAD_REQUEST.as_u16(),"error":{
        "code":error_code::PREVIOUS_RESPONSE_NOT_FOUND, "message":message
    }});
    if let Some(stream_id) = stream_id {
        event[field::STREAM_ID] = stream_id;
    }
    WebSocketAction::Respond(async move { vec![Message::Text(event.to_string().into())] }.boxed())
}

async fn open(
    context: WebSocketContext,
    stream_id: Option<Value>,
) -> Result<reqwest::Upgraded, Vec<Message>> {
    let reply = match context.connect().await {
        Ok(reply) => reply,
        Err(error) => return Err(connection_failure(error, stream_id.as_ref()).await),
    };
    if reply.status() == StatusCode::SWITCHING_PROTOCOLS {
        return match reply.upgrade().await {
            Ok(connection) => Ok(connection),
            Err(error) => {
                Err(connection_failure(Error::bad_gateway(error), stream_id.as_ref()).await)
            }
        };
    }
    let status = crate::websocket::rejection_status(reply.status());
    let event = if status == StatusCode::UPGRADE_REQUIRED {
        let mut event = serde_json::json!({"type":"error","status":status.as_u16(),"error":{
            "code":error_code::WEBSOCKET_UPGRADE_REJECTED,
            "message":format!("Upstream rejected WebSocket upgrade (HTTP {}); reconnect using a supported transport", reply.status().as_u16())
        }});
        if let Some(stream_id) = stream_id {
            event[field::STREAM_ID] = stream_id;
        }
        event
    } else {
        compaction_transport::failure_event(
            crate::router::stream_response(reply),
            stream_id.as_ref(),
        )
        .await
    };
    Err(vec![
        Message::Text(event.to_string().into()),
        Message::Close(None),
    ])
}

async fn connection_failure(error: Error, stream_id: Option<&Value>) -> Vec<Message> {
    vec![
        Message::Text(
            compaction_transport::failure_event(error.into_response(), stream_id)
                .await
                .to_string()
                .into(),
        ),
        Message::Close(None),
    ]
}
