use std::{borrow::Cow, sync::Arc};

use axum::{body::Body, http::request::Parts, response::Response};
use http_body_util::BodyExt;
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

use crate::{
    body::{buffered_body, request_model},
    compression::decode_body,
    config::Provider,
    error::{Error, Result},
    router::RouterState,
};

use super::{multiagent, response};
use crate::adapters::websocket::WebSocketContext;

pub(crate) async fn forward(
    state: &RouterState,
    mut parts: Parts,
    body: Body,
    origin: Option<&Arc<Provider>>,
) -> Result<Response> {
    let collected = body.collect().await.map_err(Error::bad_request)?;
    let trailers = collected.trailers().cloned();
    let original = collected.to_bytes();
    let decoded = if parts.headers.contains_key("content-encoding") {
        Cow::Owned(
            decode_body(std::io::Cursor::new(&original), &parts.headers)
                .await
                .map_err(Error::bad_request)?,
        )
    } else {
        Cow::Borrowed(original.as_ref())
    };
    let model = request_model(&decoded)?;
    let provider = state.resolve_provider(model.as_deref(), origin).await?;
    provider.validate_credential_scope(origin)?;
    let rewritten = prepare_request(state, &provider, origin, &parts, &decoded, model.as_deref())
        .await
        .map_err(Error::bad_request)?;
    let body = if let Some(bytes) = rewritten {
        // Changed bytes invalidate their original encoding, length, and integrity metadata.
        for header in [
            "content-encoding",
            "content-length",
            "content-md5",
            "digest",
            "content-digest",
            "repr-digest",
            "trailer",
            "te",
        ] {
            parts.headers.remove(header);
        }
        reqwest::Body::from(bytes)
    } else {
        buffered_body(original, trailers)
    };
    let (client, request) = state.prepare_request(&provider, origin, &parts, body)?;
    let upstream = client.execute(request).await.map_err(Error::bad_gateway)?;
    response::restore(upstream).await
}

pub(in crate::adapters) async fn adapt_websocket_message(
    message: Message,
    request: bool,
    context: &WebSocketContext,
) -> Message {
    let Message::Text(text) = message else {
        return message;
    };
    let rewritten = if request {
        let model = request_model(text.as_bytes()).ok().flatten();
        let mut request = axum::http::Request::builder()
            .uri(context.uri.clone())
            .body(())
            .unwrap();
        *request.headers_mut() = context.headers.clone();
        let (parts, _) = request.into_parts();
        prepare_request(
            &context.state,
            &context.provider,
            Some(&context.provider),
            &parts,
            text.as_bytes(),
            model.as_deref().or(context.model.as_deref()),
        )
        .await
    } else {
        multiagent::restore_bytes(text.as_bytes())
    };
    match rewritten {
        Ok(Some(bytes)) => Message::Text(String::from_utf8(bytes).expect("JSON is UTF-8").into()),
        Ok(None) | Err(_) => Message::Text(text),
    }
}

async fn prepare_request(
    state: &RouterState,
    provider: &Arc<Provider>,
    origin: Option<&Arc<Provider>>,
    parts: &Parts,
    bytes: &[u8],
    model: Option<&str>,
) -> serde_json::Result<Option<Vec<u8>>> {
    if !multiagent::request_may_need_adaptation(bytes) {
        return Ok(None);
    }
    let text_agent_messages = if multiagent::has_agent_message(bytes)? {
        state
            .probe_response_item(
                provider,
                origin,
                parts,
                model,
                serde_json::json!({
                    "type":"agent_message", "author":"/root", "recipient":"/root/worker",
                    "content":[{"type":"input_text","text":"probe"}]
                }),
            )
            .await
            == Some(false)
    } else {
        false
    };
    let listing = model_listing(state).await;
    multiagent::prepare_bytes(bytes, text_agent_messages, &listing)
}

async fn model_listing(state: &RouterState) -> String {
    let catalog = state.catalog.read().await;
    if !catalog.ready {
        return String::new();
    }
    let mut listing = String::from(
        "Available model overrides (optional; inherited parent model is preferred):\n",
    );
    for (id, entry) in &catalog.models {
        if !entry.listed || entry.provider.hidden_models.is_match(id) {
            continue;
        }
        listing.push_str("- ");
        listing.push_str(id);
        if let Some(description) = entry.metadata.get("description").and_then(Value::as_str) {
            listing.push_str(": ");
            listing.push_str(description);
        }
        let shared = entry.format.shared_metadata(&entry.metadata);
        if let Some(levels) = shared.get("reasoning_levels").and_then(Value::as_array) {
            let levels: Vec<_> = levels
                .iter()
                .filter_map(|level| level.get("level").and_then(Value::as_str))
                .collect();
            if !levels.is_empty() {
                listing.push_str(". Reasoning efforts: ");
                listing.push_str(&levels.join(", "));
                listing.push('.');
            }
        }
        listing.push('\n');
    }
    listing.push('\n');
    listing
}
