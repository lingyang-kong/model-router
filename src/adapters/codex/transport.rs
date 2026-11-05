use std::{borrow::Cow, sync::Arc};

use axum::{
    body::Body,
    http::{Method, request::Parts},
    response::{IntoResponse, Response},
};
use futures_util::FutureExt;
use http_body_util::BodyExt;
use serde_json::Value;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};

use crate::{
    body::{buffered_body, request_model},
    compression::decode_body,
    config::Provider,
    error::{Error, Result},
    router::RouterState,
};

use super::{compaction, compaction_transport, multiagent, response};
use crate::adapters::websocket::{WebSocketAction, WebSocketContext};

enum RequestPlan {
    Forward(Option<Vec<u8>>),
    Summary(compaction::SummaryRequest),
    Replay,
}

pub(crate) async fn forward(
    state: &Arc<RouterState>,
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
    let plan =
        prepare_request(state, &provider, origin, &parts, &decoded, model.as_deref()).await?;
    let rewritten = match plan {
        RequestPlan::Replay => {
            return Err(Error::bad_request(
                "Synthetic compaction requires the complete input history",
            ));
        }
        RequestPlan::Forward(rewritten) => rewritten,
        RequestPlan::Summary(summary) => {
            let stream = summary.stream;
            let work = compaction_transport::summarize(
                Arc::clone(state),
                Arc::clone(&provider),
                origin.cloned(),
                parts,
                summary,
            );
            return Ok(if stream {
                compaction_transport::sse(work)
            } else {
                match work.await {
                    Ok(response) => compaction_transport::json(response),
                    Err(response) => *response,
                }
            });
        }
    };
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
    if parts.method == Method::HEAD {
        return Ok(crate::router::stream_response(upstream));
    }
    response::restore(upstream).await
}

pub(in crate::adapters) fn adapt_websocket_response(message: Message) -> Message {
    let Message::Text(text) = message else {
        return message;
    };
    let rewritten = multiagent::restore_bytes(text.as_bytes());
    match rewritten {
        Ok(Some(bytes)) => Message::Text(String::from_utf8(bytes).expect("JSON is UTF-8").into()),
        Ok(None) | Err(_) => Message::Text(text),
    }
}

pub(in crate::adapters) async fn websocket_request(
    text: Utf8Bytes,
    context: &WebSocketContext,
    model: Option<&str>,
    stream_id: Option<Value>,
) -> WebSocketAction {
    let mut request = axum::http::Request::builder()
        .uri(context.uri.clone())
        .body(())
        .unwrap();
    *request.headers_mut() = context.headers.clone();
    let (parts, _) = request.into_parts();
    let prepared = match prepare_request(
        &context.state,
        &context.provider,
        context.origin.as_ref(),
        &parts,
        text.as_bytes(),
        model,
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(error) => {
            return WebSocketAction::Respond(
                async move {
                    let event = compaction_transport::failure_event(
                        error.into_response(),
                        stream_id.as_ref(),
                    )
                    .await;
                    vec![Message::Text(event.to_string().into())]
                }
                .boxed(),
            );
        }
    };
    let summary = match prepared {
        RequestPlan::Replay => {
            let mut event = serde_json::json!({"type":"error","status":400,"error":{
                "code":"previous_response_not_found",
                "message":"Synthetic compaction requires full input; retry without previous_response_id"
            }});
            if let Some(stream_id) = stream_id {
                event["stream_id"] = stream_id;
            }
            return WebSocketAction::Respond(
                async move { vec![Message::Text(event.to_string().into())] }.boxed(),
            );
        }
        RequestPlan::Forward(rewritten) => {
            return WebSocketAction::Forward(match rewritten {
                Some(bytes) => {
                    Message::Text(String::from_utf8(bytes).expect("JSON is UTF-8").into())
                }
                None => Message::Text(text),
            });
        }
        RequestPlan::Summary(summary) => summary,
    };
    let state = Arc::clone(&context.state);
    let provider = Arc::clone(&context.provider);
    WebSocketAction::Respond(
        async move {
            let events = match compaction_transport::summarize(
                state,
                Arc::clone(&provider),
                Some(provider),
                parts,
                summary,
            )
            .await
            {
                Ok(response) => compaction::events(&response, stream_id.as_ref()),
                Err(response) => {
                    vec![compaction_transport::failure_event(*response, stream_id.as_ref()).await]
                }
            };
            events
                .into_iter()
                .map(|event| Message::Text(event.to_string().into()))
                .collect()
        }
        .boxed(),
    )
}

async fn prepare_request(
    state: &RouterState,
    provider: &Arc<Provider>,
    origin: Option<&Arc<Provider>>,
    parts: &Parts,
    bytes: &[u8],
    model: Option<&str>,
) -> crate::error::Result<RequestPlan> {
    let mut prepared = compaction::prepare(bytes, compaction::Mode::Native, model)?;
    let mut mode = compaction::Mode::Native;
    if prepared.native_trigger
        && state
            .probe_response_item(
                provider,
                origin,
                parts,
                model,
                serde_json::json!({"type":"compaction_trigger"}),
            )
            .await
            == Some(false)
    {
        mode = compaction::Mode::Text;
    }
    if compaction::needs_full_history(bytes, mode)? {
        return Ok(RequestPlan::Replay);
    }
    if mode == compaction::Mode::Text {
        prepared = compaction::prepare(bytes, mode, model)?;
    }
    if let Some(summary) = prepared.summary {
        return Ok(RequestPlan::Summary(summary));
    }
    let bytes = prepared.rewritten.as_deref().unwrap_or(bytes);
    if multiagent::request_may_need_adaptation(bytes) {
        let listing = state
            .catalog
            .read()
            .await
            .listing(&parts.headers, origin)
            .to_owned();
        let rewritten =
            prepare_multiagent(state, provider, origin, parts, bytes, model, &listing).await?;
        prepared.rewritten = rewritten.or(prepared.rewritten);
    }
    Ok(RequestPlan::Forward(prepared.rewritten))
}

pub(super) async fn prepare_multiagent(
    state: &RouterState,
    provider: &Arc<Provider>,
    origin: Option<&Arc<Provider>>,
    parts: &Parts,
    bytes: &[u8],
    model: Option<&str>,
    listing: &str,
) -> Result<Option<Vec<u8>>> {
    let convert = multiagent::has_agent_message(bytes).map_err(Error::bad_request)?
        && state
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
            == Some(false);
    multiagent::prepare_bytes(bytes, convert, listing).map_err(Error::bad_request)
}
