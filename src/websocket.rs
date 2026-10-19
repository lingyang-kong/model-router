use std::sync::Arc;

use axum::{
    body::Body,
    extract::Request,
    http::{HeaderMap, HeaderValue, StatusCode, Version, request::Parts},
    response::Response,
};
use hyper_util::rt::TokioIo;

use crate::{
    adapters::{Client, ModelHint, websocket::WebSocketContext},
    body::stream_body,
    config::{Provider, RouteAction, header_has_token, sanitize_headers},
    error::{Error, Result},
    router::{RouterState, stream_response},
};

pub(crate) async fn forward(
    state: &Arc<RouterState>,
    mut request: Request,
    action: &RouteAction,
    origin: Option<&Arc<Provider>>,
    adapter: Option<Client>,
) -> Result<Response> {
    let model = match action {
        RouteAction::Forward { .. } => None,
        RouteAction::Model { .. } => {
            let model = model_hint(request.headers(), adapter)?;
            if model.is_none() && adapter.is_none() {
                return Err(Error::bad_request(
                    "WebSocket model routes require a model hint",
                ));
            }
            model
        }
        RouteAction::Catalog { .. } => {
            return Err(Error::bad_request("Catalog actions use HTTP requests"));
        }
    };
    let provider = state.resolve_provider(model, origin).await?;
    let model = model.map(str::to_owned);
    let mut downstream = hyper::upgrade::on(&mut request);
    let (parts, body) = request.into_parts();
    let adapted = matches!(action, RouteAction::Model { .. }) && adapter.is_some();
    let opening = connect(state, &provider, origin, &parts, stream_body(body), adapted);
    let response = tokio::select! {
        response = opening => response?,
        _ = &mut downstream => return Err(Error::bad_request("Client disconnected during WebSocket handshake")),
    };
    if response.status() != StatusCode::SWITCHING_PROTOCOLS {
        let status = rejection_status(response.status());
        let mut response = stream_response(response);
        *response.status_mut() = status;
        return Ok(response);
    }
    let mut headers = response.headers().clone();
    sanitize_headers(&mut headers);
    restore_upgrade_headers(response.headers(), &mut headers);
    let upstream = response.upgrade().await.map_err(Error::bad_gateway)?;
    let context = WebSocketContext {
        state: Arc::clone(state),
        provider: Arc::clone(&provider),
        headers: parts.headers,
        uri: parts.uri,
        model,
        origin: origin.cloned(),
        connected: true,
    };
    tokio::spawn(async move {
        if let Ok(downstream) = downstream.await {
            let downstream = TokioIo::new(downstream);
            if let Some(adapter) = adapter.filter(|_| adapted) {
                crate::adapters::websocket::relay(downstream, upstream, adapter, context).await;
            } else {
                let mut downstream = downstream;
                let mut upstream = upstream;
                let _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await;
            }
        }
    });
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    *response.headers_mut() = headers;
    Ok(response)
}

pub(crate) async fn connect(
    state: &RouterState,
    provider: &Arc<Provider>,
    origin: Option<&Arc<Provider>>,
    parts: &Parts,
    body: reqwest::Body,
    adapted: bool,
) -> Result<reqwest::Response> {
    let (client, mut request) = state.prepare_request(provider, origin, parts, body)?;
    restore_upgrade_headers(&parts.headers, request.headers_mut());
    if adapted {
        request.headers_mut().remove("sec-websocket-extensions");
    }
    *request.version_mut() = Version::HTTP_11;
    let response = client.execute(request).await.map_err(Error::bad_gateway)?;
    if adapted
        && response.status() == StatusCode::SWITCHING_PROTOCOLS
        && response.headers().contains_key("sec-websocket-extensions")
    {
        return Err(Error::bad_gateway(
            "Upstream negotiated an unsupported WebSocket extension",
        ));
    }
    Ok(response)
}

pub(crate) fn rejection_status(status: StatusCode) -> StatusCode {
    if status.is_success()
        || matches!(
            status,
            StatusCode::NOT_FOUND
                | StatusCode::METHOD_NOT_ALLOWED
                | StatusCode::UPGRADE_REQUIRED
                | StatusCode::NOT_IMPLEMENTED
        )
    {
        StatusCode::UPGRADE_REQUIRED
    } else {
        status
    }
}

/// Re-establish the upgrade on each HTTP hop; negotiation headers pass through.
fn restore_upgrade_headers(original: &HeaderMap, headers: &mut HeaderMap) {
    if header_has_token(original, "connection", "upgrade") {
        headers.append("connection", HeaderValue::from_static("upgrade"));
    }
    headers.remove("upgrade");
    for value in original.get_all("upgrade") {
        headers.append("upgrade", value.clone());
    }
}

fn model_hint(headers: &HeaderMap, client: Option<Client>) -> Result<Option<&str>> {
    let mut model = ModelHint::default();
    for value in headers.get_all("x-model") {
        let value = value
            .to_str()
            .map_err(|_| Error::bad_request("Invalid WebSocket model hint"))?;
        model.set(value)?;
    }
    if let Some(client) = client {
        client.model_hint(headers, &mut model)?;
    }
    Ok(model.optional())
}
