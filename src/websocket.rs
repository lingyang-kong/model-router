use std::sync::Arc;

use axum::{
    body::Body,
    extract::Request,
    http::{HeaderMap, HeaderValue, StatusCode, Version},
    response::Response,
};
use hyper_util::rt::TokioIo;

use crate::{
    adapters::{Client, ModelHint},
    body::stream_body,
    config::{Provider, RouteAction, header_has_token, sanitize_headers},
    error::{Error, Result},
    router::{RouterState, stream_response},
};

pub(crate) async fn forward(
    state: &RouterState,
    mut request: Request,
    action: &RouteAction,
    origin: Option<&Arc<Provider>>,
    adapter: Option<Client>,
) -> Result<Response> {
    let model = match action {
        RouteAction::Forward { .. } => None,
        RouteAction::Model { .. } => Some(model_hint(request.headers(), adapter)?),
        RouteAction::Catalog { .. } => {
            return Err(Error::bad_request("Catalog actions use HTTP requests"));
        }
    };
    let provider = state.resolve_provider(model, origin).await?;
    let mut downstream = hyper::upgrade::on(&mut request);
    let (parts, body) = request.into_parts();
    let (client, mut request) =
        state.prepare_request(&provider, origin, &parts, stream_body(body))?;
    restore_upgrade_headers(&parts.headers, request.headers_mut());
    *request.version_mut() = Version::HTTP_11;
    let opening = client.execute(request);
    let response = tokio::select! {
        response = opening => response.map_err(Error::bad_gateway)?,
        _ = &mut downstream => return Err(Error::bad_request("Client disconnected during WebSocket handshake")),
    };
    if response.status() != StatusCode::SWITCHING_PROTOCOLS {
        return Ok(stream_response(response));
    }
    let mut headers = response.headers().clone();
    sanitize_headers(&mut headers);
    restore_upgrade_headers(response.headers(), &mut headers);
    let mut upstream = response.upgrade().await.map_err(Error::bad_gateway)?;
    tokio::spawn(async move {
        if let Ok(downstream) = downstream.await {
            let mut downstream = TokioIo::new(downstream);
            let _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await;
        }
    });
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    *response.headers_mut() = headers;
    Ok(response)
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

fn model_hint(headers: &HeaderMap, client: Option<Client>) -> Result<&str> {
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
    model.require()
}
