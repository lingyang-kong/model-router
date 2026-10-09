use axum::{
    body::Body,
    extract::Request,
    http::{HeaderMap, HeaderValue, StatusCode, Version},
    response::Response,
};
use hyper_util::rt::TokioIo;

use crate::{
    body::stream_body,
    config::{RouteAction, header_has_token, sanitize_headers},
    error::{Error, Result},
    router::{RouterState, stream_response},
};

pub(crate) async fn forward(
    state: &RouterState,
    mut request: Request,
    action: &RouteAction,
) -> Result<Response> {
    let provider_name = match action {
        RouteAction::Forward { provider } => provider.clone(),
        RouteAction::Model { provider } => {
            let model = model_hint(request.headers())?;
            state
                .resolve_provider(Some(model), provider.as_deref())
                .await?
        }
        RouteAction::Catalog { .. } => {
            return Err(Error::bad_request("Catalog actions use HTTP requests"));
        }
    };
    let mut downstream = hyper::upgrade::on(&mut request);
    let (parts, body) = request.into_parts();
    let (client, mut request) =
        state.prepare_request(&provider_name, action.provider(), &parts, stream_body(body))?;
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

fn model_hint(headers: &HeaderMap) -> Result<&str> {
    let mut model = None;
    for value in headers.get_all("x-model") {
        // OpenCode
        let value = value
            .to_str()
            .map_err(|_| Error::bad_request("Invalid WebSocket model hint"))?;
        set_model(&mut model, value)?;
    }
    for value in headers.get_all("x-codex-routing-hint") {
        // Codex
        let value = value
            .to_str()
            .map_err(|_| Error::bad_request("Invalid WebSocket model hint"))?;
        for field in value.split(';') {
            if let Some(value) = field.trim().strip_prefix("model=") {
                set_model(&mut model, value)?;
            }
        }
    }
    model.ok_or_else(|| {
        Error::bad_request("WebSocket model routes require x-model or x-codex-routing-hint")
    })
}

fn set_model<'a>(model: &mut Option<&'a str>, value: &'a str) -> Result<()> {
    if value.is_empty() || model.is_some_and(|previous| previous != value) {
        return Err(Error::bad_request(
            "Empty or conflicting WebSocket model hints",
        ));
    }
    *model = Some(value);
    Ok(())
}
