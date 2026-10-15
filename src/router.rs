use std::{io, sync::Arc};

use axum::{
    Json, Router,
    body::Body,
    extract::{Request, State},
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Response},
};
use futures_util::TryStreamExt;
use tokio::sync::RwLock;
use tokio_util::io::StreamReader;

use crate::{
    Config,
    adapters::Client,
    body::{inspect_body, stream_body},
    catalog::{ModelRegistry, load_manual_models},
    compression::decode_body,
    config::{Provider, RouteAction, sanitize_headers},
    error::{Error, Result},
    websocket,
};

pub(crate) struct RouterState {
    pub config: Config,
    pub models: RwLock<ModelRegistry>,
    client: reqwest::Client,
    loopback_client: reqwest::Client,
}

impl RouterState {
    pub async fn resolve_provider(
        &self,
        model: Option<&str>,
        fallback: Option<&Arc<Provider>>,
    ) -> Result<Arc<Provider>> {
        if let Some(model) = model
            && let Some(entry) = self.models.read().await.get(model)
        {
            return Ok(Arc::clone(&entry.provider));
        }
        if let Some(fallback) = fallback {
            return Ok(Arc::clone(fallback));
        }
        Err(match model {
            Some(model) => Error::unknown_model(model),
            None => Error::bad_request("A model is required for this route"),
        })
    }

    pub fn prepare_request(
        &self,
        provider: &Arc<Provider>,
        origin: Option<&Arc<Provider>>,
        parts: &Parts,
        body: reqwest::Body,
    ) -> Result<(&reqwest::Client, reqwest::Request)> {
        provider.validate_credential_scope(origin)?;
        let client = if provider.is_loopback {
            &self.loopback_client
        } else {
            &self.client
        };
        let request = client
            .request(parts.method.clone(), provider.upstream_url(&parts.uri))
            .headers(provider.request_headers(&parts.headers)?)
            .body(body)
            .build()
            .map_err(Error::bad_gateway)?;
        Ok((client, request))
    }
}

pub fn router(config: Config) -> anyhow::Result<Router> {
    let models = if config
        .routes
        .iter()
        .any(|route| !matches!(route.action, RouteAction::Forward { .. }))
    {
        load_manual_models(&config)?
    } else {
        ModelRegistry::new()
    };
    let state = RouterState {
        config,
        models: RwLock::new(models),
        client: build_http_client(false)?,
        loopback_client: build_http_client(true)?,
    };
    Ok(Router::new()
        .fallback(handle_request)
        .with_state(Arc::new(state)))
}

fn build_http_client(local: bool) -> reqwest::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_gzip()
        .no_deflate()
        .no_brotli()
        .no_zstd();
    if local {
        builder = builder.no_proxy();
    }
    builder.build()
}

async fn handle_request(
    State(state): State<Arc<RouterState>>,
    request: Request,
) -> Result<Response> {
    let adapter = Client::from_headers(request.headers())?;
    let action = &state
        .config
        .route(request.method(), request.uri(), request.headers())?
        .action;
    let origin = action
        .provider()
        .map(|name| state.config.provider(name))
        .transpose()?;
    if request
        .headers()
        .get("upgrade")
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"))
    {
        return websocket::forward(&state, request, action, origin, adapter).await;
    }
    forward_http(&state, request, action, origin, adapter).await
}

async fn forward_http(
    state: &RouterState,
    request: Request,
    action: &RouteAction,
    origin: Option<&Arc<Provider>>,
    adapter: Option<Client>,
) -> Result<Response> {
    if let (Some(adapter), RouteAction::Catalog { format, .. }) = (adapter, action)
        && adapter.catalog_format() != *format
    {
        return Err(Error::bad_request(
            "Catalog format does not match the client adapter",
        ));
    }
    let (parts, body) = request.into_parts();
    let (model, body) = match action {
        RouteAction::Forward { .. } | RouteAction::Catalog { .. } => (None, stream_body(body)),
        RouteAction::Model { .. } => inspect_body(body, &parts.headers).await?,
    };
    let provider = state.resolve_provider(model.as_deref(), origin).await?;
    let (client, mut request) = state.prepare_request(&provider, origin, &parts, body)?;
    if matches!(action, RouteAction::Catalog { .. }) {
        request.headers_mut().remove("if-none-match");
        request.headers_mut().remove("if-modified-since");
    }
    let response = client.execute(request).await.map_err(Error::bad_gateway)?;
    if let RouteAction::Catalog { format, .. } = action
        && response.status() == StatusCode::OK
    {
        let (parts, body) = stream_response(response).into_parts();
        let mut headers = parts.headers;
        let reader = StreamReader::new(body.into_data_stream().map_err(io::Error::other));
        let decoded = decode_body(reader, &headers)
            .await
            .map_err(Error::bad_gateway)?;
        let mut document = serde_json::from_slice(&decoded).map_err(Error::bad_gateway)?;
        let models = format.models(&mut document)?;
        state.merge_catalog(&provider, models, *format).await?;
        if let Some(adapter) = adapter {
            adapter.prepare_catalog(models);
        }
        // The rewritten JSON has its own encoding and body representation.
        for name in [
            "content-encoding",
            "content-type",
            "etag",
            "last-modified",
            "content-md5",
            "digest",
            "content-digest",
            "repr-digest",
            "trailer",
        ] {
            headers.remove(name);
        }
        let mut merged = Json(document).into_response();
        merged.headers_mut().extend(headers);
        return Ok(merged);
    }
    Ok(stream_response(response))
}

pub(crate) fn stream_response(response: reqwest::Response) -> Response {
    let mut response: axum::http::Response<reqwest::Body> = response.into();
    sanitize_headers(response.headers_mut());
    response.map(Body::new)
}
