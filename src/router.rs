use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::request::Parts,
    response::Response,
};
use tokio::sync::RwLock;

use crate::protocol::{header, token};
use crate::{
    Config,
    adapters::{CatalogFormat, Client, ClientKind},
    body::{BufferedRequest, rewritten_body, stream_body},
    catalog::CatalogState,
    config::{Provider, RouteAction, header_has_token, is_loopback_url, sanitize_headers},
    error::{Error, Result},
    websocket,
};

pub(crate) struct RouterState {
    pub config: Config,
    pub catalog: RwLock<CatalogState>,
    client: reqwest::Client,
    loopback_client: reqwest::Client,
}

impl RouterState {
    pub(crate) fn discovery_client(&self, url: &url::Url) -> &reqwest::Client {
        if is_loopback_url(url) {
            &self.loopback_client
        } else {
            &self.client
        }
    }

    pub async fn resolve_provider(
        &self,
        model: Option<&str>,
        fallback: Option<&Arc<Provider>>,
    ) -> Result<Arc<Provider>> {
        if let Some(model) = model
            && let Some(provider) = self.catalog.read().await.local_routes.get(model)
        {
            return Ok(Arc::clone(provider));
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
            .request(parts.method.clone(), provider.upstream_url(&parts.uri)?)
            .headers(provider.request_headers(&parts.headers)?)
            .body(body)
            .build()
            .map_err(Error::bad_gateway)?;
        Ok((client, request))
    }
}

pub fn router(mut config: Config) -> anyhow::Result<Router> {
    let catalog = CatalogState::load(&mut config)?;
    let state = RouterState {
        config,
        catalog: RwLock::new(catalog),
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
    let client = ClientKind::from_headers(request.headers());
    let adapter = client.adapter();
    let profile = state.config.clients.get(&client);
    let format = profile
        .and_then(|profile| profile.format)
        .unwrap_or_else(|| client.catalog_format());
    let action = &state
        .config
        .route(request.method(), request.uri(), request.headers())?
        .action;
    let selected = profile
        .and_then(|profile| profile.provider.as_ref())
        .map(|selection| selection.select(request.headers()))
        .transpose()?;
    let origin = selected
        .or_else(|| action.provider())
        .map(|name| state.config.provider(name))
        .transpose()?;
    if matches!(action, RouteAction::Forward { .. }) && origin.is_none() {
        return Err(Error::bad_request(
            "Forward routes require a provider in the route or selected client profile",
        ));
    }
    if header_has_token(request.headers(), header::UPGRADE, token::WEBSOCKET) {
        return websocket::forward(&state, request, action, origin, adapter).await;
    }
    forward_http(&state, request, action, origin, adapter, format).await
}

async fn forward_http(
    state: &Arc<RouterState>,
    request: Request,
    action: &RouteAction,
    origin: Option<&Arc<Provider>>,
    adapter: Option<Client>,
    format: CatalogFormat,
) -> Result<Response> {
    let (mut parts, body) = request.into_parts();
    let (provider, body) = match action {
        RouteAction::Catalog {} => {
            return state.catalog_response(parts, origin, format).await;
        }
        RouteAction::Forward { provider } => {
            let provider = provider
                .as_deref()
                .map(|name| state.config.provider(name))
                .transpose()?
                .or(origin)
                .expect("forward route has a provider");
            (Arc::clone(provider), stream_body(body))
        }
        RouteAction::Model { .. } if adapter.is_some() => {
            return adapter
                .unwrap()
                .forward_model(state, parts, body, origin)
                .await;
        }
        RouteAction::Model { .. } => {
            let buffered = BufferedRequest::collect(body, &parts.headers).await?;
            let (model, rewritten) = buffered.resolve_model(&state.config.model_overrides)?;
            let provider = state.resolve_provider(model.as_deref(), origin).await?;
            let body = match rewritten {
                Some(bytes) => rewritten_body(bytes, &mut parts.headers),
                None => buffered.into_body(),
            };
            (provider, body)
        }
    };
    let (client, request) = state.prepare_request(&provider, origin, &parts, body)?;
    let response = client.execute(request).await.map_err(Error::bad_gateway)?;
    Ok(stream_response(response))
}

pub(crate) fn stream_response(response: reqwest::Response) -> Response {
    let mut response: axum::http::Response<reqwest::Body> = response.into();
    sanitize_headers(response.headers_mut());
    response.map(Body::new)
}
