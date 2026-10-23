use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::request::Parts,
    response::Response,
};
use tokio::sync::RwLock;

use crate::{
    Config,
    adapters::{CatalogFormat, Client, ClientKind},
    body::{inspect_body, stream_body},
    catalog::{CatalogState, ModelRegistry, load_manual_models},
    config::{Provider, RouteAction, sanitize_headers},
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
        let local = match url.host() {
            Some(url::Host::Domain("localhost")) => true,
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };
        if local {
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
            && let Some(entry) = self.catalog.read().await.models.get(model)
        {
            return Ok(Arc::clone(&entry.provider));
        }
        // Explicit native IDs already identify their provider before remote discovery.
        if let Some(model) = model {
            let mut configured = self
                .config
                .providers
                .values()
                .filter(|provider| provider.models.contains_key(model));
            if let Some(provider) = configured.next() {
                if configured.next().is_some() {
                    return Err(Error::bad_gateway(format!(
                        "Conflicting providers for model: {model}"
                    )));
                }
                return Ok(Arc::clone(provider));
            }
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
    let from_file = |provider: &Provider| {
        provider
            .catalog
            .as_ref()
            .is_some_and(|catalog| catalog.file.is_some())
    };
    let ready = config
        .providers
        .values()
        .any(|provider| from_file(provider))
        && config
            .providers
            .values()
            .all(|provider| provider.catalog.is_none() || from_file(provider));
    let state = RouterState {
        config,
        catalog: RwLock::new(CatalogState { models, ready }),
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
    let adapter = Client::from_headers(request.headers());
    let client = ClientKind::from_headers(request.headers());
    let profile = state.config.clients.get(&client);
    let format = profile
        .and_then(|profile| profile.format)
        .unwrap_or_else(|| client.catalog_format());
    let action = &state
        .config
        .route(request.method(), request.uri(), request.headers())?
        .action;
    let origin = action
        .provider()
        .or_else(|| profile.and_then(|profile| profile.provider.as_deref()))
        .map(|name| state.config.provider(name))
        .transpose()?;
    if request
        .headers()
        .get("upgrade")
        .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"))
    {
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
    let (parts, body) = request.into_parts();
    let (model, body) = match action {
        RouteAction::Catalog {} => {
            return state.catalog_response(parts, origin, format).await;
        }
        RouteAction::Forward { .. } => (None, stream_body(body)),
        RouteAction::Model { .. } if adapter.is_some() => {
            return adapter
                .unwrap()
                .forward_model(state, parts, body, origin)
                .await;
        }
        RouteAction::Model { .. } => inspect_body(body, &parts.headers).await?,
    };
    let provider = state.resolve_provider(model.as_deref(), origin).await?;
    let (client, request) = state.prepare_request(&provider, origin, &parts, body)?;
    let response = client.execute(request).await.map_err(Error::bad_gateway)?;
    Ok(stream_response(response))
}

pub(crate) fn stream_response(response: reqwest::Response) -> Response {
    let mut response: axum::http::Response<reqwest::Body> = response.into();
    sanitize_headers(response.headers_mut());
    response.map(Body::new)
}
