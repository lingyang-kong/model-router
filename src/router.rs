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
    adapters::Client,
    body::{inspect_body, stream_body},
    catalog::{ModelRegistry, load_manual_models},
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
        RouteAction::Catalog { format, .. } => {
            return state
                .catalog_response(
                    parts,
                    body,
                    origin.expect("catalog requires a provider"),
                    *format,
                    adapter,
                )
                .await;
        }
        RouteAction::Forward { .. } => (None, stream_body(body)),
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
