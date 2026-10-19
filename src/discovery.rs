use std::{collections::BTreeSet, io, sync::Arc, time::Duration};

use axum::http::{HeaderMap, Method, StatusCode, request::Parts};
use futures_util::{TryStreamExt, future::join_all};
use tokio_util::io::StreamReader;
use url::Url;

use crate::{
    adapters::CatalogFormat,
    capabilities::{self, Discovery},
    compression::decode_body,
    config::Provider,
    router::RouterState,
};

impl RouterState {
    /// Probe the provider's `/responses` endpoint during catalog projection.
    pub(crate) async fn probe_catalog_item(
        &self,
        provider: &Arc<Provider>,
        origin: &Arc<Provider>,
        incoming: &HeaderMap,
        model: &str,
        item: serde_json::Value,
    ) -> Option<bool> {
        let (mut parts, _) = axum::http::Request::builder()
            .uri("/responses")
            .body(())
            .ok()?
            .into_parts();
        parts.headers = incoming.clone();
        self.probe_response_item(provider, Some(origin), &parts, Some(model), item)
            .await
    }

    /// Return `Some(false)` only for an explicit rejection of the tested item;
    /// all other outcomes remain unknown. The invalid sentinel requests validation
    /// failure, but a permissive server may still generate up to one token.
    pub(crate) async fn probe_response_item(
        &self,
        provider: &Arc<Provider>,
        origin: Option<&Arc<Provider>>,
        parts: &Parts,
        model: Option<&str>,
        item: serde_json::Value,
    ) -> Option<bool> {
        provider.validate_credential_scope(origin).ok()?;
        let item_type = item.get("type")?.as_str()?.to_owned();
        let url = provider.upstream_url(&parts.uri).ok()?;
        let mut headers = discovery_headers(provider, &parts.headers, &url)?;
        for header in [
            "sec-websocket-key",
            "sec-websocket-version",
            "sec-websocket-protocol",
            "sec-websocket-extensions",
        ] {
            headers.remove(header);
        }
        let mut body = serde_json::json!({
            "input": [item, {"type":"__model_router_capability_check__"}],
            "max_output_tokens": 1,
            "store": false,
        });
        if let Some(model) = model {
            body["model"] = serde_json::Value::String(model.to_owned());
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            let response = self
                .discovery_client(&url)
                .request(Method::POST, url)
                .headers(headers)
                .header("content-type", "application/json")
                .body(body.to_string())
                .send()
                .await
                .ok()?;
            if !matches!(
                response.status(),
                StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY
            ) {
                return None;
            }
            let document = read_document(response).await?;
            document
                .pointer("/error/message")?
                .as_str()?
                .starts_with(&format!(
                    "Unsupported Responses API input item type: '{item_type}'"
                ))
                .then_some(false)
        })
        .await
        .ok()
        .flatten()
    }

    /// Optional read-only information sources. Unavailable sources supply no facts.
    pub(crate) async fn discover(
        &self,
        provider: &Arc<Provider>,
        incoming: &HeaderMap,
        mut found: Discovery,
        models: &[serde_json::Value],
        format: CatalogFormat,
    ) -> Discovery {
        let mut info = Vec::new();
        let mut tools = Vec::new();
        let mut seen = BTreeSet::new();
        for url in &provider.discovery_urls {
            if !seen.insert(url.clone()) {
                continue;
            }
            if is_tools_url(url) {
                tools.push(url.clone());
            } else {
                info.push(url.clone());
            }
        }
        add_tool_paths(
            &provider.base_url,
            &found.tools_paths,
            &mut tools,
            &mut seen,
        );
        let documents = join_all(
            info.iter()
                .map(|url| self.discover_at(provider, incoming, url)),
        )
        .await;
        for (url, document) in info.iter().zip(documents) {
            let Some(document) = document else { continue };
            add_tool_paths(url, &document.tools_paths, &mut tools, &mut seen);
            found.merge(document);
        }
        let unsupported = match provider.capabilities.tool_inventory {
            Some(supported) => !supported,
            None => {
                found.capabilities.tool_inventory == Some(false)
                    || (!models.is_empty()
                        && models.iter().all(|model| {
                            format
                                .model_id(model)
                                .is_ok_and(|id| found.for_model(id).tool_inventory == Some(false))
                        }))
            }
        };
        if !unsupported {
            for document in join_all(
                tools
                    .iter()
                    .map(|url| self.discover_at(provider, incoming, url)),
            )
            .await
            .into_iter()
            .flatten()
            {
                found.merge(document);
            }
        }
        found
    }

    async fn discover_at(
        &self,
        provider: &Provider,
        incoming: &HeaderMap,
        url: &Url,
    ) -> Option<Discovery> {
        // Explicit external sources are read anonymously; advertised links stay same-origin.
        let headers = discovery_headers(provider, incoming, url)?;
        tokio::time::timeout(Duration::from_secs(3), async {
            let response = self
                .discovery_client(url)
                .get(url.clone())
                .headers(headers)
                .send()
                .await
                .ok()?;
            if response.status() != StatusCode::OK {
                return None;
            }
            Some(capabilities::parse(&read_document(response).await?))
        })
        .await
        .ok()
        .flatten()
    }
}

fn discovery_headers(provider: &Provider, incoming: &HeaderMap, url: &Url) -> Option<HeaderMap> {
    let mut headers = if url.origin() == provider.base_url.origin() {
        provider.request_headers(incoming).ok()?
    } else {
        HeaderMap::new()
    };
    for name in [
        "idempotency-key",
        "if-none-match",
        "if-modified-since",
        "content-encoding",
        "content-type",
        "content-md5",
        "digest",
        "content-digest",
        "repr-digest",
        "trailer",
    ] {
        headers.remove(name);
    }
    Some(headers)
}

async fn read_document(response: reqwest::Response) -> Option<serde_json::Value> {
    let headers = response.headers().clone();
    let body: reqwest::Body = response.into();
    let body = axum::body::Body::new(body);
    let reader = StreamReader::new(body.into_data_stream().map_err(io::Error::other));
    let bytes = decode_body(reader, &headers).await.ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn is_tools_url(url: &Url) -> bool {
    let path = url.path().trim_end_matches('/');
    path.ends_with("/tools") || path.ends_with("/tools/list")
}

fn add_tool_paths(source: &Url, paths: &[String], tools: &mut Vec<Url>, seen: &mut BTreeSet<Url>) {
    for path in paths {
        if let Ok(endpoint) = source.join(path)
            && endpoint.origin() == source.origin()
            && seen.insert(endpoint.clone())
        {
            tools.push(endpoint);
        }
    }
}
