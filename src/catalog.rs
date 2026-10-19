use std::{io, sync::Arc};

use axum::{
    body::Body,
    extract::Request,
    http::{HeaderMap, Method, StatusCode, request::Parts},
    response::Response,
};
use futures_util::{StreamExt, TryStreamExt, future::try_join_all, stream::FuturesOrdered};
use indexmap::IndexMap;
use serde_json::{Map, Value};
use tokio_util::io::StreamReader;

use crate::{
    Config,
    adapters::CatalogFormat,
    body::stream_body,
    capabilities::{self, Discovery},
    compression::{accepts_gzip, decode_body, encode_body},
    config::{Provider, header_has_token},
    error::{Error, Result},
    router::{RouterState, stream_response},
};

pub(crate) struct ModelEntry {
    pub provider: Arc<Provider>,
    pub format: CatalogFormat,
    /// Source-format metadata after local settings, before output projection.
    pub metadata: Value,
    /// False for retired IDs kept routable but excluded from current listings.
    pub listed: bool,
}
pub(crate) type ModelRegistry = IndexMap<String, ModelEntry>;

pub(crate) struct CatalogState {
    pub models: ModelRegistry,
    /// Distinguishes an authoritative empty catalog from discovery not yet completed.
    pub ready: bool,
}

/// Object fields combine recursively; arrays and scalar values replace the old value.
pub(crate) fn overlay(target: &mut Value, fields: &Map<String, Value>) {
    for (key, value) in fields {
        if let Some(existing) = target.get_mut(key)
            && existing.is_object()
            && let Value::Object(fields) = value
        {
            overlay(existing, fields);
        } else {
            target[key] = value.clone();
        }
    }
}

fn resolve_metadata(provider: &Provider, model: &Value, format: CatalogFormat) -> Result<Value> {
    let id = format.model_id(model)?;
    let mut resolved = Value::Object(format.settings(&provider.defaults.metadata));
    overlay(
        &mut resolved,
        &format.settings(model.as_object().expect("model ID requires an object")),
    );
    if let Some(overrides) = provider.models.get(id) {
        overlay(&mut resolved, &format.settings(&overrides.metadata));
    }
    if format.model_id(&resolved)? != id {
        return Err(Error::bad_gateway(
            "Model overrides cannot change the source model ID",
        ));
    }
    Ok(resolved)
}

pub(crate) fn load_manual_models(config: &Config) -> Result<ModelRegistry> {
    let mut registry = ModelRegistry::new();
    for provider in config.providers.values() {
        let Some(settings) = &provider.catalog else {
            continue;
        };
        let Some(file) = &settings.file else { continue };
        let bytes = std::fs::read(file).map_err(Error::bad_gateway)?;
        let mut document: Value = serde_json::from_slice(&bytes).map_err(Error::bad_gateway)?;
        validate_complete_catalog(&document)?;
        for model in settings.format.models(&mut document)? {
            let metadata = resolve_metadata(provider, model, settings.format)?;
            let id = settings.format.model_id(&metadata)?.to_owned();
            validate_ownership(config, &registry, &id, provider)?;
            registry.insert(
                id,
                ModelEntry {
                    provider: Arc::clone(provider),
                    format: settings.format,
                    metadata,
                    listed: true,
                },
            );
        }
    }
    Ok(registry)
}

struct CatalogSnapshot {
    provider: Arc<Provider>,
    format: CatalogFormat,
    document: Value,
    headers: HeaderMap,
    discovery: Discovery,
}

enum CatalogReply {
    Catalog(Box<CatalogSnapshot>),
    Rejected(Response),
}

impl RouterState {
    async fn fetch_catalog(
        &self,
        provider: &Arc<Provider>,
        origin: &Arc<Provider>,
        incoming: Option<(Parts, Body)>,
        output: CatalogFormat,
    ) -> Result<CatalogReply> {
        let discovery_headers = incoming
            .as_ref()
            .map(|(parts, _)| parts.headers.clone())
            .unwrap_or_default();
        let source = provider.catalog.as_ref();
        let format = source.map_or(output, |source| source.format);
        if source.is_some_and(|source| source.file.is_some()) {
            let mut document = format.empty_catalog();
            format.models(&mut document)?.extend(
                self.catalog
                    .read()
                    .await
                    .models
                    .values()
                    .filter(|entry| Arc::ptr_eq(&entry.provider, provider))
                    .map(|entry| entry.metadata.clone()),
            );
            let initial = capabilities::parse(&document);
            let discovery = self
                .discover(
                    provider,
                    &discovery_headers,
                    initial,
                    format.models(&mut document)?,
                    format,
                )
                .await;
            return Ok(CatalogReply::Catalog(Box::new(CatalogSnapshot {
                provider: Arc::clone(provider),
                format,
                document,
                headers: HeaderMap::new(),
                discovery,
            })));
        }

        let (mut parts, body) = match source.and_then(|source| source.path.as_deref()) {
            Some(path) => {
                let uri = match incoming.as_ref().and_then(|(parts, _)| parts.uri.query()) {
                    Some(query) => format!("{path}?{query}"),
                    None => path.to_owned(),
                };
                let (mut parts, body) = Request::builder()
                    .method(Method::GET)
                    .uri(uri)
                    .body(Body::empty())
                    .map_err(Error::bad_request)?
                    .into_parts();
                // Only the selected catalog source owns the incoming credentials and headers.
                if let Some((incoming, _)) = incoming {
                    parts.headers = incoming.headers;
                }
                for name in [
                    "content-encoding",
                    "content-type",
                    "content-md5",
                    "digest",
                    "content-digest",
                    "repr-digest",
                    "trailer",
                ] {
                    parts.headers.remove(name);
                }
                (parts, body)
            }
            None => {
                incoming.ok_or_else(|| Error::bad_request("Provider needs a catalog source"))?
            }
        };
        parts.headers.remove("if-none-match");
        parts.headers.remove("if-modified-since");
        let (client, request) =
            self.prepare_request(provider, Some(origin), &parts, stream_body(body))?;
        let response = client.execute(request).await.map_err(Error::bad_gateway)?;
        if response.status() != StatusCode::OK {
            if Arc::ptr_eq(provider, origin) {
                return Ok(CatalogReply::Rejected(stream_response(response)));
            }
            return Err(Error::bad_gateway(format!(
                "Provider catalog returned {}",
                response.status()
            )));
        }
        let (parts, body) = stream_response(response).into_parts();
        let reader = StreamReader::new(body.into_data_stream().map_err(io::Error::other));
        let bytes = decode_body(reader, &parts.headers)
            .await
            .map_err(Error::bad_gateway)?;
        let mut document = serde_json::from_slice(&bytes).map_err(Error::bad_gateway)?;
        validate_complete_catalog(&document)?;
        for model in format.models(&mut document)? {
            *model = resolve_metadata(provider, model, format)?;
        }
        let initial = capabilities::parse(&document);
        let discovery = self
            .discover(
                provider,
                &discovery_headers,
                initial,
                format.models(&mut document)?,
                format,
            )
            .await;
        Ok(CatalogReply::Catalog(Box::new(CatalogSnapshot {
            provider: Arc::clone(provider),
            format,
            document,
            headers: parts.headers,
            discovery,
        })))
    }

    pub(crate) async fn catalog_response(
        &self,
        parts: Parts,
        body: Body,
        origin: &Arc<Provider>,
        output: CatalogFormat,
    ) -> Result<Response> {
        let gzip = accepts_gzip(&parts.headers)?;
        let incoming_headers = parts.headers.clone();
        let mut auxiliary = Vec::new();
        for provider in self.config.providers.values() {
            if !Arc::ptr_eq(provider, origin) && provider.catalog.is_some() {
                auxiliary.push(self.fetch_catalog(provider, origin, None, output));
            }
        }
        // Fetch both groups concurrently, but yield the origin first so its rejection
        // takes precedence. Returning drops unfinished fetches.
        let mut requests = [
            vec![self.fetch_catalog(origin, origin, Some((parts, body)), output)],
            auxiliary,
        ]
        .into_iter()
        .map(try_join_all)
        .collect::<FuturesOrdered<_>>();
        let mut snapshots = Vec::new();
        while let Some(replies) = requests.next().await {
            for reply in replies? {
                match reply {
                    CatalogReply::Catalog(snapshot) => snapshots.push(*snapshot),
                    CatalogReply::Rejected(response) => return Ok(response),
                }
            }
        }
        // References resolve against this complete refresh, never stale routing metadata.
        let mut staged = ModelRegistry::new();
        for snapshot in &mut snapshots {
            for metadata in std::mem::take(snapshot.format.models(&mut snapshot.document)?) {
                let id = snapshot.format.model_id(&metadata)?.to_owned();
                validate_ownership(&self.config, &staged, &id, &snapshot.provider)?;
                staged.insert(
                    id,
                    ModelEntry {
                        provider: Arc::clone(&snapshot.provider),
                        format: snapshot.format,
                        metadata,
                        listed: true,
                    },
                );
            }
        }
        let projections = staged.iter().map(|(id, entry)| {
            let snapshots = &snapshots;
            let staged = &staged;
            let incoming_headers = &incoming_headers;
            async move {
                // Discovery lives only in this refresh; previous observations are never reused.
                let snapshot = snapshots
                    .iter()
                    .find(|snapshot| Arc::ptr_eq(&snapshot.provider, &entry.provider))
                    .expect("staged model source");
                let mut capabilities = snapshot.discovery.for_model(id);
                capabilities.merge(capabilities::parse(&entry.metadata).for_model(id));
                capabilities.merge(snapshot.provider.capabilities.clone());
                let settings = entry.provider.models.get(id);
                let reference = settings
                    .and_then(|settings| settings.extends.as_ref())
                    .or(entry.provider.defaults.extends.as_ref());
                let base = reference
                    .map(|reference| {
                        let provider = self.config.provider(&reference.provider)?;
                        staged
                            .get(&reference.model)
                            .filter(|model| Arc::ptr_eq(&model.provider, provider))
                            .map(|model| (&model.metadata, model.format))
                            .ok_or_else(|| {
                                Error::bad_gateway(format!(
                                    "Catalog reference not found: provider {:?}, model {:?}",
                                    reference.provider, reference.model
                                ))
                            })
                    })
                    .transpose()?;
                let mut projected =
                    output.project(&entry.metadata, entry.format, base, &capabilities)?;
                if output == CatalogFormat::Codex && !entry.provider.hidden_models.is_match(id) {
                    let probe_responses_lite = capabilities.responses_lite.is_none()
                        && projected
                            .as_ref()
                            .is_some_and(|model| model["use_responses_lite"] == true);
                    let probe_tool_search = capabilities.tool_search.is_none()
                        && projected
                            .as_ref()
                            .is_some_and(|model| model["supports_search_tool"] == true);
                    if probe_responses_lite || probe_tool_search {
                        let empty = HeaderMap::new();
                        let headers = if Arc::ptr_eq(&entry.provider, origin) {
                            incoming_headers
                        } else {
                            &empty
                        };
                        let lite = async {
                            if probe_responses_lite {
                                self.probe_catalog_item(
                                    &entry.provider,
                                    origin,
                                    headers,
                                    id,
                                    serde_json::json!({
                                        "type":"additional_tools",
                                        "role":"developer",
                                        "tools":[],
                                    }),
                                )
                                .await
                            } else {
                                None
                            }
                        };
                        let search = async {
                            if probe_tool_search {
                                self.probe_catalog_item(
                                    &entry.provider,
                                    origin,
                                    headers,
                                    id,
                                    serde_json::json!({
                                        "type":"tool_search_output",
                                        "call_id":"model_router_probe",
                                        "status":"completed",
                                        "execution":"client",
                                        "tools":[],
                                    }),
                                )
                                .await
                            } else {
                                None
                            }
                        };
                        let (responses_lite, tool_search) = tokio::join!(lite, search);
                        let mut changed = false;
                        if let Some(value) = responses_lite {
                            capabilities.responses_lite = Some(value);
                            changed = true;
                        }
                        if let Some(value) = tool_search {
                            capabilities.tool_search = Some(value);
                            changed = true;
                        }
                        if changed {
                            projected = output.project(
                                &entry.metadata,
                                entry.format,
                                base,
                                &capabilities,
                            )?;
                        }
                    }
                }
                if entry.provider.hidden_models.is_match(id) {
                    return Ok(None);
                }
                Ok(projected)
            }
        });
        let models: Vec<_> = try_join_all(projections)
            .await?
            .into_iter()
            .flatten()
            .collect();
        // Validation and projection must succeed for every source before a refresh is visible.
        let mut catalog = self.catalog.write().await;
        for (id, entry) in &staged {
            validate_ownership(&self.config, &catalog.models, id, &entry.provider)?;
        }
        for entry in catalog.models.values_mut() {
            if snapshots
                .iter()
                .any(|snapshot| Arc::ptr_eq(&snapshot.provider, &entry.provider))
            {
                entry.listed = false;
            }
        }
        catalog.models.extend(staged);
        catalog.ready = true;
        drop(catalog);
        let anchor = snapshots.into_iter().next().expect("catalog has an origin");
        let mut document = if anchor.format == output {
            anchor.document
        } else {
            output.empty_catalog()
        };
        *output.models(&mut document)? = models;
        let mut headers = anchor.headers;
        for name in [
            "content-encoding",
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
        headers.insert("content-type", "application/json".parse().unwrap());
        if gzip {
            headers.insert("content-encoding", "gzip".parse().unwrap());
        }
        if !header_has_token(&headers, "vary", "accept-encoding")
            && !header_has_token(&headers, "vary", "*")
        {
            headers.append("vary", "Accept-Encoding".parse().unwrap());
        }
        let bytes = serde_json::to_vec(&document).map_err(Error::bad_gateway)?;
        let mut response = Response::new(encode_body(bytes, gzip));
        *response.headers_mut() = headers;
        Ok(response)
    }
}

fn validate_complete_catalog(document: &Value) -> Result<()> {
    if document.get("has_more").and_then(Value::as_bool) == Some(true) {
        return Err(Error::bad_gateway(
            "Catalog sources must return a complete list; pagination is not supported",
        ));
    }
    Ok(())
}

/// Catalog entries and each provider's explicit `models` table both establish
/// ownership, so a refresh cannot transfer a model to another provider.
fn validate_ownership(
    config: &Config,
    registry: &ModelRegistry,
    model: &str,
    provider: &Arc<Provider>,
) -> Result<()> {
    let claimed_elsewhere = registry
        .get(model)
        .is_some_and(|previous| !Arc::ptr_eq(&previous.provider, provider))
        || config
            .providers
            .values()
            .any(|other| !Arc::ptr_eq(other, provider) && other.models.contains_key(model));
    if claimed_elsewhere {
        return Err(Error::service_unavailable(format!(
            "Model belongs to multiple providers: {model}"
        )));
    }
    Ok(())
}
