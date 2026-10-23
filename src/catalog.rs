use std::{io, sync::Arc, time::Duration};

use axum::{
    body::Body,
    extract::Request,
    http::{HeaderMap, Method, StatusCode, request::Parts},
    response::{IntoResponse, Response},
};
use futures_util::{StreamExt, TryStreamExt, future::try_join_all, stream::FuturesOrdered};
use indexmap::IndexMap;
use serde_json::{Map, Value};
use tokio_util::io::StreamReader;

use crate::{
    Config,
    adapters::CatalogFormat,
    capabilities::{self, Discovery},
    compression::{accepts_gzip, decode_body, encode_body},
    config::{Provider, header_has_token},
    error::{Error, Result},
    router::{RouterState, stream_response},
};

const CATALOG_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) struct ModelEntry {
    pub provider: Arc<Provider>,
    pub format: CatalogFormat,
    /// Source-format metadata after local settings, before output projection.
    pub metadata: Value,
    /// Unlisted IDs remain routable, including retired or unavailable models.
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
        origin: Option<&Arc<Provider>>,
        incoming: Option<&Parts>,
    ) -> Result<CatalogReply> {
        let discovery_headers = incoming
            .map(|parts| parts.headers.clone())
            .unwrap_or_default();
        let source = provider
            .catalog
            .as_ref()
            .expect("configured catalog source");
        let format = source.format;
        if source.file.is_some() {
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

        let path = source.path.as_deref().expect("remote catalog path");
        let uri = match incoming.and_then(|parts| parts.uri.query()) {
            Some(query) => format!("{path}?{query}"),
            None => path.to_owned(),
        };
        let (mut parts, ()) = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(())
            .map_err(Error::bad_request)?
            .into_parts();
        if let Some(incoming) = incoming {
            parts.headers = incoming.headers.clone();
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
        parts.headers.remove("if-none-match");
        parts.headers.remove("if-modified-since");
        let (client, mut request) = self.prepare_request(
            provider,
            incoming.and(origin),
            &parts,
            reqwest::Body::default(),
        )?;
        // Bound catalog transport through the final body byte without limiting inference.
        *request.timeout_mut() = Some(CATALOG_TIMEOUT);
        let response = match client.execute(request).await {
            Ok(response) => response,
            Err(error) => {
                return Ok(CatalogReply::Rejected(
                    Error::bad_gateway(error).into_response(),
                ));
            }
        };
        if response.status() != StatusCode::OK {
            // Preserve the rejection even if other sources outlast this request's deadline.
            let (parts, body) = stream_response(response).into_parts();
            let body = match http_body_util::BodyExt::collect(body).await {
                Ok(body) => Body::new(body),
                Err(error) => {
                    return Ok(CatalogReply::Rejected(
                        Error::bad_gateway(error).into_response(),
                    ));
                }
            };
            return Ok(CatalogReply::Rejected(Response::from_parts(parts, body)));
        }
        let (parts, body) = stream_response(response).into_parts();
        let reader = StreamReader::new(
            body.into_data_stream()
                .map_err(|error| io::Error::new(io::ErrorKind::ConnectionAborted, error)),
        );
        let bytes = match decode_body(reader, &parts.headers).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::ConnectionAborted => {
                return Ok(CatalogReply::Rejected(
                    Error::bad_gateway(error).into_response(),
                ));
            }
            Err(error) => return Err(Error::bad_gateway(error)),
        };
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
        origin: Option<&Arc<Provider>>,
        output: CatalogFormat,
    ) -> Result<Response> {
        let gzip = accepts_gzip(&parts.headers)?;
        let incoming_headers = parts.headers.clone();
        // Client identity owns credentials; every configured catalog is a peer.
        let mut requests = self
            .config
            .providers
            .values()
            .filter(|provider| provider.catalog.is_some())
            .map(|provider| {
                let incoming = origin
                    .is_some_and(|origin| Arc::ptr_eq(provider, origin))
                    .then_some(&parts);
                async move {
                    (
                        provider,
                        self.fetch_catalog(provider, origin, incoming).await,
                    )
                }
            })
            .collect::<FuturesOrdered<_>>();
        let mut snapshots = Vec::new();
        let mut unavailable = Vec::new();
        let mut rejection = None;
        while let Some((provider, reply)) = requests.next().await {
            match reply? {
                CatalogReply::Catalog(snapshot) => snapshots.push(*snapshot),
                CatalogReply::Rejected(response) => {
                    unavailable.push(provider);
                    if rejection.is_none()
                        || origin.is_some_and(|origin| Arc::ptr_eq(provider, origin))
                    {
                        rejection = Some(response);
                    }
                }
            }
        }
        if snapshots.is_empty() {
            return Ok(rejection.expect("configured catalog sources failed"));
        }
        // References resolve against this refresh, never stale routing metadata.
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
        for (id, entry) in &mut staged {
            let reference = entry
                .provider
                .models
                .get(id)
                .and_then(|settings| settings.extends.as_ref())
                .or(entry.provider.defaults.extends.as_ref());
            if let Some(reference) = reference {
                let provider = self.config.provider(&reference.provider)?;
                // Keep dependent IDs routable, but do not advertise incomplete
                // inherited metadata to Codex or its subagent listing.
                entry.listed = !unavailable
                    .iter()
                    .any(|source| Arc::ptr_eq(source, provider));
            }
        }
        let projections = staged.iter().map(|(id, entry)| {
            let snapshots = &snapshots;
            let staged = &staged;
            let incoming_headers = &incoming_headers;
            async move {
                if !entry.listed && output == CatalogFormat::Codex {
                    return Ok(None);
                }
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
                // Generic catalogs can describe the available model directly,
                // even when its optional inheritance source is unavailable.
                let base = reference
                    .filter(|_| entry.listed)
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
                let provider_name = self
                    .config
                    .providers
                    .iter()
                    .find(|(_, provider)| Arc::ptr_eq(provider, &entry.provider))
                    .map(|(name, _)| name.as_str())
                    .expect("configured model provider");
                let mut projected = output.project(
                    &entry.metadata,
                    provider_name,
                    entry.format,
                    base,
                    &capabilities,
                )?;
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
                        let headers =
                            if origin.is_some_and(|origin| Arc::ptr_eq(&entry.provider, origin)) {
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
                                provider_name,
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
        // Validate successful sources together before publishing the partial refresh.
        let mut catalog = self.catalog.write().await;
        for (id, entry) in &staged {
            validate_ownership(&self.config, &catalog.models, id, &entry.provider)?;
        }
        for entry in catalog.models.values_mut() {
            if origin.is_some_and(|origin| Arc::ptr_eq(&entry.provider, origin))
                || entry.provider.catalog.is_some()
            {
                entry.listed = false;
            }
        }
        catalog.models.extend(staged);
        catalog.ready = true;
        drop(catalog);
        // Choose a matching remote source in provider-name order. File sources
        // supply rows only; credentials never determine the response envelope.
        let matching = self.config.providers.values().find_map(|provider| {
            snapshots.iter().position(|snapshot| {
                Arc::ptr_eq(&snapshot.provider, provider)
                    && snapshot.format == output
                    && !provider
                        .catalog
                        .as_ref()
                        .is_some_and(|source| source.file.is_some())
            })
        });
        let (mut document, mut headers) = match matching {
            Some(index) => {
                let snapshot = snapshots.swap_remove(index);
                (snapshot.document, snapshot.headers)
            }
            None => (output.empty_catalog(), HeaderMap::new()),
        };
        if output == CatalogFormat::Anthropic {
            document["first_id"] = models
                .first()
                .map_or(Value::Null, |model| model["id"].clone());
            document["last_id"] = models
                .last()
                .map_or(Value::Null, |model| model["id"].clone());
            document["has_more"] = Value::Bool(false);
        }
        *output.models(&mut document)? = models;
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
        for name in ["Accept-Encoding", "Originator", "Anthropic-Version"] {
            if !header_has_token(&headers, "vary", name) && !header_has_token(&headers, "vary", "*")
            {
                headers.append("vary", name.parse().unwrap());
            }
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
