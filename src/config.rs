use std::{
    collections::BTreeMap,
    env,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, bail};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Uri};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Deserializer, de::Error as _};
use url::{Host, Url};

use crate::{
    adapters::CatalogFormat,
    error::{Error, Result},
};

#[derive(Deserialize)]
#[serde(tag = "auth", rename_all = "lowercase", deny_unknown_fields)]
enum AuthMode {
    Passthrough {},
    None {},
    Bearer { env_key: String },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManualCatalog {
    pub file: PathBuf,
    pub format: CatalogFormat,
}

#[derive(Deserialize)]
pub(crate) struct Provider {
    pub base_url: Url,
    #[serde(skip)]
    pub is_loopback: bool,
    #[serde(flatten)]
    auth: AuthMode,
    pub catalog: Option<ManualCatalog>,
    #[serde(default, deserialize_with = "deserialize_hidden_models")]
    pub hidden_models: GlobSet,
    #[serde(default, deserialize_with = "deserialize_static_headers")]
    http_headers: Vec<(HeaderName, HeaderValue)>,
    #[serde(default, deserialize_with = "deserialize_headers")]
    env_http_headers: Vec<(HeaderName, String)>,
}

#[derive(Deserialize)]
struct ConfigDocument {
    providers: BTreeMap<String, Provider>,
    routes: Vec<Route>,
}

#[derive(Deserialize)]
pub(crate) struct Route {
    #[serde(default, deserialize_with = "deserialize_method")]
    method: Option<Method>,
    path: Option<String>,
    path_prefix: Option<String>,
    #[serde(default, deserialize_with = "deserialize_headers")]
    headers: Vec<(HeaderName, String)>,
    #[serde(default)]
    query: BTreeMap<String, String>,
    #[serde(flatten)]
    pub action: RouteAction,
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum RouteAction {
    Forward {
        provider: String,
    },
    Model {
        provider: Option<String>,
    },
    Catalog {
        provider: String,
        format: CatalogFormat,
    },
}

fn deserialize_hidden_models<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<GlobSet, D::Error> {
    let invalid =
        |error: globset::Error| D::Error::custom(format!("Invalid hidden_models: {error}"));
    let mut builder = GlobSetBuilder::new();
    for pattern in Vec::<String>::deserialize(deserializer)? {
        builder.add(
            GlobBuilder::new(&pattern)
                .backslash_escape(true)
                .empty_alternates(true)
                .build()
                .map_err(invalid)?,
        );
    }
    builder.build().map_err(invalid)
}

fn deserialize_method<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Method>, D::Error> {
    Option::<String>::deserialize(deserializer)?
        .map(|method| Method::from_bytes(method.as_bytes()))
        .transpose()
        .map_err(D::Error::custom)
}

fn deserialize_headers<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<(HeaderName, String)>, D::Error> {
    BTreeMap::<String, String>::deserialize(deserializer)?
        .into_iter()
        .map(|(name, value)| Ok((HeaderName::try_from(name).map_err(D::Error::custom)?, value)))
        .collect()
}

fn deserialize_static_headers<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<(HeaderName, HeaderValue)>, D::Error> {
    deserialize_headers(deserializer)?
        .into_iter()
        .map(|(name, value)| {
            Ok((
                name,
                HeaderValue::try_from(value).map_err(D::Error::custom)?,
            ))
        })
        .collect()
}

impl RouteAction {
    pub fn provider(&self) -> Option<&str> {
        match self {
            Self::Forward { provider } | Self::Catalog { provider, .. } => Some(provider),
            Self::Model { provider } => provider.as_deref(),
        }
    }
}

impl Route {
    pub fn matches(&self, method: &Method, uri: &Uri, headers: &HeaderMap) -> bool {
        self.method
            .as_ref()
            .is_none_or(|expected| expected == method)
            && self
                .path
                .as_deref()
                .is_none_or(|expected| expected == uri.path())
            && self
                .path_prefix
                .as_deref()
                .is_none_or(|prefix| uri.path().starts_with(prefix))
            && self.headers.iter().all(|(name, expected)| {
                headers
                    .get_all(name)
                    .iter()
                    .any(|value| value.as_bytes() == expected.as_bytes())
            })
            && self.query.iter().all(|(name, expected)| {
                url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                    .any(|(key, value)| &key == name && &value == expected)
            })
    }
}

pub struct Config {
    pub(crate) providers: BTreeMap<String, Arc<Provider>>,
    pub(crate) routes: Vec<Route>,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let (text, root) = match env::var_os("MODEL_ROUTER_CONFIG") {
            Some(path) => {
                let path = env::current_dir()?.join(path);
                let text =
                    std::fs::read_to_string(&path).context("Cannot read MODEL_ROUTER_CONFIG")?;
                (text, path.parent().unwrap().to_owned())
            }
            None => (
                env::var("MODEL_ROUTER_CONFIG_TOML")
                    .context("Set MODEL_ROUTER_CONFIG or MODEL_ROUTER_CONFIG_TOML")?,
                env::current_dir()?,
            ),
        };
        Self::from_toml(&text, &root)
    }

    pub fn from_toml(text: &str, root: &Path) -> anyhow::Result<Self> {
        let ConfigDocument {
            mut providers,
            routes,
        } = toml::from_str(text).context("Invalid router configuration")?;
        if providers.is_empty() {
            bail!("Configure at least one provider");
        }
        for (name, provider) in &mut providers {
            if name.is_empty() || !matches!(provider.base_url.scheme(), "http" | "https") {
                bail!("Providers must have names and HTTP or HTTPS URLs");
            }
            if provider.base_url.query().is_some() {
                bail!(
                    "Provider base URLs must not contain queries; clients supply query parameters"
                );
            }
            provider.is_loopback = match provider.base_url.host() {
                Some(Host::Domain("localhost")) => true,
                Some(Host::Ipv4(ip)) => ip.is_loopback(),
                Some(Host::Ipv6(ip)) => ip.is_loopback(),
                _ => false,
            };
            if let AuthMode::Bearer { env_key } = &provider.auth
                && env_key.is_empty()
            {
                bail!("Bearer auth requires env_key naming the API key environment variable");
            }
            if let Some(catalog) = &mut provider.catalog {
                let path = &mut catalog.file;
                *path = if let Ok(relative) = path.strip_prefix("~") {
                    PathBuf::from(env::var_os("HOME").context("HOME is not set")?).join(relative)
                } else {
                    root.join(&*path)
                };
            }
        }
        if routes.is_empty() {
            bail!("Configure at least one routing rule");
        }
        let mut catalog_provider = None;
        for route in &routes {
            if let Some(name) = route.action.provider()
                && !providers.contains_key(name)
            {
                bail!("Unknown provider in routing rule: {name}");
            }
            if route.path.is_some() && route.path_prefix.is_some() {
                bail!("A route must use either path or path_prefix");
            }
            for path in route.path.iter().chain(route.path_prefix.iter()) {
                if !path.starts_with('/') {
                    bail!("Route paths must start with /");
                }
            }
            if let RouteAction::Catalog { provider, .. } = &route.action {
                if catalog_provider.is_some_and(|previous| previous != provider) {
                    bail!("Catalog rules must use one live provider");
                }
                if providers[provider].catalog.is_some() {
                    bail!("Live catalog providers must not have a manual catalog");
                }
                catalog_provider = Some(provider);
            }
        }
        Ok(Self {
            providers: providers
                .into_iter()
                .map(|(name, provider)| (name, Arc::new(provider)))
                .collect(),
            routes,
        })
    }

    pub(crate) fn provider(&self, name: &str) -> Result<&Arc<Provider>> {
        self.providers
            .get(name)
            .ok_or_else(|| Error::bad_request(format!("Unknown provider: {name}")))
    }

    pub(crate) fn route(&self, method: &Method, uri: &Uri, headers: &HeaderMap) -> Result<&Route> {
        self.routes
            .iter()
            .find(|rule| rule.matches(method, uri, headers))
            .ok_or_else(|| Error::not_found("No routing rule matches this request"))
    }
}

impl Provider {
    pub(crate) fn validate_credential_scope(
        self: &Arc<Self>,
        origin: Option<&Arc<Self>>,
    ) -> Result<()> {
        if matches!(self.auth, AuthMode::Passthrough {})
            && origin.is_some_and(|origin| !Arc::ptr_eq(origin, self))
        {
            return Err(Error::bad_request(
                "Passthrough credentials require the matching provider; configure destination credentials for cross-provider routing",
            ));
        }
        Ok(())
    }

    pub(crate) fn upstream_url(&self, incoming: &Uri) -> Result<Url> {
        let mut target = self.base_url.clone();
        let path = format!("{}{}", target.path().trim_end_matches('/'), incoming.path());
        target.set_path(&path);
        // Reject normalization that could bypass the matched route.
        if target.path() != path {
            return Err(Error::bad_request(
                "Request path requires URL normalization",
            ));
        }
        target.set_query(incoming.query());
        Ok(target)
    }

    pub(crate) fn request_headers(&self, incoming: &HeaderMap) -> Result<HeaderMap> {
        let mut headers = incoming.clone();
        sanitize_headers(&mut headers);
        if header_has_token(incoming, "te", "trailers") {
            headers.insert("te", HeaderValue::from_static("trailers"));
            headers.insert("connection", HeaderValue::from_static("TE"));
        }
        if !matches!(self.auth, AuthMode::Passthrough {}) {
            let removed: Vec<_> = headers
                .keys()
                .filter(|name| {
                    let name = name.as_str();
                    matches!(
                        name,
                        "authorization"
                            | "cookie"
                            | "openai-organization"
                            | "openai-project"
                            | "x-api-key"
                            | "api-key"
                    ) || name.starts_with("chatgpt-")
                        || name.starts_with("x-openai-")
                })
                .cloned()
                .collect();
            for name in removed {
                headers.remove(name);
            }
        }
        if let AuthMode::Bearer { env_key } = &self.auth {
            let key = required_env_var(env_key)?;
            headers.insert(
                "authorization",
                HeaderValue::try_from(format!("Bearer {key}"))
                    .map_err(|_| Error::service_unavailable("Invalid provider API credentials"))?,
            );
        }
        for (name, value) in &self.http_headers {
            headers.insert(name.clone(), value.clone());
        }
        for (name, variable) in &self.env_http_headers {
            headers.insert(
                name.clone(),
                HeaderValue::try_from(required_env_var(variable)?).map_err(|_| {
                    Error::service_unavailable("Invalid provider header credentials")
                })?,
            );
        }
        Ok(headers)
    }
}

fn required_env_var(variable: &str) -> Result<String> {
    env::var(variable)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::service_unavailable("Provider credentials are not configured"))
}

pub(crate) fn header_has_token(headers: &HeaderMap, name: &str, token: &str) -> bool {
    headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|value| value.trim().eq_ignore_ascii_case(token))
}

pub(crate) fn sanitize_headers(headers: &mut HeaderMap) {
    let connections: Vec<_> = headers.get_all("connection").iter().cloned().collect();
    for connection in connections.iter().filter_map(|value| value.to_str().ok()) {
        for name in connection.split(',').map(str::trim) {
            if let Ok(name) = HeaderName::try_from(name) {
                headers.remove(name);
            }
        }
    }
    for name in [
        "host",
        "connection",
        "keep-alive",
        "content-length",
        "transfer-encoding",
        "upgrade",
        "te",
        "proxy-authorization",
        "proxy-authenticate",
        "proxy-connection",
    ] {
        headers.remove(name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hidden_models_compile_globs_with_alternatives_and_escapes() {
        let config = Config::from_toml(
            r#"
            [providers.test]
            base_url = "http://localhost:30000"
            auth = "none"
            hidden_models = [
                "gpt-{5.6-*,6-sol}", "vendor/model-?", "variant-[0-2]",
                "letter-[!0-9]", "model{,-preview}", 'literal-\*',
                "nested-{a,{b,c}}", "模型/*雪"
            ]
            [[routes]]
            action = "forward"
            provider = "test"
            "#,
            Path::new("."),
        )
        .unwrap();
        let patterns = &config.providers["test"].hidden_models;
        for (id, expected) in [
            ("gpt-5.6-sol", true),
            ("gpt-5.6-", true),
            ("gpt-5.6-vendor/model", true),
            ("prefix-gpt-5.6-sol", false),
            ("gpt-6-sol", true),
            ("gpt-6-sol-next", false),
            ("GPT-6-sol", false),
            ("vendor/model-a", true),
            ("vendor/model-ab", false),
            ("variant-2", true),
            ("variant-3", false),
            ("letter-x", true),
            ("letter-2", false),
            ("model", true),
            ("model-preview", true),
            ("model-other", false),
            ("literal-*", true),
            ("literal-x", false),
            ("nested-c", true),
            ("nested-d", false),
            ("模型/小雪", true),
        ] {
            assert_eq!(patterns.is_match(id), expected, "{id}");
        }
    }

    #[test]
    fn malformed_hidden_model_patterns_fail_configuration_loading() {
        for pattern in ["[", "[z-a]", "model-{a,b", "model-}", "model-\\"] {
            let text = format!(
                r#"
                [providers.test]
                base_url = "http://localhost:30000"
                auth = "none"
                hidden_models = [{}]
                [[routes]]
                action = "forward"
                provider = "test"
                "#,
                toml::Value::String(pattern.to_owned())
            );
            let error = Config::from_toml(&text, Path::new(".")).err().unwrap();
            assert!(format!("{error:#}").contains("Invalid hidden_models"));
        }
    }
}
