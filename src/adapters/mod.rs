mod codex;

use axum::http::HeaderMap;
use serde::Deserialize;
use serde_json::Value;

use crate::error::{Error, Result};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CatalogFormat {
    Codex,
    Openai,
    Anthropic,
}

struct CatalogLayout {
    pub models: &'static str,
    pub id: &'static str,
}

impl CatalogFormat {
    fn layout(self) -> &'static CatalogLayout {
        match self {
            Self::Codex => &codex::CATALOG,
            Self::Openai | Self::Anthropic => &CatalogLayout {
                models: "data",
                id: "id",
            },
        }
    }

    pub fn models(self, catalog: &mut Value) -> Result<&mut Vec<Value>> {
        let key = self.layout().models;
        catalog
            .get_mut(key)
            .and_then(Value::as_array_mut)
            .ok_or_else(|| Error::bad_gateway(format!("{self:?} catalog needs a {key} array")))
    }

    pub fn model_id(self, model: &Value) -> Result<&str> {
        let key = self.layout().id;
        model
            .get(key)
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                Error::bad_gateway(format!(
                    "{self:?} catalog models need a nonempty {key} string"
                ))
            })
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Client {
    Codex,
}

impl Client {
    pub fn from_headers(headers: &HeaderMap) -> Result<Option<Self>> {
        let mut values = headers.get_all("x-router-client").iter();
        let Some(value) = values.next() else {
            return Ok(None);
        };
        if values.any(|other| other != value) {
            return Err(Error::bad_request("Conflicting x-router-client headers"));
        }
        match value.as_bytes() {
            b"codex" => Ok(Some(Self::Codex)),
            _ => Err(Error::bad_request("Unknown x-router-client")),
        }
    }

    pub fn catalog_format(self) -> CatalogFormat {
        match self {
            Self::Codex => CatalogFormat::Codex,
        }
    }

    pub fn prepare_catalog(self, models: &mut [Value]) {
        match self {
            Self::Codex => codex::prepare_catalog(models),
        }
    }

    pub fn model_hint<'a>(self, headers: &'a HeaderMap, hint: &mut ModelHint<'a>) -> Result<()> {
        match self {
            Self::Codex => codex::model_hint(headers, hint),
        }
    }
}

#[derive(Default)]
pub(crate) struct ModelHint<'a>(Option<&'a str>);

impl<'a> ModelHint<'a> {
    pub fn set(&mut self, value: &'a str) -> Result<()> {
        if value.is_empty() || self.0.is_some_and(|previous| previous != value) {
            return Err(Error::bad_request(
                "Empty or conflicting WebSocket model hints",
            ));
        }
        self.0 = Some(value);
        Ok(())
    }

    pub fn require(self) -> Result<&'a str> {
        self.0
            .ok_or_else(|| Error::bad_request("WebSocket model routes require a model hint"))
    }
}
