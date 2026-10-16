use axum::http::HeaderMap;

use super::{catalog::CatalogFormat, codex, websocket};
use crate::error::{Error, Result};

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

    pub async fn forward_model(
        self,
        state: &std::sync::Arc<crate::router::RouterState>,
        parts: axum::http::request::Parts,
        body: axum::body::Body,
        origin: Option<&std::sync::Arc<crate::config::Provider>>,
    ) -> Result<axum::response::Response> {
        match self {
            Self::Codex => codex::forward(state, parts, body, origin).await,
        }
    }

    pub fn adapt_websocket_response(
        self,
        message: tokio_tungstenite::tungstenite::Message,
    ) -> tokio_tungstenite::tungstenite::Message {
        match self {
            Self::Codex => codex::adapt_websocket_response(message),
        }
    }

    pub async fn websocket_request(
        self,
        message: tokio_tungstenite::tungstenite::Message,
        context: &websocket::WebSocketContext,
    ) -> websocket::WebSocketAction {
        match self {
            Self::Codex => codex::websocket_request(message, context).await,
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
