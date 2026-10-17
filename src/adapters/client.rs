use axum::http::HeaderMap;

use super::{catalog::CatalogFormat, codex, websocket};
use crate::error::{Error, Result};

#[derive(Clone, Copy)]
pub(crate) enum Client {
    Codex,
}

impl Client {
    pub fn from_headers(headers: &HeaderMap) -> Option<Self> {
        let originator = headers.get("originator")?.to_str().ok()?;
        codex::matches_originator(originator).then_some(Self::Codex)
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

    pub fn optional(self) -> Option<&'a str> {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn originator_value_identifies_codex_not_header_presence() {
        for value in [
            "codex_cli_rs",
            "codex-tui",
            "codex_vscode",
            "codex_atlas",
            "codex_chatgpt_desktop",
            "codex_work_desktop",
            "Codex Desktop",
            "my-CODEX-host",
            "codex_unrelated",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert("originator", value.parse().unwrap());
            assert!(
                matches!(Client::from_headers(&headers), Some(Client::Codex)),
                "{value}"
            );
        }
        for value in ["", "opencode", "claude-code", "router_test_host"] {
            let mut headers = HeaderMap::new();
            headers.insert("originator", value.parse().unwrap());
            assert!(Client::from_headers(&headers).is_none(), "{value}");
        }
        assert!(Client::from_headers(&HeaderMap::new()).is_none());
    }
}
