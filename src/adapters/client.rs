use axum::http::HeaderMap;
use serde::Deserialize;

use super::{catalog::CatalogFormat, codex, websocket};
use crate::error::Result;
use crate::protocol::header;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClientKind {
    Codex,
    Openai,
    Anthropic,
}

impl ClientKind {
    pub fn from_headers(headers: &HeaderMap) -> Self {
        match (
            Client::from_headers(headers),
            headers.contains_key(header::ANTHROPIC_VERSION),
        ) {
            (Some(Client::Codex), _) => Self::Codex,
            (None, true) => Self::Anthropic,
            (None, false) => Self::Openai,
        }
    }

    pub fn catalog_format(self) -> CatalogFormat {
        match self {
            Self::Codex => CatalogFormat::Codex,
            Self::Openai => CatalogFormat::Openai,
            Self::Anthropic => CatalogFormat::Anthropic,
        }
    }

    pub fn adapter(self) -> Option<Client> {
        match self {
            Self::Codex => Some(Client::Codex),
            Self::Openai | Self::Anthropic => None,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Client {
    Codex,
}

impl Client {
    pub fn from_headers(headers: &HeaderMap) -> Option<Self> {
        let originator = headers.get(header::ORIGINATOR)?.to_str().ok()?;
        codex::matches_originator(originator).then_some(Self::Codex)
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

    pub fn model_hint(self, headers: &HeaderMap) -> Result<Option<&str>> {
        match self {
            Self::Codex => codex::model_hint(headers),
        }
    }

    pub fn override_model_hint(
        self,
        headers: &mut HeaderMap,
        overrides: &crate::model_overrides::ModelOverrides,
    ) -> Result<()> {
        match self {
            Self::Codex => codex::override_model_hint(headers, overrides),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_kind_uses_protocol_headers_with_openai_as_the_default() {
        let mut headers = HeaderMap::new();
        assert_eq!(ClientKind::from_headers(&headers), ClientKind::Openai);
        headers.insert("originator", "router_test_host".parse().unwrap());
        assert_eq!(ClientKind::from_headers(&headers), ClientKind::Openai);
        headers.insert("anthropic-version", "2023-06-01".parse().unwrap());
        assert_eq!(ClientKind::from_headers(&headers), ClientKind::Anthropic);
        headers.insert("originator", "codex_cli_rs".parse().unwrap());
        assert_eq!(ClientKind::from_headers(&headers), ClientKind::Codex);
        for (kind, format) in [
            (ClientKind::Openai, CatalogFormat::Openai),
            (ClientKind::Anthropic, CatalogFormat::Anthropic),
            (ClientKind::Codex, CatalogFormat::Codex),
        ] {
            assert_eq!(kind.catalog_format(), format);
        }
    }

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
        for value in ["", "claude-code", "router_test_host"] {
            let mut headers = HeaderMap::new();
            headers.insert("originator", value.parse().unwrap());
            assert!(Client::from_headers(&headers).is_none(), "{value}");
        }
        assert!(Client::from_headers(&HeaderMap::new()).is_none());
    }
}
