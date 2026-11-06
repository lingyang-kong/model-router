use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

pub(crate) type Result<T> = std::result::Result<T, Error>;

mod kind {
    pub const INVALID_REQUEST: &str = "invalid_request_error";
    pub const NOT_FOUND: &str = "not_found_error";
    pub const API: &str = "api_error";
}

#[derive(Debug)]
pub(crate) struct Error(StatusCode, String);

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.1)
    }
}

impl std::error::Error for Error {}

impl Error {
    pub fn not_acceptable(message: impl Into<String>) -> Self {
        Self(StatusCode::NOT_ACCEPTABLE, message.into())
    }

    pub fn unknown_model(model: &str) -> Self {
        Self(
            StatusCode::NOT_FOUND,
            format!("No configured provider for model: {model}"),
        )
    }
    pub fn bad_request(error: impl std::fmt::Display) -> Self {
        Self(StatusCode::BAD_REQUEST, error.to_string())
    }
    pub fn service_unavailable(message: impl Into<String>) -> Self {
        Self(StatusCode::SERVICE_UNAVAILABLE, message.into())
    }
    pub fn bad_gateway(error: impl std::fmt::Display) -> Self {
        Self(StatusCode::BAD_GATEWAY, error.to_string())
    }
    pub fn not_found(message: impl Into<String>) -> Self {
        Self(StatusCode::NOT_FOUND, message.into())
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let Self(status, message) = self;
        let kind = match status {
            StatusCode::BAD_REQUEST => kind::INVALID_REQUEST,
            StatusCode::NOT_FOUND => kind::NOT_FOUND,
            _ => kind::API,
        };
        (
            status,
            Json(json!({"type":"error","error":{"type":kind,"message":message}})),
        )
            .into_response()
    }
}
