use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

pub(crate) type Result<T> = std::result::Result<T, Error>;

pub(crate) struct Error(StatusCode, String);

impl Error {
    pub fn unknown_model(model: &str) -> Self {
        Self(
            StatusCode::NOT_FOUND,
            format!("No configured provider for model: {model}"),
        )
    }
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self(StatusCode::BAD_REQUEST, message.into())
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

    pub fn details(&self) -> (StatusCode, &str) {
        (self.0, &self.1)
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let Self(status, message) = self;
        let kind = match status.as_u16() {
            400 => "invalid_request_error",
            404 => "not_found_error",
            _ => "api_error",
        };
        (
            status,
            Json(json!({"type":"error","error":{"type":kind,"message":message}})),
        )
            .into_response()
    }
}
