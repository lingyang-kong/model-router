use axum::http::HeaderMap;
use serde_json::Value;

use super::{CatalogLayout, ModelHint};
use crate::error::{Error, Result};

pub(super) const CATALOG: CatalogLayout = CatalogLayout {
    models: "models",
    id: "slug",
};

pub(super) fn prepare_catalog(models: &mut [Value]) {
    for model in models {
        model["multi_agent_version"] = Value::String("v1".into());
    }
}

pub(super) fn model_hint<'a>(headers: &'a HeaderMap, hint: &mut ModelHint<'a>) -> Result<()> {
    for value in headers.get_all("x-codex-routing-hint") {
        let value = value
            .to_str()
            .map_err(|_| Error::bad_request("Invalid Codex routing hint"))?;
        for field in value.split(';') {
            if let Some(value) = field.trim().strip_prefix("model=") {
                hint.set(value)?;
            }
        }
    }
    Ok(())
}
