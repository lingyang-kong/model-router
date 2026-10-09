use indexmap::IndexMap;
use serde_json::Value;
use tokio::sync::RwLockWriteGuard;

use crate::{
    Config,
    error::{Error, Result},
    router::RouterState,
};

pub(crate) struct ModelEntry {
    pub provider: String,
    /// Manual metadata is cached; live metadata comes from each upstream refresh.
    pub metadata: Option<Value>,
}
pub(crate) type ModelRegistry = IndexMap<String, ModelEntry>;

pub(crate) fn load_manual_models(config: &Config) -> Result<ModelRegistry> {
    let mut registry = ModelRegistry::new();
    for (provider, settings) in &config.providers {
        let Some(path) = &settings.catalog_file else {
            continue;
        };
        let bytes = std::fs::read(path).map_err(Error::bad_gateway)?;
        let mut catalog: Value = serde_json::from_slice(&bytes).map_err(Error::bad_gateway)?;
        let (id_field, models) = model_array(&mut catalog)?;
        for metadata in std::mem::take(models) {
            let id = model_id(&metadata, id_field)?.to_owned();
            if let Some(previous) = registry.get(&id)
                && &previous.provider != provider
            {
                return Err(ownership_conflict(&id));
            }
            registry.insert(
                id,
                ModelEntry {
                    provider: provider.clone(),
                    metadata: Some(metadata),
                },
            );
        }
    }
    Ok(registry)
}

impl RouterState {
    pub(crate) async fn merge_catalog(&self, provider: &str, mut catalog: Value) -> Result<Value> {
        let (id_field, models) = model_array(&mut catalog)?;
        let ids = models
            .iter()
            .map(|model| model_id(model, id_field))
            .collect::<Result<Vec<_>>>()?;
        let mut registry = self.models.write().await;
        for (id, entry) in registry.iter() {
            if let Some(metadata) = &entry.metadata
                && model_id(metadata, id_field)? != id
            {
                return Err(Error::bad_gateway(format!(
                    "Manual model has an incompatible catalog identifier: {id}"
                )));
            }
        }
        for id in &ids {
            if let Some(previous) = registry.get(*id)
                && previous.provider != provider
            {
                return Err(ownership_conflict(id));
            }
        }
        for id in ids {
            if !registry.contains_key(id) {
                registry.insert(
                    id.to_owned(),
                    ModelEntry {
                        provider: provider.to_owned(),
                        metadata: None,
                    },
                );
            }
        }
        let registry = RwLockWriteGuard::downgrade(registry);
        models.extend(registry.values().filter_map(|entry| entry.metadata.clone()));
        Ok(catalog)
    }
}

fn model_array(catalog: &mut Value) -> Result<(&'static str, &mut Vec<Value>)> {
    let (key, id_field) = if catalog.get("models").is_some_and(Value::is_array) {
        ("models", "slug")
    } else {
        ("data", "id")
    };
    catalog
        .get_mut(key)
        .and_then(Value::as_array_mut)
        .map(|models| (id_field, models))
        .ok_or_else(|| Error::bad_gateway("Catalog needs a models or data array"))
}

fn model_id<'a>(metadata: &'a Value, preferred: &str) -> Result<&'a str> {
    let alternate = if preferred == "id" { "slug" } else { "id" };
    metadata
        .get(preferred)
        .or_else(|| metadata.get(alternate))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| Error::bad_gateway("Catalog models must have nonempty slug or id strings"))
}

fn ownership_conflict(model: &str) -> Error {
    Error::service_unavailable(format!("Model belongs to multiple providers: {model}"))
}
