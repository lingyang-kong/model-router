use std::sync::Arc;

use indexmap::IndexMap;
use serde_json::Value;
use tokio::sync::RwLockWriteGuard;

use crate::{
    Config,
    adapters::CatalogFormat,
    config::Provider,
    error::{Error, Result},
    router::RouterState,
};

pub(crate) struct ModelEntry {
    pub provider: Arc<Provider>,
    /// Manual metadata is cached; live metadata comes from each upstream refresh.
    pub metadata: Option<Value>,
}
pub(crate) type ModelRegistry = IndexMap<String, ModelEntry>;

pub(crate) fn load_manual_models(config: &Config) -> Result<ModelRegistry> {
    let mut registry = ModelRegistry::new();
    for provider in config.providers.values() {
        let Some(settings) = &provider.catalog else {
            continue;
        };
        let bytes = std::fs::read(&settings.file).map_err(Error::bad_gateway)?;
        let mut catalog: Value = serde_json::from_slice(&bytes).map_err(Error::bad_gateway)?;
        let format = settings.format;
        let models = format.models(&mut catalog)?;
        for metadata in std::mem::take(models) {
            let id = format.model_id(&metadata)?.to_owned();
            validate_ownership(&registry, &id, provider)?;
            registry.insert(
                id,
                ModelEntry {
                    provider: Arc::clone(provider),
                    metadata: Some(metadata),
                },
            );
        }
    }
    Ok(registry)
}

impl RouterState {
    pub(crate) async fn merge_catalog(
        &self,
        provider: &Arc<Provider>,
        models: &mut Vec<Value>,
        format: CatalogFormat,
    ) -> Result<()> {
        let ids = models
            .iter()
            .map(|model| format.model_id(model))
            .collect::<Result<Vec<_>>>()?;
        let mut registry = self.models.write().await;
        for id in &ids {
            validate_ownership(&registry, id, provider)?;
        }
        for id in ids {
            if !registry.contains_key(id) {
                registry.insert(
                    id.to_owned(),
                    ModelEntry {
                        provider: Arc::clone(provider),
                        metadata: None,
                    },
                );
            }
        }
        let registry = RwLockWriteGuard::downgrade(registry);
        models.extend(
            registry
                .values()
                .filter(|entry| {
                    entry
                        .provider
                        .catalog
                        .as_ref()
                        .is_some_and(|catalog| catalog.format == format)
                })
                .filter_map(|entry| entry.metadata.clone()),
        );
        // Hiding affects discovery, not ownership checks or direct model routing.
        models.retain(|model| {
            format
                .model_id(model)
                .is_ok_and(|id| !registry[id].provider.hidden_models.is_match(id))
        });
        Ok(())
    }
}

fn validate_ownership(
    registry: &ModelRegistry,
    model: &str,
    provider: &Arc<Provider>,
) -> Result<()> {
    if let Some(previous) = registry.get(model)
        && !Arc::ptr_eq(&previous.provider, provider)
    {
        return Err(Error::service_unavailable(format!(
            "Model belongs to multiple providers: {model}"
        )));
    }
    Ok(())
}
