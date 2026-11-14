//! Real DATA candidate through the same durable coordinator and dispatcher.
use super::{ScriptedHost, control, work};
use plenora_runtime_adapter_data::{Config, DataAdapter, registry};
use plenora_runtime_chain_store::PostgresStore;
use plenora_runtime_chains::{ChainEngine, Error, Store};
use plenora_runtime_core::{Clock, SystemClock};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
/// Trusted configuration for the real DATA candidate host.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataConfig {
    /// Operational PostgreSQL journal configuration.
    pub database: String,
    /// Authenticated sources, destinations, plans and linked discovery.
    pub adapter: Config,
}
/// Shared durable host with the DATA candidate profile.
pub type DataHost<S = PostgresStore> = ScriptedHost<S>;
impl ScriptedHost {
    /// Compose real DATA without connecting to the journal.
    /// # Errors
    /// Rejects invalid configuration and discovery.
    pub fn data(config: DataConfig) -> Result<Self, Error> {
        let store = Arc::new(PostgresStore::new(&config.database).map_err(|_| Error::Invalid)?);
        Self::with_data(config.adapter, store, Arc::new(SystemClock))
    }
}
impl<S: Store + plenora_runtime_boundary::DurableExecutionStore> ScriptedHost<S> {
    /// Compose a real DATA candidate adapter and the same durable control/step path.
    /// Store and clock injection keep journal qualification separate from real DATA operations.
    /// # Errors
    /// Refuses invalid discovery, unconfirmed plans or unsupported schema profiles.
    pub fn with_data(config: Config, store: Arc<S>, clock: Arc<dyn Clock>) -> Result<Self, Error> {
        let tenant = config.tenant.clone();
        let registry = registry(&config.artifact_sha256, &config.ports)?;
        let adapter = Arc::new(DataAdapter::new(config, store.clone(), clock.clone())?);
        let engine = Arc::new(ChainEngine::new(
            store.clone(),
            adapter,
            registry,
            clock.clone(),
            &tenant,
        )?);
        let runtime = control::runtime(store.clone(), engine.clone(), clock, &tenant)?;
        Ok(Self {
            runtime,
            store,
            worker: work::Worker(engine),
        })
    }
}
