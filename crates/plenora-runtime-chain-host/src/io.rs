//! Real IO candidate through the same durable coordinator and dispatcher.
use super::{ScriptedHost, control, work};
use plenora_runtime_adapter_io::{Config, IoAdapter, registry};
use plenora_runtime_chain_store::PostgresStore;
use plenora_runtime_chains::{ChainEngine, Error, Store};
use plenora_runtime_core::{Clock, SystemClock};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
/// Trusted configuration for the real IO candidate host.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IoConfig {
    /// Operational PostgreSQL journal configuration.
    pub database: String,
    /// Authenticated sources, destinations, plans and linked discovery.
    pub adapter: Config,
}
/// Shared durable host with the IO candidate profile.
pub type IoHost<S = PostgresStore> = ScriptedHost<S>;
impl ScriptedHost {
    /// Compose real IO without connecting to the journal.
    /// # Errors
    /// Rejects invalid configuration and discovery.
    pub fn io(config: IoConfig) -> Result<Self, Error> {
        let store = Arc::new(PostgresStore::new(&config.database).map_err(|_| Error::Invalid)?);
        Self::with_io(config.adapter, store, Arc::new(SystemClock))
    }
}
impl<S: Store + plenora_runtime_boundary::DurableExecutionStore> ScriptedHost<S> {
    /// Compose a real IO candidate adapter and the same durable control/step path.
    /// Store and clock injection keep journal qualification separate from real IO operations.
    /// # Errors
    /// Refuses invalid discovery, unconfirmed plans or unsupported schema profiles.
    pub fn with_io(config: Config, store: Arc<S>, clock: Arc<dyn Clock>) -> Result<Self, Error> {
        let tenant = config.tenant.clone();
        let registry = registry(&config.artifact_sha256, &config.ports)?;
        let adapter = Arc::new(IoAdapter::new(config, store.clone(), clock.clone())?);
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
