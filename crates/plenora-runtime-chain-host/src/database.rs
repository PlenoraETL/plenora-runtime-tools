//! Explicit real database profile over the same durable host and dispatcher.
use super::{ScriptedHost, control, work};
use plenora_runtime_adapter_database::{Config, Connection, DatabaseAdapter, postgres, registry};
use plenora_runtime_chain_store::PostgresStore;
use plenora_runtime_chains::{ChainEngine, Error, Store};
use plenora_runtime_core::{Clock, SystemClock};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

/// A trusted PostgreSQL connection; excluded from Debug and error messages.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConnectionConfig {
    /// Opaque destination reference authorized by a complete plan.
    pub destination: String,
    /// Host-resolved connection secret, never accepted from chain parameters.
    pub secret: String,
    /// Explicit deployment opt-in to plaintext (for example ephemeral loopback CI).
    #[serde(default)]
    pub allow_plaintext: bool,
}
/// Real host configuration, distinct from scripted qualification fixtures.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    /// Operational journal connection, distinct from ETL destinations.
    pub database: String,
    /// Authenticated plans, immutable artifacts and actual schema requirements.
    pub adapter: Config,
    /// Credential-reference to a real PostgreSQL connection with required TLS.
    pub connections: BTreeMap<String, DatabaseConnectionConfig>,
}
/// Shared host with a real adapter, constructed with `database` or `with_database`.
pub type DatabaseHost<S = PostgresStore> = ScriptedHost<S>;

impl ScriptedHost {
    /// Compose the real PostgreSQL adapter without opening a connection.
    /// # Errors
    /// Rejects invalid plans, discovery, credentials or journal configuration.
    pub fn database(config: DatabaseConfig) -> Result<Self, Error> {
        let store = Arc::new(PostgresStore::new(&config.database).map_err(|_| Error::Invalid)?);
        let connections = config
            .connections
            .into_iter()
            .map(|(key, connection)| {
                (
                    key,
                    (if connection.allow_plaintext {
                        plenora_runtime_adapter_database::postgres_plaintext
                    } else {
                        postgres
                    })(
                        connection.destination,
                        plenora_runtime_adapter_database::SecretString::new(connection.secret),
                    ),
                )
            })
            .collect();
        Self::with_database(config.adapter, connections, store, Arc::new(SystemClock))
    }
}
impl<S: Store + plenora_runtime_boundary::DurableExecutionStore> ScriptedHost<S> {
    /// Compose a real library adapter and the same durable control/step path.
    /// Provider injection supports explicit host TLS policies and provider qualification.
    /// # Errors
    /// Refuses invalid discovery, unconfirmed plans or unsupported schema profiles.
    pub fn with_database(
        config: Config,
        connections: BTreeMap<String, Connection>,
        store: Arc<S>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Error> {
        let tenant = config.tenant.clone();
        let registry = registry(&config.artifact_sha256, &config.ports)?;
        let adapter = Arc::new(DatabaseAdapter::new(
            config,
            connections,
            store.clone(),
            clock.clone(),
        )?);
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
