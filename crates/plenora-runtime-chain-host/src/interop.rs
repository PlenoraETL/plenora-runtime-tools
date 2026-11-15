//! A single confirmed plan, primary journal and dispatcher across real IO/DATA adapters.
use super::{DatabaseConnectionConfig, ScriptedHost, control, work};
use async_trait::async_trait;
use plenora_runtime_adapter_data::{self as data, DataAdapter};
use plenora_runtime_adapter_database::{self as database, DatabaseAdapter};
use plenora_runtime_adapter_io::{self as io, IoAdapter};
use plenora_runtime_chain_store::PostgresStore;
use plenora_runtime_chains::{
    Adapter, ChainEngine, Error, Invocation, Store,
    contracts::{Plan, Registry, RegistryBoundariesItem, StepResult, parse_registry},
};
use plenora_runtime_core::{Clock, SystemClock};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

/// Trusted IO resources and independently declared Arrow barriers.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InteropIo {
    /// IO read/write Arrow expectations.
    pub ports: io::Ports,
    /// Content-addressed source snapshots.
    pub sources: BTreeMap<String, io::Source>,
    /// Host-authorized publication targets.
    pub destinations: BTreeMap<String, io::Destination>,
    /// Absolute existing private staging parent.
    pub staging_directory: String,
}
/// Optional real PostgreSQL source profile, with host-owned credentials and ports.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InteropDatabase {
    /// Exact Arrow expectations from the linked database adapter.
    pub ports: database::Ports,
    /// Trusted credential references; secrets never enter plans or errors.
    pub connections: BTreeMap<String, DatabaseConnectionConfig>,
}
/// One principal confirms the entire immutable plan, including each step/destination.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InteropProfile {
    /// Authenticated tenant, never taken from request parameters.
    pub tenant: String,
    /// Actual deployed host identity used by both component pins.
    pub artifact_sha256: String,
    /// Complete confirmed plans; no per-component projection or second consent.
    pub authorized_plans: Vec<String>,
    /// Immutable external resources and parameter documents.
    pub artifacts: BTreeMap<String, Vec<u8>>,
    /// Real IO resources.
    pub io: InteropIo,
    /// Native DATA input/output names and independent schema barriers.
    pub data_ports: data::Ports,
    /// Optional published database provider for PG -> DATA -> IO; absent keeps IO-only discovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<InteropDatabase>,
}
/// Durable real IO -> DATA -> IO host configuration.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InteropConfig {
    /// PostgreSQL primary journal connection; never exposed in errors.
    pub database: String,
    /// Trusted single-plan authorization and resource configuration.
    pub adapter: InteropProfile,
}
/// Same host/dispatcher on all five surfaces.
pub type InteropHost<S = PostgresStore> = ScriptedHost<S>;
/// Resolve real component discovery and the two direct Arrow handoffs.
/// # Errors
/// Rejects malformed component discovery, deployment identity or ports.
pub fn interop_registry(profile: &InteropProfile) -> Result<Registry, Error> {
    let mut registry = io::registry(&profile.artifact_sha256, &profile.io.ports)?;
    let data = data::registry(&profile.artifact_sha256, &profile.data_ports)?;
    registry.components.extend(data.components);
    registry.operations.extend(data.operations);
    registry.boundaries.extend(data.boundaries);
    if let Some(db) = &profile.database {
        let linked = database::registry(&profile.artifact_sha256, &db.ports)?;
        registry.components.extend(linked.components);
        registry.operations.extend(linked.operations);
        registry.boundaries.extend(linked.boundaries);
        registry.boundaries.push(RegistryBoundariesItem {
            id: "database-data".into(),
            source_operation: "database.read".into(),
            source_version: 1,
            target_operation: "data.run".into(),
            target_version: 3,
            source_contract: "plenora-arrow-interchange-v1".into(),
            target_contract: "plenora-arrow-interchange-v1".into(),
            content_type: "application/vnd.apache.arrow.stream".into(),
            mode: "direct".into(),
            adapter: None,
            status: "draft".into(),
        });
    }

    for (id, source, sv, target, tv) in [
        ("io-data", "io.read", 2, "data.run", 3),
        ("data-io", "data.run", 3, "io.write", 2),
    ] {
        registry.boundaries.push(RegistryBoundariesItem {
            id: id.into(),
            source_operation: source.into(),
            source_version: sv,
            target_operation: target.into(),
            target_version: tv,
            source_contract: "plenora-arrow-interchange-v1".into(),
            target_contract: "plenora-arrow-interchange-v1".into(),
            content_type: "application/vnd.apache.arrow.stream".into(),
            mode: "direct".into(),
            adapter: None,
            status: "draft".into(),
        });
    }
    parse_registry(&serde_json::to_vec(&registry).map_err(|_| Error::Invalid)?).map_err(Into::into)
}
struct InteropAdapter<S> {
    io: IoAdapter<S>,
    data: DataAdapter<S>,
    database: Option<DatabaseAdapter<S>>,
}
impl<S: Store> InteropAdapter<S> {
    fn target(&self, i: &Invocation) -> Result<&dyn Adapter, Error> {
        match (
            i.step.component.as_str(),
            i.step.operation.as_str(),
            i.step.operation_version,
        ) {
            ("io", "io.read" | "io.write", 2) => Ok(&self.io),
            ("data", "data.run", 3) => Ok(&self.data),
            ("database", "database.read", 1) | ("database", "database.write", 2) => self
                .database
                .as_ref()
                .map(|db| db as &dyn Adapter)
                .ok_or(Error::Unsupported),
            _ => Err(Error::Unsupported),
        }
    }
}
#[async_trait]
impl<S: Store> Adapter for InteropAdapter<S> {
    async fn authorize(&self, tenant: &str, plan: &Plan) -> Result<(), Error> {
        self.io.authorize(tenant, plan).await?;
        self.data.authorize(tenant, plan).await?;
        if let Some(db) = &self.database {
            db.authorize(tenant, plan).await?;
        }
        Ok(())
    }
    async fn artifact(
        &self,
        tenant: &str,
        reference: &str,
        limit: usize,
    ) -> Result<Vec<u8>, Error> {
        if reference == "ref:data-catalog" || reference.starts_with("ref:data-") {
            self.data.artifact(tenant, reference, limit).await
        } else if reference == "ref:database-catalog" || reference.starts_with("ref:db-") {
            self.database
                .as_ref()
                .ok_or(Error::Unsupported)?
                .artifact(tenant, reference, limit)
                .await
        } else {
            self.io.artifact(tenant, reference, limit).await
        }
    }
    async fn prepare(&self, i: &Invocation) -> Result<(), Error> {
        self.target(i)?.prepare(i).await
    }
    async fn invoke(&self, i: &Invocation) -> Result<StepResult, Error> {
        self.target(i)?.invoke(i).await
    }
    async fn request_cancel(&self, i: &Invocation) -> Result<(), Error> {
        self.target(i)?.request_cancel(i).await
    }
    async fn recover(&self, i: &Invocation) -> Result<Option<StepResult>, Error> {
        self.target(i)?.recover(i).await
    }
}
impl ScriptedHost {
    /// Compose the real adapters without connecting to the primary.
    /// # Errors
    /// Rejects invalid trusted configuration; no fallback to scripted adapters.
    pub fn interop(config: InteropConfig) -> Result<Self, Error> {
        let store = Arc::new(PostgresStore::new(&config.database).map_err(|_| Error::Invalid)?);
        Self::with_interop(config.adapter, store, Arc::new(SystemClock))
    }
}
impl<S: Store + plenora_runtime_boundary::DurableExecutionStore> ScriptedHost<S> {
    /// Compose both adapters on one store, clock, complete plan and coordinator.
    /// # Errors
    /// Rejects reserved artifact namespaces, altered discovery or unconfirmed plans.
    pub fn with_interop(
        profile: InteropProfile,
        store: Arc<S>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Error> {
        let tenant = profile.tenant.clone();
        let engine = Arc::new(interop_engine(profile, store.clone(), clock.clone())?);
        let runtime = control::runtime(store.clone(), engine.clone(), clock, &tenant)?;
        Ok(Self {
            runtime,
            store,
            worker: work::Worker(engine),
        })
    }
}

/// Build the same real composite coordinator for Rust callers driving durable progress.
/// # Errors
/// Rejects conflicting resource namespaces and plans not validated against real discovery.
pub fn interop_engine<S: Store + plenora_runtime_boundary::DurableExecutionStore>(
    profile: InteropProfile,
    store: Arc<S>,
    clock: Arc<dyn Clock>,
) -> Result<ChainEngine<S>, Error> {
    if profile.artifacts.keys().any(|r| {
        r.starts_with("ref:io-")
            || r.starts_with("ref:data-")
            || r.starts_with("ref:db-")
            || r == "ref:database-catalog"
    }) {
        return Err(Error::Invalid);
    }
    let registry = interop_registry(&profile)?;
    let tenant = profile.tenant.clone();
    let io = IoAdapter::with_registry(
        io::Config {
            tenant: tenant.clone(),
            artifact_sha256: profile.artifact_sha256.clone(),
            ports: profile.io.ports,
            authorized_plans: profile.authorized_plans.clone(),
            artifacts: profile.artifacts.clone(),
            sources: profile.io.sources,
            destinations: profile.io.destinations,
            staging_directory: profile.io.staging_directory,
        },
        store.clone(),
        clock.clone(),
        &registry,
    )?;
    let database = profile
        .database
        .map(|db| {
            let connections = db
                .connections
                .into_iter()
                .map(|(key, c)| {
                    let constructor = if c.allow_plaintext {
                        database::postgres_plaintext
                    } else {
                        database::postgres
                    };
                    (
                        key,
                        constructor(c.destination, database::SecretString::new(c.secret)),
                    )
                })
                .collect();
            DatabaseAdapter::with_registry(
                database::Config {
                    tenant: tenant.clone(),
                    artifact_sha256: profile.artifact_sha256.clone(),
                    ports: db.ports,
                    authorized_plans: profile.authorized_plans.clone(),
                    artifacts: profile.artifacts.clone(),
                },
                connections,
                store.clone(),
                clock.clone(),
                &registry,
            )
        })
        .transpose()?;
    let data = DataAdapter::with_registry(
        data::Config {
            tenant: tenant.clone(),
            artifact_sha256: profile.artifact_sha256,
            ports: profile.data_ports,
            authorized_plans: profile.authorized_plans,
            artifacts: profile.artifacts,
        },
        store.clone(),
        clock.clone(),
        &registry,
    )?;
    let engine = ChainEngine::new(
        store,
        Arc::new(InteropAdapter { io, data, database }),
        registry,
        clock,
        &tenant,
    )?;
    Ok(engine)
}
