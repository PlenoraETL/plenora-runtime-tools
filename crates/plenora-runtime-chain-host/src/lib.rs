//! Scripted qualification composition. This is not an attestation of real library adapters.
//! The production boundary integration gate is documented in docs/qualification.md.
#![forbid(unsafe_code)]
mod remote;
use async_trait::async_trait;
use plenora_runtime_chain_store::{PostgresStore, Retention};
use plenora_runtime_chains::{
    Adapter, ChainEngine, Error, Invocation, Request, Response,
    contracts::{StepResult, parse_registry, parse_result},
};
use plenora_runtime_core::SystemClock;
pub use remote::RemoteClient;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

/// Explicit scripted profile configuration. No filesystem path is accepted as a domain reference.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptedConfig {
    /// PostgreSQL connection configuration; never included in errors or Debug.
    pub database: String,
    /// Host-authorized tenant.
    pub tenant: String,
    /// Closed registry JSON from the fixture qualification corpus.
    pub registry: String,
    /// Closed result JSON supplying scripted step outcomes.
    pub outcomes: String,
    /// Optional authenticated scripted recovery receipts, distinct from reinvocation.
    pub recovery: Option<String>,
    /// Tenant-local immutable fixture resources, identified only by opaque references.
    pub artifacts: BTreeMap<String, Vec<u8>>,
}
struct ScriptedAdapter {
    steps: Vec<StepResult>,
    recovery: Vec<StepResult>,
    artifacts: BTreeMap<String, Vec<u8>>,
}
#[async_trait]
impl Adapter for ScriptedAdapter {
    async fn prepare(&self, i: &Invocation) -> Result<(), Error> {
        let s = self
            .steps
            .iter()
            .find(|s| s.step_id == i.step.id)
            .ok_or(Error::Unsupported)?;
        if s.operation != i.step.operation
            || s.operation_version != i.step.operation_version
            || s.input_manifests != i.inputs
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }
    async fn invoke(&self, i: &Invocation) -> Result<StepResult, Error> {
        self.steps
            .iter()
            .find(|s| s.step_id == i.step.id)
            .cloned()
            .ok_or(Error::Unsupported)
    }
    async fn request_cancel(&self, _: &Invocation) -> Result<(), Error> {
        Ok(())
    }
    async fn recover(&self, i: &Invocation) -> Result<Option<StepResult>, Error> {
        Ok(self
            .recovery
            .iter()
            .find(|s| s.step_id == i.step.id)
            .cloned())
    }
    async fn artifact(&self, _: &str, reference: &str, limit: usize) -> Result<Vec<u8>, Error> {
        let bytes = self.artifacts.get(reference).ok_or(Error::NotFound)?;
        if bytes.len() > limit {
            return Err(Error::ResourceLimit);
        }
        Ok(bytes.clone())
    }
}
/// Qualification-only composition with durable PostgreSQL state and declared fake domain adapters.
pub struct ScriptedHost {
    engine: ChainEngine<PostgresStore, ScriptedAdapter>,
    store: Arc<PostgresStore>,
}
impl ScriptedHost {
    /// Construct the explicit fake profile without opening the database or domain resources.
    /// # Errors
    /// Rejects malformed closed fixture documents or invalid tenant/database configuration.
    pub fn new(config: ScriptedConfig) -> Result<Self, Error> {
        let registry = parse_registry(config.registry.as_bytes())?;
        let steps = parse_result(config.outcomes.as_bytes())?.steps;
        let recovery = config
            .recovery
            .as_ref()
            .map(|r| parse_result(r.as_bytes()))
            .transpose()?
            .map_or_else(Vec::new, |r| r.steps);
        let store = Arc::new(PostgresStore::new(&config.database).map_err(|_| Error::Invalid)?);
        let adapter = Arc::new(ScriptedAdapter {
            steps,
            recovery,
            artifacts: config.artifacts,
        });
        let engine = ChainEngine::new(
            store.clone(),
            adapter,
            registry,
            Arc::new(SystemClock),
            &config.tenant,
        )?;
        Ok(Self { engine, store })
    }
    /// Apply the versioned schema on a primary with confirmed durable commits.
    /// # Errors
    /// Readiness is not asserted when migration or its commit is unconfirmed.
    pub async fn migrate(&self) -> Result<(), Error> {
        self.store
            .migrate(Retention::default())
            .await
            .map_err(|_| Error::Storage)
    }
    /// Dispatch a closed call through the sole native coordinator.
    pub async fn execute(&self, request: Request) -> Response {
        self.engine.execute(request).await
    }
    /// Decode a bounded facade call; duplicate/unknown envelope fields are rejected.
    pub async fn call(&self, bytes: &[u8]) -> Response {
        let request = if bytes.len() > 1024 * 1024 {
            Err(Error::ResourceLimit)
        } else {
            serde_json::from_slice::<Request>(bytes).map_err(|_| Error::Invalid)
        };
        match request {
            Ok(request) => self.execute(request).await,
            Err(error) => Response {
                value: serde_json::Value::Null,
                error: Some(error),
            },
        }
    }
}
