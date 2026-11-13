//! Scripted qualification composition. This is not an attestation of real library adapters.
//! Host and step calls use the durable dispatcher; remaining gates are in docs/qualification.md.
#![forbid(unsafe_code)]
mod control;
mod remote;
use async_trait::async_trait;
use plenora_runtime_chain_store::{PostgresStore, Retention};
use plenora_runtime_chains::{
    Adapter, ChainEngine, Error, Invocation, Request, Response, Store,
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
        // Scripted outcomes are known before invocation: reserve the complete encoded report.
        let size = serde_json::to_vec(s).map_err(|_| Error::Invalid)?.len();
        if size > 1024 * 1024
            || u64::try_from(size).map_err(|_| Error::ResourceLimit)?
                > i.reservation.limits.result_bytes
        {
            return Err(Error::ResourceLimit);
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
pub struct ScriptedHost<S: Store + plenora_runtime_boundary::DurableExecutionStore = PostgresStore>
{
    runtime: plenora_runtime_boundary::DurableRuntime<S>,
    store: Arc<S>,
}
impl ScriptedHost {
    /// Construct the explicit fake profile without opening the database or domain resources.
    /// # Errors
    /// Rejects malformed closed fixture documents or invalid tenant/database configuration.
    pub fn new(config: ScriptedConfig) -> Result<Self, Error> {
        let store = Arc::new(PostgresStore::new(&config.database).map_err(|_| Error::Invalid)?);
        Self::with_store(config, store, Arc::new(SystemClock))
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
}
impl<S: Store + plenora_runtime_boundary::DurableExecutionStore> ScriptedHost<S> {
    /// Compose a declared scripted profile over one primary implementing both chain and runtime journals.
    /// No alternate execution backend can replace the concrete durable step executor.
    /// # Errors
    /// Rejects invalid fixture documents, discovery or tenant configuration.
    pub fn with_store(
        config: ScriptedConfig,
        store: Arc<S>,
        clock: Arc<dyn plenora_runtime_core::Clock>,
    ) -> Result<Self, Error> {
        let registry = parse_registry(config.registry.as_bytes())?;
        let steps = parse_result(config.outcomes.as_bytes())?.steps;
        let recovery = config
            .recovery
            .as_ref()
            .map(|r| parse_result(r.as_bytes()))
            .transpose()?
            .map_or_else(Vec::new, |r| r.steps);
        let adapter = Arc::new(ScriptedAdapter {
            steps,
            recovery,
            artifacts: config.artifacts,
        });
        let engine = Arc::new(ChainEngine::new(
            store.clone(),
            adapter,
            registry,
            clock.clone(),
            &config.tenant,
        )?);
        let runtime = control::runtime(store.clone(), engine, clock, &config.tenant)?;
        Ok(Self { runtime, store })
    }
    /// Dispatch every typed API through durable ingress, evidenced component and terminal settlement.
    pub async fn execute(&self, request: Request) -> Response {
        match control::message(request) {
            Ok(message) => self.dispatch_message(&message).await,
            Err(error) => control::error(error),
        }
    }
    /// Raw ingress for protocol tests and transports; the caller owns identity and negotiation.
    /// # Errors
    /// Never returns an unconfirmed terminal or invokes outside the registered dispatcher.
    pub async fn process(
        &self,
        message: &plenora_runtime_messaging::SerializedMessage,
    ) -> Result<
        plenora_runtime_messaging::SerializedMessage,
        plenora_runtime_boundary::SettlementError,
    > {
        self.runtime
            .process_confirmed(
                message,
                plenora_runtime_boundary::DeliveryRoute::Ingress,
                control::worker(),
            )
            .await?
            .message()
    }
    async fn dispatch_message(
        &self,
        message: &plenora_runtime_messaging::SerializedMessage,
    ) -> Response {
        match self
            .runtime
            .process_confirmed(
                message,
                plenora_runtime_boundary::DeliveryRoute::Ingress,
                control::worker(),
            )
            .await
        {
            Ok(prepared) => match prepared.message() {
                Ok(terminal) => control::response(&terminal, prepared.report()),
                Err(_) => control::error(Error::Storage),
            },
            Err(_) => control::error(Error::Storage),
        }
    }
    /// Decode a bounded typed facade request before building its first external carrier.
    pub async fn call(&self, bytes: &[u8]) -> Response {
        if bytes.len() > 1024 * 1024 {
            return control::error(Error::ResourceLimit);
        }
        match serde_json::from_slice::<Request>(bytes) {
            Ok(request) => self.execute(request).await,
            Err(_) => control::error(Error::Invalid),
        }
    }
    async fn call_wire(&self, bytes: &[u8]) -> Response {
        if bytes.len() > 8 * 1024 * 1024 {
            return control::error(Error::ResourceLimit);
        }
        match serde_json::from_slice::<control::Wire>(bytes) {
            Ok(wire) => self.dispatch_message(&wire.restore()).await,
            Err(_) => control::error(Error::Invalid),
        }
    }
}

#[cfg(test)]
#[path = "../tests/support/host.rs"]
mod tests;
