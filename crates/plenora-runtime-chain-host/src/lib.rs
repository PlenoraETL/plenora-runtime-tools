//! Shared durable host composition with explicit scripted and real database profiles.
//! Host and step calls use the durable dispatcher; remaining gates are in docs/qualification.md.
#![forbid(unsafe_code)]
mod control;
mod data;
mod database;
mod io;
pub use data::{DataConfig, DataHost};
pub use database::{DatabaseConfig, DatabaseConnectionConfig, DatabaseHost};
pub use io::{IoConfig, IoHost};
mod ownership;
mod remote;
mod verification;
mod work;
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
    /// Finite serialized run-journal reservation budget; immutable for accepted runs.
    #[serde(default = "journal_budget")]
    pub journal_bytes: usize,
    /// Host-authorized tenant.
    pub tenant: String,
    /// Closed registry JSON from the fixture qualification corpus.
    pub registry: String,
    /// Complete plans confirmed once by this trusted tenant profile; absent means deny.
    /// This scripted allowlist is not authentication of an external user.
    #[serde(default)]
    pub authorized_plans: Vec<String>,
    /// Closed result JSON supplying scripted step outcomes.
    pub outcomes: String,
    /// Optional authenticated scripted recovery receipts, distinct from reinvocation.
    pub recovery: Option<String>,
    /// Additional explicit per-attempt step receipts for retry qualification; never synthesized.
    #[serde(default)]
    pub attempts: Vec<String>,
    /// Tenant-local immutable fixture resources, identified only by opaque references.
    pub artifacts: BTreeMap<String, Vec<u8>>,
}
fn journal_budget() -> usize {
    plenora_runtime_chains::JOURNAL_BYTES
}
struct ScriptedAdapter {
    store: Arc<dyn Store>,
    tenant: String,
    authorized: BTreeMap<String, plenora_runtime_chains::contracts::Plan>,
    steps: Vec<StepResult>,
    recovery: Vec<StepResult>,
    artifacts: BTreeMap<String, Vec<u8>>,
}
#[async_trait]
impl Adapter for ScriptedAdapter {
    async fn authorize(
        &self,
        tenant: &str,
        plan: &plenora_runtime_chains::contracts::Plan,
    ) -> Result<(), Error> {
        if tenant != self.tenant
            || !self
                .authorized
                .contains_key(&plenora_runtime_chains::contracts::plan_digest(plan)?)
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }
    async fn prepare(&self, i: &Invocation) -> Result<(), Error> {
        if i.tenant != self.tenant || !self.authorized.contains_key(&i.plan_digest) {
            return Err(Error::Invalid);
        }
        let s = self
            .steps
            .iter()
            .find(|s| s.step_id == i.step.id && s.attempt == i.attempt)
            .ok_or(Error::Unsupported)?;
        let s = Self::materialize(i, s)?;
        self.check_owned_inputs(i).await?;
        if s.operation != i.step.operation
            || s.operation_version != i.step.operation_version
            || s.input_manifests != i.inputs
        {
            return Err(Error::Invalid);
        }
        // Scripted outcomes are known before invocation: reserve the complete encoded report.
        let size = serde_json::to_vec(&s).map_err(|_| Error::Invalid)?.len();
        if size > 1024 * 1024
            || u64::try_from(size).map_err(|_| Error::ResourceLimit)?
                > i.reservation.limits.result_bytes
        {
            return Err(Error::ResourceLimit);
        }
        self.verify(i, &s)?;
        self.reserve_owned(i, &s).await
    }
    async fn invoke(&self, i: &Invocation) -> Result<StepResult, Error> {
        let result = self
            .steps
            .iter()
            .find(|s| s.step_id == i.step.id && s.attempt == i.attempt)
            .cloned()
            .ok_or(Error::Unsupported)?;
        let result = Self::materialize(i, &result)?;
        let request = self.owned_request(i, &result)?;
        let lease = self.store.acquire_resources(&request).await?;
        // Scripted calls are immediate. Real providers must drive renewal while work is active;
        // the same primary lease API refuses expiry, and no expiry authorizes another invocation.
        self.store
            .renew_resources(&i.tenant, &i.run_id, &lease)
            .await?;
        Ok(result)
    }
    async fn request_cancel(&self, _: &Invocation) -> Result<(), Error> {
        Ok(())
    }
    async fn recover(&self, i: &Invocation) -> Result<Option<StepResult>, Error> {
        let proof = self
            .recovery
            .iter()
            .find(|s| s.step_id == i.step.id && s.attempt == i.attempt)
            .map(|s| Self::materialize(i, s))
            .transpose()?;
        if let Some(proof) = &proof {
            self.check_owned_inputs(i).await?;
            self.verify(i, proof)?;
            let bytes = serde_json::to_vec(proof).map_err(|_| Error::Invalid)?;
            if bytes.len() as u64 > i.reservation.limits.result_bytes {
                return Err(Error::ResourceLimit);
            }
            self.store
                .reconcile_resources(&self.owned_request(i, proof)?)
                .await?;
        }
        Ok(proof)
    }
    async fn artifact(
        &self,
        tenant: &str,
        reference: &str,
        limit: usize,
    ) -> Result<Vec<u8>, Error> {
        if tenant != self.tenant {
            return Err(Error::Invalid);
        }
        if reference.starts_with("ref:owned-") {
            let run = ownership::owned_run(reference).ok_or(Error::Invalid)?;
            return self
                .store
                .read_resource(tenant, run, reference, limit)
                .await;
        }
        let bytes = self.artifacts.get(reference).ok_or(Error::NotFound)?;
        if bytes.len() > limit {
            return Err(Error::ResourceLimit);
        }
        Ok(bytes.clone())
    }
}
/// Durable host shared by explicit scripted and real database constructors.
pub struct ScriptedHost<S: Store + plenora_runtime_boundary::DurableExecutionStore = PostgresStore>
{
    runtime: plenora_runtime_boundary::DurableRuntime<S>,
    store: Arc<S>,
    worker: work::Worker<S>,
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
        if config.artifacts.keys().any(|r| r.starts_with("ref:owned-")) {
            return Err(Error::Invalid);
        }
        let registry = parse_registry(config.registry.as_bytes())?;
        let mut steps = parse_result(config.outcomes.as_bytes())?.steps;
        for raw in &config.attempts {
            let next = plenora_runtime_chains::contracts::parse_step_result(raw.as_bytes())?;
            if steps
                .iter()
                .any(|s| s.step_id == next.step_id && s.attempt == next.attempt)
            {
                return Err(Error::Invalid);
            }
            steps.push(next);
        }
        let recovery = config
            .recovery
            .as_ref()
            .map(|r| parse_result(r.as_bytes()))
            .transpose()?
            .map_or_else(Vec::new, |r| r.steps);
        let authorized = config
            .authorized_plans
            .iter()
            .map(|raw| {
                let plan = plenora_runtime_chains::contracts::parse_plan(raw.as_bytes())?;
                Ok((
                    plenora_runtime_chains::contracts::validate_plan(&plan, &registry)?.plan_digest,
                    plan,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;
        let adapter = Arc::new(ScriptedAdapter {
            store: store.clone(),
            tenant: config.tenant.clone(),
            authorized,
            steps,
            recovery,
            artifacts: config.artifacts,
        });
        let engine = Arc::new(
            ChainEngine::new(
                store.clone(),
                adapter,
                registry,
                clock.clone(),
                &config.tenant,
            )?
            .with_journal_budget(config.journal_bytes)?,
        );
        let runtime = control::runtime(store.clone(), engine.clone(), clock, &config.tenant)?;
        Ok(Self {
            runtime,
            store,
            worker: work::Worker(engine),
        })
    }
    /// Advance persisted runs until host shutdown, independently of request lifetimes.
    /// Shutdown drains the current invocation; expired leases never permit blind replay.
    /// # Errors
    /// Primary failures are visible to the host supervisor; restarting scans the same journal.
    pub async fn work(&self, stop: tokio::sync::watch::Receiver<bool>) -> Result<(), Error> {
        self.worker.run(stop).await
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
