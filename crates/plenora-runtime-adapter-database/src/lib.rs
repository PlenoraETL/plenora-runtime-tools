//! Real, materialized database adapter. Versions come from the pinned library.
//! read@2 is absent upstream; there is deliberately no private alias.
#![forbid(unsafe_code)]
mod arrow;
mod operation;
mod receipt;
mod registry;

use async_trait::async_trait;
pub use plenora_database_core::provider::SecretString;
use plenora_database_core::{CancellationToken, provider::Provider, strict_json};
use plenora_database_engine::public_ops::{OperationRequest, WriteRequest};
use plenora_runtime_chains::{
    Adapter, Error, Invocation, OwnedResource, ResourceLedger, ResourceRequest, Store,
    contracts::{Plan, StepResult, parse_plan, plan_digest, validate_plan},
};
use plenora_runtime_core::Clock;
pub use registry::{Ports, discovery, registry};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::UNIX_EPOCH,
};

/// Published source revision, independent of operation versions.
pub const REVISION: &str = "f4fdb9b5f09d8babe795f68e894949693aa9d0a1";
/// Maximum materialized artifact or evidence document in this profile.
pub const MAX_BLOB: usize = 1024 * 1024;
pub(crate) const CATALOG: &[u8] = include_bytes!("../catalog.json");
pub(crate) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(crate) fn encode(value: &impl Serialize) -> Result<Vec<u8>, Error> {
    let mut out = arrow::Limited {
        bytes: Vec::new(),
        limit: MAX_BLOB,
    };
    serde_json::to_writer(&mut out, value).map_err(|_| Error::ResourceLimit)?;
    Ok(out.bytes)
}
pub(crate) fn reference(i: &Invocation, label: &str) -> String {
    format!(
        "ref:db-{}-{}",
        i.run_id,
        digest(format!("{}:{label}", i.message_id).as_bytes())
    )
}
fn run(reference: &str) -> Option<&str> {
    let tail = reference.strip_prefix("ref:db-")?;
    if tail.len() != 101 || tail.as_bytes().get(36) != Some(&b'-') {
        return None;
    }
    tail.get(..36)
}

/// Trusted, immutable host configuration. Never accept this as a chain payload.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Authenticated tenant.
    pub tenant: String,
    /// SHA-256 of the deployed host artifact, supplied by its deployment manifest.
    pub artifact_sha256: String,
    /// Exact Arrow ports of this host profile; verified against real IPC.
    pub ports: Ports,
    /// Complete plans independently confirmed by the host principal.
    pub authorized_plans: Vec<String>,
    /// Immutable, tenant-authorized documents and input artifacts. No filesystem fallback.
    pub artifacts: BTreeMap<String, Vec<u8>>,
}
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DatabaseConfig([redacted])")
    }
}
/// Resolved provider and secret. Construction is an authorization decision of the host.
pub struct Connection {
    /// Chain destination reference covered by full-plan consent.
    pub destination: String,
    /// Concrete pinned provider; PostgreSQL is supplied by `postgres`.
    pub provider: Arc<dyn Provider>,
    /// Secret never copied to the plan, evidence, Display or Debug.
    pub secret: SecretString,
}
impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DatabaseConnection([redacted])")
    }
}
/// Construct the real PostgreSQL provider with secure defaults.
#[must_use]
pub fn postgres(destination: String, secret: SecretString) -> Connection {
    Connection {
        destination,
        secret,
        provider: Arc::new(plenora_db_postgres::PostgresProvider::default()),
    }
}
/// Explicit trusted-host transport policy. TLS remains the default constructor.
/// Plaintext must only be selected by a deployment that accepts the network risk.
#[must_use]
pub fn postgres_plaintext(destination: String, secret: SecretString) -> Connection {
    Connection {
        destination,
        secret,
        provider: Arc::new(plenora_db_postgres::PostgresProvider::insecure_local()),
    }
}
/// Adapter over the primary chain resource ledger and real database provider.
pub struct DatabaseAdapter<S> {
    config: Config,
    store: Arc<S>,
    clock: Arc<dyn Clock>,
    plans: BTreeMap<String, Plan>,
    connections: BTreeMap<String, Connection>,
    running: Mutex<BTreeMap<String, CancellationToken>>,
}
struct Prepared {
    operation: operation::Operation,
    credential: String,
    request_bytes: u64,
}
impl<S: Store> DatabaseAdapter<S> {
    /// Freeze an authenticated resolver, real discovery and full-plan authorizations.
    /// # Errors
    /// Rejects malformed configuration, unresolvable plans and unsupported operation versions.
    pub fn new(
        config: Config,
        connections: BTreeMap<String, Connection>,
        store: Arc<S>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Error> {
        let registry = registry(&config.artifact_sha256, &config.ports)?;
        if config.tenant.is_empty()
            || config
                .artifacts
                .iter()
                .any(|(r, b)| r.starts_with("ref:db-") || b.len() > MAX_BLOB)
        {
            return Err(Error::Invalid);
        }
        let plans = config
            .authorized_plans
            .iter()
            .map(|raw| {
                let plan = parse_plan(raw.as_bytes())?;
                validate_plan(&plan, &registry)?;
                Ok((plan_digest(&plan)?, plan))
            })
            .collect::<Result<_, Error>>()?;
        Ok(Self {
            config,
            store,
            clock,
            plans,
            connections,
            running: Mutex::new(BTreeMap::new()),
        })
    }
    fn now(&self) -> Result<u64, Error> {
        u64::try_from(
            self.clock
                .now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Error::Invalid)?
                .as_millis(),
        )
        .map_err(|_| Error::ResourceLimit)
    }
    fn plan(&self, i: &Invocation) -> Result<&Plan, Error> {
        if i.tenant != self.config.tenant {
            return Err(Error::Invalid);
        }
        let plan = self.plans.get(&i.plan_digest).ok_or(Error::Invalid)?;
        if !plan.steps.contains(&i.step) {
            return Err(Error::Invalid);
        }
        Ok(plan)
    }
    fn document<T: serde::de::DeserializeOwned>(&self, reference: &str) -> Result<T, Error> {
        let bytes = self
            .config
            .artifacts
            .get(reference)
            .ok_or(Error::NotFound)?;
        // Nested upstream references are content-addressed, so a restarted host
        // cannot change an operation behind the same authorized parameter digest.
        if reference.strip_prefix("artifact://sha256/") != Some(digest(bytes).as_str()) {
            return Err(Error::Invalid);
        }
        strict_json::from_slice(bytes).map_err(|_| Error::Invalid)
    }
    #[allow(clippy::too_many_lines)] // One admission path, before any provider effect.
    async fn prepared(&self, i: &Invocation) -> Result<Prepared, Error> {
        self.plan(i)?;
        if self.now()? >= i.reservation.deadline_ms {
            return Err(Error::Timeout);
        }
        operation::budget(i)?;
        let raw = self
            .config
            .artifacts
            .get(&i.step.parameters.artifact_ref)
            .ok_or(Error::NotFound)?;
        if digest(raw) != i.step.parameters.sha256 {
            return Err(Error::Invalid);
        }
        let (target, operation) = match i.step.operation.as_str() {
            "database.read" if i.step.operation_version == 1 => {
                let request: OperationRequest =
                    strict_json::from_slice(raw).map_err(|_| Error::Invalid)?;
                let target = request.target().validate().map_err(|_| Error::Invalid)?;
                let operation = self.document(&request.operation_path)?;
                let parameters = request
                    .parameters_path
                    .as_ref()
                    .map(|r| self.document(r))
                    .transpose()?
                    .unwrap_or_default();
                if !i.inputs.is_empty() {
                    return Err(Error::Invalid);
                }
                (
                    target,
                    operation::Operation::Read(operation, parameters, self.config.ports.read()),
                )
            }
            "database.write" if i.step.operation_version == 2 => {
                let request: WriteRequest =
                    strict_json::from_slice(raw).map_err(|_| Error::Invalid)?;
                if request.accept_loss != i.step.loss_consent.unwrap_or(false) {
                    return Err(Error::Invalid);
                }
                let target = request.target().validate().map_err(|_| Error::Invalid)?;
                let operation = self.document(&request.operation_path)?;
                if i.inputs.len() != 1 {
                    return Err(Error::Invalid);
                }
                let input = i.inputs.get("data").ok_or(Error::Invalid)?;
                let bytes = self
                    .artifact(&i.tenant, &input.artifact_ref, MAX_BLOB)
                    .await?;
                if input.bytes != bytes.len() as u64 || digest(&bytes) != input.sha256 {
                    return Err(Error::Invalid);
                }
                let decoded = arrow::decode(
                    &bytes,
                    &input.port,
                    i.reservation.limits.rows_read,
                    i.reservation.limits.memory_bytes / 4,
                )?;
                arrow::schema(
                    &decoded.schema,
                    &i.step.inputs.get("data").ok_or(Error::Invalid)?.expected,
                )?;
                (
                    target,
                    operation::Operation::Write(
                        operation,
                        decoded,
                        request.accept_loss,
                        self.plan(i)?.requirements.fidelity == "reported",
                    ),
                )
            }
            _ => return Err(Error::Unsupported),
        };
        let connection = self
            .connections
            .get(&target.secret_environment)
            .ok_or(Error::Invalid)?;
        if connection.provider.kind() != target.provider
            || !target.provider_arguments.is_empty()
            || (i.step.operation == "database.write"
                && i.step.destination_ref.as_ref() != Some(&connection.destination))
        {
            return Err(Error::Invalid);
        }
        let document: serde_json::Value =
            strict_json::from_slice(raw).map_err(|_| Error::Invalid)?;
        let mut request_bytes = raw.len() as u64;
        for field in ["operation_path", "parameters_path"] {
            if let Some(reference) = document.get(field).and_then(serde_json::Value::as_str) {
                request_bytes = request_bytes
                    .checked_add(
                        self.config
                            .artifacts
                            .get(reference)
                            .ok_or(Error::NotFound)?
                            .len() as u64,
                    )
                    .ok_or(Error::ResourceLimit)?;
            }
        }
        for input in i.inputs.values() {
            request_bytes = request_bytes
                .checked_add(input.bytes)
                .ok_or(Error::ResourceLimit)?;
        }
        if request_bytes > i.reservation.limits.request_bytes
            || i.reservation.limits.result_bytes < receipt::RECEIPT_RESERVATION
        {
            return Err(Error::ResourceLimit);
        }
        Ok(Prepared {
            operation,
            credential: target.secret_environment,
            request_bytes,
        })
    }
    fn allocation(&self, i: &Invocation) -> Result<ResourceRequest, Error> {
        let plan = self.plan(i)?;
        // Reserve the worst materialized output and evidence copies before any provider call.
        let reserve = (MAX_BLOB as u64)
            .checked_mul(32)
            .ok_or(Error::ResourceLimit)?;
        if i.reservation.limits.memory_bytes < reserve || i.reservation.limits.spool_bytes < reserve
        {
            return Err(Error::ResourceLimit);
        }
        Ok(ResourceRequest::new(
            i,
            plan.budgets.clone(),
            vec![OwnedResource {
                reference: reference(i, "reservation"),
                bytes: vec![255; MAX_BLOB],
                consumers: vec![],
                temporary: true,
            }],
        ))
    }
}
#[async_trait]
impl<S: Store> Adapter for DatabaseAdapter<S> {
    async fn authorize(&self, tenant: &str, plan: &Plan) -> Result<(), Error> {
        if tenant != self.config.tenant || self.plans.get(&plan_digest(plan)?) != Some(plan) {
            return Err(Error::Invalid);
        }
        Ok(())
    }
    async fn artifact(
        &self,
        tenant: &str,
        reference: &str,
        limit: usize,
    ) -> Result<Vec<u8>, Error> {
        if tenant != self.config.tenant {
            return Err(Error::Invalid);
        }
        if let Some(run) = run(reference) {
            return self
                .store
                .read_resource(tenant, run, reference, limit)
                .await;
        }
        let bytes = if reference == "ref:database-catalog" {
            CATALOG
        } else {
            self.config
                .artifacts
                .get(reference)
                .ok_or(Error::NotFound)?
                .as_slice()
        };
        if bytes.len() > limit {
            return Err(Error::ResourceLimit);
        }
        Ok(bytes.to_vec())
    }
    async fn prepare(&self, i: &Invocation) -> Result<(), Error> {
        self.prepared(i).await?;
        let request = self.allocation(i)?;
        let mut ledger = self
            .store
            .resource_snapshot(&i.tenant, &i.run_id)
            .await?
            .unwrap_or_else(|| ResourceLedger::new(&request.limits));
        ledger.acquire(
            &request,
            self.now()?,
            i.started_ms
                .checked_add(plenora_runtime_chains::REPORT_MS)
                .ok_or(Error::ResourceLimit)?,
        )?;
        // Trial addition proves headroom for bounded documents and Arrow after the reserved buffer.
        let mut worst = request;
        for label in ["output", "upstream", "loss"] {
            worst.objects.push(OwnedResource {
                reference: reference(i, label),
                bytes: vec![255; MAX_BLOB],
                consumers: vec![],
                temporary: false,
            });
        }
        ledger.reconcile(
            &worst,
            self.now()?,
            i.started_ms
                .checked_add(plenora_runtime_chains::REPORT_MS)
                .ok_or(Error::ResourceLimit)?,
        )?;
        Ok(())
    }
    async fn invoke(&self, i: &Invocation) -> Result<StepResult, Error> {
        let prepared = self.prepared(i).await?;
        let mut request = self.allocation(i)?;
        let mut lease = self.store.acquire_resources(&request).await?;
        let token = {
            let mut running = self.running.lock().map_err(|_| Error::Recovery)?;
            running.entry(i.message_id.clone()).or_default().clone()
        };
        let connection = self
            .connections
            .get(&prepared.credential)
            .ok_or(Error::Invalid)?;
        let future = operation::execute(
            connection.provider.clone(),
            connection.secret.clone(),
            prepared.operation,
            i,
            &token,
        );
        tokio::pin!(future);
        let mut ownership_lost = false;
        let outcome = loop {
            tokio::select! {
                biased;
                outcome = &mut future => break outcome,
                () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                    let Ok(now) = self.now() else { ownership_lost = true; token.cancel(); continue; };
                    tokio::select! {
                        biased;
                        outcome = &mut future => break outcome,
                        journal = tokio::time::timeout(std::time::Duration::from_millis(100), self.store.load(&i.tenant, &i.run_id)) => {
                            match journal {
                                Ok(Ok(journal)) if !journal.cancellation_requested() => {},
                                _ => token.cancel(),
                            }
                        }
                    }
                    if now >= i.reservation.deadline_ms { token.cancel_due_to_deadline(); }
                    if !ownership_lost && now.saturating_add(10_000) >= lease.until {
                        tokio::select! {
                            biased;
                            outcome = &mut future => break outcome,
                            renewed = tokio::time::timeout(std::time::Duration::from_millis(100), self.store.renew_resources(&i.tenant, &i.run_id, &lease)) => {
                                if let Ok(Ok(next)) = renewed { lease = next; }
                                else { ownership_lost = true; token.cancel(); }
                            }
                        }
                    }
                }
            }
        };
        self.running
            .lock()
            .map_err(|_| Error::Recovery)?
            .remove(&i.message_id);
        let outcome = outcome?;
        let (result, objects) = receipt::build(
            i,
            outcome,
            prepared.request_bytes,
            self.now()?.saturating_sub(i.started_ms),
        )?;
        request.objects.extend(objects);
        // Publication of documents is atomic with the subsequent confirmed chain view.
        // A failed confirmation returns recovery, never success or authorization to invoke again.
        self.store.reconcile_resources(&request).await?;
        // Even after lease loss, return the drained original evidence. The primary
        // journal fence, not this adapter, decides whether it may advance the view.
        Ok(result)
    }
    async fn request_cancel(&self, i: &Invocation) -> Result<(), Error> {
        self.plan(i)?;
        self.running
            .lock()
            .map_err(|_| Error::Recovery)?
            .entry(i.message_id.clone())
            .or_default()
            .cancel();
        Ok(())
    }
    async fn recover(&self, i: &Invocation) -> Result<Option<StepResult>, Error> {
        self.plan(i)?;
        // No upstream transaction-evidence resolver exists in this profile. Never re-invoke.
        Ok(None)
    }
}
