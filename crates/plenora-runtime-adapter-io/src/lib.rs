//! IO 9.0.0 candidate adapter over real Rust operations; no release claim.
#![forbid(unsafe_code)]
mod ipc;
mod operation;
mod receipt;
mod registry;
use async_trait::async_trait;
use plenora_io_model::{CancellationToken, json::from_slice as strict_json};
use plenora_runtime_chains::{
    Adapter, Error, Invocation, OwnedResource, ResourceLedger, ResourceRequest, Store,
    contracts::{Plan, StepResult, parse_plan, plan_digest, validate_plan},
};
use plenora_runtime_core::Clock;
pub use registry::{Ports, catalog, discovery, registry};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::UNIX_EPOCH,
};
/// Immutable candidate source revision, NOT a published 9.0.0 release.
pub const REVISION: &str = "9c087aa7daf5d9ad10843ebf3027ae8858f66b94";
/// Per-document and materialized input/output ceiling of this host profile.
pub const MAX_BLOB: usize = 1024 * 1024;
pub(crate) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(crate) fn encode(value: &impl Serialize) -> Result<Vec<u8>, Error> {
    let mut out = ipc::Limited {
        bytes: Vec::new(),
        limit: MAX_BLOB,
    };
    serde_json::to_writer(&mut out, value).map_err(|_| Error::ResourceLimit)?;
    Ok(out.bytes)
}
pub(crate) fn reference(i: &Invocation, label: &str) -> String {
    format!(
        "ref:io-{}-{}",
        i.run_id,
        digest(format!("{}:{label}", i.message_id).as_bytes())
    )
}
fn run(reference: &str) -> Option<&str> {
    let tail = reference.strip_prefix("ref:io-")?;
    if tail.len() != 101 || tail.as_bytes().get(36) != Some(&b'-') {
        return None;
    }
    tail.get(..36)
}

/// Immutable snapshot of a source, including its companion files.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    /// Relative entry point understood by the real IO driver.
    pub entry: String,
    /// Relative paths to exact bytes; no links or external filesystem fallback.
    pub files: BTreeMap<String, Vec<u8>>,
}
/// Authorized publication target: an exclusive bundle directory.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Destination {
    /// Absolute bundle directory, absent before the first confirmed write.
    pub directory: String,
    /// Entry filename inside the bundle (for example data.arrow or data.shp).
    pub entry: String,
}
/// Trusted host configuration, never accepted as chain parameters.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Authenticated principal scope.
    pub tenant: String,
    /// Deployed host artifact digest from its manifest.
    pub artifact_sha256: String,
    /// Arrow ports checked at the consumer and producer barriers.
    pub ports: Ports,
    /// Full plans authorized by this host principal.
    pub authorized_plans: Vec<String>,
    /// Immutable parameter documents and external Arrow artifacts.
    pub artifacts: BTreeMap<String, Vec<u8>>,
    /// Read-source snapshots, bound by digest in their opaque references.
    pub sources: BTreeMap<String, Source>,
    /// Destination references resolved only by the trusted host.
    pub destinations: BTreeMap<String, Destination>,
    /// Private staging parent managed by the host.
    pub staging_directory: String,
}
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("IoConfig([redacted])")
    }
}
/// Real IO adapter, with durable resource ownership and an injected coordinator clock.
pub struct IoAdapter<S> {
    config: Config,
    store: Arc<S>,
    clock: Arc<dyn Clock>,
    plans: BTreeMap<String, Plan>,
    running: Mutex<BTreeMap<String, CancellationToken>>,
}
impl<S: Store> IoAdapter<S> {
    /// Freeze authenticated resolvers and the discovery of the actual linked candidate.
    /// # Errors
    /// Rejects invalid plans, paths, snapshots, ports and deployment identity.
    pub fn new(config: Config, store: Arc<S>, clock: Arc<dyn Clock>) -> Result<Self, Error> {
        let registry = registry(&config.artifact_sha256, &config.ports)?;
        if config.tenant.is_empty()
            || !std::path::Path::new(&config.staging_directory).is_absolute()
            || config
                .artifacts
                .iter()
                .any(|(r, b)| r.starts_with("ref:io-") || b.len() > MAX_BLOB)
        {
            return Err(Error::Invalid);
        }
        for (reference, source) in &config.sources {
            operation::validate_source(source)?;
            if reference.strip_prefix("artifact://sha256/")
                != Some(digest(&encode(source)?).as_str())
            {
                return Err(Error::Invalid);
            }
        }
        for destination in config.destinations.values() {
            if !std::path::Path::new(&destination.directory).is_absolute()
                || !operation::relative(&destination.entry)
            {
                return Err(Error::Invalid);
            }
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
    async fn prepared(&self, i: &Invocation) -> Result<operation::Prepared, Error> {
        self.plan(i)?;
        if self.now()? >= i.reservation.deadline_ms {
            return Err(Error::Timeout);
        }
        let raw = self
            .config
            .artifacts
            .get(&i.step.parameters.artifact_ref)
            .ok_or(Error::NotFound)?;
        if digest(raw) != i.step.parameters.sha256 {
            return Err(Error::Invalid);
        }
        let document: serde_json::Value = strict_json(raw).map_err(|_| Error::Invalid)?;
        if document
            .as_object()
            .ok_or(Error::Invalid)?
            .values()
            .any(serde_json::Value::is_null)
            || document
                .get("budgets")
                .and_then(serde_json::Value::as_object)
                .is_some_and(|b| b.values().any(serde_json::Value::is_null))
        {
            return Err(Error::Invalid);
        }
        let parameters: operation::Parameters = strict_json(raw).map_err(|_| Error::Invalid)?;
        parameters.validate(i)?;
        let mut input = None;
        let mut input_rows = 0;
        let source = if i.step.operation == "io.read" {
            if !i.inputs.is_empty() {
                return Err(Error::Invalid);
            }
            Some(
                self.config
                    .sources
                    .get(&parameters.source)
                    .ok_or(Error::NotFound)?
                    .clone(),
            )
        } else {
            if i.inputs.len() != 1 || parameters.source != "ref:chain-data" {
                return Err(Error::Invalid);
            }
            let manifest = i.inputs.get("data").ok_or(Error::Invalid)?;
            let bytes = self
                .artifact(&i.tenant, &manifest.artifact_ref, MAX_BLOB)
                .await?;
            if bytes.len() as u64 != manifest.bytes || digest(&bytes) != manifest.sha256 {
                return Err(Error::Invalid);
            }
            input_rows = ipc::check(&bytes, &manifest.port, i)?.0;
            ipc::check(
                &bytes,
                &i.step.inputs.get("data").ok_or(Error::Invalid)?.expected,
                i,
            )?;
            input = Some(bytes);
            None
        };
        let destination = parameters
            .destination
            .as_ref()
            .map(|r| {
                self.config
                    .destinations
                    .get(r)
                    .cloned()
                    .ok_or(Error::Invalid)
            })
            .transpose()?;
        let bytes = source
            .as_ref()
            .map_or(0, |s| s.files.values().map(|b| b.len() as u64).sum::<u64>())
            + input.as_ref().map_or(0, |b| b.len() as u64);
        let request_bytes = (raw.len() as u64)
            .checked_add(bytes)
            .ok_or(Error::ResourceLimit)?;
        if request_bytes > i.reservation.limits.request_bytes
            || i.reservation.limits.result_bytes < receipt::RECEIPT_RESERVATION
        {
            return Err(Error::ResourceLimit);
        }
        let prepared = operation::Prepared {
            parameters,
            source,
            input,
            input_rows,
            destination,
            staging: self.config.staging_directory.clone(),
            request_bytes,
        };
        operation::limits(i, &prepared.parameters)?;
        Ok(prepared)
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
impl<S: Store> Adapter for IoAdapter<S> {
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
        let owned;
        let bytes = if reference == "ref:io-catalog" {
            owned = encode(&catalog())?;
            &owned
        } else {
            self.config
                .artifacts
                .get(reference)
                .ok_or(Error::NotFound)?
        };
        if bytes.len() > limit {
            return Err(Error::ResourceLimit);
        }
        Ok(bytes.clone())
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
        let request_bytes = prepared.request_bytes;
        let invocation = Invocation {
            tenant: i.tenant.clone(),
            run_id: i.run_id.clone(),
            message_id: i.message_id.clone(),
            plan_digest: i.plan_digest.clone(),
            step: i.step.clone(),
            inputs: i.inputs.clone(),
            reservation: i.reservation.clone(),
            started_ms: i.started_ms,
            attempt: i.attempt,
        };
        let worker_token = token.clone();
        let future = tokio::task::spawn_blocking(move || {
            operation::execute(prepared, &invocation, &worker_token)
        });
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
                    if now >= i.reservation.deadline_ms { token.cancel(); }
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
        let outcome = outcome.map_err(|_| Error::Recovery)??;
        let (result, objects) = receipt::build(
            i,
            outcome,
            request_bytes,
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
