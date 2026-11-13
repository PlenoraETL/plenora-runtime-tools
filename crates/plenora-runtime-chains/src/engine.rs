//! Single coordinator for Rust and transport facades; adapters own domain evidence.
use crate::{
    Error, Journal, Reservation, RunHandle, RunOptions, Store, budget,
    contracts::{
        ChainError, ChainResult, ChainResultRecovery, Event, Input, Plan, Registry, Retry, Retry0,
        Source, Step, StepResult, ValidationReport, canonical_plan, parse_event, parse_plan,
        parse_registry, parse_result, validate_plan, validate_result,
    },
    encode,
    journal::{DELIVERY_MS, Intent},
};
use async_trait::async_trait;
use plenora_runtime_core::Clock;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::UNIX_EPOCH,
};

/// Fully resolved invocation, authorized only after the durable intention is confirmed.
pub struct Invocation {
    /// Authenticated tenant; never taken from arbitrary message headers.
    pub tenant: String,
    /// Stable logical run.
    pub run_id: String,
    /// Durable message identity for this attempt.
    pub message_id: String,
    /// Digest of the complete immutable plan confirmed by the trusted host.
    pub plan_digest: String,
    /// Exact step, including scoped consent and destination.
    pub step: Step,
    /// Complete immutable input manifests.
    pub inputs: BTreeMap<String, Input>,
    /// Global reservation available to this attempt.
    pub reservation: Reservation,
    /// Original invocation start, unchanged during reconciliation.
    pub started_ms: u64,
    /// One-based logical step attempt; each attempt has its own durable carrier.
    pub attempt: u64,
}
/// Trusted adapter boundary. Implementations must verify resources and preserve complete evidence.
/// Scripted implementations prove coordination only, never real provider behavior.
#[async_trait]
pub trait Adapter: Send + Sync + 'static {
    /// Verify one complete-plan confirmation in the authenticated tenant context.
    /// Default deny: a document containing loss grants is not itself authorization.
    async fn authorize(&self, _tenant: &str, _plan: &Plan) -> Result<(), Error> {
        Err(Error::Invalid)
    }
    /// Read an authorized immutable resource with a finite host limit.
    async fn artifact(
        &self,
        _tenant: &str,
        _reference: &str,
        _limit: usize,
    ) -> Result<Vec<u8>, Error> {
        Err(Error::Unsupported)
    }
    /// Check authorization, discovery, schema/fidelity, artifact integrity and reservation.
    /// This is read-only and must not invoke a domain operation.
    async fn prepare(&self, invocation: &Invocation) -> Result<(), Error>;
    /// Invoke through the protected runtime dispatcher. The key in `RunOptions` is not forwarded.
    /// Counters describe this attempt (`max_attempts=1`); the coordinator aggregates the public result.
    /// An error means no complete evidence was confirmed, not that effects were absent.
    async fn invoke(&self, invocation: &Invocation) -> Result<StepResult, Error>;
    /// Ask the running provider to stop; acceptance is not confirmation of termination.
    async fn request_cancel(&self, invocation: &Invocation) -> Result<(), Error>;
    /// Reconcile the original invocation using authenticated evidence. Must not start it again.
    async fn recover(&self, invocation: &Invocation) -> Result<Option<StepResult>, Error>;
}
/// Sequential engine over a durable primary and frozen registry.
pub struct ChainEngine<S: plenora_runtime_boundary::DurableExecutionStore> {
    store: Arc<S>,
    registry: Registry,
    clock: Arc<dyn Clock>,
    tenant: String,
    executor: Arc<crate::execution::StepExecutor<S>>,
    lease: Option<crate::RunLease>,
}
impl<S: Store + plenora_runtime_boundary::DurableExecutionStore> ChainEngine<S> {
    /// Bind trusted tenant policy and a frozen independently obtained registry.
    /// # Errors
    /// Refuses noncanonical tenant scope or malformed registry.
    pub fn new<A: Adapter>(
        store: Arc<S>,
        adapter: Arc<A>,
        registry: Registry,
        clock: Arc<dyn Clock>,
        tenant: &str,
    ) -> Result<Self, Error> {
        if !crate::scope(tenant) {
            return Err(Error::Invalid);
        }
        parse_registry(&encode(&registry)?)?;
        let executor = crate::execution::StepExecutor::new(
            store.clone(),
            adapter,
            &registry,
            clock.clone(),
            tenant,
        )?;
        Ok(Self {
            executor: Arc::new(executor),
            lease: None,
            store,
            registry,
            clock,
            tenant: tenant.into(),
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
        .map_err(|_| Error::Invalid)
    }
    /// Claim and advance one persisted run independently of any submitted request.
    /// An unfinished receipt is replayed with the same identity; Running cannot start twice.
    /// # Errors
    /// Unconfirmed ownership, stale fences and storage errors are surfaced without new effects.
    pub async fn work_once(&self, owner: &str, lease_ms: u64) -> Result<bool, Error> {
        let Some(lease) = self.store.claim_work(&self.tenant, owner, lease_ms).await? else {
            return Ok(false);
        };
        let worker = Self {
            store: self.store.clone(),
            registry: self.registry.clone(),
            clock: self.clock.clone(),
            tenant: self.tenant.clone(),
            executor: self.executor.clone(),
            lease: Some(lease.clone()),
        };
        let result = worker.work_claimed(lease.run_id()).await;
        // Uncertain active work stays leased until expiry, preventing a busy reconciliation loop.
        if !matches!(result, Err(Error::Recovery)) {
            match self.store.release_work(&lease).await {
                Ok(()) => {}
                Err(Error::Conflict) if matches!(result, Err(Error::Conflict)) => {}
                Err(e) => return Err(e),
            }
        }
        match result {
            Ok(_) | Err(Error::Pending | Error::Recovery | Error::Conflict) => Ok(true),
            Err(e) => Err(e),
        }
    }
    async fn work_claimed(&self, run_id: &str) -> Result<RunHandle, Error> {
        let j = self.load(run_id).await?;
        self.delivery(&j)?;
        if let Some(intent) = &j.active {
            let invocation = self.invocation(
                &j,
                &parse_plan(&j.plan)?,
                &parse_result(&intent.prior)?,
                intent,
            )?;
            let current = parse_result(&j.view)?;
            if current
                .steps
                .get(intent.index)
                .is_some_and(|s| s.attempt == invocation.attempt && s.remote_effect == "unknown")
            {
                self.require_recovery(&j).await?;
                return Err(Error::Recovery);
            }
            // Same durable carrier, never provider recovery or a second logical attempt.
            let evidence = self.executor.invoke(&j).await?;
            let handle = self.finish(&j, &invocation, evidence).await?;
            if handle.state == "requires_recovery" {
                Err(Error::Recovery)
            } else {
                Ok(handle)
            }
        } else {
            self.advance(run_id).await
        }
    }
    /// Exact effective fixture discovery, with no claim about real ETL libraries.
    #[must_use]
    pub const fn capabilities(&self) -> &Registry {
        &self.registry
    }
    /// Preventive validation; does not open resources or invoke adapters.
    /// # Errors
    /// Returns a data-free contract rejection.
    pub fn validate(&self, plan: &Plan) -> Result<ValidationReport, Error> {
        Ok(validate_plan(plan, &self.registry)?)
    }
    /// Persist a new logical run or reuse the exact tenant/operation/key submission.
    /// # Errors
    /// Invalid, conflicting, expired or unconfirmed submissions never authorize execution.
    pub async fn submit(&self, plan: &Plan, options: RunOptions) -> Result<RunHandle, Error> {
        let validated = self.validate(plan)?;
        if !crate::uuid(&options.run_id)
            || options.idempotency_key.as_ref().is_some_and(|k| {
                k.is_empty() || k.len() > 256 || !k.bytes().all(|b| b.is_ascii_graphic())
            })
        {
            return Err(Error::Invalid);
        }
        let canonical = canonical_plan(plan)?;
        let registry = encode(&self.registry)?;
        if let Some(previous) = self
            .store
            .lookup(
                &self.tenant,
                &options.run_id,
                options.idempotency_key.as_deref(),
            )
            .await?
        {
            previous.validate()?;
            self.delivery(&previous)?;
            if previous.tenant != self.tenant
                || previous.plan != canonical
                || previous.registry != registry
                || previous.options.idempotency_key != options.idempotency_key
                || previous.options.deadline_ms != options.deadline_ms
                || (previous.run_id() != options.run_id && options.idempotency_key.is_none())
            {
                return Err(Error::Conflict);
            }
            return previous.handle();
        }
        self.executor.authorize(&self.tenant, plan).await?;
        let reserved = plan
            .steps
            .iter()
            .try_fold(8 * 1024 * 1024_u64, |sum, step| {
                sum.checked_add(
                    step.budgets
                        .result_bytes
                        .checked_mul(32)
                        .and_then(|n| n.checked_mul(step.retry_policy.max_attempts))
                        .ok_or(Error::ResourceLimit)?,
                )
                .ok_or(Error::ResourceLimit)
            })?;
        if reserved > crate::JOURNAL_BYTES as u64 {
            return Err(Error::ResourceLimit);
        }
        let now = self.now()?;
        let deadline = now
            .checked_add(plan.budgets.elapsed_ms)
            .ok_or(Error::Invalid)?
            .min(options.deadline_ms.unwrap_or(u64::MAX));
        if deadline <= now {
            return Err(Error::Timeout);
        }
        let view = initial(plan, &validated, &options.run_id, now)?;
        let mut j = Journal {
            version: 1,
            tenant: self.tenant.clone(),
            options,
            created: now,
            deadline,
            revision: 1,
            plan: canonical,
            registry,
            view: encode(&view)?,
            active: None,
            cancelled: false,
            terminal: false,
            events: vec![],
            history: vec![],
            evidence: None,
            progress: BTreeMap::new(),
            retry_due: None,
            waiting_recovery: false,
        };
        j.event("accepted", "pending", now, None)?;
        j.to_json()?;
        self.store.create(crate::Submission(j)).await?.handle()
    }
    async fn load(&self, id: &str) -> Result<Journal, Error> {
        if !crate::uuid(id) {
            return Err(Error::Invalid);
        }
        let j = Store::load(self.store.as_ref(), &self.tenant, id).await?;
        if j.tenant != self.tenant || j.run_id() != id || j.registry != encode(&self.registry)? {
            return Err(Error::Conflict);
        }
        j.validate()?;
        Ok(j)
    }
    fn delivery(&self, j: &Journal) -> Result<u64, Error> {
        let now = self.now()?;
        if now < j.created || now - j.created >= DELIVERY_MS {
            return Err(Error::Expired);
        }
        Ok(now)
    }
    async fn save(&self, j: Journal, kind: &str, state: &str) -> Result<Journal, Error> {
        self.save_step(j, kind, state, None).await
    }
    async fn save_step(
        &self,
        mut j: Journal,
        kind: &str,
        state: &str,
        step: Option<&str>,
    ) -> Result<Journal, Error> {
        let old = j.revision;
        j.revision = old.checked_add(1).ok_or(Error::ResourceLimit)?;
        j.event(kind, state, self.now()?, step)?;
        j.to_json()?;
        self.store
            .compare_exchange(old, crate::Change(j.clone(), self.lease.clone()))
            .await?;
        Ok(j)
    }
    /// Read current authoritative state; a cancel request is never reported as confirmed stop.
    /// # Errors
    /// Fails closed on missing, expired, corrupt or unavailable storage.
    pub async fn status(&self, run_id: &str) -> Result<RunHandle, Error> {
        self.load(run_id).await?.handle()
    }
    /// Read a complete outcome, including failures; does not execute or resume work.
    /// # Errors
    /// Pending work has no terminal result; unknown active work requires reconciliation.
    pub async fn result(&self, run_id: &str) -> Result<ChainResult, Error> {
        let j = self.load(run_id).await?;
        if j.active.is_some() && parse_result(&j.view)?.state != "requires_recovery" {
            return Err(if j.waiting_recovery {
                Error::Recovery
            } else {
                Error::Pending
            });
        }
        if !j.terminal && parse_result(&j.view)?.state != "requires_recovery" {
            return Err(Error::Pending);
        }
        Ok(parse_result(&j.view)?)
    }
    /// Original evidence, including reports that could not be classified as valid results.
    /// # Errors
    /// Fails when the run is missing, expired or storage is unavailable.
    pub async fn report(&self, run_id: &str) -> Result<Vec<Vec<u8>>, Error> {
        let j = self.load(run_id).await?;
        let mut reports = j.history;
        if let Some(last) = j.evidence {
            reports.push(last);
        }
        Ok(reports)
    }
    /// Retrieve an authorized immutable artifact, bounded to one MiB per call.
    /// # Errors
    /// Rejects missing authorization, changed content or unsupported references.
    pub async fn artifact(&self, reference: &str) -> Result<Vec<u8>, Error> {
        if !reference.starts_with("ref:") || reference.len() > 256 {
            return Err(Error::Invalid);
        }
        let bytes = self
            .executor
            .artifact(&self.tenant, reference, 1024 * 1024)
            .await?;
        if bytes.len() > 1024 * 1024 {
            return Err(Error::ResourceLimit);
        }
        Ok(bytes)
    }
    /// Durable cursor page. A cursor is the last consumed sequence, starting at zero.
    /// # Errors
    /// Refuses a cursor beyond the journal or a page outside 1..=256.
    pub async fn events(
        &self,
        run_id: &str,
        cursor: u64,
        limit: usize,
    ) -> Result<Vec<Event>, Error> {
        let j = self.load(run_id).await?;
        if !(1..=256).contains(&limit) || cursor > j.events.len() as u64 {
            return Err(Error::Invalid);
        }
        j.events
            .iter()
            .skip(usize::try_from(cursor).map_err(|_| Error::Invalid)?)
            .take(limit)
            .map(|b| parse_event(b).map_err(Error::from))
            .collect()
    }
    fn invocation(
        &self,
        j: &Journal,
        plan: &Plan,
        view: &ChainResult,
        intent: &Intent,
    ) -> Result<Invocation, Error> {
        resolve_invocation(&self.tenant, j.run_id(), plan, view, intent)
    }
    /// Advance at most one sequential step. An active intention never grants another invocation.
    /// # Errors
    /// Storage uncertainty and incomplete adapter evidence require recovery, never replay.
    pub async fn advance(&self, run_id: &str) -> Result<RunHandle, Error> {
        let mut j = self.load(run_id).await?;
        let now = self.delivery(&j)?;
        if j.terminal {
            return j.handle();
        }
        if j.active.is_some() {
            return Err(Error::Recovery);
        }
        let plan = parse_plan(&j.plan)?;
        let view = parse_result(&j.view)?;
        let validation = self.validate(&plan)?;
        let id = validation
            .order
            .iter()
            .find(|id| {
                !view
                    .steps
                    .iter()
                    .any(|s| &s.step_id == *id && s.state == "succeeded")
            })
            .ok_or(Error::Invalid)?;
        let index = plan
            .steps
            .iter()
            .position(|s| &s.id == id)
            .ok_or(Error::Invalid)?;
        if j.cancelled || now >= j.deadline {
            let failure = if now >= j.deadline {
                Error::Timeout
            } else {
                Error::Pending
            };
            return self.reject(j, index, failure).await;
        }
        if j.retry_due.is_some_and(|due| now < due) {
            return Err(Error::Pending);
        }
        let progress = j
            .progress
            .entry(id.clone())
            .or_insert(crate::journal::StepProgress {
                started: now,
                deadline: j.deadline.min(
                    now.checked_add(plan.steps[index].budgets.elapsed_ms)
                        .ok_or(Error::Invalid)?,
                ),
                last_message: None,
            });
        let step_deadline = progress.deadline;
        let first_started = progress.started;
        let causation = progress.last_message.clone();
        if now >= step_deadline {
            return self.reject(j, index, Error::Timeout).await;
        }
        let intent = Intent {
            index,
            message_id: uuid::Uuid::new_v4().to_string(),
            started: now,
            prior: j.view.clone(),
            first_started: Some(first_started),
            causation,
            deadline: step_deadline,
        };
        let invocation = match self.invocation(&j, &plan, &view, &intent) {
            Ok(i) => i,
            Err(e) => return self.reject(j, index, e).await,
        };
        if let Err(e) = self.executor.prepare(&invocation).await {
            return self.reject(j, index, e).await;
        }
        if self.now()? >= invocation.reservation.deadline_ms {
            return self.reject(j, index, Error::Timeout).await;
        }
        j.progress.get_mut(id).ok_or(Error::Invalid)?.last_message =
            Some(intent.message_id.clone());
        j.retry_due = None;
        j.active = Some(intent);
        let started = self
            .save_step(j, "step_state", "running", Some(&invocation.step.id))
            .await?;
        if let Ok(evidence) = self.executor.invoke(&started).await {
            self.finish(&started, &invocation, evidence).await
        } else {
            if self.executor.confirmed_unknown(&started).await? {
                self.require_recovery(&started).await?;
            }
            Err(Error::Recovery)
        }
    }

    async fn reject(
        &self,
        mut j: Journal,
        index: usize,
        failure: Error,
    ) -> Result<RunHandle, Error> {
        let plan = parse_plan(&j.plan)?;
        let mut view = parse_result(&j.view)?;
        let step = view.steps.get_mut(index).ok_or(Error::Invalid)?;
        if j.cancelled {
            step.state = "cancelled".into();
            step.stop_confirmed = true;
            step.reason = "stopped".into();
        } else {
            step.state = if step.attempt == 0 {
                "rejected"
            } else {
                "failed"
            }
            .into();
            step.reason = if step.attempt == 0 {
                "not_started"
            } else {
                "upstream_error"
            }
            .into();
            step.error = Some(problem(failure, "none", Some(&step.step_id)));
        }
        if let Some(progress) = j.progress.get(&step.step_id) {
            step.counters.elapsed_ms = step.counters.elapsed_ms.max(
                self.now()?
                    .checked_sub(progress.started)
                    .ok_or(Error::Invalid)?,
            );
            if self.now()? >= progress.deadline
                && step.error.as_ref().is_none_or(|e| e.category != "timeout")
            {
                step.other_errors.push(problem(
                    Error::Timeout,
                    &step.remote_effect,
                    Some(&step.step_id),
                ));
            }
        }
        step.retry = retry("never");
        summarize(&mut view, &plan, self.now()?, j.revision + 1)?;
        j.view = encode(&view)?;
        j.terminal = true;
        j.retry_due = None;
        self.save(j, "result_persisted", &view.state)
            .await?
            .handle()
    }
    async fn record_evidence(
        &self,
        started: &Journal,
        invocation: &Invocation,
        raw: Vec<u8>,
    ) -> Result<(), Error> {
        let intent = started.active.as_ref().ok_or(Error::Invalid)?;
        let mut recorded = false;
        for _ in 0..16 {
            let mut j = self.load(started.run_id()).await?;
            if j.active
                .as_ref()
                .is_none_or(|a| a.message_id != intent.message_id)
            {
                return Err(Error::Conflict);
            }
            if j.evidence.as_ref() == Some(&raw) {
                recorded = true;
                break;
            }
            if let Some(previous) = j.evidence.replace(raw.clone()) {
                j.history.push(previous);
            }
            match self
                .save_step(j, "step_state", "finalizing", Some(&invocation.step.id))
                .await
            {
                Ok(_) => {
                    recorded = true;
                    break;
                }
                Err(Error::Conflict) => {
                    if self.lease.is_some() {
                        return Err(Error::Conflict);
                    }
                }
                Err(e) => return Err(e),
            }
        }
        if !recorded {
            return Err(Error::Storage);
        }
        Ok(())
    }
    async fn require_recovery(&self, started: &Journal) -> Result<(), Error> {
        let mut current = self.load(started.run_id()).await?;
        if current.active.as_ref().map(|i| &i.message_id)
            != started.active.as_ref().map(|i| &i.message_id)
        {
            return Err(Error::Conflict);
        }
        if !current.waiting_recovery {
            current.waiting_recovery = true;
            self.save(current, "recovery_required", "requires_recovery")
                .await?;
        }
        Ok(())
    }
    async fn finish(
        &self,
        started: &Journal,
        invocation: &Invocation,
        evidence: StepResult,
    ) -> Result<RunHandle, Error> {
        match self.complete(started, invocation, evidence).await {
            Err(Error::Recovery) => {
                self.require_recovery(started).await?;
                Err(Error::Recovery)
            }
            result => result,
        }
    }
    async fn complete(
        &self,
        started: &Journal,
        invocation: &Invocation,
        mut evidence: StepResult,
    ) -> Result<RunHandle, Error> {
        let plan = parse_plan(&started.plan)?;
        let intent = started.active.as_ref().ok_or(Error::Invalid)?;
        self.record_evidence(started, invocation, encode(&evidence)?)
            .await?;
        let now = self.now()?;
        if now >= invocation.reservation.deadline_ms {
            evidence.counters.elapsed_ms = now
                .checked_sub(invocation.started_ms)
                .ok_or(Error::Invalid)?;
            let mut timeout = problem(
                Error::Timeout,
                &evidence.remote_effect,
                Some(&evidence.step_id),
            );
            timeout.phase = "finalize".into();
            if evidence.error.is_some() {
                evidence.other_errors.push(timeout);
            } else {
                evidence.error = Some(timeout);
            }
            if evidence.state == "succeeded" {
                evidence.state = "failed".into();
                evidence.reason = "upstream_error".into();
            }
        }
        let mut accounted = evidence.counters.clone();
        // Exact wall-clock overruns remain in evidence and require a timeout outcome.
        accounted.elapsed_ms = 0;
        if evidence.step_id != invocation.step.id
            || evidence.attempt != invocation.attempt
            || evidence.input_manifests != invocation.inputs
            || !budget::within(&accounted, &invocation.reservation.limits)
            || evidence.counters.max_attempts != 1
        {
            return Err(Error::Recovery);
        }
        let prior = parse_result(&intent.prior)?;
        evidence = crate::retry::cumulative(
            prior.steps.get(intent.index).ok_or(Error::Invalid)?,
            &evidence,
            now.checked_sub(intent.first_started.unwrap_or(intent.started))
                .ok_or(Error::Invalid)?,
        )?;
        // Re-read after invocation: cancellation may have committed while the provider was running.
        // A revision conflict retries persistence only, never the domain call.
        for _ in 0..16 {
            let mut j = self.load(started.run_id()).await?;
            if j.active
                .as_ref()
                .is_none_or(|a| a.message_id != intent.message_id)
            {
                return Err(Error::Conflict);
            }
            let mut view = parse_result(&j.view)?;
            *view.steps.get_mut(intent.index).ok_or(Error::Invalid)? = evidence.clone();
            summarize(&mut view, &plan, self.now()?, j.revision + 1)?;
            validate_result(&view, &plan, &self.registry).map_err(|_| Error::Recovery)?;
            j.view = encode(&view)?;
            let unknown = evidence.remote_effect == "unknown";
            j.waiting_recovery = unknown;
            if !unknown {
                j.active = None;
            }
            j.retry_due = crate::retry::backoff(&evidence, &invocation.step)
                .and_then(|wait| now.checked_add(wait))
                .filter(|due| *due < intent.deadline && *due < j.deadline);
            if j.cancelled || view.counters.max_attempts >= plan.budgets.max_attempts {
                j.retry_due = None;
            }
            j.terminal = !unknown
                && j.retry_due.is_none()
                && (evidence.state != "succeeded" || view.state == "succeeded");
            let kind = if j.terminal {
                "result_persisted"
            } else {
                "step_state"
            };
            let step = (!j.terminal).then_some(evidence.step_id.as_str());
            let state = if j.terminal {
                &view.state
            } else {
                &evidence.state
            };
            match self.save_step(j, kind, state, step).await {
                Err(Error::Conflict) => {
                    if self.lease.is_some() {
                        return Err(Error::Conflict);
                    }
                }
                result => return result?.handle(),
            }
        }
        Err(Error::Storage)
    }
    /// Drive the same submit/advance path until a terminal outcome is durably available.
    /// # Errors
    /// Unknown work is returned as recovery, not retried. Failed domain results remain inspectable.
    pub async fn run(&self, plan: &Plan, options: RunOptions) -> Result<ChainResult, Error> {
        let handle = self.submit(plan, options).await?;
        loop {
            match self.result(&handle.run_id).await {
                Ok(result) => return Ok(result),
                Err(Error::Pending) => match self.advance(&handle.run_id).await {
                    Ok(_) => {}
                    Err(Error::Recovery) => {
                        let current = self.load(&handle.run_id).await?;
                        if current.waiting_recovery || self.now()? >= current.deadline {
                            return Err(Error::Recovery);
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    Err(Error::Pending | Error::Conflict) => {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    Err(e) => return Err(e),
                },
                Err(e) => return Err(e),
            }
        }
    }
    /// Persist a versioned stop request before contacting the provider.
    /// # Errors
    /// A stale revision conflicts; provider request failure leaves the durable request visible.
    pub async fn cancel(&self, run_id: &str, expected: u64) -> Result<RunHandle, Error> {
        let mut j = self.load(run_id).await?;
        self.delivery(&j)?;
        if j.revision != expected {
            return Err(Error::Conflict);
        }
        if j.terminal {
            return j.handle();
        }
        let j = if j.cancelled {
            j
        } else {
            j.cancelled = true;
            self.save(j, "cancel_requested", "cancel_requested").await?
        };
        if let Some(intent) = &j.active {
            let _ = intent;
            self.executor.cancel(&j).await?;
        }
        j.handle()
    }
    /// Reconcile active work through the trusted adapter; never reinvoke an unknown intention.
    /// Successful artifacts are revalidated by `prepare` before any subsequent step.
    /// # Errors
    /// Missing recovery proof, stale revisions and expired windows fail closed.
    pub async fn resume(&self, run_id: &str, expected: u64) -> Result<RunHandle, Error> {
        let j = self.load(run_id).await?;
        self.delivery(&j)?;
        if j.revision != expected {
            return Err(Error::Conflict);
        }
        if let Some(intent) = &j.active {
            let invocation = self.invocation(
                &j,
                &parse_plan(&j.plan)?,
                &parse_result(&intent.prior)?,
                intent,
            )?;
            if let Some(bytes) = &j.evidence
                && let Ok(evidence) = crate::contracts::parse_step_result(bytes)
                && evidence.remote_effect != "unknown"
                && evidence.step_id == invocation.step.id
                && evidence.attempt == invocation.attempt
            {
                return self.complete(&j, &invocation, evidence).await;
            }
            let evidence = self.executor.recover(&j).await?.ok_or(Error::Recovery)?;
            return self.complete(&j, &invocation, evidence).await;
        }
        if j.terminal {
            return j.handle();
        }
        self.advance(run_id).await
    }
}

fn retry(kind: &str) -> Retry {
    Retry::Variant0(Retry0 { kind: kind.into() })
}
fn problem(error: Error, effect: &str, step: Option<&str>) -> ChainError {
    ChainError {
        contract: "plenora-chain-error-v2".into(),
        schema_version: 2,
        category: match error {
            Error::ResourceLimit => "resource_limit",
            Error::Timeout => "timeout",
            Error::Unsupported => "unsupported",
            _ => "execution",
        }
        .into(),
        phase: "validate".into(),
        code: "CHAIN_UPSTREAM".into(),
        remote_effect: effect.into(),
        retry: retry(if effect == "unknown" {
            "requires_recovery"
        } else {
            "never"
        }),
        message: "Chain operation rejected or incomplete.".into(),
        step_id: step.map(str::to_owned),
        upstream_error: None,
        reports: vec![],
    }
}
fn initial(
    plan: &Plan,
    validation: &ValidationReport,
    id: &str,
    now: u64,
) -> Result<ChainResult, Error> {
    Ok(ChainResult {
        contract: "plenora-chain-result-v2".into(),
        schema_version: 2,
        run_id: id.into(),
        result_message_id: uuid::Uuid::new_v4().to_string(),
        plan_id: plan.plan_id.clone(),
        plan_revision: plan.revision,
        plan_digest: validation.plan_digest.clone(),
        state_revision: 1,
        state: "rejected".into(),
        started_at: crate::timestamp(now)?,
        finished_at: crate::timestamp(now)?,
        counters: budget::zero(),
        steps: plan
            .steps
            .iter()
            .map(|s| StepResult {
                step_id: s.id.clone(),
                operation: s.operation.clone(),
                operation_version: s.operation_version,
                attempt: 0,
                state: "skipped".into(),
                input_manifests: BTreeMap::new(),
                outputs: BTreeMap::new(),
                remote_effect: "none".into(),
                retry: retry("never"),
                error: None,
                other_errors: vec![],
                reports: vec![],
                counters: budget::zero(),
                started_at: None,
                finished_at: None,
                stop_confirmed: false,
                reason: "not_started".into(),
                upstream_result: None,
            })
            .collect(),
        outputs: BTreeMap::new(),
        error: Some(problem(Error::Pending, "none", None)),
        other_errors: vec![],
        remote_effect: "none".into(),
        retry: retry("never"),
        reports: vec![],
        reports_complete: true,
        losses: "none".into(),
        recovery: ChainResultRecovery {
            required: false,
            evidence_ref: None,
            resumable_steps: vec![],
        },
        delivery: "persisted".into(),
    })
}
fn view_time(view: &ChainResult) -> Result<u64, Error> {
    u64::try_from(
        chrono::DateTime::parse_from_rfc3339(&view.started_at)
            .map_err(|_| Error::Invalid)?
            .timestamp_millis(),
    )
    .map_err(|_| Error::Invalid)
}
#[allow(
    clippy::too_many_lines,
    reason = "Keep conservative outcome axes and manifest aggregation auditable in one reducer"
)]
fn summarize(view: &mut ChainResult, plan: &Plan, now: u64, revision: u64) -> Result<(), Error> {
    let unknown = view.steps.iter().any(|s| s.remote_effect == "unknown");
    let all = view.steps.iter().all(|s| s.state == "succeeded");
    let any = view.steps.iter().any(|s| s.state == "succeeded");
    let cancelled = view.steps.iter().any(|s| s.state == "cancelled");
    view.state = if unknown {
        "requires_recovery"
    } else if all {
        "succeeded"
    } else if cancelled {
        "cancelled"
    } else if any || view.steps.iter().any(|s| s.state == "partial") {
        "partial"
    } else if view.steps.iter().any(|s| s.attempt > 0) {
        "failed"
    } else {
        "rejected"
    }
    .into();
    let committed = view.steps.iter().any(|s| s.remote_effect == "committed");
    view.remote_effect = if unknown {
        "unknown"
    } else if view.steps.iter().any(|s| s.remote_effect == "partial") || (committed && !all) {
        "partial"
    } else if committed {
        "committed"
    } else {
        "none"
    }
    .into();
    view.retry = retry(if unknown {
        "requires_recovery"
    } else {
        "never"
    });
    view.error = if all || cancelled {
        None
    } else {
        Some(
            view.steps
                .iter()
                .find_map(|s| s.error.clone())
                .unwrap_or_else(|| problem(Error::Pending, &view.remote_effect, None)),
        )
    };
    view.recovery.required = unknown;
    view.recovery.resumable_steps = view
        .steps
        .iter()
        .filter(|s| {
            s.remote_effect == "unknown"
                || plan
                    .steps
                    .iter()
                    .find(|d| d.id == s.step_id)
                    .is_some_and(|d| crate::retry::backoff(s, d).is_some())
        })
        .map(|s| s.step_id.clone())
        .collect();
    view.state_revision = revision;
    view.finished_at = crate::timestamp(now)?;
    view.counters = view
        .steps
        .iter()
        .try_fold(budget::zero(), |a, s| budget::add(&a, &s.counters))?;
    view.counters.elapsed_ms = now.checked_sub(view_time(view)?).ok_or(Error::Invalid)?;
    view.outputs.clear();
    for (name, output) in &plan.outputs {
        if let Source::Variant1(source) = &output.source
            && let Some(a) = view
                .steps
                .iter()
                .find(|s| s.step_id == source.step)
                .and_then(|s| s.outputs.get(&source.port))
        {
            view.outputs.insert(name.clone(), a.clone());
        }
    }
    let mut blocked: BTreeSet<String> = view
        .steps
        .iter()
        .filter(|s| !matches!(s.state.as_str(), "succeeded" | "skipped" | "blocked"))
        .map(|s| s.step_id.clone())
        .collect();
    loop {
        let before = blocked.len();
        for step in &plan.steps {
            if step.depends_on.iter().any(|s| blocked.contains(s))
                || step
                    .inputs
                    .values()
                    .any(|b| matches!(&b.source, Source::Variant1(s) if blocked.contains(&s.step)))
            {
                blocked.insert(step.id.clone());
            }
        }
        if before == blocked.len() {
            break;
        }
    }
    for step in &mut view.steps {
        if step.attempt == 0 && matches!(step.state.as_str(), "skipped" | "blocked") {
            step.state = if blocked.contains(&step.step_id) {
                "blocked"
            } else {
                "skipped"
            }
            .into();
        }
    }
    let reports: Vec<_> = view
        .steps
        .iter()
        .flat_map(|s| {
            s.reports
                .iter()
                .chain(s.outputs.values().flat_map(|a| &a.reports))
                .chain(s.error.iter().flat_map(|e| &e.reports))
                .chain(s.other_errors.iter().flat_map(|e| &e.reports))
        })
        .collect();
    view.reports_complete = reports.iter().all(|r| r.availability != "unavailable");
    view.losses = if !view.reports_complete {
        "unavailable"
    } else if reports.is_empty() {
        "none"
    } else {
        "present"
    }
    .into();
    Ok(())
}

pub(crate) fn resolve_invocation(
    tenant: &str,
    run_id: &str,
    plan: &Plan,
    view: &ChainResult,
    intent: &Intent,
) -> Result<Invocation, Error> {
    let step = plan.steps.get(intent.index).ok_or(Error::Invalid)?.clone();
    let mut inputs = BTreeMap::new();
    for (name, binding) in &step.inputs {
        let input = match &binding.source {
            Source::Variant0(s) => plan.inputs.get(&s.input).ok_or(Error::Invalid)?.clone(),
            Source::Variant1(s) => {
                let producer = view
                    .steps
                    .iter()
                    .find(|p| p.step_id == s.step && p.state == "succeeded")
                    .ok_or(Error::Recovery)?;
                let a = producer.outputs.get(&s.port).ok_or(Error::Recovery)?;
                Input {
                    artifact_ref: a.artifact_ref.clone(),
                    port: a.port.clone(),
                    bytes: a.bytes,
                    sha256: a.sha256.clone(),
                }
            }
        };
        inputs.insert(name.clone(), input);
    }
    // All handoffs remain retained for recovery. Aliased fan-out never copies a manifest's bytes.
    let mut retained = BTreeMap::new();
    for input in plan.inputs.values() {
        if let Some((bytes, digest)) =
            retained.insert(&input.artifact_ref, (input.bytes, &input.sha256))
            && (bytes != input.bytes || digest != &input.sha256)
        {
            return Err(Error::Conflict);
        }
    }
    for a in view.steps.iter().flat_map(|s| s.outputs.values()) {
        if let Some((bytes, digest)) = retained.insert(&a.artifact_ref, (a.bytes, &a.sha256))
            && (bytes != a.bytes || digest != &a.sha256)
        {
            return Err(Error::Conflict);
        }
    }
    let retained = retained.values().try_fold(0_u64, |sum, (bytes, _)| {
        sum.checked_add(*bytes).ok_or(Error::ResourceLimit)
    })?;
    let reservation = Reservation::new(
        &plan.budgets,
        &step.budgets,
        &view.counters,
        &view.steps.get(intent.index).ok_or(Error::Invalid)?.counters,
        retained,
        intent.started,
        intent.deadline,
    )?;
    Ok(Invocation {
        tenant: tenant.to_owned(),
        run_id: run_id.to_owned(),
        message_id: intent.message_id.clone(),
        plan_digest: crate::contracts::plan_digest(plan)?,
        step,
        inputs,
        reservation,
        started_ms: intent.started,
        attempt: view
            .steps
            .get(intent.index)
            .ok_or(Error::Invalid)?
            .attempt
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?,
    })
}
