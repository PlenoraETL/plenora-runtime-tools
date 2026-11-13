//! Immutable identity and fenced snapshots. Storage implementations own atomicity and retention.
use crate::{
    Error,
    contracts::{
        Event, parse_event, parse_plan, parse_registry, parse_result, validate_event,
        validate_result,
    },
    encode,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Delivery window, separately enforced from report retention.
pub const DELIVERY_MS: u64 = 604_800_000;
/// Complete reports are retained for ninety days; identities are permanent.
pub const REPORT_MS: u64 = 7_776_000_000;
/// Hard snapshot bound, including immutable attempt history and events.
pub const JOURNAL_BYTES: usize = 64 * 1024 * 1024;

/// Immutable submission controls supplied by the authenticated host.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunOptions {
    /// Canonical logical run UUID, generated once by the caller before submission.
    pub run_id: String,
    /// Optional host-level key; never forwarded as a provider idempotency control.
    pub idempotency_key: Option<String>,
    /// Absolute caller deadline; a wait timeout must never replace this value.
    pub deadline_ms: Option<u64>,
}
impl std::fmt::Debug for RunOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RunOptions(<redacted>)")
    }
}
/// Public durable receipt; acceptance does not imply completion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunHandle {
    /// Logical execution identity.
    pub run_id: String,
    /// Compare-and-swap revision.
    pub revision: u64,
    /// Explicit execution state.
    pub state: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Intent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_limit: Option<u64>,
    pub index: usize,
    pub message_id: String,
    pub started: u64,
    pub deadline: u64,
    pub prior: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_started: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causation: Option<String>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StepProgress {
    pub started: u64,
    pub deadline: u64,
    pub last_message: Option<String>,
}
/// Opaque authoritative snapshot. Only the coordinator can construct transitions.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Journal {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) allocation: Option<crate::accounting::Allocation>,
    pub(crate) version: u8,
    pub(crate) tenant: String,
    pub(crate) options: RunOptions,
    pub(crate) created: u64,
    pub(crate) deadline: u64,
    pub(crate) revision: u64,
    pub(crate) plan: Vec<u8>,
    pub(crate) registry: Vec<u8>,
    pub(crate) view: Vec<u8>,
    pub(crate) active: Option<Intent>,
    pub(crate) cancelled: bool,
    pub(crate) terminal: bool,
    pub(crate) events: Vec<Vec<u8>>,
    pub(crate) history: Vec<Vec<u8>>,
    pub(crate) evidence: Option<Vec<u8>>,
    #[serde(default)]
    pub(crate) progress: std::collections::BTreeMap<String, StepProgress>,
    #[serde(default)]
    pub(crate) retry_due: Option<u64>,
    #[serde(default)]
    pub(crate) waiting_recovery: bool,
}
impl std::fmt::Debug for Journal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Journal(<redacted>)")
    }
}
impl Journal {
    /// Authenticated tenant, independent of run identity and untrusted message metadata.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    /// Stable run UUID.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.options.run_id
    }
    /// Optional exact key in the tenant/chain.submit@2 scope.
    #[must_use]
    pub fn idempotency_key(&self) -> Option<&str> {
        self.options.idempotency_key.as_deref()
    }
    /// Revision used by primary CAS.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
    /// Acceptance time used for retention; never renewed by resume.
    #[must_use]
    pub const fn created_ms(&self) -> u64 {
        self.created
    }
    /// Only the monotone cancel request may cross another coordinator's active lease.
    /// This preserves every byte of intent, result, budget and scheduling state.
    #[must_use]
    pub fn is_cancel_successor(&self, next: &Self) -> bool {
        if self.cancelled
            || !next.cancelled
            || self.revision.checked_add(1) != Some(next.revision)
            || next.events.len() != self.events.len() + 1
            || !next.events.starts_with(&self.events)
            || next
                .events
                .last()
                .and_then(|b| parse_event(b).ok())
                .is_none_or(|e| e.kind != "cancel_requested")
        {
            return false;
        }
        let mut comparable = next.clone();
        comparable.cancelled = self.cancelled;
        comparable.revision = self.revision;
        comparable.events.clone_from(&self.events);
        matches!((encode(self),encode(&comparable)),(Ok(a),Ok(b)) if a==b)
    }
    /// Whether this successor introduces a new domain intention.
    #[must_use]
    pub fn starts_invocation_after(&self, previous: &Self) -> bool {
        self.active.is_some() && previous.active.is_none()
    }
    /// Compare the entire effective submission, excluding the candidate's generated identity.
    #[must_use]
    pub fn same_submission(&self, other: &Self) -> bool {
        self.tenant == other.tenant
            && self.plan == other.plan
            && self.registry == other.registry
            && self.options.idempotency_key == other.options.idempotency_key
            && self.options.deadline_ms == other.options.deadline_ms
    }
    /// Verify immutable identity and monotone evidence before a storage CAS.
    /// # Errors
    /// Refuses rewritten history, deadline extension or reactivation of completed work.
    pub fn validate_successor(&self, next: &Self) -> Result<(), Error> {
        self.validate()?;
        next.validate()?;
        if self.allocation.as_ref().is_some_and(|a| {
            next.allocation
                .as_ref()
                .is_none_or(|b| a.limit != b.limit || b.events < a.events || b.history < a.history)
        }) {
            return Err(Error::Conflict);
        }
        if !self.same_submission(next)
            || self.run_id() != next.run_id()
            || self.created != next.created
            || self.deadline != next.deadline
            || self.revision.checked_add(1) != Some(next.revision)
            || (self.cancelled && !next.cancelled)
            || self.terminal
            || !next.events.starts_with(&self.events)
            || next.events.len() != self.events.len() + 1
            || !next.history.starts_with(&self.history)
        {
            return Err(Error::Conflict);
        }
        if self.progress.iter().any(|(id, old)| {
            next.progress
                .get(id)
                .is_none_or(|new| new.started != old.started || new.deadline != old.deadline)
        }) {
            return Err(Error::Conflict);
        }
        if let Some(old) = &self.allocation {
            let new = next.allocation.as_ref().ok_or(Error::Conflict)?;
            if old.recovery.is_some()
                && new.recovery.is_some()
                && (old.recovery != new.recovery || old.recovery_limit != new.recovery_limit)
            {
                return Err(Error::Conflict);
            }
        }
        if let (Some(old), Some(new)) = (&self.active, &next.active)
            && encode(old)? != encode(new)?
        {
            return Err(Error::Conflict);
        }
        if self.active.is_none() && next.active.is_some() && self.cancelled {
            return Err(Error::Conflict);
        }
        let before = parse_result(&self.view)?;
        let after = parse_result(&next.view)?;
        if before.result_message_id != after.result_message_id {
            return Err(Error::Conflict);
        }
        for step in &before.steps {
            if step.state == "succeeded" && !after.steps.contains(step) {
                return Err(Error::Conflict);
            }
        }
        self.schedule_successor(next, &before)?;
        self.transition(next, &before, &after)?;
        Ok(())
    }
    fn schedule_successor(
        &self,
        next: &Self,
        before: &crate::contracts::ChainResult,
    ) -> Result<(), Error> {
        let starting = self.active.is_none() && next.active.is_some();
        let plan = parse_plan(&self.plan)?;
        for (id, old) in &self.progress {
            let new = next.progress.get(id).ok_or(Error::Conflict)?;
            if new.last_message != old.last_message
                && (!starting
                    || next
                        .active
                        .as_ref()
                        .and_then(|i| plan.steps.get(i.index))
                        .is_none_or(|s| &s.id != id))
            {
                return Err(Error::Conflict);
            }
        }
        if starting {
            let intent = next.active.as_ref().ok_or(Error::Conflict)?;
            if intent.result_limit
                != Some(
                    self.report_limit(&plan.steps.get(intent.index).ok_or(Error::Conflict)?.id)?,
                )
            {
                return Err(Error::Conflict);
            }
            let definition = plan.steps.get(intent.index).ok_or(Error::Conflict)?;
            let step = before.steps.get(intent.index).ok_or(Error::Conflict)?;
            let previous = self
                .progress
                .get(&definition.id)
                .and_then(|p| p.last_message.as_ref());
            if intent.causation.as_ref() != previous
                || previous == Some(&intent.message_id)
                || (step.attempt > 0
                    && (self.retry_due.is_none_or(|due| intent.started < due)
                        || crate::retry::backoff(step, definition).is_none()))
            {
                return Err(Error::Conflict);
            }
        }
        Ok(())
    }
    fn transition(
        &self,
        next: &Self,
        before: &crate::contracts::ChainResult,
        after: &crate::contracts::ChainResult,
    ) -> Result<(), Error> {
        if self.evidence != next.evidence {
            let mut expected = self.history.clone();
            if let Some(bytes) = &self.evidence {
                expected.push(bytes.clone());
            }
            if next.history != expected
                || next.evidence.is_none()
                || self.active.is_none()
                || next.active.is_none()
                || self.view != next.view
            {
                return Err(Error::Conflict);
            }
        } else if self.history != next.history {
            return Err(Error::Conflict);
        }
        match (&self.active, &next.active) {
            (Some(intent), None) => {
                let raw = self.evidence.as_ref().ok_or(Error::Conflict)?;
                let original =
                    crate::contracts::parse_step_result(raw).map_err(|_| Error::Conflict)?;
                let completed = after.steps.get(intent.index).ok_or(Error::Conflict)?;
                if !same_evidence(
                    completed,
                    &crate::retry::cumulative(
                        parse_result(&intent.prior)?
                            .steps
                            .get(intent.index)
                            .ok_or(Error::Conflict)?,
                        &original,
                        0,
                    )?,
                    crate::retry::elapsed(intent, next.events.last().ok_or(Error::Conflict)?)?,
                ) || completed.remote_effect == "unknown"
                    || completed.attempt == 0
                    || self.evidence != next.evidence
                {
                    return Err(Error::Conflict);
                }
                for (index, (old, new)) in before.steps.iter().zip(&after.steps).enumerate() {
                    if index != intent.index && old != new {
                        let mut allowed = old.clone();
                        if old.attempt == 0
                            && matches!(old.state.as_str(), "skipped" | "blocked")
                            && matches!(new.state.as_str(), "skipped" | "blocked")
                        {
                            allowed.state.clone_from(&new.state);
                        }
                        if &allowed != new {
                            return Err(Error::Conflict);
                        }
                    }
                }
            }
            (None, Some(intent)) => {
                let plan = parse_plan(&self.plan)?;
                let order =
                    crate::contracts::validate_plan(&plan, &parse_registry(&self.registry)?)?.order;
                let first = order
                    .iter()
                    .find(|id| {
                        before
                            .steps
                            .iter()
                            .any(|s| &s.step_id == *id && s.state != "succeeded")
                    })
                    .ok_or(Error::Conflict)?;
                if before.steps.get(intent.index).is_none_or(|s| {
                    &s.step_id != first || (s.attempt != 0 && self.retry_due.is_none())
                }) || intent.prior != self.view
                    || self.view != next.view
                    || self.evidence != next.evidence
                {
                    return Err(Error::Conflict);
                }
            }
            (None, None) => {
                if before
                    .steps
                    .iter()
                    .zip(&after.steps)
                    .any(|(old, new)| old.attempt != new.attempt || old.outputs != new.outputs)
                {
                    return Err(Error::Conflict);
                }
            }
            (Some(_), Some(_)) => {}
        }
        Ok(())
    }
    /// Encode a bounded snapshot, validating all public documents before commit.
    /// # Errors
    /// Corrupt or oversized state is never silently persisted.
    pub fn to_json(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        self.encoded_size()?;
        let bytes = encode(self)?;
        if bytes.len() > JOURNAL_BYTES {
            return Err(Error::ResourceLimit);
        }
        Ok(bytes)
    }
    /// Restore persisted state and recheck closed contracts and sequence continuity.
    /// # Errors
    /// Rejects corrupt, incompatible or oversized snapshots.
    pub fn from_json(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > JOURNAL_BYTES {
            return Err(Error::ResourceLimit);
        }
        let j: Self = serde_json::from_slice(bytes).map_err(|_| Error::Storage)?;
        if j.allocation.as_ref().is_some_and(|a| bytes.len() > a.limit) {
            return Err(Error::ResourceLimit);
        }
        j.validate()?;
        Ok(j)
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.check_allocation()?;
        if self.version != 1
            || !crate::scope(&self.tenant)
            || !crate::uuid(self.run_id())
            || self.revision == 0
            || self.deadline <= self.created
            || self.options.idempotency_key.as_ref().is_some_and(|k| {
                k.is_empty() || k.len() > 256 || !k.bytes().all(|b| b.is_ascii_graphic())
            })
        {
            return Err(Error::Invalid);
        }
        let plan = parse_plan(&self.plan)?;
        let registry = parse_registry(&self.registry)?;
        let view = parse_result(&self.view)?;
        validate_result(&view, &plan, &registry)?;
        if view.run_id != self.options.run_id
            || view.result_message_id == view.run_id
            || view.state_revision > self.revision
            || self.events.len() > 16384
            || self.history.len() > 1024
        {
            return Err(Error::Invalid);
        }
        if let Some(active) = &self.active
            && (self.terminal
                || active.index >= plan.steps.len()
                || !crate::uuid(&active.message_id)
                || active.started < self.created
                || active.deadline > self.deadline
                || active.deadline <= active.started)
        {
            return Err(Error::Invalid);
        }
        if self.progress.iter().any(|(id, p)| {
            !plan.steps.iter().any(|s| &s.id == id)
                || p.deadline > self.deadline
                || p.deadline <= p.started
                || p.started < self.created
                || p.last_message.as_ref().is_some_and(|id| !crate::uuid(id))
        }) || (self.retry_due.is_some() && (self.active.is_some() || self.terminal))
        {
            return Err(Error::Invalid);
        }
        if self.waiting_recovery && self.active.is_none() {
            return Err(Error::Invalid);
        }
        if let Some(due) = self.retry_due {
            let pending = view
                .steps
                .iter()
                .find(|s| s.state != "succeeded")
                .ok_or(Error::Invalid)?;
            let definition = plan
                .steps
                .iter()
                .find(|s| s.id == pending.step_id)
                .ok_or(Error::Invalid)?;
            let progress = self.progress.get(&pending.step_id).ok_or(Error::Invalid)?;
            if crate::retry::backoff(pending, definition).is_none()
                || due >= progress.deadline
                || due < progress.started
                || view.counters.max_attempts >= plan.budgets.max_attempts
            {
                return Err(Error::Invalid);
            }
        }
        if let Some(active) = &self.active {
            if let Some(first) = active.first_started {
                let id = &plan.steps.get(active.index).ok_or(Error::Invalid)?.id;
                let progress = self.progress.get(id).ok_or(Error::Invalid)?;
                if progress.started != first
                    || progress.deadline != active.deadline
                    || progress.last_message.as_ref() != Some(&active.message_id)
                    || active
                        .causation
                        .as_ref()
                        .is_some_and(|id| !crate::uuid(id) || id == &active.message_id)
                    || active.started < first
                {
                    return Err(Error::Invalid);
                }
            }
            validate_result(&parse_result(&active.prior)?, &plan, &registry)?;
        }
        for (index, bytes) in self.events.iter().enumerate() {
            let event = parse_event(bytes)?;
            validate_event(&event)?;
            if event.run_id != self.options.run_id
                || event.sequence != index as u64 + 1
                || event.state_revision > self.revision
            {
                return Err(Error::Invalid);
            }
        }
        Ok(())
    }
    /// Public status derived from authoritative state, without confusing requested and confirmed stop.
    /// # Errors
    /// Returns an error for corrupt stored evidence.
    pub fn handle(&self) -> Result<RunHandle, Error> {
        let state = if self.terminal {
            parse_result(&self.view)?.state
        } else if self.cancelled {
            "cancel_requested".into()
        } else if self.waiting_recovery || parse_result(&self.view)?.state == "requires_recovery" {
            "requires_recovery".into()
        } else if self.active.is_some() {
            "running".into()
        } else {
            "pending".into()
        };
        Ok(RunHandle {
            run_id: self.options.run_id.clone(),
            revision: self.revision,
            state,
        })
    }
    pub(crate) fn event(
        &mut self,
        kind: &str,
        state: &str,
        now: u64,
        step: Option<&str>,
    ) -> Result<(), Error> {
        let event = Event {
            contract: "plenora-chain-event-v2".into(),
            schema_version: 2,
            run_id: self.options.run_id.clone(),
            event_id: uuid::Uuid::new_v4().to_string(),
            sequence: self.events.len() as u64 + 1,
            state_revision: self.revision,
            occurred_at: crate::timestamp(now)?,
            kind: kind.into(),
            state: state.into(),
            step_id: step.map(str::to_owned),
            attempt: if let Some(id) = step {
                if let Some(intent) = &self.active {
                    parse_result(&intent.prior)?
                        .steps
                        .get(intent.index)
                        .ok_or(Error::Invalid)?
                        .attempt
                        .checked_add(1)
                        .ok_or(Error::ResourceLimit)?
                } else {
                    parse_result(&self.view)?
                        .steps
                        .iter()
                        .find(|s| s.step_id == id)
                        .ok_or(Error::Invalid)?
                        .attempt
                }
            } else {
                0
            },
            result_ref: (kind == "result_persisted")
                .then(|| format!("ref:result-{}", self.options.run_id)),
            error: None,
        };
        validate_event(&event)?;
        self.events.push(encode(&event)?);
        Ok(())
    }
}

/// Primary durable storage. A successful mutation means its transaction was confirmed.
/// Implementations enforce seven-day delivery, ninety-day reports and permanent tombstones.
#[async_trait]
pub trait Store: Send + Sync + 'static {
    /// Claim one due run in the trusted tenant under a primary transaction, fencing older owners.
    /// Lease duration is finite (1..=60000 ms); absence does not mean all runs are complete.
    async fn claim_work(
        &self,
        _tenant: &str,
        _owner: &str,
        _lease_ms: u64,
    ) -> Result<Option<crate::RunLease>, Error> {
        Err(Error::Unsupported)
    }
    /// Release only the current owner/token. Expired owners cannot release a successor's lease.
    async fn release_work(&self, _lease: &crate::RunLease) -> Result<(), Error> {
        Err(Error::Unsupported)
    }

    /// Lookup both identities before new-admission deadline checks. Two matching rows conflict.
    /// Expired identities return `Expired`, never absence; this does not authorize a mutation.
    async fn lookup(
        &self,
        tenant: &str,
        run_id: &str,
        key: Option<&str>,
    ) -> Result<Option<Journal>, Error>;
    /// Atomically lookup-or-create by tenant/run and tenant/chain.submit@2/key.
    /// Changed bindings conflict; expired identities are never created again.
    async fn create(&self, candidate: Submission) -> Result<Journal, Error>;
    /// Read authoritative state in the authenticated tenant. Never fall back to a replica/cache.
    async fn load(&self, tenant: &str, run_id: &str) -> Result<Journal, Error>;
    /// Replace only the exact preceding revision. No mutation on a failed comparison.
    async fn compare_exchange(&self, expected: u64, next: Change) -> Result<(), Error>;
}

/// Single-use submission authority created only after coordinator admission.
/// Decoding a persisted Journal does not grant this authority.
pub struct Submission(pub(crate) Journal);
impl Submission {
    /// Consume the admitted candidate in an implementation of the primary store.
    #[must_use]
    pub fn into_journal(self) -> Journal {
        self.0
    }
}
/// Single-use transition authority. It is neither constructible nor deserializable by callers.
pub struct Change(pub(crate) Journal, pub(crate) Option<crate::RunLease>);
impl Change {
    /// Inspect the optional coordination fence before committing the successor.
    #[must_use]
    pub fn lease(&self) -> Option<&crate::RunLease> {
        self.1.as_ref()
    }

    /// Consume the authorized successor in a primary CAS implementation.
    #[must_use]
    pub fn into_journal(self) -> Journal {
        self.0
    }
}

fn same_evidence(
    completed: &crate::contracts::StepResult,
    original: &crate::contracts::StepResult,
    elapsed: u64,
) -> bool {
    if completed.counters.elapsed_ms < original.counters.elapsed_ms
        || completed.counters.elapsed_ms > elapsed.max(original.counters.elapsed_ms)
    {
        return false;
    }
    let mut checked = completed.clone();
    checked.counters.elapsed_ms = original.counters.elapsed_ms;
    // The coordinator may add an observed timeout, never alter domain evidence.
    if checked != *original {
        let expected_state = if original.state == "succeeded" {
            "failed"
        } else {
            original.state.as_str()
        };
        if checked.state != expected_state
            || checked.counters.elapsed_ms < original.counters.elapsed_ms
            || !checked.other_errors.starts_with(&original.other_errors)
            || (original.error.is_some() && checked.error != original.error)
            || !checked
                .error
                .iter()
                .chain(&checked.other_errors)
                .any(|e| e.category == "timeout")
        {
            return false;
        }
        checked.state.clone_from(&original.state);
        checked.reason.clone_from(&original.reason);
        checked.error.clone_from(&original.error);
        checked.other_errors.clone_from(&original.other_errors);
        checked.counters.elapsed_ms = original.counters.elapsed_ms;
    }
    checked == *original
}
