//! One durable state machine for admission, bounded attempts, evidence and routed outbox.
use crate::{SettlementError, StoredMessage};
use async_trait::async_trait;
use plenora_runtime_capabilities::AcceptanceStore;
use plenora_runtime_messaging::MessageId;
use serde::{Deserialize, Serialize};
use std::io;

/// Hard encoded JSON journal bound, including every retained attempt and terminal.
pub const JOURNAL_LIMIT: usize = 64 * 1024 * 1024;
const FIXED_RESERVE: usize = 1024 * 1024;
/// Host-owned immutable route and reservation; no data from untrusted headers selects a sink.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeliveryPlan {
    pub(crate) route: String,
    pub(crate) attempts: u16,
    pub(crate) retry_ms: u64,
    pub(crate) output_bytes: usize,
    pub(crate) sink_bytes: usize,
}
impl DeliveryPlan {
    /// Reserves worst-case JSON byte-array expansion for two copies per attempt, plus metadata.
    /// The exact serialized length is also checked without allocation before every commit.
    /// # Errors
    /// Refuses invalid routes, more than 16 attempts, unbounded time or a reservation above 64 MiB.
    pub fn new(
        route: &str,
        attempts: u16,
        retry_ms: u64,
        output_bytes: usize,
        sink_bytes: usize,
    ) -> Result<Self, SettlementError> {
        let p = Self {
            route: route.into(),
            attempts,
            retry_ms,
            output_bytes,
            sink_bytes,
        };
        p.validate()?;
        Ok(p)
    }
    fn validate(&self) -> Result<(), SettlementError> {
        if self.route.is_empty()
            || self.route.len() > 256
            || !self.route.bytes().all(|b| b.is_ascii_graphic())
            || !(1..=16).contains(&self.attempts)
            || self.retry_ms > 604_800_000
            || self.output_bytes == 0
            || self.sink_bytes == 0
            || self.sink_bytes > JOURNAL_LIMIT
        {
            return Err(SettlementError);
        }
        let each = self
            .output_bytes
            .checked_mul(8)
            .and_then(|n| n.checked_add(FIXED_RESERVE))
            .ok_or(SettlementError)?;
        if each
            .checked_mul(usize::from(self.attempts))
            .and_then(|n| n.checked_add(FIXED_RESERVE))
            .ok_or(SettlementError)?
            > JOURNAL_LIMIT
        {
            return Err(SettlementError);
        }
        Ok(())
    }
    /// Durable destination identity.
    #[must_use]
    pub fn route(&self) -> &str {
        &self.route
    }
    /// Component payload budget known before invocation.
    #[must_use]
    pub const fn output_bytes(&self) -> usize {
        self.output_bytes
    }
}
/// Complete bounded outcome and already rendered terminal, written before the wrapper returns.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedResult {
    pub(crate) report: serde_json::Value,
    pub(crate) terminal: StoredMessage,
    pub(crate) retry_ms: Option<u64>,
    #[serde(default)]
    pub(crate) reuse_origin: Option<IdempotencyReceipt>,
}
impl PreparedResult {
    fn validate(&self) -> Result<(), SettlementError> {
        let message = self.message()?;
        if message
            .headers
            .get_text("plenora.negotiation.requires")
            .map_err(|_| SettlementError)?
            != Some("plenora-runtime-binding-v2")
            || message
                .headers
                .get_text("plenora.negotiation.accepts_downgrade")
                .map_err(|_| SettlementError)?
                != Some("false")
        {
            return Err(SettlementError);
        }
        if let Some(delay) = self.retry_ms {
            let error: serde_json::Value =
                serde_json::from_slice(&message.bytes).map_err(|_| SettlementError)?;
            let report = &self.report;
            let expected = match report["retry"]["kind"].as_str() {
                Some("safe") => Some(0),
                Some("after") => report["retry"]["delay_ms"].as_u64(),
                _ => None,
            };
            if message.content_type.as_ref()
                != plenora_runtime_capabilities::PLENORA_ERROR_CONTENT_TYPE
                || !matches!(
                    report["remote_effect"].as_str(),
                    Some("none" | "rolled_back")
                )
                || report["category"] == "resource_limit"
                || expected != Some(delay)
                || delay > 86_400_000
                || error["remote_effect"] != report["remote_effect"]
                || error["retry"] != report["retry"]
            {
                return Err(SettlementError);
            }
        }
        Ok(())
    }
    /// Inspect the complete report; authorization is owned by the host.
    #[must_use]
    pub const fn report(&self) -> &serde_json::Value {
        &self.report
    }
    /// Restore the immutable result envelope, including its persisted identity.
    /// # Errors
    /// Refuses invalid stored metadata.
    pub fn message(&self) -> Result<plenora_runtime_messaging::SerializedMessage, SettlementError> {
        self.terminal.clone().restore()
    }
}
/// Explicit phases; Running never automatically becomes startable after a timeout or crash.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeliveryStage {
    /// Ready for a first or proven-safe attempt.
    Ready,
    /// Invocation might have started; only evidence can advance it.
    Running,
    /// Complete evidence permits finalization without any domain invocation.
    Evidence,
    /// Inbox and routed terminal are durable.
    Finished,
}
/// Serializable authoritative state. Fields are private; transitions validate every invariant.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryJournal {
    version: u8,
    binding: [u8; 32],
    plan: DeliveryPlan,
    result_id: String,
    revision: u64,
    stage: DeliveryStage,
    attempt: u16,
    created: u64,
    due: u64,
    delivery_ms: u64,
    report_ms: u64,
    history: Vec<PreparedResult>,
    delivered: bool,
    lease_until: u64,
    expired_report: bool,
}
/// Commands are applied under a primary transaction/CAS; there is no lease-based domain restart.
pub enum DeliveryChange {
    /// Claims one invocation and increments its fenced attempt.
    Start,
    /// Records a complete component result.
    Evidence(PreparedResult),
    /// Records a pre-invocation rejection.
    Reject(PreparedResult),
    /// Copies confirmed evidence from the invocation selected by a scoped host key.
    Reuse(PreparedResult, IdempotencyReceipt),
    /// Consumes evidence into either safe retry or the terminal outbox.
    Settle,
    /// Claims publication only; never grants invocation authority.
    Claim,
    /// Records broker-confirmed publication for the current revision.
    Confirm,
    /// Releases an unconfirmed publication, preserving the terminal.
    Release,
}
impl DeliveryJournal {
    /// Creates the sole result identity. Called only within an authoritative lookup-or-create.
    /// # Errors
    /// Refuses invalid reservations or retention extensions.
    pub fn new(
        key: &str,
        binding: [u8; 32],
        plan: DeliveryPlan,
        now: u64,
        delivery_ms: u64,
        report_ms: u64,
    ) -> Result<Self, SettlementError> {
        plan.validate()?;
        if delivery_ms == 0
            || delivery_ms > 604_800_000
            || report_ms < delivery_ms
            || report_ms > 7_776_000_000
        {
            return Err(SettlementError);
        }
        let mut result_id = MessageId::random().to_string();
        while key.ends_with(&result_id) {
            result_id = MessageId::random().to_string();
        }
        Ok(Self {
            version: 1,
            binding,
            plan,
            result_id,
            revision: 0,
            stage: DeliveryStage::Ready,
            attempt: 0,
            created: now,
            due: now,
            delivery_ms,
            report_ms,
            history: Vec::new(),
            delivered: false,
            lease_until: 0,
            expired_report: false,
        })
    }
    /// Restores a bounded versioned journal, refusing unknown versions and invalid reservations.
    /// # Errors
    /// Corrupt or oversized journals fail closed.
    pub fn from_json(bytes: &[u8]) -> Result<Self, SettlementError> {
        if bytes.len() > JOURNAL_LIMIT {
            return Err(SettlementError);
        }
        let j: Self = serde_json::from_slice(bytes).map_err(|_| SettlementError)?;
        j.plan.validate()?;
        if j.version != 1
            || !crate::lexical::uuid(&j.result_id)
            || j.history.len() > usize::from(j.plan.attempts)
            || j.attempt > j.plan.attempts
        {
            return Err(SettlementError);
        }
        for p in &j.history {
            j.check_prepared(p)?;
        }
        j.validate_state()?;
        if !j.expired_report
            && matches!(j.stage, DeliveryStage::Evidence | DeliveryStage::Finished)
            && j.history.is_empty()
        {
            return Err(SettlementError);
        }
        Ok(j)
    }
    /// Serializes only after an allocation-free exact byte count.
    /// # Errors
    /// Refuses journals above the hard bound.
    pub fn to_json(&self) -> Result<Vec<u8>, SettlementError> {
        if encoded_size(self)? > JOURNAL_LIMIT {
            return Err(SettlementError);
        }
        serde_json::to_vec(self).map_err(|_| SettlementError)
    }
    fn validate_state(&self) -> Result<(), SettlementError> {
        if self.delivery_ms == 0
            || self.delivery_ms > 604_800_000
            || self.report_ms < self.delivery_ms
            || self.report_ms > 7_776_000_000
            || (self.delivered && self.stage != DeliveryStage::Finished)
            || (self.lease_until != 0 && self.stage != DeliveryStage::Finished)
        {
            return Err(SettlementError);
        }
        if self.expired_report {
            return if self.history.is_empty() {
                Ok(())
            } else {
                Err(SettlementError)
            };
        }
        if let Some(origin) = self.history.last().and_then(|p| p.reuse_origin.as_ref()) {
            return if self.attempt == 0
                && self.history.len() == 1
                && matches!(
                    self.stage,
                    DeliveryStage::Evidence | DeliveryStage::Finished
                )
                && self.history[0].retry_ms.is_none()
                && !origin.key.is_empty()
                && origin.key.len() <= 256
                && origin.key.bytes().all(|b| b.is_ascii_graphic())
            {
                Ok(())
            } else {
                Err(SettlementError)
            };
        }
        let count = self.history.len();
        let attempt = usize::from(self.attempt);
        let valid = match self.stage {
            DeliveryStage::Ready => {
                (attempt == 0 && count == 0)
                    || (attempt > 0
                        && count == attempt
                        && self.attempt < self.plan.attempts
                        && self.history.last().is_some_and(|p| p.retry_ms.is_some()))
            }
            DeliveryStage::Running => attempt > 0 && count + 1 == attempt,
            DeliveryStage::Evidence | DeliveryStage::Finished => {
                if attempt > 0 && count == attempt {
                    true
                } else if count == attempt + 1 {
                    self.history.last().is_some_and(|p| {
                        p.retry_ms.is_none()
                            && p.report["phase"] == "validate"
                            && p.report["remote_effect"] == "none"
                    })
                } else {
                    false
                }
            }
        };
        if valid { Ok(()) } else { Err(SettlementError) }
    }
    /// Checks exact input and immutable host policy on every ingress, including terminal replay.
    /// # Errors
    /// Any identity reuse with changed input or policy is a conflict, never a new invocation.
    pub fn verify(&self, binding: [u8; 32], plan: &DeliveryPlan) -> Result<(), SettlementError> {
        if self.version != 1 || self.binding != binding || &self.plan != plan {
            Err(SettlementError)
        } else {
            Ok(())
        }
    }
    /// Applies retention reductions and removes reports after their independent deadline.
    /// # Errors
    /// Rejects malformed backend policy.
    pub fn maintain(
        &mut self,
        now: u64,
        delivery_ms: u64,
        report_ms: u64,
    ) -> Result<(), SettlementError> {
        if delivery_ms == 0
            || delivery_ms > 604_800_000
            || report_ms < delivery_ms
            || report_ms > 7_776_000_000
        {
            return Err(SettlementError);
        }
        self.delivery_ms = self.delivery_ms.min(delivery_ms);
        self.report_ms = self.report_ms.min(report_ms);
        if now.saturating_sub(self.created) >= self.report_ms {
            self.history.clear();
            self.expired_report = true;
        }
        Ok(())
    }
    /// Executes a fenced transition. Both the DB and test storage use this same reducer.
    /// # Errors
    /// Stale revisions, uncertain execution, elapsed windows and invalid transitions fail closed.
    pub fn apply(
        &mut self,
        revision: u64,
        change: DeliveryChange,
        now: u64,
    ) -> Result<(), SettlementError> {
        if revision != self.revision
            || self.expired_report
            || (self.expired(now)
                && !matches!(
                    &change,
                    DeliveryChange::Evidence(_)
                        | DeliveryChange::Reject(_)
                        | DeliveryChange::Reuse(_, _)
                        | DeliveryChange::Settle
                ))
        {
            return Err(SettlementError);
        }
        self.validate_state()?;
        let mut next = self.clone();
        match change {
            DeliveryChange::Start
                if next.stage == DeliveryStage::Ready
                    && now >= next.due
                    && next.attempt < next.plan.attempts =>
            {
                if next.attempt > 0 && now.saturating_sub(next.created) >= next.plan.retry_ms {
                    return Err(SettlementError);
                }
                next.attempt += 1;
                next.stage = DeliveryStage::Running;
            }
            DeliveryChange::Evidence(p) if next.stage == DeliveryStage::Running => {
                next.check_prepared(&p)?;
                next.history.push(p);
                next.stage = DeliveryStage::Evidence;
            }
            DeliveryChange::Reject(p)
                if next.stage == DeliveryStage::Ready && p.retry_ms.is_none() =>
            {
                next.check_prepared(&p)?;
                next.history.push(p);
                next.stage = DeliveryStage::Evidence;
            }
            DeliveryChange::Reuse(mut p, origin)
                if next.stage == DeliveryStage::Ready
                    && next.attempt == 0
                    && p.retry_ms.is_none() =>
            {
                next.check_prepared(&p)?;
                p.reuse_origin = Some(origin);
                next.history.push(p);
                next.stage = DeliveryStage::Evidence;
            }
            DeliveryChange::Settle
                if next.stage == DeliveryStage::Evidence
                    || (next.stage == DeliveryStage::Ready
                        && next.attempt > 0
                        && now.saturating_sub(next.created) >= next.plan.retry_ms) =>
            {
                let last = next.history.last().ok_or(SettlementError)?;
                if let Some(delay) = last.retry_ms.filter(|delay| {
                    next.attempt < next.plan.attempts
                        && now.saturating_sub(next.created).saturating_add(*delay)
                            < next.plan.retry_ms
                        && now.saturating_sub(next.created).saturating_add(*delay)
                            < next.delivery_ms
                }) {
                    next.stage = DeliveryStage::Ready;
                    next.due = now.checked_add(delay).ok_or(SettlementError)?;
                } else {
                    next.stage = DeliveryStage::Finished;
                }
            }
            DeliveryChange::Claim
                if next.stage == DeliveryStage::Finished
                    && !next.delivered
                    && now >= next.lease_until =>
            {
                next.lease_until = now.checked_add(30_000).ok_or(SettlementError)?;
            }
            DeliveryChange::Confirm
                if next.stage == DeliveryStage::Finished
                    && !next.delivered
                    && now < next.lease_until =>
            {
                next.delivered = true;
            }
            DeliveryChange::Release if next.stage == DeliveryStage::Finished && !next.delivered => {
                next.lease_until = 0;
            }
            _ => return Err(SettlementError),
        }
        next.validate_state()?;
        next.revision = next.revision.checked_add(1).ok_or(SettlementError)?;
        if next.history.len() > usize::from(next.plan.attempts)
            || encoded_size(&next)? > JOURNAL_LIMIT
        {
            return Err(SettlementError);
        }
        *self = next;
        Ok(())
    }
    fn check_prepared(&self, p: &PreparedResult) -> Result<(), SettlementError> {
        p.validate()?;
        let m = p.message()?;
        if m.headers
            .get_text(plenora_runtime_messaging::MESSAGE_ID_METADATA_KEY)
            .map_err(|_| SettlementError)?
            != Some(self.result_id.as_str())
            || encoded_size(p)? > self.plan.output_bytes * 8 + FIXED_RESERVE
        {
            return Err(SettlementError);
        }
        Ok(())
    }
    /// Exact encoded size, without allocating the serialized journal.
    /// # Errors
    /// Returns a serialization/overflow error.
    pub fn encoded_size(&self) -> Result<usize, SettlementError> {
        encoded_size(self)
    }
    /// Fencing revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
    /// Immutable primary-clock origin for indexed retention selection.
    #[must_use]
    pub const fn created_ms(&self) -> u64 {
        self.created
    }
    /// Whether report payloads have already been purged, independently of delivery expiry.
    #[must_use]
    pub const fn report_expired(&self) -> bool {
        self.expired_report
    }
    /// Current state, never inferred from missing payloads.
    #[must_use]
    pub const fn stage(&self) -> DeliveryStage {
        self.stage
    }
    /// Fixed result message identity.
    #[must_use]
    pub fn result_id(&self) -> &str {
        &self.result_id
    }
    /// Immutable destination and budgets.
    #[must_use]
    pub const fn plan(&self) -> &DeliveryPlan {
        &self.plan
    }
    /// Number of actually authorized attempts.
    #[must_use]
    pub const fn attempt(&self) -> u16 {
        self.attempt
    }
    /// Whether another delivery/publication is forbidden; reports may still be queried.
    #[must_use]
    pub fn expired(&self, now: u64) -> bool {
        self.expired_report || now.saturating_sub(self.created) >= self.delivery_ms
    }
    /// Whether external delivery has been confirmed.
    #[must_use]
    pub const fn delivered(&self) -> bool {
        self.delivered
    }
    /// Last complete evidence, retained independently of delivery eligibility.
    #[must_use]
    pub fn evidence(&self) -> Option<&PreparedResult> {
        self.history.last()
    }
    /// Whether a retry can start under its persisted elapsed-time budget.
    #[must_use]
    pub fn retry_exhausted(&self, now: u64) -> bool {
        self.attempt > 0 && now.saturating_sub(self.created) >= self.plan.retry_ms
    }
    /// Earliest retry time on the authoritative store clock.
    #[must_use]
    pub const fn due(&self) -> u64 {
        self.due
    }
}
struct Counter(usize);
impl io::Write for Counter {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(b.len())
            .ok_or_else(|| io::Error::other("encoded size overflow"))?;
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(crate) fn encoded_size(v: &impl Serialize) -> Result<usize, SettlementError> {
    let mut c = Counter(0);
    serde_json::to_writer(&mut c, v).map_err(|_| SettlementError)?;
    Ok(c.0)
}
/// Receipt lookup distinguishes a durable identity conflict from storage uncertainty.
pub enum DeliveryOpen {
    /// Exact immutable binding was created or recovered.
    Opened(DeliveryJournal, u64),
    /// Existing identity is bound to other input/policy or legacy invocation authority.
    Conflict,
}
/// Immutable pointer to an exact receipt. Constructed only by the durable ingress.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdempotencyReceipt {
    pub(crate) key: String,
    pub(crate) binding: [u8; 32],
}
/// Host-key lookup is separate from the exact message receipt and its correlation identity.
pub enum IdempotencyOpen {
    /// No binding exists; lookup alone never grants execution authority.
    Absent,
    /// Original carrier selected by the immutable semantic binding.
    Selected(IdempotencyReceipt),
    /// The permanent key already binds different semantics.
    Conflict,
}
/// Atomic primary storage for the product journal, using PostgreSQL in production.
#[async_trait]
pub trait DurableExecutionStore: AcceptanceStore + Send + Sync + 'static {
    /// Lookup or atomically bind an authenticated tenant/operation key to its original carrier.
    /// `None` performs lookup only. Expiration must never turn an identity into absence.
    /// # Errors
    /// Unconfirmed storage or expired delivery prevents any new invocation.
    async fn idempotency(
        &self,
        _key: &str,
        _binding: [u8; 32],
        _candidate: Option<IdempotencyReceipt>,
    ) -> Result<IdempotencyOpen, SettlementError> {
        Err(SettlementError)
    }
    /// Authoritative receipt lookup-or-create; returns the store clock with the snapshot.
    /// # Errors
    /// Fails on conflicts, retention, stale replicas or uncertain commit.
    async fn open(
        &self,
        key: &str,
        binding: [u8; 32],
        plan: &DeliveryPlan,
    ) -> Result<DeliveryOpen, SettlementError>;
    /// Lookup a result identity on the primary, confined to the authenticated scope prefix.
    /// # Errors
    /// Missing or foreign identities and unconfirmed reads fail closed.
    async fn result_receipt(
        &self,
        _scope_prefix: &str,
        _result_id: &str,
    ) -> Result<(String, DeliveryJournal), SettlementError> {
        Err(SettlementError)
    }
    /// Reads a retained journal, including terminal replay past the execution deadline.
    /// # Errors
    /// Missing/non-authoritative data is explicit.
    async fn load(&self, key: &str) -> Result<(DeliveryJournal, u64), SettlementError>;
    /// Applies exactly one fenced state transition and confirms its commit.
    /// # Errors
    /// Failed/uncertain commits never grant invocation or ACK authority.
    async fn change(
        &self,
        key: &str,
        revision: u64,
        change: DeliveryChange,
    ) -> Result<(DeliveryJournal, u64), SettlementError>;
    /// Enumerates bounded finalization/outbox work for one immutable destination.
    /// # Errors
    /// Refuses unavailable or non-authoritative storage.
    async fn pending(
        &self,
        scope_prefix: &str,
        route: &str,
        limit: usize,
    ) -> Result<Vec<String>, SettlementError>;
}
