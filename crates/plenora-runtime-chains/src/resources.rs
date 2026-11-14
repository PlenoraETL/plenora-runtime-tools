//! Durable ownership of declared provider doubles; expiry is never proof of termination.
use crate::{Error, Invocation, contracts::Budget};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

fn measured(value: &impl Serialize, limit: u64) -> Result<u64, Error> {
    struct Count {
        size: u64,
        limit: u64,
    }
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.size = self
                .size
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| std::io::Error::other("resource quota"))?;
            if self.size > self.limit {
                return Err(std::io::Error::other("resource quota"));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count { size: 0, limit };
    serde_json::to_writer(&mut count, value).map_err(|_| Error::ResourceLimit)?;
    Ok(count.size)
}

/// Run-owned provider bytes. Fixture templates and evidence journals are not owned resources.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedResource {
    /// Opaque resource identity, scoped by tenant/run.
    pub reference: String,
    /// Exact immutable bytes supplied by the declared provider.
    pub bytes: Vec<u8>,
    /// Logical consumers; serialized handles are charged separately from the shared blob.
    pub consumers: Vec<String>,
    /// Temporary buffers are never public output or evidence.
    pub temporary: bool,
}
impl OwnedResource {
    fn validate(&self) -> Result<(), Error> {
        if self.reference.len() > 124
            || self.reference.strip_prefix("ref:").is_none_or(|s| {
                s.is_empty()
                    || !s
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            })
            || self
                .consumers
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.consumers.len()
        {
            return Err(Error::Invalid);
        }
        if self.consumers.len() > 4096 || self.bytes.len() > 1024 * 1024 {
            return Err(Error::Invalid);
        }
        Ok(())
    }
}
/// Admission of one step's owned bytes, after its durable intent exists.
#[derive(Clone, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRequest {
    /// Authenticated scope.
    pub tenant: String,
    /// Persisted run.
    pub run_id: String,
    /// Immutable attempt identity, also the lease identity; never reused for another writer.
    pub message_id: String,
    /// Exact step identity.
    pub step: String,
    /// One-based attempt within the step.
    pub attempt: u64,
    /// Canonical authorized plan identity.
    pub plan_digest: String,
    /// Complete global limits, checked against the persisted plan by the store.
    pub limits: Budget,
    /// Objects this invocation may create; registration precedes provider work.
    pub objects: Vec<OwnedResource>,
}
impl ResourceRequest {
    /// Bind the provider's allocation declaration to the resolved invocation.
    #[must_use]
    pub fn new(i: &Invocation, limits: Budget, objects: Vec<OwnedResource>) -> Self {
        Self {
            tenant: i.tenant.clone(),
            run_id: i.run_id.clone(),
            message_id: i.message_id.clone(),
            step: i.step.id.clone(),
            attempt: i.attempt,
            plan_digest: i.plan_digest.clone(),
            limits,
            objects,
        }
    }
}
/// Primary-issued resource lease. An expired attempt can only be reconciled, never restarted.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLease {
    /// Immutable primary attempt identity.
    pub message_id: String,
    /// Primary expiry, not confirmation of stop.
    pub until: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Consumer {
    Pending,
    Consumed,
    Abandoned,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Object {
    sha256: String,
    published: bool,
    bytes: Option<Vec<u8>>,
    consumers: BTreeMap<String, Consumer>,
    temporary: bool,
    owner: String,
    expires: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Allocation {
    step: String,
    attempt: u64,
    until: u64,
    stopped: bool,
    objects: Vec<String>,
}
/// Resource ledger persists independently of the run journal and retains ownership tombstones.
/// The provider double stores actual run-owned bytes here; no filesystem deletion is implied.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLedger {
    limit: u64,
    published: u64,
    objects: BTreeMap<String, Object>,
    attempts: BTreeMap<String, Allocation>,
}
impl ResourceLedger {
    /// Start a bounded ledger. The encoded ledger, including byte expansion, is governed.
    #[must_use]
    pub fn new(limits: &Budget) -> Self {
        Self {
            published: 0,
            limit: limits
                .memory_bytes
                .min(limits.spool_bytes)
                .min(crate::JOURNAL_BYTES as u64),
            objects: BTreeMap::new(),
            attempts: BTreeMap::new(),
        }
    }
    /// Encode exact owned bytes and metadata under the original quota.
    /// # Errors
    /// Refuses a ledger that exceeds its finite reservation.
    pub fn to_json(&self) -> Result<Vec<u8>, Error> {
        self.check()?;
        serde_json::to_vec(self).map_err(|_| Error::Invalid)
    }
    /// Decode only the canonical primary encoding; duplicate members cannot be normalized.
    /// # Errors
    /// Refuses malformed, noncanonical or oversized state.
    pub fn from_json(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > crate::JOURNAL_BYTES {
            return Err(Error::ResourceLimit);
        }
        let ledger: Self = serde_json::from_slice(bytes).map_err(|_| Error::Invalid)?;
        if ledger.to_json()? != bytes {
            return Err(Error::Invalid);
        }
        Ok(ledger)
    }
    fn check(&self) -> Result<(), Error> {
        let Self {
            limit,
            published: _,
            objects: _,
            attempts: _,
        } = self;
        let mut reserved = self.clone();
        for object in reserved.objects.values_mut() {
            object.expires = u64::MAX;
            object.published = false;
            for consumer in object.consumers.values_mut() {
                *consumer = Consumer::Abandoned;
            }
            if object.bytes.as_ref().is_some_and(Vec::is_empty) {
                object.bytes = None;
            }
        }
        for attempt in reserved.attempts.values_mut() {
            attempt.until = u64::MAX;
            attempt.stopped = false;
        }
        // Reserve the exact worst encoding, plus two payload copies for transactional staging.
        let total = measured(&reserved, *limit)?
            .checked_add(self.live_bytes() as u64 * 2)
            .ok_or(Error::ResourceLimit)?;
        if total > *limit {
            return Err(Error::ResourceLimit);
        }
        Ok(())
    }
    /// Atomically reserve all copies and fan-out handles before a provider may use them.
    /// # Errors
    /// Refuses exhausted quotas, aliases with changed bytes and any reused attempt identity.
    pub fn acquire(
        &mut self,
        request: &ResourceRequest,
        now: u64,
        retain_until: u64,
    ) -> Result<ResourceLease, Error> {
        self.apply(request, now, retain_until, false)
    }
    /// Import recovered output ownership under the original attempt, without new invocation authority.
    /// # Errors
    /// Refuses changed scope, attempts, bytes or exhausted quotas.
    pub fn reconcile(
        &mut self,
        request: &ResourceRequest,
        now: u64,
        retain_until: u64,
    ) -> Result<ResourceLease, Error> {
        self.apply(request, now, retain_until, true)
    }
    fn apply(
        &mut self,
        request: &ResourceRequest,
        now: u64,
        retain_until: u64,
        recovery: bool,
    ) -> Result<ResourceLease, Error> {
        if now >= retain_until {
            return Err(Error::Expired);
        }
        let old = self.attempts.get(&request.message_id);
        if let Some(old) = old {
            if !recovery {
                return Err(Error::Recovery);
            }
            if old.step != request.step || old.attempt != request.attempt || old.stopped {
                return Err(Error::Conflict);
            }
        }
        if !crate::uuid(&request.message_id) || request.objects.len() > 128 {
            return Err(Error::Invalid);
        }
        measured(&request.objects, self.limit)?;
        let mut next = self.clone();
        let mut refs = old.map_or_else(Vec::new, |a| a.objects.clone());
        let mut supplied = std::collections::BTreeSet::new();
        let until = old.map_or(now.checked_add(30_000).ok_or(Error::ResourceLimit)?, |a| {
            a.until
        });
        let expires = retain_until;
        for resource in &request.objects {
            resource.validate()?;
            if !supplied.insert(&resource.reference) {
                return Err(Error::Invalid);
            }
            if !refs.contains(&resource.reference) {
                refs.push(resource.reference.clone());
            }
            if let Some(old) = next.objects.get_mut(&resource.reference) {
                if old.temporary != resource.temporary
                    || old.bytes.as_ref() != Some(&resource.bytes)
                    || (old.temporary && old.owner != request.message_id)
                {
                    return Err(Error::Conflict);
                }
                old.expires = old.expires.max(expires);
                for consumer in &resource.consumers {
                    old.consumers
                        .entry(consumer.clone())
                        .or_insert(Consumer::Pending);
                }
            } else {
                if !resource.temporary {
                    next.published = next
                        .published
                        .checked_add(resource.bytes.len() as u64)
                        .ok_or(Error::ResourceLimit)?;
                    if next.published > request.limits.artifact_bytes {
                        return Err(Error::ResourceLimit);
                    }
                }
                next.objects.insert(
                    resource.reference.clone(),
                    Object {
                        sha256: format!("{:x}", Sha256::digest(&resource.bytes)),
                        published: false,
                        bytes: Some(resource.bytes.clone()),
                        consumers: resource
                            .consumers
                            .iter()
                            .map(|c| (c.clone(), Consumer::Pending))
                            .collect(),
                        temporary: resource.temporary,
                        owner: request.message_id.clone(),
                        expires,
                    },
                );
            }
        }
        next.attempts.insert(
            request.message_id.clone(),
            Allocation {
                step: request.step.clone(),
                attempt: request.attempt,
                until,
                stopped: false,
                objects: refs,
            },
        );
        next.check()?;
        *self = next;
        Ok(ResourceLease {
            message_id: request.message_id.clone(),
            until,
        })
    }
    /// Renew a live step lease without reviving expired work or permitting another invocation.
    /// # Errors
    /// Expired, completed or unknown identities conflict.
    pub fn renew(&mut self, lease: &ResourceLease, now: u64) -> Result<ResourceLease, Error> {
        let attempt = self
            .attempts
            .get_mut(&lease.message_id)
            .ok_or(Error::Conflict)?;
        if attempt.stopped || now >= attempt.until {
            return Err(Error::Conflict);
        }
        attempt.until = attempt
            .until
            .max(now.checked_add(30_000).ok_or(Error::ResourceLimit)?);
        Ok(ResourceLease {
            message_id: lease.message_id.clone(),
            until: attempt.until,
        })
    }
    /// Record authenticated stop evidence, including a late receipt after lease expiry.
    /// # Errors
    /// Unknown attempts cannot authorize cleanup.
    fn stopped(&mut self, message_id: &str) -> Result<(), Error> {
        let attempt = self.attempts.get_mut(message_id).ok_or(Error::Conflict)?;
        attempt.stopped = true;
        let step = attempt.step.clone();
        for object in self.objects.values_mut() {
            if let Some(consumed) = object.consumers.get_mut(&step) {
                *consumed = Consumer::Consumed;
            }
        }
        Ok(())
    }
    /// Derive stop authority only from the validated committed view, atomically with its CAS.
    /// # Errors
    /// Invalid journal evidence is never interpreted as permission to delete resources.
    pub fn observe(&mut self, journal: &crate::Journal) -> Result<(), Error> {
        journal.validate()?;
        let result = crate::contracts::parse_result(&journal.view)?;
        // Evidence documents are useful even when the remote effect is unknown.
        // Publishing immutable reports does not confirm provider termination or
        // publish data outputs; those authorities remain separate below.
        for step in &result.steps {
            if let Some(allocation) = self
                .attempts
                .values()
                .find(|a| a.step == step.step_id && a.attempt == step.attempt)
            {
                let owned = allocation.objects.clone();
                self.observe_documents(step, &owned)?;
            }
        }
        let stopped: Vec<_> = self
            .attempts
            .iter()
            .filter(|(_, a)| {
                result.steps.iter().any(|s| {
                    s.step_id == a.step
                        && s.attempt == a.attempt
                        && s.stop_confirmed
                        && s.remote_effect != "unknown"
                        && matches!(s.state.as_str(), "succeeded" | "failed" | "cancelled")
                })
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in stopped {
            let allocation = self.attempts.get(&id).ok_or(Error::Storage)?;
            let step = result
                .steps
                .iter()
                .find(|s| s.step_id == allocation.step && s.attempt == allocation.attempt)
                .ok_or(Error::Storage)?;
            for artifact in step.outputs.values() {
                let object = self
                    .objects
                    .get_mut(&artifact.artifact_ref)
                    .ok_or(Error::Storage)?;
                if !allocation.objects.contains(&artifact.artifact_ref)
                    || object.temporary
                    || artifact.sha256 != object.sha256
                    || object.bytes.as_ref().is_none_or(|b| {
                        b.len() as u64 != artifact.bytes
                            || format!("{:x}", Sha256::digest(b)) != object.sha256
                    })
                {
                    return Err(Error::Conflict);
                }
                if artifact.publication == "confirmed" {
                    object.published = true;
                }
            }
            self.stopped(&id)?;
        }
        Ok(())
    }
    fn observe_documents(
        &mut self,
        step: &crate::contracts::StepResult,
        owned: &[String],
    ) -> Result<(), Error> {
        let errors = step.error.iter().chain(&step.other_errors);
        let reports = step
            .reports
            .iter()
            .chain(step.outputs.values().flat_map(|a| &a.reports))
            .chain(errors.clone().flat_map(|e| &e.reports));
        let documents: Vec<_> = step
            .upstream_result
            .iter()
            .chain(errors.filter_map(|e| e.upstream_error.as_ref()))
            .chain(reports.filter_map(|r| r.document.as_ref()))
            .collect();
        // External immutable resolvers remain allowed. For ledger-owned objects,
        // validate all documents before publishing any of them.
        for document in &documents {
            if let Some(object) = self.objects.get(&document.artifact_ref)
                && (object.temporary
                    || !owned.contains(&document.artifact_ref)
                    || document.sha256 != object.sha256
                    || object.bytes.as_ref().is_none_or(|bytes| {
                        format!("{:x}", Sha256::digest(bytes)) != document.sha256
                    }))
            {
                return Err(Error::Conflict);
            }
        }
        for document in documents {
            if let Some(object) = self.objects.get_mut(&document.artifact_ref) {
                object.published = true;
            }
        }
        Ok(())
    }
    /// Apply a primary retention reduction; ownership and stop evidence are unchanged.
    pub fn reduce_retention(&mut self, until: u64) {
        for object in self.objects.values_mut() {
            object.expires = object.expires.min(until);
        }
    }
    /// Drop actual owned buffers only after stop, preserving receipts, reference tombstones and fences.
    /// Published bytes remain evidence-pinned until report retention, even after the last consumer.
    /// Unknown writers remain quarantined regardless of expiry; reconciliation is required.
    pub fn cleanup(&mut self, now: u64) {
        for (reference, object) in &mut self.objects {
            let stopped = self
                .attempts
                .values()
                .filter(|a| a.objects.contains(reference))
                .all(|a| a.stopped);
            if now >= object.expires && stopped {
                // This timestamp is the primary report-retention boundary, later than the
                // delivery cutoff. Unstarted consumers can no longer acquire this run.
                for state in object.consumers.values_mut() {
                    if matches!(state, Consumer::Pending) {
                        *state = Consumer::Abandoned;
                    }
                }
            }
            let consumed = object
                .consumers
                .values()
                .all(|c| !matches!(c, Consumer::Pending));
            if stopped && (object.temporary || (now >= object.expires && consumed)) {
                object.bytes = None;
            }
        }
    }
    /// Read an actual run-owned immutable artifact, never a template or a temporary buffer.
    /// # Errors
    /// Missing, unpublished temporary or expired data is not silently replaced.
    pub fn artifact(&self, reference: &str, now: u64) -> Result<&[u8], Error> {
        let object = self.objects.get(reference).ok_or(Error::NotFound)?;
        if object.temporary {
            return Err(Error::Invalid);
        }
        if now >= object.expires {
            return Err(Error::Expired);
        }
        if !object.published {
            return Err(Error::Pending);
        }
        let bytes = object.bytes.as_deref().ok_or(Error::Expired)?;
        if format!("{:x}", Sha256::digest(bytes)) != object.sha256 {
            return Err(Error::Invalid);
        }
        Ok(bytes)
    }
    /// Actual retained provider payload bytes; distinct from cumulative published bytes.
    #[must_use]
    pub fn live_bytes(&self) -> usize {
        self.objects
            .values()
            .filter_map(|o| o.bytes.as_ref())
            .map(Vec::len)
            .sum()
    }
    /// Outstanding provider attempts; expiry never silently decrements this count.
    #[must_use]
    pub fn active(&self) -> usize {
        self.attempts.values().filter(|a| !a.stopped).count()
    }
}
impl crate::Journal {
    /// Check resource admission against the immutable persisted step intent and global plan.
    /// # Errors
    /// Refuses a changed plan, scope, step, limits or attempt carrier.
    pub fn authorize_resources(&self, request: &ResourceRequest) -> Result<(), Error> {
        let plan = crate::contracts::parse_plan(&self.plan)?;
        let intent = self.active.as_ref().ok_or(Error::Conflict)?;
        let prior = crate::contracts::parse_result(&intent.prior)?;
        let expected = prior
            .steps
            .get(intent.index)
            .ok_or(Error::Conflict)?
            .attempt
            .checked_add(1)
            .ok_or(Error::ResourceLimit)?;
        if request.objects.iter().any(|o| {
            o.consumers
                .iter()
                .any(|c| !plan.steps.iter().any(|s| &s.id == c))
        }) {
            return Err(Error::Invalid);
        }
        if request.attempt != expected {
            return Err(Error::Conflict);
        }
        if request.tenant != self.tenant
            || request.run_id != self.run_id()
            || request.message_id != intent.message_id
            || request.plan_digest != crate::contracts::plan_digest(&plan)?
            || request.limits != plan.budgets
            || plan
                .steps
                .get(intent.index)
                .is_none_or(|s| s.id != request.step)
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request() -> Result<ResourceRequest, Error> {
        let plan = crate::contracts::parse_plan(include_bytes!(
            "../../../contracts/chains-v2/fixtures/plan-valid.json"
        ))?;
        Ok(ResourceRequest {
            tenant: "tenant".into(),
            run_id: uuid::Uuid::new_v4().to_string(),
            message_id: uuid::Uuid::new_v4().to_string(),
            step: "read".into(),
            attempt: 1,
            plan_digest: crate::contracts::plan_digest(&plan)?,
            limits: plan.budgets,
            objects: vec![
                OwnedResource {
                    reference: "ref:blob".into(),
                    bytes: (0..=255).collect(),
                    consumers: vec!["left".into(), "right".into()],
                    temporary: false,
                },
                OwnedResource {
                    reference: "ref:temporary".into(),
                    bytes: vec![0; 32],
                    consumers: vec![],
                    temporary: true,
                },
            ],
        })
    }
    #[test]
    fn expiry_renewal_and_cleanup_never_invent_stop_or_replay() -> Result<(), Error> {
        let request = request()?;
        let mut ledger = ResourceLedger::new(&request.limits);
        let lease = ledger.acquire(&request, 0, 100_000)?;
        let renewed = ledger.renew(&lease, 20_000)?;
        assert_eq!(renewed.until, 50_000);
        assert!(matches!(ledger.renew(&lease, 50_000), Err(Error::Conflict)));
        ledger.cleanup(200_000);
        assert_eq!(ledger.live_bytes(), 288); // An expired writer still owns both buffers.
        assert_eq!(ledger.active(), 1);
        assert!(matches!(
            ledger.acquire(&request, 60_000, 100_000),
            Err(Error::Recovery)
        ));
        let bytes = serde_json::to_vec(&ledger).map_err(|_| Error::Invalid)?;
        let mut restarted: ResourceLedger =
            serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
        restarted.stopped(&request.message_id)?;
        restarted.cleanup(60_000);
        assert_eq!(restarted.live_bytes(), 256); // Public/evidence pin survives last consumer.
        restarted.cleanup(100_000);
        assert_eq!(restarted.live_bytes(), 0);
        assert_eq!(restarted.attempts.len(), 1);
        assert_eq!(restarted.objects.len(), 2);
        assert!(
            restarted.objects["ref:blob"]
                .consumers
                .values()
                .all(|c| matches!(c, Consumer::Abandoned))
        );
        assert!(matches!(
            restarted.acquire(&request, 100_000, 100_000),
            Err(Error::Expired)
        ));
        restarted.cleanup(100_000); // Repeating after lost commit confirmation is harmless.
        assert_eq!(restarted.live_bytes(), 0);
        Ok(())
    }
    #[test]
    fn fanout_and_copies_are_globally_reserved_without_partial_admission() -> Result<(), Error> {
        let mut request = request()?;
        request.limits.memory_bytes = 4000;
        request.limits.spool_bytes = 4000;
        let mut ledger = ResourceLedger::new(&request.limits);
        ledger.acquire(&request, 0, 100_000)?;
        let before = serde_json::to_vec(&ledger).map_err(|_| Error::Invalid)?;
        let mut fanout = request.clone();
        fanout.message_id = uuid::Uuid::new_v4().to_string();
        fanout.objects.truncate(1);
        fanout.objects[0].consumers = (0..1000).map(|i| format!("consumer-{i}")).collect();
        assert!(matches!(
            ledger.acquire(&fanout, 1, 100_000),
            Err(Error::ResourceLimit)
        ));
        assert_eq!(
            serde_json::to_vec(&ledger).map_err(|_| Error::Invalid)?,
            before
        );
        let mut shared = request.clone();
        shared.message_id = uuid::Uuid::new_v4().to_string();
        shared.objects.truncate(1);
        ledger.acquire(&shared, 1, 100_000)?;
        assert_eq!(ledger.live_bytes(), 288); // One physical blob, multiple ownership handles.
        shared.message_id = uuid::Uuid::new_v4().to_string();
        shared.objects[0].reference = "ref:copy".into();
        assert!(matches!(
            ledger.acquire(&shared, 2, 100_000),
            Err(Error::ResourceLimit)
        ));
        assert_eq!(ledger.live_bytes(), 288);
        request.limits.memory_bytes = 100_000;
        request.limits.spool_bytes = 100_000;
        let mut roomy = ResourceLedger::new(&request.limits);
        roomy.acquire(&request, 0, 100_000)?;
        shared.limits = request.limits.clone();
        roomy.acquire(&shared, 2, 100_000)?;
        assert_eq!(roomy.live_bytes(), 544);
        ledger.check()?;
        Ok(())
    }
    #[test]
    fn recovery_imports_new_outputs_without_reviving_lease_or_allocating_twice() -> Result<(), Error>
    {
        let mut request = request()?;
        let output = request.objects.remove(0);
        let mut ledger = ResourceLedger::new(&request.limits);
        let lease = ledger.acquire(&request, 0, 100_000)?;
        request.objects.push(output);
        let restored = ledger.reconcile(&request, 40_000, 100_000)?;
        assert_eq!(restored.until, lease.until);
        assert_eq!(ledger.live_bytes(), 288);
        ledger.reconcile(&request, 50_000, 100_000)?;
        assert_eq!(ledger.live_bytes(), 288);
        request.attempt += 1;
        assert!(matches!(
            ledger.reconcile(&request, 50_000, 100_000),
            Err(Error::Conflict)
        ));
        Ok(())
    }

    #[test]
    fn evidence_publication_checks_digest_ownership_and_temporary_before_restart()
    -> Result<(), Error> {
        let request = request()?;
        let mut ledger = ResourceLedger::new(&request.limits);
        ledger.acquire(&request, 0, 100_000)?;
        let result = crate::contracts::parse_result(include_bytes!(
            "../../../contracts/chains-v2/fixtures/result-valid.json"
        ))?;
        let mut step = result.steps.first().ok_or(Error::Invalid)?.clone();
        step.outputs.clear();
        step.reports.clear();
        step.error = None;
        step.other_errors.clear();
        step.remote_effect = "unknown".into();
        step.stop_confirmed = false;
        let document = crate::contracts::Document {
            artifact_ref: "ref:blob".into(),
            sha256: format!("{:x}", Sha256::digest(&request.objects[0].bytes)),
            contract: "plenora-error-v1".into(),
        };
        step.upstream_result = Some(document.clone());
        assert!(ledger.observe_documents(&step, &[]).is_err());
        assert!(matches!(
            ledger.artifact("ref:blob", 0),
            Err(Error::Pending)
        ));
        step.upstream_result.as_mut().ok_or(Error::Invalid)?.sha256 = "0".repeat(64);
        assert!(
            ledger
                .observe_documents(&step, &["ref:blob".into()])
                .is_err()
        );
        step.upstream_result
            .as_mut()
            .ok_or(Error::Invalid)?
            .artifact_ref = "ref:temporary".into();
        assert!(
            ledger
                .observe_documents(&step, &["ref:temporary".into()])
                .is_err()
        );
        step.upstream_result = Some(document);
        ledger.observe_documents(&step, &["ref:blob".into()])?;
        let bytes = serde_json::to_vec(&ledger).map_err(|_| Error::Invalid)?;
        let restored: ResourceLedger =
            serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
        assert_eq!(restored.artifact("ref:blob", 0)?, &request.objects[0].bytes);
        assert_eq!(restored.active(), 1); // Evidence availability is not remote stop confirmation.
        Ok(())
    }
}
