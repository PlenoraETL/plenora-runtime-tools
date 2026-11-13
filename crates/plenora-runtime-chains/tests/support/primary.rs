//! Declared primary-store fake shared by public chain and host probes; no infrastructure claim.
use async_trait::async_trait;
use plenora_runtime_chains::{Error, Journal, Store};
use plenora_runtime_core::Clock;
use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::UNIX_EPOCH,
};
type Alias = ([u8; 32], plenora_runtime_boundary::IdempotencyReceipt, u64);
type Leases = BTreeMap<(String, String), (u64, Option<plenora_runtime_chains::RunLease>)>;
type Pause = (
    std::sync::Arc<tokio::sync::Notify>,
    std::sync::Arc<tokio::sync::Notify>,
);
#[derive(Default)]
pub struct ScriptStore(
    pub Mutex<BTreeMap<(String, String), Journal>>,
    pub Mutex<BTreeMap<String, plenora_runtime_boundary::DeliveryJournal>>,
    pub  Mutex<
        BTreeMap<
            plenora_runtime_messaging::MessageId,
            plenora_runtime_capabilities::NegotiationContext,
        >,
    >,
    pub plenora_runtime_testkit::ManualClock,
    pub Mutex<BTreeMap<String, Alias>>,
    pub AtomicBool,
    pub AtomicBool,
    pub Mutex<Option<std::sync::Arc<plenora_runtime_testkit::ManualClock>>>,
    pub Mutex<Leases>,
    pub Mutex<Option<Pause>>,
    pub Mutex<BTreeMap<(String, String), plenora_runtime_chains::ResourceLedger>>,
    pub AtomicBool,
    pub Mutex<Option<Pause>>,
    pub std::sync::atomic::AtomicU64,
    pub Mutex<Option<Pause>>,
);
#[async_trait]
impl Store for ScriptStore {
    async fn read_resource(
        &self,
        tenant: &str,
        run: &str,
        reference: &str,
        limit: usize,
    ) -> Result<Vec<u8>, Error> {
        let now = u64::try_from(
            self.3
                .now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Error::Storage)?
                .as_millis(),
        )
        .map_err(|_| Error::Storage)?;
        let ledger = self
            .resource_snapshot(tenant, run)
            .await?
            .ok_or(Error::NotFound)?;
        let bytes = ledger.artifact(reference, now)?;
        if bytes.len() > limit {
            return Err(Error::ResourceLimit);
        }
        Ok(bytes.to_vec())
    }
    async fn resource_snapshot(
        &self,
        tenant: &str,
        run: &str,
    ) -> Result<Option<plenora_runtime_chains::ResourceLedger>, Error> {
        Ok(self
            .10
            .lock()
            .map_err(|_| Error::Storage)?
            .get(&(tenant.into(), run.into()))
            .cloned())
    }
    async fn acquire_resources(
        &self,
        request: &plenora_runtime_chains::ResourceRequest,
    ) -> Result<plenora_runtime_chains::ResourceLease, Error> {
        let now = u64::try_from(
            self.3
                .now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Error::Storage)?
                .as_millis(),
        )
        .map_err(|_| Error::Storage)?;
        let rows = self.0.lock().map_err(|_| Error::Storage)?;
        let journal = rows
            .get(&(request.tenant.clone(), request.run_id.clone()))
            .ok_or(Error::NotFound)?;
        journal.authorize_resources(request)?;
        let mut ledgers = self.10.lock().map_err(|_| Error::Storage)?;
        let ledger = ledgers
            .entry((request.tenant.clone(), request.run_id.clone()))
            .or_insert_with(|| plenora_runtime_chains::ResourceLedger::new(&request.limits));
        ledger.acquire(
            request,
            now,
            journal
                .created_ms()
                .checked_add(plenora_runtime_chains::REPORT_MS)
                .ok_or(Error::ResourceLimit)?,
        )
    }
    async fn reconcile_resources(
        &self,
        request: &plenora_runtime_chains::ResourceRequest,
    ) -> Result<(), Error> {
        let now = u64::try_from(
            self.3
                .now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Error::Storage)?
                .as_millis(),
        )
        .map_err(|_| Error::Storage)?;
        let rows = self.0.lock().map_err(|_| Error::Storage)?;
        let journal = rows
            .get(&(request.tenant.clone(), request.run_id.clone()))
            .ok_or(Error::NotFound)?;
        journal.authorize_resources(request)?;
        let mut ledgers = self.10.lock().map_err(|_| Error::Storage)?;
        let ledger = ledgers
            .entry((request.tenant.clone(), request.run_id.clone()))
            .or_insert_with(|| plenora_runtime_chains::ResourceLedger::new(&request.limits));
        ledger.reconcile(
            request,
            now,
            journal
                .created_ms()
                .checked_add(plenora_runtime_chains::REPORT_MS)
                .ok_or(Error::ResourceLimit)?,
        )?;
        Ok(())
    }
    async fn renew_resources(
        &self,
        tenant: &str,
        run: &str,
        lease: &plenora_runtime_chains::ResourceLease,
    ) -> Result<plenora_runtime_chains::ResourceLease, Error> {
        let pause = self.12.lock().map_err(|_| Error::Storage)?.take();
        if let Some((entered, release)) = pause {
            entered.notify_one();
            release.notified().await;
        }
        let now = u64::try_from(
            self.3
                .now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Error::Storage)?
                .as_millis(),
        )
        .map_err(|_| Error::Storage)?;
        self.10
            .lock()
            .map_err(|_| Error::Storage)?
            .get_mut(&(tenant.into(), run.into()))
            .ok_or(Error::NotFound)?
            .renew(lease, now)
    }
    async fn cleanup_resources(&self, tenant: &str) -> Result<(), Error> {
        let now = u64::try_from(
            self.3
                .now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Error::Storage)?
                .as_millis(),
        )
        .map_err(|_| Error::Storage)?;
        for ((owner, _), ledger) in self.10.lock().map_err(|_| Error::Storage)?.iter_mut() {
            if owner == tenant {
                ledger.cleanup(now);
            }
        }
        Ok(())
    }

    async fn claim_work(
        &self,
        tenant: &str,
        owner: &str,
        lease_ms: u64,
    ) -> Result<Option<plenora_runtime_chains::RunLease>, Error> {
        if !(1..=60_000).contains(&lease_ms) {
            return Err(Error::Invalid);
        }
        let now = u64::try_from(
            self.3
                .now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Error::Storage)?
                .as_millis(),
        )
        .map_err(|_| Error::Storage)?;
        let rows = self.0.lock().map_err(|_| Error::Storage)?;
        let mut leases = self.8.lock().map_err(|_| Error::Storage)?;
        for (key, j) in rows
            .iter()
            .filter(|(_, j)| j.tenant() == tenant && j.work_due(now))
        {
            let entry = leases.entry(key.clone()).or_insert((0, None));
            if entry.1.as_ref().is_some_and(|l| l.until() > now) {
                continue;
            }
            entry.0 = entry.0.checked_add(1).ok_or(Error::ResourceLimit)?;
            let lease = plenora_runtime_chains::RunLease::new(
                tenant,
                j.run_id(),
                owner,
                entry.0,
                now + lease_ms,
            )?;
            entry.1 = Some(lease.clone());
            return Ok(Some(lease));
        }
        Ok(None)
    }
    async fn release_work(&self, lease: &plenora_runtime_chains::RunLease) -> Result<(), Error> {
        let mut leases = self.8.lock().map_err(|_| Error::Storage)?;
        let entry = leases
            .get_mut(&(lease.tenant().into(), lease.run_id().into()))
            .ok_or(Error::Conflict)?;
        if entry.1.as_ref().is_none_or(|current| {
            current.owner() != lease.owner() || current.token() != lease.token()
        }) {
            return Err(Error::Conflict);
        }
        entry.1 = None;
        Ok(())
    }

    async fn renew_work(
        &self,
        lease: &plenora_runtime_chains::RunLease,
        lease_ms: u64,
    ) -> Result<plenora_runtime_chains::RunLease, Error> {
        let pause = self.14.lock().map_err(|_| Error::Storage)?.take();
        if let Some((entered, release)) = pause {
            entered.notify_one();
            release.notified().await;
        }
        if !(1..=60_000).contains(&lease_ms) {
            return Err(Error::Invalid);
        }
        let now = u64::try_from(
            self.3
                .now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Error::Storage)?
                .as_millis(),
        )
        .map_err(|_| Error::Storage)?;
        let mut leases = self.8.lock().map_err(|_| Error::Storage)?;
        let entry = leases
            .get_mut(&(lease.tenant().into(), lease.run_id().into()))
            .ok_or(Error::Conflict)?;
        let current = entry.1.as_ref().ok_or(Error::Conflict)?;
        if current.owner() != lease.owner()
            || current.token() != lease.token()
            || current.until() <= now
        {
            return Err(Error::Conflict);
        }
        let renewed = plenora_runtime_chains::RunLease::new(
            lease.tenant(),
            lease.run_id(),
            lease.owner(),
            lease.token(),
            current
                .until()
                .max(now.checked_add(lease_ms).ok_or(Error::ResourceLimit)?),
        )?;
        entry.1 = Some(renewed.clone());
        if self.11.swap(false, Ordering::SeqCst) {
            return Err(Error::Storage);
        }
        Ok(renewed)
    }

    async fn lookup(
        &self,
        tenant: &str,
        id: &str,
        key: Option<&str>,
    ) -> Result<Option<Journal>, Error> {
        let rows = self.0.lock().map_err(|_| Error::Storage)?;
        let mut matches = rows.values().filter(|j| {
            j.tenant() == tenant
                && (j.run_id() == id || (key.is_some() && j.idempotency_key() == key))
        });
        let result = matches.next().cloned();
        if matches.next().is_some() {
            return Err(Error::Conflict);
        }
        Ok(result)
    }
    async fn create(
        &self,
        candidate: plenora_runtime_chains::Submission,
    ) -> Result<Journal, Error> {
        let candidate = candidate.into_journal();
        let mut rows = self.0.lock().map_err(|_| Error::Storage)?;
        for old in rows.values().filter(|j| j.tenant() == candidate.tenant()) {
            if old.run_id() == candidate.run_id()
                || (candidate.idempotency_key().is_some()
                    && old.idempotency_key() == candidate.idempotency_key())
            {
                return if old.same_submission(&candidate) {
                    Ok(old.clone())
                } else {
                    Err(Error::Conflict)
                };
            }
        }
        rows.insert(
            (candidate.tenant().into(), candidate.run_id().into()),
            candidate.clone(),
        );
        Ok(candidate)
    }
    async fn load(&self, tenant: &str, id: &str) -> Result<Journal, Error> {
        self.0
            .lock()
            .map_err(|_| Error::Storage)?
            .get(&(tenant.into(), id.into()))
            .cloned()
            .ok_or(Error::NotFound)
    }
    async fn compare_exchange(
        &self,
        expected: u64,
        next: plenora_runtime_chains::Change,
    ) -> Result<(), Error> {
        let lease = next.lease().cloned();
        let next = next.into_journal();
        let mut rows = self.0.lock().map_err(|_| Error::Storage)?;
        let leases = self.8.lock().map_err(|_| Error::Storage)?;
        let now = u64::try_from(
            self.3
                .now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Error::Storage)?
                .as_millis(),
        )
        .map_err(|_| Error::Storage)?;
        let current = leases
            .get(&(next.tenant().into(), next.run_id().into()))
            .and_then(|(_, l)| l.as_ref());
        let cancel = rows
            .get(&(next.tenant().into(), next.run_id().into()))
            .is_some_and(|old| old.is_cancel_successor(&next));
        match (&lease, current) {
            (Some(l), Some(c))
                if l.owner() == c.owner() && l.token() == c.token() && now < c.until() => {}
            (None, c) if cancel || c.is_none_or(|l| now >= l.until()) => {}
            _ => return Err(Error::Conflict),
        }
        let old = rows
            .get_mut(&(next.tenant().into(), next.run_id().into()))
            .ok_or(Error::NotFound)?;
        if old.revision() != expected || next.revision() != expected + 1 {
            return Err(Error::Conflict);
        }
        old.validate_successor(&next)?;
        if let Some(ledger) = self
            .10
            .lock()
            .map_err(|_| Error::Storage)?
            .get_mut(&(next.tenant().into(), next.run_id().into()))
        {
            let mut candidate = ledger.clone();
            candidate.observe(&next)?;
            let closing = candidate.active() < ledger.active();
            if closing && self.13.load(Ordering::SeqCst) == 1 {
                self.13.store(0, Ordering::SeqCst);
                return Err(Error::Storage);
            }
            candidate.cleanup(now);
            *ledger = candidate;
        }
        *old = Journal::from_json(&next.to_json()?)?;
        if self.13.load(Ordering::SeqCst) == 2
            && self
                .10
                .lock()
                .map_err(|_| Error::Storage)?
                .get(&(next.tenant().into(), next.run_id().into()))
                .is_some_and(|l| l.active() == 0)
        {
            self.13.store(0, Ordering::SeqCst);
            return Err(Error::Storage);
        }
        Ok(())
    }
}
impl plenora_runtime_capabilities::AcceptanceStore for ScriptStore {
    fn accept(
        &self,
        id: &plenora_runtime_messaging::MessageId,
        proposed: Option<plenora_runtime_capabilities::NegotiationContext>,
    ) -> Result<
        plenora_runtime_capabilities::AcceptanceRecord,
        plenora_runtime_capabilities::NegotiationError,
    > {
        use plenora_runtime_capabilities::{AcceptanceRecord, NegotiationError};
        let mut rows = self.2.lock().map_err(|_| NegotiationError)?;
        if let Some(old) = rows.get(id) {
            return Ok(AcceptanceRecord::Existing(*old));
        }
        rows.insert(*id, proposed.ok_or(NegotiationError)?);
        Ok(AcceptanceRecord::Created)
    }
}
#[async_trait]
impl plenora_runtime_boundary::DurableExecutionStore for ScriptStore {
    async fn idempotency(
        &self,
        key: &str,
        binding: [u8; 32],
        candidate: Option<plenora_runtime_boundary::IdempotencyReceipt>,
    ) -> Result<plenora_runtime_boundary::IdempotencyOpen, plenora_runtime_boundary::SettlementError>
    {
        use plenora_runtime_boundary::{IdempotencyOpen, SettlementError};
        let now = ledger_now(self)?;
        let mut rows = self.4.lock().map_err(|_| SettlementError)?;
        if let Some((old, receipt, created)) = rows.get(key) {
            if now.saturating_sub(*created) >= plenora_runtime_chains::DELIVERY_MS {
                return Err(SettlementError);
            }
            return Ok(if old == &binding {
                IdempotencyOpen::Selected(receipt.clone())
            } else {
                IdempotencyOpen::Conflict
            });
        }
        if let Some(candidate) = candidate {
            rows.insert(key.into(), (binding, candidate.clone(), now));
            Ok(IdempotencyOpen::Selected(candidate))
        } else {
            Ok(IdempotencyOpen::Absent)
        }
    }
    async fn open(
        &self,
        key: &str,
        binding: [u8; 32],
        plan: &plenora_runtime_boundary::DeliveryPlan,
    ) -> Result<plenora_runtime_boundary::DeliveryOpen, plenora_runtime_boundary::SettlementError>
    {
        use plenora_runtime_boundary::{DeliveryJournal, DeliveryOpen, SettlementError};
        use plenora_runtime_capabilities::{AcceptanceStore, NegotiationContext, RuntimeBinding};
        let pause = { self.9.lock().map_err(|_| SettlementError)?.take() };
        if let Some((entered, release)) = pause {
            entered.notify_one();
            release.notified().await;
        }
        if self.5.swap(false, Ordering::SeqCst) {
            return Err(SettlementError);
        }
        let now = ledger_now(self)?;
        let mut rows = self.1.lock().map_err(|_| SettlementError)?;
        if let Some(j) = rows.get(key) {
            return Ok(if j.verify(binding, plan).is_ok() {
                DeliveryOpen::Opened(j.clone(), now)
            } else {
                DeliveryOpen::Conflict
            });
        }
        let j = DeliveryJournal::new(
            key,
            binding,
            plan.clone(),
            now,
            plenora_runtime_chains::DELIVERY_MS,
            plenora_runtime_chains::REPORT_MS,
        )?;
        self.accept(
            &j.result_id().parse().map_err(|_| SettlementError)?,
            Some(NegotiationContext {
                requires: RuntimeBinding::V2,
                accepts_downgrade: false,
            }),
        )
        .map_err(|_| SettlementError)?;
        rows.insert(key.into(), j.clone());
        Ok(DeliveryOpen::Opened(j, now))
    }
    async fn load(
        &self,
        key: &str,
    ) -> Result<
        (plenora_runtime_boundary::DeliveryJournal, u64),
        plenora_runtime_boundary::SettlementError,
    > {
        use plenora_runtime_boundary::SettlementError;
        Ok((
            self.1
                .lock()
                .map_err(|_| SettlementError)?
                .get(key)
                .cloned()
                .ok_or(SettlementError)?,
            ledger_now(self)?,
        ))
    }
    async fn change(
        &self,
        key: &str,
        revision: u64,
        change: plenora_runtime_boundary::DeliveryChange,
    ) -> Result<
        (plenora_runtime_boundary::DeliveryJournal, u64),
        plenora_runtime_boundary::SettlementError,
    > {
        use plenora_runtime_boundary::SettlementError;
        if matches!(change, plenora_runtime_boundary::DeliveryChange::Start)
            && let Some(clock) = self.7.lock().map_err(|_| SettlementError)?.take()
        {
            clock
                .advance(std::time::Duration::from_hours(24))
                .map_err(|_| SettlementError)?;
        }
        if matches!(change, plenora_runtime_boundary::DeliveryChange::Settle)
            && self.6.swap(false, Ordering::SeqCst)
        {
            return Err(SettlementError);
        }
        let now = ledger_now(self)?;
        let mut rows = self.1.lock().map_err(|_| SettlementError)?;
        let j = rows.get_mut(key).ok_or(SettlementError)?;
        j.apply(revision, change, now)?;
        Ok((j.clone(), now))
    }
    async fn result_receipt(
        &self,
        scope: &str,
        id: &str,
    ) -> Result<
        (String, plenora_runtime_boundary::DeliveryJournal),
        plenora_runtime_boundary::SettlementError,
    > {
        use plenora_runtime_boundary::SettlementError;
        self.1
            .lock()
            .map_err(|_| SettlementError)?
            .iter()
            .find(|(key, j)| key.starts_with(scope) && j.result_id() == id)
            .map(|(key, j)| (key.clone(), j.clone()))
            .ok_or(SettlementError)
    }
    async fn pending(
        &self,
        _: &str,
        _: &str,
        _: usize,
    ) -> Result<Vec<String>, plenora_runtime_boundary::SettlementError> {
        Ok(vec![])
    }
}
fn ledger_now(store: &ScriptStore) -> Result<u64, plenora_runtime_boundary::SettlementError> {
    u64::try_from(
        store
            .3
            .now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| plenora_runtime_boundary::SettlementError)?
            .as_millis(),
    )
    .map_err(|_| plenora_runtime_boundary::SettlementError)
}
