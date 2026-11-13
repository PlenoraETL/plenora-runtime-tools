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
);
#[async_trait]
impl Store for ScriptStore {
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
        let next = next.into_journal();
        let mut rows = self.0.lock().map_err(|_| Error::Storage)?;
        let old = rows
            .get_mut(&(next.tenant().into(), next.run_id().into()))
            .ok_or(Error::NotFound)?;
        if old.revision() != expected || next.revision() != expected + 1 {
            return Err(Error::Conflict);
        }
        old.validate_successor(&next)?;
        *old = Journal::from_json(&next.to_json()?)?;
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
