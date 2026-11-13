//! Public Rust coordinator probes with explicitly scripted adapters and storage.
#![allow(clippy::wildcard_imports, reason = "Public contract API probes")]
use async_trait::async_trait;
use plenora_runtime_chains::{
    Adapter, ChainEngine, Error, Invocation, Journal, RunOptions, Store, contracts::*,
};
use plenora_runtime_testkit::ManualClock;
use std::{
    collections::BTreeMap,
    error::Error as StdError,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, UNIX_EPOCH},
};

#[derive(Default)]
struct ScriptStore(Mutex<BTreeMap<(String, String), Journal>>);
#[async_trait]
impl Store for ScriptStore {
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
struct ScriptAdapter {
    calls: AtomicUsize,
    unknown: bool,
    recovery: bool,
    outcomes: Vec<StepResult>,
    delay: Option<Arc<ManualClock>>,
}
#[async_trait]
impl Adapter for ScriptAdapter {
    async fn prepare(&self, _: &Invocation) -> Result<(), Error> {
        Ok(())
    }
    async fn invoke(&self, i: &Invocation) -> Result<StepResult, Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if i.step.id == "write"
            && let Some(clock) = &self.delay
        {
            clock
                .advance(Duration::from_millis(100_001))
                .map_err(|_| Error::Invalid)?;
        }
        if self.unknown {
            return Err(Error::Recovery);
        }
        self.outcomes
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
            .then(|| {
                self.outcomes
                    .iter()
                    .find(|s| s.step_id == i.step.id)
                    .cloned()
            })
            .flatten())
    }
}
fn plan() -> Result<Plan, Rejection> {
    parse_plan(include_bytes!(
        "../../../contracts/chains-v2/fixtures/plan-valid.json"
    ))
}
fn registry() -> Result<Registry, Rejection> {
    parse_registry(include_bytes!(
        "../../../contracts/chains-v2/fixtures/context.json"
    ))
}
fn adapter(unknown: bool, recovery: bool) -> Result<Arc<ScriptAdapter>, Rejection> {
    Ok(Arc::new(ScriptAdapter {
        calls: AtomicUsize::new(0),
        delay: None,
        unknown,
        recovery,
        outcomes: parse_result(include_bytes!(
            "../../../contracts/chains-v2/fixtures/result-valid.json"
        ))?
        .steps,
    }))
}
fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(UNIX_EPOCH + Duration::from_hours(498_492)))
}
fn options() -> RunOptions {
    RunOptions {
        run_id: uuid::Uuid::new_v4().to_string(),
        idempotency_key: None,
        deadline_ms: None,
    }
}

#[tokio::test]
async fn public_run_preserves_all_steps_outputs_and_replays_without_invocation()
-> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    let adapter = adapter(false, false)?;
    let engine = ChainEngine::new(store, adapter.clone(), registry()?, clock(), "tenant-a")?;
    let options = options();
    let result = engine.run(&plan()?, options.clone()).await?;
    assert_eq!(result.state, "succeeded");
    assert_eq!(result.steps.len(), 3);
    assert_eq!(result.outputs.len(), plan()?.outputs.len());
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 3);
    assert_eq!(engine.run(&plan()?, options).await?, result);
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 3);
    let events = engine.events(&result.run_id, 0, 256).await?;
    assert_eq!(events.len(), 10);
    assert_eq!(events.last().ok_or("event")?.kind, "result_persisted");
    assert!(engine.events(&result.run_id, 100, 2).await.is_err());
    Ok(())
}

#[tokio::test]
async fn unknown_crash_never_replays_and_cancel_does_not_claim_stop()
-> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    let adapter = adapter(true, false)?;
    let engine = ChainEngine::new(
        store.clone(),
        adapter.clone(),
        registry()?,
        clock(),
        "tenant-a",
    )?;
    let handle = engine.submit(&plan()?, options()).await?;
    assert_eq!(engine.advance(&handle.run_id).await, Err(Error::Recovery));
    let restarted = ChainEngine::new(store, adapter.clone(), registry()?, clock(), "tenant-a")?;
    assert_eq!(
        restarted.advance(&handle.run_id).await,
        Err(Error::Recovery)
    );
    let running = restarted.status(&handle.run_id).await?;
    let receipt = restarted.cancel(&handle.run_id, running.revision).await?;
    assert_eq!(receipt.state, "cancel_requested");
    assert_eq!(restarted.result(&handle.run_id).await, Err(Error::Recovery));
    assert_eq!(
        restarted.resume(&handle.run_id, receipt.revision).await,
        Err(Error::Recovery)
    );
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn authenticated_recovery_consumes_evidence_without_reinvoking()
-> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    let adapter = adapter(true, true)?;
    let engine = ChainEngine::new(store, adapter.clone(), registry()?, clock(), "tenant-a")?;
    let handle = engine.submit(&plan()?, options()).await?;
    assert_eq!(engine.advance(&handle.run_id).await, Err(Error::Recovery));
    let running = engine.status(&handle.run_id).await?;
    let resumed = engine.resume(&handle.run_id, running.revision).await?;
    assert_eq!(resumed.state, "pending");
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        engine.resume(&handle.run_id, running.revision).await,
        Err(Error::Conflict)
    );
    Ok(())
}

#[tokio::test]
async fn host_key_is_tenant_scoped_binds_plan_and_never_reaches_adapter()
-> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    let adapter = adapter(false, false)?;
    let engine = ChainEngine::new(
        store.clone(),
        adapter.clone(),
        registry()?,
        clock(),
        "tenant-a",
    )?;
    let mut first = options();
    first.idempotency_key = Some("key-1".into());
    let accepted = engine.submit(&plan()?, first.clone()).await?;
    first.run_id = uuid::Uuid::new_v4().to_string();
    assert_eq!(engine.submit(&plan()?, first.clone()).await?, accepted);
    let mut changed = plan()?;
    changed.revision += 1;
    assert_eq!(
        engine.submit(&changed, first.clone()).await,
        Err(Error::Conflict)
    );
    let other = ChainEngine::new(store, adapter, registry()?, clock(), "tenant-b")?;
    assert_ne!(other.submit(&plan()?, first).await?.run_id, accepted.run_id);
    assert_eq!(other.status(&accepted.run_id).await, Err(Error::NotFound));
    Ok(())
}

#[tokio::test]
async fn cancel_before_start_confirms_no_invocation_and_stale_control_conflicts()
-> Result<(), Box<dyn StdError>> {
    let adapter = adapter(false, false)?;
    let engine = ChainEngine::new(
        Arc::new(ScriptStore::default()),
        adapter.clone(),
        registry()?,
        clock(),
        "tenant-a",
    )?;
    let h = engine.submit(&plan()?, options()).await?;
    let receipt = engine.cancel(&h.run_id, h.revision).await?;
    assert_eq!(
        engine.cancel(&h.run_id, h.revision).await,
        Err(Error::Conflict)
    );
    assert_eq!(
        engine.resume(&h.run_id, receipt.revision).await?.state,
        "cancelled"
    );
    assert_eq!(engine.result(&h.run_id).await?.state, "cancelled");
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn complete_out_of_budget_evidence_remains_retrievable() -> Result<(), Box<dyn StdError>> {
    let mut scripted = adapter(false, false)?;
    let inner = Arc::get_mut(&mut scripted).ok_or("unique adapter")?;
    inner
        .outcomes
        .first_mut()
        .ok_or("first")?
        .counters
        .rows_read = 100_001;
    let engine = ChainEngine::new(
        Arc::new(ScriptStore::default()),
        scripted,
        registry()?,
        clock(),
        "tenant-a",
    )?;
    let h = engine.submit(&plan()?, options()).await?;
    assert_eq!(engine.advance(&h.run_id).await, Err(Error::Recovery));
    let reports = engine.report(&h.run_id).await?;
    assert_eq!(reports.len(), 1);
    let evidence: serde_json::Value = serde_json::from_slice(&reports[0])?;
    assert_eq!(evidence["counters"]["rows_read"], 100_001);
    assert!(evidence["outputs"]["dataset"].is_object());
    assert_eq!(engine.status(&h.run_id).await?.state, "requires_recovery");
    Ok(())
}

#[tokio::test]
async fn elapsed_deadline_is_finalized_without_clamping_observed_time()
-> Result<(), Box<dyn StdError>> {
    let clock = clock();
    let adapter = adapter(false, false)?;
    let engine = ChainEngine::new(
        Arc::new(ScriptStore::default()),
        adapter.clone(),
        registry()?,
        clock.clone(),
        "tenant-a",
    )?;
    let p = plan()?;
    let h = engine.submit(&p, options()).await?;
    clock.advance(Duration::from_millis(p.budgets.elapsed_ms + 1))?;
    assert_eq!(engine.advance(&h.run_id).await?.state, "rejected");
    let result = engine.result(&h.run_id).await?;
    assert_eq!(result.counters.elapsed_ms, p.budgets.elapsed_ms + 1);
    assert_eq!(result.error.ok_or("timeout")?.category, "timeout");
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn reconstructed_snapshot_cannot_extend_deadline_or_erase_cancel()
-> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    let engine = ChainEngine::new(
        store.clone(),
        adapter(false, false)?,
        registry()?,
        clock(),
        "tenant-a",
    )?;
    let h = engine.submit(&plan()?, options()).await?;
    let cancelled = engine.cancel(&h.run_id, h.revision).await?;
    let old = store.load("tenant-a", &h.run_id).await?;
    for field in ["cancelled", "deadline"] {
        let mut value: serde_json::Value = serde_json::from_slice(&old.to_json()?)?;
        value["revision"] = (cancelled.revision + 1).into();
        let mut event: serde_json::Value =
            serde_json::from_slice(&serde_json::from_value::<Vec<u8>>(
                value["events"]
                    .as_array()
                    .ok_or("events")?
                    .last()
                    .ok_or("event")?
                    .clone(),
            )?)?;
        event["sequence"] = (value["events"].as_array().ok_or("events")?.len() as u64 + 1).into();
        event["state_revision"] = (cancelled.revision + 1).into();
        event["event_id"] = uuid::Uuid::new_v4().to_string().into();
        value["events"]
            .as_array_mut()
            .ok_or("events")?
            .push(serde_json::to_value(serde_json::to_vec(&event)?)?);
        if field == "cancelled" {
            value[field] = false.into();
        } else {
            value[field] = (value[field].as_u64().ok_or("deadline")? + 1).into();
        }
        let forged = Journal::from_json(&serde_json::to_vec(&value)?)?;
        assert_eq!(old.validate_successor(&forged), Err(Error::Conflict));
    }
    Ok(())
}

#[tokio::test]
async fn forged_cas_cannot_clear_unknown_intention_even_with_a_valid_event()
-> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    let engine = ChainEngine::new(
        store.clone(),
        adapter(true, false)?,
        registry()?,
        clock(),
        "tenant-a",
    )?;
    let h = engine.submit(&plan()?, options()).await?;
    assert_eq!(engine.advance(&h.run_id).await, Err(Error::Recovery));
    let old = store.load("tenant-a", &h.run_id).await?;
    let mut value: serde_json::Value = serde_json::from_slice(&old.to_json()?)?;
    value["active"] = serde_json::Value::Null;
    value["revision"] = (old.revision() + 1).into();
    let mut event: serde_json::Value = serde_json::from_slice(&serde_json::from_value::<Vec<u8>>(
        value["events"]
            .as_array()
            .ok_or("events")?
            .last()
            .ok_or("event")?
            .clone(),
    )?)?;
    event["sequence"] = (value["events"].as_array().ok_or("events")?.len() as u64 + 1).into();
    event["state_revision"] = (old.revision() + 1).into();
    event["event_id"] = uuid::Uuid::new_v4().to_string().into();
    value["events"]
        .as_array_mut()
        .ok_or("events")?
        .push(serde_json::to_value(serde_json::to_vec(&event)?)?);
    let next = Journal::from_json(&serde_json::to_vec(&value)?)?;
    assert_eq!(old.validate_successor(&next), Err(Error::Conflict));
    Ok(())
}

#[tokio::test]
async fn late_known_failure_preserves_original_error_and_adds_timeout()
-> Result<(), Box<dyn StdError>> {
    let clock = clock();
    let mut scripted = adapter(false, false)?;
    let inner = Arc::get_mut(&mut scripted).ok_or("unique")?;
    inner.outcomes = parse_result(include_bytes!(
        "../../../contracts/chains-v2/fixtures/result-failed-valid.json"
    ))?
    .steps;
    inner.delay = Some(clock.clone());
    let engine = ChainEngine::new(
        Arc::new(ScriptStore::default()),
        scripted,
        registry()?,
        clock,
        "tenant-a",
    )?;
    let result = engine.run(&plan()?, options()).await?;
    assert_eq!(result.state, "partial");
    let step = result
        .steps
        .iter()
        .find(|s| s.step_id == "write")
        .ok_or("write")?;
    assert_eq!(step.error.as_ref().ok_or("original")?.category, "execution");
    assert!(step.other_errors.iter().any(|e| e.category == "timeout"));
    assert_eq!(step.counters.elapsed_ms, 100_001);
    Ok(())
}

#[tokio::test]
async fn multiple_named_outputs_are_complete_and_missing_one_never_succeeds()
-> Result<(), Box<dyn StdError>> {
    for omit in [false, true] {
        let mut plan = plan()?;
        let mut registry = registry()?;
        let port = plan.steps[1].outputs.get("dataset").ok_or("port")?.clone();
        plan.steps[1].outputs.insert("second".into(), port.clone());
        registry.operations[1]
            .outputs
            .insert("second".into(), port.clone());
        let mut output = plan.outputs.get("dataset").ok_or("output")?.clone();
        if let Source::Variant1(source) = &mut output.source {
            source.port = "second".into();
        }
        plan.outputs.insert("second".into(), output);
        let mut scripted = adapter(false, false)?;
        let inner = Arc::get_mut(&mut scripted).ok_or("unique")?;
        if !omit {
            let mut artifact = inner.outcomes[1]
                .outputs
                .get("dataset")
                .ok_or("artifact")?
                .clone();
            artifact.artifact_ref = "ref:second".into();
            inner.outcomes[1].outputs.insert("second".into(), artifact);
        }
        let engine = ChainEngine::new(
            Arc::new(ScriptStore::default()),
            scripted,
            registry,
            clock(),
            "tenant-a",
        )?;
        let result = engine.run(&plan, options()).await;
        if omit {
            assert_eq!(result, Err(Error::Recovery));
        } else {
            let result = result?;
            assert_eq!(result.state, "succeeded");
            assert_eq!(result.outputs.len(), 2);
            assert_eq!(result.steps[1].outputs.len(), 2);
        }
    }
    Ok(())
}
