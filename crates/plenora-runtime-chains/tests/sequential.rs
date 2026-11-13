//! Public Rust coordinator probes with explicitly scripted adapters and storage.
#![allow(clippy::wildcard_imports, reason = "Public contract API probes")]
use async_trait::async_trait;
use plenora_runtime_chains::{
    Adapter, ChainEngine, Error, Invocation, Journal, RunOptions, Store, contracts::*,
};
use plenora_runtime_core::Clock;
use plenora_runtime_testkit::ManualClock;
use std::{
    error::Error as StdError,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, UNIX_EPOCH},
};

#[path = "support/primary.rs"]
mod primary;
use primary::ScriptStore;
struct ScriptAdapter {
    denied: AtomicUsize,
    empty_recoveries: usize,
    calls: AtomicUsize,
    recoveries: AtomicUsize,
    unknown: bool,
    recovery: bool,
    outcomes: Vec<StepResult>,
    delay: Option<Arc<ManualClock>>,
}
#[async_trait]
impl Adapter for ScriptAdapter {
    // Declared coordinator fake: all plans in this test are explicitly authorized.
    async fn authorize(
        &self,
        _: &str,
        _: &plenora_runtime_chains::contracts::Plan,
    ) -> Result<(), Error> {
        if self.denied.load(Ordering::SeqCst) == 1 {
            Err(Error::Invalid)
        } else {
            Ok(())
        }
    }
    async fn prepare(&self, _: &Invocation) -> Result<(), Error> {
        if self.denied.load(Ordering::SeqCst) == 2 {
            Err(Error::ResourceLimit)
        } else {
            Ok(())
        }
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
        let count = self.recoveries.fetch_add(1, Ordering::SeqCst) + 1;
        if count <= self.empty_recoveries {
            return Ok(None);
        }
        let mut proof = self
            .recovery
            .then(|| {
                self.outcomes
                    .iter()
                    .find(|s| s.step_id == i.step.id)
                    .cloned()
            })
            .flatten();
        // Explicit fake: each unknown inquiry reports newly observed read work.
        if let Some(proof) = &mut proof
            && proof.remote_effect == "unknown"
        {
            proof.counters.rows_read = count as u64;
        }
        Ok(proof)
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
        denied: AtomicUsize::new(0),
        empty_recoveries: 0,
        calls: AtomicUsize::new(0),
        recoveries: AtomicUsize::new(0),
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
    assert!(Journal::from_json(&serde_json::to_vec(&value)?).is_err());
    value["waiting_recovery"] = false.into();
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

#[tokio::test]
async fn expired_deadline_replays_binding_but_never_accepts_new_work()
-> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    let adapter = adapter(false, false)?;
    let clock = clock();
    let engine = ChainEngine::new(
        store.clone(),
        adapter.clone(),
        registry()?,
        clock.clone(),
        "tenant-a",
    )?;
    let now = u64::try_from(clock.now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let mut controls = options();
    controls.idempotency_key = Some("expired-deadline".into());
    controls.deadline_ms = Some(now + 10);
    let accepted = engine.submit(&plan()?, controls.clone()).await?;
    clock.advance(Duration::from_millis(11))?;
    assert_eq!(engine.submit(&plan()?, controls.clone()).await?, accepted);
    controls.run_id = uuid::Uuid::new_v4().to_string();
    assert_eq!(engine.submit(&plan()?, controls.clone()).await?, accepted);
    let mut changed = plan()?;
    changed.revision += 1;
    assert_eq!(
        engine.submit(&changed, controls.clone()).await,
        Err(Error::Conflict)
    );
    controls.deadline_ms = Some(now + 20);
    assert_eq!(
        engine.submit(&plan()?, controls.clone()).await,
        Err(Error::Conflict)
    );
    controls.deadline_ms = Some(now + 10);
    controls.idempotency_key = Some("new-key".into());
    assert_eq!(
        engine.submit(&plan()?, controls.clone()).await,
        Err(Error::Timeout)
    );
    let other = ChainEngine::new(
        store,
        adapter.clone(),
        registry()?,
        clock.clone(),
        "tenant-b",
    )?;
    assert_eq!(other.submit(&plan()?, controls).await, Err(Error::Timeout));
    clock.advance(Duration::from_millis(plenora_runtime_chains::DELIVERY_MS))?;
    let mut replay = options();
    replay.run_id = accepted.run_id;
    replay.idempotency_key = Some("expired-deadline".into());
    replay.deadline_ms = Some(now + 10);
    assert_eq!(engine.submit(&plan()?, replay).await, Err(Error::Expired));
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn event_cursor_rejects_whole_malformed_pages_without_advancing()
-> Result<(), Box<dyn StdError>> {
    let engine = ChainEngine::new(
        Arc::new(ScriptStore::default()),
        adapter(false, false)?,
        registry()?,
        clock(),
        "tenant-a",
    )?;
    let result = engine.run(&plan()?, options()).await?;
    let events = engine.events(&result.run_id, 0, 256).await?;
    let mut cursor = plenora_runtime_chains::EventCursor::new(&result.run_id, 0, 2)?;
    let initial_request = serde_json::to_value(cursor.request())?;
    let mut bad = serde_json::to_value(&events[..2])?;
    bad[1]["sequence"] = serde_json::json!(3);
    assert!(cursor.accept(&bad).is_err());
    assert_eq!(serde_json::to_value(cursor.request())?, initial_request);
    let mut foreign = serde_json::to_value(&events[..2])?;
    foreign[1]["run_id"] = serde_json::json!(uuid::Uuid::new_v4().to_string());
    assert!(cursor.accept(&foreign).is_err());
    assert_eq!(serde_json::to_value(cursor.request())?, initial_request);
    assert!(cursor.accept(&serde_json::to_value(&events[..3])?).is_err());
    assert_eq!(serde_json::to_value(cursor.request())?, initial_request);
    let mut actual = Vec::new();
    for page in events.chunks(2) {
        actual.extend(cursor.accept(&serde_json::to_value(page)?)?);
    }
    assert_eq!(actual, events);
    assert!(cursor.accept(&serde_json::json!([]))?.is_empty());
    assert_eq!(serde_json::to_value(cursor.request())?["cursor"], 10);
    Ok(())
}

#[tokio::test]
async fn crash_before_runtime_receipt_never_grants_a_reconciliation_shortcut()
-> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    let adapter = adapter(false, true)?;
    let engine = ChainEngine::new(
        store.clone(),
        adapter.clone(),
        registry()?,
        clock(),
        "tenant",
    )?;
    let handle = engine.submit(&plan()?, options()).await?;
    store.5.store(true, Ordering::SeqCst);
    assert_eq!(engine.advance(&handle.run_id).await, Err(Error::Recovery));
    let current = engine.status(&handle.run_id).await?;
    assert_eq!(
        engine.resume(&handle.run_id, current.revision).await,
        Err(Error::Recovery)
    );
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    assert!(store.1.lock().map_err(|_| "ledger")?.is_empty());
    Ok(())
}
#[tokio::test]
async fn crash_after_runtime_evidence_finalizes_without_reinvoking_domain()
-> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    let adapter = adapter(false, false)?;
    let engine = ChainEngine::new(
        store.clone(),
        adapter.clone(),
        registry()?,
        clock(),
        "tenant",
    )?;
    let controls = options();
    let handle = engine.submit(&plan()?, controls.clone()).await?;
    store.6.store(true, Ordering::SeqCst);
    assert_eq!(engine.advance(&handle.run_id).await, Err(Error::Recovery));
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    let current = engine.status(&handle.run_id).await?;
    engine.resume(&handle.run_id, current.revision).await?;
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.run(&plan()?, controls).await?.state, "succeeded");
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn deadline_after_start_cannot_use_fake_recovery_as_execution_proof()
-> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    let adapter = adapter(false, true)?;
    let clock = clock();
    let engine = ChainEngine::new(
        store.clone(),
        adapter.clone(),
        registry()?,
        clock.clone(),
        "tenant",
    )?;
    let handle = engine.submit(&plan()?, options()).await?;
    *store.7.lock().map_err(|_| "clock hook")? = Some(clock);
    assert_eq!(engine.advance(&handle.run_id).await, Err(Error::Recovery));
    {
        let rows = store.1.lock().map_err(|_| "ledger")?;
        assert_eq!(rows.len(), 1);
        let receipt = rows.values().next().ok_or("receipt")?;
        assert_eq!(receipt.attempt(), 1);
        assert_eq!(
            receipt.stage(),
            plenora_runtime_boundary::DeliveryStage::Finished
        );
        assert!(
            receipt
                .evidence()
                .ok_or("evidence")?
                .report()
                .get("output")
                .is_none()
        );
    }
    let current = engine.status(&handle.run_id).await?;
    assert_eq!(
        engine.resume(&handle.run_id, current.revision).await,
        Err(Error::Recovery)
    );
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    assert_eq!(adapter.recoveries.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn replayed_run_returns_confirmed_recovery_without_waiting_for_deadline()
-> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    let adapter = adapter(true, false)?;
    let e = ChainEngine::new(store, adapter.clone(), registry()?, clock(), "tenant")?;
    let controls = options();
    let h = e.submit(&plan()?, controls.clone()).await?;
    assert_eq!(e.advance(&h.run_id).await, Err(Error::Recovery));
    let p = plan()?;
    let result = e.run(&p, controls);
    tokio::pin!(result);
    let observed = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(result.as_mut(), cx))
    })
    .await;
    assert!(matches!(
        observed,
        std::task::Poll::Ready(Err(Error::Recovery))
    ));
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn revoked_admission_after_intent_never_mints_unknown_or_recovery_authority()
-> Result<(), Box<dyn StdError>> {
    for denial in [1, 2] {
        let store = Arc::new(ScriptStore::default());
        store.3.advance(Duration::from_hours(498_492))?;
        let adapter = adapter(false, true)?;
        let engine = ChainEngine::new(
            store.clone(),
            adapter.clone(),
            registry()?,
            clock(),
            "tenant",
        )?;
        let accepted = engine.submit(&plan()?, options()).await?;
        store.5.store(true, Ordering::SeqCst);
        assert_eq!(engine.advance(&accepted.run_id).await, Err(Error::Recovery));
        adapter.denied.store(denial, Ordering::SeqCst);
        engine
            .work_once(&uuid::Uuid::new_v4().to_string(), 30_000)
            .await?;
        {
            let rows = store.1.lock().map_err(|_| "ledger")?;
            assert_eq!(rows.len(), 1);
            let report = rows
                .values()
                .next()
                .ok_or("receipt")?
                .evidence()
                .ok_or("evidence")?
                .report();
            assert_eq!(report["remote_effect"], "none");
            assert!(report.get("output").is_none());
        }
        adapter.denied.store(0, Ordering::SeqCst);
        let current = engine.status(&accepted.run_id).await?;
        assert_eq!(
            engine.resume(&accepted.run_id, current.revision).await,
            Err(Error::Recovery)
        );
        assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
        assert_eq!(adapter.recoveries.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn journal_reservation_rejects_before_effect_and_preserves_reports()
-> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    let adapter = adapter(false, false)?;
    let engine = ChainEngine::new(
        store.clone(),
        adapter.clone(),
        registry()?,
        clock(),
        "tenant",
    )?;
    let accepted = engine.submit(&plan()?, options()).await?;
    let initial = Store::load(store.as_ref(), "tenant", &accepted.run_id).await?;
    assert_eq!(initial.encoded_size()?, initial.to_json()?.len());
    let cap = initial.reserved_bytes()?;
    let engine = ChainEngine::new(
        store.clone(),
        adapter.clone(),
        registry()?,
        clock(),
        "tenant",
    )?
    .with_journal_budget(cap)?;
    let limited = engine.submit(&plan()?, options()).await?;
    let rejected = engine.advance(&limited.run_id).await?;
    assert_eq!(rejected.state, "rejected");
    assert_eq!(
        engine
            .result(&limited.run_id)
            .await?
            .error
            .ok_or("error")?
            .category,
        "resource_limit"
    );
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    // An accepted quota cannot be increased by a restarted host with a larger default.
    let restarted = ChainEngine::new(
        store.clone(),
        adapter.clone(),
        registry()?,
        clock(),
        "tenant",
    )?;
    assert_eq!(restarted.advance(&limited.run_id).await?, rejected);
    // The original run has a full allocation; complete raw receipts remain retrievable.
    restarted.advance(&accepted.run_id).await?;
    let raw = restarted.report(&accepted.run_id).await?;
    assert_eq!(raw.len(), 1);
    assert_eq!(parse_step_result(&raw[0])?.step_id, "read");
    let stored = Store::load(store.as_ref(), "tenant", &accepted.run_id).await?;
    assert_eq!(stored.encoded_size()?, stored.to_json()?.len());
    assert!(stored.encoded_size()? <= stored.reserved_bytes()?);
    let mut forged: serde_json::Value = serde_json::from_slice(&stored.to_json()?)?;
    forged["allocation"]["limit"] = serde_json::json!(1);
    assert!(matches!(
        Journal::from_json(&serde_json::to_vec(&forged)?),
        Err(Error::ResourceLimit)
    ));
    Ok(())
}

#[tokio::test]
async fn confirmed_empty_recovery_allows_a_new_reserved_inquiry() -> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    let mut adapter = adapter(true, true)?;
    Arc::get_mut(&mut adapter)
        .ok_or("unique fake")?
        .empty_recoveries = 1;
    let engine = ChainEngine::new(store, adapter.clone(), registry()?, clock(), "tenant")?;
    let accepted = engine.submit(&plan()?, options()).await?;
    assert_eq!(engine.advance(&accepted.run_id).await, Err(Error::Recovery));
    let status = engine.status(&accepted.run_id).await?;
    assert_eq!(
        engine.resume(&accepted.run_id, status.revision).await,
        Err(Error::Recovery)
    );
    let status = engine.status(&accepted.run_id).await?;
    engine.resume(&accepted.run_id, status.revision).await?;
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    assert_eq!(adapter.recoveries.load(Ordering::SeqCst), 2);
    assert_eq!(engine.report(&accepted.run_id).await?.len(), 1);
    Ok(())
}
#[tokio::test]
async fn recovery_growth_stops_before_inquiry_and_keeps_every_original_report()
-> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    let mut adapter = adapter(false, true)?;
    Arc::get_mut(&mut adapter).ok_or("unique fake")?.outcomes = parse_result(include_bytes!(
        "../../../contracts/chains-v2/fixtures/result-recovery-valid.json"
    ))?
    .steps;
    let engine = ChainEngine::new(
        store.clone(),
        adapter.clone(),
        registry()?,
        clock(),
        "tenant",
    )?;
    let options = options();
    assert_eq!(
        engine.run(&plan()?, options.clone()).await?.state,
        "requires_recovery"
    );
    let original = engine.report(&options.run_id).await?;
    let mut exhausted = false;
    for _ in 0..20 {
        let status = engine.status(&options.run_id).await?;
        let calls = adapter.recoveries.load(Ordering::SeqCst);
        match engine.resume(&options.run_id, status.revision).await {
            Ok(_) => {}
            Err(Error::ResourceLimit) => {
                assert_eq!(adapter.recoveries.load(Ordering::SeqCst), calls);
                exhausted = true;
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
    assert!(exhausted);
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 3);
    let reports = engine.report(&options.run_id).await?;
    assert!(reports.starts_with(&original));
    assert_eq!(
        reports.len(),
        original.len() + adapter.recoveries.load(Ordering::SeqCst)
    );
    assert!(serde_json::to_vec(&reports)?.len() <= 1024 * 1024 / 2);
    let journal = Store::load(store.as_ref(), "tenant", &options.run_id).await?;
    assert!(journal.encoded_size()? <= journal.reserved_bytes()?);
    assert!(journal.reserved_bytes()? <= plenora_runtime_chains::JOURNAL_BYTES);
    Ok(())
}

#[tokio::test]
async fn denied_recovery_can_be_reauthorized_without_reinvoking_the_original()
-> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    let adapter = adapter(true, true)?;
    let engine = ChainEngine::new(store, adapter.clone(), registry()?, clock(), "tenant")?;
    let accepted = engine.submit(&plan()?, options()).await?;
    assert_eq!(engine.advance(&accepted.run_id).await, Err(Error::Recovery));
    adapter.denied.store(1, Ordering::SeqCst);
    let status = engine.status(&accepted.run_id).await?;
    assert_eq!(
        engine.resume(&accepted.run_id, status.revision).await,
        Err(Error::Recovery)
    );
    assert_eq!(adapter.recoveries.load(Ordering::SeqCst), 0);
    adapter.denied.store(0, Ordering::SeqCst);
    let status = engine.status(&accepted.run_id).await?;
    engine.resume(&accepted.run_id, status.revision).await?;
    assert_eq!(adapter.recoveries.load(Ordering::SeqCst), 1);
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    Ok(())
}
#[tokio::test]
async fn legacy_unreserved_intention_never_starts_a_new_receipt() -> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    store.3.advance(Duration::from_hours(498_492))?;
    let adapter = adapter(false, true)?;
    let engine = ChainEngine::new(
        store.clone(),
        adapter.clone(),
        registry()?,
        clock(),
        "tenant",
    )?;
    let accepted = engine.submit(&plan()?, options()).await?;
    store.5.store(true, Ordering::SeqCst);
    assert_eq!(engine.advance(&accepted.run_id).await, Err(Error::Recovery));
    {
        let mut rows = store.0.lock().map_err(|_| "primary")?;
        let old = rows
            .get_mut(&("tenant".into(), accepted.run_id.clone()))
            .ok_or("run")?;
        let mut value = serde_json::to_value(&*old)?;
        value.as_object_mut().ok_or("object")?.remove("allocation");
        value["active"]
            .as_object_mut()
            .ok_or("intent")?
            .remove("result_limit");
        *old = Journal::from_json(&serde_json::to_vec(&value)?)?;
    }
    engine
        .work_once(&uuid::Uuid::new_v4().to_string(), 30_000)
        .await?;
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    assert!(store.1.lock().map_err(|_| "receipts")?.is_empty());
    Ok(())
}

#[tokio::test]
async fn crash_before_intention_confirmation_never_invokes_and_restart_is_safe()
-> Result<(), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    let adapter = adapter(false, false)?;
    let engine = ChainEngine::new(
        store.clone(),
        adapter.clone(),
        registry()?,
        clock(),
        "tenant",
    )?;
    let controls = options();
    let accepted = engine.submit(&plan()?, controls.clone()).await?;
    let before = store.load("tenant", &accepted.run_id).await?.to_json()?;
    store.15.store(true, Ordering::SeqCst);
    assert_eq!(engine.advance(&accepted.run_id).await, Err(Error::Storage));
    assert_eq!(
        store.load("tenant", &accepted.run_id).await?.to_json()?,
        before
    );
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    assert!(store.1.lock().map_err(|_| "ledger")?.is_empty());
    drop(engine);
    let restarted = ChainEngine::new(store, adapter.clone(), registry()?, clock(), "tenant")?;
    assert_eq!(restarted.run(&plan()?, controls).await?.state, "succeeded");
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 3);
    Ok(())
}
