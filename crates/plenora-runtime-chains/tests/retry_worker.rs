//! Deterministic retry and worker probes with a declared primary fake and scripted adapter.
use async_trait::async_trait;
use plenora_runtime_chains::{
    Adapter, ChainEngine, Error, Invocation, RunOptions, Store, contracts::*,
};
use plenora_runtime_core::Clock;
use std::{
    error::Error as StdError,
    sync::{Arc, Mutex},
    time::{Duration, UNIX_EPOCH},
};
#[path = "support/primary.rs"]
mod primary;
use primary::ScriptStore;
type Test = Result<(), Box<dyn StdError>>;
struct Script {
    calls: Mutex<Vec<(String, u64)>>,
    failures: u64,
    effect: &'static str,
    pause_step: &'static str,
    pause_attempt: u64,
    pause: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}
#[async_trait]
impl Adapter for Script {
    // Declared coordinator fake: all plans in this test are explicitly authorized.
    async fn authorize(
        &self,
        _: &str,
        _: &plenora_runtime_chains::contracts::Plan,
    ) -> Result<(), Error> {
        Ok(())
    }
    async fn prepare(&self, i: &Invocation) -> Result<(), Error> {
        if i.step.id == "transform" && i.reservation.limits.rows_produced < 3 {
            Err(Error::ResourceLimit)
        } else {
            Ok(())
        }
    }
    async fn request_cancel(&self, _: &Invocation) -> Result<(), Error> {
        Ok(())
    }
    async fn recover(&self, _: &Invocation) -> Result<Option<StepResult>, Error> {
        Err(Error::Recovery)
    }
    async fn invoke(&self, i: &Invocation) -> Result<StepResult, Error> {
        self.calls
            .lock()
            .map_err(|_| Error::Storage)?
            .push((i.step.id.clone(), i.attempt));
        if i.step.id == self.pause_step
            && i.attempt == self.pause_attempt
            && let Some((entered, release)) = &self.pause
        {
            entered.notify_one();
            release.notified().await;
        }
        let mut s = fixture()?
            .steps
            .into_iter()
            .find(|s| s.step_id == i.step.id)
            .ok_or(Error::Invalid)?;
        s.attempt = i.attempt;
        if i.step.id == "transform" {
            s.counters.rows_read = 2;
            s.counters.rows_produced = 3;
        }
        s.input_manifests.clone_from(&i.inputs);
        for output in s.outputs.values_mut() {
            output.attempt = i.attempt;
        }
        if i.step.id == "transform" && i.attempt <= self.failures {
            s.state = if self.effect == "unknown" {
                "requires_recovery"
            } else {
                "failed"
            }
            .into();
            s.outputs.clear();
            s.remote_effect = self.effect.into();
            s.reason = "upstream_error".into();
            s.retry = if self.effect == "unknown" {
                Retry::Variant0(Retry0 {
                    kind: "requires_recovery".into(),
                })
            } else {
                Retry::Variant1(Retry1 {
                    kind: "after".into(),
                    delay_ms: 40,
                })
            };
            s.error = Some(ChainError {
                contract: "plenora-chain-error-v2".into(),
                schema_version: 2,
                category: "execution".into(),
                phase: "read".into(),
                code: "CHAIN_UPSTREAM".into(),
                remote_effect: s.remote_effect.clone(),
                retry: s.retry.clone(),
                message: "Chain operation rejected or incomplete.".into(),
                step_id: Some(s.step_id.clone()),
                upstream_error: None,
                reports: vec![],
            });
        }
        assert!(
            parse_step_result(&serde_json::to_vec(&s).map_err(|_| Error::Invalid)?).is_ok(),
            "script must obey step schema"
        );
        Ok(s)
    }
}
fn fixture() -> Result<ChainResult, Rejection> {
    parse_result(include_bytes!(
        "../../../contracts/chains-v2/fixtures/result-valid.json"
    ))
}
fn plan() -> Result<Plan, Rejection> {
    let mut p = parse_plan(include_bytes!(
        "../../../contracts/chains-v2/fixtures/plan-valid.json"
    ))?;
    p.budgets.max_attempts = 6;
    for s in &mut p.steps {
        s.budgets.max_attempts = 6;
    }
    p.steps[1].budgets.max_attempts = 4;
    p.steps[1].retry_policy.max_attempts = 4;
    p.steps[1].retry_policy.backoff_ms = 50;
    Ok(p)
}
struct Time(Arc<ScriptStore>);
impl Clock for Time {
    fn now(&self) -> std::time::SystemTime {
        self.0.3.now()
    }
}
fn engine(
    store: &Arc<ScriptStore>,
    script: &Arc<Script>,
) -> Result<ChainEngine<ScriptStore>, Error> {
    ChainEngine::new(
        store.clone(),
        script.clone(),
        parse_registry(include_bytes!(
            "../../../contracts/chains-v2/fixtures/context.json"
        ))?,
        Arc::new(Time(store.clone())),
        "tenant",
    )
}
fn setup(
    failures: u64,
    effect: &'static str,
) -> Result<(Arc<ScriptStore>, Arc<Script>), Box<dyn StdError>> {
    let store = Arc::new(ScriptStore::default());
    store.3.advance(Duration::from_hours(498_492))?;
    Ok((
        store,
        Arc::new(Script {
            calls: Mutex::new(vec![]),
            failures,
            effect,
            pause_step: "read",
            pause_attempt: 1,
            pause: None,
        }),
    ))
}
fn options() -> RunOptions {
    RunOptions {
        run_id: uuid::Uuid::new_v4().to_string(),
        idempotency_key: None,
        deadline_ms: None,
    }
}

#[tokio::test]
async fn selective_resume_preserves_success_and_all_attempt_counters_across_restart() -> Test {
    let (store, script) = setup(1, "none")?;
    let e = engine(&store, &script)?;
    let h = e.submit(&plan()?, options()).await?;
    e.advance(&h.run_id).await.map_err(|e| {
        format!(
            "advance at {} calls: {e}",
            script.calls.lock().map_or(0, |c| c.len())
        )
    })?;
    let successful = serde_json::to_value(Store::load(store.as_ref(), "tenant", &h.run_id).await?)?
        ["view"]
        .clone();
    e.advance(&h.run_id).await.map_err(|e| {
        format!(
            "advance at {} calls: {e}",
            script.calls.lock().map_or(0, |c| c.len())
        )
    })?;
    let current = e.status(&h.run_id).await?;
    assert_eq!(current.state, "pending");
    assert_eq!(
        e.resume(&h.run_id, current.revision).await,
        Err(Error::Pending)
    );
    store.3.advance(Duration::from_millis(49))?;
    assert_eq!(e.advance(&h.run_id).await, Err(Error::Pending));
    store.3.advance(Duration::from_millis(1))?;
    let restarted = engine(&store, &script)?;
    restarted
        .resume(&h.run_id, current.revision)
        .await
        .map_err(|e| format!("resume: {e}"))?;
    restarted.advance(&h.run_id).await?;
    let result = restarted.result(&h.run_id).await?;
    assert_eq!(result.state, "succeeded");
    assert_eq!(result.steps[1].attempt, 2);
    assert!(result.steps[1].counters.elapsed_ms >= 50);
    assert_eq!(result.counters.max_attempts, 4);
    assert_eq!(result.steps[1].counters.rows_read, 4);
    let old_bytes: Vec<u8> = serde_json::from_value(successful)?;
    assert_eq!(result.steps[0], parse_result(&old_bytes)?.steps[0]);
    assert_eq!(
        script.calls.lock().map_err(|_| "calls")?.as_slice(),
        &[
            ("read".into(), 1),
            ("transform".into(), 1),
            ("transform".into(), 2),
            ("write".into(), 1)
        ]
    );
    assert_eq!(restarted.report(&h.run_id).await?.len(), 4);
    Ok(())
}
#[tokio::test]
async fn exhaustion_and_unknown_never_continue_to_the_next_step() -> Test {
    for (failures, effect) in [(9, "none"), (1, "unknown")] {
        let (store, script) = setup(failures, effect)?;
        let e = engine(&store, &script)?;
        let h = e.submit(&plan()?, options()).await?;
        e.advance(&h.run_id).await.map_err(|e| {
            format!(
                "advance at {} calls: {e}",
                script.calls.lock().map_or(0, |c| c.len())
            )
        })?;
        for _ in 0..4 {
            let observed = e.advance(&h.run_id).await;
            assert!(observed.is_ok() || observed == Err(Error::Recovery));
            store.3.advance(Duration::from_millis(50))?;
        }
        let result = e.result(&h.run_id).await?;
        assert_ne!(result.state, "succeeded");
        assert_eq!(
            result.steps[1].attempt,
            if effect == "unknown" { 1 } else { 4 }
        );
        assert!(
            !script
                .calls
                .lock()
                .map_err(|_| "calls")?
                .iter()
                .any(|(id, _)| id == "write")
        );
        if effect == "none" {
            let status = e.status(&h.run_id).await?;
            assert_eq!(e.resume(&h.run_id, status.revision).await?, status);
        }
    }
    Ok(())
}
#[tokio::test]
async fn worker_recovers_receipt_crashes_with_monotone_ownership_and_no_double_invoke() -> Test {
    let (store, script) = setup(0, "none")?;
    let e = engine(&store, &script)?;
    let h = e.submit(&plan()?, options()).await?;
    let owner = uuid::Uuid::new_v4().to_string();
    let first = store
        .claim_work("tenant", &owner, 10)
        .await?
        .ok_or("lease")?;
    assert!(
        store
            .claim_work("tenant", &uuid::Uuid::new_v4().to_string(), 10)
            .await?
            .is_none()
    );
    store.3.advance(Duration::from_millis(10))?;
    let second = store
        .claim_work("tenant", &uuid::Uuid::new_v4().to_string(), 10)
        .await?
        .ok_or("lease")?;
    assert!(second.token() > first.token());
    assert_eq!(store.release_work(&first).await, Err(Error::Conflict));
    store.release_work(&second).await?;
    store.5.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(e.work_once(&owner, 10).await?);
    assert!(script.calls.lock().map_err(|_| "calls")?.is_empty());
    store.3.advance(Duration::from_millis(10))?;
    store.6.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(engine(&store, &script)?.work_once(&owner, 10).await?);
    assert_eq!(script.calls.lock().map_err(|_| "calls")?.len(), 1);
    store.3.advance(Duration::from_millis(10))?;
    let a = engine(&store, &script)?;
    let b = engine(&store, &script)?;
    let other = uuid::Uuid::new_v4().to_string();
    let (left, right) = tokio::join!(a.work_once(&owner, 10), b.work_once(&other, 10));
    assert!(left.is_ok() && right.is_ok());
    for _ in 0..4 {
        e.work_once(&owner, 10).await?;
    }
    assert_eq!(e.result(&h.run_id).await?.state, "succeeded");
    assert_eq!(script.calls.lock().map_err(|_| "calls")?.len(), 3);
    assert!(store.3.now() > UNIX_EPOCH);
    Ok(())
}

#[tokio::test]
async fn renewal_keeps_fence_and_lost_confirmation_drains_the_same_receipt() -> Test {
    for lose_confirmation in [false, true] {
        let (store, mut script) = setup(0, "none")?;
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        Arc::get_mut(&mut script).ok_or("adapter")?.pause =
            Some((entered.clone(), release.clone()));
        let engine = Arc::new(engine(&store, &script)?);
        let handle = engine.submit(&plan()?, options()).await?;
        let worker = engine.clone();
        let task = tokio::spawn(async move {
            worker
                .work_once(&uuid::Uuid::new_v4().to_string(), 60)
                .await
        });
        entered.notified().await;
        let original = store
            .8
            .lock()
            .map_err(|_| "leases")?
            .get(&("tenant".into(), handle.run_id.clone()))
            .and_then(|(_, l)| l.clone())
            .ok_or("lease")?;
        store
            .11
            .store(lose_confirmation, std::sync::atomic::Ordering::SeqCst);
        store.3.advance(Duration::from_millis(30))?;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let renewed = store
                    .8
                    .lock()
                    .map_err(|_| Error::Storage)?
                    .get(&("tenant".into(), handle.run_id.clone()))
                    .and_then(|(_, l)| l.clone())
                    .ok_or(Error::Storage)?;
                if renewed.until() > original.until() {
                    assert_eq!(renewed.token(), original.token());
                    break Ok::<(), Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        store.3.advance(Duration::from_millis(40))?;
        assert!(
            store
                .claim_work("tenant", &uuid::Uuid::new_v4().to_string(), 60)
                .await?
                .is_none()
        );
        release.notify_one();
        let result = task.await?;
        if lose_confirmation {
            assert_eq!(result, Err(Error::Storage));
        } else {
            assert_eq!(result, Ok(true));
        }
        assert_eq!(script.calls.lock().map_err(|_| "calls")?.len(), 1);
        assert_eq!(engine.report(&handle.run_id).await?.len(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn cancel_crosses_only_the_control_boundary_of_an_active_lease() -> Test {
    let (store, script) = setup(1, "none")?;
    let e = engine(&store, &script)?;
    let h = e.submit(&plan()?, options()).await?;
    e.advance(&h.run_id).await?;
    e.advance(&h.run_id).await?;
    store.3.advance(Duration::from_millis(50))?;
    let owner = uuid::Uuid::new_v4().to_string();
    let lease = store
        .claim_work("tenant", &owner, 30_000)
        .await?
        .ok_or("lease")?;
    let status = e.status(&h.run_id).await?;
    assert_eq!(e.advance(&h.run_id).await, Err(Error::Conflict));
    e.cancel(&h.run_id, status.revision).await?;
    store.release_work(&lease).await?;
    e.work_once(&owner, 30_000).await?;
    assert_eq!(e.result(&h.run_id).await?.state, "cancelled");
    assert_eq!(script.calls.lock().map_err(|_| "calls")?.len(), 2);
    Ok(())
}
#[tokio::test]
async fn old_owner_finishes_after_takeover_without_losing_receipt_or_stopping_worker() -> Test {
    let (store, mut script) = setup(0, "none")?;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    Arc::get_mut(&mut script).ok_or("adapter ownership")?.pause =
        Some((entered.clone(), release.clone()));
    let e = Arc::new(engine(&store, &script)?);
    let h = e.submit(&plan()?, options()).await?;
    let first = e.clone();
    let old =
        tokio::spawn(async move { first.work_once(&uuid::Uuid::new_v4().to_string(), 10).await });
    entered.notified().await;
    store.3.advance(Duration::from_millis(10))?;
    let owner = uuid::Uuid::new_v4().to_string();
    assert!(e.work_once(&owner, 10).await?); // Running receipt: no second invoke.
    let current = e.status(&h.run_id).await?;
    e.cancel(&h.run_id, current.revision).await?; // Also valid while the actual provider is suspended.
    release.notify_one();
    assert!(old.await??);
    assert_eq!(script.calls.lock().map_err(|_| "calls")?.len(), 1);
    store.3.advance(Duration::from_millis(10))?;
    e.work_once(&owner, 10).await?;
    e.work_once(&owner, 10).await?;
    assert_eq!(script.calls.lock().map_err(|_| "calls")?.len(), 1);
    assert_eq!(e.result(&h.run_id).await?.state, "cancelled");
    Ok(())
}
#[tokio::test]
async fn retry_budgets_deadline_and_unknown_are_quiescent_and_do_not_reset() -> Test {
    for mode in ["global", "local", "deadline", "unknown"] {
        let (store, script) = setup(9, if mode == "unknown" { "unknown" } else { "none" })?;
        let e = engine(&store, &script)?;
        let mut p = plan()?;
        if mode == "global" {
            p.budgets.max_attempts = 3;
            for s in &mut p.steps {
                s.budgets.max_attempts = 3;
                s.retry_policy.max_attempts = s.retry_policy.max_attempts.min(3);
            }
        }
        if mode == "local" {
            p.steps[1].budgets.rows_produced = 3;
        }
        if mode == "deadline" {
            p.steps[1].budgets.elapsed_ms = 50;
        }
        let h = e.submit(&p, options()).await?;
        let owner = uuid::Uuid::new_v4().to_string();
        for _ in 0..8 {
            e.work_once(&owner, 10).await?;
            store.3.advance(Duration::from_millis(50))?;
        }
        let result = e.result(&h.run_id).await?;
        assert_ne!(result.state, "succeeded");
        assert_eq!(result.steps[2].attempt, 0);
        let revision = e.status(&h.run_id).await?.revision;
        assert!(!e.work_once(&owner, 10).await?);
        assert_eq!(e.status(&h.run_id).await?.revision, revision);
    }
    Ok(())
}

#[tokio::test]
async fn current_running_attempt_is_not_confused_with_previous_evidence() -> Test {
    for attempt in [1, 2] {
        let (store, mut script) = setup(1, "none")?;
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let adapter = Arc::get_mut(&mut script).ok_or("unique")?;
        adapter.pause_step = "transform";
        adapter.pause_attempt = attempt;
        adapter.pause = Some((entered.clone(), release.clone()));
        let e = Arc::new(engine(&store, &script)?);
        let h = e.submit(&plan()?, options()).await?;
        e.advance(&h.run_id).await?;
        if attempt == 2 {
            e.advance(&h.run_id).await?;
            store.3.advance(Duration::from_millis(50))?;
        }
        let running = e.clone();
        let id = h.run_id.clone();
        let task = tokio::spawn(async move { running.advance(&id).await });
        entered.notified().await;
        assert_eq!(e.status(&h.run_id).await?.state, "running");
        assert!(matches!(e.result(&h.run_id).await, Err(Error::Pending)));
        release.notify_one();
        task.await??;
    }
    Ok(())
}
#[tokio::test]
async fn intent_causation_deadline_and_fence_inputs_cannot_be_rewritten() -> Test {
    let (store, script) = setup(1, "none")?;
    let e = engine(&store, &script)?;
    let h = e.submit(&plan()?, options()).await?;
    e.advance(&h.run_id).await?;
    e.advance(&h.run_id).await?;
    let before = Store::load(store.as_ref(), "tenant", &h.run_id).await?;
    let before_json = serde_json::to_value(&before)?;
    store.3.advance(Duration::from_millis(50))?;
    store.5.store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(e.advance(&h.run_id).await, Err(Error::Recovery));
    let current = Store::load(store.as_ref(), "tenant", &h.run_id).await?;
    let json = serde_json::to_value(&current)?;
    assert_eq!(
        json["active"]["causation"],
        before_json["progress"]["transform"]["last_message"]
    );
    assert_ne!(json["active"]["message_id"], json["active"]["causation"]);
    for field in ["first_started", "deadline"] {
        let mut changed = json.clone();
        changed["active"][field] = serde_json::json!(1);
        assert!(
            plenora_runtime_chains::Journal::from_json(&serde_json::to_vec(&changed)?).is_err()
        );
    }
    let mut changed = json.clone();
    changed["active"]["causation"] = serde_json::json!(uuid::Uuid::new_v4().to_string());
    let forged = plenora_runtime_chains::Journal::from_json(&serde_json::to_vec(&changed)?)?;
    assert_eq!(before.validate_successor(&forged), Err(Error::Conflict));
    let owner = uuid::Uuid::new_v4().to_string();
    engine(&store, &script)?.work_once(&owner, 30_000).await?;
    assert_eq!(script.calls.lock().map_err(|_| "calls")?.len(), 3);
    assert!(
        e.events(&h.run_id, 0, 256)
            .await?
            .iter()
            .any(|event| event.attempt == 2)
    );
    Ok(())
}

#[tokio::test]
async fn two_owners_before_receipt_creation_share_one_immutable_carrier() -> Test {
    let (store, script) = setup(0, "none")?;
    let e = Arc::new(engine(&store, &script)?);
    let h = e.submit(&plan()?, options()).await?;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *store.9.lock().map_err(|_| "hook")? = Some((entered.clone(), release.clone()));
    let old = e.clone();
    let task =
        tokio::spawn(async move { old.work_once(&uuid::Uuid::new_v4().to_string(), 10).await });
    entered.notified().await;
    store.3.advance(Duration::from_millis(10))?;
    let new_owner = uuid::Uuid::new_v4().to_string();
    assert!(e.work_once(&new_owner, 10).await?);
    release.notify_one();
    assert!(task.await??);
    assert_eq!(script.calls.lock().map_err(|_| "calls")?.len(), 1);
    for _ in 0..3 {
        e.work_once(&new_owner, 10).await?;
    }
    assert_eq!(e.result(&h.run_id).await?.state, "succeeded");
    assert_eq!(script.calls.lock().map_err(|_| "calls")?.len(), 3);
    Ok(())
}

#[tokio::test]
async fn run_waits_for_worker_owned_invocation_without_false_recovery() -> Test {
    let (store, mut script) = setup(0, "none")?;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    Arc::get_mut(&mut script).ok_or("unique")?.pause = Some((entered.clone(), release.clone()));
    let e = Arc::new(engine(&store, &script)?);
    let controls = options();
    let p = plan()?;
    e.submit(&p, controls.clone()).await?;
    let w = e.clone();
    let worker =
        tokio::spawn(async move { w.work_once(&uuid::Uuid::new_v4().to_string(), 30_000).await });
    entered.notified().await;
    let observed = e.run(&p, controls);
    tokio::pin!(observed);
    let pending = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(observed.as_mut(), cx).is_pending())
    })
    .await;
    assert!(pending);
    release.notify_one();
    worker.await??;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), observed)
            .await??
            .state,
        "succeeded"
    );
    assert_eq!(script.calls.lock().map_err(|_| "calls")?.len(), 3);
    Ok(())
}

/// A declared slow primary fake models renewal waiting behind the step transaction.
#[tokio::test]
async fn pending_renewal_keeps_polling_the_step_that_can_release_the_primary() -> Test {
    let (store, mut script) = setup(0, "none")?;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    Arc::get_mut(&mut script).ok_or("unique")?.pause = Some((entered.clone(), release.clone()));
    let renewing = Arc::new(tokio::sync::Notify::new());
    let blocked = Arc::new(tokio::sync::Notify::new());
    *store.14.lock().map_err(|_| "hook")? = Some((renewing.clone(), blocked));
    let e = Arc::new(engine(&store, &script)?);
    e.submit(&plan()?, options()).await?;
    let w = e.clone();
    let worker =
        tokio::spawn(async move { w.work_once(&uuid::Uuid::new_v4().to_string(), 60).await });
    entered.notified().await;
    tokio::time::timeout(Duration::from_secs(10), renewing.notified()).await?;
    release.notify_one();
    assert!(tokio::time::timeout(Duration::from_secs(10), worker).await???);
    assert_eq!(script.calls.lock().map_err(|_| "calls")?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn public_run_error_keeps_the_accepted_receipt_for_recovery() -> Test {
    let (store, script) = setup(0, "none")?;
    let e = engine(&store, &script)?;
    let original = options();
    let handle = e.submit(&plan()?, original.clone()).await?;
    store.5.store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(e.advance(&handle.run_id).await, Err(Error::Recovery));
    store
        .3
        .advance(Duration::from_millis(plan()?.budgets.elapsed_ms))?;
    let reply = e
        .execute(plenora_runtime_chains::Request::Run {
            plan: String::from_utf8(canonical_plan(&plan()?)?)?,
            options: original,
        })
        .await;
    assert_eq!(reply.error, Some(Error::Recovery));
    assert_eq!(reply.value["run_id"], handle.run_id);
    assert!(reply.value["revision"].as_u64().is_some());
    assert_eq!(
        reply.failure().ok_or("failure")?.run_id.as_deref(),
        Some(handle.run_id.as_str())
    );
    assert_eq!(reply.failure().ok_or("failure")?.remote_effect, "unknown");
    assert!(script.calls.lock().map_err(|_| "calls")?.is_empty());
    Ok(())
}

#[tokio::test]
async fn coordination_error_preserves_prior_committed_effect_and_active_uncertainty() -> Test {
    let (store, script) = setup(0, "none")?;
    let e = engine(&store, &script)?;
    let handle = e.submit(&plan()?, options()).await?;
    let mut latest = e.status(&handle.run_id).await?;
    for _ in 0..3 {
        latest = e.advance(&handle.run_id).await?;
    }
    let reply = e
        .execute(plenora_runtime_chains::Request::Cancel {
            run_id: handle.run_id,
            revision: latest.revision - 1,
        })
        .await;
    assert_eq!(reply.error, Some(Error::Conflict));
    let failure = reply.failure().ok_or("failure")?;
    assert_eq!(failure.effect_scope, "run");
    assert_eq!(failure.remote_effect, "committed");
    assert_eq!(reply.value["observed_result"]["state"], "succeeded");
    Ok(())
}
