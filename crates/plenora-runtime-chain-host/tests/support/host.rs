//! End-to-end host probes using a declared primary fake and the real durable dispatcher.
use super::*;
use plenora_runtime_boundary::DeliveryStage;
use plenora_runtime_capabilities::{IDEMPOTENCY_KEY_METADATA_KEY, OUTPUT_CONTRACT_METADATA_KEY};
use plenora_runtime_chains::RunOptions;
use plenora_runtime_messaging::{CORRELATION_ID_METADATA_KEY, MESSAGE_ID_METADATA_KEY};
use plenora_runtime_testkit::ManualClock;
use serde_json::{Value, json};
use std::{
    error::Error as StdError,
    time::{Duration, UNIX_EPOCH},
};
#[path = "../../../plenora-runtime-chains/tests/support/primary.rs"]
mod primary;
use primary::ScriptStore;
type ResultTest = Result<(), Box<dyn StdError>>;
fn config(tenant: &str) -> ScriptedConfig {
    ScriptedConfig {
        database: "host=127.0.0.1 port=1".into(),
        tenant: tenant.into(),
        registry: include_str!("../../../../contracts/chains-v2/fixtures/context.json").into(),
        outcomes: include_str!("../../../../contracts/chains-v2/fixtures/result-valid.json").into(),
        recovery: None,
        attempts: vec![],
        artifacts: BTreeMap::new(),
    }
}
fn host(store: Arc<ScriptStore>, tenant: &str) -> Result<ScriptedHost<ScriptStore>, Error> {
    ScriptedHost::with_store(
        config(tenant),
        store,
        Arc::new(ManualClock::new(UNIX_EPOCH + Duration::from_hours(498_492))),
    )
}
fn request(run: bool, key: Option<&str>) -> Request {
    let plan = include_str!("../../../../contracts/chains-v2/fixtures/plan-valid.json").into();
    let options = RunOptions {
        run_id: uuid::Uuid::new_v4().to_string(),
        idempotency_key: key.map(str::to_owned),
        deadline_ms: None,
    };
    if run {
        Request::Run { plan, options }
    } else {
        Request::Submit { plan, options }
    }
}
fn domain_attempts(store: &ScriptStore) -> Result<u64, Box<dyn StdError>> {
    Ok(store
        .1
        .lock()
        .map_err(|_| "ledger")?
        .values()
        .filter(|j| j.plan().route() == "chain.primary.step-results")
        .map(|j| u64::from(j.attempt()))
        .sum())
}
#[tokio::test]
async fn every_step_and_control_uses_confirmed_dispatcher_receipts() -> ResultTest {
    let store = Arc::new(ScriptStore::default());
    let h = host(store.clone(), "tenant")?;
    let response = h.execute(request(true, Some("run-once"))).await;
    assert_eq!(response.error, None);
    assert_eq!(response.value["state"], "succeeded");
    assert_eq!(response.value["steps"].as_array().ok_or("steps")?.len(), 3);
    assert_eq!(domain_attempts(&store)?, 3);
    {
        let rows = store.1.lock().map_err(|_| "ledger")?;
        assert_eq!(rows.len(), 4);
        for j in rows.values() {
            assert_eq!(j.stage(), DeliveryStage::Finished);
            assert!(j.delivered());
            assert!(j.evidence().is_some());
        }
    }
    let repeated = h.execute(request(true, Some("run-once"))).await;
    assert_eq!(repeated.value, response.value);
    assert_eq!(repeated.error, None);
    assert_eq!(domain_attempts(&store)?, 3);
    Ok(())
}
#[tokio::test]
async fn rt022_aliases_preserve_correlation_contract_and_scope() -> ResultTest {
    let store = Arc::new(ScriptStore::default());
    let h = host(store.clone(), "tenant-a")?;
    let first = control::message(request(false, Some("key")))?;
    let original = h.process(&first).await?;
    let replay = control::message(request(false, Some("key")))?;
    let result = h.process(&replay).await?;
    assert_eq!(original.bytes, result.bytes);
    assert_ne!(
        original.headers.get_text(MESSAGE_ID_METADATA_KEY)?,
        result.headers.get_text(MESSAGE_ID_METADATA_KEY)?
    );
    assert_eq!(
        result.headers.get_text(CORRELATION_ID_METADATA_KEY)?,
        replay.headers.get_text(CORRELATION_ID_METADATA_KEY)?
    );
    assert_eq!(
        original.headers.get_text(OUTPUT_CONTRACT_METADATA_KEY)?,
        result.headers.get_text(OUTPUT_CONTRACT_METADATA_KEY)?
    );
    assert_eq!(h.process(&replay).await?, result);
    let run = h.execute(request(true, Some("key"))).await;
    assert_eq!(run.error, None);
    assert_eq!(run.value["state"], "succeeded");
    let other = host(store.clone(), "tenant-b")?;
    assert_eq!(other.execute(request(false, Some("key"))).await.error, None);
    assert_eq!(store.4.lock().map_err(|_| "aliases")?.len(), 3);
    assert_eq!(domain_attempts(&store)?, 3);
    Ok(())
}
#[tokio::test]
async fn rt022_binding_conflict_and_exact_receipt_conflict_are_distinct() -> ResultTest {
    let store = Arc::new(ScriptStore::default());
    let h = host(store.clone(), "tenant")?;
    let first = control::message(request(false, Some("key")))?;
    assert_eq!(h.dispatch_message(&first).await.error, None);
    let mut changed = control::message(request(false, Some("key")))?;
    let mut body: Value = serde_json::from_slice(&changed.bytes)?;
    let mut plan: Value = serde_json::from_str(body["plan"].as_str().ok_or("plan")?)?;
    plan["revision"] = json!(2);
    body["plan"] = json!(serde_json::to_string(&plan)?);
    changed.bytes = serde_json::to_vec(&body)?.into();
    assert_eq!(
        h.dispatch_message(&changed).await.error,
        Some(Error::Conflict)
    );
    let mut same_id = first.clone();
    same_id.bytes = changed.bytes.clone();
    assert_eq!(
        h.dispatch_message(&same_id).await.error,
        Some(Error::Conflict)
    );
    assert_eq!(domain_attempts(&store)?, 0);
    Ok(())
}
#[tokio::test]
async fn rt022_race_restart_and_permanent_expired_identity() -> ResultTest {
    let store = Arc::new(ScriptStore::default());
    let a = host(store.clone(), "tenant")?;
    let b = host(store.clone(), "tenant")?;
    let (left, right) = tokio::join!(
        a.execute(request(false, Some("race"))),
        b.execute(request(false, Some("race")))
    );
    assert!(left.error.is_none() || right.error.is_none());
    for result in [&left, &right] {
        assert!(result.error.is_none() || result.error == Some(Error::Storage));
    }
    let restarted = host(store.clone(), "tenant")?;
    let replay = restarted.execute(request(false, Some("race"))).await;
    assert_eq!(replay.error, None);
    for result in [&left, &right].into_iter().filter(|r| r.error.is_none()) {
        assert_eq!(result.value, replay.value);
    }
    assert_eq!(store.0.lock().map_err(|_| "runs")?.len(), 1);
    store
        .3
        .advance(Duration::from_millis(plenora_runtime_chains::DELIVERY_MS))?;
    assert_eq!(
        restarted.execute(request(false, Some("race"))).await.error,
        Some(Error::Storage)
    );
    assert_eq!(store.4.lock().map_err(|_| "aliases")?.len(), 1);
    assert_eq!(store.0.lock().map_err(|_| "runs")?.len(), 1);
    Ok(())
}
#[tokio::test]
async fn raw_rejections_and_remote_carrier_cannot_bypass_admission() -> ResultTest {
    let store = Arc::new(ScriptStore::default());
    let h = host(store.clone(), "tenant")?;
    let valid = control::message(Request::Capabilities)?;
    let wire = serde_json::to_vec(&control::Wire::capture(&valid))?;
    let local = h.dispatch_message(&valid).await;
    let remote = h.call_wire(&wire).await;
    assert_eq!(local.error, None);
    assert_eq!(remote.error, None);
    assert_eq!(local.value, remote.value);
    let mut invalid = control::message(Request::Capabilities)?;
    invalid
        .headers
        .insert_text(IDEMPOTENCY_KEY_METADATA_KEY, "unsupported")?;
    assert_eq!(
        h.dispatch_message(&invalid).await.error,
        Some(Error::Unsupported)
    );
    let mut invalid = control::message(Request::Capabilities)?;
    invalid
        .headers
        .insert_text("plenora.negotiation.accepts_downgrade", "true")?;
    assert_eq!(
        h.dispatch_message(&invalid).await.error,
        Some(Error::Unsupported)
    );
    assert_eq!(domain_attempts(&store)?, 0);
    Ok(())
}
#[tokio::test]
async fn unknown_and_recovery_keep_complete_runtime_evidence_without_reinvoke() -> ResultTest {
    let store = Arc::new(ScriptStore::default());
    let mut config = config("tenant");
    config.outcomes =
        include_str!("../../../../contracts/chains-v2/fixtures/result-recovery-valid.json").into();
    config.recovery =
        Some(include_str!("../../../../contracts/chains-v2/fixtures/result-valid.json").into());
    let h = ScriptedHost::with_store(
        config,
        store.clone(),
        Arc::new(ManualClock::new(UNIX_EPOCH + Duration::from_hours(498_492))),
    )?;
    let result = h.execute(request(true, Some("unknown"))).await;
    assert_eq!(result.error, Some(Error::Recovery));
    assert_eq!(result.value["remote_effect"], "unknown");
    assert_eq!(result.value["steps"].as_array().ok_or("steps")?.len(), 3);
    let run_id = result.value["run_id"].as_str().ok_or("id")?.to_owned();
    let status = h
        .execute(Request::Status {
            run_id: run_id.clone(),
        })
        .await;
    let resumed = h
        .execute(Request::Resume {
            run_id: run_id.clone(),
            revision: status.value["revision"].as_u64().ok_or("revision")?,
        })
        .await;
    assert_eq!(resumed.error, None);
    let final_result = h.execute(Request::Result { run_id }).await;
    assert_eq!(final_result.value["state"], "succeeded");
    assert_eq!(domain_attempts(&store)?, 4); // Three original calls, one read-only reconciliation receipt.
    Ok(())
}

#[tokio::test]
async fn rt022_replay_after_deadline_and_new_admission_error_precedence() -> ResultTest {
    let store = Arc::new(ScriptStore::default());
    let h = host(store.clone(), "tenant")?;
    for key in [None, Some("new-key")] {
        let mut message = control::message(request(false, key))?;
        let mut body: Value = serde_json::from_slice(&message.bytes)?;
        body["plan"] = json!("{}");
        message.bytes = serde_json::to_vec(&body)?.into();
        message.headers.insert_text(
            plenora_runtime_capabilities::EXECUTION_DEADLINE_METADATA_KEY,
            "1970-01-01T00:00:00Z",
        )?;
        assert_eq!(
            h.dispatch_message(&message).await.error,
            Some(Error::Timeout)
        );
    }
    let clock = Arc::new(ManualClock::new(UNIX_EPOCH + Duration::from_hours(498_492)));
    let h = ScriptedHost::with_store(config("deadline"), store.clone(), clock.clone())?;
    let mut message = control::message(request(false, Some("replay-key")))?;
    let deadline = "2026-11-13T12:00:01Z";
    message.headers.insert_text(
        plenora_runtime_capabilities::EXECUTION_DEADLINE_METADATA_KEY,
        deadline,
    )?;
    let original = h.dispatch_message(&message).await;
    assert_eq!(original.error, None);
    clock.advance(Duration::from_secs(2))?;
    message
        .headers
        .insert_text(MESSAGE_ID_METADATA_KEY, uuid::Uuid::new_v4().to_string())?;
    message.headers.insert_text(
        CORRELATION_ID_METADATA_KEY,
        uuid::Uuid::new_v4().to_string(),
    )?;
    let replay = h.dispatch_message(&message).await;
    assert_eq!(replay.error, None);
    assert_eq!(original.value, replay.value);
    Ok(())
}

#[tokio::test]
async fn scripted_output_reservation_is_checked_before_any_step_invocation() -> ResultTest {
    let store = Arc::new(ScriptStore::default());
    let h = host(store.clone(), "tenant")?;
    let mut call = request(true, None);
    if let Request::Run { plan, .. } = &mut call {
        let mut value: Value = serde_json::from_str(plan)?;
        value["steps"][0]["budgets"]["result_bytes"] = json!(1);
        *plan = serde_json::to_string(&value)?;
    }
    let result = h.execute(call).await;
    assert!(result.error.is_some());
    assert_eq!(result.value["state"], "rejected");
    assert_eq!(result.value["error"]["category"], "resource_limit");
    assert_eq!(domain_attempts(&store)?, 0);
    Ok(())
}

#[tokio::test]
async fn autonomous_host_worker_scans_runs_after_host_restart_and_drains_shutdown() -> ResultTest {
    let store = Arc::new(ScriptStore::default());
    store.3.advance(Duration::from_hours(498_492))?;
    let first = host(store.clone(), "worker")?;
    let accepted = first.execute(request(false, None)).await;
    assert_eq!(accepted.error, None);
    let run_id = accepted.value["run_id"].as_str().ok_or("run")?.to_owned();
    drop(first);
    let restarted = Arc::new(host(store.clone(), "worker")?);
    let worker = restarted.clone();
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move { worker.work(stopped).await });
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let response = restarted
                .execute(Request::Result {
                    run_id: run_id.clone(),
                })
                .await;
            if response.error.is_none() {
                break response;
            }
            assert!(matches!(
                response.error,
                Some(Error::Pending | Error::Recovery)
            ));
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    stop.send(true)?;
    task.await??;
    assert_eq!(result.value["state"], "succeeded");
    assert_eq!(domain_attempts(&store)?, 3);
    Ok(())
}
