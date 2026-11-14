//! Ownership and cleanup crash probes with an explicitly declared primary/provider double.
use super::*;

// Keep the crash window and its restart/evidence assertions readable in one sequence.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn cleanup_survives_cancel_and_both_sides_of_commit_without_losing_evidence() -> ResultTest {
    for mode in ["cancel", "before_commit", "after_commit"] {
        let store = Arc::new(ScriptStore::default());
        store.3.advance(Duration::from_hours(498_492))?;
        let h = Arc::new(host(store.clone(), "owned")?);
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *store.12.lock().map_err(|_| "pause")? = Some((entered.clone(), release.clone()));
        let call = request(true, None);
        let run_id = match &call {
            Request::Run { options, .. } => options.run_id.clone(),
            _ => return Err("call".into()),
        };
        let running = h.clone();
        let task = tokio::spawn(async move { running.execute(call).await });
        tokio::time::timeout(Duration::from_secs(10), entered.notified()).await?;
        let ledger = store
            .resource_snapshot("owned", &run_id)
            .await?
            .ok_or("ledger")?;
        assert_eq!(ledger.active(), 1);
        assert!(ledger.live_bytes() > 0);
        let reference = crate::ownership::scoped(&run_id, "ref:read");
        assert_eq!(
            h.execute(Request::Artifact {
                reference: reference.clone()
            })
            .await
            .error,
            Some(Error::Pending)
        );
        if mode == "cancel" {
            let status = h
                .execute(Request::Status {
                    run_id: run_id.clone(),
                })
                .await;
            assert_eq!(
                h.execute(Request::Cancel {
                    run_id: run_id.clone(),
                    revision: status.value["revision"].as_u64().ok_or("revision")?
                })
                .await
                .error,
                None
            );
            store.cleanup_resources("owned").await?;
            assert_eq!(
                store
                    .resource_snapshot("owned", &run_id)
                    .await?
                    .ok_or("ledger")?
                    .live_bytes(),
                ledger.live_bytes()
            );
        } else {
            store.13.store(
                if mode == "before_commit" { 1 } else { 2 },
                std::sync::atomic::Ordering::SeqCst,
            );
        }
        release.notify_one();
        let _first_response = task.await?;
        drop(h);
        let restarted = Arc::new(host(store.clone(), "owned")?);
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let serving = restarted.clone();
        let worker = tokio::spawn(async move { serving.work(stopped).await });
        let completed = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let response = restarted
                    .execute(Request::Result {
                        run_id: run_id.clone(),
                    })
                    .await;
                if response.error.is_none() {
                    break response;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        stop.send(true)?;
        worker.await??;
        assert_eq!(
            completed.value["state"],
            if mode == "cancel" {
                "cancelled"
            } else {
                "succeeded"
            }
        );
        assert_eq!(
            store
                .resource_snapshot("owned", &run_id)
                .await?
                .ok_or("ledger")?
                .active(),
            0
        );
        let actual = restarted
            .execute(Request::Artifact {
                reference: reference.clone(),
            })
            .await;
        assert_eq!(actual.error, None);
        assert_eq!(
            actual.value,
            serde_json::to_value(&config("owned")?.artifacts["ref:read"])?
        );
        assert_eq!(
            host(store.clone(), "other")?
                .execute(Request::Artifact {
                    reference: reference.clone()
                })
                .await
                .error,
            Some(Error::NotFound)
        );
        let before = serde_json::to_vec(&store.load("owned", &run_id).await?)?;
        store
            .3
            .advance(Duration::from_millis(plenora_runtime_chains::REPORT_MS))?;
        assert_eq!(
            restarted
                .execute(Request::Artifact {
                    reference: reference.clone()
                })
                .await
                .error,
            Some(Error::Expired)
        );
        // The expired read does not perform cleanup or erase physical evidence.
        assert!(
            store
                .resource_snapshot("owned", &run_id)
                .await?
                .ok_or("ledger")?
                .live_bytes()
                > 0
        );
        let receipts = store.1.lock().map_err(|_| "receipts")?.len();
        store.cleanup_resources("owned").await?;
        store.cleanup_resources("owned").await?;
        assert_eq!(
            store
                .resource_snapshot("owned", &run_id)
                .await?
                .ok_or("tombstones")?
                .live_bytes(),
            0
        );
        assert_eq!(
            serde_json::to_vec(&store.load("owned", &run_id).await?)?,
            before
        );
        assert_eq!(store.1.lock().map_err(|_| "receipts")?.len(), receipts);
        assert_eq!(
            restarted
                .execute(Request::Artifact { reference })
                .await
                .error,
            Some(Error::Expired)
        );
        assert_eq!(
            domain_attempts(&store)?,
            if mode == "cancel" { 2 } else { 3 }
        ); // cancel has its own durable control inquiry.
    }
    Ok(())
}

// Compare known and unknown stop authority using the same explicit fault scenario.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn failed_and_unknown_steps_keep_distinct_cleanup_authority_and_recovery_imports_outputs()
-> ResultTest {
    for unknown in [false, true] {
        let store = Arc::new(ScriptStore::default());
        store.3.advance(Duration::from_hours(498_492))?;
        let mut profile = config("failure")?;
        let original = profile.outcomes.clone();
        let plan_raw = profile.authorized_plans[0].clone();
        let mut outcomes: Value = serde_json::from_str(&original)?;
        let step = &mut outcomes["steps"][0];
        step["state"] = json!(if unknown {
            "requires_recovery"
        } else {
            "failed"
        });
        step["reason"] = json!(if unknown {
            "recovery_required"
        } else {
            "upstream_error"
        });
        step["outputs"] = json!({});
        step["remote_effect"] = json!(if unknown { "unknown" } else { "none" });
        step["retry"] = json!({"kind": if unknown { "requires_recovery" } else { "never" }});
        step["stop_confirmed"] = json!(!unknown);
        step["error"] = json!({"contract":"plenora-chain-error-v2","schema_version":2,"category":"io","phase":"read","code":"CHAIN_UPSTREAM","remote_effect":step["remote_effect"],"retry":step["retry"],"message":"Chain operation rejected or incomplete.","step_id":"read","upstream_error":null,"reports":[]});
        profile.outcomes = serde_json::to_string(&outcomes)?;
        profile.recovery = Some(original);
        let h = ScriptedHost::with_store(
            profile,
            store.clone(),
            Arc::new(ManualClock::new(UNIX_EPOCH + Duration::from_hours(498_492))),
        )?;
        let response = h.execute(request(true, None)).await;
        assert!(response.error.is_some());
        let id = response.value["run_id"].as_str().ok_or("run")?.to_owned();
        let ledger = store
            .resource_snapshot("failure", &id)
            .await?
            .ok_or("resources")?;
        assert_eq!(ledger.active(), usize::from(unknown));
        if !unknown {
            assert_eq!(ledger.live_bytes(), 0);
            continue;
        }
        assert!(ledger.live_bytes() > 0);
        store.3.advance(Duration::from_millis(30_001))?;
        store.cleanup_resources("failure").await?;
        assert_eq!(
            store
                .resource_snapshot("failure", &id)
                .await?
                .ok_or("resources")?
                .live_bytes(),
            ledger.live_bytes()
        );
        let journal = store.load("failure", &id).await?;
        let raw = serde_json::to_value(&journal)?;
        let plan = plenora_runtime_chains::contracts::parse_plan(plan_raw.as_bytes())?;
        let wrong = plenora_runtime_chains::ResourceRequest {
            tenant: "failure".into(),
            run_id: id.clone(),
            message_id: raw["active"]["message_id"].as_str().ok_or("intent")?.into(),
            step: "read".into(),
            attempt: 0,
            plan_digest: plenora_runtime_chains::contracts::plan_digest(&plan)?,
            limits: plan.budgets,
            objects: vec![],
        };
        assert!(matches!(
            store.acquire_resources(&wrong).await,
            Err(Error::Conflict)
        ));
        let resumed = h
            .execute(Request::Resume {
                recovery: plenora_runtime_chains::Recovery::Reconcile,
                run_id: id.clone(),
                revision: journal.revision(),
            })
            .await;
        assert_eq!(resumed.error, None);
        let imported = store
            .resource_snapshot("failure", &id)
            .await?
            .ok_or("imported")?;
        assert_eq!(imported.active(), 0);
        assert_eq!(
            imported.live_bytes(),
            config("failure")?.artifacts["ref:read"].len()
        );
        assert_eq!(domain_attempts(&store)?, 2); // Original invoke and read-only reconciliation.
        assert_eq!(
            h.execute(Request::Report {
                run_id: id,
                cursor: None
            })
            .await
            .value
            .as_array()
            .ok_or("reports")?
            .len(),
            2
        );
    }
    Ok(())
}

/// The owned-buffer quota is checked before the first provider invocation.
#[tokio::test]
async fn owned_budget_rejects_before_allocating_or_invoking() -> ResultTest {
    let store = Arc::new(ScriptStore::default());
    let mut config = config("owned")?;
    let mut plan: Value = serde_json::from_str(&config.authorized_plans[0])?;
    plan["budgets"]["memory_bytes"] = json!(1000);
    plan["budgets"]["spool_bytes"] = json!(1000);
    for step in plan["steps"].as_array_mut().ok_or("steps")? {
        step["budgets"]["memory_bytes"] = json!(1000);
        step["budgets"]["spool_bytes"] = json!(1000);
    }
    let plan = serde_json::to_string(&plan)?;
    config.authorized_plans = vec![plan.clone()];
    let host = ScriptedHost::with_store(
        config,
        store.clone(),
        Arc::new(ManualClock::new(UNIX_EPOCH + Duration::from_hours(498_492))),
    )?;
    let run_id = uuid::Uuid::new_v4().to_string();
    let response = host
        .execute(Request::Run {
            plan,
            options: RunOptions {
                run_id: run_id.clone(),
                idempotency_key: None,
                deadline_ms: None,
            },
        })
        .await;
    assert!(response.error.is_some());
    assert_eq!(response.value["state"], "rejected");
    assert_eq!(response.value["error"]["category"], "resource_limit");
    assert_eq!(domain_attempts(&store)?, 0);
    assert!(store.resource_snapshot("owned", &run_id).await?.is_none());
    Ok(())
}
