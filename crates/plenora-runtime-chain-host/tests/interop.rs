//! Real libraries and files; `ScriptStore` is only a declared primary/fault double.
#![allow(clippy::too_many_lines)]
#[path = "support/interop_fixture.rs"]
mod fixture;
#[path = "../../plenora-runtime-chains/tests/support/primary.rs"]
mod primary;
use plenora_runtime_chain_host::{InteropHost, InteropProfile};
use plenora_runtime_chains::{Request, RunOptions};
use plenora_runtime_testkit::ManualClock;
use serde_json::Value;
use std::{sync::Arc, time::UNIX_EPOCH};
type Test = Result<(), Box<dyn std::error::Error>>;
fn options() -> RunOptions {
    RunOptions {
        run_id: "00000000-0000-4000-8000-000000000071".into(),
        idempotency_key: None,
        deadline_ms: None,
    }
}
fn clone_profile(p: &InteropProfile) -> Result<InteropProfile, serde_json::Error> {
    serde_json::from_slice(&serde_json::to_vec(p)?)
}
fn host(
    p: InteropProfile,
    s: Arc<primary::ScriptStore>,
) -> Result<InteropHost<primary::ScriptStore>, plenora_runtime_chains::Error> {
    InteropHost::with_interop(p, s, Arc::new(ManualClock::new(UNIX_EPOCH)))
}
fn run(p: &plenora_runtime_chains::contracts::Plan) -> Result<Request, serde_json::Error> {
    Ok(Request::Run {
        plan: serde_json::to_string(p)?,
        options: options(),
    })
}
async fn document(
    h: &InteropHost<primary::ScriptStore>,
    doc: &Value,
) -> Result<Value, Box<dyn std::error::Error>> {
    let r = h
        .execute(Request::Artifact {
            reference: doc["artifact_ref"].as_str().ok_or("reference")?.into(),
        })
        .await;
    assert!(r.error.is_none());
    let bytes: Vec<u8> = serde_json::from_value(r.value)?;
    assert_eq!(fixture::digest(&bytes), doc["sha256"]);
    Ok(serde_json::from_slice(&bytes)?)
}
#[tokio::test]
async fn real_interop_ipc_preserves_independent_rows_metadata_and_reports() -> Test {
    let root = tempfile::tempdir()?;
    let (profile, plan) = fixture::fixture(root.path(), "ipc", false)?;
    let h = host(profile, Arc::new(primary::ScriptStore::default()))?;
    let result = h.execute(run(&plan)?).await;
    assert!(result.error.is_none());
    assert_eq!(result.value["state"], "succeeded");
    assert_eq!(result.value["remote_effect"], "committed");
    assert_eq!(result.value["reports_complete"], true);
    let mut reader = arrow_ipc::reader::StreamReader::try_new(
        std::fs::File::open(root.path().join("bundle/result.arrow"))?,
        None,
    )?;
    let batch = reader.next().ok_or("batch")??;
    assert!(reader.next().is_none());
    assert_eq!(batch.schema(), fixture::data::batch(None, 0)?.schema());
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or("strings")?;
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![Some(""), Some("hello"), None]
    );
    for (index, step) in result.value["steps"]
        .as_array()
        .ok_or("steps")?
        .iter()
        .enumerate()
    {
        assert_eq!(step["state"], "succeeded");
        assert_eq!(step["attempt"], 1);
        assert_eq!(step["counters"]["rows_read"], 3);
        for report in step["reports"].as_array().ok_or("reports")? {
            assert_eq!(report["availability"], "available");
            let evidence = document(&h, &report["document"]).await?;
            if index == 1 && report["kind"] == "fidelity" {
                assert_eq!(evidence["contract"], "plenora-data-localized-effects-v1");
            }
        }
    }
    assert_eq!(h.execute(run(&plan)?).await.value, result.value); // No second file publication.
    Ok(())
}
#[tokio::test]
async fn real_formats_refuse_unqualified_losses_and_stop_at_first_error() -> Test {
    for (format, geometry, failed, category) in [
        ("csv", false, 0, "crs"),
        ("csv", true, 0, "data_mapping"),
        ("xls", true, 0, "data_mapping"),
        ("geoparquet", false, 0, "data_mapping"),
        ("geoparquet", true, 1, "schema"),
    ] {
        let root = tempfile::tempdir()?;
        let (profile, plan) = fixture::fixture(root.path(), format, geometry)?;
        let h = host(profile, Arc::new(primary::ScriptStore::default()))?;
        let result = h.execute(run(&plan)?).await;
        assert!(result.error.is_some(), "{format}");
        let steps = result.value["steps"].as_array().ok_or("steps")?;
        assert_eq!(steps[failed]["state"], "failed", "{format}");
        assert_eq!(steps[failed]["error"]["category"], category, "{format}");
        for step in &steps[failed + 1..] {
            assert_eq!(step["state"], "blocked");
            assert_eq!(step["attempt"], 0);
        }
        assert!(!root.path().join("bundle").exists());
        for step in &steps[..=failed] {
            for report in step["reports"].as_array().ok_or("reports")? {
                if report["availability"] == "available" {
                    let evidence = document(&h, &report["document"]).await?;
                    if geometry && format == "geoparquet" && report["kind"] == "diagnostics" {
                        assert!(evidence.to_string().contains("metadata_invalidated"));
                    }
                }
            }
        }
    }
    Ok(())
}
#[tokio::test]
async fn whole_plan_confirmation_rejects_changed_step_destination_and_loss_grant() -> Test {
    let root = tempfile::tempdir()?;
    let (profile, plan) = fixture::fixture(root.path(), "ipc", false)?;
    let h = host(profile, Arc::new(primary::ScriptStore::default()))?;
    for change in ["destination", "parameters", "loss"] {
        let mut changed = serde_json::to_value(&plan)?;
        match change {
            "destination" => {
                changed["steps"][2]["destination_ref"] = serde_json::json!("ref:other");
            }
            "parameters" => {
                changed["steps"][1]["parameters"]["sha256"] = serde_json::json!("a".repeat(64));
            }
            _ => changed["steps"][2]["loss_consent"] = serde_json::json!(true),
        }
        let r = h
            .execute(Request::Run {
                plan: changed.to_string(),
                options: options(),
            })
            .await;
        assert!(r.error.is_some());
        assert!(!root.path().join("bundle").exists());
    }
    Ok(())
}
#[tokio::test]
async fn real_restart_after_confirmed_step_receipt_does_not_reinvoke() -> Test {
    for cut in [1, 2] {
        let root = tempfile::tempdir()?;
        let (profile, plan) = fixture::fixture(root.path(), "ipc", false)?;
        let store = Arc::new(primary::ScriptStore::default());
        let clock = Arc::new(ManualClock::new(UNIX_EPOCH));
        let engine = plenora_runtime_chain_host::interop_engine(
            clone_profile(&profile)?,
            store.clone(),
            clock.clone(),
        )?;
        let accepted = engine.submit(&plan, options()).await?;
        store.13.store(cut, std::sync::atomic::Ordering::SeqCst);
        assert!(engine.advance(&accepted.run_id).await.is_err());
        drop(engine);
        let restored = plenora_runtime_chain_host::interop_engine(profile, store, clock)?;
        let state = restored.status(&accepted.run_id).await?;
        restored.resume(&accepted.run_id, state.revision).await?;
        for _ in 0..16 {
            if restored.result(&accepted.run_id).await.is_ok() {
                break;
            }
            let _progress = restored.advance(&accepted.run_id).await;
        }
        let result = restored.result(&accepted.run_id).await?;
        assert_eq!(result.state, "succeeded");
        assert_eq!(result.counters.max_attempts, 3);
        assert!(root.path().join("bundle/result.arrow").is_file());
    }
    Ok(())
}

#[tokio::test]
async fn publication_without_durable_evidence_is_not_replayed_after_restart() -> Test {
    let root = tempfile::tempdir()?;
    let (profile, plan) = fixture::fixture(root.path(), "ipc", false)?;
    let store = Arc::new(primary::ScriptStore::default());
    *store.17.lock().map_err(|_| "fault")? = Some("write".into());
    let h = host(clone_profile(&profile)?, store.clone())?;
    let _first = h.execute(run(&plan)?).await;
    drop(h);
    let original = std::fs::read(root.path().join("bundle/result.arrow"))?;
    let restored = host(profile, store.clone())?;
    let result = restored.execute(run(&plan)?).await;
    assert_ne!(result.value["state"], "succeeded");
    assert_eq!(
        std::fs::read(root.path().join("bundle/result.arrow"))?,
        original
    );
    let status = restored
        .execute(Request::Status {
            run_id: options().run_id.clone(),
        })
        .await;
    let resume = restored
        .execute(Request::Resume {
            run_id: options().run_id,
            revision: status.value["revision"].as_u64().ok_or("revision")?,
            recovery: plenora_runtime_chains::Recovery::default(),
        })
        .await;
    assert_ne!(resume.value["state"], "succeeded");
    assert_eq!(
        std::fs::read(root.path().join("bundle/result.arrow"))?,
        original
    );
    assert_eq!(store.19.load(std::sync::atomic::Ordering::SeqCst), 1);
    Ok(())
}
#[tokio::test]
async fn cancellation_during_read_settlement_never_starts_data_or_write() -> Test {
    let root = tempfile::tempdir()?;
    let (profile, plan) = fixture::fixture(root.path(), "ipc", false)?;
    let store = Arc::new(primary::ScriptStore::default());
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *store.18.lock().map_err(|_| "pause")? =
        Some(("read".into(), (entered.clone(), release.clone())));
    let h = Arc::new(host(profile, store)?);
    let active = h.clone();
    let request = run(&plan)?;
    let task = tokio::spawn(async move { active.execute(request).await });
    tokio::time::timeout(std::time::Duration::from_secs(20), entered.notified()).await?;
    let status = h
        .execute(Request::Status {
            run_id: options().run_id.clone(),
        })
        .await;
    let cancel = h
        .execute(Request::Cancel {
            run_id: options().run_id.clone(),
            revision: status.value["revision"].as_u64().ok_or("revision")?,
        })
        .await;
    release.notify_one();
    assert!(cancel.error.is_none());
    let result = task.await?;
    assert_eq!(result.value["state"], "cancelled");
    assert_eq!(result.value["steps"][1]["attempt"], 0);
    assert_eq!(result.value["steps"][2]["attempt"], 0);
    assert!(!root.path().join("bundle").exists());
    Ok(())
}

#[tokio::test]
async fn geoparquet_identity_refusal_preserves_native_evidence() -> Test {
    let root = tempfile::tempdir()?;
    let (mut profile, mut plan) = fixture::fixture(root.path(), "geoparquet", true)?;
    fixture::identity(&mut profile, &mut plan)?;
    let h = host(profile, Arc::new(primary::ScriptStore::default()))?;
    let result = h.execute(run(&plan)?).await;
    assert_eq!(result.value["state"], "partial");
    let step = &result.value["steps"][1];
    assert_eq!(step["error"]["category"], "schema");
    assert_eq!(step["reports"][0]["availability"], "unavailable");
    let diag = step["reports"]
        .as_array()
        .ok_or("reports")?
        .iter()
        .find(|r| r["kind"] == "diagnostics")
        .ok_or("C01")?;
    let evidence = document(&h, &diag["document"]).await?;
    assert_eq!(evidence["steps"], serde_json::json!([]));
    assert!(!root.path().join("bundle").exists());
    Ok(())
}
