//! Real lossless handoffs and refused lossy publications, without an IO consent extension.
#[path = "support/interop_fixture.rs"]
mod fixture;
#[path = "support/interop_oracle.rs"]
mod oracle;
#[path = "../../plenora-runtime-chains/tests/support/primary.rs"]
mod primary;
use plenora_runtime_chain_host::InteropHost;
use plenora_runtime_chains::{Request, RunOptions};
use plenora_runtime_testkit::ManualClock;
use std::{sync::Arc, time::UNIX_EPOCH};
#[tokio::test]
async fn real_outputs_require_lossless_evidence_before_publication()
-> Result<(), Box<dyn std::error::Error>> {
    for format in ["ipc", "geoparquet", "csv", "xls"] {
        let root = tempfile::tempdir()?;
        let (mut profile, mut plan) = fixture::fixture(root.path(), "ipc", true)?;
        fixture::target(&mut profile, &mut plan, format)?;
        let host = InteropHost::with_interop(
            profile,
            Arc::new(primary::ScriptStore::default()),
            Arc::new(ManualClock::new(UNIX_EPOCH)),
        )?;
        let result = host
            .execute(Request::Run {
                plan: serde_json::to_string(&plan)?,
                options: RunOptions {
                    run_id: "00000000-0000-4000-8000-000000000081".into(),
                    idempotency_key: None,
                    deadline_ms: None,
                },
            })
            .await;

        assert_eq!(result.value["steps"][0]["state"], "succeeded");
        assert_eq!(result.value["steps"][1]["state"], "succeeded");
        if matches!(format, "ipc" | "geoparquet") {
            assert_eq!(result.value["state"], "succeeded");
            assert_eq!(result.value["reports_complete"], true);
            oracle::output(root.path(), format, true)?;
        } else {
            let error = &result.value["steps"][2]["error"];
            assert_eq!(error["category"], "data_mapping");
            assert_eq!(error["phase"], "validate");
            assert_eq!(error["remote_effect"], "none");
            assert_eq!(error["retry"], serde_json::json!({"kind":"never"}));
            assert!(!root.path().join("bundle").exists());
            let report = &result.value["steps"][2]["reports"][1];
            assert_eq!(report["availability"], "available");
            let doc = host
                .execute(Request::Artifact {
                    reference: report["document"]["artifact_ref"]
                        .as_str()
                        .ok_or("reference")?
                        .into(),
                })
                .await;
            assert!(doc.error.is_none());
            let bytes: Vec<u8> = serde_json::from_value(doc.value)?;
            assert_eq!(fixture::digest(&bytes), report["document"]["sha256"]);
            let raw: serde_json::Value = serde_json::from_slice(&bytes)?;
            assert_eq!(raw["write_loss"]["lossless"], false);
        }
    }
    Ok(())
}

#[tokio::test]
async fn pg_composition_admits_only_the_real_read1_plan_without_connecting()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let (mut profile, mut plan) = fixture::fixture(root.path(), "ipc", false)?;
    fixture::postgres(
        &mut profile,
        &mut plan,
        "s4_source",
        "host=127.0.0.1 port=1 dbname=absent".into(),
    )?;
    let registry = plenora_runtime_chain_host::interop_registry(&profile)?;
    assert!(
        registry
            .operations
            .iter()
            .any(|o| o.operation == "database.read" && o.version == 1)
    );
    assert!(
        !registry
            .operations
            .iter()
            .any(|o| o.operation == "database.read" && o.version == 2)
    );
    let host = InteropHost::with_interop(
        profile,
        Arc::new(primary::ScriptStore::default()),
        Arc::new(ManualClock::new(UNIX_EPOCH)),
    )?;
    let valid = host
        .execute(Request::Validate {
            plan: serde_json::to_string(&plan)?,
        })
        .await;
    assert!(valid.error.is_none());
    let mut wrong = serde_json::to_value(&plan)?;
    wrong["steps"][0]["operation_version"] = serde_json::json!(2);
    assert!(
        host.execute(Request::Validate {
            plan: wrong.to_string()
        })
        .await
        .error
        .is_some()
    );
    Ok(())
}
