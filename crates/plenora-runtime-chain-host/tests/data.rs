//! DATA files and reports through real PostgreSQL journal and common dispatcher.
use plenora_runtime_chain_host::{DataConfig, DataHost};
use plenora_runtime_chains::{Request, RunOptions};
#[tokio::test]
#[ignore = "requires existing PostgreSQL CI service and Rust-generated DATA fixture"]
async fn postgres_real_data_and_restart_reports() -> Result<(), Box<dyn std::error::Error>> {
    let fixture: serde_json::Value =
        serde_json::from_slice(&std::fs::read(std::env::var("PLENORA_DATA_FIXTURE")?)?)?;
    let config = serde_json::json!({"database":std::env::var("PLENORA_POSTGRES_URL")?,"adapter":fixture["adapter"]});
    let raw = serde_json::to_string(&config)?;
    let host = DataHost::data(serde_json::from_str::<DataConfig>(&raw)?)?;
    host.migrate().await?;
    let run_id = uuid::Uuid::new_v4().to_string();
    let response = host
        .execute(Request::Run {
            plan: serde_json::to_string(&fixture["plan"])?,
            options: RunOptions {
                run_id: run_id.clone(),
                idempotency_key: None,
                deadline_ms: None,
            },
        })
        .await;
    assert!(response.error.is_none());
    assert_eq!(response.value["state"], "succeeded");
    assert_eq!(response.value["remote_effect"], "none");
    assert_eq!(response.value["reports_complete"], true);
    drop(host);
    let restored = DataHost::data(serde_json::from_str::<DataConfig>(&raw)?)?;
    assert_eq!(
        restored.execute(Request::Result { run_id }).await.value,
        response.value
    );
    for step in response.value["steps"].as_array().ok_or("steps")? {
        let doc = restored
            .execute(Request::Artifact {
                reference: step["upstream_result"]["artifact_ref"]
                    .as_str()
                    .ok_or("document")?
                    .into(),
            })
            .await;
        assert!(doc.error.is_none());
        assert_eq!(step["reports"].as_array().ok_or("reports")?.len(), 4);
        for report in step["reports"].as_array().ok_or("reports")? {
            let evidence = restored
                .execute(Request::Artifact {
                    reference: report["document"]["artifact_ref"]
                        .as_str()
                        .ok_or("report")?
                        .into(),
                })
                .await;
            assert!(evidence.error.is_none());
            if report["kind"] == "fidelity" {
                let bytes: Vec<u8> = serde_json::from_value(evidence.value)?;
                let value: serde_json::Value = serde_json::from_slice(&bytes)?;
                assert_eq!(value["contract"], "plenora-data-localized-effects-v1");
            }
        }
    }
    Ok(())
}
