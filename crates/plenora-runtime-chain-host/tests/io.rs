//! IO files and reports through real PostgreSQL journal and common dispatcher.
use plenora_runtime_chain_host::{IoConfig, IoHost};
use plenora_runtime_chains::{Request, RunOptions};
#[tokio::test]
#[ignore = "requires existing PostgreSQL CI service and Rust-generated IO fixture"]
async fn postgres_real_io_and_restart_reports() -> Result<(), Box<dyn std::error::Error>> {
    let fixture: serde_json::Value =
        serde_json::from_slice(&std::fs::read(std::env::var("PLENORA_IO_FIXTURE")?)?)?;
    let mut config = serde_json::json!({"database":std::env::var("PLENORA_POSTGRES_URL")?,"adapter":fixture["adapter"]});
    let staging = std::path::Path::new(
        fixture["adapter"]["staging_directory"]
            .as_str()
            .ok_or("fixture staging directory required")?,
    );
    if !staging.is_absolute() || !staging.is_dir() {
        return Err("fixture staging directory must exist and be absolute".into());
    }
    let root = staging.join(format!("io-ci-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root)?;
    config["adapter"]["destinations"]["ref:destination"]["directory"] =
        serde_json::json!(root.join("bundle").to_str().ok_or("path")?);
    config["adapter"]["staging_directory"] = serde_json::json!(root.to_str().ok_or("path")?);
    let raw = serde_json::to_string(&config)?;
    let host = IoHost::io(serde_json::from_str::<IoConfig>(&raw)?)?;
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
    assert_eq!(response.value["remote_effect"], "committed");
    assert_eq!(response.value["reports_complete"], true);
    assert!(root.join("bundle/data.ipc").is_file());
    drop(host);
    let restored = IoHost::io(serde_json::from_str::<IoConfig>(&raw)?)?;
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
        assert_eq!(step["reports"].as_array().ok_or("reports")?.len(), 2);
    }
    std::fs::remove_dir_all(&root)?;
    Ok(())
}
