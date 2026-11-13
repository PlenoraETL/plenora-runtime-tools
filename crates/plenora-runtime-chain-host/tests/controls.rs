//! Installed controls over real PostgreSQL/NATS; no domain library is qualified.
use plenora_runtime_chain_host::{ScriptedConfig, ScriptedHost};
use plenora_runtime_chain_store::PostgresStore;
use plenora_runtime_chains::{Request, RunLease, RunOptions, Store};
use std::{error::Error, sync::Arc, time::Duration};

async fn hold_until_cancelled(
    primary: Arc<PostgresStore>,
    mut lease: RunLease,
) -> Result<(), plenora_runtime_chains::Error> {
    loop {
        let journal = primary
            .lookup(lease.tenant(), lease.run_id(), None)
            .await?
            .ok_or(plenora_runtime_chains::Error::NotFound)?;
        let value: serde_json::Value = serde_json::from_slice(&journal.to_json()?)
            .map_err(|_| plenora_runtime_chains::Error::Invalid)?;
        if value["cancelled"] == true {
            primary.release_work(&lease).await?;
            return Ok(());
        }
        lease = primary.renew_work(&lease, 60_000).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[ignore = "Requires real PostgreSQL/NATS and installed wheel/binary in the infrastructure job"]
async fn nats_real_installed_cancel_resume_events() -> Result<(), Box<dyn Error>> {
    let server = std::env::var("PLENORA_NATS_URL")?;
    let database = std::env::var("PLENORA_POSTGRES_URL")?;
    let binary = std::env::var("PLENORA_CHAIN_BIN")?;
    let config = ScriptedConfig {
        database: database.clone(),
        tenant: format!("controls-{}", uuid::Uuid::new_v4()),
        journal_bytes: plenora_runtime_chains::JOURNAL_BYTES,
        registry: include_str!("fixtures/context.json").into(),
        outcomes: include_str!("fixtures/result-valid.json").into(),
        authorized_plans: vec![include_str!("fixtures/plan-valid.json").into()],
        recovery: None,
        attempts: vec![],
        artifacts: serde_json::from_str(include_str!("fixtures/artifacts.json"))?,
    };
    let tenant = config.tenant.clone();
    let host = Arc::new(ScriptedHost::new(config)?);
    host.migrate().await?;
    let primary = Arc::new(PostgresStore::new(&database)?);
    let mut runs = serde_json::Map::new();
    let mut guards = Vec::new();
    for mode in ["sync", "async", "cli"] {
        let response = host
            .execute(Request::Submit {
                plan: include_str!("fixtures/plan-valid.json").into(),
                options: RunOptions {
                    run_id: uuid::Uuid::new_v4().to_string(),
                    idempotency_key: None,
                    deadline_ms: None,
                },
            })
            .await;
        assert_eq!(response.error, None);
        let lease = primary
            .claim_work(&tenant, &uuid::Uuid::new_v4().to_string(), 60_000)
            .await?
            .ok_or("lease")?;
        assert_eq!(response.value["run_id"], lease.run_id());
        runs.insert(mode.into(), response.value);
        // The declared coordinator barrier prevents a timing race with the autonomous worker.
        // It releases only after the actual installed client has persisted cancellation.
        guards.push(tokio::spawn(hold_until_cancelled(primary.clone(), lease)));
    }
    let subject = format!("controls.{}", uuid::Uuid::new_v4().simple());
    let endpoint = serde_json::json!({"server":server,"subject":subject,"runs":runs});
    let (ready, received) = tokio::sync::oneshot::channel();
    let serving = tokio::spawn(async move { host.serve(&server, &subject, ready).await });
    tokio::time::timeout(Duration::from_secs(10), received).await??;
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../plenora-runtime-chain-python/tests/test_controls_real.py");
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("python")
            .arg(script)
            .arg("Controls.test_remote_installed_cancel_resume_events")
            .env("PLENORA_CONTROL_ENDPOINT", endpoint.to_string())
            .env("PLENORA_POSTGRES_URL", database)
            .env("PLENORA_CHAIN_BIN", binary)
            .output()
    })
    .await;
    serving.abort();
    assert!(serving.await.is_err_and(|e| e.is_cancelled()));
    let output = output??;
    for guard in guards {
        if !output.status.success() {
            guard.abort();
        }
        let result = tokio::time::timeout(Duration::from_secs(10), guard).await?;
        if output.status.success() {
            result??;
        }
    }
    assert!(
        output.status.success(),
        "installed controls: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
