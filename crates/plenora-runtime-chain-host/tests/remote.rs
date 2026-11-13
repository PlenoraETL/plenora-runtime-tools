//! Real PostgreSQL/NATS traversal of the durable scripted host; no real domain adapters.
use plenora_runtime_chain_host::{RemoteClient, ScriptedConfig, ScriptedHost};
use plenora_runtime_chains::{Request, Transport};
use std::{error::Error, sync::Arc, time::Duration};

#[tokio::test]
#[ignore = "Requires ephemeral PostgreSQL and NATS; no fake backend replacement"]
async fn nats_real_scripted_calls_preserve_errors_and_discovery() -> Result<(), Box<dyn Error>> {
    let server = std::env::var("PLENORA_NATS_URL")?;
    let subject = format!("scripted.{}", uuid::Uuid::new_v4().simple());
    let config = ScriptedConfig {
        journal_bytes: plenora_runtime_chains::JOURNAL_BYTES,
        database: std::env::var("PLENORA_POSTGRES_URL")?,
        tenant: format!("qualification-{}", uuid::Uuid::new_v4()),
        registry: include_str!("fixtures/context.json").into(),
        outcomes: include_str!("fixtures/result-valid.json").into(),
        authorized_plans: vec![include_str!("fixtures/plan-valid.json").into()],
        recovery: None,
        attempts: vec![],
        artifacts: serde_json::from_str(include_str!("fixtures/artifacts.json"))?,
    };
    let host = Arc::new(ScriptedHost::new(serde_json::from_value(
        serde_json::to_value(&config)?,
    )?)?);
    host.migrate().await?;
    let (ready, received) = tokio::sync::oneshot::channel();
    let serving = host.clone();
    let address = server.clone();
    let destination = subject.clone();
    let task = tokio::spawn(async move { serving.serve(&address, &destination, ready).await });
    tokio::time::timeout(Duration::from_secs(10), received).await??;
    let client = RemoteClient::connect(&server, &subject).await?;
    for request in [
        Request::Capabilities,
        Request::Validate { plan: "{}".into() },
        Request::Status {
            run_id: "bad".into(),
        },
    ] {
        let bytes = serde_json::to_vec(&request)?;
        let local = host.call(&bytes).await;
        let remote = client.exchange(request).await?;
        assert_eq!(local.error, remote.error);
        assert_eq!(local.value, remote.value);
    }
    let run = || Request::Run {
        plan: include_str!("fixtures/plan-valid.json").into(),
        options: plenora_runtime_chains::RunOptions {
            run_id: uuid::Uuid::new_v4().to_string(),
            idempotency_key: Some("rt022-real".into()),
            deadline_ms: None,
        },
    };
    // Same real transport/primary, but a fresh identity cannot borrow confirmation
    // from a different complete plan, even when its contract remains valid.
    let mut unauthorized = run();
    if let Request::Run { plan, options } = &mut unauthorized {
        options.idempotency_key = None;
        let mut value: serde_json::Value = serde_json::from_str(plan)?;
        value["revision"] = serde_json::json!(2);
        *plan = serde_json::to_string(&value)?;
    }
    assert_eq!(
        client.exchange(unauthorized).await?.error,
        Some(plenora_runtime_chains::Error::Invalid)
    );
    let completed = client.exchange(run()).await?;
    assert_eq!(completed.error, None);
    assert_eq!(completed.value["state"], "succeeded");
    assert_eq!(completed.value["steps"].as_array().ok_or("steps")?.len(), 3);
    let replay = client.exchange(run()).await?;
    assert_eq!(replay.error, None);
    assert_eq!(replay.value, completed.value);
    let local_replay = host.execute(run()).await;
    assert_eq!(local_replay.value, completed.value);
    let mut changed = run();
    if let Request::Run { plan, .. } = &mut changed {
        let mut value: serde_json::Value = serde_json::from_str(plan)?;
        value["revision"] = serde_json::json!(2);
        *plan = serde_json::to_string(&value)?;
    }
    assert_eq!(
        client.exchange(changed).await?.error,
        Some(plenora_runtime_chains::Error::Conflict)
    );
    let Request::Run { plan, options } = run() else {
        return Err("run fixture".into());
    };
    let submitted = client.exchange(Request::Submit { plan, options }).await?;
    assert_eq!(submitted.error, None);
    assert_ne!(submitted.value["run_id"], completed.value["run_id"]);
    let mut other_config = config;
    other_config.tenant = format!("qualification-{}", uuid::Uuid::new_v4());
    let other = ScriptedHost::new(other_config)?;
    let independent = other.execute(run()).await;
    assert_eq!(independent.error, None);
    assert_ne!(independent.value["run_id"], completed.value["run_id"]);
    corrupted_resources(&server).await?;
    task.abort();
    assert!(task.await.is_err_and(|error| error.is_cancelled()));
    Ok(())
}

fn other_config_for_resources() -> Result<ScriptedConfig, Box<dyn Error>> {
    Ok(ScriptedConfig {
        database: "host=127.0.0.1 port=1".into(),
        journal_bytes: plenora_runtime_chains::JOURNAL_BYTES,
        tenant: "fixture".into(),
        registry: include_str!("fixtures/context.json").into(),
        authorized_plans: vec![include_str!("fixtures/plan-valid.json").into()],
        outcomes: include_str!("fixtures/result-valid.json").into(),
        recovery: None,
        attempts: vec![],
        artifacts: serde_json::from_str(include_str!("fixtures/artifacts.json"))?,
    })
}

#[path = "support/journal_real.rs"]
mod journal_real;

async fn corrupted_resources(server: &str) -> Result<(), Box<dyn Error>> {
    // Restart a trusted profile with corrupted resources: the fake write must not run.
    // The error and complete earlier reports traverse the real broker and primary.
    let mut corrupted: ScriptedConfig = other_config_for_resources()?;
    corrupted.database = std::env::var("PLENORA_POSTGRES_URL")?;
    corrupted.tenant = format!("integrity-{}", uuid::Uuid::new_v4());
    corrupted
        .artifacts
        .get_mut("ref:fidelity-write")
        .ok_or("fidelity")?
        .push(0);
    let rejected_host = Arc::new(ScriptedHost::new(corrupted)?);
    let rejected_subject = format!("scripted.integrity.{}", uuid::Uuid::new_v4().simple());
    let serving = rejected_host.clone();
    let address = server.to_owned();
    let destination = rejected_subject.clone();
    let (ready, received) = tokio::sync::oneshot::channel();
    let rejected_task =
        tokio::spawn(async move { serving.serve(&address, &destination, ready).await });
    tokio::time::timeout(Duration::from_secs(10), received).await??;
    let rejected_client = RemoteClient::connect(server, &rejected_subject).await?;
    let denied = rejected_client
        .exchange(Request::Run {
            plan: include_str!("fixtures/plan-valid.json").into(),
            options: plenora_runtime_chains::RunOptions {
                run_id: uuid::Uuid::new_v4().to_string(),
                idempotency_key: None,
                deadline_ms: None,
            },
        })
        .await?;
    assert!(denied.error.is_some());
    assert_eq!(denied.value["steps"][0]["state"], "succeeded");
    assert_eq!(denied.value["steps"][1]["state"], "succeeded");
    assert_eq!(denied.value["steps"][2]["remote_effect"], "none");
    let run_id = denied.value["run_id"].as_str().ok_or("run")?.to_owned();
    let reports = rejected_client
        .exchange(Request::Report {
            run_id,
            cursor: None,
        })
        .await?;
    assert_eq!(reports.error, None);
    assert_eq!(reports.value.as_array().ok_or("reports")?.len(), 2);
    rejected_task.abort();
    assert!(rejected_task.await.is_err_and(|error| error.is_cancelled()));
    Ok(())
}
