//! Real PostgreSQL/NATS traversal of the durable scripted host; no real domain adapters.
use plenora_runtime_chain_host::{RemoteClient, ScriptedConfig, ScriptedHost};
use plenora_runtime_chains::{Request, Transport};
use std::{collections::BTreeMap, error::Error, sync::Arc, time::Duration};

#[tokio::test]
#[ignore = "Requires ephemeral PostgreSQL and NATS; no fake backend replacement"]
async fn nats_real_scripted_calls_preserve_errors_and_discovery() -> Result<(), Box<dyn Error>> {
    let server = std::env::var("PLENORA_NATS_URL")?;
    let subject = format!("scripted.{}", uuid::Uuid::new_v4().simple());
    let config = ScriptedConfig {
        database: std::env::var("PLENORA_POSTGRES_URL")?,
        tenant: format!("qualification-{}", uuid::Uuid::new_v4()),
        registry: include_str!("../../../contracts/chains-v2/fixtures/context.json").into(),
        outcomes: include_str!("../../../contracts/chains-v2/fixtures/result-valid.json").into(),
        recovery: None,
        attempts: vec![],
        artifacts: BTreeMap::new(),
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
        plan: include_str!("../../../contracts/chains-v2/fixtures/plan-valid.json").into(),
        options: plenora_runtime_chains::RunOptions {
            run_id: uuid::Uuid::new_v4().to_string(),
            idempotency_key: Some("rt022-real".into()),
            deadline_ms: None,
        },
    };
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
    task.abort();
    assert!(task.await.is_err_and(|error| error.is_cancelled()));
    Ok(())
}
