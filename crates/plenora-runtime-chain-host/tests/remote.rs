//! Real PostgreSQL/NATS traversal of the durable scripted host; no real domain adapters.
use plenora_runtime_chain_host::{RemoteClient, ScriptedConfig, ScriptedHost};
use plenora_runtime_chains::{Request, Store, Transport};
use std::{error::Error, sync::Arc, time::Duration};
// The fixture permits 100s execution and 1s cleanup. Admission and terminal
// settlement each get one primary statement budget (15s); this is observation,
// not permission to extend the authorized run deadline or to replay on timeout.
const OBSERVATION_MS: u64 = 100_000 + 1_000 + 2 * 15_000;

struct Observed(RemoteClient);
impl Observed {
    async fn exchange(
        &self,
        request: Request,
    ) -> Result<plenora_runtime_chains::Response, plenora_runtime_chains::Error> {
        let encoded =
            serde_json::to_value(&request).map_err(|_| plenora_runtime_chains::Error::Invalid)?;
        let function = encoded["function"]
            .as_str()
            .ok_or(plenora_runtime_chains::Error::Invalid)?;
        let start = std::time::Instant::now();
        let response = self.0.exchange(request).await;
        eprintln!(
            "Qualification call {function}; elapsed_ms={}; result={}",
            start.elapsed().as_millis(),
            response.as_ref().map_or_else(
                |error| error.code(),
                |reply| reply.error.map_or("ok", |error| error.code())
            )
        );
        response
    }
}

#[test]
fn observation_budget_covers_authorized_fixture_execution() -> Result<(), Box<dyn Error>> {
    let matrix: serde_json::Value = serde_json::from_str(include_str!("fixtures/surfaces.json"))?;
    for profile in matrix["profiles"].as_object().ok_or("profiles")?.values() {
        for raw in profile["authorized_plans"]
            .as_array()
            .ok_or("authorized plans")?
        {
            let plan: serde_json::Value = serde_json::from_str(raw.as_str().ok_or("plan")?)?;
            let execution = plan["budgets"]["elapsed_ms"]
                .as_u64()
                .ok_or("elapsed budget")?;
            let cleanup = plan["execution"]["cleanup_ms"]
                .as_u64()
                .ok_or("cleanup budget")?;
            assert!(
                execution
                    .checked_add(cleanup)
                    .and_then(|n| n.checked_add(30_000))
                    .is_some_and(|n| n <= OBSERVATION_MS)
            );
        }
    }
    Ok(())
}

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
    let client =
        Observed(RemoteClient::connect_with_timeout(&server, &subject, OBSERVATION_MS).await?);
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
    check_owned(&config, &client, &completed.value).await?;
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
    let rejected_client = Observed(
        RemoteClient::connect_with_timeout(server, &rejected_subject, OBSERVATION_MS).await?,
    );
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

/// Real primary retains only published evidence after all providers stop.
async fn check_owned(
    config: &ScriptedConfig,
    client: &Observed,
    completed: &serde_json::Value,
) -> Result<(), Box<dyn Error>> {
    let run = completed["run_id"].as_str().ok_or("run")?;
    let reference = completed["steps"][1]["outputs"]["dataset"]["artifact_ref"]
        .as_str()
        .ok_or("artifact")?;
    let artifact = client
        .exchange(Request::Artifact {
            reference: reference.into(),
        })
        .await?;
    assert_eq!(artifact.error, None);
    assert_eq!(
        artifact.value,
        serde_json::to_value(&config.artifacts["ref:transform"])?
    );

    let primary = plenora_runtime_chain_store::PostgresStore::new(&config.database)?;
    let owned = primary
        .resource_snapshot(&config.tenant, run)
        .await?
        .ok_or("ownership")?;
    assert_eq!(owned.active(), 0);
    assert_eq!(
        owned.live_bytes(),
        config.artifacts["ref:read"].len() + config.artifacts["ref:transform"].len()
    );
    Ok(())
}

/// Installed bindings exercise every matrix cell while real broker hosts keep polling.
#[tokio::test]
#[ignore = "Requires real PostgreSQL/NATS and the installed wheel/binary from the existing CI job"]
async fn nats_real_installed_surface_matrix() -> Result<(), Box<dyn Error>> {
    let server = std::env::var("PLENORA_NATS_URL")?;
    let database = std::env::var("PLENORA_POSTGRES_URL")?;
    let binary = std::env::var("PLENORA_CHAIN_BIN")?;
    let matrix: serde_json::Value = serde_json::from_str(include_str!("fixtures/surfaces.json"))?;
    let mut endpoints = serde_json::Map::new();
    let mut tasks = Vec::new();
    for (name, profile) in matrix["profiles"].as_object().ok_or("profiles")? {
        let mut config: ScriptedConfig = serde_json::from_value(profile.clone())?;
        config.database.clone_from(&database);
        config.tenant = uuid::Uuid::new_v4().to_string();
        let host = Arc::new(ScriptedHost::new(config)?);
        host.migrate().await?;
        let subject = format!("matrix.{}", uuid::Uuid::new_v4().simple());
        endpoints.insert(
            name.clone(),
            serde_json::json!({"server":server,"subject":subject,"observation_timeout_ms":OBSERVATION_MS}),
        );
        let address = server.clone();
        let (ready, received) = tokio::sync::oneshot::channel();
        tasks.push(tokio::spawn(async move {
            host.serve(&address, &subject, ready).await
        }));
        tokio::time::timeout(Duration::from_secs(10), received).await??;
    }
    let endpoints = serde_json::to_string(&endpoints)?;
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../plenora-runtime-chain-python/tests/test_surface_matrix.py");
    let process = tokio::task::spawn_blocking(move || {
        std::process::Command::new("python")
            .arg(script)
            .arg("Matrix.test_remote_installed_every_rt_and_operation")
            .env("PLENORA_MATRIX_ENDPOINTS", endpoints)
            .env("PLENORA_POSTGRES_URL", database)
            .env("PLENORA_CHAIN_BIN", binary)
            .output()
    })
    .await;
    let mut host_failure = None;
    for task in tasks {
        task.abort();
        match task.await {
            Err(error) if error.is_cancelled() => {}
            Ok(Err(error)) => {
                host_failure.get_or_insert(error);
            }
            _ => {
                host_failure.get_or_insert(plenora_runtime_chains::Error::Storage);
            }
        }
    }
    let output = process??;
    assert!(
        output.status.success(),
        "installed matrix: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    if let Some(error) = host_failure {
        return Err(error.into());
    }
    Ok(())
}
