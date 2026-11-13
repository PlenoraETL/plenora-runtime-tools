//! Real primary and broker worker qualification; domain outcomes are explicitly scripted.
use plenora_runtime_chain_host::{RemoteClient, ScriptedConfig, ScriptedHost};
use plenora_runtime_chain_store::PostgresStore;
use plenora_runtime_chains::{Error, Request, RunOptions, Store, Transport};
use std::{collections::BTreeMap, error::Error as StdError, sync::Arc, time::Duration};

#[allow(
    clippy::too_many_lines,
    reason = "Keep the real restart and lease lifecycle in one auditable probe"
)]
#[tokio::test]
#[ignore = "Requires real ephemeral PostgreSQL and NATS; executed by the existing infrastructure CI job"]
async fn postgres_nats_real_worker_restarts_and_retries_persisted_runs()
-> Result<(), Box<dyn StdError>> {
    let database = std::env::var("PLENORA_POSTGRES_URL")?;
    let server = std::env::var("PLENORA_NATS_URL")?;
    let tenant = format!("worker-{}", uuid::Uuid::new_v4());
    let mut plan: serde_json::Value = serde_json::from_str(include_str!(
        "../../../contracts/chains-v2/fixtures/plan-valid.json"
    ))?;
    plan["steps"][1]["retry_policy"] = serde_json::json!({"max_attempts":2,"backoff_ms":20});
    let mut outcomes: serde_json::Value = serde_json::from_str(include_str!(
        "../../../contracts/chains-v2/fixtures/result-valid.json"
    ))?;
    let mut second = outcomes["steps"][1].clone();
    second["attempt"] = serde_json::json!(2);
    for output in second["outputs"]
        .as_object_mut()
        .ok_or("outputs")?
        .values_mut()
    {
        output["attempt"] = serde_json::json!(2);
    }
    let mut failure = second.clone();
    failure["attempt"] = serde_json::json!(1);
    failure["state"] = serde_json::json!("failed");
    failure["reason"] = serde_json::json!("upstream_error");
    failure["outputs"] = serde_json::json!({});
    failure["remote_effect"] = serde_json::json!("none");
    failure["retry"] = serde_json::json!({"kind":"safe"});
    failure["error"] = serde_json::json!({"contract":"plenora-chain-error-v2","schema_version":2,"category":"io","phase":"read","code":"CHAIN_UPSTREAM","remote_effect":"none","retry":{"kind":"safe"},"message":"Chain operation rejected or incomplete.","step_id":"transform","upstream_error":null,"reports":[]});
    outcomes["steps"][1] = failure;
    let config = ScriptedConfig {
        database: database.clone(),
        tenant: tenant.clone(),
        registry: include_str!("../../../contracts/chains-v2/fixtures/context.json").into(),
        outcomes: serde_json::to_string(&outcomes)?,
        authorized_plans: vec![serde_json::to_string(&plan)?],
        recovery: None,
        attempts: vec![serde_json::to_string(&second)?],
        artifacts: BTreeMap::new(),
    };
    let serialized = serde_json::to_vec(&config)?;
    let first = ScriptedHost::new(config)?;
    first.migrate().await?;
    let accepted = first
        .execute(Request::Submit {
            plan: serde_json::to_string(&plan)?,
            options: RunOptions {
                run_id: uuid::Uuid::new_v4().to_string(),
                idempotency_key: None,
                deadline_ms: None,
            },
        })
        .await;
    assert_eq!(accepted.error, None);
    let id = accepted.value["run_id"].as_str().ok_or("run")?.to_owned();
    drop(first); // No request or in-memory engine survives this restart.
    let primary = PostgresStore::new(&database)?;
    let a = primary
        .claim_work(&tenant, &uuid::Uuid::new_v4().to_string(), 30_000)
        .await?
        .ok_or("lease")?;
    assert!(
        primary
            .claim_work(&tenant, &uuid::Uuid::new_v4().to_string(), 30_000)
            .await?
            .is_none()
    );
    primary.release_work(&a).await?;
    let b = primary
        .claim_work(&tenant, &uuid::Uuid::new_v4().to_string(), 30_000)
        .await?
        .ok_or("lease")?;
    assert!(b.token() > a.token());
    assert_eq!(primary.release_work(&a).await, Err(Error::Conflict));
    primary.release_work(&b).await?;
    let host = Arc::new(ScriptedHost::new(serde_json::from_slice(&serialized)?)?);
    let subject = format!("worker.{}", uuid::Uuid::new_v4().simple());
    let (ready, received) = tokio::sync::oneshot::channel();
    let serving = host.clone();
    let address = server.clone();
    let destination = subject.clone();
    let task = tokio::spawn(async move { serving.serve(&address, &destination, ready).await });
    tokio::time::timeout(Duration::from_secs(10), received).await??;
    let client = RemoteClient::connect(&server, &subject).await?;
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let result = client
                .exchange(Request::Result { run_id: id.clone() })
                .await?;
            if result.error.is_none() {
                break Ok::<_, Error>(result);
            }
            if !matches!(result.error, Some(Error::Pending | Error::Recovery)) {
                return Err(Error::Storage);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    assert_eq!(result.value["state"], "succeeded");
    assert_eq!(result.value["steps"][1]["attempt"], 2);
    assert_eq!(result.value["counters"]["max_attempts"], 4);
    let reports = client
        .exchange(Request::Report { run_id: id.clone() })
        .await?;
    assert_eq!(reports.error, None);
    assert_eq!(reports.value.as_array().ok_or("reports")?.len(), 4);
    assert!(
        !Store::load(&primary, &tenant, &id)
            .await?
            .work_due(u64::MAX)
    );
    task.abort();
    assert!(task.await.is_err_and(|e| e.is_cancelled()));
    Ok(())
}
