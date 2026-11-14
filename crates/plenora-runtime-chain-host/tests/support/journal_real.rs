//! Real primary/broker with an explicitly defective adapter, plus installed-surface handoff.
use plenora_runtime_chain_host::{RemoteClient, ScriptedConfig, ScriptedHost};
use plenora_runtime_chain_store::PostgresStore;
use plenora_runtime_chains::{
    ChainEngine, Error, ReportCursor, ReportPage, Request, RunOptions, Transport, contracts::*,
};
use std::{collections::BTreeMap, error::Error as StdError, sync::Arc, time::Duration};
#[path = "../../../plenora-runtime-chains/tests/support/large_report.rs"]
mod large_report;
#[tokio::test]
#[ignore = "Real PostgreSQL/NATS and installed wheel/binary qualification in the infrastructure CI job"]
async fn oversized_journal_report_survives_real_primary_restart_and_broker()
-> Result<(), Box<dyn StdError>> {
    let plan = include_str!("../../../../contracts/chains-v2/fixtures/plan-valid.json");
    let registry = include_str!("../../../../contracts/chains-v2/fixtures/context.json");
    let config = ScriptedConfig {
        database: std::env::var("PLENORA_POSTGRES_URL")?,
        tenant: format!("journal-{}", uuid::Uuid::new_v4()),
        journal_bytes: plenora_runtime_chains::JOURNAL_BYTES,
        registry: registry.into(),
        authorized_plans: vec![plan.into()],
        outcomes: include_str!("../../../../contracts/chains-v2/fixtures/result-valid.json").into(),
        recovery: None,
        attempts: vec![],
        artifacts: BTreeMap::new(),
    };
    let saved = serde_json::to_value(&config)?;
    let host = Arc::new(ScriptedHost::new(config)?);
    host.migrate().await?;
    let primary = Arc::new(PostgresStore::new(
        saved["database"].as_str().ok_or("database")?,
    )?);
    let engine = ChainEngine::new(
        primary,
        Arc::new(large_report::Oversized),
        parse_registry(registry.as_bytes())?,
        Arc::new(plenora_runtime_core::SystemClock),
        saved["tenant"].as_str().ok_or("tenant")?,
    )?;
    let handle = engine
        .submit(
            &parse_plan(plan.as_bytes())?,
            RunOptions {
                run_id: uuid::Uuid::new_v4().to_string(),
                idempotency_key: None,
                deadline_ms: None,
            },
        )
        .await?;
    assert_eq!(engine.advance(&handle.run_id).await, Err(Error::Recovery));
    let expected = serde_json::to_vec(&large_report::outcome()?)?;
    assert_eq!(engine.report(&handle.run_id).await?, vec![expected.clone()]);
    drop(engine);
    let server = std::env::var("PLENORA_NATS_URL")?;
    let subject = format!("journal.{}", uuid::Uuid::new_v4().simple());
    let serving = host.clone();
    let address = server.clone();
    let destination = subject.clone();
    let (ready, received) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move { serving.serve(&address, &destination, ready).await });
    tokio::time::timeout(Duration::from_secs(10), received).await??;
    let client = RemoteClient::connect(&server, &subject).await?;
    let mut cursor = Some(ReportCursor {
        index: 0,
        offset: 0,
    });
    let mut bytes = Vec::new();
    while let Some(at) = cursor {
        let reply = client
            .exchange(Request::Report {
                run_id: handle.run_id.clone(),
                cursor: Some(at),
            })
            .await?;
        assert_eq!(reply.error, None);
        let page: ReportPage = serde_json::from_value(reply.value)?;
        assert_eq!(page.cursor.offset, bytes.len());
        bytes.extend(page.bytes);
        cursor = page.next;
    }
    assert_eq!(bytes, expected);
    // The following installed Python/CLI CI step reads this exact durable run.
    // File I/O is kept outside the async runtime and never emits credentials.
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/p14c1-journal-real.json");
    let fixture = serde_json::to_vec(
        &serde_json::json!({"config":saved,"run_id":handle.run_id,"expected":expected}),
    )?;
    tokio::task::spawn_blocking(move || std::fs::write(path, fixture)).await??;
    task.abort();
    assert!(task.await.is_err_and(|error| error.is_cancelled()));
    Ok(())
}
