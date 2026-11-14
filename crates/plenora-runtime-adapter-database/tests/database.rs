//! Real Rust library/IPC/settlement through the durable coordinator; provider doubles are explicit.
#![allow(clippy::too_many_lines)]
#[path = "support/fixture.rs"]
mod fixture;
#[path = "../../plenora-runtime-chains/tests/support/primary.rs"]
mod primary;
#[path = "support/provider.rs"]
mod provider;
use fixture::fixture;
use plenora_database_core::{
    arrow::{DataType, Field, RecordBatch, array::StringArray},
    outcome::WriteStatus,
    provider::Provider,
};
use plenora_runtime_adapter_database::{
    Config, Connection, DatabaseAdapter, SecretString, registry,
};
use plenora_runtime_chains::{ChainEngine, Error, RunOptions};
use plenora_runtime_testkit::ManualClock;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::Ordering},
    time::UNIX_EPOCH,
};
type TestResult = Result<(), Box<dyn std::error::Error>>;
const RUN: &str = "00000000-0000-4000-8000-000000000041";
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn batch() -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let schema = plenora_database_core::protocol::contract_schema(vec![Field::new(
        "value",
        DataType::Utf8,
        true,
    )])?;
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            None,
            Some(""),
            Some("hello"),
        ]))],
    )?)
}

fn engine(
    config: Config,
    store: Arc<primary::ScriptStore>,
    provider: Arc<dyn Provider>,
) -> Result<ChainEngine<primary::ScriptStore>, Error> {
    let registry = registry(&config.artifact_sha256, &config.ports)?;
    let clock = Arc::new(ManualClock::new(UNIX_EPOCH));
    let adapter = Arc::new(DatabaseAdapter::new(
        config,
        BTreeMap::from([(
            "DB".into(),
            Connection {
                destination: "ref:destination".into(),
                provider,
                secret: SecretString::new("SECRET_SENTINEL"),
            },
        )]),
        store.clone(),
        clock.clone(),
    )?);
    ChainEngine::new(store, adapter, registry, clock, "tenant")
}
fn options() -> RunOptions {
    RunOptions {
        run_id: RUN.into(),
        idempotency_key: Some("single-run".into()),
        deadline_ms: None,
    }
}

#[tokio::test]
async fn real_ipc_and_library_settlement_preserve_null_empty_schema_and_reports() -> TestResult {
    let (config, plan) = fixture(false, false)?;
    let source = batch()?;
    let provider = Arc::new(provider::TestProvider::new(
        source.clone(),
        WriteStatus::Committed,
        false,
    ));
    let store = Arc::new(primary::ScriptStore::default());
    let engine = engine(config, store.clone(), provider.clone())?;
    let result = engine.run(&plan, options()).await?;
    assert_eq!(result.state, "succeeded");
    assert_eq!(result.remote_effect, "committed");
    assert!(result.reports_complete);
    assert_eq!(
        provider.received.lock().map_err(|_| "lock")?.as_slice(),
        &[source]
    );
    for step in &result.steps {
        let document = step.upstream_result.as_ref().ok_or("document")?;
        let bytes = engine.artifact(&document.artifact_ref).await?;
        assert_eq!(digest(&bytes), document.sha256);
        let raw: Value = serde_json::from_slice(&bytes)?;
        if step.operation == "database.read" {
            assert_eq!(raw["read_loss"]["status"], "complete");
        } else {
            assert_eq!(raw["schema_version"], 3);
            assert!(raw["loss_report"].is_object());
        }
    }
    let replay = engine.run(&plan, options()).await?;
    assert_eq!(result, replay);
    assert_eq!(provider.writes.load(Ordering::SeqCst), 1);
    assert_eq!(provider.reads.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn real_consent_gate_and_settlement_keep_all_effect_states() -> TestResult {
    for (consent, reported, loss, status, expected, called) in [
        (false, false, true, WriteStatus::Committed, "partial", 0),
        (true, false, true, WriteStatus::Committed, "partial", 0),
        (true, true, true, WriteStatus::Committed, "succeeded", 1),
        (false, false, false, WriteStatus::RolledBack, "partial", 1),
        (
            false,
            false,
            false,
            WriteStatus::PartiallyCommitted,
            "partial",
            1,
        ),
        (
            false,
            false,
            false,
            WriteStatus::OutcomeUnknown,
            "requires_recovery",
            1,
        ),
    ] {
        let (config, plan) = fixture(consent, reported)?;
        let provider = Arc::new(provider::TestProvider::new(batch()?, status, loss));
        let store = Arc::new(primary::ScriptStore::default());
        let engine = engine(config, store, provider.clone())?;
        let _execution = engine.run(&plan, options()).await;
        let result = engine.result(RUN).await?;
        assert_eq!(
            result.state, expected,
            "consent={consent} reported={reported} status={status:?}"
        );
        assert_eq!(provider.writes.load(Ordering::SeqCst), called);
        let step = &result.steps[1];
        let doc = step.upstream_result.as_ref().ok_or("upstream")?;
        let raw: Value = serde_json::from_slice(&engine.artifact(&doc.artifact_ref).await?)?;
        if status == WriteStatus::OutcomeUnknown {
            assert_eq!(raw["details"]["write_outcome"]["status"], "outcome_unknown");
            assert_eq!(step.remote_effect, "unknown");
        }
        assert!(!serde_json::to_string(&result)?.contains("SECRET_SENTINEL"));
    }
    Ok(())
}

#[tokio::test]
async fn changed_transitive_document_is_rejected_before_provider() -> TestResult {
    let (mut config, plan) = fixture(false, false)?;
    let key = config
        .artifacts
        .keys()
        .find(|key| key.starts_with("artifact://"))
        .ok_or("document")?
        .clone();
    config.artifacts.get_mut(&key).ok_or("document")?.push(b' ');
    let provider = Arc::new(provider::TestProvider::new(
        batch()?,
        WriteStatus::Committed,
        false,
    ));
    let engine = engine(
        config,
        Arc::new(primary::ScriptStore::default()),
        provider.clone(),
    )?;
    let _result = engine.run(&plan, options()).await;
    assert_eq!(provider.writes.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn cancellation_from_second_host_reaches_running_provider() -> TestResult {
    let (config, plan) = fixture(false, false)?;
    let second = serde_json::from_slice(&serde_json::to_vec(&config)?)?;
    let mut provider = provider::TestProvider::new(batch()?, WriteStatus::Committed, false);
    provider.wait_read = true;
    let provider = Arc::new(provider);
    let store = Arc::new(primary::ScriptStore::default());
    let worker = engine(config, store.clone(), provider.clone())?;
    let controller = engine(second, store.clone(), provider.clone())?;
    let running = tokio::spawn(async move { worker.run(&plan, options()).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        provider.entered.notified(),
    )
    .await?;
    let revision = plenora_runtime_chains::Store::load(store.as_ref(), "tenant", RUN)
        .await?
        .revision();
    controller.cancel(RUN, revision).await?;
    let _result = tokio::time::timeout(std::time::Duration::from_secs(10), running).await??;
    let result = controller.result(RUN).await?;
    assert_eq!(result.state, "cancelled");
    assert_eq!(provider.reads.load(Ordering::SeqCst), 1);
    assert_eq!(provider.writes.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn read_error_retains_its_terminal_report_and_consumed_rows() -> TestResult {
    let (config, plan) = fixture(false, false)?;
    let mut provider = provider::TestProvider::new(batch()?, WriteStatus::Committed, false);
    provider.fail_after_batch = true;
    let provider = Arc::new(provider);
    let engine = engine(
        config,
        Arc::new(primary::ScriptStore::default()),
        provider.clone(),
    )?;
    let _execution = engine.run(&plan, options()).await;
    let result = engine.result(RUN).await?;
    let step = &result.steps[0];
    assert_eq!(
        step.counters.rows_read,
        u64::try_from(provider.batch.num_rows())?
    );
    assert_eq!(step.reports[0].availability, "available");
    let doc = step.upstream_result.as_ref().ok_or("document")?;
    let bytes = engine.artifact(&doc.artifact_ref).await?;
    assert!(!String::from_utf8_lossy(&bytes).contains("SECRET_SENTINEL"));
    let raw: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(raw["details"]["read_loss"]["status"], "failed");
    assert_eq!(provider.writes.load(Ordering::SeqCst), 0);
    Ok(())
}
