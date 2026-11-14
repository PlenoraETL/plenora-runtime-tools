//! Real IO candidate operations and files, through the durable chain coordinator.
#[path = "support/fixture.rs"]
mod fixture;
#[path = "../../plenora-runtime-chains/tests/support/primary.rs"]
mod primary;
use plenora_runtime_adapter_io::{IoAdapter, registry};
use plenora_runtime_chains::{ChainEngine, RunOptions};
use plenora_runtime_testkit::ManualClock;
#[path = "support/data.rs"]
mod data;
use data::{batch, ipc};
use std::{io::Cursor, sync::Arc, time::UNIX_EPOCH};
type TestResult = Result<(), Box<dyn std::error::Error>>;
fn options() -> RunOptions {
    RunOptions {
        run_id: "00000000-0000-4000-8000-000000000041".into(),
        idempotency_key: Some("single-run".into()),
        deadline_ms: None,
    }
}
#[tokio::test]
async fn real_roundtrip_primary_geometry_nulls_and_reports() -> TestResult {
    for (key, count, primary) in [
        (Some("91"), 2, true),
        (Some("17"), 2, true),
        (None, 2, false),
        (None, 1, true),
        (Some("none"), 1, false),
        (None, 0, false),
    ] {
        let root = tempfile::tempdir()?;
        let original = batch(key, count)?;
        let (config, plan) = fixture::fixture(root.path(), ipc(&original)?, primary, "ipc")?;
        let registry = registry(&config.artifact_sha256, &config.ports)?;
        let store = Arc::new(primary::ScriptStore::default());
        let clock = Arc::new(ManualClock::new(UNIX_EPOCH));
        let adapter = Arc::new(IoAdapter::new(config, store.clone(), clock.clone())?);
        let engine = ChainEngine::new(store, adapter, registry, clock, "tenant")?;
        let result = engine.run(&plan, options()).await?;
        assert_eq!(result.state, "succeeded", "{result:?}");
        assert_eq!(result.remote_effect, "committed");
        assert!(result.reports_complete);
        let bytes = std::fs::read(root.path().join("bundle/data.ipc"))?;
        let mut reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes), None)?;
        assert_eq!(reader.schema(), original.schema());
        assert_eq!(reader.next().ok_or("batch")??, original);
        assert!(reader.next().is_none());
        for step in &result.steps {
            let document = step.upstream_result.as_ref().ok_or("document")?;
            let raw: serde_json::Value =
                serde_json::from_slice(&engine.artifact(&document.artifact_ref).await?)?;
            assert!(
                raw[if step.operation == "io.read" {
                    "loss"
                } else {
                    "write_loss"
                }]
                .is_object()
            );
            assert_eq!(step.reports.len(), 2);
            assert!(step.reports.iter().all(|r| r.availability == "available"));
        }
        assert_eq!(engine.run(&plan, options()).await?, result);
    }
    Ok(())
}

async fn run_case(
    config: plenora_runtime_adapter_io::Config,
    plan: &plenora_runtime_chains::contracts::Plan,
) -> Result<
    (
        plenora_runtime_chains::contracts::ChainResult,
        ChainEngine<primary::ScriptStore>,
    ),
    Box<dyn std::error::Error>,
> {
    let registry = registry(&config.artifact_sha256, &config.ports)?;
    let store = Arc::new(primary::ScriptStore::default());
    let clock = Arc::new(ManualClock::new(UNIX_EPOCH));
    let adapter = Arc::new(IoAdapter::new(config, store.clone(), clock.clone())?);
    let engine = ChainEngine::new(store, adapter, registry, clock, "tenant")?;
    let result = engine.run(plan, options()).await?;
    Ok((result, engine))
}
#[tokio::test]
async fn real_conflict_keeps_original_bundle_and_input_counter() -> TestResult {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("bundle"))?;
    std::fs::write(root.path().join("bundle/sentinel"), b"untouched")?;
    let input = batch(None, 0)?;
    let expected_rows = u64::try_from(input.num_rows())?;
    let (config, plan) = fixture::fixture(root.path(), ipc(&input)?, false, "ipc")?;
    let (result, engine) = run_case(config, &plan).await?;
    assert_ne!(result.state, "succeeded");
    assert_ne!(result.remote_effect, "committed");
    assert_eq!(result.steps[1].counters.rows_read, expected_rows);
    assert_eq!(
        std::fs::read(root.path().join("bundle/sentinel"))?,
        b"untouched"
    );
    assert!(!root.path().join("bundle/data.ipc").exists());
    assert!(!engine.report(&options().run_id).await?.is_empty());
    Ok(())
}
#[tokio::test]
async fn real_invalid_primary_and_malformed_arrow_fail_without_publication() -> TestResult {
    for bytes in [ipc(&batch(Some("2"), 2)?)?, b"not Arrow".to_vec()] {
        let root = tempfile::tempdir()?;
        let (config, plan) = fixture::fixture(root.path(), bytes, true, "ipc")?;
        let (result, _) = run_case(config, &plan).await?;
        assert_ne!(result.state, "succeeded");
        assert_eq!(result.remote_effect, "none");
        assert!(!root.path().join("bundle").exists());
        assert!(!result.reports_complete);
        assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    }
    Ok(())
}
#[tokio::test]
async fn real_loss_is_reported_and_never_granted_by_reported_policy() -> TestResult {
    for policy in ["exact", "reported"] {
        let root = tempfile::tempdir()?;
        let (mut config, mut plan) =
            fixture::fixture(root.path(), ipc(&batch(None, 0)?)?, false, "csv")?;
        plan.requirements.fidelity = policy.into();
        config.authorized_plans = vec![serde_json::to_string(&plan)?];
        let (result, engine) = run_case(config, &plan).await?;
        assert_ne!(result.state, "succeeded");
        assert_eq!(result.remote_effect, "none");
        assert!(!root.path().join("bundle").exists());
        let step = &result.steps[1];
        assert_eq!(step.error.as_ref().ok_or("error")?.code, "CHAIN_BOUNDARY");
        let doc = step.upstream_result.as_ref().ok_or("evidence")?;
        let raw: serde_json::Value =
            serde_json::from_slice(&engine.artifact(&doc.artifact_ref).await?)?;
        assert_eq!(raw["write_loss"]["lossless"], false);
        assert!(result.reports_complete);
        assert!(
            engine
                .report(&options().run_id)
                .await?
                .iter()
                .any(|b| String::from_utf8_lossy(b).contains(&doc.artifact_ref))
        );
    }
    Ok(())
}
#[test]
fn windows_aliases_and_case_collisions_are_rejected() -> TestResult {
    use plenora_runtime_adapter_io::Source;
    use sha2::{Digest, Sha256};
    use std::collections::BTreeMap;
    for files in [
        vec!["NUL"],
        vec!["a."],
        vec!["COM1.txt"],
        vec!["a/A", "A/b"],
        vec!["a", "a/b"],
        vec!["../a"],
    ] {
        let root = tempfile::tempdir()?;
        let (mut config, _) = fixture::fixture(root.path(), ipc(&batch(None, 0)?)?, false, "ipc")?;
        let source = Source {
            entry: files[0].into(),
            files: files.into_iter().map(|s| (s.into(), vec![1])).collect(),
        };
        let reference = format!(
            "artifact://sha256/{:x}",
            Sha256::digest(serde_json::to_vec(&source)?)
        );
        config.sources = BTreeMap::from([(reference, source)]);
        assert!(
            IoAdapter::new(
                config,
                Arc::new(primary::ScriptStore::default()),
                Arc::new(ManualClock::new(UNIX_EPOCH))
            )
            .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn upstream_primary_rewrite_and_unverifiable_ipc_are_explicitly_rejected() -> TestResult {
    let original = batch(Some("none"), 2)?;
    let stream = ipc(&original)?;
    let mut file = Vec::new();
    let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut file, &original.schema())?;
    writer.write(&original)?;
    writer.finish()?;
    drop(writer);
    let mut no_eos = stream.clone();
    no_eos.truncate(no_eos.len() - 8);
    for bytes in [stream, file, no_eos] {
        let root = tempfile::tempdir()?;
        let (config, plan) = fixture::fixture(root.path(), bytes, false, "ipc")?;
        let (result, _) = run_case(config, &plan).await?;
        assert_ne!(result.state, "succeeded");
        assert!(!result.reports_complete);
        assert!(!root.path().join("bundle").exists());
    }
    Ok(())
}
fn invocation(
    plan: &plenora_runtime_chains::contracts::Plan,
    index: usize,
) -> Result<plenora_runtime_chains::Invocation, Box<dyn std::error::Error>> {
    Ok(plenora_runtime_chains::Invocation {
        tenant: "tenant".into(),
        run_id: options().run_id,
        message_id: "00000000-0000-4000-8000-000000000042".into(),
        plan_digest: plenora_runtime_chains::contracts::plan_digest(plan)?,
        step: plan.steps[index].clone(),
        inputs: std::collections::BTreeMap::default(),
        reservation: plenora_runtime_chains::Reservation {
            limits: plan.budgets.clone(),
            deadline_ms: 60_000,
        },
        started_ms: 0,
        attempt: 1,
    })
}
struct CancelBeforeInvoke(IoAdapter<primary::ScriptStore>);
#[async_trait::async_trait]
impl plenora_runtime_chains::Adapter for CancelBeforeInvoke {
    async fn authorize(
        &self,
        t: &str,
        p: &plenora_runtime_chains::contracts::Plan,
    ) -> Result<(), plenora_runtime_chains::Error> {
        self.0.authorize(t, p).await
    }
    async fn artifact(
        &self,
        t: &str,
        r: &str,
        n: usize,
    ) -> Result<Vec<u8>, plenora_runtime_chains::Error> {
        self.0.artifact(t, r, n).await
    }
    async fn prepare(
        &self,
        i: &plenora_runtime_chains::Invocation,
    ) -> Result<(), plenora_runtime_chains::Error> {
        self.0.prepare(i).await
    }
    async fn invoke(
        &self,
        i: &plenora_runtime_chains::Invocation,
    ) -> Result<plenora_runtime_chains::contracts::StepResult, plenora_runtime_chains::Error> {
        self.0.request_cancel(i).await?;
        self.0.invoke(i).await
    }
    async fn request_cancel(
        &self,
        i: &plenora_runtime_chains::Invocation,
    ) -> Result<(), plenora_runtime_chains::Error> {
        self.0.request_cancel(i).await
    }
    async fn recover(
        &self,
        i: &plenora_runtime_chains::Invocation,
    ) -> Result<Option<plenora_runtime_chains::contracts::StepResult>, plenora_runtime_chains::Error>
    {
        self.0.recover(i).await
    }
}
#[tokio::test]
async fn native_cancellation_is_drained_and_deadline_uses_injected_clock() -> TestResult {
    use plenora_runtime_chains::Adapter;
    let root = tempfile::tempdir()?;
    let (config, plan) = fixture::fixture(root.path(), ipc(&batch(Some("91"), 2)?)?, true, "ipc")?;
    let registry = registry(&config.artifact_sha256, &config.ports)?;
    let store = Arc::new(primary::ScriptStore::default());
    let clock = Arc::new(ManualClock::new(UNIX_EPOCH));
    let adapter = Arc::new(CancelBeforeInvoke(IoAdapter::new(
        config,
        store.clone(),
        clock.clone(),
    )?));
    let mut i = invocation(&plan, 0)?;
    i.reservation.deadline_ms = 0;
    assert_eq!(
        adapter.prepare(&i).await,
        Err(plenora_runtime_chains::Error::Timeout)
    );
    let engine = ChainEngine::new(store, adapter, registry, clock, "tenant")?;
    let result = engine.run(&plan, options()).await?;
    assert_eq!(result.state, "cancelled");
    assert!(result.steps[0].stop_confirmed);
    assert_eq!(result.remote_effect, "none");
    assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    Ok(())
}
#[tokio::test]
async fn write_primary_rewrite_is_rejected_before_external_effect() -> TestResult {
    use plenora_runtime_chains::contracts::{Input, Source, Source0};
    use sha2::{Digest, Sha256};
    let root = tempfile::tempdir()?;
    let bytes = ipc(&batch(Some("none"), 2)?)?;
    let (mut config, mut plan) = fixture::fixture(root.path(), bytes.clone(), false, "ipc")?;
    let port = plan.steps[1].inputs["data"].expected.clone();
    let digest = format!("{:x}", Sha256::digest(&bytes));
    let input = Input {
        artifact_ref: "ref:external".into(),
        port: port.clone(),
        bytes: bytes.len() as u64,
        sha256: digest,
    };
    plan.inputs.insert("external".into(), input.clone());
    plan.steps[1]
        .inputs
        .get_mut("data")
        .ok_or("binding")?
        .source = Source::Variant0(Source0 {
        kind: "input".into(),
        input: "external".into(),
    });
    plan.steps[1]
        .inputs
        .get_mut("data")
        .ok_or("binding")?
        .boundary = "plan-input".into();
    plan.steps.remove(0);
    plan.outputs.clear();
    config.artifacts.insert("ref:external".into(), bytes);
    config.authorized_plans = vec![serde_json::to_string(&plan)?];
    let (chain, _) = run_case(config, &plan).await?;
    let result = &chain.steps[0];
    assert_eq!(result.state, "failed");
    assert_eq!(result.error.as_ref().ok_or("error")?.category, "schema");
    assert_eq!(result.remote_effect, "none");
    assert!(!root.path().join("bundle").exists());
    Ok(())
}

#[tokio::test]
async fn real_discovery_limits_and_closed_parameters_reject_without_effect() -> TestResult {
    use plenora_runtime_chains::Adapter;
    use sha2::{Digest, Sha256};
    let discovery: serde_json::Value =
        serde_json::from_slice(&plenora_runtime_adapter_io::discovery()?)?;
    for name in ["io.read", "io.write"] {
        assert!(
            discovery["operations"]
                .as_array()
                .ok_or("operations")?
                .iter()
                .any(|o| o["id"] == name && o["version"] == 2 && o["status"] == "available")
        );
    }
    assert!(
        plenora_runtime_adapter_io::catalog()["drivers"]
            .as_array()
            .ok_or("drivers")?
            .iter()
            .any(|d| d["id"] == "filegdb" && d["available"] == false)
    );
    for patch in [
        serde_json::json!({"limit":1}),
        serde_json::json!({"deadline_ms":1}),
        serde_json::json!({"unknown":true}),
        serde_json::json!({"layer":null}),
        serde_json::json!({"budgets":{"memory_bytes":999_999_999}}),
    ] {
        let root = tempfile::tempdir()?;
        let (mut config, mut plan) =
            fixture::fixture(root.path(), ipc(&batch(None, 0)?)?, false, "ipc")?;
        let bytes = config
            .artifacts
            .get_mut("ref:read-parameters")
            .ok_or("params")?;
        let mut value: serde_json::Value = serde_json::from_slice(bytes)?;
        value
            .as_object_mut()
            .ok_or("object")?
            .extend(patch.as_object().ok_or("patch")?.clone());
        *bytes = serde_json::to_vec(&value)?;
        plan.steps[0].parameters.sha256 = format!("{:x}", Sha256::digest(bytes));
        config.authorized_plans = vec![serde_json::to_string(&plan)?];
        let adapter = IoAdapter::new(
            config,
            Arc::new(primary::ScriptStore::default()),
            Arc::new(ManualClock::new(UNIX_EPOCH)),
        )?;
        assert!(adapter.prepare(&invocation(&plan, 0)?).await.is_err());
        assert_eq!(std::fs::read_dir(root.path())?.count(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn real_durable_publication_never_claims_unconfirmed_success() -> TestResult {
    use sha2::{Digest, Sha256};
    let root = tempfile::tempdir()?;
    let (mut config, mut plan) =
        fixture::fixture(root.path(), ipc(&batch(Some("91"), 2)?)?, true, "ipc")?;
    let bytes = config
        .artifacts
        .get_mut("ref:write-parameters")
        .ok_or("params")?;
    let mut value: serde_json::Value = serde_json::from_slice(bytes)?;
    value["durable"] = serde_json::json!(true);
    *bytes = serde_json::to_vec(&value)?;
    plan.steps[1].parameters.sha256 = format!("{:x}", Sha256::digest(bytes));
    config.authorized_plans = vec![serde_json::to_string(&plan)?];
    let (result, engine) = run_case(config, &plan).await?;
    assert!(root.path().join("bundle/data.ipc").is_file());
    if cfg!(unix) {
        assert_eq!(result.state, "succeeded");
        assert_eq!(result.remote_effect, "committed");
    } else {
        assert_eq!(result.state, "requires_recovery");
        assert_eq!(result.remote_effect, "unknown");
        assert_eq!(
            result.steps[1].error.as_ref().ok_or("error")?.code,
            "CHAIN_RECOVERY"
        );
    }
    let document = result.steps[1].upstream_result.as_ref().ok_or("document")?;
    let original: serde_json::Value =
        serde_json::from_slice(&engine.artifact(&document.artifact_ref).await?)?;
    assert_eq!(original["publish_outcome"], "published");
    Ok(())
}
