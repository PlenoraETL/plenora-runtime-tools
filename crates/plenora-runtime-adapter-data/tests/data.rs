//! Actual candidate runtime binding through the chain coordinator and immutable journal.
#![allow(clippy::too_many_lines)]
#[path = "support/fixture.rs"]
mod fixture;
#[path = "../../plenora-runtime-chains/tests/support/primary.rs"]
mod primary;
use plenora_runtime_adapter_data::{Config, DataAdapter, registry};
use plenora_runtime_chains::{
    ChainEngine, RunOptions,
    contracts::{ChainResult, Plan},
};
use plenora_runtime_testkit::ManualClock;
use serde_json::{Value, json};
use std::{sync::Arc, time::UNIX_EPOCH};
type TestResult = Result<(), Box<dyn std::error::Error>>;
fn options() -> RunOptions {
    RunOptions {
        run_id: "00000000-0000-4000-8000-000000000051".into(),
        idempotency_key: None,
        deadline_ms: None,
    }
}
async fn execute(
    config: Config,
    plan: &Plan,
) -> Result<(ChainResult, ChainEngine<primary::ScriptStore>), Box<dyn std::error::Error>> {
    let reg = registry(&config.artifact_sha256, &config.ports)?;
    let store = Arc::new(primary::ScriptStore::default());
    let clock = Arc::new(ManualClock::new(UNIX_EPOCH));
    let adapter = Arc::new(DataAdapter::new(config, store.clone(), clock.clone())?);
    let engine = ChainEngine::new(store, adapter, reg, clock, "tenant")?;
    let _attempt = engine.run(plan, options()).await;
    let result = engine.result(&options().run_id).await?;
    Ok((result, engine))
}
#[tokio::test]
async fn real_sort_preserves_null_empty_and_separate_c01_documents() -> TestResult {
    let (config, plan) =
        fixture::fixture(&fixture::native(), fixture::ipc(&fixture::batch()?)?, true)?;
    let (result, engine) = execute(config, &plan).await?;
    assert_eq!(result.state, "succeeded");
    assert!(result.reports_complete);
    let step = &result.steps[0];
    assert_eq!(step.counters.rows_read, 3);
    assert_eq!(step.counters.rows_produced, 3);
    let artifact = &step.outputs["output"];
    let bytes = engine.artifact(&artifact.artifact_ref).await?;
    let mut reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    let batch = reader.next().ok_or("batch")??;
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or("strings")?;
    let values: Vec<_> = values.iter().collect();
    assert_eq!(values, vec![Some(""), Some("hello"), None]);
    let document = step
        .reports
        .iter()
        .find(|r| r.kind == "fidelity")
        .and_then(|r| r.document.as_ref())
        .ok_or("effects")?;
    assert_eq!(document.contract, "plenora-data-localized-effects-v1");
    let effects: Value = serde_json::from_slice(&engine.artifact(&document.artifact_ref).await?)?;
    assert_eq!(effects["steps"][0]["op"], "table.sort");
    assert!(
        String::from_utf8(engine.report(&options().run_id).await?.concat())?
            .contains(&document.artifact_ref)
    );
    assert_eq!(engine.run(&plan, options()).await?, result);
    Ok(())
}
#[tokio::test]
async fn real_multiple_inputs_and_outputs_are_not_collapsed() -> TestResult {
    let mut native = fixture::native();
    native["inputs"] = json!(["left", "right"]);
    native["steps"] =
        json!([{"out":"output","op":"table.concat","in":["left","right"],"config":{}}]);
    native["outputs"] = json!(["left", "output"]);
    let (config, plan) = fixture::fixture(&native, fixture::ipc(&fixture::batch()?)?, false)?;
    let (result, _) = execute(config, &plan).await?;
    assert_eq!(result.state, "succeeded");
    assert_eq!(result.steps[0].outputs.len(), 2);
    assert_eq!(result.steps[0].outputs["output"].rows, Some(6));
    Ok(())
}
#[tokio::test]
async fn real_kernel_error_is_redacted_and_missing_effects_are_unavailable() -> TestResult {
    let mut native = fixture::native();
    native["steps"][0]["config"] = json!({"columns":["SECRET_SENTINEL"]});
    let (config, plan) = fixture::fixture(&native, fixture::ipc(&fixture::batch()?)?, true)?;
    let (result, engine) = execute(config, &plan).await?;
    assert_eq!(result.state, "failed");
    assert!(!result.reports_complete);
    assert_eq!(result.remote_effect, "none");
    let step = &result.steps[0];
    assert!(step.outputs.is_empty());
    assert!(
        step.reports
            .iter()
            .any(|r| r.kind == "fidelity" && r.availability == "unavailable")
    );
    let doc = step.upstream_result.as_ref().ok_or("error")?;
    assert!(
        !String::from_utf8(engine.artifact(&doc.artifact_ref).await?)?.contains("SECRET_SENTINEL")
    );
    Ok(())
}
#[tokio::test]
async fn changed_artifact_and_noncanonical_parameters_do_not_invoke() -> TestResult {
    for duplicate in [false, true] {
        let (mut config, plan) =
            fixture::fixture(&fixture::native(), fixture::ipc(&fixture::batch()?)?, true)?;
        if duplicate {
            config.artifacts.insert(
                "ref:parameters".into(),
                br#"{"schema_version":1,"schema_version":1}"#.to_vec(),
            );
        } else {
            config.artifacts.insert("ref:source".into(), vec![0]);
        }
        let (result, _) = execute(config, &plan).await?;
        assert_ne!(result.state, "succeeded");
        assert_eq!(result.remote_effect, "none");
    }
    Ok(())
}
#[tokio::test]
async fn c01_removal_is_preserved_but_reported_is_not_a_loss_grant() -> TestResult {
    let original = fixture::batch()?;
    let mut fields: Vec<_> = original.schema().fields().iter().cloned().collect();
    fields.push(Arc::new(
        arrow_schema::Field::new("discard", arrow_schema::DataType::Utf8, true).with_metadata(
            std::collections::BTreeMap::from([("plenora.field_id".into(), "2".into())]),
        ),
    ));
    let schema = Arc::new(arrow_schema::Schema::new_with_metadata(
        fields,
        original.schema().metadata().clone(),
    ));
    let batch = arrow_array::RecordBatch::try_new(
        schema,
        vec![original.column(0).clone(), original.column(0).clone()],
    )?;
    for exact in [false, true] {
        let mut native = fixture::native();
        native["steps"][0]["op"] = json!("table.select_columns");
        let (config, plan) = fixture::fixture(&native, fixture::ipc(&batch)?, exact)?;
        let (result, engine) = execute(config, &plan).await?;
        assert_eq!(result.state, "failed");
        let doc = result.steps[0].reports[0].document.as_ref().ok_or("C01")?;
        assert!(String::from_utf8(engine.artifact(&doc.artifact_ref).await?)?.contains("removed"));
    }
    Ok(())
}

struct CancelBeforeInvoke(DataAdapter<primary::ScriptStore>);
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
async fn native_cancel_is_drained_and_deadline_is_injected() -> TestResult {
    use plenora_runtime_chains::{Adapter, Invocation, Reservation};
    let (config, plan) =
        fixture::fixture(&fixture::native(), fixture::ipc(&fixture::batch()?)?, true)?;
    let reg = registry(&config.artifact_sha256, &config.ports)?;
    let clock = Arc::new(ManualClock::new(UNIX_EPOCH));
    let store = Arc::new(primary::ScriptStore::default());
    let adapter = Arc::new(CancelBeforeInvoke(DataAdapter::new(
        config,
        store.clone(),
        clock.clone(),
    )?));
    let i = Invocation {
        tenant: "tenant".into(),
        run_id: options().run_id,
        message_id: "00000000-0000-4000-8000-000000000052".into(),
        plan_digest: plenora_runtime_chains::contracts::plan_digest(&plan)?,
        step: plan.steps[0].clone(),
        inputs: plan.inputs.clone(),
        reservation: Reservation {
            limits: plan.budgets.clone(),
            deadline_ms: 0,
        },
        started_ms: 0,
        attempt: 1,
    };
    assert_eq!(
        adapter.prepare(&i).await,
        Err(plenora_runtime_chains::Error::Timeout)
    );
    let engine = ChainEngine::new(store, adapter, reg, clock, "tenant")?;
    let result = engine.run(&plan, options()).await?;
    assert_eq!(result.state, "cancelled");
    assert!(result.steps[0].stop_confirmed);
    assert_eq!(result.remote_effect, "none");
    Ok(())
}
#[tokio::test]
async fn output_barrier_failure_keeps_native_effect_evidence() -> TestResult {
    let (mut config, mut plan) =
        fixture::fixture(&fixture::native(), fixture::ipc(&fixture::batch()?)?, true)?;
    let expected = Some("f".repeat(64));
    config
        .ports
        .outputs
        .get_mut("output")
        .ok_or("port")?
        .schema_digest = expected.clone();
    plan.steps[0]
        .outputs
        .get_mut("output")
        .ok_or("port")?
        .schema_digest = expected.clone();
    plan.outputs
        .get_mut("output")
        .ok_or("output")?
        .expected
        .schema_digest = expected;
    config.authorized_plans = vec![serde_json::to_string(&plan)?];
    let (result, engine) = execute(config, &plan).await?;
    assert_eq!(result.state, "failed");
    assert!(result.steps[0].outputs.is_empty());
    let doc = result.steps[0]
        .reports
        .iter()
        .find(|r| r.kind == "diagnostics")
        .ok_or("diagnostics")?
        .document
        .as_ref()
        .ok_or("effects preserved")?;
    assert!(!engine.artifact(&doc.artifact_ref).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn real_primary_geometry_and_unreported_none_rewrite_are_distinguished() -> TestResult {
    use arrow_array::{BinaryArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::BTreeMap;
    let wkb = [
        1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64,
    ];
    let fields: Vec<_> = ["a", "b"]
        .iter()
        .enumerate()
        .map(|(n, name)| {
            Field::new(*name, DataType::Binary, true).with_metadata(BTreeMap::from([
                ("plenora.field_id".into(), (10 + n).to_string()),
                ("ARROW:extension:name".into(), "geoarrow.wkb".into()),
            ]))
        })
        .collect();
    for primary in ["10", "none"] {
        let schema = Arc::new(Schema::new_with_metadata(
            fields.clone(),
            BTreeMap::from([
                ("plenora.contract.version".into(), "1".into()),
                ("plenora.primary_geometry".into(), primary.into()),
            ]),
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BinaryArray::from(vec![Some(wkb.as_slice()), None])),
                Arc::new(BinaryArray::from(vec![None, Some(wkb.as_slice())])),
            ],
        )?;
        let mut native = fixture::native();
        native["steps"] = json!([]);
        native["outputs"] = json!(["data"]);
        if primary == "none" {
            let pipeline = plenora_pipeline::Pipeline::from_json(&native.to_string())?;
            let validated = pipeline.validate(&[("data", schema)])?;
            assert_ne!(
                validated
                    .schema_uscita("data")
                    .ok_or("schema")?
                    .metadata()
                    .get("plenora.primary_geometry")
                    .map(String::as_str),
                Some("none")
            );
        }
        let (config, plan) = fixture::fixture(&native, fixture::ipc(&batch)?, true)?;
        let (result, engine) = execute(config, &plan).await?;
        if primary == "none" {
            assert_eq!(result.state, "failed");
            assert!(!result.reports_complete);
            assert_eq!(
                result.steps[0].counters.rows_produced,
                u64::try_from(batch.num_rows())?
            );
            let report = result.steps[0]
                .reports
                .iter()
                .find(|r| r.kind == "diagnostics")
                .and_then(|r| r.document.as_ref())
                .ok_or("C01 retained")?;
            let c01: Value = serde_json::from_slice(&engine.artifact(&report.artifact_ref).await?)?;
            assert_eq!(c01["steps"], json!([]));
        } else {
            assert_eq!(result.state, "succeeded");
            assert!(result.reports_complete);
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_division_by_zero_is_not_hidden_by_empty_c01() -> TestResult {
    use arrow_array::{Float64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::BTreeMap;
    let field = Field::new("value", DataType::Float64, false)
        .with_metadata(BTreeMap::from([("plenora.field_id".into(), "1".into())]));
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            field,
            Field::new("denominator", DataType::Float64, false)
                .with_metadata(BTreeMap::from([("plenora.field_id".into(), "2".into())])),
        ],
        BTreeMap::from([("plenora.contract.version".into(), "1".into())]),
    ));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
            Arc::new(Float64Array::from(vec![0.0, 0.0])),
        ],
    )?;
    let mut native = fixture::native();
    native["steps"][0]["op"] = json!("table.formula");
    native["steps"][0]["config"] =
        json!({"new_column":"result","formula":"value / denominator","on_division_by_zero":"null"});
    let (config, plan) = fixture::fixture(&native, fixture::ipc(&batch)?, false)?;
    let (result, engine) = execute(config, &plan).await?;
    assert_eq!(result.state, "failed");
    assert_eq!(result.remote_effect, "none");
    let doc = result.steps[0].upstream_result.as_ref().ok_or("manifest")?;
    let manifest: Value = serde_json::from_slice(&engine.artifact(&doc.artifact_ref).await?)?;
    assert_eq!(
        manifest["steps"][0]["division_by_zero_rows"],
        batch.num_rows()
    );
    assert!(result.steps[0].outputs.is_empty());
    Ok(())
}

#[test]
fn discovery_uses_real_runtime_version_and_artifact_ports() -> TestResult {
    let caps: Value = serde_json::from_slice(&plenora_runtime_adapter_data::discovery()?)?;
    let ops = caps["operations"].as_array().ok_or("operations")?;
    assert!(
        ops.iter()
            .any(|op| op["id"] == "data.run" && op["version"] == 3)
    );
    assert!(
        !ops.iter()
            .any(|op| op["id"] == "data.run" && op["version"] == 2)
    );
    let catalog = plenora_runtime_adapter_data::catalog();
    assert_eq!(catalog["kernels"].as_array().ok_or("kernels")?.len(), 146);
    let (mut config, _) =
        fixture::fixture(&fixture::native(), fixture::ipc(&fixture::batch()?)?, true)?;
    let reg = registry(&config.artifact_sha256, &config.ports)?;
    assert_eq!(
        reg.components["data"].catalog.contract,
        "plenora-data-catalog-result-v2"
    );
    config.ports.outputs.clear();
    assert!(registry(&config.artifact_sha256, &config.ports).is_err());
    Ok(())
}
