//! Native Arrow and authorized data.run@3 plan, generated without external fixtures.
use arrow_array::{RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use plenora_runtime_adapter_data::{ArrowPort, Config, Ports, registry};
use plenora_runtime_chains::contracts::{self, Plan};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn batch() -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let field = Field::new("value", DataType::Utf8, true)
        .with_metadata(BTreeMap::from([("plenora.field_id".into(), "1".into())]));
    let schema = Schema::new_with_metadata(
        vec![field],
        BTreeMap::from([("plenora.contract.version".into(), "1".into())]),
    );
    Ok(RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(StringArray::from(vec![
            Some("hello"),
            None,
            Some(""),
        ]))],
    )?)
}
pub fn ipc(batch: &RecordBatch) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(bytes)
}
pub fn native() -> Value {
    json!({"version":1,"inputs":["data"],"steps":[{"out":"output","op":"table.sort","in":["data"],"config":{"columns":["value"]}}],"outputs":["output"],"limits":{"max_input_rows":1000,"max_output_rows":1000,"max_rows_per_edge":1000,"max_governed_memory_bytes":16_777_216}})
}
pub fn fixture(
    native: &Value,
    bytes: Vec<u8>,
    exact: bool,
) -> Result<(Config, Plan), Box<dyn std::error::Error>> {
    let names = |key: &str| -> Result<Vec<String>, Box<dyn std::error::Error>> {
        native[key]
            .as_array()
            .ok_or("names")?
            .iter()
            .map(|v| Ok(v.as_str().ok_or("name")?.into()))
            .collect()
    };
    let inputs = names("inputs")?;
    let outputs = names("outputs")?;
    let schema =
        arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(&bytes), None)?.schema();
    let primary = match schema
        .metadata()
        .get("plenora.primary_geometry")
        .map(String::as_str)
    {
        Some("none") => false,
        Some(_) => true,
        None => {
            schema
                .fields()
                .iter()
                .filter(|f| {
                    f.metadata().get("ARROW:extension:name").map(String::as_str)
                        == Some("geoarrow.wkb")
                })
                .count()
                == 1
        }
    };
    let spec = ArrowPort {
        schema_digest: None,
        primary_geometry: primary,
    };
    let ports = Ports {
        inputs: inputs.iter().map(|n| (n.clone(), spec.clone())).collect(),
        outputs: outputs.iter().map(|n| (n.clone(), spec.clone())).collect(),
    };
    let reg = registry(&digest(b"DATA host test artifact"), &ports)?;
    let sources:BTreeMap<_,_>=inputs.iter().map(|n|(n.clone(),json!({"reference":format!("ref:input-{n}"),"content_type":"application/vnd.apache.arrow.stream","expected":{"size":bytes.len(),"sha256":digest(&bytes)}}))).collect();
    let targets:BTreeMap<_,_>=outputs.iter().map(|n|(n.clone(),json!({"reference":format!("ref:output-{n}"),"content_type":"application/vnd.apache.arrow.stream","overwrite":false}))).collect();
    let raw = serde_json::to_vec(
        &json!({"schema_version":1,"plan":native,"inputs":sources,"outputs":targets}),
    )?;
    let mut plan: Value = serde_json::from_str(include_str!(
        "../../../../contracts/chains-v2/fixtures/plan-valid.json"
    ))?;
    let budget = json!({"memory_bytes":268_435_456,"elapsed_ms":60_000,"rows_read":1000,"rows_produced":1000,"artifact_bytes":16_777_216,"spool_bytes":268_435_456,"request_bytes":16_777_216,"result_bytes":1_048_576,"max_attempts":2});
    plan["components"] = serde_json::to_value(&reg.components)?;
    plan["budgets"] = budget.clone();
    plan["requirements"]["contracts"] = json!([
        "plenora-arrow-interchange-v1",
        "plenora-arrow-primary-geometry-v1"
    ]);
    plan["requirements"]["fidelity"] = json!(if exact { "exact" } else { "reported" });
    let external:BTreeMap<_,_>=inputs.iter().map(|n|(n.clone(),json!({"artifact_ref":"ref:source","port":spec.port(),"bytes":bytes.len(),"sha256":digest(&bytes)}))).collect();
    plan["inputs"] = json!(external);
    let mut step = plan["steps"][0].clone();
    step["id"] = json!("transform");
    step["component"] = json!("data");
    step["operation"] = json!("data.run");
    step["operation_version"] = json!(3);
    step["input_contract"] = json!(reg.operations[0].input_contract);
    step["output_contract"] = json!(reg.operations[0].output_contract);
    step["parameters"] = json!({"contract":reg.operations[0].input_contract,"artifact_ref":"ref:parameters","sha256":digest(&raw)});
    let bindings:BTreeMap<_,_>=inputs.iter().map(|n|(n.clone(),json!({"source":{"kind":"input","input":n},"expected":spec.port(),"boundary":"plan-input","schema_barrier":true}))).collect();
    step["inputs"] = json!(bindings);
    step["outputs"] = json!(reg.operations[0].outputs);
    step["budgets"] = budget;
    plan["steps"] = json!([step]);
    let final_outputs:BTreeMap<_,_>=outputs.iter().map(|n|(n.clone(),json!({"source":{"kind":"step","step":"transform","port":n},"expected":spec.port(),"publication":"durable"}))).collect();
    plan["outputs"] = json!(final_outputs);
    let plan = contracts::parse_plan(&serde_json::to_vec(&plan)?)?;
    contracts::validate_plan(&plan, &reg)?;
    let config = Config {
        tenant: "tenant".into(),
        artifact_sha256: digest(b"DATA host test artifact"),
        ports,
        authorized_plans: vec![serde_json::to_string(&plan)?],
        artifacts: BTreeMap::from([("ref:source".into(), bytes), ("ref:parameters".into(), raw)]),
    };
    Ok((config, plan))
}
