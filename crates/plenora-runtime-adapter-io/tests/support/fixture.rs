//! Shared concrete database plan fixture; real upstream contracts and content-addressed documents.
use plenora_runtime_adapter_io::{Config, Ports, registry};
use plenora_runtime_chains::contracts::{self, Plan};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn fixture(
    root: &std::path::Path,
    bytes: Vec<u8>,
    primary: bool,
    format: &str,
) -> Result<(Config, Plan), Box<dyn std::error::Error>> {
    let ports = Ports {
        read_schema: None,
        write_schema: None,
        primary_geometry: primary,
    };
    let registry = registry(&digest(b"test host artifact"), &ports)?;
    let source = plenora_runtime_adapter_io::Source {
        entry: "source.arrow".into(),
        files: BTreeMap::from([("source.arrow".into(), bytes)]),
    };
    let source_ref = format!(
        "artifact://sha256/{}",
        digest(&serde_json::to_vec(&source)?)
    );
    let read = serde_json::to_vec(&json!({"source":source_ref}))?;
    let write = serde_json::to_vec(
        &json!({"source":"ref:chain-data","destination":"ref:destination","format":format,"output_options":if format=="ipc" {json!({"serialization":"stream"})} else {json!({})}}),
    )?;
    let mut plan: Value = serde_json::from_str(include_str!(
        "../../../../contracts/chains-v2/fixtures/plan-valid.json"
    ))?;
    let budget = json!({"memory_bytes":67_108_864,"elapsed_ms":60_000,"rows_read":1000,"rows_produced":1000,"artifact_bytes":8_388_608,"spool_bytes":67_108_864,"request_bytes":4_194_304,"result_bytes":1_048_576,"max_attempts":2});
    plan["requirements"]["contracts"] = json!([
        "plenora-arrow-interchange-v1",
        "plenora-arrow-primary-geometry-v1"
    ]);
    plan["components"] = serde_json::to_value(&registry.components)?;
    plan["budgets"] = budget.clone();
    let mut first = plan["steps"][0].clone();
    first["component"] = json!("io");
    first["operation"] = json!("io.read");
    first["operation_version"] = json!(2);
    first["input_contract"] = json!(registry.operations[0].input_contract);
    first["output_contract"] = json!(registry.operations[0].output_contract);
    first["outputs"] = serde_json::to_value(&registry.operations[0].outputs)?;
    first["parameters"] = json!({"contract":registry.operations[0].input_contract,"artifact_ref":"ref:read-parameters","sha256":digest(&read)});
    first["budgets"] = budget.clone();
    let mut second = plan["steps"][2].clone();
    second["component"] = json!("io");
    second["operation"] = json!("io.write");
    second["input_contract"] = json!(registry.operations[1].input_contract);
    second["output_contract"] = json!(registry.operations[1].output_contract);
    second["parameters"] = json!({"contract":registry.operations[1].input_contract,"artifact_ref":"ref:write-parameters","sha256":digest(&write)});
    second["inputs"] = json!({"data":{"source":{"kind":"step","step":"read","port":"data"},"expected":registry.operations[1].inputs["data"],"boundary":"io-read-write","schema_barrier":true}});
    second["budgets"] = budget;
    second.as_object_mut().ok_or("step")?.remove("loss_consent");
    plan["steps"] = json!([first, second]);
    plan["outputs"] = json!({"data":{"source":{"kind":"step","step":"read","port":"data"},"expected":registry.operations[0].outputs["data"],"publication":"durable"}});
    plan["requirements"]["fidelity"] = json!("exact");
    let plan = contracts::parse_plan(&serde_json::to_vec(&plan)?)?;
    contracts::validate_plan(&plan, &registry)?;
    let config = Config {
        tenant: "tenant".into(),
        artifact_sha256: digest(b"test host artifact"),
        ports,
        authorized_plans: vec![serde_json::to_string(&plan)?],
        staging_directory: root.to_string_lossy().into_owned(),
        sources: BTreeMap::from([(source_ref, source)]),
        destinations: BTreeMap::from([(
            "ref:destination".into(),
            plenora_runtime_adapter_io::Destination {
                directory: root.join("bundle").to_string_lossy().into_owned(),
                entry: format!("data.{format}"),
            },
        )]),
        artifacts: BTreeMap::from([
            ("ref:read-parameters".into(), read),
            ("ref:write-parameters".into(), write),
        ]),
    };
    Ok((config, plan))
}
