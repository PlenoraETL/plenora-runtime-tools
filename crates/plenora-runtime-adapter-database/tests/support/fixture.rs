//! Shared concrete database plan fixture; real upstream contracts and content-addressed documents.
use plenora_runtime_adapter_database::{Config, Ports, registry};
use plenora_runtime_chains::contracts::{self, Plan};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
#[allow(dead_code)] // This shared helper is unused by the real PostgreSQL test.
pub fn fixture(
    consent: bool,
    reported: bool,
) -> Result<(Config, Plan), Box<dyn std::error::Error>> {
    fixture_names(consent, reported, "s3_source", "s3_target")
}
pub fn fixture_names(
    consent: bool,
    reported: bool,
    source: &str,
    target: &str,
) -> Result<(Config, Plan), Box<dyn std::error::Error>> {
    let ports = Ports {
        read_schema: None,
        write_schema: None,
        primary_geometry: false,
    };
    let registry = registry(&digest(b"test host artifact"), &ports)?;
    let read_op = serde_json::to_vec(
        &json!({"source":{"schema":"public","object":source}, "projection":[],"order_by":[],"row_limit":null,"filter":null}),
    )?;
    let write_op = serde_json::to_vec(
        &json!({"target":{"schema":"public","object":target},"mode":"append","mapping_policy":"compatible","transaction_profile":"single_transaction","srid_policy":null}),
    )?;
    let read_ref = format!("artifact://sha256/{}", digest(&read_op));
    let write_ref = format!("artifact://sha256/{}", digest(&write_op));
    let read = serde_json::to_vec(
        &json!({"provider":"postgres","secret_environment":"DB","operation_path":read_ref}),
    )?;
    let write = serde_json::to_vec(
        &json!({"provider":"postgres","secret_environment":"DB","operation_path":write_ref,"accept_loss":consent}),
    )?;
    let mut plan: Value = serde_json::from_str(include_str!(
        "../../../../contracts/chains-v2/fixtures/plan-valid.json"
    ))?;
    let budget = json!({"memory_bytes":67_108_864,"elapsed_ms":60_000,"rows_read":1000,"rows_produced":1000,"artifact_bytes":8_388_608,"spool_bytes":67_108_864,"request_bytes":4_194_304,"result_bytes":1_048_576,"max_attempts":2});
    plan["requirements"]["contracts"] = json!(["plenora-arrow-interchange-v1"]);
    plan["components"] = serde_json::to_value(&registry.components)?;
    plan["budgets"] = budget.clone();
    let mut first = plan["steps"][0].clone();
    first["component"] = json!("database");
    first["operation"] = json!("database.read");
    first["operation_version"] = json!(1);
    first["input_contract"] = json!(registry.operations[0].input_contract);
    first["output_contract"] = json!(registry.operations[0].output_contract);
    first["outputs"] = serde_json::to_value(&registry.operations[0].outputs)?;
    first["parameters"] = json!({"contract":registry.operations[0].input_contract,"artifact_ref":"ref:read-parameters","sha256":digest(&read)});
    first["budgets"] = budget.clone();
    let mut second = plan["steps"][2].clone();
    second["component"] = json!("database");
    second["input_contract"] = json!(registry.operations[1].input_contract);
    second["output_contract"] = json!(registry.operations[1].output_contract);
    second["parameters"] = json!({"contract":registry.operations[1].input_contract,"artifact_ref":"ref:write-parameters","sha256":digest(&write)});
    second["inputs"] = json!({"data":{"source":{"kind":"step","step":"read","port":"data"},"expected":registry.operations[1].inputs["data"],"boundary":"database-read-write","schema_barrier":true}});
    second["budgets"] = budget;
    second["loss_consent"] = json!(consent);
    plan["steps"] = json!([first, second]);
    plan["outputs"] = json!({"data":{"source":{"kind":"step","step":"read","port":"data"},"expected":registry.operations[0].outputs["data"],"publication":"durable"}});
    plan["requirements"]["fidelity"] = json!(if reported { "reported" } else { "exact" });
    let mut plan = contracts::parse_plan(&serde_json::to_vec(&plan)?)?;
    if consent {
        let step = &plan.steps[1];
        plan.loss_policy.grants.push(contracts::Grant {
            step: step.id.clone(),
            operation: step.operation.clone(),
            operation_version: 2,
            destination_ref: "ref:destination".into(),
            input_digest: contracts::consent_input_digest(&plan, step)?,
            parameters_digest: step.parameters.sha256.clone(),
        });
    }
    contracts::validate_plan(&plan, &registry)?;
    let config = Config {
        tenant: "tenant".into(),
        artifact_sha256: digest(b"test host artifact"),
        ports,
        authorized_plans: vec![serde_json::to_string(&plan)?],
        artifacts: BTreeMap::from([
            ("ref:read-parameters".into(), read),
            ("ref:write-parameters".into(), write),
            (read_ref, read_op),
            (write_ref, write_op),
        ]),
    };
    Ok((config, plan))
}
