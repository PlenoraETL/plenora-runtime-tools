//! Generated native files, immutable whole plan and independent row oracle.
use plenora_runtime_adapter_data::{ArrowPort, Ports};
use plenora_runtime_adapter_io::{Destination, Source};
use plenora_runtime_chain_host::{InteropIo, InteropProfile, interop_registry};
use plenora_runtime_chains::contracts::{self, Plan};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path};
#[path = "../../../plenora-runtime-adapter-io/tests/support/data.rs"]
pub mod data;
pub fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .flat_map(|byte| [byte >> 4, byte & 0x0f])
        .map(|nibble| char::from(b"0123456789abcdef"[usize::from(nibble)]))
        .collect::<String>()
}
#[allow(
    clippy::too_many_lines,
    reason = "Keep the generated whole-plan oracle together"
)]
pub fn fixture(
    root: &Path,
    format: &str,
    geometry: bool,
) -> Result<(InteropProfile, Plan), Box<dyn std::error::Error>> {
    let extension = match format {
        "ipc" => "arrow",
        "xls" => "xlsx",
        "geoparquet" => "parquet",
        v => v,
    };
    let original = data::batch(
        if geometry && format != "ipc" {
            Some("17")
        } else {
            None
        },
        usize::from(geometry),
    )?;
    let ipc = data::ipc(&original)?;
    let source = if format == "ipc" {
        ipc
    } else if format == "geoparquet" && !geometry {
        let mut bytes = Vec::new();
        let mut writer =
            s4_parquet::arrow::ArrowWriter::try_new(&mut bytes, original.schema(), None)?;
        writer.write(&original)?;
        writer.close()?;
        bytes
    } else if format == "csv" {
        // Literal external input, not the output of the implementation under test.
        if geometry {
            b"geometry,value\nPOINT (1 2),hello\nPOINT (3 4),world\nPOINT (5 6),alpha\n".to_vec()
        } else {
            b"value\nhello\nworld\nalpha\n".to_vec()
        }
    } else {
        let input = root.join("seed.arrow");
        let output = root.join(format!("source.{extension}"));
        std::fs::write(&input, ipc)?;
        let mut r = plenora_io_tools::operazioni::Richiesta::sulla_sorgente(input)
            .con_destinazione(&output);
        r.target_format = Some(format.into());
        let result = plenora_io_tools::operazioni::write(r);
        if result.is_err() {
            return Err("fixture native format generation failed".into());
        }
        std::fs::read(output)?
    };
    let source = Source {
        entry: format!("source.{extension}"),
        files: BTreeMap::from([(format!("source.{extension}"), source)]),
    };
    let source_ref = format!(
        "artifact://sha256/{}",
        digest(&serde_json::to_vec(&source)?)
    );
    let spec = ArrowPort {
        schema_digest: None,
        primary_geometry: geometry,
    };
    let mut profile = InteropProfile {
        tenant: "interop".into(),
        database: None,
        artifact_sha256: digest(b"S4 test host artifact"),
        authorized_plans: vec![],
        artifacts: BTreeMap::new(),
        io: InteropIo {
            ports: plenora_runtime_adapter_io::Ports {
                read_schema: None,
                write_schema: None,
                primary_geometry: geometry,
            },
            sources: BTreeMap::from([(source_ref.clone(), source)]),
            destinations: BTreeMap::from([(
                "ref:destination".into(),
                Destination {
                    directory: root.join("bundle").to_string_lossy().into_owned(),
                    entry: format!("result.{extension}"),
                },
            )]),
            staging_directory: root.to_string_lossy().into_owned(),
        },
        data_ports: Ports {
            inputs: BTreeMap::from([("data".into(), spec.clone())]),
            outputs: BTreeMap::from([("output".into(), spec.clone())]),
        },
    };
    let registry = interop_registry(&profile)?;
    let native = json!({"schema_version":1,"plan":{"version":1,"inputs":["data"],"steps":[{"out":"output","op":"table.sort","in":["data"],"config":{"columns":["value"]}}],"outputs":["output"],"limits":{"max_input_rows":1000,"max_output_rows":1000,"max_rows_per_edge":1000,"max_governed_memory_bytes":16_777_216}},"inputs":{"data":{"reference":"ref:input-data","content_type":"application/vnd.apache.arrow.stream"}},"outputs":{"output":{"reference":"ref:output-output","content_type":"application/vnd.apache.arrow.stream","overwrite":false}}});
    let parameters = [
        if geometry && matches!(format, "csv" | "xls") {
            json!({"source":source_ref,"assume_crs":"EPSG:4326","input_options":{"wkt_column":if format=="csv" {"geometry"} else {"g0"}}})
        } else {
            json!({"source":source_ref})
        },
        native,
        json!({"source":"ref:chain-data","destination":"ref:destination","format":format,"output_options":if format=="ipc" {json!({"serialization":"stream"})} else {json!({})}}),
    ];
    let mut plan: Value = serde_json::from_str(include_str!(
        "../../../../contracts/chains-v2/fixtures/plan-valid.json"
    ))?;
    plan["components"] = serde_json::to_value(&registry.components)?;
    let budget = json!({"memory_bytes":268_435_456,"elapsed_ms":60_000,"rows_read":1000,"rows_produced":1000,"artifact_bytes":16_777_216,"spool_bytes":268_435_456,"request_bytes":16_777_216,"result_bytes":1_048_576,"max_attempts":1});
    plan["budgets"] = json!({"memory_bytes":1_073_741_824,"elapsed_ms":0,"rows_read":3000,"rows_produced":3000,"artifact_bytes":67_108_864,"spool_bytes":1_073_741_824,"request_bytes":67_108_864,"result_bytes":4_194_304,"max_attempts":3});
    for (index, (component, operation, version)) in [
        ("io", "io.read", 2),
        ("data", "data.run", 3),
        ("io", "io.write", 2),
    ]
    .into_iter()
    .enumerate()
    {
        let op = registry
            .operations
            .iter()
            .find(|o| o.operation == operation)
            .ok_or("operation")?;
        let reference = format!("ref:parameters-{index}");
        let bytes = serde_json::to_vec(&parameters[index])?;
        let s = &mut plan["steps"][index];
        s["component"] = json!(component);
        s["operation"] = json!(operation);
        s["operation_version"] = json!(version);
        s["input_contract"] = json!(op.input_contract);
        s["output_contract"] = json!(op.output_contract);
        s["parameters"] =
            json!({"contract":op.input_contract,"artifact_ref":reference,"sha256":digest(&bytes)});
        s["outputs"] = serde_json::to_value(&op.outputs)?;
        s["budgets"] = budget.clone();
        s.as_object_mut().ok_or("step")?.remove("loss_consent");
        profile.artifacts.insert(reference, bytes);
    }
    // Sequential plan execution is authorized from its declared step budgets.
    plan["budgets"]["elapsed_ms"] = json!(
        plan["steps"]
            .as_array()
            .ok_or("steps")?
            .iter()
            .map(|step| step["budgets"]["elapsed_ms"].as_u64().ok_or("elapsed"))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .sum::<u64>()
    );
    plan["steps"][1]["inputs"] = json!({"data":{"source":{"kind":"step","step":"read","port":"data"},"expected":spec.port(),"boundary":"io-data","schema_barrier":true}});
    plan["steps"][2]["inputs"] = json!({"data":{"source":{"kind":"step","step":"transform","port":"output"},"expected":spec.port(),"boundary":"data-io","schema_barrier":true}});
    plan["outputs"] = json!({"data":{"source":{"kind":"step","step":"transform","port":"output"},"expected":spec.port(),"publication":"durable"}});
    plan["requirements"]["contracts"] = json!([
        "plenora-arrow-interchange-v1",
        "plenora-arrow-primary-geometry-v1"
    ]);
    plan["requirements"]["fidelity"] = json!("exact");
    let plan = contracts::parse_plan(&serde_json::to_vec(&plan)?)?;
    contracts::validate_plan(&plan, &registry)?;
    profile.authorized_plans = vec![serde_json::to_string(&plan)?];
    Ok((profile, plan))
}

/// Native identity pipeline: exercise interchange separately from metadata-changing kernels.
#[allow(
    dead_code,
    reason = "Shared fixture module; identity regression is exercised by interop.rs"
)]
pub fn identity(
    profile: &mut InteropProfile,
    plan: &mut Plan,
) -> Result<(), Box<dyn std::error::Error>> {
    let spec = profile
        .data_ports
        .outputs
        .remove("output")
        .ok_or("output")?;
    profile
        .data_ports
        .outputs
        .insert("data".into(), spec.clone());
    let raw = profile
        .artifacts
        .get_mut("ref:parameters-1")
        .ok_or("params")?;
    let mut native: Value = serde_json::from_slice(raw)?;
    native["plan"]["steps"] = json!([]);
    native["plan"]["outputs"] = json!(["data"]);
    native["outputs"] = json!({"data":{"reference":"ref:output-data","content_type":"application/vnd.apache.arrow.stream","overwrite":false}});
    *raw = serde_json::to_vec(&native)?;
    let mut p = serde_json::to_value(&*plan)?;
    p["steps"][1]["parameters"]["sha256"] = json!(digest(raw));
    p["steps"][1]["outputs"] = json!({"data":spec.port()});
    p["steps"][2]["inputs"]["data"]["source"]["port"] = json!("data");
    p["outputs"]["data"]["source"]["port"] = json!("data");
    *plan = contracts::parse_plan(&serde_json::to_vec(&p)?)?;
    contracts::validate_plan(plan, &interop_registry(profile)?)?;
    profile.authorized_plans = vec![serde_json::to_string(plan)?];
    Ok(())
}

/// Select a real target format independently of the input format, then confirm the whole plan.
#[allow(
    dead_code,
    reason = "Shared fixture target selection is used by publication and real CI tests"
)]
pub fn target(
    profile: &mut InteropProfile,
    plan: &mut Plan,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let raw = profile
        .artifacts
        .get_mut("ref:parameters-2")
        .ok_or("parameters")?;
    let mut parameters: Value = serde_json::from_slice(raw)?;
    parameters["format"] = json!(format);
    parameters["output_options"] = if format == "ipc" {
        json!({"serialization":"stream"})
    } else {
        json!({})
    };
    *raw = serde_json::to_vec(&parameters)?;
    plan.steps[2].parameters.sha256 = digest(raw);
    let extension = match format {
        "ipc" => "arrow",
        "xls" => "xlsx",
        "geoparquet" => "parquet",
        v => v,
    };
    profile
        .io
        .destinations
        .get_mut("ref:destination")
        .ok_or("destination")?
        .entry = format!("result.{extension}");
    profile.authorized_plans = vec![serde_json::to_string(plan)?];
    contracts::validate_plan(plan, &interop_registry(profile)?)?;
    Ok(())
}

/// Replace the first step with real database.read@1; the full plan is confirmed anew.
#[allow(
    dead_code,
    reason = "Shared fixture supports the PostgreSQL CI campaign"
)]
pub fn postgres(
    profile: &mut InteropProfile,
    plan: &mut Plan,
    source: &str,
    connection: String,
) -> Result<(), Box<dyn std::error::Error>> {
    profile.database = Some(plenora_runtime_chain_host::InteropDatabase {
        ports: plenora_runtime_adapter_database::Ports {
            read_schema: None,
            write_schema: None,
            primary_geometry: false,
        },
        connections: BTreeMap::from([(
            "DB".into(),
            plenora_runtime_chain_host::DatabaseConnectionConfig {
                destination: "ref:pg-source".into(),
                secret: connection,
                allow_plaintext: true,
            },
        )]),
    });
    profile.io.sources.clear();
    let operation = serde_json::to_vec(
        &json!({"source":{"schema":"public","object":source},"projection":[],"order_by":[],"row_limit":null,"filter":null}),
    )?;
    let reference = format!("artifact://sha256/{}", digest(&operation));
    let parameters = serde_json::to_vec(
        &json!({"provider":"postgres","secret_environment":"DB","operation_path":reference}),
    )?;
    profile.artifacts.insert(reference, operation);
    profile
        .artifacts
        .insert("ref:parameters-0".into(), parameters.clone());
    let registry = interop_registry(profile)?;
    let read = registry
        .operations
        .iter()
        .find(|o| o.operation == "database.read")
        .ok_or("read")?;
    let mut p = serde_json::to_value(&*plan)?;
    p["components"] = serde_json::to_value(&registry.components)?;
    p["requirements"]["contracts"] = json!(["plenora-arrow-interchange-v1"]);
    let step = &mut p["steps"][0];
    step["component"] = json!("database");
    step["operation"] = json!("database.read");
    step["operation_version"] = json!(1);
    step["input_contract"] = json!(read.input_contract);
    step["output_contract"] = json!(read.output_contract);
    step["outputs"] = json!(read.outputs);
    step["parameters"] = json!({"contract":read.input_contract,"artifact_ref":"ref:parameters-0","sha256":digest(&parameters)});
    p["steps"][1]["inputs"]["data"]["boundary"] = json!("database-data");
    *plan = contracts::parse_plan(&serde_json::to_vec(&p)?)?;
    contracts::validate_plan(plan, &registry)?;
    profile.authorized_plans = vec![serde_json::to_string(plan)?];
    Ok(())
}
