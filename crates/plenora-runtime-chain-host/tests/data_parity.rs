//! Real DATA versus the already qualified scripted host, through the same dispatcher.
#![allow(clippy::too_many_lines)]
#[path = "../../plenora-runtime-adapter-data/tests/support/fixture.rs"]
mod fixture;
#[path = "../../plenora-runtime-chains/tests/support/primary.rs"]
mod primary;
use plenora_runtime_chain_host::{DataHost, ScriptedConfig, ScriptedHost};
use plenora_runtime_chains::{Request, RunOptions};
use plenora_runtime_testkit::ManualClock;
use serde_json::Value;
use std::{sync::Arc, time::UNIX_EPOCH};
#[tokio::test]
async fn real_and_scripted_data_share_states_errors_reports_and_counters()
-> Result<(), Box<dyn std::error::Error>> {
    for bad in [false, true] {
        let mut native = fixture::native();
        if bad {
            native["steps"][0]["config"]["columns"] = serde_json::json!(["absent"]);
        }
        let (config, plan) = fixture::fixture(&native, fixture::ipc(&fixture::batch()?)?, true)?;
        let reg = plenora_runtime_adapter_data::registry(&config.artifact_sha256, &config.ports)?;
        let mut artifacts = config.artifacts.clone();
        artifacts.insert(
            "ref:data-catalog".into(),
            serde_json::to_vec(&plenora_runtime_adapter_data::catalog())?,
        );
        let clock = Arc::new(ManualClock::new(UNIX_EPOCH));
        let host = DataHost::with_data(
            config,
            Arc::new(primary::ScriptStore::default()),
            clock.clone(),
        )?;
        let request = Request::Run {
            plan: serde_json::to_string(&plan)?,
            options: RunOptions {
                run_id: "00000000-0000-4000-8000-000000000061".into(),
                idempotency_key: None,
                deadline_ms: None,
            },
        };
        let actual = host
            .execute(serde_json::from_slice(&serde_json::to_vec(&request)?)?)
            .await;
        assert_eq!(
            actual.value["state"],
            if bad { "failed" } else { "succeeded" }
        );
        // The scripted adapter intentionally consumes JSON fixture envelopes, never real Arrow.
        // Build an explicitly fake oracle with the same terminal axes and measured row counts.
        let mut fake_plan = serde_json::to_value(&plan)?;
        let mut fake_registry = serde_json::to_value(&reg)?;
        let mut oracle = actual.value.clone();
        let fake_arrow = serde_json::to_vec(
            &serde_json::json!({"fixture":"scripted-arrow-v1","schema":{"fields":[{"id":"1","name":"value","data_type":"utf8","nullable":true,"geometry":false,"metadata":{}}],"metadata":{}},"rows":[[""],["hello"],[null]]}),
        )?;
        let document = |contract: &str,
                        reference: &str,
                        body: Value,
                        artifacts: &mut std::collections::BTreeMap<String, Vec<u8>>|
         -> Result<Value, Box<dyn std::error::Error>> {
            let bytes = serde_json::to_vec(
                &serde_json::json!({"contract":contract,"fixture":"scripted-v1","body":body}),
            )?;
            let value = serde_json::json!({"contract":contract,"artifact_ref":reference,"sha256":fixture::digest(&bytes)});
            artifacts.insert(reference.into(), bytes);
            Ok(value)
        };
        let cat = document(
            &reg.components["data"].catalog.contract,
            "ref:data-catalog",
            serde_json::json!({"kind":"catalog"}),
            &mut artifacts,
        )?;
        fake_registry["components"]["data"]["catalog"] = cat.clone();
        fake_plan["components"]["data"]["catalog"] = cat;
        let params = document(
            &plan.steps[0].input_contract,
            "ref:parameters",
            serde_json::json!({"kind":"parameters","operation":"data.run","version":3}),
            &mut artifacts,
        )?;
        fake_plan["steps"][0]["parameters"] = params;
        artifacts.insert("ref:source".into(), fake_arrow.clone());
        fake_plan["inputs"]["data"]["sha256"] = serde_json::json!(fixture::digest(&fake_arrow));
        fake_plan["inputs"]["data"]["bytes"] = serde_json::json!(fake_arrow.len());
        oracle["steps"][0]["input_manifests"] = fake_plan["inputs"].clone();
        if !bad {
            let output = &mut oracle["steps"][0]["outputs"]["output"];
            let reference = output["artifact_ref"].as_str().ok_or("output")?.to_owned();
            artifacts.insert(reference, fake_arrow.clone());
            output["sha256"] = serde_json::json!(fixture::digest(&fake_arrow));
            output["bytes"] = serde_json::json!(fake_arrow.len());
        }
        let inputs = serde_json::json!({"data":["ref:source",fixture::digest(&fake_arrow)]});
        let outputs = if bad {
            serde_json::json!({})
        } else {
            serde_json::json!({"output":[oracle["steps"][0]["outputs"]["output"]["artifact_ref"],fixture::digest(&fake_arrow)]})
        };
        let fidelity = document(
            "plenora-fixture-fidelity-v1",
            "ref:fake-fidelity",
            serde_json::json!({"kind":"fidelity","step":"transform","operation":"data.run","version":3,"parameters_sha256":fake_plan["steps"][0]["parameters"]["sha256"],"destination":null,"inputs":inputs,"outputs":outputs,"verdict":"exact"}),
            &mut artifacts,
        )?;
        let reports = serde_json::json!([{"kind":"fidelity","availability":"available","document":fidelity},{"kind":"loss_report","availability":"available","document":fidelity}]);
        let upstream = document(
            &plan.steps[0].output_contract,
            "ref:fake-upstream",
            serde_json::json!({"kind":"upstream"}),
            &mut artifacts,
        )?;
        oracle["steps"][0]["upstream_result"] = upstream.clone();
        oracle["steps"][0]["reports"] = reports.clone();
        if bad {
            oracle["steps"][0]["error"]["reports"] = reports.clone();
            oracle["steps"][0]["error"]["upstream_error"] = upstream;
        } else {
            oracle["steps"][0]["outputs"]["output"]["reports"] = reports.clone();
        }
        oracle["reports"] = reports.clone();
        oracle["reports_complete"] = serde_json::json!(true);
        oracle["outputs"] = oracle["steps"][0]["outputs"].clone();
        if bad {
            oracle["error"]["reports"] = reports;
        }
        let scripted = ScriptedHost::with_store(
            ScriptedConfig {
                database: "unused".into(),
                journal_bytes: plenora_runtime_chains::JOURNAL_BYTES,
                tenant: "tenant".into(),
                registry: serde_json::to_string(&fake_registry)?,
                authorized_plans: vec![serde_json::to_string(&fake_plan)?],
                outcomes: serde_json::to_string(&oracle)?,
                recovery: None,
                attempts: vec![],
                artifacts,
            },
            Arc::new(primary::ScriptStore::default()),
            clock,
        )?;
        let request = Request::Run {
            plan: serde_json::to_string(&fake_plan)?,
            options: RunOptions {
                run_id: "00000000-0000-4000-8000-000000000061".into(),
                idempotency_key: None,
                deadline_ms: None,
            },
        };
        let expected = scripted.execute(request).await;
        assert_eq!(actual.error, expected.error);
        for key in ["state", "remote_effect"] {
            assert_eq!(actual.value[key], expected.value[key]);
        }
        let axes = |v: &Value| -> Value {
            let s = &v["steps"][0];
            serde_json::json!([
                s["state"],
                s["error"]["category"],
                s["error"]["retry"],
                s["remote_effect"],
                s["counters"]["rows_read"],
                s["counters"]["rows_produced"]
            ])
        };
        assert_eq!(axes(&actual.value), axes(&expected.value));
    }
    Ok(())
}
