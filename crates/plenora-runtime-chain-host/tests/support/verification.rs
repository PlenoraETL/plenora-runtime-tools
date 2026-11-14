//! Declared JSON resource/Arrow/fidelity doubles; the primary and library providers are fake.
use super::*;
use sha2::{Digest, Sha256};

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn changed_resource(
    profile: &mut ScriptedConfig,
    reference: &str,
    change: impl FnOnce(&mut Value),
) -> ResultTest {
    let bytes = profile.artifacts.get_mut(reference).ok_or("resource")?;
    let old = hash(bytes);
    let mut value: Value = serde_json::from_slice(bytes)?;
    change(&mut value);
    *bytes = serde_json::to_vec(&value)?;
    let new = hash(bytes);
    for raw in profile
        .authorized_plans
        .iter_mut()
        .chain([&mut profile.outcomes, &mut profile.registry])
    {
        *raw = raw.replace(&old, &new);
    }
    Ok(())
}
async fn run_profile(profile: ScriptedConfig) -> Result<(Response, u64), Box<dyn StdError>> {
    let plan = profile.authorized_plans.first().ok_or("plan")?.clone();
    let store = Arc::new(ScriptStore::default());
    let h = ScriptedHost::with_store(
        profile,
        store.clone(),
        Arc::new(ManualClock::new(UNIX_EPOCH + Duration::from_hours(498_492))),
    )?;
    let response = h
        .execute(Request::Run {
            plan,
            options: RunOptions {
                run_id: uuid::Uuid::new_v4().to_string(),
                idempotency_key: None,
                deadline_ms: None,
            },
        })
        .await;
    Ok((response, domain_attempts(&store)?))
}

#[tokio::test]
async fn every_resolved_reference_is_checked_before_its_effect() -> ResultTest {
    for (reference, calls) in [
        ("ref:catalog", 0),
        ("ref:parameters-read", 0),
        ("ref:read", 0),
        ("ref:upstream-read", 0),
        ("ref:fidelity-read", 0),
        ("ref:parameters-transform", 1),
        ("ref:transform", 1),
        ("ref:upstream-transform", 1),
        ("ref:fidelity-transform", 1),
        ("ref:parameters-write", 2),
        ("ref:upstream-write", 2),
        ("ref:fidelity-write", 2),
    ] {
        for missing in [true, false] {
            let mut profile = config("resolver")?;
            if missing {
                profile.artifacts.remove(reference);
            } else {
                profile
                    .artifacts
                    .get_mut(reference)
                    .ok_or("resource")?
                    .push(b' ');
            }
            let (response, actual) = run_profile(profile).await?;
            assert!(response.error.is_some(), "{reference}");
            assert_eq!(actual, calls, "{reference}");
            assert_eq!(
                response.value["steps"][usize::try_from(calls)?]["remote_effect"],
                "none"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn closed_parameter_schema_and_fidelity_evidence_cannot_be_self_certified() -> ResultTest {
    for mutation in ["contract", "operation", "version", "extra", "fixture"] {
        let mut profile = config("parameters")?;
        changed_resource(
            &mut profile,
            "ref:parameters-read",
            |value| match mutation {
                "contract" => value["contract"] = json!("plenora-other-v1"),
                "operation" => value["body"]["operation"] = json!("io.write"),
                "version" => value["body"]["version"] = json!(1),
                "fixture" => value["fixture"] = json!("real-arrow"),
                _ => value["body"]["ignored"] = json!(true),
            },
        )?;
        let (response, calls) = run_profile(profile).await?;
        assert!(response.error.is_some(), "{mutation}");
        assert_eq!(calls, 0);
    }
    for mutation in [
        "forbidden_loss",
        "database_loss",
        "wrong_step",
        "wrong_parameters",
        "wrong_inputs",
        "wrong_outputs",
        "wrong_destination",
    ] {
        let mut profile = config("fidelity")?;
        changed_resource(&mut profile, "ref:fidelity-read", |value| match mutation {
            "wrong_step" => value["body"]["step"] = json!("transform"),
            "wrong_parameters" => value["body"]["parameters_sha256"] = json!("0".repeat(64)),
            "wrong_inputs" => value["body"]["inputs"] = json!({"in":["ref:read","0".repeat(64)]}),
            "wrong_outputs" => value["body"]["outputs"] = json!({}),
            "wrong_destination" => value["body"]["destination"] = json!("ref:other"),
            _ => value["body"]["verdict"] = json!(mutation),
        })?;
        let (response, calls) = run_profile(profile).await?;
        assert!(response.error.is_some(), "{mutation}");
        assert_eq!(calls, 0);
    }
    Ok(())
}

#[tokio::test]
async fn arrow_full_schema_metadata_and_primary_are_checked_without_mutating_manifests()
-> ResultTest {
    for mutation in [
        "metadata",
        "nullable",
        "type",
        "id",
        "primary",
        "name",
        "duplicate",
    ] {
        let mut profile = config("schema")?;
        changed_resource(&mut profile, "ref:read", |value| match mutation {
            "metadata" => value["schema"]["fields"][0]["metadata"]["unknown"] = json!("changed"),
            "nullable" => value["schema"]["fields"][0]["nullable"] = json!(false),
            "type" => value["schema"]["fields"][0]["data_type"] = json!("utf8"),
            "id" => value["schema"]["fields"][0]["id"] = json!("changed"),
            "primary" => value["schema"]["metadata"]["plenora.primary_geometry"] = json!("missing"),
            "name" => value["schema"]["fields"][0]["name"] = json!("changed"),
            _ => {
                let field = value["schema"]["fields"][0].clone();
                value["schema"]["fields"] = json!([field, field]);
            }
        })?;
        // Hash and length are correct: rejection must be schema, not a stale-byte accident.
        let length = profile.artifacts["ref:read"].len();
        let mut result: Value = serde_json::from_str(&profile.outcomes)?;
        result["steps"][0]["outputs"]["dataset"]["bytes"] = json!(length);
        profile.outcomes = serde_json::to_string(&result)?;
        let (response, calls) = run_profile(profile).await?;
        assert_eq!(response.value["error"]["category"], "schema", "{mutation}");
        assert_eq!(calls, 0);
    }
    let mut profile = config("dynamic")?;
    for raw in profile
        .authorized_plans
        .iter_mut()
        .chain([&mut profile.registry, &mut profile.outcomes])
    {
        let mut value: Value = serde_json::from_str(raw)?;
        dynamic(&mut value);
        *raw = serde_json::to_string(&value)?;
    }
    let (response, calls) = run_profile(profile).await?;
    assert_eq!(response.error, None);
    assert_eq!(calls, 3);
    assert!(
        response.value["steps"][1]["input_manifests"]["dataset"]["port"]["schema_digest"].is_null()
    );
    Ok(())
}
fn dynamic(value: &mut Value) {
    match value {
        Value::Object(map) => {
            if map.contains_key("schema_digest") {
                map.insert("schema_digest".into(), Value::Null);
            }
            if map.contains_key("schema_barrier") {
                map.insert("schema_barrier".into(), json!(true));
            }
            for child in map.values_mut() {
                dynamic(child);
            }
        }
        Value::Array(values) => {
            for child in values {
                dynamic(child);
            }
        }
        _ => {}
    }
}

#[tokio::test]
async fn dynamic_producer_is_compared_to_static_consumer_before_consumer_effect() -> ResultTest {
    for mismatch in [false, true] {
        let mut profile = config("dynamic-static")?;
        let mut plan: Value = serde_json::from_str(&profile.authorized_plans[0])?;
        let mut registry: Value = serde_json::from_str(&profile.registry)?;
        let mut result: Value = serde_json::from_str(&profile.outcomes)?;
        plan["steps"][0]["outputs"]["dataset"]["schema_digest"] = Value::Null;
        plan["steps"][1]["inputs"]["dataset"]["schema_barrier"] = json!(true);
        registry["operations"][0]["outputs"]["dataset"]["schema_digest"] = Value::Null;
        result["steps"][0]["outputs"]["dataset"]["port"]["schema_digest"] = Value::Null;
        result["steps"][1]["input_manifests"]["dataset"]["port"]["schema_digest"] = Value::Null;
        if mismatch {
            plan["steps"][1]["inputs"]["dataset"]["expected"]["schema_digest"] =
                json!("0".repeat(64));
            registry["operations"][1]["inputs"]["dataset"]["schema_digest"] = json!("0".repeat(64));
        }
        profile.authorized_plans = vec![serde_json::to_string(&plan)?];
        profile.registry = serde_json::to_string(&registry)?;
        profile.outcomes = serde_json::to_string(&result)?;
        let (response, calls) = run_profile(profile).await?;
        assert_eq!(response.error.is_some(), mismatch);
        assert_eq!(calls, if mismatch { 1 } else { 3 });
        if mismatch {
            assert_eq!(response.value["error"]["category"], "schema");
        }
    }
    Ok(())
}

#[tokio::test]
async fn output_counts_cannot_disagree_with_resolved_schema_and_rows() -> ResultTest {
    for key in ["rows", "columns", "bytes"] {
        let mut profile = config("counts")?;
        let mut result: Value = serde_json::from_str(&profile.outcomes)?;
        result["steps"][0]["outputs"]["dataset"][key] = json!(999);
        profile.outcomes = serde_json::to_string(&result)?;
        let (response, calls) = run_profile(profile).await?;
        assert!(response.error.is_some());
        assert_eq!(calls, 0);
    }
    Ok(())
}

#[tokio::test]
async fn initial_plan_inputs_are_resolved_and_hashed_before_the_first_effect() -> ResultTest {
    for mutation in ["none", "missing", "bytes"] {
        let mut profile = config("initial-input")?;
        let mut plan: Value = serde_json::from_str(&profile.authorized_plans[0])?;
        let mut registry: Value = serde_json::from_str(&profile.registry)?;
        let mut result: Value = serde_json::from_str(&profile.outcomes)?;
        let mut input = result["steps"][1]["input_manifests"]["dataset"].clone();
        input["artifact_ref"] = json!("ref:seed");
        profile
            .artifacts
            .insert("ref:seed".into(), profile.artifacts["ref:read"].clone());
        plan["inputs"]["seed"] = input.clone();
        plan["steps"][0]["inputs"]["dataset"] = json!({
            "source":{"kind":"input","input":"seed"}, "expected": input["port"],
            "boundary":"plan-input", "schema_barrier":false,
        });
        registry["operations"][0]["inputs"]["dataset"] = input["port"].clone();
        result["steps"][0]["input_manifests"]["dataset"] = input.clone();
        profile.authorized_plans = vec![serde_json::to_string(&plan)?];
        profile.registry = serde_json::to_string(&registry)?;
        profile.outcomes = serde_json::to_string(&result)?;
        changed_resource(&mut profile, "ref:fidelity-read", |v| {
            v["body"]["inputs"]["dataset"] = json!(["ref:seed", input["sha256"]]);
        })?;
        match mutation {
            "missing" => {
                profile.artifacts.remove("ref:seed");
            }
            "bytes" => {
                profile.artifacts.get_mut("ref:seed").ok_or("seed")?.push(0);
            }
            _ => {}
        }
        let (response, calls) = run_profile(profile).await?;
        assert_eq!(response.error.is_none(), mutation == "none");
        assert_eq!(calls, if mutation == "none" { 3 } else { 0 });
    }
    Ok(())
}

#[tokio::test]
async fn database_loss_requires_exact_scoped_consent_and_reported_policy() -> ResultTest {
    for (reported, consent, report, success) in [
        (true, true, true, true),
        (false, true, true, false),
        (true, false, true, false),
        (true, true, false, false),
    ] {
        let mut profile = config("loss-consent")?;
        changed_resource(&mut profile, "ref:fidelity-write", |v| {
            v["body"]["verdict"] = json!("database_loss");
        })?;
        let mut plan: Value = serde_json::from_str(&profile.authorized_plans[0])?;
        if reported {
            plan["requirements"]["fidelity"] = json!("reported");
        }
        if consent {
            let parsed =
                plenora_runtime_chains::contracts::parse_plan(&serde_json::to_vec(&plan)?)?;
            let step = &parsed.steps[2];
            let input_digest =
                plenora_runtime_chains::contracts::consent_input_digest(&parsed, step)?;
            plan["steps"][2]["loss_consent"] = json!(true);
            plan["loss_policy"]["grants"] = json!([{
                "step": "write", "operation": "database.write", "operation_version": 2,
                "destination_ref": "ref:destination", "input_digest": input_digest,
                "parameters_digest": step.parameters.sha256,
            }]);
        }
        profile.authorized_plans = vec![serde_json::to_string(&plan)?];
        if report {
            let mut result: Value = serde_json::from_str(&profile.outcomes)?;
            let mut loss = result["steps"][2]["reports"][0].clone();
            loss["kind"] = json!("loss_report");
            result["steps"][2]["reports"]
                .as_array_mut()
                .ok_or("reports")?
                .push(loss);
            profile.outcomes = serde_json::to_string(&result)?;
        }
        let (response, calls) = run_profile(profile).await?;
        assert_eq!(response.error.is_none(), success);
        assert_eq!(calls, if success { 3 } else { 2 });
    }
    Ok(())
}

#[tokio::test]
async fn restart_recovery_rechecks_resources_and_preserves_unknown_evidence() -> ResultTest {
    let store = Arc::new(ScriptStore::default());
    let mut profile = config("restart")?;
    profile.outcomes = include_str!("../fixtures/result-recovery-valid.json").into();
    profile.recovery = Some(include_str!("../fixtures/result-valid.json").into());
    let snapshot = serde_json::to_vec(&profile)?;
    let clock = Arc::new(ManualClock::new(UNIX_EPOCH + Duration::from_hours(498_492)));
    let h = ScriptedHost::with_store(profile, store.clone(), clock.clone())?;
    let result = h.execute(request(true, None)).await;
    assert_eq!(result.error, Some(Error::Recovery));
    let run_id = result.value["run_id"].as_str().ok_or("run")?.to_owned();
    drop(h);
    let mut profile: ScriptedConfig = serde_json::from_slice(&snapshot)?;
    profile
        .artifacts
        .get_mut("ref:transform")
        .ok_or("resource")?
        .push(0);
    let h = ScriptedHost::with_store(profile, store.clone(), clock)?;
    let status = h
        .execute(Request::Status {
            run_id: run_id.clone(),
        })
        .await;
    let resumed = h
        .execute(Request::Resume {
            recovery: plenora_runtime_chains::Recovery::Reconcile,
            run_id: run_id.clone(),
            revision: status.value["revision"].as_u64().ok_or("revision")?,
        })
        .await;
    assert_eq!(resumed.error, Some(Error::Recovery));
    let after = h.execute(Request::Result { run_id }).await;
    assert_eq!(after.value["remote_effect"], "unknown");
    assert_eq!(after.value["steps"][0], result.value["steps"][0]);
    assert_eq!(after.value["steps"][1], result.value["steps"][1]);
    assert_eq!(domain_attempts(&store)?, 4);
    Ok(())
}
