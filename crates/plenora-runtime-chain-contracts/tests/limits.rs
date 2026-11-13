//! Boundary probes independent of the pinned fixture verdicts.
use plenora_runtime_chain_contracts::*;
use std::{error::Error, fs, path::PathBuf};
fn load(name: &str) -> Result<Vec<u8>, std::io::Error> {
    fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../contracts/chains-v2/fixtures")
            .join(name),
    )
}
fn setup() -> Result<(Plan, Registry), Box<dyn Error>> {
    Ok((
        parse_plan(&load("plan-valid.json")?)?,
        parse_registry(&load("context.json")?)?,
    ))
}

#[test]
fn parser_limits_and_lexical_numbers() -> Result<(), Box<dyn Error>> {
    for raw in [
        b"-0".as_slice(),
        b"2.0",
        b"2e0",
        b"9007199254740992",
        b"\xef\xbb\xbf{}",
        b"\"\\ud800\"",
        b"{} trailing",
    ] {
        assert_eq!(parse_plan(raw).err(), Some(Rejection(Code::Document)));
    }
    let mut raw = b"0".to_vec();
    raw.resize(1_048_576, b' ');
    assert_eq!(parse_plan(&raw).err(), Some(Rejection(Code::Schema)));
    raw.push(b' ');
    assert_eq!(parse_plan(&raw).err(), Some(Rejection(Code::Limit)));
    for (depth, code) in [(63, Code::Schema), (64, Code::Limit)] {
        let raw = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
        assert_eq!(parse_plan(raw.as_bytes()).err(), Some(Rejection(code)));
    }
    for raw in [
        br#"{"x":1,"\u0078":2}"#.as_slice(),
        br#"{"a":{"x":1,"x":2}}"#,
    ] {
        assert_eq!(parse_plan(raw).err(), Some(Rejection(Code::Duplicate)));
    }
    let (plan, _) = setup()?;
    assert_eq!(parse_plan(&canonical_plan(&plan)?)?, plan);
    Ok(())
}

#[test]
fn exact_step_limit_and_lexicographic_order() -> Result<(), Box<dyn Error>> {
    let (mut plan, registry) = setup()?;
    let first = plan.steps.first().ok_or("missing fixture step")?.clone();
    plan.steps = (0..1024)
        .rev()
        .map(|i| {
            let mut s = first.clone();
            s.id = format!("s{i}");
            s
        })
        .collect();
    plan.outputs.clear();
    let report = validate_plan(&plan, &registry)?;
    assert_eq!(report.order.len(), 1024);
    assert!(report.order.windows(2).all(|p| p.first() < p.get(1)));
    plan.steps.push(first);
    assert_eq!(
        validate_plan(&plan, &registry).err(),
        Some(Rejection(Code::Schema))
    );
    Ok(())
}

#[test]
fn primary_claims_and_fidelity_have_explicit_verdicts() -> Result<(), Box<dyn Error>> {
    let (plan, registry) = setup()?;
    let report = validate_plan(&plan, &registry)?;
    assert_eq!(report.admission(), "reference_only");
    assert_eq!(report.fidelity, FidelityVerdict::ExactRequiresEvidence);
    let mut reported = plan.clone();
    reported.requirements.fidelity = "reported".into();
    assert_eq!(
        validate_plan(&reported, &registry)?.fidelity,
        FidelityVerdict::ReportedRequiresEvidence
    );
    reported.requirements.fidelity = "lossy".into();
    assert_eq!(
        validate_plan(&reported, &registry).err(),
        Some(Rejection(Code::Schema))
    );
    for name in plan.components.keys() {
        let mut p = plan.clone();
        let mut r = registry.clone();
        if let Some(c) = p.components.get_mut(name) {
            c.claims
                .retain(|c| c != "plenora-arrow-primary-geometry-v1");
        }
        r.components = p.components.clone();
        assert_eq!(
            validate_plan(&p, &r).err(),
            Some(Rejection(Code::Boundary)),
            "claim: {name}"
        );
    }
    Ok(())
}

#[test]
fn consent_false_is_not_absence_and_debug_is_redacted() -> Result<(), Box<dyn Error>> {
    let (mut plan, registry) = setup()?;
    let digest = plan_digest(&plan)?;
    let step = plan.steps.last_mut().ok_or("missing fixture step")?;
    step.loss_consent = Some(false);
    let changed = plan_digest(&plan)?;
    // Both forms deny loss but they are distinct signed documents.
    if digest == changed {
        plan.steps.last_mut().ok_or("missing step")?.loss_consent = None;
    }
    assert_ne!(plan_digest(&plan)?, changed);
    validate_plan(&plan, &registry)?;
    plan.steps.first_mut().ok_or("missing step")?.loss_consent = Some(false);
    assert_eq!(
        validate_plan(&plan, &registry).err(),
        Some(Rejection(Code::Consent))
    );
    plan.plan_id = "SENTINEL-SECRET".into();
    assert!(!format!("{plan:?}").contains("SENTINEL"));
    assert_eq!(
        validate_plan(&plan, &registry).err(),
        Some(Rejection(Code::Schema))
    );
    Ok(())
}

#[test]
fn duplicate_discovery_and_dynamic_schema_barriers() -> Result<(), Box<dyn Error>> {
    let (plan, mut registry) = setup()?;
    registry.operations.push(
        registry
            .operations
            .first()
            .ok_or("missing operation")?
            .clone(),
    );
    assert_eq!(
        validate_plan(&plan, &registry).err(),
        Some(Rejection(Code::Duplicate))
    );
    let plan = parse_plan(&load("dynamic-schema-true.json")?)?;
    let registry = parse_registry(&load("dynamic-schema-true-context.json")?)?;
    let report = validate_plan(&plan, &registry)?;
    assert!(!report.deferred.is_empty());
    assert!(report.deferred.windows(2).all(|pair| {
        pair.first()
            .zip(pair.get(1))
            .is_some_and(|(a, b)| (&a.step, &a.port) <= (&b.step, &b.port))
    }));
    Ok(())
}

#[test]
fn primary_geometry_on_public_output_is_not_an_unchecked_sink() -> Result<(), Box<dyn Error>> {
    let (mut plan, mut registry) = setup()?;
    let read = plan.steps.first().ok_or("missing step")?.clone();
    plan.steps = vec![read.clone()];
    for output in plan.outputs.values_mut() {
        output.source = Source::Variant1(Source1 {
            kind: "step".into(),
            step: read.id.clone(),
            port: "dataset".into(),
        });
    }
    validate_plan(&plan, &registry)?;
    plan.components
        .get_mut(&read.component)
        .ok_or("missing component")?
        .claims
        .clear();
    registry.components = plan.components.clone();
    assert_eq!(
        validate_plan(&plan, &registry).err(),
        Some(Rejection(Code::Boundary))
    );
    Ok(())
}

#[test]
fn unavailable_error_reports_cannot_claim_complete_or_no_losses() -> Result<(), Box<dyn Error>> {
    let (plan, registry) = setup()?;
    let original = parse_result(&load("result-failed-valid.json")?)?;
    let report = Report {
        kind: "fidelity".into(),
        availability: "unavailable".into(),
        document: None,
    };
    for location in 0..4 {
        let mut result = original.clone();
        let mut error = result.error.clone().ok_or("missing error")?;
        error.reports.push(report.clone());
        match location {
            0 => result.error = Some(error),
            1 => result.other_errors.push(error),
            2 => {
                result
                    .steps
                    .iter_mut()
                    .find(|s| s.error.is_some())
                    .ok_or("missing failed step")?
                    .error = Some(error);
            }
            _ => result
                .steps
                .iter_mut()
                .find(|s| s.error.is_some())
                .ok_or("missing failed step")?
                .other_errors
                .push(error),
        }
        result.reports_complete = true;
        result.losses = "none".into();
        assert_eq!(
            validate_result(&result, &plan, &registry),
            Err(Rejection(Code::Result))
        );
        result.reports_complete = false;
        assert_eq!(
            validate_result(&result, &plan, &registry),
            Err(Rejection(Code::Result))
        );
        result.losses = "unavailable".into();
        validate_result(&result, &plan, &registry)?;
    }
    Ok(())
}

#[test]
fn closed_objects_reject_unknown_fields_at_every_depth() -> Result<(), Box<dyn Error>> {
    use serde_json::Value;
    fn mutations(value: &Value) -> Vec<Value> {
        let mut result = Vec::new();
        match value {
            Value::Object(map) => {
                let mut changed = map.clone();
                changed.insert("unknown".into(), "SENTINEL".into());
                result.push(changed.into());
                for (name, child) in map {
                    for mutation in mutations(child) {
                        let mut changed = map.clone();
                        changed.insert(name.clone(), mutation);
                        result.push(changed.into());
                    }
                }
            }
            Value::Array(items) => {
                for (index, child) in items.iter().enumerate() {
                    for mutation in mutations(child) {
                        let mut changed = items.clone();
                        if let Some(slot) = changed.get_mut(index) {
                            *slot = mutation;
                        }
                        result.push(changed.into());
                    }
                }
            }
            _ => {}
        }
        result
    }
    for kind in ["plan", "result", "event", "error", "context"] {
        let file = if kind == "context" {
            "context.json".into()
        } else {
            format!("{kind}-valid.json")
        };
        let value: Value = serde_json::from_slice(&load(&file)?)?;
        for mutation in mutations(&value) {
            let raw = serde_json::to_vec(&mutation)?;
            let rejected = match kind {
                "plan" => parse_plan(&raw).err(),
                "result" => parse_result(&raw).err(),
                "event" => parse_event(&raw).err(),
                "error" => parse_error(&raw).err(),
                _ => parse_registry(&raw).err(),
            };
            assert_eq!(
                rejected,
                Some(Rejection(Code::Schema)),
                "closed object: {kind}"
            );
        }
    }
    Ok(())
}

#[test]
fn canonical_digest_matches_python_golden_and_changes_with_scope() -> Result<(), Box<dyn Error>> {
    let (mut plan, _) = setup()?;
    assert_eq!(
        plan_digest(&plan)?,
        "9b5a0a0e9dc989bba5222b0697dc2d1823b7683ad2471dc37a2c55698c694dcb"
    );
    let original = plan_digest(&plan)?;
    plan.steps.last_mut().ok_or("missing step")?.destination_ref = Some("ref:other".into());
    assert_ne!(plan_digest(&plan)?, original);
    Ok(())
}

#[test]
fn combined_port_limit_is_exact() -> Result<(), Box<dyn Error>> {
    let (mut plan, mut registry) = setup()?;
    let mut step = plan.steps.first().ok_or("missing step")?.clone();
    let port = step.outputs.values().next().ok_or("missing port")?.clone();
    step.outputs = (0..32).map(|i| (format!("out{i}"), port.clone())).collect();
    plan.inputs.insert(
        "initial".into(),
        Input {
            artifact_ref: "ref:initial".into(),
            port: port.clone(),
            bytes: 0,
            sha256: "a".repeat(64),
        },
    );
    step.inputs = (0..32)
        .map(|i| {
            (
                format!("in{i}"),
                Binding {
                    source: Source::Variant0(Source0 {
                        kind: "input".into(),
                        input: "initial".into(),
                    }),
                    expected: port.clone(),
                    boundary: "plan-input".into(),
                    schema_barrier: false,
                },
            )
        })
        .collect();
    let descriptor = registry
        .operations
        .iter_mut()
        .find(|d| d.operation == step.operation)
        .ok_or("missing descriptor")?;
    descriptor.inputs = step
        .inputs
        .iter()
        .map(|(k, b)| (k.clone(), b.expected.clone()))
        .collect();
    descriptor.outputs = step.outputs.clone();
    plan.outputs.clear();
    plan.steps = vec![step.clone()];
    validate_plan(&plan, &registry)?;
    step.outputs.insert("extra".into(), port);
    registry
        .operations
        .iter_mut()
        .find(|d| d.operation == step.operation)
        .ok_or("missing descriptor")?
        .outputs = step.outputs.clone();
    plan.steps = vec![step];
    assert_eq!(
        validate_plan(&plan, &registry).err(),
        Some(Rejection(Code::Limit))
    );
    Ok(())
}
