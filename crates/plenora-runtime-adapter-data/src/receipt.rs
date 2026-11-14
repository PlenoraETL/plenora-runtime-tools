//! Separate immutable upstream evidence from externally confirmed publication.
use crate::{
    Error, Invocation, OwnedResource, digest, encode,
    operation::{Failure, Outcome},
    reference,
};
use plenora_runtime_chains::contracts::{
    Artifact, ChainError, Counters, Document, Report, Retry, Retry0, StepResult, parse_step_result,
};
use std::collections::BTreeMap;
pub(crate) const RECEIPT_RESERVATION: u64 = 64 * 1024;
fn retry(value: &serde_json::Value) -> Result<Retry, Error> {
    let kind = value["kind"].as_str().ok_or(Error::Recovery)?;
    if !matches!(kind, "never" | "requires_recovery") {
        return Err(Error::Recovery);
    }
    Ok(Retry::Variant0(Retry0 { kind: kind.into() }))
}
fn error(
    f: &Failure,
    i: &Invocation,
    upstream: Option<Document>,
    reports: &[Report],
) -> Result<ChainError, Error> {
    Ok(ChainError {
        contract: "plenora-chain-error-v2".into(),
        schema_version: 2,
        category: f.category.clone(),
        phase: f.phase.clone(),
        code: f.code.clone(),
        remote_effect: f.effect.clone(),
        retry: retry(&f.retry)?,
        message: "Chain operation rejected or incomplete.".into(),
        step_id: Some(i.step.id.clone()),
        upstream_error: upstream,
        reports: reports.to_vec(),
    })
}
#[allow(clippy::too_many_lines)]
pub(crate) fn build(
    i: &Invocation,
    outcome: Outcome,
    request_bytes: u64,
    elapsed_ms: u64,
) -> Result<(StepResult, Vec<OwnedResource>), Error> {
    let bytes = encode(&outcome.upstream)?;
    let upstream = (!outcome.upstream.is_null()).then(|| Document {
        contract: if outcome.upstream_error {
            "plenora-error-v1".into()
        } else {
            i.step.output_contract.clone()
        },
        artifact_ref: reference(i, "upstream"),
        sha256: digest(&bytes),
    });
    let effects = outcome
        .effects
        .as_ref()
        .map(|value| -> Result<_, Error> {
            let bytes = encode(value)?;
            Ok((
                Document {
                    contract: "plenora-data-localized-effects-v1".into(),
                    artifact_ref: reference(i, "effects"),
                    sha256: digest(&bytes),
                },
                bytes,
            ))
        })
        .transpose()?;
    let reports: Vec<_> = ["fidelity", "loss_report"]
        .into_iter()
        .map(|kind| Report {
            kind: kind.into(),
            availability: if outcome.complete {
                "available"
            } else {
                "unavailable"
            }
            .into(),
            document: if outcome.complete {
                effects.as_ref().map(|(d, _)| d.clone())
            } else {
                None
            },
        })
        .chain(std::iter::once(Report {
            kind: "upstream_result".into(),
            availability: if upstream.is_some() {
                "available"
            } else {
                "unavailable"
            }
            .into(),
            document: upstream.clone(),
        }))
        .collect();
    let mut reports = reports;
    reports.push(Report {
        kind: "diagnostics".into(),
        availability: if effects.is_some() {
            "available"
        } else {
            "unavailable"
        }
        .into(),
        document: effects.as_ref().map(|(d, _)| d.clone()),
    });
    let mut objects = Vec::new();
    if let Some(d) = &upstream {
        objects.push(OwnedResource {
            reference: d.artifact_ref.clone(),
            bytes,
            consumers: vec![],
            temporary: false,
        });
    }
    if let Some((d, bytes)) = effects {
        objects.push(OwnedResource {
            reference: d.artifact_ref,
            bytes,
            consumers: vec![],
            temporary: false,
        });
    }
    let failure = outcome
        .failure
        .as_ref()
        .map(|f| {
            error(
                f,
                i,
                if outcome.upstream_error {
                    upstream.clone()
                } else {
                    None
                },
                &reports,
            )
        })
        .transpose()?;
    let other_errors = vec![];
    let state = if outcome.effect == "unknown" {
        "requires_recovery"
    } else if outcome.effect == "partial" {
        "partial"
    } else if outcome
        .failure
        .as_ref()
        .is_some_and(|f| f.category == "cancelled")
    {
        "cancelled"
    } else if failure.is_some() {
        "failed"
    } else {
        "succeeded"
    };
    let retry: Retry = retry(
        &outcome
            .failure
            .as_ref()
            .map_or_else(|| serde_json::json!({"kind":"never"}), |f| f.retry.clone()),
    )?;
    let mut outputs = BTreeMap::new();
    let mut artifact_bytes = 0;
    if state == "succeeded" {
        for (name, output) in outcome.outputs {
            let reference = reference(i, &format!("output-{name}"));
            artifact_bytes += output.bytes.len() as u64;
            outputs.insert(
                name.clone(),
                Artifact {
                    artifact_ref: reference.clone(),
                    port: i.step.outputs.get(&name).ok_or(Error::Invalid)?.clone(),
                    bytes: output.bytes.len() as u64,
                    sha256: digest(&output.bytes),
                    rows: Some(output.rows),
                    columns: Some(output.columns),
                    producer_step: i.step.id.clone(),
                    attempt: i.attempt,
                    publication: "confirmed".into(),
                    lease_ref: format!("ref:lease-{}", i.message_id),
                    reports: reports.clone(),
                },
            );
            objects.push(OwnedResource {
                reference,
                bytes: output.bytes,
                consumers: vec![],
                temporary: false,
            });
        }
    }
    let result = StepResult {
        step_id: i.step.id.clone(),
        operation: i.step.operation.clone(),
        operation_version: i.step.operation_version,
        attempt: i.attempt,
        state: state.into(),
        input_manifests: i.inputs.clone(),
        outputs,
        remote_effect: outcome.effect,
        retry,
        error: failure,
        other_errors,
        reports,
        counters: Counters {
            memory_bytes: 128 * crate::MAX_BLOB as u64,
            elapsed_ms,
            rows_read: outcome.rows,
            rows_produced: outcome.produced,
            artifact_bytes,
            spool_bytes: 128 * crate::MAX_BLOB as u64,
            request_bytes,
            result_bytes: RECEIPT_RESERVATION,
            max_attempts: 1,
        },
        started_at: None,
        finished_at: None,
        stop_confirmed: true,
        reason: if state == "succeeded" {
            "completed"
        } else {
            "upstream_error"
        }
        .into(),
        upstream_result: upstream,
    };
    let bytes = encode(&result)?;
    if bytes.len() as u64 > RECEIPT_RESERVATION {
        return Err(Error::Recovery);
    }
    Ok((parse_step_result(&bytes)?, objects))
}
