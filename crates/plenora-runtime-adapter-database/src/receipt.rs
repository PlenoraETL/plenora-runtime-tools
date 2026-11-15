//! Complete upstream documents remain retrievable, including uncertain effects.
use crate::{Error, Invocation, OwnedResource, digest, encode, operation::Outcome, reference};
use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition,
};
use plenora_runtime_chains::contracts::{
    Artifact, ChainError, Counters, Document, Report, Retry, Retry0, Retry1, StepResult,
    parse_step_result,
};
use std::collections::BTreeMap;

pub(crate) const RECEIPT_RESERVATION: u64 = 64 * 1024;
fn axis(value: &impl serde::Serialize) -> Result<String, Error> {
    serde_json::to_value(value)
        .map_err(|_| Error::Recovery)?
        .as_str()
        .map(str::to_owned)
        .ok_or(Error::Recovery)
}
fn retry(value: RetryDisposition) -> Result<Retry, Error> {
    let value = serde_json::to_value(value).map_err(|_| Error::Recovery)?;
    let kind = value
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .ok_or(Error::Recovery)?
        .to_owned();
    Ok(
        if let Some(delay_ms) = value.get("delay_ms").and_then(serde_json::Value::as_u64) {
            Retry::Variant1(Retry1 { kind, delay_ms })
        } else {
            Retry::Variant0(Retry0 { kind })
        },
    )
}
// Keep settlement and evidence construction together for independent review.
#[allow(clippy::too_many_lines)]
pub(crate) fn build(
    i: &Invocation,
    mut outcome: Outcome,
    request_bytes: u64,
    elapsed_ms: u64,
) -> Result<(StepResult, Vec<OwnedResource>), Error> {
    let original_error = outcome.error.is_some();
    if outcome.loss.is_none() && outcome.error.is_none() {
        let mut error = DatabaseError::new(
            ErrorCategory::Protocol,
            ErrorPhase::Finalize,
            None,
            "Database loss evidence unavailable.",
        );
        error.remote_effect = if i.step.operation == "database.write" {
            RemoteEffect::Committed
        } else {
            RemoteEffect::None
        };
        outcome.error = Some(error);
    }
    let upstream_bytes = encode(&outcome.upstream)?;
    let upstream = Document {
        contract: if original_error {
            "plenora-error-v1".into()
        } else {
            i.step.output_contract.clone()
        },
        artifact_ref: reference(i, "upstream"),
        sha256: digest(&upstream_bytes),
    };
    // The report points to the REAL result/error containing the original read_loss/loss_report.
    // No private contract identifier or downgraded report is invented.
    let reports = vec![Report {
        kind: "loss_report".into(),
        availability: if outcome.loss.is_some() {
            "available"
        } else {
            "unavailable"
        }
        .into(),
        document: outcome.loss.as_ref().map(|_| upstream.clone()),
    }];
    let mut objects = vec![OwnedResource {
        reference: upstream.artifact_ref.clone(),
        bytes: upstream_bytes,
        consumers: vec![],
        temporary: false,
    }];
    let effect = outcome.error.as_ref().map_or_else(
        || {
            Ok(if i.step.operation == "database.write" {
                "committed".into()
            } else {
                "none".into()
            })
        },
        |e| axis(&e.remote_effect),
    )?;
    let retry = retry(
        outcome
            .error
            .as_ref()
            .map_or(RetryDisposition::Never, |e| e.retry),
    )?;
    let error = outcome
        .error
        .as_ref()
        .map(|error| -> Result<_, Error> {
            Ok(ChainError {
                contract: "plenora-chain-error-v2".into(),
                schema_version: 2,
                category: axis(&error.category)?,
                phase: axis(&error.phase)?,
                code: "CHAIN_UPSTREAM".into(),
                remote_effect: effect.clone(),
                retry: retry.clone(),
                message: "Chain operation rejected or incomplete.".into(),
                step_id: Some(i.step.id.clone()),
                upstream_error: original_error.then(|| upstream.clone()),
                reports: reports.clone(),
            })
        })
        .transpose()?;
    let state = match (effect.as_str(), outcome.error.as_ref().map(|e| e.category)) {
        ("unknown", _) => "requires_recovery",
        ("partial", _) => "partial",
        (_, Some(ErrorCategory::Cancelled)) => "cancelled",
        (_, Some(_)) => "failed",
        _ => "succeeded",
    };
    let mut outputs = BTreeMap::new();
    let mut artifact_bytes = 0;
    if state == "succeeded"
        && let Some(bytes) = outcome.data
    {
        let reference = reference(i, "output");
        artifact_bytes = bytes.len() as u64;
        outputs.insert(
            "data".into(),
            Artifact {
                artifact_ref: reference.clone(),
                port: i.step.outputs.get("data").ok_or(Error::Invalid)?.clone(),
                bytes: artifact_bytes,
                sha256: digest(&bytes),
                rows: Some(outcome.rows),
                columns: outcome.columns,
                producer_step: i.step.id.clone(),
                attempt: i.attempt,
                publication: "confirmed".into(),
                lease_ref: format!("ref:lease-{}", i.message_id),
                reports: reports.clone(),
            },
        );
        objects.push(OwnedResource {
            reference,
            bytes,
            consumers: vec![],
            temporary: false,
        });
    }
    let result = StepResult {
        step_id: i.step.id.clone(),
        operation: i.step.operation.clone(),
        operation_version: i.step.operation_version,
        attempt: i.attempt,
        state: state.into(),
        input_manifests: i.inputs.clone(),
        outputs,
        remote_effect: effect,
        retry,
        error,
        other_errors: vec![],
        reports,
        counters: Counters {
            memory_bytes: 32 * crate::MAX_BLOB as u64,
            elapsed_ms,
            rows_read: outcome.rows,
            rows_produced: if i.step.operation == "database.read" {
                outcome.rows
            } else {
                0
            },
            artifact_bytes,
            spool_bytes: 32 * crate::MAX_BLOB as u64,
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
        upstream_result: Some(upstream),
    };
    let bytes = encode(&result)?;
    if bytes.len() as u64 > RECEIPT_RESERVATION {
        return Err(Error::Recovery);
    }
    Ok((parse_step_result(&bytes)?, objects))
}
