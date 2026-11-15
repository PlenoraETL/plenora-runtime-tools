//! Real provider operations and loss evidence, independent of publication.
use crate::{Error, arrow};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase,
    plan::{ReadOperation, WriteOperation},
    provider::{ParameterBag, Provider, SecretString},
    resource::{ResourceBudget, ResourceLimits},
};
use plenora_database_engine::Engine;
use plenora_runtime_chains::{Invocation, contracts::Port};
use serde_json::Value;
use std::sync::Arc;

pub(crate) enum Operation {
    Read(ReadOperation, ParameterBag, Port),
    Write(WriteOperation, arrow::Input, bool, bool),
}
pub(crate) struct Outcome {
    pub data: Option<Vec<u8>>,
    pub upstream: Value,
    pub error: Option<DatabaseError>,
    pub loss: Option<Value>,
    pub rows: u64,
    pub columns: Option<u64>,
}
fn value<T: serde::Serialize>(value: &T) -> Result<Value, Error> {
    serde_json::to_value(value).map_err(|_| Error::Recovery)
}
impl Outcome {
    pub fn failed(mut error: DatabaseError) -> Result<Self, Error> {
        // Upstream permits names in error messages; the runtime policy does not.
        error.message = "Database operation failed.".into();
        let loss = if let Some(report) = &error.read_loss {
            Some(value(report)?)
        } else {
            error.loss_report.as_ref().map(value).transpose()?
        };
        Ok(Self {
            upstream: value(&error.public_projection())?,
            error: Some(error),
            loss,
            data: None,
            rows: 0,
            columns: None,
        })
    }
}
pub(crate) fn budget(i: &Invocation) -> Result<ResourceBudget, Error> {
    let limits = &i.reservation.limits;
    let memory = limits.memory_bytes / 4;
    // Zero is exhausted capacity, not permission to substitute upstream defaults.
    let rows = if i.step.operation == "database.read" {
        limits.rows_read.min(limits.rows_produced)
    } else {
        limits.rows_read
    };
    let resource = ResourceLimits {
        memory_bytes: memory,
        rows,
        output_bytes: limits.artifact_bytes,
        spill_bytes: limits.spool_bytes,
        duration_ms: limits.elapsed_ms,
        cell_bytes: memory.min(1024 * 1024),
        concurrent_operations: 1,
        columns: 32,
        ..ResourceLimits::default()
    };
    ResourceBudget::new(resource).map_err(|_| Error::ResourceLimit)
}
// Keep settlement and evidence construction together for independent review.
#[allow(clippy::too_many_lines)]
pub(crate) async fn execute(
    provider: Arc<dyn Provider>,
    secret: SecretString,
    operation: Operation,
    i: &Invocation,
    token: &CancellationToken,
) -> Result<Outcome, Error> {
    let budget = budget(i)?;
    let engine = Engine::new(provider.clone(), secret.clone());
    let session = engine.session().map_err(|_| Error::Recovery)?;
    let token = CancellationToken::linked(&[token, &session.cancellation_token()]);
    if token.is_cancelled() {
        return Outcome::failed(DatabaseError::new(
            ErrorCategory::Cancelled,
            ErrorPhase::Validate,
            None,
            "Database invocation cancelled.",
        ));
    }
    match operation {
        Operation::Read(operation, parameters, port) => {
            let mut stream = match provider
                .read(&secret, &operation, &parameters, &budget, &token)
                .await
            {
                Ok(stream) => stream,
                Err(error) => return Outcome::failed(error),
            };
            let mut consumed = 0;
            let result = arrow::encode(
                stream.as_mut(),
                &port,
                &token,
                usize::try_from(
                    i.reservation
                        .limits
                        .artifact_bytes
                        .min(crate::MAX_BLOB as u64),
                )
                .map_err(|_| Error::ResourceLimit)?,
                i.reservation
                    .limits
                    .rows_read
                    .min(i.reservation.limits.rows_produced),
                i.reservation.limits.memory_bytes / 4,
                &mut consumed,
            )
            .await;
            // Capture BEFORE any publication, including failures. None means unmeasured.
            let loss = stream.read_loss();
            if result.is_ok()
                && loss.as_ref().is_some_and(|r| {
                    r.status != plenora_database_core::read_loss::ReadStatus::Complete
                        || !r.entries.is_empty()
                })
            {
                let mut error = DatabaseError::new(
                    ErrorCategory::DataMapping,
                    ErrorPhase::Read,
                    None,
                    "Database read loss rejected.",
                );
                error.read_loss = loss.map(Box::new);
                let mut result = Outcome::failed(error)?;
                result.rows = consumed;
                return Ok(result);
            }
            match result {
                Ok((bytes, rows, columns, batches)) => Ok(Outcome {
                    upstream: serde_json::json!({"path": crate::reference(i, "output"), "batches": batches, "rows": rows, "byte_count": bytes.len(), "checksum_algorithm": "sha256", "checksum": crate::digest(&bytes), "read_loss": loss}),
                    data: Some(bytes),
                    error: None,
                    loss: loss.as_ref().map(value).transpose()?,
                    rows,
                    columns: Some(columns),
                }),
                Err(mut error) => {
                    // The terminal error can carry newer evidence than the stream snapshot.
                    if error.read_loss.is_none() {
                        error.read_loss = loss.map(Box::new);
                    }
                    let mut result = Outcome::failed(error)?;
                    result.rows = consumed;
                    Ok(result)
                }
            }
        }
        Operation::Write(operation, input, consent, reported) => {
            let consumed = input.consumed.clone();
            let input = plenora_database_core::arrow_validation::validated_input(Box::new(input));
            let prepared = match provider
                .prepare_write(&secret, &operation, input.schema(), &budget, &token)
                .await
            {
                Ok(prepared) => prepared,
                Err(error) => return Outcome::failed(error),
            };
            // Bound the static report before effects. Evaluation/effective entries are
            // keyed by this finite risk set in the pinned v3/v4 validator.
            if prepared.loss_report.losses.len() > 128
                || crate::encode(&prepared.loss_report)?.len() > 32 * 1024
                || prepared
                    .execution_loss()
                    .is_some_and(|r| r.risk.len() > 128)
                || prepared
                    .execution_loss()
                    .map(crate::encode)
                    .transpose()?
                    .is_some_and(|b| b.len() > 32 * 1024)
            {
                return Outcome::failed(DatabaseError::resource_limit(
                    "Database evidence reservation exceeded.",
                ));
            }
            if let Some(report) = prepared.execution_loss() {
                // Worst encoding includes evaluation/effective entries and u64::MAX counters.
                // Leave headroom for two copies in an unsettled public document.
                if let Err(error) = report.validate_transport_budget(128 * 1024) {
                    return Outcome::failed(error);
                }
            }
            let risks = !prepared.loss_report.losses.is_empty()
                || prepared
                    .execution_loss()
                    .is_some_and(|r| !r.risk.is_empty());
            if risks && !(reported && consent) {
                let mut error = DatabaseError::new(
                    ErrorCategory::DataMapping,
                    ErrorPhase::Prepare,
                    None,
                    "Database fidelity or consent rejected.",
                );
                error.loss_report =
                    Some(Box::new(prepared.execution_loss().cloned().map_or_else(
                        || prepared.loss_report.clone().into(),
                        Into::into,
                    )));
                return Outcome::failed(error);
            }
            let outcome = match provider
                .write_with_consent(&secret, prepared, input, &budget, &token, consent)
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    let mut result = Outcome::failed(error)?;
                    result.rows = consumed.load(std::sync::atomic::Ordering::Relaxed);
                    return Ok(result);
                }
            };
            let rows = consumed.load(std::sync::atomic::Ordering::Relaxed);
            if outcome.schema_version != 3 {
                let mut error = DatabaseError::new(
                    ErrorCategory::Protocol,
                    ErrorPhase::Finalize,
                    None,
                    "Database outcome version rejected.",
                );
                error.remote_effect = plenora_database_core::RemoteEffect::Unknown;
                error.retry = plenora_database_core::RetryDisposition::RequiresRecovery;
                let mut result = Outcome::failed(error)?;
                result.rows = rows;
                return Ok(result);
            }
            match outcome.settle() {
                Ok(outcome) => {
                    let upstream = value(&outcome)?;
                    Ok(Outcome {
                        loss: outcome.loss_report.as_ref().map(value).transpose()?,
                        upstream,
                        data: None,
                        error: None,
                        rows,
                        columns: None,
                    })
                }
                Err(unsettled) => {
                    let mut result = Outcome::failed(unsettled.error().clone())?;
                    result.upstream = unsettled.public_document().map_err(|_| Error::Recovery)?;
                    result.upstream["message"] = Value::String("Database operation failed.".into());
                    result.loss = unsettled
                        .outcome()
                        .and_then(|o| o.loss_report.as_ref())
                        .map(value)
                        .transpose()?;
                    result.rows = rows;
                    Ok(result)
                }
            }
        }
    }
}
