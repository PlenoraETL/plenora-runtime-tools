//! Closed facade dispatch. Bindings never duplicate orchestration.
use crate::{ChainEngine, Error, RunOptions, Store, contracts::parse_plan};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Functions requiring independent evidence on each public surface.
pub const FUNCTIONS: &[&str] = &[
    "capabilities",
    "validate",
    "submit",
    "run",
    "status",
    "result",
    "events",
    "cancel",
    "resume",
    "artifact",
    "report",
];

/// Closed transport-neutral call; raw plan JSON preserves duplicate-member admission.
#[derive(Serialize, Deserialize)]
#[serde(tag = "function", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    /// Effective registry.
    Capabilities,
    /// Preventive validation.
    Validate {
        /// Exact source document.
        plan: String,
    },
    /// Durable submission.
    Submit {
        /// Exact source document.
        plan: String,
        /// Immutable controls.
        options: RunOptions,
    },
    /// Submit and drive the same engine.
    Run {
        /// Exact source document.
        plan: String,
        /// Immutable controls.
        options: RunOptions,
    },
    /// Authoritative state.
    Status {
        /// Canonical run UUID.
        run_id: String,
    },
    /// Terminal outcome, including failures.
    Result {
        /// Canonical run UUID.
        run_id: String,
    },
    /// Durable cursor page.
    Events {
        /// Canonical run UUID.
        run_id: String,
        /// Last consumed sequence.
        cursor: u64,
        /// Finite page bound.
        limit: usize,
    },
    /// Durable stop request.
    Cancel {
        /// Canonical run UUID.
        run_id: String,
        /// Exact state revision.
        revision: u64,
    },
    /// Authenticated reconciliation.
    Resume {
        /// Canonical run UUID.
        run_id: String,
        /// Exact state revision.
        revision: u64,
    },
    /// Bounded authorized artifact retrieval.
    Artifact {
        /// Opaque reference.
        reference: String,
    },
    /// Complete original adapter evidence.
    Report {
        /// Canonical run UUID.
        run_id: String,
    },
}
/// Identical facade envelope; failed runs retain the complete domain result in `value`.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    /// Complete document, or null when unavailable.
    pub value: Value,
    /// Data-free coordination failure.
    pub error: Option<Error>,
}
impl<S: Store + plenora_runtime_boundary::DurableExecutionStore> ChainEngine<S> {
    /// Dispatch through the shared engine, retaining domain failures separately from transport errors.
    pub async fn execute(&self, request: Request) -> Response {
        let is_run = matches!(request, Request::Run { .. });
        match self.dispatch(request).await {
            Ok(value) => {
                let error = if is_run {
                    match value["state"].as_str() {
                        Some("succeeded") => None,
                        Some("cancelled") => Some(Error::Cancelled),
                        Some("requires_recovery") => Some(Error::Recovery),
                        Some("rejected") => Some(Error::Invalid),
                        _ => Some(Error::Failed),
                    }
                } else {
                    None
                };
                Response { value, error }
            }
            Err(error) => Response {
                value: Value::Null,
                error: Some(error),
            },
        }
    }
    async fn dispatch(&self, request: Request) -> Result<Value, Error> {
        let value = match request {
            Request::Capabilities => serde_json::to_value(self.capabilities()),
            Request::Validate { plan } => {
                let r = self.validate(&parse_plan(plan.as_bytes())?)?;
                return Ok(
                    json!({"order":r.order,"deferred":r.deferred,"plan_digest":r.plan_digest,"admission":r.admission()}),
                );
            }
            Request::Submit { plan, options } => {
                serde_json::to_value(self.submit(&parse_plan(plan.as_bytes())?, options).await?)
            }
            Request::Run { plan, options } => {
                serde_json::to_value(self.run(&parse_plan(plan.as_bytes())?, options).await?)
            }
            Request::Status { run_id } => serde_json::to_value(self.status(&run_id).await?),
            Request::Result { run_id } => serde_json::to_value(self.result(&run_id).await?),
            Request::Events {
                run_id,
                cursor,
                limit,
            } => serde_json::to_value(self.events(&run_id, cursor, limit).await?),
            Request::Cancel { run_id, revision } => {
                serde_json::to_value(self.cancel(&run_id, revision).await?)
            }
            Request::Resume { run_id, revision } => {
                serde_json::to_value(self.resume(&run_id, revision).await?)
            }
            Request::Artifact { reference } => {
                serde_json::to_value(self.artifact(&reference).await?)
            }
            Request::Report { run_id } => serde_json::to_value(self.report(&run_id).await?),
        };
        value.map_err(|_| Error::Invalid)
    }
}
