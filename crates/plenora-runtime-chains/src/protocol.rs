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

/// Explicit recovery authority: observation never drives work; continuation rejects active intents.
#[derive(Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Recovery {
    /// Query the current revision/state without invoking or reconciling any provider.
    Observe,
    /// Reconcile only through trusted provider evidence, then follow the persisted policy.
    #[default]
    Reconcile,
    /// Advance known stopped work only; unknown or active work is rejected.
    Continue,
}

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
        /// Explicit provider reconciliation policy; omitted means reconcile for compatibility.
        #[serde(default)]
        recovery: Recovery,
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
        /// Optional bounded byte cursor; absence preserves the complete legacy response.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<crate::ReportCursor>,
    },
}
/// Identical facade envelope; failed runs retain the complete domain result in `value`.
pub struct Response {
    /// Complete document, or null when unavailable.
    pub value: Value,
    /// Data-free coordination failure.
    pub error: Option<Error>,
}
impl<S: Store + plenora_runtime_boundary::DurableExecutionStore> ChainEngine<S> {
    /// Dispatch through the shared engine, retaining domain failures separately from transport errors.
    pub async fn execute(&self, request: Request) -> Response {
        if let Request::Run { plan, options } = request {
            return self.execute_run(&plan, options).await;
        }
        let run = match &request {
            Request::Status { run_id }
            | Request::Result { run_id }
            | Request::Events { run_id, .. }
            | Request::Cancel { run_id, .. }
            | Request::Resume { run_id, .. }
            | Request::Report { run_id, .. } => Some(run_id.clone()),
            _ => None,
        };
        match self.dispatch(request).await {
            Ok(value) => Response { value, error: None },
            Err(error) => {
                let value = match run {
                    Some(run) => self.error_context(&run, None).await,
                    None => Value::Null,
                };
                Response {
                    value,
                    error: Some(error),
                }
            }
        }
    }
    async fn execute_run(&self, source: &str, options: RunOptions) -> Response {
        let Ok(plan) = parse_plan(source.as_bytes()) else {
            return Response {
                value: Value::Null,
                error: Some(Error::Invalid),
            };
        };
        let handle = match self.submit(&plan, options).await {
            Ok(handle) => handle,
            Err(error) => {
                return Response {
                    value: Value::Null,
                    error: Some(error),
                };
            }
        };
        match self.wait_run(&handle).await {
            Ok(result) => {
                let error = match result.state.as_str() {
                    "succeeded" => None,
                    "cancelled" => Some(Error::Cancelled),
                    "requires_recovery" => Some(Error::Recovery),
                    "rejected" => Some(Error::Invalid),
                    _ => Some(Error::Failed),
                };
                match serde_json::to_value(result) {
                    Ok(value) => Response { value, error },
                    Err(_) => Response {
                        value: self.error_context(&handle.run_id, Some(&handle)).await,
                        error: Some(Error::Invalid),
                    },
                }
            }
            Err(error) => {
                // A confirmed alias receipt is never replaced by the caller's candidate run id.
                Response {
                    value: self.error_context(&handle.run_id, Some(&handle)).await,
                    error: Some(error),
                }
            }
        }
    }
    async fn error_context(&self, run: &str, fallback: Option<&crate::RunHandle>) -> Value {
        if let Ok(journal) = self.load(run).await
            && let Ok(handle) = journal.handle()
            && let Ok(observed) = crate::contracts::parse_result(&journal.view)
        {
            // The last classified view is evidence, not a claim that an active provider stopped.
            let effect = if journal.active.is_some() {
                "unknown"
            } else {
                &observed.remote_effect
            };
            let retry = if effect == "unknown" {
                json!({"kind":"requires_recovery"})
            } else {
                json!(observed.retry)
            };
            return json!({"run_id":handle.run_id,"revision":handle.revision,"state":handle.state,
                "remote_effect":effect,"retry":retry,"observed_result":observed});
        }
        fallback.map_or(Value::Null, |handle| json!({"run_id":handle.run_id,"revision":handle.revision,
            "state":handle.state,"remote_effect":"unknown","retry":{"kind":"requires_recovery"},"observed_result":null}))
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
            Request::Run { .. } => return Err(Error::Invalid),
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
            Request::Resume {
                run_id,
                revision,
                recovery,
            } => serde_json::to_value(self.resume_controlled(&run_id, revision, recovery).await?),
            Request::Artifact { reference } => {
                serde_json::to_value(self.artifact(&reference).await?)
            }
            Request::Report { run_id, cursor } => match cursor {
                Some(cursor) => serde_json::to_value(self.report_page(&run_id, cursor).await?),
                None => serde_json::to_value(self.report(&run_id).await?),
            },
        };
        value.map_err(|_| Error::Invalid)
    }
}
