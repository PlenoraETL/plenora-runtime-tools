//! Closed public chain documents and preventive validation against fixture discovery.
//!
//! All admission entry points recheck constructed DTOs. Parsing checks lexical and
//! structural invariants, validation adds graph/evidence semantics. A successful
//! report is reference-only: payloads, authenticated authorization, real Arrow
//! fidelity, resource reservations and unchanged discovery must be checked by the
//! host before invoke. This crate contains no resolver, executor or I/O client.
//!
//! Wire parsing cannot bypass duplicate/numeric admission with serde:
//! ```compile_fail
//! let plan: plenora_runtime_chain_contracts::Plan = serde_json::from_str("{}").unwrap();
//! ```
mod codec;
mod error;
mod outcome;
mod plan;
mod schema;
pub mod types;

use error::Checked;
pub use error::{Code, Rejection};
pub use plan::{DeferredSchema, FidelityVerdict, ValidationReport};
use serde::Serialize;
use serde_json::Value;
pub use types::*;

fn value<T: Serialize>(document: &T) -> Checked<Value> {
    serde_json::to_value(document).map_err(|_| Code::Document.into())
}
trait Decode: Sized {
    fn decode(value: Value) -> Checked<Self>;
}
fn parse<T: Decode>(raw: &[u8], file: &str) -> Checked<T> {
    let value = codec::parse(raw)?;
    schema::structure(&value, file)?;
    T::decode(value)
}
fn structure<T: Serialize>(document: &T, file: &str) -> Checked<Value> {
    let value = value(document)?;
    codec::canonical(&value)?;
    schema::structure(&value, file)?;
    Ok(value)
}

/// Parse a closed plan without claiming semantic admission.
/// # Errors
/// Returns a data-free lexical, size or structural rejection.
pub fn parse_plan(raw: &[u8]) -> Checked<Plan> {
    parse(raw, "chain-plan-v2.schema.json")
}
/// Parse a closed fixture registry, supplied independently by the caller.
/// # Errors
/// Returns a data-free lexical, size or structural rejection.
pub fn parse_registry(raw: &[u8]) -> Checked<Registry> {
    parse(raw, "reference-context.schema.json")
}
/// Parse a closed result; use `validate_result` for reconciliation with its plan.
/// # Errors
/// Returns a data-free lexical, size or structural rejection.
pub fn parse_result(raw: &[u8]) -> Checked<ChainResult> {
    parse(raw, "chain-result-v2.schema.json")
}
/// Parse a closed event without attesting sequence history.
/// # Errors
/// Returns a data-free lexical, size or structural rejection.
pub fn parse_event(raw: &[u8]) -> Checked<Event> {
    parse(raw, "chain-event-v2.schema.json")
}
/// Parse a closed error without exposing source parser diagnostics.
/// # Errors
/// Returns a data-free lexical, size or structural rejection.
pub fn parse_error(raw: &[u8]) -> Checked<ChainError> {
    parse(raw, "chain-error-v2.schema.json")
}

/// Compute CHAIN-C14N-1 SHA-256 after checking the complete plan structure.
/// # Errors
/// Rejects constructed plans outside the closed lexical/structural contract.
pub fn plan_digest(plan: &Plan) -> Checked<String> {
    codec::digest(&structure(plan, "chain-plan-v2.schema.json")?)
}
/// Emit canonical plan bytes; array order and all optional-field presence are preserved.
/// # Errors
/// Rejects constructed plans outside the closed lexical/structural contract.
pub fn canonical_plan(plan: &Plan) -> Checked<Vec<u8>> {
    codec::canonical(&structure(plan, "chain-plan-v2.schema.json")?)
}
/// Compute the scoped input digest required by a loss grant.
/// # Errors
/// Rejects malformed plans or a step not belonging exactly to the plan.
pub fn consent_input_digest(plan: &Plan, step: &Step) -> Checked<String> {
    structure(plan, "chain-plan-v2.schema.json")?;
    error::ensure(plan.steps.contains(step), Code::Reference)?;
    plan::input_digest(step, plan)
}
/// Validate every static invariant against an independently supplied fixture registry.
/// No resolver or invoke can run in this API. Dynamic schema/fidelity remain explicit.
/// # Errors
/// Returns the fixed category/validate/code for the first incompatible invariant.
pub fn validate_plan(plan: &Plan, registry: &Registry) -> Checked<ValidationReport> {
    structure(plan, "chain-plan-v2.schema.json")?;
    structure(registry, "reference-context.schema.json")?;
    plan::semantics(plan, registry)
}
/// Reconcile a terminal result with a statically valid plan and registry.
/// # Errors
/// Rejects inconsistent identities, effects, retries, manifests, counters or reports.
pub fn validate_result(result: &ChainResult, plan: &Plan, registry: &Registry) -> Checked<()> {
    structure(result, "chain-result-v2.schema.json")?;
    let validated = validate_plan(plan, registry)?;
    outcome::result(result, plan, &validated)
}
/// Validate a single event; durable sequence/deduplication belongs to the store.
/// # Errors
/// Rejects inconsistent kind, state, step, attempt, result reference or error.
pub fn validate_event(event: &Event) -> Checked<()> {
    structure(event, "chain-event-v2.schema.json")?;
    outcome::event(event)
}
/// Validate error evidence and conservative retry/effect axes.
/// # Errors
/// Rejects contradictory reports or unsafe retry classifications.
pub fn validate_error(error: &ChainError) -> Checked<()> {
    structure(error, "chain-error-v2.schema.json")?;
    outcome::error(error)
}
