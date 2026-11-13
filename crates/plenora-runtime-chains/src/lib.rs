//! Sequential coordination over a primary durable journal and trusted adapter evidence.
//! No library/provider is qualified by the scripted adapter profile.
//!
//! Decoding stored bytes cannot mint invocation or transition authority:
//! ```compile_fail
//! fn forged(journal: plenora_runtime_chains::Journal) {
//!     let _ = plenora_runtime_chains::Change(journal);
//! }
//! ```
//! ```compile_fail
//! fn forged(journal: plenora_runtime_chains::Journal) {
//!     let _ = plenora_runtime_chains::Submission(journal);
//! }
//! ```
//! The concrete step executor cannot be supplied or replaced by callers:
//! ```compile_fail
//! use plenora_runtime_chains::execution::StepExecutor;
//! ```
//! A chain store alone cannot construct an engine without the durable runtime store contract:
//! ```compile_fail
//! use plenora_runtime_chains::{ChainEngine, Store};
//! fn bypass<S: Store>(_: ChainEngine<S>) {}
//! ```
#![forbid(unsafe_code)]

mod budget;
mod client;
mod engine;
mod execution;
mod journal;
mod protocol;

pub use budget::Reservation;
pub use client::{ChainClient, EventCursor, Transport};
pub use engine::{Adapter, ChainEngine, Invocation};
pub use journal::{
    Change, DELIVERY_MS, JOURNAL_BYTES, Journal, REPORT_MS, RunHandle, RunOptions, Store,
    Submission,
};
pub use plenora_runtime_chain_contracts as contracts;
pub use protocol::{FUNCTIONS, Request, Response};

use serde::{Deserialize, Serialize};
use std::fmt;

/// Data-free coordination failure. Domain outcomes remain available through `result`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Error {
    /// Closed admission failed.
    Invalid,
    /// Unsupported execution profile or control.
    Unsupported,
    /// Identity, immutable binding or state revision disagrees.
    Conflict,
    /// A cumulative reservation cannot be met.
    ResourceLimit,
    /// The original absolute deadline has elapsed.
    Timeout,
    /// Durable confirmation could not be obtained.
    Storage,
    /// No terminal result exists yet.
    Pending,
    /// Invocation may have started; reconciliation is required.
    Recovery,
    /// A complete known domain failure; the facade retains its result.
    Failed,
    /// A complete result confirms cancellation.
    Cancelled,
    /// The permanent identity exists but its delivery/report window elapsed.
    Expired,
    /// Tenant-authorized identity was not found.
    NotFound,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "chain coordination: {self:?}")
    }
}
impl std::error::Error for Error {}
impl From<contracts::Rejection> for Error {
    fn from(_: contracts::Rejection) -> Self {
        Self::Invalid
    }
}

pub(crate) fn encode(value: &impl Serialize) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(value).map_err(|_| Error::Invalid)
}
pub(crate) fn uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|id| id.hyphenated().to_string() == value)
}
pub(crate) fn scope(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.bytes().all(|b| b.is_ascii_graphic())
}
pub(crate) fn timestamp(ms: u64) -> Result<String, Error> {
    chrono::DateTime::from_timestamp_millis(i64::try_from(ms).map_err(|_| Error::Invalid)?)
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .ok_or(Error::Invalid)
}
