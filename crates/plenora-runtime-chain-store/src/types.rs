//! Store-owned authority and data-free failure categories.
use std::{error::Error, fmt};

/// Explicit storage failures; driver messages and source chains never escape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreError {
    /// Invalid configuration or noncanonical invocation.
    Invalid,
    /// The primary or schema could not be verified or reached.
    Unavailable,
    /// COMMIT acknowledgement was not received; do not start or acknowledge work.
    Unconfirmed,
    /// An identity, key, step or immutable result was reused with different data.
    Conflict,
    /// The delivery window ended; the identity can never become new again.
    Expired,
    /// No accepted execution exists.
    Missing,
    /// A stale execution or publication authority was presented.
    StaleFence,
}
impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "chain store {self:?}")
    }
}
impl Error for StoreError {}
impl From<tokio_postgres::Error> for StoreError {
    #[track_caller]
    fn from(error: tokio_postgres::Error) -> Self {
        crate::diagnostics::report(&error);
        Self::Unavailable
    }
}

/// Finite retention, in whole seconds. Persisted reductions cannot be undone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Retention {
    pub(crate) delivery: i64,
    pub(crate) reports: i64,
}
impl Default for Retention {
    fn default() -> Self {
        Self {
            delivery: 604_800,
            reports: 7_776_000,
        }
    }
}
impl Retention {
    /// Restricts the seven-day delivery and ninety-day report policy.
    /// # Errors
    /// Rejects zero, extension, or report retention shorter than delivery.
    pub fn reduced(delivery_seconds: u32, report_seconds: u32) -> Result<Self, StoreError> {
        if delivery_seconds == 0
            || delivery_seconds > 604_800
            || report_seconds < delivery_seconds
            || report_seconds > 7_776_000
        {
            return Err(StoreError::Invalid);
        }
        Ok(Self {
            delivery: i64::from(delivery_seconds),
            reports: i64::from(report_seconds),
        })
    }
}

/// Authority returned only after a start transaction is confirmed.
/// It has no public constructor, cloning, or serialization path.
pub struct StartPermit {
    pub(crate) id: String,
    pub(crate) fence: String,
}
impl fmt::Debug for StartPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StartPermit(<redacted>)")
    }
}
/// Atomic decision for a step; existing work never receives another start permit.
#[derive(Debug)]
pub enum StartDecision {
    /// This caller alone may start the invocation.
    Started(StartPermit),
    /// A start was recorded, with no confirmed outcome; reconcile, never replay blindly.
    RequiresRecovery(plenora_runtime_messaging::MessageId),
    /// A terminal outcome is already authoritative and readable.
    Replay(plenora_runtime_messaging::MessageId),
}

/// Complete admitted invocation; `canonical_input` must include every effective
/// selector, version, destination, input, consent and budget, without normalization.
/// Scope is an authorized run identity chosen by the host, never an untrusted header.
pub struct Invocation<'a> {
    /// Already accepted request identity.
    pub message_id: plenora_runtime_messaging::MessageId,
    /// Tenant/run scope, nonempty and at most 256 ASCII bytes.
    pub scope: &'a str,
    /// Canonical step identifier.
    pub step: &'a str,
    /// Optional operation-supported scoped key; at most 256 bytes.
    pub key: Option<ScopedKey<'a>>,
    /// Complete canonical invocation bytes, bounded at 64 MiB.
    pub canonical_input: &'a [u8],
}

/// Authoritative state surviving process restarts.
#[allow(
    clippy::struct_excessive_bools,
    reason = "Independent execution, retention and delivery evidence axes"
)]
pub struct StoredStep {
    /// Whether a terminal report and outbox were committed together.
    pub finished: bool,
    /// Immutable result identity, if finalized.
    pub result_id: Option<String>,
    /// Complete report, absent only before finish or after explicit report expiry.
    pub report: Option<Vec<u8>>,
    /// The report's retention elapsed.
    pub report_expired: bool,
    /// Confirmed external publication, independent of step completion.
    pub delivered: bool,
    /// Publication is no longer eligible within the delivery window.
    pub delivery_expired: bool,
}
impl fmt::Debug for StoredStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StoredStep(<redacted>)")
    }
}

/// Fenced publication lease. Payload is the exact serialized external envelope.
pub struct Publication {
    pub(crate) id: String,
    pub(crate) fence: String,
    /// Immutable result message identity; use it on every resend.
    pub result_id: String,
    /// Exact bytes persisted by finalization.
    pub payload: Vec<u8>,
}
impl fmt::Debug for Publication {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Publication(<redacted>)")
    }
}

/// Host-authorized idempotency scope, distinct from a run/step identity.
#[derive(Clone, Copy)]
pub struct ScopedKey<'a> {
    /// Tenant and public operation scope; must not contain the generated run ID.
    pub scope: &'a str,
    /// Exact operation-supported key, nonempty and at most 256 bytes.
    pub value: &'a str,
}
