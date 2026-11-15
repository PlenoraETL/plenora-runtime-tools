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
