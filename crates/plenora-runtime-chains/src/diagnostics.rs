//! Opt-in operational facts, without provider messages, identifiers or payloads.
use crate::Error;

/// Closed execution locations used to distinguish unconfirmed effects.
#[derive(Clone, Copy, Debug)]
pub enum ExecutionStage {
    /// Resolve and validate resources before invoking a provider.
    Prepare,
    /// Confirm primary ownership before provider work.
    Acquire,
    /// Drain the original provider operation.
    Provider,
    /// Construct the bounded original receipt.
    Receipt,
    /// Confirm resource/evidence publication on the primary.
    Publish,
    /// Convert an adapter failure at the protected component boundary.
    Component,
    /// Validate and persist the classified chain view.
    Finalize,
}

/// Observe a failure without changing its category or handling.
/// Only closed enums are printable; no source error, request or result is accepted.
#[must_use]
pub fn execution_failure(stage: ExecutionStage, error: Error) -> Error {
    if enabled() {
        eprintln!("Chain execution failure: {}", facts(stage, error));
    }
    error
}

pub(crate) fn enabled() -> bool {
    std::env::var_os("PLENORA_CHAIN_DIAGNOSTICS").is_some_and(|value| value == "1")
}

fn facts(stage: ExecutionStage, error: Error) -> String {
    format!("stage={stage:?}; code={}", error.code())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_is_closed_and_does_not_reclassify_failure() {
        for stage in [
            ExecutionStage::Prepare,
            ExecutionStage::Acquire,
            ExecutionStage::Provider,
            ExecutionStage::Receipt,
            ExecutionStage::Publish,
            ExecutionStage::Component,
            ExecutionStage::Finalize,
        ] {
            for error in [
                Error::Invalid,
                Error::Schema,
                Error::Unsupported,
                Error::Conflict,
                Error::ResourceLimit,
                Error::Timeout,
                Error::Storage,
                Error::Pending,
                Error::Recovery,
                Error::Failed,
                Error::Cancelled,
                Error::Expired,
                Error::NotFound,
            ] {
                assert_eq!(execution_failure(stage, error), error);
                let text = facts(stage, error);
                assert_eq!(text.split(';').count(), 2);
                assert!(text.ends_with(error.code()));
            }
        }
    }
}
