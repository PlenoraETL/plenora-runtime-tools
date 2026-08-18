use std::{
    fmt::{self, Debug, Formatter},
    time::Duration,
};

use plenora_runtime_messaging::RetryDecision;
use plenora_runtime_worker::{WorkerAdmissionReason, WorkerExecutionError};

/// Default broker-native delay used while reversible worker admission is paused.
pub const DEFAULT_PAUSED_ADMISSION_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Action that a concrete source adapter should apply after worker execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApalisDisposition {
    /// Processing completed and the source may acknowledge the job.
    Completed,
    /// Processing should be attempted again after the supplied delay.
    RetryAfter(Duration),
    /// Processing failed and must not be retried automatically.
    DoNotRetry,
    /// Processing failed and should be routed to dead-letter handling.
    DeadLetter,
    /// Processing did not start because shutdown or drain was observed.
    Shutdown(WorkerAdmissionReason),
}

/// Preserved worker failure paired with its adapter disposition.
pub struct ApalisFailure<E> {
    disposition: ApalisDisposition,
    error: WorkerExecutionError<E>,
}

impl<E> ApalisFailure<E> {
    /// Returns the action selected for the source adapter.
    #[must_use]
    pub const fn disposition(&self) -> ApalisDisposition {
        self.disposition
    }

    /// Returns the structured Plenora worker error.
    #[must_use]
    pub const fn error(&self) -> &WorkerExecutionError<E> {
        &self.error
    }

    /// Consumes the failure into its structured Plenora worker error.
    #[must_use]
    pub fn into_error(self) -> WorkerExecutionError<E> {
        self.error
    }
}

impl<E> Debug for ApalisFailure<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApalisFailure")
            .field("disposition", &self.disposition)
            .field("error", &self.error)
            .finish()
    }
}

/// Result returned by the Plenora-to-Apalis service bridge.
pub enum ApalisExecutionOutcome<E> {
    /// The handler completed successfully.
    Completed,
    /// The handler or admission phase failed with an explicit disposition.
    Failed(ApalisFailure<E>),
}

impl<E> ApalisExecutionOutcome<E> {
    pub(crate) fn from_worker_result(result: Result<(), WorkerExecutionError<E>>) -> Self {
        match result {
            Ok(()) => Self::Completed,
            Err(error) => {
                let disposition = disposition_for(&error);
                Self::Failed(ApalisFailure { disposition, error })
            }
        }
    }

    /// Returns the source-adapter disposition.
    #[must_use]
    pub const fn disposition(&self) -> ApalisDisposition {
        match self {
            Self::Completed => ApalisDisposition::Completed,
            Self::Failed(failure) => failure.disposition(),
        }
    }

    /// Returns failure details when processing did not complete.
    #[must_use]
    pub const fn failure(&self) -> Option<&ApalisFailure<E>> {
        match self {
            Self::Completed => None,
            Self::Failed(failure) => Some(failure),
        }
    }

    /// Consumes the outcome into failure details when present.
    #[must_use]
    pub fn into_failure(self) -> Option<ApalisFailure<E>> {
        match self {
            Self::Completed => None,
            Self::Failed(failure) => Some(failure),
        }
    }
}

impl<E> Debug for ApalisExecutionOutcome<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed => formatter.write_str("ApalisExecutionOutcome::Completed"),
            Self::Failed(failure) => formatter
                .debug_tuple("ApalisExecutionOutcome::Failed")
                .field(failure)
                .finish(),
        }
    }
}

fn disposition_for<E>(error: &WorkerExecutionError<E>) -> ApalisDisposition {
    if let Some(reason) = error.admission_reason() {
        if reason == WorkerAdmissionReason::Paused {
            return ApalisDisposition::RetryAfter(DEFAULT_PAUSED_ADMISSION_RETRY_DELAY);
        }
        return ApalisDisposition::Shutdown(reason);
    }

    match error.retry_decision() {
        Some(RetryDecision::RetryAfter(delay)) => ApalisDisposition::RetryAfter(delay),
        Some(RetryDecision::DoNotRetry) | None => ApalisDisposition::DoNotRetry,
        Some(RetryDecision::DeadLetter) => ApalisDisposition::DeadLetter,
    }
}
