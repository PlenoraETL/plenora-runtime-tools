use std::sync::Arc;

use plenora_runtime_core::{Clock, ShutdownSignal};
use plenora_runtime_messaging::{CausationId, CorrelationId, MessageId, MessageMetadata};

use crate::{
    TaskCancellationToken, TaskLifecycleObserver, TaskProgress, TaskProgressError,
    TaskProgressReporter, WorkerCancellationReason,
};

/// Stable identities decoded before a worker invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerContextIdentity {
    /// Identity of the message being processed.
    pub message_id: MessageId,
    /// Identity shared by the correlated operation.
    pub correlation_id: CorrelationId,
    /// Identity of the direct causal message, when present.
    pub causation_id: Option<CausationId>,
}

impl WorkerContextIdentity {
    /// Creates a worker identity set.
    #[must_use]
    pub const fn new(
        message_id: MessageId,
        correlation_id: CorrelationId,
        causation_id: Option<CausationId>,
    ) -> Self {
        Self {
            message_id,
            correlation_id,
            causation_id,
        }
    }
}

/// Message and lifecycle context supplied to one handler invocation.
#[derive(Clone, Debug)]
pub struct WorkerContext {
    /// Identity of the message being processed.
    pub message_id: MessageId,
    /// Identity shared by the correlated operation.
    pub correlation_id: CorrelationId,
    /// Identity of the message that directly caused this message.
    pub causation_id: Option<CausationId>,
    /// One-based delivery attempt reported by the source adapter.
    pub attempt: u32,
    /// Namespaced message metadata. Values remain redacted by its Debug implementation.
    pub metadata: MessageMetadata,
    /// Cooperative process shutdown signal.
    pub shutdown: ShutdownSignal,
    /// Cooperative cancellation scoped to this delivery attempt.
    pub cancellation: TaskCancellationToken,
    /// Payload-free progress and task-heartbeat reporter.
    pub progress: TaskProgressReporter,
}

impl WorkerContext {
    /// Creates a context for a delivery attempt.
    #[must_use]
    pub fn new(
        message_id: MessageId,
        correlation_id: CorrelationId,
        causation_id: Option<CausationId>,
        attempt: u32,
        metadata: MessageMetadata,
        shutdown: ShutdownSignal,
    ) -> Self {
        let cancellation = TaskCancellationToken::new();
        let progress = TaskProgressReporter::noop(message_id, correlation_id, attempt);
        Self {
            message_id,
            correlation_id,
            causation_id,
            attempt,
            metadata,
            shutdown,
            cancellation,
            progress,
        }
    }

    /// Creates a context from decoded identities and delivery-specific fields.
    #[must_use]
    pub fn from_identity(
        identity: WorkerContextIdentity,
        attempt: u32,
        metadata: MessageMetadata,
        shutdown: ShutdownSignal,
    ) -> Self {
        Self::new(
            identity.message_id,
            identity.correlation_id,
            identity.causation_id,
            attempt,
            metadata,
            shutdown,
        )
    }

    /// Replaces the default task token with an adapter-owned shared token.
    #[must_use]
    pub fn with_cancellation(mut self, cancellation: TaskCancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }

    /// Reports validated numeric task progress.
    ///
    /// # Errors
    ///
    /// Returns an error after terminal completion or sequence exhaustion.
    pub fn report_progress(&self, progress: TaskProgress) -> Result<(), TaskProgressError> {
        self.progress.report(progress)
    }

    /// Waits for either process shutdown or task-local cancellation.
    pub async fn cancelled(&self) -> WorkerCancellationReason {
        tokio::select! {
            biased;
            reason = self.cancellation.cancelled() => WorkerCancellationReason::Task(reason),
            () = self.shutdown.cancelled() => WorkerCancellationReason::RuntimeShutdown,
        }
    }

    pub(crate) fn install_lifecycle(
        &mut self,
        observer: Arc<dyn TaskLifecycleObserver>,
        clock: Arc<dyn Clock>,
    ) -> TaskProgressReporter {
        let progress = TaskProgressReporter::start(
            self.message_id,
            self.correlation_id,
            self.attempt,
            observer,
            clock,
        );
        self.progress = progress.clone();
        progress
    }
}
