use std::fmt::{self, Debug, Formatter};

use plenora_runtime_worker::WorkerContext;

/// Adapter job containing only a Plenora worker context and typed payload.
pub struct ApalisJob<T> {
    context: WorkerContext,
    message: T,
}

impl<T> ApalisJob<T> {
    /// Creates a typed adapter job.
    #[must_use]
    pub const fn new(context: WorkerContext, message: T) -> Self {
        Self { context, message }
    }

    /// Returns the Plenora worker context.
    #[must_use]
    pub const fn context(&self) -> &WorkerContext {
        &self.context
    }

    /// Returns the typed payload.
    #[must_use]
    pub const fn message(&self) -> &T {
        &self.message
    }

    /// Consumes the job into its context and payload.
    #[must_use]
    pub fn into_parts(self) -> (WorkerContext, T) {
        (self.context, self.message)
    }
}

impl<T> Debug for ApalisJob<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApalisJob")
            .field("context", &self.context)
            .field("message", &"<redacted>")
            .finish()
    }
}
