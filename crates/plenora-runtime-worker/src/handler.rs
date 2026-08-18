use std::error::Error;

use async_trait::async_trait;

use crate::WorkerContext;

/// Engine-neutral asynchronous message handler.
#[async_trait]
pub trait WorkerHandler<T>: Send + Sync
where
    T: Send,
{
    /// Handler-specific error preserved by worker execution.
    type Error: Error + Send + Sync + 'static;

    /// Processes one typed message.
    ///
    /// Implementations should observe [`WorkerContext::cancelled`] during long-running operations,
    /// forward the task token into cooperative child operations, and report only bounded numeric
    /// progress. Retry is deliberately outside this method and is delegated to an injected
    /// messaging retry policy.
    ///
    /// # Errors
    ///
    /// Returns the handler-specific processing failure.
    async fn handle(&self, ctx: WorkerContext, message: T) -> Result<(), Self::Error>;
}
