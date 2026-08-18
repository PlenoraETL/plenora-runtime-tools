use plenora_runtime_messaging::MessageId;

use crate::InboxStore;

/// Result of a non-atomic inbox deduplication lookup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeduplicationDecision {
    /// No processed record was found and the handler may proceed.
    Process,
    /// The message was already processed and its effect should be skipped.
    Duplicate,
}

/// Convenience wrapper around an inbox store.
///
/// The check and record operations are intentionally separate because the business
/// transaction belongs to the consumer. A concrete transactional adapter must combine
/// the inbox check, business effect, and processed record atomically to avoid races.
#[derive(Clone, Debug)]
pub struct InboxDeduplicator<S> {
    store: S,
}

impl<S> InboxDeduplicator<S> {
    /// Creates a helper over an inbox store.
    #[must_use]
    pub const fn new(store: S) -> Self {
        Self { store }
    }

    /// Returns the wrapped store.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// Consumes the helper and returns the wrapped store.
    #[must_use]
    pub fn into_inner(self) -> S {
        self.store
    }
}

impl<S> InboxDeduplicator<S>
where
    S: InboxStore,
{
    /// Checks whether processing should proceed.
    ///
    /// # Errors
    ///
    /// Returns the inbox adapter error when the lookup fails.
    pub async fn check(&self, message_id: MessageId) -> Result<DeduplicationDecision, S::Error> {
        if self.store.contains(message_id).await? {
            Ok(DeduplicationDecision::Duplicate)
        } else {
            Ok(DeduplicationDecision::Process)
        }
    }

    /// Records successful processing.
    ///
    /// # Errors
    ///
    /// Returns the inbox adapter error when the record cannot be written.
    pub async fn record_processed(&self, message_id: MessageId) -> Result<(), S::Error> {
        self.store.record_processed(message_id).await
    }
}
