use std::error::Error;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use plenora_runtime_messaging::{MessageId, SerializedMessage};

use crate::{IdempotencyKey, OutboxId, RequestFingerprint};

/// One durable message awaiting broker publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxEntry {
    /// Store-owned record identity.
    pub id: OutboxId,
    /// Broker-neutral serialized message.
    pub message: SerializedMessage,
    /// Time at which the record was created transactionally.
    pub created_at: DateTime<Utc>,
    /// Number of publication attempts already started by the store.
    pub attempts: u32,
}

/// Category of a broker publication failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublishFailureKind {
    /// The producer returned an adapter error before an outcome was available.
    ProducerError,
    /// The producer could not determine the remote effect.
    OutcomeUnknown,
}

/// Persistence disposition requested after a publication failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureDisposition {
    /// Make the entry eligible for a later relay run.
    Retry,
    /// Remove the entry from automatic relay and await reconciliation.
    Reconcile,
    /// Mark the entry as permanently failed.
    Terminal,
}

/// Store-neutral failure information recorded for an outbox entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublishFailure {
    kind: PublishFailureKind,
    disposition: FailureDisposition,
}

impl PublishFailure {
    /// Describes a producer adapter error.
    #[must_use]
    pub const fn producer_error(disposition: FailureDisposition) -> Self {
        Self {
            kind: PublishFailureKind::ProducerError,
            disposition,
        }
    }

    /// Describes a publication with unknown remote effect.
    #[must_use]
    pub const fn outcome_unknown(disposition: FailureDisposition) -> Self {
        Self {
            kind: PublishFailureKind::OutcomeUnknown,
            disposition,
        }
    }

    /// Returns the failure category.
    #[must_use]
    pub const fn kind(self) -> PublishFailureKind {
        self.kind
    }

    /// Returns the requested persistence disposition.
    #[must_use]
    pub const fn disposition(self) -> FailureDisposition {
        self.disposition
    }
}

/// Persistence boundary for transactional outbox records.
#[async_trait]
pub trait OutboxStore: Send + Sync {
    /// Concrete persistence adapter error.
    type Error: Error + Send + Sync + 'static;

    /// Claims at most limit pending entries for exclusive publication.
    ///
    /// A concrete store must define bounded claim leases or crash recovery. The returned
    /// attempt counter should include the claim being returned.
    ///
    /// # Errors
    ///
    /// Returns a persistence error when entries cannot be claimed.
    async fn claim_pending(&self, limit: usize) -> Result<Vec<OutboxEntry>, Self::Error>;

    /// Records confirmed broker publication.
    ///
    /// # Errors
    ///
    /// Returns a persistence error when the entry cannot be updated.
    async fn mark_published(&self, id: OutboxId) -> Result<(), Self::Error>;

    /// Records a failed or uncertain publication and its next disposition.
    ///
    /// # Errors
    ///
    /// Returns a persistence error when the entry cannot be updated.
    async fn mark_failed(&self, id: OutboxId, failure: PublishFailure) -> Result<(), Self::Error>;
}

/// Persistence boundary for processed-message deduplication.
#[async_trait]
pub trait InboxStore: Send + Sync {
    /// Concrete persistence adapter error.
    type Error: Error + Send + Sync + 'static;

    /// Returns whether a message was already processed.
    ///
    /// # Errors
    ///
    /// Returns a persistence error when the lookup fails.
    async fn contains(&self, message_id: MessageId) -> Result<bool, Self::Error>;

    /// Records successful processing of a message.
    ///
    /// # Errors
    ///
    /// Returns a persistence error when the record cannot be written.
    async fn record_processed(&self, message_id: MessageId) -> Result<(), Self::Error>;
}

/// Decision made when beginning an idempotent operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdempotencyDecision {
    /// The caller owns execution of the new operation.
    Execute,
    /// The same operation completed and its stored result should be returned.
    ReturnStoredResult,
    /// The key was reused with a different request fingerprint.
    Conflict,
    /// The same operation is currently being executed.
    InProgress,
}

/// Persistence boundary for request idempotency coordination.
#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    /// Concrete persistence adapter error.
    type Error: Error + Send + Sync + 'static;

    /// Atomically begins an operation or reports the existing state.
    ///
    /// # Errors
    ///
    /// Returns a persistence error when the decision cannot be made atomically.
    async fn begin(
        &self,
        key: IdempotencyKey,
        fingerprint: RequestFingerprint,
    ) -> Result<IdempotencyDecision, Self::Error>;
}
