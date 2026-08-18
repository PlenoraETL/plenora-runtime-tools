use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

use plenora_runtime_messaging::{MessageProducer, PublishOutcome};

use crate::{FailureDisposition, OutboxId, OutboxStore, PublishFailure};

/// Store operation during which a relay run failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelayStoreOperation {
    /// Claiming a pending batch.
    ClaimPending,
    /// Recording a confirmed publication.
    MarkPublished,
    /// Recording a publication failure or uncertain outcome.
    MarkFailed,
}

/// Explicit policy for publication failures and uncertain remote effects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelayPolicy {
    producer_error: FailureDisposition,
    outcome_unknown: FailureDisposition,
}

impl RelayPolicy {
    /// Creates a publication policy.
    ///
    /// Selecting Retry for `outcome_unknown` is an explicit opt-in to possible duplicate
    /// publication. Reconcile is the safe default.
    #[must_use]
    pub const fn new(
        producer_error: FailureDisposition,
        outcome_unknown: FailureDisposition,
    ) -> Self {
        Self {
            producer_error,
            outcome_unknown,
        }
    }

    /// Returns the disposition used for producer adapter errors.
    #[must_use]
    pub const fn producer_error(self) -> FailureDisposition {
        self.producer_error
    }

    /// Returns the disposition used when the remote effect is unknown.
    #[must_use]
    pub const fn outcome_unknown(self) -> FailureDisposition {
        self.outcome_unknown
    }
}

impl Default for RelayPolicy {
    fn default() -> Self {
        Self::new(FailureDisposition::Retry, FailureDisposition::Reconcile)
    }
}

/// Invalid relay configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelayConfigError {
    /// A relay batch cannot have a zero entry limit.
    ZeroBatchSize,
}

impl Display for RelayConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroBatchSize => formatter.write_str("outbox relay batch size must be positive"),
        }
    }
}

impl Error for RelayConfigError {}

/// Bounded configuration for one outbox relay batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelayConfig {
    batch_size: usize,
    policy: RelayPolicy,
}

impl RelayConfig {
    /// Creates a relay configuration.
    ///
    /// # Errors
    ///
    /// Returns `ZeroBatchSize` when `batch_size` is zero.
    pub const fn new(batch_size: usize, policy: RelayPolicy) -> Result<Self, RelayConfigError> {
        if batch_size == 0 {
            Err(RelayConfigError::ZeroBatchSize)
        } else {
            Ok(Self { batch_size, policy })
        }
    }

    /// Returns the maximum number of entries claimed per run.
    #[must_use]
    pub const fn batch_size(self) -> usize {
        self.batch_size
    }

    /// Returns the publication failure policy.
    #[must_use]
    pub const fn policy(self) -> RelayPolicy {
        self.policy
    }
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            policy: RelayPolicy::default(),
        }
    }
}

/// Aggregate outcome of a completed or partially completed relay batch.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RelayBatchReport {
    /// Entries claimed at the beginning of the run.
    pub claimed: usize,
    /// Entries confirmed by the broker and marked published.
    pub published: usize,
    /// Entries made eligible for a later retry.
    pub retry_scheduled: usize,
    /// Entries removed from automatic publication for reconciliation.
    pub awaiting_reconciliation: usize,
    /// Entries marked as permanently failed.
    pub terminal_failures: usize,
}

impl RelayBatchReport {
    fn record_disposition(&mut self, disposition: FailureDisposition) {
        match disposition {
            FailureDisposition::Retry => self.retry_scheduled += 1,
            FailureDisposition::Reconcile => self.awaiting_reconciliation += 1,
            FailureDisposition::Terminal => self.terminal_failures += 1,
        }
    }
}

/// Failure returned by one relay run.
pub enum RelayError<StoreError, ProducerError> {
    /// An outbox store operation failed.
    Store {
        /// Failed operation.
        operation: RelayStoreOperation,
        /// Partial batch report up to the failure.
        report: RelayBatchReport,
        /// Concrete store error.
        source: StoreError,
    },
    /// A producer error was recorded successfully in the store.
    Publish {
        /// Entry whose publication failed.
        outbox_id: OutboxId,
        /// Partial batch report including the recorded failure disposition.
        report: RelayBatchReport,
        /// Concrete producer error.
        source: ProducerError,
    },
    /// Publication failed and recording that failure also failed.
    PublishAndStore {
        /// Entry whose publication failed.
        outbox_id: OutboxId,
        /// Partial batch report before failure recording.
        report: RelayBatchReport,
        /// Concrete producer error.
        publish_source: ProducerError,
        /// Concrete store error.
        store_source: StoreError,
    },
}

impl<StoreError, ProducerError> RelayError<StoreError, ProducerError> {
    /// Returns the partial report accumulated before the error.
    #[must_use]
    pub const fn report(&self) -> RelayBatchReport {
        match self {
            Self::Store { report, .. }
            | Self::Publish { report, .. }
            | Self::PublishAndStore { report, .. } => *report,
        }
    }

    /// Returns the affected outbox identity when publication had started.
    #[must_use]
    pub const fn outbox_id(&self) -> Option<OutboxId> {
        match self {
            Self::Store { .. } => None,
            Self::Publish { outbox_id, .. } | Self::PublishAndStore { outbox_id, .. } => {
                Some(*outbox_id)
            }
        }
    }
}

impl<StoreError, ProducerError> Debug for RelayError<StoreError, ProducerError> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store {
                operation, report, ..
            } => formatter
                .debug_struct("RelayError::Store")
                .field("operation", operation)
                .field("report", report)
                .field("source", &"<redacted>")
                .finish(),
            Self::Publish {
                outbox_id, report, ..
            } => formatter
                .debug_struct("RelayError::Publish")
                .field("outbox_id", outbox_id)
                .field("report", report)
                .field("source", &"<redacted>")
                .finish(),
            Self::PublishAndStore {
                outbox_id, report, ..
            } => formatter
                .debug_struct("RelayError::PublishAndStore")
                .field("outbox_id", outbox_id)
                .field("report", report)
                .field("publish_source", &"<redacted>")
                .field("store_source", &"<redacted>")
                .finish(),
        }
    }
}

impl<StoreError, ProducerError> Display for RelayError<StoreError, ProducerError> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store { operation, .. } => {
                write!(formatter, "outbox store operation {operation:?} failed")
            }
            Self::Publish { outbox_id, .. } => {
                write!(
                    formatter,
                    "publication failed for outbox entry {outbox_id:?}"
                )
            }
            Self::PublishAndStore { outbox_id, .. } => write!(
                formatter,
                "publication and failure recording failed for outbox entry {outbox_id:?}"
            ),
        }
    }
}

impl<StoreError, ProducerError> Error for RelayError<StoreError, ProducerError>
where
    StoreError: Error + 'static,
    ProducerError: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Store { source, .. } => Some(source),
            Self::Publish { source, .. } => Some(source),
            Self::PublishAndStore { store_source, .. } => Some(store_source),
        }
    }
}

/// Bounded, broker-neutral relay over an outbox store and message producer.
#[derive(Clone, Debug)]
pub struct OutboxRelay<S, P> {
    store: S,
    producer: P,
    config: RelayConfig,
}

impl<S, P> OutboxRelay<S, P> {
    /// Creates an outbox relay.
    #[must_use]
    pub const fn new(store: S, producer: P, config: RelayConfig) -> Self {
        Self {
            store,
            producer,
            config,
        }
    }

    /// Returns the configured outbox store.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// Returns the configured producer.
    #[must_use]
    pub const fn producer(&self) -> &P {
        &self.producer
    }

    /// Returns the relay configuration.
    #[must_use]
    pub const fn config(&self) -> RelayConfig {
        self.config
    }
}

impl<S, P> OutboxRelay<S, P>
where
    S: OutboxStore,
    P: MessageProducer,
{
    /// Claims and publishes one bounded batch.
    ///
    /// Producer errors are first recorded according to policy and then returned with
    /// their concrete source. An unknown remote effect is a reportable outcome, not an
    /// error, and is never retried unless the configured policy explicitly selects Retry.
    ///
    /// # Errors
    ///
    /// Returns a source-preserving error when publication or persistence fails.
    pub async fn run_once(&self) -> Result<RelayBatchReport, RelayError<S::Error, P::Error>> {
        let entries = self
            .store
            .claim_pending(self.config.batch_size())
            .await
            .map_err(|source| RelayError::Store {
                operation: RelayStoreOperation::ClaimPending,
                report: RelayBatchReport::default(),
                source,
            })?;

        let mut report = RelayBatchReport {
            claimed: entries.len(),
            ..RelayBatchReport::default()
        };

        for entry in entries {
            match self.producer.publish(entry.message.clone()).await {
                Ok(PublishOutcome::Confirmed) => {
                    self.store
                        .mark_published(entry.id)
                        .await
                        .map_err(|source| RelayError::Store {
                            operation: RelayStoreOperation::MarkPublished,
                            report,
                            source,
                        })?;
                    report.published += 1;
                }
                Ok(PublishOutcome::OutcomeUnknown) => {
                    let disposition = self.config.policy().outcome_unknown();
                    self.store
                        .mark_failed(entry.id, PublishFailure::outcome_unknown(disposition))
                        .await
                        .map_err(|source| RelayError::Store {
                            operation: RelayStoreOperation::MarkFailed,
                            report,
                            source,
                        })?;
                    report.record_disposition(disposition);
                }
                Err(source) => {
                    let disposition = self.config.policy().producer_error();
                    match self
                        .store
                        .mark_failed(entry.id, PublishFailure::producer_error(disposition))
                        .await
                    {
                        Ok(()) => {
                            report.record_disposition(disposition);
                            return Err(RelayError::Publish {
                                outbox_id: entry.id,
                                report,
                                source,
                            });
                        }
                        Err(store_source) => {
                            return Err(RelayError::PublishAndStore {
                                outbox_id: entry.id,
                                report,
                                publish_source: source,
                                store_source,
                            });
                        }
                    }
                }
            }
        }

        Ok(report)
    }
}
