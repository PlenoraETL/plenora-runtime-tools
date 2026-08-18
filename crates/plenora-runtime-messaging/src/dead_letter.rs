use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    sync::Arc,
};

use async_trait::async_trait;

use crate::{
    DeadLetter, MESSAGE_ID_METADATA_KEY, MessageProducer, PublishOutcome, SerializedMessage,
};

/// Metadata key containing the deterministic identity of the dead-letter record.
pub const DEAD_LETTER_ID_METADATA_KEY: &str = "plenora.dead_letter.id";
/// Metadata key containing a stable, redaction-safe dead-letter reason.
pub const DEAD_LETTER_REASON_METADATA_KEY: &str = "plenora.dead_letter.reason";
/// Metadata key containing the number of processing attempts.
pub const DEAD_LETTER_ATTEMPTS_METADATA_KEY: &str = "plenora.dead_letter.attempts";
/// Metadata key containing the RFC 3339 failure timestamp.
pub const DEAD_LETTER_FAILED_AT_METADATA_KEY: &str = "plenora.dead_letter.failed_at";

/// Stable category for a dead-letter publication failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeadLetterPublishErrorKind {
    /// Portable dead-letter metadata could not be added within the configured bounds.
    Metadata,
    /// The dedicated producer could not publish the dead letter.
    Publication,
}

/// Source-preserving and payload-redacted dead-letter publication failure.
#[derive(Clone)]
pub struct DeadLetterPublishError {
    kind: DeadLetterPublishErrorKind,
    source: Arc<dyn Error + Send + Sync + 'static>,
}

impl DeadLetterPublishError {
    /// Creates an error while preserving its concrete source.
    #[must_use]
    pub fn with_source<E>(kind: DeadLetterPublishErrorKind, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            kind,
            source: Arc::new(source),
        }
    }

    /// Returns the stable failure category.
    #[must_use]
    pub const fn kind(&self) -> DeadLetterPublishErrorKind {
        self.kind
    }

    /// Returns the concrete source without exposing it through diagnostics.
    #[must_use]
    pub fn source_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        self.source.as_ref()
    }
}

impl Debug for DeadLetterPublishError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeadLetterPublishError")
            .field("kind", &self.kind)
            .field("has_source", &true)
            .finish_non_exhaustive()
    }
}

impl Display for DeadLetterPublishError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "dead-letter publication failed during {:?}",
            self.kind
        )
    }
}

impl Error for DeadLetterPublishError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Broker-neutral destination for messages abandoned by a worker.
///
/// A returned [`PublishOutcome::Confirmed`] is the only result that permits a consumer adapter to
/// terminate the original delivery. `OutcomeUnknown` must remain observable because blindly
/// terminating can lose the message while blindly retrying can duplicate the dead letter.
#[async_trait]
pub trait DeadLetterSink: Send + Sync {
    /// Publishes one portable dead-letter record.
    ///
    /// # Errors
    ///
    /// Returns a source-preserving, redaction-safe publication error.
    async fn publish_dead_letter(
        &self,
        dead_letter: DeadLetter,
    ) -> Result<PublishOutcome, DeadLetterPublishError>;
}

#[async_trait]
impl<P> DeadLetterSink for P
where
    P: MessageProducer + ?Sized,
{
    async fn publish_dead_letter(
        &self,
        dead_letter: DeadLetter,
    ) -> Result<PublishOutcome, DeadLetterPublishError> {
        let message = dead_letter_message(dead_letter)?;
        self.publish(message).await.map_err(|source| {
            DeadLetterPublishError::with_source(DeadLetterPublishErrorKind::Publication, source)
        })
    }
}

fn dead_letter_message(
    dead_letter: DeadLetter,
) -> Result<SerializedMessage, DeadLetterPublishError> {
    let DeadLetter {
        mut message,
        reason,
        attempts,
        failed_at,
    } = dead_letter;
    let metadata = &mut message.headers;
    let dead_letter_id = metadata
        .get_text(MESSAGE_ID_METADATA_KEY)
        .map_err(metadata_error)?
        .map(|message_id| format!("{message_id}.dlq"));
    if let Some(dead_letter_id) = dead_letter_id {
        let _previous = metadata
            .insert_text(DEAD_LETTER_ID_METADATA_KEY, dead_letter_id)
            .map_err(metadata_error)?;
    }
    let _previous = metadata
        .insert_text(DEAD_LETTER_REASON_METADATA_KEY, reason.to_string())
        .map_err(metadata_error)?;
    let _previous = metadata
        .insert_text(DEAD_LETTER_ATTEMPTS_METADATA_KEY, attempts.to_string())
        .map_err(metadata_error)?;
    let _previous = metadata
        .insert_text(DEAD_LETTER_FAILED_AT_METADATA_KEY, failed_at.to_rfc3339())
        .map_err(metadata_error)?;
    Ok(message)
}

fn metadata_error(source: impl Error + Send + Sync + 'static) -> DeadLetterPublishError {
    DeadLetterPublishError::with_source(DeadLetterPublishErrorKind::Metadata, source)
}
