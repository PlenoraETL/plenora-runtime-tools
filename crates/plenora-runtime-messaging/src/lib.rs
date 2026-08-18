//! Broker-neutral messaging contracts for Plenora runtimes.

#![forbid(unsafe_code)]

mod broker;
mod dead_letter;
mod identifiers;
mod message;
mod metadata;
mod replay;
mod retry;

pub use broker::{
    AckError, AckOperation, Delivery, DeliveryAcknowledger, DeliveryHeartbeatConfig,
    DeliveryHeartbeatConfigError, MessageConsumer, MessageProducer, NackReason, PublishOutcome,
};
pub use dead_letter::{
    DEAD_LETTER_ATTEMPTS_METADATA_KEY, DEAD_LETTER_FAILED_AT_METADATA_KEY,
    DEAD_LETTER_ID_METADATA_KEY, DEAD_LETTER_REASON_METADATA_KEY, DeadLetterPublishError,
    DeadLetterPublishErrorKind, DeadLetterSink,
};
pub use identifiers::{CausationId, CorrelationId, MessageId};
pub use message::{
    CAUSATION_ID_METADATA_KEY, CORRELATION_ID_METADATA_KEY, MESSAGE_ID_METADATA_KEY, MessageCodec,
    MessageEnvelope, SerializedMessage,
};
pub use metadata::{
    MAX_METADATA_ENTRIES, MAX_METADATA_KEY_BYTES, MAX_METADATA_TOTAL_BYTES,
    MAX_METADATA_VALUE_BYTES, MessageMetadata, MetadataKeyError, MetadataKeyErrorKind,
};
pub use replay::{BrokerCapabilities, DeadLetter, ReplayRequest, ReplaySource};
pub use retry::{
    BackoffConfigError, ClassifyRetry, ExponentialBackoff, ExponentialBackoffConfig, JitterConfig,
    RetryDecision, RetryErrorClass, RetryExhaustedAction, RetryPolicy,
};
