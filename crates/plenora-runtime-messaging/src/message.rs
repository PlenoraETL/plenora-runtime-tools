use std::{
    error::Error,
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use bytes::Bytes;
use chrono::{DateTime, Utc};

use crate::{CausationId, CorrelationId, MessageId, MessageMetadata};

/// Portable metadata key containing the stable message identifier.
pub const MESSAGE_ID_METADATA_KEY: &str = "plenora.message.id";
/// Portable metadata key containing the correlation identifier.
pub const CORRELATION_ID_METADATA_KEY: &str = "plenora.trace.correlation_id";
/// Portable metadata key containing the optional causation identifier.
pub const CAUSATION_ID_METADATA_KEY: &str = "plenora.message.causation_id";

/// Serialized message data passed to broker adapters.
#[derive(Clone, Eq, PartialEq)]
pub struct SerializedMessage {
    /// Media type describing the payload representation.
    pub content_type: Arc<str>,
    /// Encoded payload bytes.
    pub bytes: Bytes,
    /// Namespaced transport headers.
    pub headers: MessageMetadata,
}

impl SerializedMessage {
    /// Creates serialized message data with empty headers.
    #[must_use]
    pub fn new(content_type: impl Into<Arc<str>>, bytes: impl Into<Bytes>) -> Self {
        Self {
            content_type: content_type.into(),
            bytes: bytes.into(),
            headers: MessageMetadata::new(),
        }
    }

    /// Adds transport headers.
    #[must_use]
    pub fn with_headers(mut self, headers: MessageMetadata) -> Self {
        self.headers = headers;
        self
    }

    /// Returns the encoded payload size in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Returns whether the encoded payload is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl Debug for SerializedMessage {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SerializedMessage")
            .field("content_type", &self.content_type)
            .field("byte_len", &self.bytes.len())
            .field("headers", &self.headers)
            .finish_non_exhaustive()
    }
}

/// Typed message plus identity, version, time, and propagation metadata.
#[derive(Clone, Eq, PartialEq)]
pub struct MessageEnvelope<T> {
    /// Globally unique message identity.
    pub message_id: MessageId,
    /// Stable logical message type.
    pub message_type: Arc<str>,
    /// Schema version understood by the codec and consumer.
    pub schema_version: Arc<str>,
    /// Time at which the represented event occurred.
    pub occurred_at: DateTime<Utc>,
    /// Identity shared by the correlated operation.
    pub correlation_id: CorrelationId,
    /// Identity of the message that directly caused this one.
    pub causation_id: Option<CausationId>,
    /// Namespaced application and propagation metadata.
    pub metadata: MessageMetadata,
    /// Domain payload. The messaging crate imposes no serialization format on it.
    pub payload: T,
}

impl<T> MessageEnvelope<T> {
    /// Creates an envelope with a new message identifier and empty metadata.
    #[must_use]
    pub fn new(
        message_type: impl Into<Arc<str>>,
        schema_version: impl Into<Arc<str>>,
        occurred_at: DateTime<Utc>,
        correlation_id: CorrelationId,
        payload: T,
    ) -> Self {
        Self {
            message_id: MessageId::random(),
            message_type: message_type.into(),
            schema_version: schema_version.into(),
            occurred_at,
            correlation_id,
            causation_id: None,
            metadata: MessageMetadata::new(),
            payload,
        }
    }

    /// Sets the causal message identifier.
    #[must_use]
    pub fn with_causation(mut self, causation_id: CausationId) -> Self {
        self.causation_id = Some(causation_id);
        self
    }

    /// Sets envelope metadata.
    #[must_use]
    pub fn with_metadata(mut self, metadata: MessageMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Transforms the payload while preserving envelope identity and metadata.
    #[must_use]
    pub fn map_payload<U>(self, map: impl FnOnce(T) -> U) -> MessageEnvelope<U> {
        MessageEnvelope {
            message_id: self.message_id,
            message_type: self.message_type,
            schema_version: self.schema_version,
            occurred_at: self.occurred_at,
            correlation_id: self.correlation_id,
            causation_id: self.causation_id,
            metadata: self.metadata,
            payload: map(self.payload),
        }
    }
}

impl<T> Debug for MessageEnvelope<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MessageEnvelope")
            .field("message_id", &self.message_id)
            .field("message_type", &self.message_type)
            .field("schema_version", &self.schema_version)
            .field("occurred_at", &self.occurred_at)
            .field("correlation_id", &self.correlation_id)
            .field("causation_id", &self.causation_id)
            .field("metadata", &self.metadata)
            .field("payload", &"<redacted>")
            .finish()
    }
}

/// Codec boundary between typed values and transport-neutral bytes.
pub trait MessageCodec<T>: Send + Sync {
    /// Encoding or decoding error.
    type Error: Error + Send + Sync + 'static;

    /// Encodes a typed value.
    ///
    /// # Errors
    ///
    /// Returns a codec-specific error when encoding fails.
    fn encode(&self, value: &T) -> Result<SerializedMessage, Self::Error>;

    /// Decodes a typed value.
    ///
    /// # Errors
    ///
    /// Returns a codec-specific error when decoding fails.
    fn decode(&self, message: &SerializedMessage) -> Result<T, Self::Error>;
}
