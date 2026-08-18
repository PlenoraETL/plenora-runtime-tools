use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    marker::PhantomData,
};

use plenora_runtime_messaging::{
    CAUSATION_ID_METADATA_KEY, CORRELATION_ID_METADATA_KEY, CausationId, CorrelationId,
    MESSAGE_ID_METADATA_KEY, MessageCodec, MessageId, SerializedMessage,
};

use crate::WorkerContextIdentity;

/// Typed payload and stable identities decoded before a worker invocation.
pub struct DecodedWorkerMessage<T> {
    /// Stable message, correlation, and optional causation identifiers.
    pub identity: WorkerContextIdentity,
    /// Typed application message passed to the handler.
    pub message: T,
}

impl<T> DecodedWorkerMessage<T> {
    /// Creates a decoded worker message.
    #[must_use]
    pub const fn new(identity: WorkerContextIdentity, message: T) -> Self {
        Self { identity, message }
    }
}

impl<T> Debug for DecodedWorkerMessage<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecodedWorkerMessage")
            .field("identity", &self.identity)
            .field("message", &"<redacted>")
            .finish()
    }
}

/// Converts transport-neutral bytes into a typed worker message and its stable identities.
pub trait WorkerMessageDecoder<T>: Send + Sync {
    /// Concrete decoder error.
    type Error: Error + Send + Sync + 'static;

    /// Decodes one serialized delivery without applying broker settlement.
    ///
    /// # Errors
    ///
    /// Returns a typed error when payload decoding or identity validation fails.
    fn decode(&self, message: &SerializedMessage) -> Result<DecodedWorkerMessage<T>, Self::Error>;
}

/// Decoder that delegates payload decoding to [`MessageCodec`] and reads canonical identity keys.
pub struct MetadataMessageDecoder<C, T> {
    codec: C,
    message: PhantomData<fn() -> T>,
}

impl<C, T> MetadataMessageDecoder<C, T> {
    /// Creates a canonical metadata-backed worker decoder.
    #[must_use]
    pub const fn new(codec: C) -> Self {
        Self {
            codec,
            message: PhantomData,
        }
    }

    /// Returns the wrapped payload codec.
    #[must_use]
    pub const fn codec(&self) -> &C {
        &self.codec
    }
}

impl<C: Debug, T> Debug for MetadataMessageDecoder<C, T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataMessageDecoder")
            .field("codec", &self.codec)
            .finish_non_exhaustive()
    }
}

impl<C, T> WorkerMessageDecoder<T> for MetadataMessageDecoder<C, T>
where
    C: MessageCodec<T>,
    T: Send + Sync,
{
    type Error = MetadataMessageDecodeError<C::Error>;

    fn decode(&self, message: &SerializedMessage) -> Result<DecodedWorkerMessage<T>, Self::Error> {
        let identity = WorkerContextIdentity::new(
            required_message_id::<C::Error>(&message.headers)?,
            required_correlation_id::<C::Error>(&message.headers)?,
            optional_causation_id::<C::Error>(&message.headers)?,
        );
        let payload = self
            .codec
            .decode(message)
            .map_err(MetadataMessageDecodeError::Codec)?;
        Ok(DecodedWorkerMessage::new(identity, payload))
    }
}

/// Failure while decoding canonical worker identity metadata or a typed payload.
pub enum MetadataMessageDecodeError<E> {
    /// A required identity key is absent.
    MissingIdentity(&'static str),
    /// An identity value is not UTF-8.
    InvalidIdentityEncoding(&'static str),
    /// An identity value is not a UUID.
    InvalidIdentity(&'static str),
    /// The injected payload codec rejected the message.
    Codec(E),
}

impl<E> MetadataMessageDecodeError<E> {
    /// Returns the stable metadata key or payload category that failed.
    #[must_use]
    pub const fn field(&self) -> &'static str {
        match self {
            Self::MissingIdentity(key)
            | Self::InvalidIdentityEncoding(key)
            | Self::InvalidIdentity(key) => key,
            Self::Codec(_) => "payload",
        }
    }
}

impl<E> Debug for MetadataMessageDecodeError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataMessageDecodeError")
            .field("field", &self.field())
            .finish_non_exhaustive()
    }
}

impl<E> Display for MetadataMessageDecodeError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "worker message decoding failed for {}",
            self.field()
        )
    }
}

impl<E> Error for MetadataMessageDecodeError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            Self::MissingIdentity(_)
            | Self::InvalidIdentityEncoding(_)
            | Self::InvalidIdentity(_) => None,
        }
    }
}

fn required_message_id<E>(
    metadata: &plenora_runtime_messaging::MessageMetadata,
) -> Result<MessageId, MetadataMessageDecodeError<E>> {
    let value = required_identity_text(metadata, MESSAGE_ID_METADATA_KEY)?;
    value
        .parse()
        .map_err(|_error| MetadataMessageDecodeError::InvalidIdentity(MESSAGE_ID_METADATA_KEY))
}

fn required_correlation_id<E>(
    metadata: &plenora_runtime_messaging::MessageMetadata,
) -> Result<CorrelationId, MetadataMessageDecodeError<E>> {
    let value = required_identity_text(metadata, CORRELATION_ID_METADATA_KEY)?;
    value
        .parse()
        .map_err(|_error| MetadataMessageDecodeError::InvalidIdentity(CORRELATION_ID_METADATA_KEY))
}

fn required_identity_text<'a, E>(
    metadata: &'a plenora_runtime_messaging::MessageMetadata,
    key: &'static str,
) -> Result<&'a str, MetadataMessageDecodeError<E>> {
    metadata
        .get_text(key)
        .map_err(|_error| MetadataMessageDecodeError::InvalidIdentityEncoding(key))?
        .ok_or(MetadataMessageDecodeError::MissingIdentity(key))
}

fn optional_causation_id<E>(
    metadata: &plenora_runtime_messaging::MessageMetadata,
) -> Result<Option<CausationId>, MetadataMessageDecodeError<E>> {
    metadata
        .get_text(CAUSATION_ID_METADATA_KEY)
        .map_err(|_error| {
            MetadataMessageDecodeError::InvalidIdentityEncoding(CAUSATION_ID_METADATA_KEY)
        })?
        .map(|value| {
            value.parse().map_err(|_error| {
                MetadataMessageDecodeError::InvalidIdentity(CAUSATION_ID_METADATA_KEY)
            })
        })
        .transpose()
}
