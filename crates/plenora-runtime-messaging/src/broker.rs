use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    num::NonZeroU32,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;

use crate::{MessageMetadata, SerializedMessage};

/// Broker confirmation state returned after publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublishOutcome {
    /// The broker confirmed publication.
    Confirmed,
    /// The remote effect cannot be determined safely.
    OutcomeUnknown,
}

/// Broker-neutral message publication contract.
#[async_trait]
pub trait MessageProducer: Send + Sync {
    /// Concrete adapter error.
    type Error: Error + Send + Sync + 'static;

    /// Publishes one serialized message.
    ///
    /// # Errors
    ///
    /// Returns an adapter-specific error when publication fails before an outcome can be returned.
    async fn publish(&self, message: SerializedMessage) -> Result<PublishOutcome, Self::Error>;
}

/// Broker-neutral message consumption contract.
#[async_trait]
pub trait MessageConsumer: Send {
    /// Concrete adapter error.
    type Error: Error + Send + Sync + 'static;

    /// Receives the next delivery, or no delivery when the consumer has ended.
    ///
    /// # Errors
    ///
    /// Returns an adapter-specific error when polling fails.
    async fn receive(&mut self) -> Result<Option<Delivery>, Self::Error>;
}

/// Reason supplied when a delivery is negatively acknowledged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NackReason {
    /// Processing may succeed on a later attempt.
    Retryable,
    /// Processing may succeed after the supplied broker-native delay.
    RetryAfter(Duration),
    /// Processing cannot succeed for this message.
    Permanent,
    /// Processing stopped because the runtime is draining.
    Shutdown,
    /// The consumer rejected the message before processing.
    ConsumerRejected,
}

/// Acknowledgement operation that failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckOperation {
    /// Positive acknowledgement.
    Ack,
    /// Negative acknowledgement.
    Nack,
    /// Non-terminal heartbeat used to renew broker ownership.
    Heartbeat,
}

/// Validated cadence and failure budget for non-terminal delivery heartbeats.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeliveryHeartbeatConfig {
    interval: Duration,
    max_consecutive_failures: NonZeroU32,
}

impl DeliveryHeartbeatConfig {
    /// Creates a heartbeat policy with explicit bounded failure tolerance.
    ///
    /// # Errors
    ///
    /// Returns an error when the interval or consecutive-failure limit is zero.
    pub fn new(
        interval: Duration,
        max_consecutive_failures: u32,
    ) -> Result<Self, DeliveryHeartbeatConfigError> {
        if interval.is_zero() {
            return Err(DeliveryHeartbeatConfigError::ZeroInterval);
        }
        let max_consecutive_failures = NonZeroU32::new(max_consecutive_failures)
            .ok_or(DeliveryHeartbeatConfigError::ZeroFailureLimit)?;
        Ok(Self {
            interval,
            max_consecutive_failures,
        })
    }

    /// Returns the delay between heartbeat attempts.
    #[must_use]
    pub const fn interval(self) -> Duration {
        self.interval
    }

    /// Returns how many consecutive renewal failures terminate processing.
    #[must_use]
    pub const fn max_consecutive_failures(self) -> u32 {
        self.max_consecutive_failures.get()
    }
}

/// Invalid delivery-heartbeat policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryHeartbeatConfigError {
    /// A zero interval would create a busy loop.
    ZeroInterval,
    /// At least one renewal failure must be observed before processing is cancelled.
    ZeroFailureLimit,
}

impl Display for DeliveryHeartbeatConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroInterval => {
                formatter.write_str("delivery heartbeat interval must be nonzero")
            }
            Self::ZeroFailureLimit => {
                formatter.write_str("delivery heartbeat consecutive-failure limit must be nonzero")
            }
        }
    }
}

impl Error for DeliveryHeartbeatConfigError {}

/// Broker-neutral acknowledgement failure.
#[derive(Clone, Debug)]
pub struct AckError {
    operation: AckOperation,
    message: Arc<str>,
    source: Option<Arc<dyn Error + Send + Sync + 'static>>,
}

impl AckError {
    /// Creates an acknowledgement error without a source.
    #[must_use]
    pub fn new(operation: AckOperation, message: impl Into<Arc<str>>) -> Self {
        Self {
            operation,
            message: message.into(),
            source: None,
        }
    }

    /// Creates an acknowledgement error that preserves its adapter source.
    #[must_use]
    pub fn with_source<E>(operation: AckOperation, message: impl Into<Arc<str>>, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            operation,
            message: message.into(),
            source: Some(Arc::new(source)),
        }
    }

    /// Returns the failed operation.
    #[must_use]
    pub const fn operation(&self) -> AckOperation {
        self.operation
    }

    /// Returns an operator-facing description.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the original adapter error when available.
    #[must_use]
    pub fn source_error(&self) -> Option<&(dyn Error + Send + Sync + 'static)> {
        self.source.as_deref()
    }
}

impl Display for AckError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for AckError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

/// Adapter-owned acknowledgement capability attached to one delivery.
#[async_trait]
pub trait DeliveryAcknowledger: Send {
    /// Renews ownership without settling the delivery.
    ///
    /// Adapters only receive this call for deliveries constructed with an explicit heartbeat
    /// policy. The default keeps older broker implementations source-compatible and fails closed.
    ///
    /// # Errors
    ///
    /// Returns a broker-neutral heartbeat error when ownership cannot be renewed.
    async fn heartbeat(&mut self) -> Result<(), AckError> {
        Err(AckError::new(
            AckOperation::Heartbeat,
            "delivery acknowledger does not support heartbeats",
        ))
    }

    /// Positively acknowledges the delivery.
    ///
    /// # Errors
    ///
    /// Returns a broker-neutral acknowledgement error.
    async fn ack(self: Box<Self>) -> Result<(), AckError>;

    /// Negatively acknowledges the delivery with an explicit reason.
    ///
    /// # Errors
    ///
    /// Returns a broker-neutral acknowledgement error.
    async fn nack(self: Box<Self>, reason: NackReason) -> Result<(), AckError>;
}

/// An owned broker delivery that can be acknowledged exactly once by construction.
pub struct Delivery {
    /// Serialized message received from the broker.
    pub message: SerializedMessage,
    /// One-based delivery attempt reported by the adapter.
    pub attempt: u32,
    /// Broker-specific metadata represented with portable namespaced keys.
    pub broker_metadata: MessageMetadata,
    heartbeat: Option<DeliveryHeartbeatConfig>,
    acknowledger: Box<dyn DeliveryAcknowledger>,
}

impl Delivery {
    /// Creates a delivery from an adapter acknowledgement implementation.
    #[must_use]
    pub fn new<A>(
        message: SerializedMessage,
        attempt: u32,
        broker_metadata: MessageMetadata,
        acknowledger: A,
    ) -> Self
    where
        A: DeliveryAcknowledger + 'static,
    {
        Self {
            message,
            attempt,
            broker_metadata,
            heartbeat: None,
            acknowledger: Box::new(acknowledger),
        }
    }

    /// Creates a delivery whose ownership must be renewed while processing is active.
    #[must_use]
    pub fn new_with_heartbeat<A>(
        message: SerializedMessage,
        attempt: u32,
        broker_metadata: MessageMetadata,
        heartbeat: DeliveryHeartbeatConfig,
        acknowledger: A,
    ) -> Self
    where
        A: DeliveryAcknowledger + 'static,
    {
        Self {
            message,
            attempt,
            broker_metadata,
            heartbeat: Some(heartbeat),
            acknowledger: Box::new(acknowledger),
        }
    }

    /// Returns the adapter-provided heartbeat policy, when lease renewal is required.
    #[must_use]
    pub const fn heartbeat_config(&self) -> Option<DeliveryHeartbeatConfig> {
        self.heartbeat
    }

    /// Renews ownership without consuming or settling this delivery.
    ///
    /// # Errors
    ///
    /// Returns an acknowledgement error from the adapter.
    pub async fn heartbeat(&mut self) -> Result<(), AckError> {
        self.acknowledger.heartbeat().await
    }

    /// Positively acknowledges and consumes the delivery.
    ///
    /// # Errors
    ///
    /// Returns an acknowledgement error from the adapter.
    pub async fn ack(self) -> Result<(), AckError> {
        self.acknowledger.ack().await
    }

    /// Negatively acknowledges and consumes the delivery.
    ///
    /// # Errors
    ///
    /// Returns an acknowledgement error from the adapter.
    pub async fn nack(self, reason: NackReason) -> Result<(), AckError> {
        self.acknowledger.nack(reason).await
    }
}

impl Debug for Delivery {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Delivery")
            .field("message", &self.message)
            .field("attempt", &self.attempt)
            .field("broker_metadata", &self.broker_metadata)
            .field("heartbeat", &self.heartbeat)
            .finish_non_exhaustive()
    }
}
