//! Tests for broker contracts and owned acknowledgement capabilities.

use std::{
    convert::Infallible,
    error::Error,
    fmt::{self, Display, Formatter},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use plenora_runtime_messaging::{
    AckError, AckOperation, Delivery, DeliveryAcknowledger, DeliveryHeartbeatConfig,
    DeliveryHeartbeatConfigError, MessageConsumer, MessageMetadata, MessageProducer, NackReason,
    PublishOutcome, SerializedMessage,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AckEvent {
    Ack,
    Nack(NackReason),
    Heartbeat,
}

#[derive(Clone)]
struct RecordingAcknowledger {
    events: Arc<Mutex<Vec<AckEvent>>>,
}

#[async_trait]
impl DeliveryAcknowledger for RecordingAcknowledger {
    async fn heartbeat(&mut self) -> Result<(), AckError> {
        lock_recover(&self.events).push(AckEvent::Heartbeat);
        Ok(())
    }

    async fn ack(self: Box<Self>) -> Result<(), AckError> {
        lock_recover(&self.events).push(AckEvent::Ack);
        Ok(())
    }

    async fn nack(self: Box<Self>, reason: NackReason) -> Result<(), AckError> {
        lock_recover(&self.events).push(AckEvent::Nack(reason));
        Ok(())
    }
}

fn delivery_with_heartbeat(
    events: Arc<Mutex<Vec<AckEvent>>>,
) -> Result<Delivery, DeliveryHeartbeatConfigError> {
    Ok(Delivery::new_with_heartbeat(
        SerializedMessage::new("application/octet-stream", Bytes::from_static(b"body")),
        2,
        MessageMetadata::new(),
        DeliveryHeartbeatConfig::new(Duration::from_secs(5), 2)?,
        RecordingAcknowledger { events },
    ))
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn delivery(events: Arc<Mutex<Vec<AckEvent>>>) -> Delivery {
    Delivery::new(
        SerializedMessage::new("application/octet-stream", Bytes::from_static(b"body")),
        2,
        MessageMetadata::new(),
        RecordingAcknowledger { events },
    )
}

#[tokio::test]
async fn ack_consumes_delivery_and_invokes_the_capability_once() -> Result<(), Box<dyn Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));

    delivery(Arc::clone(&events)).ack().await?;

    assert_eq!(lock_recover(&events).as_slice(), &[AckEvent::Ack]);
    Ok(())
}

#[tokio::test]
async fn nack_preserves_the_explicit_reason() -> Result<(), Box<dyn Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));

    delivery(Arc::clone(&events))
        .nack(NackReason::Shutdown)
        .await?;

    assert_eq!(
        lock_recover(&events).as_slice(),
        &[AckEvent::Nack(NackReason::Shutdown)]
    );
    Ok(())
}

#[tokio::test]
async fn heartbeat_is_non_terminal_and_ack_remains_owned() -> Result<(), Box<dyn Error>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut delivery = delivery_with_heartbeat(Arc::clone(&events))?;

    assert_eq!(
        delivery.heartbeat_config(),
        Some(DeliveryHeartbeatConfig::new(Duration::from_secs(5), 2)?)
    );
    delivery.heartbeat().await?;
    delivery.heartbeat().await?;
    delivery.ack().await?;

    assert_eq!(
        lock_recover(&events).as_slice(),
        &[AckEvent::Heartbeat, AckEvent::Heartbeat, AckEvent::Ack]
    );
    Ok(())
}

#[test]
fn heartbeat_config_rejects_busy_loop_and_zero_failure_budget() {
    assert_eq!(
        DeliveryHeartbeatConfig::new(Duration::ZERO, 1),
        Err(DeliveryHeartbeatConfigError::ZeroInterval)
    );
    assert_eq!(
        DeliveryHeartbeatConfig::new(Duration::from_secs(1), 0),
        Err(DeliveryHeartbeatConfigError::ZeroFailureLimit)
    );
    assert_eq!(
        DeliveryHeartbeatConfigError::ZeroInterval.to_string(),
        "delivery heartbeat interval must be nonzero"
    );
    assert_eq!(
        DeliveryHeartbeatConfigError::ZeroFailureLimit.to_string(),
        "delivery heartbeat consecutive-failure limit must be nonzero"
    );
}

#[derive(Debug)]
struct AdapterFailure;

impl Display for AdapterFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("adapter failed")
    }
}

impl Error for AdapterFailure {}

#[test]
fn acknowledgement_error_preserves_operation_and_source() {
    let error = AckError::with_source(AckOperation::Nack, "cannot reject delivery", AdapterFailure);

    assert_eq!(error.operation(), AckOperation::Nack);
    assert_eq!(error.message(), "cannot reject delivery");
    assert_eq!(
        error.source_error().map(ToString::to_string).as_deref(),
        Some("adapter failed")
    );
    assert!(Error::source(&error).is_some());

    let source_free = AckError::new(AckOperation::Ack, "safe acknowledgement failure");
    assert_eq!(source_free.operation(), AckOperation::Ack);
    assert_eq!(source_free.message(), "safe acknowledgement failure");
    assert_eq!(source_free.to_string(), "safe acknowledgement failure");
    assert!(source_free.source_error().is_none());
    assert!(Error::source(&source_free).is_none());
}

struct HeartbeatUnsupported;

#[async_trait]
impl DeliveryAcknowledger for HeartbeatUnsupported {
    async fn ack(self: Box<Self>) -> Result<(), AckError> {
        Ok(())
    }

    async fn nack(self: Box<Self>, _reason: NackReason) -> Result<(), AckError> {
        Ok(())
    }
}

#[tokio::test]
async fn default_heartbeat_fails_closed_and_delivery_debug_redacts_payload()
-> Result<(), Box<dyn Error>> {
    let mut delivery = Delivery::new(
        SerializedMessage::new(
            "application/octet-stream",
            Bytes::from_static(b"private-payload"),
        ),
        3,
        MessageMetadata::new(),
        HeartbeatUnsupported,
    );
    assert_eq!(delivery.heartbeat_config(), None);
    let debug = format!("{delivery:?}");
    assert!(debug.contains("Delivery"));
    assert!(!debug.contains("private-payload"));

    let error = delivery
        .heartbeat()
        .await
        .err()
        .ok_or("default heartbeat unexpectedly succeeded")?;
    assert_eq!(error.operation(), AckOperation::Heartbeat);
    assert_eq!(
        error.message(),
        "delivery acknowledger does not support heartbeats"
    );
    Ok(())
}

#[derive(Default)]
struct FakeProducer {
    published_lengths: Mutex<Vec<usize>>,
}

#[async_trait]
impl MessageProducer for FakeProducer {
    type Error = Infallible;

    async fn publish(&self, message: SerializedMessage) -> Result<PublishOutcome, Self::Error> {
        lock_recover(&self.published_lengths).push(message.len());
        Ok(PublishOutcome::Confirmed)
    }
}

struct EmptyConsumer;

#[async_trait]
impl MessageConsumer for EmptyConsumer {
    type Error = Infallible;

    async fn receive(&mut self) -> Result<Option<Delivery>, Self::Error> {
        Ok(None)
    }
}

#[tokio::test]
async fn producer_and_consumer_contracts_require_no_concrete_broker() -> Result<(), Box<dyn Error>>
{
    let producer = FakeProducer::default();
    let outcome = producer
        .publish(SerializedMessage::new(
            "application/octet-stream",
            Bytes::from_static(b"1234"),
        ))
        .await?;
    let mut consumer = EmptyConsumer;

    assert_eq!(outcome, PublishOutcome::Confirmed);
    assert_eq!(lock_recover(&producer.published_lengths).as_slice(), &[4]);
    assert!(consumer.receive().await?.is_none());
    Ok(())
}
