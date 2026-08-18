//! Broker fake behavior and fault-injection tests.

use std::{error::Error, fmt, time::Duration};

use bytes::Bytes;
use plenora_runtime_messaging::{
    AckOperation, DeliveryHeartbeatConfig, MessageConsumer, MessageProducer, NackReason,
    PublishOutcome, SerializedMessage,
};
use plenora_runtime_testkit::{
    AckEvent, FakeBroker, FakeBrokerErrorKind, FakeBrokerLimits, FakeDeliveryId, FakeProducer,
    HeartbeatEvent, ManualClock, UnknownPublishEffect,
};

#[derive(Debug)]
struct MissingDelivery;

impl fmt::Display for MissingDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("expected a fake delivery")
    }
}

impl Error for MissingDelivery {}

fn message(value: &'static [u8]) -> SerializedMessage {
    SerializedMessage::new("application/octet-stream", Bytes::from_static(value))
}

#[tokio::test]
async fn confirmed_publish_is_received_and_acknowledged() -> Result<(), Box<dyn Error>> {
    let broker = FakeBroker::default();
    let producer = broker.producer();
    let mut consumer = broker.consumer();

    assert_eq!(
        producer.publish(message(b"one")).await?,
        PublishOutcome::Confirmed
    );
    let delivery = consumer.receive().await?.ok_or(MissingDelivery)?;
    assert_eq!(delivery.attempt, 1);
    delivery.ack().await?;

    let snapshot = broker.snapshot();
    assert_eq!(snapshot.pending_deliveries, 0);
    assert_eq!(snapshot.applied_publishes, 1);
    assert_eq!(snapshot.acknowledgement_count, 1);
    assert_eq!(broker.acknowledgement_records()[0].event, AckEvent::Acked);
    Ok(())
}

#[tokio::test]
async fn delayed_delivery_does_not_block_a_ready_message() -> Result<(), Box<dyn Error>> {
    let broker = FakeBroker::default();
    let clock = broker.clock();
    let _delayed = broker.enqueue_delayed(message(b"later"), Duration::from_secs(10))?;
    let ready_id = broker.enqueue(message(b"now"))?;

    let ready = broker.dequeue()?.ok_or(MissingDelivery)?;
    assert_eq!(ready.id(), ready_id);
    ready.ack().await?;
    assert!(broker.dequeue()?.is_none());

    let _advanced = clock.advance(Duration::from_secs(10))?;
    let delayed = broker.dequeue()?.ok_or(MissingDelivery)?;
    assert_eq!(delayed.message.bytes, Bytes::from_static(b"later"));
    Ok(())
}

#[tokio::test]
async fn nack_redelivers_the_same_logical_delivery() -> Result<(), Box<dyn Error>> {
    let broker = FakeBroker::default();
    let id = broker.enqueue(message(b"retry"))?;

    let first = broker.dequeue()?.ok_or(MissingDelivery)?;
    assert_eq!(first.id(), id);
    first.nack(NackReason::Retryable).await?;

    let second = broker.dequeue()?.ok_or(MissingDelivery)?;
    assert_eq!(second.id(), id);
    assert_eq!(second.attempt, 2);
    second.ack().await?;
    assert_eq!(
        broker.acknowledgement_records()[0].event,
        AckEvent::Nacked(NackReason::Retryable)
    );
    Ok(())
}

#[test]
fn duplicate_injection_preserves_bytes_with_a_distinct_delivery_id() -> Result<(), Box<dyn Error>> {
    let broker = FakeBroker::default();
    let original_id = broker.enqueue(message(b"duplicate"))?;
    let duplicate_id = broker.inject_duplicate(original_id)?;

    assert_ne!(original_id, duplicate_id);
    let original = broker.dequeue()?.ok_or(MissingDelivery)?;
    let duplicate = broker.dequeue()?.ok_or(MissingDelivery)?;
    assert_eq!(original.message, duplicate.message);
    assert_ne!(original.id(), duplicate.id());
    Ok(())
}

#[tokio::test]
async fn unknown_publish_effect_is_explicit_and_propagated() -> Result<(), Box<dyn Error>> {
    let broker = FakeBroker::default();
    let producer = broker.producer();

    broker.return_unknown_for_next_publish(UnknownPublishEffect::NotApplied)?;
    assert_eq!(
        producer.publish(message(b"not-applied")).await?,
        PublishOutcome::OutcomeUnknown
    );
    assert_eq!(broker.snapshot().pending_deliveries, 0);

    broker.return_unknown_for_next_publish(UnknownPublishEffect::Applied)?;
    assert_eq!(
        producer.publish(message(b"applied")).await?,
        PublishOutcome::OutcomeUnknown
    );
    assert_eq!(broker.snapshot().pending_deliveries, 1);
    assert_eq!(broker.published_messages(), [message(b"applied")]);
    Ok(())
}

#[tokio::test]
async fn publish_error_is_fifo_scripted_and_does_not_apply_an_effect() -> Result<(), Box<dyn Error>>
{
    let broker = FakeBroker::default();
    let producer = broker.producer();
    broker.fail_next_publish("first publish fails")?;

    let error = producer
        .publish(message(b"failed"))
        .await
        .err()
        .ok_or(MissingDelivery)?;
    assert_eq!(error.kind(), FakeBrokerErrorKind::Injected);
    assert_eq!(broker.snapshot().pending_deliveries, 0);

    assert_eq!(
        producer.publish(message(b"next")).await?,
        PublishOutcome::Confirmed
    );
    assert_eq!(broker.snapshot().pending_deliveries, 1);
    Ok(())
}

#[tokio::test]
async fn acknowledgement_failure_is_recorded_and_redelivered() -> Result<(), Box<dyn Error>> {
    let broker = FakeBroker::default();
    let _id = broker.enqueue(message(b"ack-failure"))?;
    broker.fail_next_ack("scripted ack failure")?;

    let first = broker.dequeue()?.ok_or(MissingDelivery)?;
    let error = first.ack().await.err().ok_or(MissingDelivery)?;
    assert_eq!(error.operation(), AckOperation::Ack);
    assert!(error.source_error().is_some());
    assert_eq!(
        broker.acknowledgement_records()[0].event,
        AckEvent::Failed(AckOperation::Ack)
    );

    let redelivery = broker.dequeue()?.ok_or(MissingDelivery)?;
    assert_eq!(redelivery.attempt, 2);
    Ok(())
}

#[tokio::test]
async fn nack_failure_is_recorded_and_redelivered() -> Result<(), Box<dyn Error>> {
    let broker = FakeBroker::default();
    let _id = broker.enqueue(message(b"nack-failure"))?;
    broker.fail_next_nack("scripted nack failure")?;

    let first = broker.dequeue()?.ok_or(MissingDelivery)?;
    let error = first
        .nack(NackReason::Shutdown)
        .await
        .err()
        .ok_or(MissingDelivery)?;
    assert_eq!(error.operation(), AckOperation::Nack);
    assert_eq!(
        broker.acknowledgement_records()[0].event,
        AckEvent::Failed(AckOperation::Nack)
    );

    let redelivery = broker.dequeue()?.ok_or(MissingDelivery)?;
    assert_eq!(redelivery.attempt, 2);
    Ok(())
}

#[tokio::test]
async fn disconnect_and_scripted_receive_failure_are_recoverable() -> Result<(), Box<dyn Error>> {
    let broker = FakeBroker::default();
    let mut consumer = broker.consumer();
    let _id = broker.enqueue(message(b"available"))?;

    broker.disconnect();
    let disconnected = consumer.receive().await.err().ok_or(MissingDelivery)?;
    assert_eq!(disconnected.kind(), FakeBrokerErrorKind::Disconnected);

    broker.reconnect();
    broker.fail_next_receive("scripted receive failure")?;
    let injected = consumer.receive().await.err().ok_or(MissingDelivery)?;
    assert_eq!(injected.kind(), FakeBrokerErrorKind::Injected);
    assert!(consumer.receive().await?.is_some());

    consumer.close();
    assert!(consumer.receive().await?.is_none());
    Ok(())
}

#[test]
fn pending_deliveries_and_payload_bytes_are_explicitly_bounded() -> Result<(), Box<dyn Error>> {
    let broker = FakeBroker::with_limits(
        ManualClock::default(),
        FakeBrokerLimits {
            max_pending_deliveries: 1,
            max_message_bytes: 3,
            ..FakeBrokerLimits::default()
        },
    );

    let _accepted = broker.enqueue(message(b"one"))?;
    let capacity = broker
        .enqueue(message(b"two"))
        .err()
        .ok_or(MissingDelivery)?;
    assert_eq!(capacity.kind(), FakeBrokerErrorKind::CapacityExceeded);

    let broker = FakeBroker::with_limits(
        ManualClock::default(),
        FakeBrokerLimits {
            max_message_bytes: 2,
            ..FakeBrokerLimits::default()
        },
    );
    let payload = broker
        .enqueue(message(b"three"))
        .err()
        .ok_or(MissingDelivery)?;
    assert_eq!(payload.kind(), FakeBrokerErrorKind::PayloadTooLarge);
    Ok(())
}

#[tokio::test]
async fn permanent_nack_is_terminal_and_observable() -> Result<(), Box<dyn Error>> {
    let broker = FakeBroker::default();
    let _id = broker.enqueue(message(b"permanent"))?;
    let delivery = broker.dequeue()?.ok_or(MissingDelivery)?;

    delivery.nack(NackReason::Permanent).await?;

    assert!(broker.dequeue()?.is_none());
    assert_eq!(broker.snapshot().terminal_delivery_count, 1);
    assert_eq!(
        broker.terminal_delivery_records()[0].event,
        AckEvent::Nacked(NackReason::Permanent)
    );
    Ok(())
}

#[tokio::test]
async fn heartbeat_is_observable_non_terminal_and_fault_injectable() -> Result<(), Box<dyn Error>> {
    let broker = FakeBroker::default();
    let _id = broker.enqueue(message(b"heartbeat"))?;
    let mut delivery = broker
        .dequeue()?
        .ok_or(MissingDelivery)?
        .into_delivery_with_heartbeat(DeliveryHeartbeatConfig::new(Duration::from_secs(1), 2)?);

    delivery.heartbeat().await?;
    broker.fail_next_heartbeat("scripted heartbeat failure")?;
    let error = delivery.heartbeat().await.err().ok_or(MissingDelivery)?;

    assert_eq!(error.operation(), AckOperation::Heartbeat);
    assert_eq!(broker.snapshot().heartbeat_count, 2);
    assert_eq!(broker.snapshot().in_flight_deliveries, 1);
    assert_eq!(
        broker
            .heartbeat_records()
            .iter()
            .map(|record| record.event)
            .collect::<Vec<_>>(),
        vec![HeartbeatEvent::Renewed, HeartbeatEvent::Failed]
    );

    delivery.nack(NackReason::Retryable).await?;
    assert_eq!(broker.snapshot().pending_deliveries, 1);
    Ok(())
}

#[tokio::test]
async fn dropped_owned_delivery_is_conservatively_requeued() -> Result<(), Box<dyn Error>> {
    let broker = FakeBroker::default();
    let _id = broker.enqueue(message(b"cancelled"))?;
    let mut consumer = broker.consumer();

    let first = consumer.receive_fake()?.ok_or(MissingDelivery)?;
    assert_eq!(broker.snapshot().in_flight_deliveries, 1);
    drop(first);

    let second = consumer.receive_fake()?.ok_or(MissingDelivery)?;
    assert_eq!(second.attempt, 2);
    assert_eq!(
        broker.acknowledgement_records()[0].event,
        AckEvent::Nacked(NackReason::Shutdown)
    );
    second.ack().await?;
    Ok(())
}

#[test]
fn fault_scripts_reject_entries_beyond_their_bound() -> Result<(), Box<dyn Error>> {
    let broker = FakeBroker::with_limits(
        ManualClock::default(),
        FakeBrokerLimits {
            max_scripted_faults: 1,
            ..FakeBrokerLimits::default()
        },
    );

    broker.fail_next_receive("accepted")?;
    let error = broker
        .fail_next_receive("rejected")
        .err()
        .ok_or(MissingDelivery)?;

    assert_eq!(error.kind(), FakeBrokerErrorKind::CapacityExceeded);
    Ok(())
}

#[tokio::test]
async fn public_fake_handles_debug_connectivity_and_consumer_lifecycle()
-> Result<(), Box<dyn Error>> {
    let id = FakeDeliveryId::from_u64(42);
    assert_eq!(id.as_u64(), 42);
    assert_eq!(id.to_string(), "42");

    let broker = FakeBroker::new(ManualClock::default());
    assert!(broker.is_connected());
    assert!(format!("{broker:?}").contains("FakeBroker"));
    let producer = FakeProducer::new(broker.clone());
    assert!(producer.broker().is_connected());

    let heartbeat = DeliveryHeartbeatConfig::new(Duration::from_secs(1), 2)?;
    let mut consumer = broker.consumer_with_heartbeat(heartbeat);
    assert!(!consumer.is_closed());
    consumer.close();
    assert!(consumer.is_closed());
    assert!(consumer.receive().await?.is_none());
    consumer.reopen();
    assert!(!consumer.is_closed());

    broker.enqueue(message(b"private-debug-payload"))?;
    let delivery = consumer.receive_fake()?.ok_or(MissingDelivery)?;
    let debug = format!("{delivery:?}");
    assert!(debug.contains("FakeDelivery"));
    assert!(!debug.contains("private-debug-payload"));
    drop(delivery);

    let delivery = consumer.receive().await?.ok_or(MissingDelivery)?;
    assert_eq!(delivery.heartbeat_config(), Some(heartbeat));
    delivery.ack().await?;
    Ok(())
}

#[tokio::test]
async fn disconnected_settlement_and_heartbeat_fail_conservatively() -> Result<(), Box<dyn Error>> {
    let broker = FakeBroker::default();
    broker.enqueue(message(b"ack-disconnect"))?;
    let delivery = broker.dequeue()?.ok_or(MissingDelivery)?;
    broker.disconnect();
    let error = delivery.ack().await.err().ok_or(MissingDelivery)?;
    assert_eq!(error.operation(), AckOperation::Ack);
    assert_eq!(
        error
            .source_error()
            .and_then(|source| source.downcast_ref::<plenora_runtime_testkit::FakeBrokerError>())
            .map(plenora_runtime_testkit::FakeBrokerError::kind),
        Some(FakeBrokerErrorKind::Disconnected)
    );

    broker.reconnect();
    let mut redelivery = broker
        .dequeue()?
        .ok_or(MissingDelivery)?
        .into_delivery_with_heartbeat(DeliveryHeartbeatConfig::new(Duration::from_secs(1), 1)?);
    broker.disconnect();
    let heartbeat_error = redelivery.heartbeat().await.err().ok_or(MissingDelivery)?;
    assert_eq!(heartbeat_error.operation(), AckOperation::Heartbeat);
    broker.reconnect();
    redelivery.nack(NackReason::Permanent).await?;
    Ok(())
}

#[test]
fn fake_errors_expose_only_stable_safe_diagnostics() -> Result<(), Box<dyn Error>> {
    let broker = FakeBroker::default();
    let error = broker
        .inject_duplicate(FakeDeliveryId::from_u64(999))
        .err()
        .ok_or(MissingDelivery)?;
    assert_eq!(error.kind(), FakeBrokerErrorKind::UnknownDelivery);
    assert_eq!(error.message(), "cannot duplicate an unknown fake delivery");
    assert_eq!(error.to_string(), error.message());
    Ok(())
}
