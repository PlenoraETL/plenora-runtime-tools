//! Opt-in integration coverage against a real, ephemeral NATS `JetStream` server.

use std::{
    env,
    error::Error,
    io,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use plenora_runtime_core::{HealthRegistry, HealthStatus, ReadinessStatus};
use plenora_runtime_messaging::{
    DEAD_LETTER_ATTEMPTS_METADATA_KEY, DEAD_LETTER_ID_METADATA_KEY,
    DEAD_LETTER_REASON_METADATA_KEY, DeadLetter, DeadLetterSink as _, Delivery,
    DeliveryHeartbeatConfig, MESSAGE_ID_METADATA_KEY, MessageConsumer as _, MessageId,
    MessageMetadata, MessageProducer as _, NackReason, PublishOutcome, ReplayRequest, ReplaySource,
    SerializedMessage,
};
use plenora_runtime_nats::{
    InfrastructureMode, JetStreamConsumer, JetStreamConsumerConfig, JetStreamProducer,
    JetStreamProducerConfig, NatsConfig, NatsConnection, NatsErrorCategory, NatsTlsConfig,
    ReplayConsumerConfig, TlsMode,
};

const RECEIVE_TIMEOUT: Duration = Duration::from_secs(10);
const STABILITY_MESSAGES: usize = 256;
const STABILITY_TIMEOUT: Duration = Duration::from_mins(1);

#[tokio::test]
#[ignore = "requires PLENORA_NATS_URL pointing to an ephemeral loopback JetStream server"]
#[allow(
    clippy::too_many_lines,
    reason = "one real-server scenario deliberately verifies the complete lifecycle in order"
)]
async fn real_jetstream_publish_redelivery_ack_replay_and_health() -> Result<(), Box<dyn Error>> {
    let server_url = configured_loopback_url()?;
    let suffix = unique_resource_suffix()?;
    let stream: Arc<str> = Arc::from(format!("PLENORA_REAL_{suffix}"));
    let subject: Arc<str> = Arc::from(format!("plenora.real.{suffix}"));
    let dead_letter_subject: Arc<str> = Arc::from(format!("plenora.real.{suffix}.dlq"));
    let operational_durable: Arc<str> = Arc::from(format!("operational_{suffix}"));
    let dead_letter_durable: Arc<str> = Arc::from(format!("dead_letter_{suffix}"));
    let replay_durable: Arc<str> = Arc::from(format!("replay_{suffix}"));
    let health_component: Arc<str> = Arc::from(format!("nats.real.{suffix}"));

    let health_registry = HealthRegistry::new();
    let connection = connect(
        server_url,
        health_registry.clone(),
        Arc::clone(&health_component),
    )
    .await?;
    assert!(connection.is_connected());
    connection.probe().await?;
    assert_health_ready(&health_registry, &health_component);

    let infrastructure = InfrastructureMode::CreateIfMissing {
        stream_subjects: vec![Arc::clone(&subject), Arc::clone(&dead_letter_subject)],
    };
    let mut consumer = connection
        .consumer(JetStreamConsumerConfig {
            stream: Arc::clone(&stream),
            durable_name: Arc::clone(&operational_durable),
            filter_subject: Arc::clone(&subject),
            ack_wait: Duration::from_secs(2),
            heartbeat: Some(DeliveryHeartbeatConfig::new(Duration::from_millis(250), 3)?),
            max_deliver: Some(5),
            max_ack_pending: Some(16),
            max_payload_bytes: 64 * 1024,
            shutdown_nak_delay: Duration::from_millis(100),
            infrastructure: infrastructure.clone(),
        })
        .await?;
    let incompatible_bound_consumer = connection
        .consumer(JetStreamConsumerConfig {
            stream: Arc::clone(&stream),
            durable_name: Arc::clone(&operational_durable),
            filter_subject: Arc::clone(&subject),
            ack_wait: Duration::from_secs(3),
            heartbeat: Some(DeliveryHeartbeatConfig::new(Duration::from_millis(250), 3)?),
            max_deliver: Some(5),
            max_ack_pending: Some(16),
            max_payload_bytes: 64 * 1024,
            shutdown_nak_delay: Duration::from_millis(100),
            infrastructure: InfrastructureMode::BindExisting,
        })
        .await;
    assert!(
        incompatible_bound_consumer
            .is_err_and(|error| error.category() == NatsErrorCategory::Infrastructure)
    );
    let producer = connection.producer(JetStreamProducerConfig {
        subject: Arc::clone(&subject),
        max_payload_bytes: 64 * 1024,
        message_id_metadata_key: None,
    })?;

    let binary_metadata = vec![0, 255, 13, 10, 128, 42];
    let mut headers = MessageMetadata::new();
    headers.insert("test.binary", binary_metadata.clone())?;
    headers.insert_text("test.run", suffix.clone())?;
    headers.insert_text(MESSAGE_ID_METADATA_KEY, MessageId::random().to_string())?;
    let published = SerializedMessage::new(
        "application/vnd.plenora.real-nats+octet-stream",
        format!("real-nats-payload-{suffix}").into_bytes(),
    )
    .with_headers(headers);

    assert_eq!(
        producer.publish(published.clone()).await?,
        PublishOutcome::Confirmed
    );

    heartbeat_past_ack_wait_and_ack(
        &mut consumer,
        &published,
        &binary_metadata,
        &operational_durable,
        &subject,
    )
    .await?;
    assert_eq!(
        producer.publish(published.clone()).await?,
        PublishOutcome::Confirmed
    );
    redeliver_then_resume(
        &mut consumer,
        &published,
        &binary_metadata,
        &operational_durable,
        &subject,
    )
    .await?;

    reconnect_publish_and_ack(
        &connection,
        &producer,
        &mut consumer,
        &published,
        &binary_metadata,
        &operational_durable,
        &subject,
        &health_registry,
        &health_component,
    )
    .await?;

    dead_letter_publish_and_ack(
        &connection,
        &stream,
        &dead_letter_durable,
        &dead_letter_subject,
        &infrastructure,
        &published,
    )
    .await?;

    assert_ne!(operational_durable, replay_durable);
    replay_and_ack(
        &connection,
        &stream,
        &replay_durable,
        &operational_durable,
        &subject,
        &infrastructure,
        &published,
        &binary_metadata,
    )
    .await?;

    connection.probe().await?;
    assert!(connection.is_connected());
    assert_health_ready(&health_registry, &health_component);
    connection.begin_drain().await?;
    wait_until_consumer_ended(&mut consumer).await?;
    wait_until_health_closed(&health_registry, &health_component).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires PLENORA_NATS_URL pointing to an ephemeral loopback JetStream server"]
async fn real_jetstream_bounded_soak_survives_reconnect_without_loss_or_duplicates()
-> Result<(), Box<dyn Error>> {
    tokio::time::timeout(STABILITY_TIMEOUT, real_jetstream_bounded_soak())
        .await
        .map_err(|error| -> Box<dyn Error> { Box::new(error) })??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "the real-server soak keeps provisioning, reconnect, and delivery accounting together"
)]
async fn real_jetstream_bounded_soak() -> Result<(), Box<dyn Error>> {
    let server_url = configured_loopback_url()?;
    let suffix = unique_resource_suffix()?;
    let stream: Arc<str> = Arc::from(format!("PLENORA_STABILITY_{suffix}"));
    let subject: Arc<str> = Arc::from(format!("plenora.stability.{suffix}"));
    let durable: Arc<str> = Arc::from(format!("stability_{suffix}"));
    let health_component: Arc<str> = Arc::from(format!("nats.stability.{suffix}"));
    let health_registry = HealthRegistry::new();
    let connection = connect(
        server_url,
        health_registry.clone(),
        Arc::clone(&health_component),
    )
    .await?;
    let infrastructure = InfrastructureMode::CreateIfMissing {
        stream_subjects: vec![Arc::clone(&subject)],
    };
    let mut consumer = connection
        .consumer(JetStreamConsumerConfig {
            stream,
            durable_name: durable,
            filter_subject: Arc::clone(&subject),
            ack_wait: Duration::from_secs(5),
            heartbeat: Some(DeliveryHeartbeatConfig::new(Duration::from_secs(1), 3)?),
            max_deliver: Some(5),
            max_ack_pending: Some(64),
            max_payload_bytes: 64 * 1024,
            shutdown_nak_delay: Duration::from_millis(100),
            infrastructure,
        })
        .await?;
    let producer = connection.producer(JetStreamProducerConfig {
        subject,
        max_payload_bytes: 64 * 1024,
        message_id_metadata_key: None,
    })?;

    for sequence in 0..STABILITY_MESSAGES {
        if sequence == STABILITY_MESSAGES / 2 {
            let (reconnect, not_ready) = tokio::join!(
                connection.force_reconnect(),
                wait_until_health_not_ready(&health_registry, &health_component)
            );
            reconnect?;
            not_ready?;
            wait_until_reconnected(&connection).await?;
            assert_health_ready(&health_registry, &health_component);
        }
        let mut headers = MessageMetadata::new();
        let _previous =
            headers.insert_text(MESSAGE_ID_METADATA_KEY, MessageId::random().to_string())?;
        let _previous = headers.insert_text("test.stability.sequence", sequence.to_string())?;
        let message = SerializedMessage::new(
            "application/vnd.plenora.stability",
            format!("stability-payload-{sequence}"),
        )
        .with_headers(headers);
        if producer.publish(message).await? != PublishOutcome::Confirmed {
            return Err(io::Error::other("stability publication was not confirmed").into());
        }
    }

    let mut observed = vec![false; STABILITY_MESSAGES];
    for _received in 0..STABILITY_MESSAGES {
        let delivery = receive_bounded(&mut consumer).await?;
        let sequence = delivery
            .message
            .headers
            .get_text("test.stability.sequence")?
            .ok_or_else(|| io::Error::other("stability sequence metadata is missing"))?
            .parse::<usize>()
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("stability sequence metadata is invalid: {error}"),
                )
            })?;
        let seen = observed
            .get_mut(sequence)
            .ok_or_else(|| io::Error::other("stability sequence is outside the published range"))?;
        if *seen {
            return Err(io::Error::other("stability delivery was duplicated").into());
        }
        *seen = true;
        delivery.ack().await?;
    }
    if observed.iter().any(|seen| !seen) {
        return Err(io::Error::other("stability delivery set is incomplete").into());
    }

    connection.probe().await?;
    assert_health_ready(&health_registry, &health_component);
    connection.begin_drain().await?;
    wait_until_consumer_ended(&mut consumer).await?;
    wait_until_health_closed(&health_registry, &health_component).await?;
    Ok(())
}

async fn heartbeat_past_ack_wait_and_ack(
    consumer: &mut JetStreamConsumer,
    published: &SerializedMessage,
    binary_metadata: &[u8],
    operational_durable: &str,
    subject: &str,
) -> Result<(), Box<dyn Error>> {
    let mut delivery = receive_bounded(consumer).await?;
    assert_delivery(
        &delivery,
        published,
        binary_metadata,
        operational_durable,
        subject,
    )?;
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        delivery.heartbeat().await?;
    }
    delivery.ack().await?;

    if tokio::time::timeout(Duration::from_millis(2_250), consumer.receive())
        .await
        .is_ok()
    {
        return Err(io::Error::other(
            "JetStream redelivered a message whose lease was renewed and acknowledged",
        )
        .into());
    }
    Ok(())
}

async fn redeliver_then_resume(
    consumer: &mut JetStreamConsumer,
    published: &SerializedMessage,
    binary_metadata: &[u8],
    operational_durable: &str,
    subject: &str,
) -> Result<(), Box<dyn Error>> {
    let mut first = receive_bounded(consumer).await?;
    assert_delivery(
        &first,
        published,
        binary_metadata,
        operational_durable,
        subject,
    )?;
    assert_eq!(first.attempt, 1);
    assert!(first.heartbeat_config().is_some());
    first.heartbeat().await?;
    let first_attempt = first.attempt;
    first.nack(NackReason::Retryable).await?;

    let retry = receive_bounded(consumer).await?;
    assert_delivery(
        &retry,
        published,
        binary_metadata,
        operational_durable,
        subject,
    )?;
    assert!(retry.attempt > first_attempt);
    let retry_attempt = retry.attempt;
    retry.nack(NackReason::Shutdown).await?;

    let resumed = receive_bounded(consumer).await?;
    assert_delivery(
        &resumed,
        published,
        binary_metadata,
        operational_durable,
        subject,
    )?;
    assert!(resumed.attempt > retry_attempt);
    resumed.ack().await?;
    Ok(())
}

async fn dead_letter_publish_and_ack(
    connection: &NatsConnection,
    stream: &Arc<str>,
    durable: &Arc<str>,
    subject: &Arc<str>,
    infrastructure: &InfrastructureMode,
    published: &SerializedMessage,
) -> Result<(), Box<dyn Error>> {
    let producer = connection.producer(JetStreamProducerConfig {
        subject: Arc::clone(subject),
        max_payload_bytes: 64 * 1024,
        message_id_metadata_key: Some(Arc::from(DEAD_LETTER_ID_METADATA_KEY)),
    })?;
    let mut consumer = connection
        .consumer(JetStreamConsumerConfig {
            stream: Arc::clone(stream),
            durable_name: Arc::clone(durable),
            filter_subject: Arc::clone(subject),
            ack_wait: Duration::from_secs(2),
            heartbeat: None,
            max_deliver: Some(5),
            max_ack_pending: Some(4),
            max_payload_bytes: 64 * 1024,
            shutdown_nak_delay: Duration::from_millis(100),
            infrastructure: infrastructure.clone(),
        })
        .await?;
    let outcome = producer
        .publish_dead_letter(DeadLetter {
            message: published.clone(),
            reason: Arc::from("handler_failed"),
            attempts: 4,
            failed_at: SystemTime::now().into(),
        })
        .await?;
    assert_eq!(outcome, PublishOutcome::Confirmed);

    let delivery = receive_bounded(&mut consumer).await?;
    assert_eq!(delivery.message.bytes, published.bytes);
    assert_eq!(
        delivery
            .message
            .headers
            .get_text(DEAD_LETTER_REASON_METADATA_KEY)?,
        Some("handler_failed")
    );
    assert_eq!(
        delivery
            .message
            .headers
            .get_text(DEAD_LETTER_ATTEMPTS_METADATA_KEY)?,
        Some("4")
    );
    assert_eq!(
        delivery.broker_metadata.get_text("plenora.nats.subject")?,
        Some(subject.as_ref())
    );
    delivery.ack().await?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the integration helper keeps every asserted broker identity explicit"
)]
async fn reconnect_publish_and_ack(
    connection: &NatsConnection,
    producer: &JetStreamProducer,
    consumer: &mut JetStreamConsumer,
    published: &SerializedMessage,
    binary_metadata: &[u8],
    operational_durable: &str,
    subject: &str,
    health_registry: &HealthRegistry,
    health_component: &str,
) -> Result<(), Box<dyn Error>> {
    let (reconnect, not_ready) = tokio::join!(
        connection.force_reconnect(),
        wait_until_health_not_ready(health_registry, health_component)
    );
    reconnect?;
    not_ready?;
    wait_until_reconnected(connection).await?;
    assert!(connection.is_connected());
    assert_health_ready(health_registry, health_component);
    assert_eq!(
        producer.publish(published.clone()).await?,
        PublishOutcome::Confirmed
    );
    let delivery = receive_bounded(consumer).await?;
    assert_delivery(
        &delivery,
        published,
        binary_metadata,
        operational_durable,
        subject,
    )?;
    delivery.ack().await?;
    Ok(())
}

async fn wait_until_health_not_ready(
    registry: &HealthRegistry,
    component_name: &str,
) -> Result<(), Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + RECEIVE_TIMEOUT;
    loop {
        let is_not_ready = registry.readiness().components.iter().any(|component| {
            component.component.as_ref() == component_name
                && component.status == ReadinessStatus::NotReady
        });
        if is_not_ready {
            assert_health_not_ready(registry, component_name);
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "NATS readiness did not report the reconnect transition",
            )
            .into());
        }
        tokio::task::yield_now().await;
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the integration helper keeps operational and replay identities explicit"
)]
async fn replay_and_ack(
    connection: &NatsConnection,
    stream: &Arc<str>,
    replay_durable: &Arc<str>,
    operational_durable: &Arc<str>,
    subject: &Arc<str>,
    infrastructure: &InfrastructureMode,
    published: &SerializedMessage,
    binary_metadata: &[u8],
) -> Result<(), Box<dyn Error>> {
    let mut replay = connection
        .replay_consumer(
            ReplayConsumerConfig {
                stream: Arc::clone(stream),
                durable_name: Arc::clone(replay_durable),
                operational_durable_name: Arc::clone(operational_durable),
                filter_subject: Arc::clone(subject),
                ack_wait: Duration::from_secs(2),
                heartbeat: Some(DeliveryHeartbeatConfig::new(Duration::from_millis(250), 3)?),
                max_deliver: Some(5),
                max_ack_pending: Some(16),
                max_payload_bytes: 64 * 1024,
                shutdown_nak_delay: Duration::from_millis(100),
                infrastructure: infrastructure.clone(),
            },
            ReplayRequest {
                source: ReplaySource::All,
            },
        )
        .await?;
    let replayed = receive_bounded(&mut replay).await?;
    assert_delivery(
        &replayed,
        published,
        binary_metadata,
        replay_durable,
        subject,
    )?;
    assert_eq!(replayed.attempt, 1);
    replayed.ack().await?;
    Ok(())
}

async fn connect(
    server_url: String,
    health_registry: HealthRegistry,
    health_component: Arc<str>,
) -> Result<NatsConnection, Box<dyn Error>> {
    let config = NatsConfig {
        servers: vec![Arc::from(server_url)],
        tls: NatsTlsConfig {
            mode: TlsMode::AllowPlaintext,
            ..NatsTlsConfig::default()
        },
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(1),
        max_reconnects: Some(10),
        health_component,
        ..NatsConfig::default()
    };
    Ok(NatsConnection::connect(config, health_registry).await?)
}

fn configured_loopback_url() -> Result<String, Box<dyn Error>> {
    let url = env::var("PLENORA_NATS_URL").map_err(|error| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("PLENORA_NATS_URL is required: {error}"),
        )
    })?;
    let authority = url.strip_prefix("nats://").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "PLENORA_NATS_URL must use the plaintext nats:// scheme",
        )
    })?;
    let address: SocketAddr = authority.parse().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("PLENORA_NATS_URL must contain a numeric host and port: {error}"),
        )
    })?;
    if !address.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "PLENORA_NATS_URL must resolve directly to a loopback address",
        )
        .into());
    }
    Ok(url)
}

fn unique_resource_suffix() -> Result<String, std::time::SystemTimeError> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(format!("{}_{timestamp}", std::process::id()))
}

async fn receive_bounded(consumer: &mut JetStreamConsumer) -> Result<Delivery, Box<dyn Error>> {
    tokio::time::timeout(RECEIVE_TIMEOUT, consumer.receive())
        .await
        .map_err(|error| -> Box<dyn Error> { Box::new(error) })??
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "JetStream consumer ended before delivering a message",
            )
            .into()
        })
}

async fn wait_until_reconnected(connection: &NatsConnection) -> Result<(), Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + RECEIVE_TIMEOUT;
    loop {
        if connection.is_connected() && connection.probe().await.is_ok() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "NATS client did not reconnect within the bounded timeout",
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_until_consumer_ended(consumer: &mut JetStreamConsumer) -> Result<(), Box<dyn Error>> {
    let delivery = tokio::time::timeout(RECEIVE_TIMEOUT, consumer.receive())
        .await
        .map_err(|error| -> Box<dyn Error> { Box::new(error) })??;
    if delivery.is_some() {
        return Err(
            io::Error::other("draining NATS consumer produced an unexpected delivery").into(),
        );
    }
    Ok(())
}

async fn wait_until_health_closed(
    registry: &HealthRegistry,
    component_name: &str,
) -> Result<(), Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + RECEIVE_TIMEOUT;
    loop {
        let is_closed = registry.health().components.iter().any(|component| {
            component.component.as_ref() == component_name
                && component.status == HealthStatus::Unhealthy
        });
        if is_closed {
            assert_health_closed(registry, component_name);
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "NATS health did not report closure within the bounded timeout",
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn assert_delivery(
    delivery: &Delivery,
    expected: &SerializedMessage,
    binary_metadata: &[u8],
    durable_name: &str,
    subject: &str,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(&delivery.message, expected);
    assert_eq!(
        delivery
            .message
            .headers
            .get("test.binary")
            .map(AsRef::as_ref),
        Some(binary_metadata)
    );
    assert_eq!(
        delivery.broker_metadata.get_text("plenora.nats.consumer")?,
        Some(durable_name)
    );
    assert_eq!(
        delivery.broker_metadata.get_text("plenora.nats.subject")?,
        Some(subject)
    );
    Ok(())
}

fn assert_health_ready(registry: &HealthRegistry, component_name: &str) {
    let health = registry.health();
    assert_eq!(health.status, HealthStatus::Healthy);
    assert!(health.components.iter().any(|component| {
        component.component.as_ref() == component_name && component.status == HealthStatus::Healthy
    }));

    let readiness = registry.readiness();
    assert_eq!(readiness.status, ReadinessStatus::Ready);
    assert!(readiness.components.iter().any(|component| {
        component.component.as_ref() == component_name && component.status == ReadinessStatus::Ready
    }));
}

fn assert_health_not_ready(registry: &HealthRegistry, component_name: &str) {
    let health = registry.health();
    assert_eq!(health.status, HealthStatus::Degraded);
    assert!(health.components.iter().any(|component| {
        component.component.as_ref() == component_name && component.status == HealthStatus::Degraded
    }));

    let readiness = registry.readiness();
    assert_eq!(readiness.status, ReadinessStatus::NotReady);
    assert!(readiness.components.iter().any(|component| {
        component.component.as_ref() == component_name
            && component.status == ReadinessStatus::NotReady
    }));
}

fn assert_health_closed(registry: &HealthRegistry, component_name: &str) {
    let health = registry.health();
    assert_eq!(health.status, HealthStatus::Unhealthy);
    assert!(health.components.iter().any(|component| {
        component.component.as_ref() == component_name
            && component.status == HealthStatus::Unhealthy
    }));

    let readiness = registry.readiness();
    assert_eq!(readiness.status, ReadinessStatus::NotReady);
    assert!(readiness.components.iter().any(|component| {
        component.component.as_ref() == component_name
            && component.status == ReadinessStatus::NotReady
    }));
}
