//! Opt-in NATS-to-Apalis dead-letter integration coverage.

use std::{
    env,
    error::Error,
    fmt::{self, Display, Formatter},
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use plenora_runtime_apalis::{ApalisAdapterConfig, BrokerWorkerRunner};
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata, SystemClock};
use plenora_runtime_messaging::{
    CORRELATION_ID_METADATA_KEY, CorrelationId, DEAD_LETTER_ATTEMPTS_METADATA_KEY,
    DEAD_LETTER_ID_METADATA_KEY, DEAD_LETTER_REASON_METADATA_KEY, DeliveryHeartbeatConfig,
    MESSAGE_ID_METADATA_KEY, MessageCodec, MessageConsumer as _, MessageId, MessageMetadata,
    MessageProducer as _, PublishOutcome, RetryDecision, RetryPolicy, SerializedMessage,
};
use plenora_runtime_nats::{
    InfrastructureMode, JetStreamConsumerConfig, JetStreamProducerConfig, NatsConfig,
    NatsConnection, NatsTlsConfig, TlsMode,
};
use plenora_runtime_worker::{
    MetadataMessageDecoder, WorkerConcurrency, WorkerConfig, WorkerContext, WorkerHandler,
    WorkerInstanceHeartbeat, WorkerInstanceHeartbeatConfig, WorkerInstanceHeartbeatObserver,
    WorkerInstanceStatus,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug)]
struct TestError;

impl Display for TestError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("test operation failed")
    }
}

impl Error for TestError {}

#[derive(Clone, Copy, Debug)]
struct TextCodec;

impl MessageCodec<String> for TextCodec {
    type Error = TestError;

    fn encode(&self, value: &String) -> Result<SerializedMessage, Self::Error> {
        Ok(SerializedMessage::new("text/plain", value.clone()))
    }

    fn decode(&self, message: &SerializedMessage) -> Result<String, Self::Error> {
        String::from_utf8(message.bytes.to_vec()).map_err(|_error| TestError)
    }
}

#[derive(Clone, Debug)]
struct FailingHandler {
    invocations: Arc<AtomicUsize>,
}

#[async_trait]
impl WorkerHandler<String> for FailingHandler {
    type Error = TestError;

    async fn handle(&self, _context: WorkerContext, _message: String) -> Result<(), Self::Error> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        Err(TestError)
    }
}

#[derive(Clone, Copy, Debug)]
struct DeadLetterPolicy;

impl RetryPolicy<TestError> for DeadLetterPolicy {
    fn decide(&self, _attempt: u32, _error: &TestError) -> RetryDecision {
        RetryDecision::DeadLetter
    }
}

#[derive(Clone, Debug, Default)]
struct InstanceHeartbeatRecorder {
    heartbeats: Arc<Mutex<Vec<WorkerInstanceHeartbeat>>>,
}

impl InstanceHeartbeatRecorder {
    fn snapshots(&self) -> Vec<WorkerInstanceHeartbeat> {
        match self.heartbeats.lock() {
            Ok(heartbeats) => heartbeats.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
}

impl WorkerInstanceHeartbeatObserver for InstanceHeartbeatRecorder {
    fn record(&self, heartbeat: WorkerInstanceHeartbeat) {
        match self.heartbeats.lock() {
            Ok(mut heartbeats) => heartbeats.push(heartbeat),
            Err(poisoned) => poisoned.into_inner().push(heartbeat),
        }
    }
}

#[tokio::test]
#[ignore = "requires PLENORA_NATS_URL pointing to an ephemeral loopback JetStream server"]
async fn nats_apalis_handler_routes_confirmed_dlq_then_terms_original() -> Result<(), Box<dyn Error>>
{
    let server_url = configured_loopback_url()?;
    let suffix = unique_resource_suffix()?;
    let stream: Arc<str> = Arc::from(format!("PLENORA_WORKER_DLQ_{suffix}"));
    let work_subject: Arc<str> = Arc::from(format!("plenora.worker.{suffix}.work"));
    let dead_letter_subject: Arc<str> = Arc::from(format!("plenora.worker.{suffix}.dlq"));
    let runtime = RuntimeHandle::new(ServiceMetadata::new(
        "nats-worker-dlq-test",
        "0.1.0",
        suffix.clone(),
    ));
    let connection = NatsConnection::connect(
        nats_config(server_url, Arc::from(format!("nats.worker.{suffix}"))),
        runtime.health_registry(),
    )
    .await?;
    let infrastructure = InfrastructureMode::CreateIfMissing {
        stream_subjects: vec![Arc::clone(&work_subject), Arc::clone(&dead_letter_subject)],
    };
    let operational = connection
        .consumer(consumer_config(
            Arc::clone(&stream),
            Arc::from(format!("worker_{suffix}")),
            Arc::clone(&work_subject),
            infrastructure.clone(),
        )?)
        .await?;
    let mut dead_letters = connection
        .consumer(consumer_config(
            Arc::clone(&stream),
            Arc::from(format!("dlq_{suffix}")),
            Arc::clone(&dead_letter_subject),
            infrastructure,
        )?)
        .await?;
    let work_producer = connection.producer(producer_config(Arc::clone(&work_subject)))?;
    let dead_letter_producer = connection.producer(dead_letter_producer_config(Arc::clone(
        &dead_letter_subject,
    )))?;
    let invocations = Arc::new(AtomicUsize::new(0));
    let heartbeat_recorder = InstanceHeartbeatRecorder::default();
    let runner = BrokerWorkerRunner::new(
        operational,
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        FailingHandler {
            invocations: Arc::clone(&invocations),
        },
        DeadLetterPolicy,
        ApalisAdapterConfig::new(
            "nats-worker-dlq",
            WorkerConfig::new(WorkerConcurrency::new(1)?, Duration::from_secs(3)),
        )?,
        runtime.shutdown_signal(),
    )?
    .with_dead_letter_sink(dead_letter_producer)
    .with_instance_heartbeat(
        runtime.metadata(),
        WorkerInstanceHeartbeatConfig::new(Duration::from_millis(100))?,
        Arc::new(heartbeat_recorder.clone()),
        Arc::new(SystemClock),
    );
    let runner_task = tokio::spawn(runner.run());

    assert_eq!(
        work_producer.publish(worker_message("poison")?).await?,
        PublishOutcome::Confirmed
    );
    let dead_letter = receive_bounded(&mut dead_letters).await?;
    assert_eq!(dead_letter.message.bytes.as_ref(), b"poison");
    assert_eq!(
        dead_letter
            .message
            .headers
            .get_text(DEAD_LETTER_REASON_METADATA_KEY)?,
        Some("handler_failed")
    );
    assert_eq!(
        dead_letter
            .message
            .headers
            .get_text(DEAD_LETTER_ATTEMPTS_METADATA_KEY)?,
        Some("1")
    );
    dead_letter.ack().await?;

    tokio::time::sleep(Duration::from_millis(2_250)).await;
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    let _started = runtime.request_shutdown();
    tokio::time::timeout(TEST_TIMEOUT, runner_task)
        .await
        .map_err(|error| -> Box<dyn Error> { Box::new(error) })???;
    assert_worker_lifecycle(&heartbeat_recorder.snapshots());
    connection.begin_drain().await?;
    Ok(())
}

fn assert_worker_lifecycle(heartbeats: &[WorkerInstanceHeartbeat]) {
    assert_eq!(
        heartbeats.first().map(|heartbeat| heartbeat.status),
        Some(WorkerInstanceStatus::Starting)
    );
    assert!(
        heartbeats
            .iter()
            .any(|heartbeat| heartbeat.status == WorkerInstanceStatus::Ready)
    );
    assert!(
        heartbeats
            .iter()
            .any(|heartbeat| heartbeat.status == WorkerInstanceStatus::Draining)
    );
    assert_eq!(
        heartbeats.last().map(|heartbeat| heartbeat.status),
        Some(WorkerInstanceStatus::Stopped)
    );
}

fn consumer_config(
    stream: Arc<str>,
    durable_name: Arc<str>,
    filter_subject: Arc<str>,
    infrastructure: InfrastructureMode,
) -> Result<JetStreamConsumerConfig, Box<dyn Error>> {
    Ok(JetStreamConsumerConfig {
        stream,
        durable_name,
        filter_subject,
        ack_wait: Duration::from_secs(2),
        heartbeat: Some(DeliveryHeartbeatConfig::new(Duration::from_millis(250), 3)?),
        max_deliver: Some(5),
        max_ack_pending: Some(4),
        max_payload_bytes: 64 * 1024,
        shutdown_nak_delay: Duration::from_millis(100),
        infrastructure,
    })
}

fn producer_config(subject: Arc<str>) -> JetStreamProducerConfig {
    JetStreamProducerConfig {
        subject,
        max_payload_bytes: 64 * 1024,
        message_id_metadata_key: Some(Arc::from(MESSAGE_ID_METADATA_KEY)),
    }
}

fn dead_letter_producer_config(subject: Arc<str>) -> JetStreamProducerConfig {
    JetStreamProducerConfig {
        subject,
        max_payload_bytes: 64 * 1024,
        message_id_metadata_key: Some(Arc::from(DEAD_LETTER_ID_METADATA_KEY)),
    }
}

fn nats_config(server_url: String, health_component: Arc<str>) -> NatsConfig {
    NatsConfig {
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
    }
}

fn worker_message(payload: &str) -> Result<SerializedMessage, Box<dyn Error>> {
    let mut metadata = MessageMetadata::new();
    let _previous =
        metadata.insert_text(MESSAGE_ID_METADATA_KEY, MessageId::random().to_string())?;
    let _previous = metadata.insert_text(
        CORRELATION_ID_METADATA_KEY,
        CorrelationId::random().to_string(),
    )?;
    Ok(SerializedMessage::new("text/plain", payload.to_owned()).with_headers(metadata))
}

async fn receive_bounded(
    consumer: &mut plenora_runtime_nats::JetStreamConsumer,
) -> Result<plenora_runtime_messaging::Delivery, Box<dyn Error>> {
    tokio::time::timeout(TEST_TIMEOUT, consumer.receive())
        .await
        .map_err(|error| -> Box<dyn Error> { Box::new(error) })??
        .ok_or_else(|| io::Error::other("DLQ consumer ended before delivery").into())
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
            "PLENORA_NATS_URL must use nats://",
        )
    })?;
    let address: SocketAddr = authority.parse()?;
    if !address.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "PLENORA_NATS_URL must point directly to loopback",
        )
        .into());
    }
    Ok(url)
}

fn unique_resource_suffix() -> Result<String, std::time::SystemTimeError> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(format!("{}_{timestamp}", std::process::id()))
}
