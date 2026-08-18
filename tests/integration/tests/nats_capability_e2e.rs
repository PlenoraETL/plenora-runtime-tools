//! Opt-in, full-chain NATS capability worker qualification.

use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fmt::{self, Display, Formatter},
    future, io,
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use plenora_runtime_apalis::{ApalisAdapterConfig, BrokerWorkerRunner};
use plenora_runtime_capabilities::{
    CapabilityDispatcher, CapabilityDispatcherConfig, CapabilityFailure, CapabilityHandler,
    CapabilityId, CapabilityMessageCodec, CapabilityRegistryBuilder, CapabilityRegistryConfig,
    CapabilityRemoteEffect, CapabilityRequest, OperationName,
};
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata, SystemClock};
use plenora_runtime_messaging::{
    CORRELATION_ID_METADATA_KEY, CorrelationId, DEAD_LETTER_REASON_METADATA_KEY,
    DeliveryHeartbeatConfig, ExponentialBackoff, ExponentialBackoffConfig, MESSAGE_ID_METADATA_KEY,
    MessageCodec, MessageConsumer as _, MessageId, MessageProducer as _, PublishOutcome,
    RetryErrorClass, SerializedMessage,
};
use plenora_runtime_nats::{
    InfrastructureMode, JetStreamConsumerConfig, JetStreamProducerConfig, NatsConfig,
    NatsConnection, NatsTlsConfig, TlsMode,
};
use plenora_runtime_worker::{
    MetadataMessageDecoder, WorkerConcurrency, WorkerConfig, WorkerContext,
    WorkerInstanceHeartbeat, WorkerInstanceHeartbeatConfig, WorkerInstanceHeartbeatObserver,
    WorkerInstanceStatus,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONDITION_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(10);
const CAPABILITY_NAME: &str = "plenora.fake-tools";

#[derive(Clone, Copy, Debug)]
enum FakeAdapterError {
    Retryable,
    DeadLetter,
    Cancelled,
}

impl Display for FakeAdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("fake adapter operation failed")
    }
}

impl Error for FakeAdapterError {}

#[derive(Clone, Debug, Default)]
struct FakeCapabilityState {
    attempts: Arc<Mutex<BTreeMap<String, Vec<u32>>>>,
    effects: Arc<Mutex<BTreeMap<String, Vec<MessageId>>>>,
}

impl FakeCapabilityState {
    fn record(&self, operation: &str, attempt: u32) {
        lock(&self.attempts)
            .entry(operation.to_owned())
            .or_default()
            .push(attempt);
    }

    fn attempts(&self, operation: &str) -> Vec<u32> {
        lock(&self.attempts)
            .get(operation)
            .cloned()
            .unwrap_or_default()
    }

    fn record_effect_once(&self, operation: &str, message_id: MessageId) {
        let mut effects = lock(&self.effects);
        let messages = effects.entry(operation.to_owned()).or_default();
        if !messages.contains(&message_id) {
            messages.push(message_id);
        }
    }

    fn effect_count(&self, operation: &str) -> usize {
        lock(&self.effects).get(operation).map_or(0, Vec::len)
    }
}

#[derive(Clone, Debug)]
struct FakeCapabilityHandler {
    state: FakeCapabilityState,
}

#[async_trait]
impl CapabilityHandler for FakeCapabilityHandler {
    async fn invoke(
        &self,
        context: WorkerContext,
        request: CapabilityRequest,
    ) -> Result<(), CapabilityFailure> {
        let operation = request.operation().as_str();
        self.state.record(operation, context.attempt);
        match operation {
            "retry-once" if context.attempt == 1 => Err(failure(
                RetryErrorClass::Retryable,
                FakeAdapterError::Retryable,
            )),
            "succeed" | "retry-once" => {
                self.state.record_effect_once(operation, context.message_id);
                Ok(())
            }
            "heartbeat" => {
                tokio::time::sleep(Duration::from_millis(2_500)).await;
                self.state.record_effect_once(operation, context.message_id);
                Ok(())
            }
            "cancel" => {
                let _reason = context.cancelled().await;
                Err(failure(
                    RetryErrorClass::Permanent,
                    FakeAdapterError::Cancelled,
                ))
            }
            _ => Err(failure(
                RetryErrorClass::DeadLetter,
                FakeAdapterError::DeadLetter,
            )),
        }
    }
}

fn failure(class: RetryErrorClass, source: FakeAdapterError) -> CapabilityFailure {
    CapabilityFailure::new(class, CapabilityRemoteEffect::NotStarted, source)
}

#[derive(Clone, Debug, Default)]
struct HeartbeatRecorder {
    snapshots: Arc<Mutex<Vec<WorkerInstanceHeartbeat>>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MultiWorkerEvent {
    worker: &'static str,
    operation: String,
    message_id: MessageId,
    attempt: u32,
}

#[derive(Clone, Debug, Default)]
struct MultiWorkerState {
    events: Arc<Mutex<Vec<MultiWorkerEvent>>>,
    effects: Arc<Mutex<BTreeMap<String, Vec<MessageId>>>>,
}

impl MultiWorkerState {
    fn record(&self, event: MultiWorkerEvent) {
        lock(&self.events).push(event);
    }

    fn record_effect_once(&self, operation: &str, message_id: MessageId) {
        let mut effects = lock(&self.effects);
        let messages = effects.entry(operation.to_owned()).or_default();
        if !messages.contains(&message_id) {
            messages.push(message_id);
        }
    }

    fn events(&self) -> Vec<MultiWorkerEvent> {
        lock(&self.events).clone()
    }

    fn effect_count(&self, operation: &str) -> usize {
        lock(&self.effects).get(operation).map_or(0, Vec::len)
    }
}

#[derive(Clone, Debug)]
struct MultiWorkerHandler {
    worker: &'static str,
    block_crash: bool,
    state: MultiWorkerState,
}

#[async_trait]
impl CapabilityHandler for MultiWorkerHandler {
    async fn invoke(
        &self,
        context: WorkerContext,
        request: CapabilityRequest,
    ) -> Result<(), CapabilityFailure> {
        let operation = request.operation().as_str();
        self.state.record(MultiWorkerEvent {
            worker: self.worker,
            operation: operation.to_owned(),
            message_id: context.message_id,
            attempt: context.attempt,
        });
        if self.block_crash && operation == "crash" {
            return future::pending::<Result<(), CapabilityFailure>>().await;
        }
        if operation == "saturate" {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.state.record_effect_once(operation, context.message_id);
        Ok(())
    }
}

impl HeartbeatRecorder {
    fn snapshots(&self) -> Vec<WorkerInstanceHeartbeat> {
        lock(&self.snapshots).clone()
    }
}

impl WorkerInstanceHeartbeatObserver for HeartbeatRecorder {
    fn record(&self, heartbeat: WorkerInstanceHeartbeat) {
        lock(&self.snapshots).push(heartbeat);
    }
}

#[tokio::test]
#[ignore = "requires PLENORA_NATS_URL pointing to an ephemeral loopback JetStream server"]
async fn nats_capability_worker_covers_success_retry_dlq_cancellation_heartbeat_and_shutdown()
-> Result<(), Box<dyn Error>> {
    tokio::time::timeout(TEST_TIMEOUT, full_chain_scenario())
        .await
        .map_err(|error| -> Box<dyn Error> { Box::new(error) })??;
    Ok(())
}

#[tokio::test]
#[ignore = "requires PLENORA_NATS_URL pointing to an ephemeral loopback JetStream server"]
async fn nats_multi_worker_crash_redelivery_restart_and_saturation_are_bounded()
-> Result<(), Box<dyn Error>> {
    tokio::time::timeout(Duration::from_secs(45), multi_worker_scenario())
        .await
        .map_err(|error| -> Box<dyn Error> { Box::new(error) })??;
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "the multi-instance fault scenario keeps crash, restart, and distribution assertions together"
)]
async fn multi_worker_scenario() -> Result<(), Box<dyn Error>> {
    const SATURATION_MESSAGES: usize = 16;

    let server_url = configured_loopback_url()?;
    let suffix = unique_resource_suffix()?;
    let stream: Arc<str> = Arc::from(format!("PLENORA_MULTI_WORKER_{suffix}"));
    let subject: Arc<str> = Arc::from(format!("plenora.multi.{suffix}.work"));
    let durable: Arc<str> = Arc::from(format!("multi_{suffix}"));
    let connection = NatsConnection::connect(
        nats_config(server_url, Arc::from(format!("nats.multi.{suffix}"))),
        plenora_runtime_core::HealthRegistry::new(),
    )
    .await?;
    let infrastructure = InfrastructureMode::CreateIfMissing {
        stream_subjects: vec![Arc::clone(&subject)],
    };
    let first_consumer = connection
        .consumer(consumer_config(
            Arc::clone(&stream),
            Arc::clone(&durable),
            Arc::clone(&subject),
            infrastructure,
        )?)
        .await?;
    let producer = connection.producer(producer_config(Arc::clone(&subject), None))?;
    let state = MultiWorkerState::default();

    let first_runtime = RuntimeHandle::new(ServiceMetadata::new(
        "multi-worker",
        "0.1.0",
        format!("first-{suffix}"),
    ));
    let first_runner = BrokerWorkerRunner::new(
        first_consumer,
        MetadataMessageDecoder::<_, CapabilityRequest>::new(CapabilityMessageCodec),
        dispatcher_for(MultiWorkerHandler {
            worker: "worker-one",
            block_crash: true,
            state: state.clone(),
        })?,
        retry_policy()?,
        worker_config("worker-one", 1)?,
        first_runtime.shutdown_signal(),
    )?;
    let first_task = tokio::spawn(first_runner.run());
    let crash_message = publish_operation(&producer, "crash").await?;
    wait_until("first worker accepted crash message", || {
        state.events().iter().any(|event| {
            event.worker == "worker-one"
                && event.operation == "crash"
                && event.message_id == crash_message
        })
    })
    .await?;
    first_task.abort();
    assert!(first_task.await.is_err());

    let second_runtime = RuntimeHandle::new(ServiceMetadata::new(
        "multi-worker",
        "0.1.0",
        format!("second-{suffix}"),
    ));
    let second_consumer = connection
        .consumer(consumer_config(
            Arc::clone(&stream),
            Arc::clone(&durable),
            Arc::clone(&subject),
            InfrastructureMode::BindExisting,
        )?)
        .await?;
    let second_runner = BrokerWorkerRunner::new(
        second_consumer,
        MetadataMessageDecoder::<_, CapabilityRequest>::new(CapabilityMessageCodec),
        dispatcher_for(MultiWorkerHandler {
            worker: "worker-two",
            block_crash: false,
            state: state.clone(),
        })?,
        retry_policy()?,
        worker_config("worker-two", 1)?,
        second_runtime.shutdown_signal(),
    )?;
    let second_task = tokio::spawn(second_runner.run());
    wait_until("crashed delivery was reassigned", || {
        state.events().iter().any(|event| {
            event.worker == "worker-two"
                && event.operation == "crash"
                && event.message_id == crash_message
                && event.attempt > 1
        })
    })
    .await?;
    wait_until("reassigned delivery applied one effect", || {
        state.effect_count("crash") == 1
    })
    .await?;

    let replacement_runtime = RuntimeHandle::new(ServiceMetadata::new(
        "multi-worker",
        "0.1.0",
        format!("replacement-{suffix}"),
    ));
    let replacement_consumer = connection
        .consumer(consumer_config(
            Arc::clone(&stream),
            Arc::clone(&durable),
            Arc::clone(&subject),
            InfrastructureMode::BindExisting,
        )?)
        .await?;
    let replacement_runner = BrokerWorkerRunner::new(
        replacement_consumer,
        MetadataMessageDecoder::<_, CapabilityRequest>::new(CapabilityMessageCodec),
        dispatcher_for(MultiWorkerHandler {
            worker: "worker-one-restarted",
            block_crash: false,
            state: state.clone(),
        })?,
        retry_policy()?,
        worker_config("worker-one-restarted", 1)?,
        replacement_runtime.shutdown_signal(),
    )?;
    let replacement_task = tokio::spawn(replacement_runner.run());

    for _sequence in 0..SATURATION_MESSAGES {
        let _message_id = publish_operation(&producer, "saturate").await?;
    }
    wait_until("saturated messages applied exactly once", || {
        state.effect_count("saturate") == SATURATION_MESSAGES
    })
    .await?;
    let saturation_events = state
        .events()
        .into_iter()
        .filter(|event| event.operation == "saturate")
        .collect::<Vec<_>>();
    assert!(
        saturation_events
            .iter()
            .any(|event| event.worker == "worker-two")
    );
    assert!(
        saturation_events
            .iter()
            .any(|event| event.worker == "worker-one-restarted")
    );

    let _second_shutdown = second_runtime.request_shutdown();
    let _replacement_shutdown = replacement_runtime.request_shutdown();
    tokio::time::timeout(TEST_TIMEOUT, second_task)
        .await
        .map_err(|error| -> Box<dyn Error> { Box::new(error) })???;
    tokio::time::timeout(TEST_TIMEOUT, replacement_task)
        .await
        .map_err(|error| -> Box<dyn Error> { Box::new(error) })???;
    connection.begin_drain().await?;
    Ok(())
}

fn dispatcher_for<H>(handler: H) -> Result<CapabilityDispatcher, Box<dyn Error>>
where
    H: CapabilityHandler + 'static,
{
    let mut registry = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(1)?)?;
    registry.register(CapabilityId::new(CAPABILITY_NAME, 1)?, handler)?;
    Ok(CapabilityDispatcher::new(
        registry.build(),
        CapabilityDispatcherConfig::new(64 * 1024)?,
    )?)
}

fn retry_policy() -> Result<ExponentialBackoff, Box<dyn Error>> {
    Ok(ExponentialBackoff::new(ExponentialBackoffConfig {
        initial_delay: Duration::from_millis(25),
        max_delay: Duration::from_millis(25),
        multiplier: 1,
        max_attempts: 3,
        ..ExponentialBackoffConfig::default()
    })?)
}

fn worker_config(
    name: &'static str,
    max_in_flight: usize,
) -> Result<ApalisAdapterConfig, Box<dyn Error>> {
    Ok(ApalisAdapterConfig::new(
        name,
        WorkerConfig::new(
            WorkerConcurrency::new(max_in_flight)?,
            Duration::from_secs(3),
        ),
    )?)
}

#[allow(
    clippy::too_many_lines,
    reason = "the real-broker acceptance scenario keeps all externally observed effects together"
)]
async fn full_chain_scenario() -> Result<(), Box<dyn Error>> {
    let server_url = configured_loopback_url()?;
    let suffix = unique_resource_suffix()?;
    let stream: Arc<str> = Arc::from(format!("PLENORA_CAPABILITY_{suffix}"));
    let work_subject: Arc<str> = Arc::from(format!("plenora.capability.{suffix}.work"));
    let dlq_subject: Arc<str> = Arc::from(format!("plenora.capability.{suffix}.dlq"));
    let runtime = RuntimeHandle::new(ServiceMetadata::new(
        "capability-e2e",
        "0.1.0",
        suffix.clone(),
    ));
    let connection = NatsConnection::connect(
        nats_config(server_url, Arc::from(format!("nats.capability.{suffix}"))),
        runtime.health_registry(),
    )
    .await?;
    let infrastructure = InfrastructureMode::CreateIfMissing {
        stream_subjects: vec![Arc::clone(&work_subject), Arc::clone(&dlq_subject)],
    };
    let consumer = connection
        .consumer(consumer_config(
            Arc::clone(&stream),
            Arc::from(format!("capability_{suffix}")),
            Arc::clone(&work_subject),
            infrastructure.clone(),
        )?)
        .await?;
    let mut dead_letters = connection
        .consumer(consumer_config(
            Arc::clone(&stream),
            Arc::from(format!("capability_dlq_{suffix}")),
            Arc::clone(&dlq_subject),
            infrastructure,
        )?)
        .await?;
    let producer = connection.producer(producer_config(Arc::clone(&work_subject), None))?;
    let dlq_producer = connection.producer(producer_config(
        Arc::clone(&dlq_subject),
        Some(Arc::from(
            plenora_runtime_messaging::DEAD_LETTER_ID_METADATA_KEY,
        )),
    ))?;

    let state = FakeCapabilityState::default();
    let mut registry = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(1)?)?;
    registry.register(
        CapabilityId::new(CAPABILITY_NAME, 1)?,
        FakeCapabilityHandler {
            state: state.clone(),
        },
    )?;
    let dispatcher = CapabilityDispatcher::new(
        registry.build(),
        CapabilityDispatcherConfig::new(64 * 1024)?,
    )?;
    let retry = ExponentialBackoff::new(ExponentialBackoffConfig {
        initial_delay: Duration::from_millis(25),
        max_delay: Duration::from_millis(25),
        multiplier: 1,
        max_attempts: 3,
        ..ExponentialBackoffConfig::default()
    })?;
    let heartbeat_recorder = HeartbeatRecorder::default();
    let runner = BrokerWorkerRunner::new(
        consumer,
        MetadataMessageDecoder::<_, CapabilityRequest>::new(CapabilityMessageCodec),
        dispatcher,
        retry,
        ApalisAdapterConfig::new(
            "capability-e2e",
            WorkerConfig::new(WorkerConcurrency::new(2)?, Duration::from_secs(3)),
        )?,
        runtime.shutdown_signal(),
    )?
    .with_dead_letter_sink(dlq_producer)
    .with_instance_heartbeat(
        runtime.metadata(),
        WorkerInstanceHeartbeatConfig::new(Duration::from_millis(50))?,
        Arc::new(heartbeat_recorder.clone()),
        Arc::new(SystemClock),
    );
    let task_control = runner.task_control();
    let runner_task = tokio::spawn(runner.run());

    publish_operation(&producer, "succeed").await?;
    publish_operation(&producer, "retry-once").await?;
    publish_operation(&producer, "heartbeat").await?;
    let cancelled_message = publish_operation(&producer, "cancel").await?;
    publish_operation(&producer, "dead-letter").await?;

    wait_for_attempt_count(&state, "succeed", 1).await?;
    wait_for_attempt_count(&state, "retry-once", 2).await?;
    wait_for_attempt_count(&state, "heartbeat", 1).await?;
    wait_until("cancellable task admission", || {
        task_control
            .active_tasks()
            .iter()
            .any(|task| task.message_id == cancelled_message)
    })
    .await?;
    let cancellation = task_control.request_message_cancellation(cancelled_message);
    assert_eq!(cancellation.matched, 1);
    assert_eq!(cancellation.requested, 1);
    wait_until("active task drain after cancellation", || {
        task_control.active_tasks().is_empty()
    })
    .await?;

    let dead_letter = receive_bounded(&mut dead_letters).await?;
    assert_eq!(
        dead_letter
            .message
            .headers
            .get_text(DEAD_LETTER_REASON_METADATA_KEY)?,
        Some("handler_failed")
    );
    let decoded = CapabilityMessageCodec.decode(&dead_letter.message)?;
    assert_eq!(decoded.operation().as_str(), "dead-letter");
    dead_letter.ack().await?;

    let retry_attempts = state.attempts("retry-once");
    assert_eq!(retry_attempts.first(), Some(&1));
    assert!(retry_attempts.iter().any(|attempt| *attempt > 1));
    assert_eq!(state.effect_count("succeed"), 1);
    assert_eq!(state.effect_count("retry-once"), 1);
    assert_eq!(state.effect_count("heartbeat"), 1);
    assert_eq!(state.attempts("dead-letter").first(), Some(&1));
    assert_eq!(state.attempts("cancel").first(), Some(&1));
    assert_eq!(state.effect_count("dead-letter"), 0);
    assert_eq!(state.effect_count("cancel"), 0);
    let _started = runtime.request_shutdown();
    tokio::time::timeout(TEST_TIMEOUT, runner_task)
        .await
        .map_err(|error| -> Box<dyn Error> { Box::new(error) })???;
    assert_worker_lifecycle(&heartbeat_recorder.snapshots());
    connection.begin_drain().await?;
    Ok(())
}

async fn publish_operation(
    producer: &plenora_runtime_nats::JetStreamProducer,
    operation: &str,
) -> Result<MessageId, Box<dyn Error>> {
    let message_id = MessageId::random();
    let request = CapabilityRequest::new(
        CapabilityId::new(CAPABILITY_NAME, 1)?,
        OperationName::new(operation)?,
        SerializedMessage::new("application/octet-stream", operation.to_owned()),
    );
    let mut message = CapabilityMessageCodec.encode(&request)?;
    let _previous = message
        .headers
        .insert_text(MESSAGE_ID_METADATA_KEY, message_id.to_string())?;
    let _previous = message.headers.insert_text(
        CORRELATION_ID_METADATA_KEY,
        CorrelationId::random().to_string(),
    )?;
    assert_eq!(producer.publish(message).await?, PublishOutcome::Confirmed);
    Ok(message_id)
}

async fn wait_until<F>(description: &'static str, predicate: F) -> Result<(), Box<dyn Error>>
where
    F: Fn() -> bool,
{
    let deadline = tokio::time::Instant::now() + CONDITION_TIMEOUT;
    loop {
        if predicate() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("condition was not observed: {description}"),
            )
            .into());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_attempt_count(
    state: &FakeCapabilityState,
    operation: &'static str,
    expected: usize,
) -> Result<(), Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + CONDITION_TIMEOUT;
    loop {
        let observed = state.attempts(operation);
        if observed.len() >= expected {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "operation '{operation}' did not reach {expected} attempts; observed={observed:?}"
                ),
            )
            .into());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
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
        max_ack_pending: Some(8),
        max_payload_bytes: 64 * 1024,
        shutdown_nak_delay: Duration::from_millis(25),
        infrastructure,
    })
}

fn producer_config(
    subject: Arc<str>,
    message_id_metadata_key: Option<Arc<str>>,
) -> JetStreamProducerConfig {
    JetStreamProducerConfig {
        subject,
        max_payload_bytes: 64 * 1024,
        message_id_metadata_key,
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

async fn receive_bounded(
    consumer: &mut plenora_runtime_nats::JetStreamConsumer,
) -> Result<plenora_runtime_messaging::Delivery, Box<dyn Error>> {
    tokio::time::timeout(TEST_TIMEOUT, consumer.receive())
        .await
        .map_err(|error| -> Box<dyn Error> { Box::new(error) })??
        .ok_or_else(|| io::Error::other("consumer ended before delivery").into())
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

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
