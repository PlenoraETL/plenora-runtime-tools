//! Fake end-to-end acceptance test for the first runtime milestone.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use plenora_runtime_core::{
    DrainOutcome, RuntimeConfig, RuntimeHandle, ServiceMetadata, TaskCriticality, TaskFailureKind,
    TaskSpec,
};
use plenora_runtime_messaging::{
    CorrelationId, MessageConsumer, MessageId, NackReason, RetryDecision, RetryPolicy,
    SerializedMessage,
};
use plenora_runtime_outbox::{
    InboxStore, MemoryStoreError, OutboxEntry, OutboxEntryState, OutboxId, OutboxRelay, RelayConfig,
};
use plenora_runtime_testkit::{AckEvent, FakeBroker, FakeInboxStore, FakeOutboxStore};
use plenora_runtime_worker::{WorkerConfig, WorkerContext, WorkerExecutor, WorkerHandler};

#[derive(Clone)]
struct DeduplicatingHandler {
    inbox: FakeInboxStore,
    effects: Arc<AtomicUsize>,
}

#[async_trait]
impl WorkerHandler<SerializedMessage> for DeduplicatingHandler {
    type Error = MemoryStoreError;

    async fn handle(
        &self,
        context: WorkerContext,
        _message: SerializedMessage,
    ) -> Result<(), Self::Error> {
        if self.inbox.contains(context.message_id).await? {
            return Ok(());
        }

        self.effects.fetch_add(1, Ordering::SeqCst);
        self.inbox.record_processed(context.message_id).await
    }
}

struct NeverRetry;

impl RetryPolicy<MemoryStoreError> for NeverRetry {
    fn decide(&self, _attempt: u32, _error: &MemoryStoreError) -> RetryDecision {
        RetryDecision::DoNotRetry
    }
}

#[derive(Clone, Copy, Debug)]
struct FlowError;

impl Display for FlowError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("flow handler failed")
    }
}

impl Error for FlowError {}

#[derive(Clone)]
struct FailOnceHandler {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl WorkerHandler<SerializedMessage> for FailOnceHandler {
    type Error = FlowError;

    async fn handle(
        &self,
        _context: WorkerContext,
        _message: SerializedMessage,
    ) -> Result<(), Self::Error> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(FlowError)
        } else {
            Ok(())
        }
    }
}

struct RetryOnce;

impl RetryPolicy<FlowError> for RetryOnce {
    fn decide(&self, attempt: u32, _error: &FlowError) -> RetryDecision {
        if attempt == 1 {
            RetryDecision::RetryAfter(Duration::from_millis(1))
        } else {
            RetryDecision::DoNotRetry
        }
    }
}

struct AlwaysFail;

#[async_trait]
impl WorkerHandler<SerializedMessage> for AlwaysFail {
    type Error = FlowError;

    async fn handle(
        &self,
        _context: WorkerContext,
        _message: SerializedMessage,
    ) -> Result<(), Self::Error> {
        Err(FlowError)
    }
}

impl RetryPolicy<FlowError> for NeverRetry {
    fn decide(&self, _attempt: u32, _error: &FlowError) -> RetryDecision {
        RetryDecision::DoNotRetry
    }
}

struct HangingHandler;

#[async_trait]
impl WorkerHandler<SerializedMessage> for HangingHandler {
    type Error = FlowError;

    async fn handle(
        &self,
        _context: WorkerContext,
        _message: SerializedMessage,
    ) -> Result<(), Self::Error> {
        pending().await
    }
}

#[tokio::test]
async fn outbox_to_worker_flow_is_bounded_acknowledged_and_deduplicated()
-> Result<(), Box<dyn Error>> {
    let outbox = FakeOutboxStore::new();
    let inbox = FakeInboxStore::new();
    let broker = FakeBroker::default();
    let message_id = MessageId::random();
    let outbox_id = OutboxId::random();
    let timestamp = DateTime::parse_from_rfc3339("2026-08-15T10:30:00Z")?.with_timezone(&Utc);
    let message = SerializedMessage::new(
        "application/octet-stream",
        Bytes::from_static(b"milestone-one"),
    );
    outbox.insert(OutboxEntry {
        id: outbox_id,
        message,
        created_at: timestamp,
        attempts: 0,
    })?;

    let relay = OutboxRelay::new(outbox.clone(), broker.producer(), RelayConfig::default());
    let relay_report = relay.run_once().await?;

    assert_eq!(relay_report.claimed, 1);
    assert_eq!(relay_report.published, 1);
    assert_eq!(
        outbox.snapshot(outbox_id)?.map(|snapshot| snapshot.state),
        Some(OutboxEntryState::Published)
    );

    let runtime = RuntimeHandle::new(ServiceMetadata::new(
        "milestone-one",
        "0.1.0",
        "test-instance",
    ));
    let effects = Arc::new(AtomicUsize::new(0));
    let worker = WorkerExecutor::new(
        DeduplicatingHandler {
            inbox: inbox.clone(),
            effects: Arc::clone(&effects),
        },
        NeverRetry,
        WorkerConfig::default(),
    )?;
    let mut consumer = broker.consumer();
    let first = consumer
        .receive_fake()?
        .ok_or_else(|| std::io::Error::other("relay did not enqueue a broker delivery"))?;
    let delivery_id = first.id();
    broker.inject_duplicate(delivery_id)?;

    let first_context = WorkerContext::new(
        message_id,
        CorrelationId::random(),
        None,
        first.attempt,
        first.message.headers.clone(),
        runtime.shutdown_signal(),
    );
    worker.execute(first_context, first.message.clone()).await?;
    first.ack().await?;

    let duplicate = consumer
        .receive_fake()?
        .ok_or_else(|| std::io::Error::other("duplicate delivery was not available"))?;
    let duplicate_context = WorkerContext::new(
        message_id,
        CorrelationId::random(),
        None,
        duplicate.attempt,
        duplicate.message.headers.clone(),
        runtime.shutdown_signal(),
    );
    worker
        .execute(duplicate_context, duplicate.message.clone())
        .await?;
    duplicate.ack().await?;

    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(inbox.processed_count()?, 1);
    assert_eq!(broker.snapshot().pending_deliveries, 0);
    assert_eq!(
        broker
            .acknowledgement_records()
            .iter()
            .filter(|record| record.event == AckEvent::Acked)
            .count(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn worker_dispositions_drive_retry_shutdown_and_terminal_broker_effects()
-> Result<(), Box<dyn Error>> {
    let broker = FakeBroker::default();
    let runtime = RuntimeHandle::new(ServiceMetadata::new("fault-flow", "0.1.0", "test-instance"));
    let retrying_worker = WorkerExecutor::new(
        FailOnceHandler {
            calls: Arc::new(AtomicUsize::new(0)),
        },
        RetryOnce,
        WorkerConfig::default(),
    )?;
    let _retry_id = broker.enqueue(SerializedMessage::new(
        "application/octet-stream",
        Bytes::from_static(b"retry"),
    ))?;

    let first = broker
        .dequeue()?
        .ok_or_else(|| std::io::Error::other("retry delivery was not available"))?;
    let first_context = context(&runtime, &first, MessageId::random());
    let first_error = retrying_worker
        .execute(first_context, first.message.clone())
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("first retry attempt unexpectedly succeeded"))?;
    assert!(matches!(
        first_error.retry_decision(),
        Some(RetryDecision::RetryAfter(_))
    ));
    first.nack(NackReason::Retryable).await?;

    let retry = broker
        .dequeue()?
        .ok_or_else(|| std::io::Error::other("retry redelivery was not available"))?;
    assert_eq!(retry.attempt, 2);
    retrying_worker
        .execute(
            context(&runtime, &retry, MessageId::random()),
            retry.message.clone(),
        )
        .await?;
    retry.ack().await?;

    let _shutdown_id = broker.enqueue(SerializedMessage::new(
        "application/octet-stream",
        Bytes::from_static(b"shutdown"),
    ))?;
    let shutdown_delivery = broker
        .dequeue()?
        .ok_or_else(|| std::io::Error::other("shutdown delivery was not available"))?;
    assert!(runtime.request_shutdown());
    let shutdown_error = retrying_worker
        .execute(
            context(&runtime, &shutdown_delivery, MessageId::random()),
            shutdown_delivery.message.clone(),
        )
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("shutdown delivery unexpectedly started"))?;
    assert!(shutdown_error.admission_reason().is_some());
    shutdown_delivery.nack(NackReason::Shutdown).await?;
    let shutdown_redelivery = broker
        .dequeue()?
        .ok_or_else(|| std::io::Error::other("shutdown redelivery was not available"))?;
    assert_eq!(shutdown_redelivery.attempt, 2);
    shutdown_redelivery.ack().await?;

    let active_runtime = RuntimeHandle::new(ServiceMetadata::new(
        "terminal-flow",
        "0.1.0",
        "test-instance",
    ));
    let terminal_worker = WorkerExecutor::new(AlwaysFail, NeverRetry, WorkerConfig::default())?;
    let _terminal_id = broker.enqueue(SerializedMessage::new(
        "application/octet-stream",
        Bytes::from_static(b"terminal"),
    ))?;
    let terminal = broker
        .dequeue()?
        .ok_or_else(|| std::io::Error::other("terminal delivery was not available"))?;
    let terminal_error = terminal_worker
        .execute(
            context(&active_runtime, &terminal, MessageId::random()),
            terminal.message.clone(),
        )
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("terminal delivery unexpectedly succeeded"))?;
    assert_eq!(
        terminal_error.retry_decision(),
        Some(RetryDecision::DoNotRetry)
    );
    terminal.nack(NackReason::Permanent).await?;

    assert_eq!(broker.snapshot().terminal_delivery_count, 1);
    assert!(broker.dequeue()?.is_none());
    Ok(())
}

#[tokio::test]
async fn forced_runtime_shutdown_drops_and_requeues_an_owned_delivery() -> Result<(), Box<dyn Error>>
{
    let runtime = RuntimeHandle::with_config(
        ServiceMetadata::new("forced-shutdown", "0.1.0", "test-instance"),
        RuntimeConfig {
            shutdown_grace_period: Duration::from_millis(10),
            ..RuntimeConfig::default()
        },
    );
    let worker = WorkerExecutor::new(HangingHandler, NeverRetry, WorkerConfig::default())?;
    let broker = FakeBroker::default();
    let _delivery_id = broker.enqueue(SerializedMessage::new(
        "application/octet-stream",
        Bytes::from_static(b"forced-shutdown"),
    ))?;
    let mut consumer = broker.consumer();
    let delivery = consumer
        .receive()
        .await?
        .ok_or_else(|| std::io::Error::other("forced-shutdown delivery was not available"))?;
    let worker_context = WorkerContext::new(
        MessageId::random(),
        CorrelationId::random(),
        None,
        delivery.attempt,
        delivery.message.headers.clone(),
        runtime.shutdown_signal(),
    );
    let message = delivery.message.clone();
    let task_worker = worker.clone();
    let completion = runtime.spawn(
        TaskSpec::new("owned-delivery", TaskCriticality::Required),
        async move {
            let _result = task_worker.execute(worker_context, message).await;
            drop(delivery);
            Ok::<(), FlowError>(())
        },
    )?;

    tokio::time::timeout(Duration::from_secs(1), async {
        while worker.in_flight() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(
        runtime.shutdown().await,
        DrainOutcome::TimedOut { remaining_tasks: 1 }
    );
    let report = tokio::time::timeout(Duration::from_secs(1), completion.wait()).await??;
    assert!(matches!(
        report
            .outcome
            .failure()
            .map(plenora_runtime_core::TaskFailure::kind),
        Some(TaskFailureKind::Cancelled)
    ));

    let redelivery = consumer
        .receive()
        .await?
        .ok_or_else(|| std::io::Error::other("cancelled delivery was not requeued"))?;
    assert_eq!(redelivery.attempt, 2);
    redelivery.ack().await?;
    Ok(())
}

fn context(
    runtime: &RuntimeHandle,
    delivery: &plenora_runtime_testkit::FakeDelivery,
    message_id: MessageId,
) -> WorkerContext {
    WorkerContext::new(
        message_id,
        CorrelationId::random(),
        None,
        delivery.attempt,
        delivery.message.headers.clone(),
        runtime.shutdown_signal(),
    )
}
