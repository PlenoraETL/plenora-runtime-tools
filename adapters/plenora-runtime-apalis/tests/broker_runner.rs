//! Broker-backed Apalis runner integration tests.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use plenora_runtime_apalis::{
    ApalisAdapterConfig, ApalisDisposition, BrokerDeliveryService, BrokerWorkerErrorKind,
    BrokerWorkerLifecycle, BrokerWorkerRunError, BrokerWorkerRunErrorKind, BrokerWorkerRunner,
};
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{
    AckOperation, CORRELATION_ID_METADATA_KEY, CorrelationId, DEAD_LETTER_ATTEMPTS_METADATA_KEY,
    DEAD_LETTER_REASON_METADATA_KEY, DeliveryHeartbeatConfig, MAX_METADATA_ENTRIES,
    MESSAGE_ID_METADATA_KEY, MessageCodec, MessageId, MessageMetadata, NackReason, RetryDecision,
    RetryPolicy, SerializedMessage,
};
use plenora_runtime_testkit::{
    AckEvent, FakeBroker, HeartbeatEvent, ManualClock, UnknownPublishEffect,
};
use plenora_runtime_worker::{
    MetadataMessageDecoder, TaskCancellationReason, TaskLifecycleEvent, TaskLifecycleEventKind,
    TaskLifecycleObserver, TaskState, WorkerConcurrency, WorkerConfig, WorkerContext,
    WorkerHandler, WorkerInstanceHeartbeat, WorkerInstanceHeartbeatConfig,
    WorkerInstanceHeartbeatObserver, WorkerInstanceStatus, WorkerTaskCancellationOutcome,
};
use tokio::{
    sync::{Notify, Semaphore},
    time::timeout,
};

#[derive(Clone, Copy, Debug)]
struct TestError(&'static str);

impl Display for TestError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
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
        String::from_utf8(message.bytes.to_vec()).map_err(|_error| TestError("invalid UTF-8"))
    }
}

#[derive(Clone, Copy, Debug)]
struct FixedPolicy(RetryDecision);

impl RetryPolicy<TestError> for FixedPolicy {
    fn decide(&self, _attempt: u32, _error: &TestError) -> RetryDecision {
        self.0
    }
}

#[derive(Clone, Debug)]
struct GateHandler {
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    changed: Arc<Notify>,
    release: Arc<Semaphore>,
}

#[async_trait]
impl WorkerHandler<String> for GateHandler {
    type Error = TestError;

    async fn handle(&self, _context: WorkerContext, _message: String) -> Result<(), Self::Error> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        self.changed.notify_waiters();
        let _guard = ActiveGuard {
            active: Arc::clone(&self.active),
            changed: Arc::clone(&self.changed),
        };
        let permit = self
            .release
            .acquire()
            .await
            .map_err(|_error| TestError("handler gate closed"))?;
        permit.forget();
        self.completed.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_waiters();
        Ok(())
    }
}

struct ActiveGuard {
    active: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}

#[derive(Clone, Copy, Debug)]
struct FailingHandler;

#[async_trait]
impl WorkerHandler<String> for FailingHandler {
    type Error = TestError;

    async fn handle(&self, _context: WorkerContext, _message: String) -> Result<(), Self::Error> {
        Err(TestError("sensitive handler failure"))
    }
}

#[derive(Clone, Copy, Debug)]
struct SuccessfulHandler;

#[async_trait]
impl WorkerHandler<String> for SuccessfulHandler {
    type Error = TestError;

    async fn handle(&self, _context: WorkerContext, _message: String) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct AwaitCancellationHandler {
    started: Arc<Semaphore>,
}

#[async_trait]
impl WorkerHandler<String> for AwaitCancellationHandler {
    type Error = TestError;

    async fn handle(&self, context: WorkerContext, _message: String) -> Result<(), Self::Error> {
        self.started.add_permits(1);
        let _reason = context.cancellation.cancelled().await;
        Ok(())
    }
}

#[derive(Debug, Default)]
struct LifecycleRecorder {
    events: Mutex<Vec<TaskLifecycleEvent>>,
}

impl LifecycleRecorder {
    fn events(&self) -> Vec<TaskLifecycleEvent> {
        lock(&self.events).clone()
    }
}

impl TaskLifecycleObserver for LifecycleRecorder {
    fn record(&self, event: TaskLifecycleEvent) {
        lock(&self.events).push(event);
    }
}

#[derive(Debug, Default)]
struct InstanceHeartbeatRecorder {
    heartbeats: Mutex<Vec<WorkerInstanceHeartbeat>>,
}

impl InstanceHeartbeatRecorder {
    fn heartbeats(&self) -> Vec<WorkerInstanceHeartbeat> {
        lock(&self.heartbeats).clone()
    }
}

impl WorkerInstanceHeartbeatObserver for InstanceHeartbeatRecorder {
    fn record(&self, heartbeat: WorkerInstanceHeartbeat) {
        lock(&self.heartbeats).push(heartbeat);
    }
}

#[tokio::test]
async fn runner_pulls_only_when_one_of_five_dynamic_slots_is_available()
-> Result<(), Box<dyn Error>> {
    const JOBS: usize = 20;
    const MAX_IN_FLIGHT: usize = 5;

    let runtime = runtime();
    let broker = FakeBroker::new(ManualClock::default());
    for index in 0..JOBS {
        let _delivery_id = broker.enqueue(worker_message(format!("job-{index}"))?)?;
    }

    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let changed = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let instance_heartbeats = Arc::new(InstanceHeartbeatRecorder::default());
    let runner = BrokerWorkerRunner::new(
        broker.consumer(),
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        GateHandler {
            active: Arc::clone(&active),
            peak: Arc::clone(&peak),
            completed: Arc::clone(&completed),
            changed: Arc::clone(&changed),
            release: Arc::clone(&release),
        },
        FixedPolicy(RetryDecision::DoNotRetry),
        adapter_config(MAX_IN_FLIGHT)?,
        runtime.shutdown_signal(),
    )?
    .with_instance_heartbeat(
        runtime.metadata(),
        WorkerInstanceHeartbeatConfig::new(Duration::from_millis(1))?,
        Arc::clone(&instance_heartbeats),
        Arc::new(broker.clock()),
    );
    assert_eq!(runner.max_in_flight(), MAX_IN_FLIGHT);
    assert_eq!(runner.in_flight(), 0);

    let control = async {
        wait_for(&active, &changed, MAX_IN_FLIGHT).await?;
        let saturated = broker.snapshot();
        assert_eq!(saturated.in_flight_deliveries, MAX_IN_FLIGHT);
        assert_eq!(saturated.pending_deliveries, JOBS - MAX_IN_FLIGHT);
        assert_eq!(peak.load(Ordering::SeqCst), MAX_IN_FLIGHT);
        tokio::time::sleep(Duration::from_millis(10)).await;

        release.add_permits(JOBS);
        wait_for(&completed, &changed, JOBS).await?;
        let _started = runtime.request_shutdown();
        Ok::<(), TestError>(())
    };

    let (run_result, control_result) = tokio::join!(runner.run(), control);
    run_result?;
    control_result?;

    assert_eq!(completed.load(Ordering::SeqCst), JOBS);
    assert_eq!(peak.load(Ordering::SeqCst), MAX_IN_FLIGHT);
    assert_eq!(broker.snapshot().acknowledgement_count, JOBS);
    assert!(
        broker
            .acknowledgement_records()
            .iter()
            .all(|record| record.event == AckEvent::Acked)
    );
    let heartbeats = instance_heartbeats.heartbeats();
    assert_eq!(
        heartbeats.first().map(|heartbeat| heartbeat.status),
        Some(WorkerInstanceStatus::Starting)
    );
    assert!(heartbeats.iter().any(|heartbeat| {
        heartbeat.status == WorkerInstanceStatus::Ready
            && heartbeat.in_flight == MAX_IN_FLIGHT
            && heartbeat.available_slots == 0
    }));
    assert!(
        heartbeats
            .iter()
            .any(|heartbeat| heartbeat.status == WorkerInstanceStatus::Draining)
    );
    assert_eq!(
        heartbeats.last().map(|heartbeat| heartbeat.status),
        Some(WorkerInstanceStatus::Stopped)
    );

    Ok(())
}

#[tokio::test]
async fn runner_task_control_cancels_active_delivery_then_settles_permanently()
-> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let broker = FakeBroker::new(ManualClock::default());
    let message = worker_message(String::from("cancel-me"))?;
    let message_id_text = message
        .headers
        .get_text(MESSAGE_ID_METADATA_KEY)?
        .ok_or(TestError("message id missing"))?;
    let message_id = message_id_text.parse()?;
    let _delivery_id = broker.enqueue(message)?;
    let started = Arc::new(Semaphore::new(0));
    let runner = BrokerWorkerRunner::new(
        broker.consumer(),
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        AwaitCancellationHandler {
            started: Arc::clone(&started),
        },
        FixedPolicy(RetryDecision::DoNotRetry),
        adapter_config(1)?,
        runtime.shutdown_signal(),
    )?;
    let task_control = runner.task_control();

    let control = async {
        let permit = started
            .acquire()
            .await
            .map_err(|_error| TestError("started gate closed"))?;
        permit.forget();
        let active = task_control.active_tasks();
        let task = active
            .first()
            .ok_or(TestError("active delivery was not registered"))?;
        assert_eq!(task.message_id, message_id);
        assert_eq!(
            task_control.request_cancellation(task.task_id),
            WorkerTaskCancellationOutcome::Requested
        );
        timeout(Duration::from_secs(1), async {
            while broker.snapshot().acknowledgement_count == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_error| TestError("cancelled delivery was not settled"))?;
        let _started = runtime.request_shutdown();
        Ok::<(), TestError>(())
    };

    let (run_result, control_result) = tokio::join!(runner.run(), control);
    run_result?;
    control_result?;
    assert_eq!(
        broker.acknowledgement_records()[0].event,
        AckEvent::Nacked(NackReason::Permanent)
    );
    assert!(task_control.active_tasks().is_empty());
    Ok(())
}

#[tokio::test]
async fn retry_disposition_becomes_broker_native_delayed_nack() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let broker = FakeBroker::new(ManualClock::default());
    let _delivery_id = broker.enqueue(worker_message(String::from("retry"))?)?;
    let delivery = broker
        .dequeue()?
        .ok_or(TestError("expected one fake delivery"))?
        .into_delivery();
    let delay = Duration::from_secs(4);
    let service = BrokerDeliveryService::new(
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        FailingHandler,
        FixedPolicy(RetryDecision::RetryAfter(delay)),
        adapter_config(1)?,
        runtime.shutdown_signal(),
    )?;

    let outcome = service.execute(delivery).await?;

    assert_eq!(outcome.disposition(), ApalisDisposition::RetryAfter(delay));
    let records = broker.acknowledgement_records();
    let record = records
        .first()
        .ok_or(TestError("expected one acknowledgement record"))?;
    assert_eq!(
        record.event,
        AckEvent::Nacked(NackReason::RetryAfter(delay))
    );
    assert_eq!(broker.snapshot().pending_deliveries, 1);

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn long_handler_renews_delivery_until_settlement() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let broker = FakeBroker::new(ManualClock::default());
    let _delivery_id = broker.enqueue(worker_message(String::from("long-running"))?)?;
    let heartbeat = DeliveryHeartbeatConfig::new(Duration::from_secs(1), 2)?;
    let delivery = broker
        .dequeue()?
        .ok_or(TestError("expected one fake delivery"))?
        .into_delivery_with_heartbeat(heartbeat);
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let changed = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let service = BrokerDeliveryService::new(
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        GateHandler {
            active: Arc::clone(&active),
            peak,
            completed: Arc::clone(&completed),
            changed,
            release: Arc::clone(&release),
        },
        FixedPolicy(RetryDecision::DoNotRetry),
        adapter_config(1)?,
        runtime.shutdown_signal(),
    )?;

    let execution = tokio::spawn(async move { service.execute(delivery).await });
    tokio::task::yield_now().await;
    assert_eq!(active.load(Ordering::SeqCst), 1);

    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(broker.snapshot().heartbeat_count, 2);
    assert!(
        broker
            .heartbeat_records()
            .iter()
            .all(|record| record.event == HeartbeatEvent::Renewed)
    );

    release.add_permits(1);
    let outcome = execution.await??;
    assert_eq!(outcome.disposition(), ApalisDisposition::Completed);
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    assert_eq!(broker.acknowledgement_records()[0].event, AckEvent::Acked);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn exhausted_heartbeat_budget_cancels_handler_and_requeues() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let broker = FakeBroker::new(ManualClock::default());
    let _delivery_id = broker.enqueue(worker_message(String::from("lease-risk"))?)?;
    broker.fail_next_heartbeat("first renewal failed")?;
    broker.fail_next_heartbeat("second renewal failed")?;
    let delivery = broker
        .dequeue()?
        .ok_or(TestError("expected one fake delivery"))?
        .into_delivery_with_heartbeat(DeliveryHeartbeatConfig::new(Duration::from_secs(1), 2)?);
    let active = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let lifecycle = Arc::new(LifecycleRecorder::default());
    let service = BrokerDeliveryService::new_with_lifecycle(
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        GateHandler {
            active: Arc::clone(&active),
            peak: Arc::new(AtomicUsize::new(0)),
            completed: Arc::clone(&completed),
            changed: Arc::new(Notify::new()),
            release: Arc::new(Semaphore::new(0)),
        },
        FixedPolicy(RetryDecision::DoNotRetry),
        adapter_config(1)?,
        runtime.shutdown_signal(),
        Arc::clone(&lifecycle),
        Arc::new(broker.clock()),
    )?;

    let execution = tokio::spawn(async move { service.execute(delivery).await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(!execution.is_finished());
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;

    let result = execution.await?;
    let error = result
        .err()
        .ok_or(TestError("heartbeat failure was ignored"))?;
    assert_eq!(error.kind(), BrokerWorkerErrorKind::Heartbeat);
    assert_eq!(
        error
            .heartbeat_error()
            .map(plenora_runtime_messaging::AckError::operation),
        Some(AckOperation::Heartbeat)
    );
    assert!(error.decoder_error().is_none());
    assert!(error.metadata_error().is_none());
    assert!(error.dead_letter_error().is_none());
    assert!(error.acknowledgement_error().is_none());
    assert!(Error::source(&error).is_some());
    assert_eq!(error.to_string(), "broker worker failed during Heartbeat");
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(completed.load(Ordering::SeqCst), 0);
    assert_eq!(broker.snapshot().heartbeat_count, 2);
    assert_eq!(broker.snapshot().pending_deliveries, 1);
    assert_eq!(
        broker.acknowledgement_records()[0].event,
        AckEvent::Nacked(NackReason::Retryable)
    );
    assert_eq!(
        lifecycle.events().last().map(|event| event.kind),
        Some(TaskLifecycleEventKind::StateChanged(TaskState::Cancelled(
            TaskCancellationReason::LeaseLost,
        )))
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn heartbeat_and_requeue_failures_remain_separately_observable() -> Result<(), Box<dyn Error>>
{
    let runtime = runtime();
    let broker = FakeBroker::new(ManualClock::default());
    let _delivery_id = broker.enqueue(worker_message(String::from("uncertain-lease"))?)?;
    broker.fail_next_heartbeat("renewal failed")?;
    broker.fail_next_nack("requeue failed")?;
    let delivery = broker
        .dequeue()?
        .ok_or(TestError("expected one fake delivery"))?
        .into_delivery_with_heartbeat(DeliveryHeartbeatConfig::new(Duration::from_secs(1), 1)?);
    let service = BrokerDeliveryService::new(
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        GateHandler {
            active: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
            completed: Arc::new(AtomicUsize::new(0)),
            changed: Arc::new(Notify::new()),
            release: Arc::new(Semaphore::new(0)),
        },
        FixedPolicy(RetryDecision::DoNotRetry),
        adapter_config(1)?,
        runtime.shutdown_signal(),
    )?;

    let execution = tokio::spawn(async move { service.execute(delivery).await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;

    let error = execution
        .await?
        .err()
        .ok_or(TestError("heartbeat and settlement failures were ignored"))?;
    assert_eq!(error.kind(), BrokerWorkerErrorKind::HeartbeatSettlement);
    assert_eq!(
        error
            .heartbeat_error()
            .map(plenora_runtime_messaging::AckError::operation),
        Some(AckOperation::Heartbeat)
    );
    assert_eq!(
        error
            .acknowledgement_error()
            .map(plenora_runtime_messaging::AckError::operation),
        Some(AckOperation::Nack)
    );
    assert_eq!(broker.snapshot().pending_deliveries, 1);
    Ok(())
}

#[tokio::test]
async fn malformed_delivery_is_terminated_without_invoking_handler() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let broker = FakeBroker::new(ManualClock::default());
    let malformed = SerializedMessage::new("text/plain", Bytes::from_static(b"secret-payload"));
    let _delivery_id = broker.enqueue(malformed)?;
    let delivery = broker
        .dequeue()?
        .ok_or(TestError("expected one fake delivery"))?
        .into_delivery();
    let service = BrokerDeliveryService::new(
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        FailingHandler,
        FixedPolicy(RetryDecision::RetryAfter(Duration::from_secs(1))),
        adapter_config(1)?,
        runtime.shutdown_signal(),
    )?;

    let result = service.execute(delivery).await;
    let Err(error) = result else {
        return Err(Box::new(TestError("malformed delivery was accepted")) as Box<dyn Error>);
    };
    let debug = format!("{error:?}");

    assert_eq!(error.kind(), BrokerWorkerErrorKind::Decode);
    assert!(error.decoder_error().is_some());
    assert!(error.metadata_error().is_none());
    assert!(error.heartbeat_error().is_none());
    assert!(error.dead_letter_error().is_none());
    assert!(error.acknowledgement_error().is_none());
    assert!(Error::source(&error).is_some());
    assert_eq!(error.to_string(), "broker worker failed during Decode");
    assert!(!debug.contains("secret-payload"));
    assert_eq!(broker.snapshot().terminal_delivery_count, 1);
    let terminal = broker.terminal_delivery_records();
    let record = terminal
        .first()
        .ok_or(TestError("expected one terminal delivery record"))?;
    assert_eq!(record.event, AckEvent::Nacked(NackReason::ConsumerRejected));

    Ok(())
}

#[tokio::test]
async fn terminal_consumer_failure_is_returned_with_its_source() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let broker = FakeBroker::new(ManualClock::default());
    broker.fail_next_receive("sensitive broker detail")?;
    let runner = BrokerWorkerRunner::new(
        broker.consumer(),
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        FailingHandler,
        FixedPolicy(RetryDecision::DoNotRetry),
        adapter_config(1)?,
        runtime.shutdown_signal(),
    )?;

    let result = runner.run().await;
    let Err(error) = result else {
        return Err(Box::new(TestError("consumer failure was ignored")) as Box<dyn Error>);
    };
    let debug = format!("{error:?}");

    assert_eq!(error.kind(), BrokerWorkerRunErrorKind::Consumer);
    assert!(error.consumer_error().is_some());
    assert!(error.source().is_some());
    assert_eq!(
        error.to_string(),
        "broker worker runner failed during Consumer"
    );
    assert!(!debug.contains("sensitive broker detail"));

    Ok(())
}

#[tokio::test]
async fn confirmed_dead_letter_is_published_before_original_is_terminated()
-> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let broker = FakeBroker::new(ManualClock::default());
    let _delivery_id = broker.enqueue(worker_message(String::from("poison"))?)?;
    let delivery = broker
        .dequeue()?
        .ok_or(TestError("expected poison delivery"))?
        .into_delivery();
    let service = BrokerDeliveryService::new(
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        FailingHandler,
        FixedPolicy(RetryDecision::DeadLetter),
        adapter_config(1)?,
        runtime.shutdown_signal(),
    )?
    .with_dead_letter_sink(broker.producer());

    let outcome = service.execute(delivery).await?;
    let published = broker.published_messages();

    assert_eq!(outcome.disposition(), ApalisDisposition::DeadLetter);
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].bytes.as_ref(), b"poison");
    assert_eq!(
        published[0]
            .headers
            .get_text(DEAD_LETTER_REASON_METADATA_KEY)?,
        Some("handler_failed")
    );
    assert_eq!(
        published[0]
            .headers
            .get_text(DEAD_LETTER_ATTEMPTS_METADATA_KEY)?,
        Some("1")
    );
    assert_eq!(broker.snapshot().terminal_delivery_count, 1);
    assert!(
        broker
            .acknowledgement_records()
            .iter()
            .any(|record| { record.event == AckEvent::Nacked(NackReason::Permanent) })
    );
    Ok(())
}

#[tokio::test]
async fn missing_dead_letter_sink_leaves_original_eligible_for_redelivery()
-> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let broker = FakeBroker::new(ManualClock::default());
    let _delivery_id = broker.enqueue(worker_message(String::from("poison"))?)?;
    let delivery = broker
        .dequeue()?
        .ok_or(TestError("expected poison delivery"))?
        .into_delivery();
    let service = BrokerDeliveryService::new(
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        FailingHandler,
        FixedPolicy(RetryDecision::DeadLetter),
        adapter_config(1)?,
        runtime.shutdown_signal(),
    )?;

    let error = service
        .execute(delivery)
        .await
        .err()
        .ok_or(TestError("missing DLQ sink unexpectedly succeeded"))?;

    assert_eq!(error.kind(), BrokerWorkerErrorKind::DeadLetterUnavailable);
    assert_eq!(broker.snapshot().terminal_delivery_count, 0);
    assert_eq!(broker.snapshot().pending_deliveries, 1);
    Ok(())
}

#[tokio::test]
async fn unknown_dead_letter_outcome_never_terminates_the_original() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let broker = FakeBroker::new(ManualClock::default());
    let _delivery_id = broker.enqueue(worker_message(String::from("poison"))?)?;
    let delivery = broker
        .dequeue()?
        .ok_or(TestError("expected poison delivery"))?
        .into_delivery();
    broker.return_unknown_for_next_publish(UnknownPublishEffect::NotApplied)?;
    let service = BrokerDeliveryService::new(
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        FailingHandler,
        FixedPolicy(RetryDecision::DeadLetter),
        adapter_config(1)?,
        runtime.shutdown_signal(),
    )?
    .with_dead_letter_sink(broker.producer());

    let error = service
        .execute(delivery)
        .await
        .err()
        .ok_or(TestError("unknown DLQ outcome unexpectedly succeeded"))?;

    assert_eq!(
        error.kind(),
        BrokerWorkerErrorKind::DeadLetterOutcomeUnknown
    );
    assert_eq!(broker.snapshot().terminal_delivery_count, 0);
    assert_eq!(broker.snapshot().pending_deliveries, 1);
    Ok(())
}

#[tokio::test]
async fn dead_letter_publish_and_settlement_failures_remain_distinct() -> Result<(), Box<dyn Error>>
{
    let runtime = runtime();
    let publish_broker = FakeBroker::new(ManualClock::default());
    let _delivery_id = publish_broker.enqueue(worker_message(String::from("publish-fail"))?)?;
    let delivery = publish_broker
        .dequeue()?
        .ok_or(TestError("expected publish-fail delivery"))?
        .into_delivery();
    publish_broker.fail_next_publish("sensitive DLQ failure")?;
    let service = BrokerDeliveryService::new(
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        FailingHandler,
        FixedPolicy(RetryDecision::DeadLetter),
        adapter_config(1)?,
        runtime.shutdown_signal(),
    )?
    .with_dead_letter_sink(publish_broker.producer());
    let publish_error = service
        .execute(delivery)
        .await
        .err()
        .ok_or(TestError("failed DLQ publish unexpectedly succeeded"))?;

    let settlement_broker = FakeBroker::new(ManualClock::default());
    let _delivery_id = settlement_broker.enqueue(worker_message(String::from("settle-fail"))?)?;
    let delivery = settlement_broker
        .dequeue()?
        .ok_or(TestError("expected settle-fail delivery"))?
        .into_delivery();
    settlement_broker.fail_next_nack("sensitive settlement failure")?;
    let service = BrokerDeliveryService::new(
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        FailingHandler,
        FixedPolicy(RetryDecision::DeadLetter),
        adapter_config(1)?,
        runtime.shutdown_signal(),
    )?
    .with_dead_letter_sink(settlement_broker.producer());
    let settlement_error = service.execute(delivery).await.err().ok_or(TestError(
        "failed original settlement unexpectedly succeeded",
    ))?;

    assert_eq!(
        publish_error.kind(),
        BrokerWorkerErrorKind::DeadLetterPublish
    );
    assert!(publish_error.dead_letter_error().is_some());
    assert!(Error::source(&publish_error).is_some());
    assert_eq!(
        settlement_error.kind(),
        BrokerWorkerErrorKind::DeadLetterSettlement
    );
    assert!(settlement_error.acknowledgement_error().is_some());
    assert!(Error::source(&settlement_error).is_some());
    assert_eq!(settlement_broker.published_messages().len(), 1);
    Ok(())
}

#[test]
fn monitor_run_error_exposes_category_and_source_without_private_text() {
    let error =
        BrokerWorkerRunError::<TestError>::Monitor(std::io::Error::other("private monitor source"));
    assert_eq!(error.kind(), BrokerWorkerRunErrorKind::Monitor);
    assert!(error.consumer_error().is_none());
    assert!(Error::source(&error).is_some());
    assert_eq!(
        error.to_string(),
        "broker worker runner failed during Monitor"
    );
    assert!(!format!("{error:?}").contains("private monitor source"));
}

#[tokio::test]
async fn decode_and_metadata_settlement_failures_preserve_both_sources()
-> Result<(), Box<dyn Error>> {
    let runtime = runtime();

    let decode_broker = FakeBroker::default();
    decode_broker.enqueue(SerializedMessage::new("text/plain", "malformed"))?;
    decode_broker.fail_next_nack("private decode settlement")?;
    let decode_delivery = decode_broker
        .dequeue()?
        .ok_or(TestError("missing malformed delivery"))?
        .into_delivery();
    let decode_service = BrokerDeliveryService::new(
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        SuccessfulHandler,
        FixedPolicy(RetryDecision::DoNotRetry),
        adapter_config(1)?,
        runtime.shutdown_signal(),
    )?;
    let decode_error = decode_service
        .execute(decode_delivery)
        .await
        .err()
        .ok_or(TestError(
            "decode settlement failure unexpectedly succeeded",
        ))?;
    assert_eq!(decode_error.kind(), BrokerWorkerErrorKind::DecodeSettlement);
    assert!(decode_error.decoder_error().is_some());
    assert!(decode_error.acknowledgement_error().is_some());
    assert!(Error::source(&decode_error).is_some());

    for settlement_fails in [false, true] {
        let broker = FakeBroker::default();
        let mut message = worker_message(String::from("metadata-bound"))?;
        for index in message.headers.len()..MAX_METADATA_ENTRIES {
            message
                .headers
                .insert_text(format!("application.fill{index}"), "x")?;
        }
        broker.enqueue(message)?;
        if settlement_fails {
            broker.fail_next_nack("private metadata settlement")?;
        }
        let delivery = broker
            .dequeue()?
            .ok_or(TestError("missing metadata-bound delivery"))?
            .into_delivery();
        let service = BrokerDeliveryService::new(
            MetadataMessageDecoder::<_, String>::new(TextCodec),
            SuccessfulHandler,
            FixedPolicy(RetryDecision::DoNotRetry),
            adapter_config(1)?,
            runtime.shutdown_signal(),
        )?;
        let error = service
            .execute(delivery)
            .await
            .err()
            .ok_or(TestError("metadata overflow unexpectedly succeeded"))?;
        assert_eq!(
            error.kind(),
            if settlement_fails {
                BrokerWorkerErrorKind::MetadataSettlement
            } else {
                BrokerWorkerErrorKind::Metadata
            }
        );
        assert!(error.metadata_error().is_some());
        assert_eq!(error.acknowledgement_error().is_some(), settlement_fails);
        assert!(Error::source(&error).is_some());
    }
    Ok(())
}

#[tokio::test]
async fn service_settlement_failure_and_public_controls_are_observable()
-> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let broker = FakeBroker::default();
    broker.enqueue(worker_message(String::from("ack-failure"))?)?;
    broker.fail_next_ack("private acknowledgement source")?;
    let delivery = broker
        .dequeue()?
        .ok_or(TestError("missing delivery"))?
        .into_delivery();
    let service = BrokerDeliveryService::new(
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        SuccessfulHandler,
        FixedPolicy(RetryDecision::DoNotRetry),
        adapter_config(2)?,
        runtime.shutdown_signal(),
    )?;
    assert!(!service.has_dead_letter_sink());
    assert_eq!(service.config().worker().concurrency.max_in_flight, 2);
    assert_eq!(service.in_flight(), 0);
    assert!(service.task_control().active_tasks().is_empty());
    assert!(format!("{service:?}").contains("BrokerDeliveryService"));

    let error = service
        .execute(delivery)
        .await
        .err()
        .ok_or(TestError("failed acknowledgement unexpectedly succeeded"))?;
    assert_eq!(error.kind(), BrokerWorkerErrorKind::Settlement);
    assert!(error.acknowledgement_error().is_some());
    assert!(Error::source(&error).is_some());
    assert!(!format!("{error:?}").contains("private acknowledgement source"));
    assert!(service.begin_drain());
    assert!(!service.begin_drain());
    Ok(())
}

#[tokio::test]
async fn lifecycle_runner_pre_cancelled_path_stops_without_polling() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let broker = FakeBroker::default();
    let lifecycle = Arc::new(LifecycleRecorder::default());
    let instance = Arc::new(InstanceHeartbeatRecorder::default());
    let runner = BrokerWorkerRunner::new_with_lifecycle(
        broker.consumer(),
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        SuccessfulHandler,
        FixedPolicy(RetryDecision::DoNotRetry),
        adapter_config(3)?,
        runtime.shutdown_signal(),
        BrokerWorkerLifecycle::new(Arc::clone(&lifecycle), Arc::new(broker.clock())),
    )?
    .with_dead_letter_sink(broker.producer())
    .with_instance_heartbeat(
        runtime.metadata(),
        WorkerInstanceHeartbeatConfig::new(Duration::from_secs(1))?,
        Arc::clone(&instance),
        Arc::new(broker.clock()),
    );
    assert_eq!(runner.max_in_flight(), 3);
    assert_eq!(runner.in_flight(), 0);
    assert!(runner.has_dead_letter_sink());
    assert!(runner.task_control().active_tasks().is_empty());
    assert!(format!("{runner:?}").contains("has_instance_heartbeat: true"));
    assert!(runtime.request_shutdown());

    runner.run().await?;
    assert!(lifecycle.events().is_empty());
    assert_eq!(
        instance
            .heartbeats()
            .last()
            .map(|heartbeat| heartbeat.status),
        Some(WorkerInstanceStatus::Stopped)
    );
    Ok(())
}

async fn wait_for(
    counter: &AtomicUsize,
    changed: &Notify,
    expected: usize,
) -> Result<(), TestError> {
    timeout(Duration::from_secs(2), async {
        loop {
            let notified = changed.notified();
            if counter.load(Ordering::SeqCst) >= expected {
                return;
            }
            notified.await;
        }
    })
    .await
    .map_err(|_elapsed| TestError("timed out waiting for worker progress"))
}

fn worker_message(payload: String) -> Result<SerializedMessage, Box<dyn Error>> {
    let mut metadata = MessageMetadata::new();
    let _previous =
        metadata.insert_text(MESSAGE_ID_METADATA_KEY, MessageId::random().to_string())?;
    let _previous = metadata.insert_text(
        CORRELATION_ID_METADATA_KEY,
        CorrelationId::random().to_string(),
    )?;
    Ok(SerializedMessage::new("text/plain", payload).with_headers(metadata))
}

fn adapter_config(max_in_flight: usize) -> Result<ApalisAdapterConfig, Box<dyn Error>> {
    Ok(ApalisAdapterConfig::new(
        "broker-worker",
        WorkerConfig::new(
            WorkerConcurrency::new(max_in_flight)?,
            Duration::from_secs(1),
        ),
    )?)
}

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(ServiceMetadata::new(
        "broker-worker-test",
        "0.1.0",
        "test-1",
    ))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
