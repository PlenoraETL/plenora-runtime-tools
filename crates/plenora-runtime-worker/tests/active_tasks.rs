//! Bounded active-task registry and cooperative control tests.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use async_trait::async_trait;
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{
    CorrelationId, MessageId, MessageMetadata, RetryDecision, RetryPolicy,
};
use plenora_runtime_worker::{
    TaskCancellationReason, WorkerConcurrency, WorkerConfig, WorkerContext, WorkerExecutor,
    WorkerHandler, WorkerTaskCancellationOutcome, WorkerTaskId,
};
use tokio::sync::{Semaphore, mpsc};

#[derive(Clone, Copy, Debug)]
struct TestError(&'static str);

impl Display for TestError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for TestError {}

#[derive(Clone, Copy, Debug)]
struct NoRetry;

impl RetryPolicy<TestError> for NoRetry {
    fn decide(&self, _attempt: u32, _error: &TestError) -> RetryDecision {
        RetryDecision::DoNotRetry
    }
}

#[derive(Debug)]
struct CancellationGateHandler {
    started: mpsc::Sender<()>,
    release: Arc<Semaphore>,
    observed: Arc<Mutex<Vec<TaskCancellationReason>>>,
}

#[async_trait]
impl WorkerHandler<()> for CancellationGateHandler {
    type Error = TestError;

    async fn handle(&self, context: WorkerContext, _message: ()) -> Result<(), Self::Error> {
        self.started
            .send(())
            .await
            .map_err(|_error| TestError("started receiver closed"))?;
        let reason = context.cancellation.cancelled().await;
        lock(&self.observed).push(reason);
        let _release = self
            .release
            .acquire()
            .await
            .map_err(|_error| TestError("release gate closed"))?;
        Ok(())
    }
}

#[tokio::test]
async fn task_control_lists_cancels_and_removes_one_active_task() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let (started_tx, mut started_rx) = mpsc::channel(1);
    let release = Arc::new(Semaphore::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let executor = Arc::new(WorkerExecutor::new(
        CancellationGateHandler {
            started: started_tx,
            release: Arc::clone(&release),
            observed: Arc::clone(&observed),
        },
        NoRetry,
        worker_config(2)?,
    )?);
    let control = executor.task_control();
    let message_id = MessageId::random();
    let correlation_id = CorrelationId::random();
    let execution = {
        let executor = Arc::clone(&executor);
        let context = context(&runtime, message_id, correlation_id, 1);
        tokio::spawn(async move { executor.execute(context, ()).await })
    };

    started_rx
        .recv()
        .await
        .ok_or(TestError("handler did not start"))?;
    let active = control.active_tasks();
    assert_eq!(control.capacity(), 2);
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].message_id, message_id);
    assert_eq!(active[0].correlation_id, correlation_id);
    assert_eq!(active[0].attempt, 1);
    assert_eq!(active[0].cancellation_reason, None);
    let task_id = active[0].task_id;
    let parsed: WorkerTaskId = task_id.to_string().parse()?;
    assert_eq!(parsed, task_id);

    assert_eq!(
        control.request_cancellation(task_id),
        WorkerTaskCancellationOutcome::Requested
    );
    assert_eq!(
        control.request_cancellation(task_id),
        WorkerTaskCancellationOutcome::AlreadyRequested(TaskCancellationReason::Requested)
    );
    assert_eq!(
        control.active_tasks()[0].cancellation_reason,
        Some(TaskCancellationReason::Requested)
    );

    release.add_permits(1);
    let error = execution
        .await?
        .err()
        .ok_or(TestError("cancelled execution unexpectedly succeeded"))?;
    assert_eq!(
        error.cancellation_reason(),
        Some(TaskCancellationReason::Requested)
    );
    assert_eq!(
        lock(&observed).as_slice(),
        &[TaskCancellationReason::Requested]
    );
    assert!(control.active_tasks().is_empty());
    assert_eq!(
        control.request_cancellation(task_id),
        WorkerTaskCancellationOutcome::NotFound
    );
    Ok(())
}

#[tokio::test]
async fn message_cancellation_reaches_every_bounded_active_attempt() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let (started_tx, mut started_rx) = mpsc::channel(2);
    let release = Arc::new(Semaphore::new(0));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let executor = Arc::new(WorkerExecutor::new(
        CancellationGateHandler {
            started: started_tx,
            release: Arc::clone(&release),
            observed: Arc::clone(&observed),
        },
        NoRetry,
        worker_config(2)?,
    )?);
    let control = executor.task_control();
    let message_id = MessageId::random();
    let correlation_id = CorrelationId::random();
    let first = spawn_execution(
        Arc::clone(&executor),
        context(&runtime, message_id, correlation_id, 1),
    );
    let second = spawn_execution(
        Arc::clone(&executor),
        context(&runtime, message_id, correlation_id, 2),
    );

    for _started in 0..2 {
        started_rx
            .recv()
            .await
            .ok_or(TestError("handler did not start"))?;
    }
    assert_eq!(control.active_tasks().len(), control.capacity());
    assert_eq!(
        control.request_message_cancellation(message_id),
        plenora_runtime_worker::WorkerMessageCancellationReport {
            matched: 2,
            requested: 2,
            already_requested: 0,
        }
    );
    assert_eq!(
        control.request_message_cancellation(MessageId::random()),
        plenora_runtime_worker::WorkerMessageCancellationReport {
            matched: 0,
            requested: 0,
            already_requested: 0,
        }
    );

    release.add_permits(2);
    assert_eq!(
        first
            .await?
            .err()
            .and_then(|error| error.cancellation_reason()),
        Some(TaskCancellationReason::Requested)
    );
    assert_eq!(
        second
            .await?
            .err()
            .and_then(|error| error.cancellation_reason()),
        Some(TaskCancellationReason::Requested)
    );
    assert_eq!(lock(&observed).len(), 2);
    assert!(control.active_tasks().is_empty());
    Ok(())
}

fn spawn_execution(
    executor: Arc<WorkerExecutor<CancellationGateHandler, NoRetry>>,
    context: WorkerContext,
) -> tokio::task::JoinHandle<Result<(), plenora_runtime_worker::WorkerExecutionError<TestError>>> {
    tokio::spawn(async move { executor.execute(context, ()).await })
}

fn worker_config(max_in_flight: usize) -> Result<WorkerConfig, Box<dyn Error>> {
    Ok(WorkerConfig::new(
        WorkerConcurrency::new(max_in_flight)?,
        Duration::from_secs(2),
    ))
}

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(ServiceMetadata::new(
        "active-task-test",
        "0.1.0",
        "active-task-instance",
    ))
}

fn context(
    runtime: &RuntimeHandle,
    message_id: MessageId,
    correlation_id: CorrelationId,
    attempt: u32,
) -> WorkerContext {
    WorkerContext::new(
        message_id,
        correlation_id,
        None,
        attempt,
        MessageMetadata::new(),
        runtime.shutdown_signal(),
    )
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
