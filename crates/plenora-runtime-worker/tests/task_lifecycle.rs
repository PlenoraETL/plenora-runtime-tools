//! Per-task lifecycle, heartbeat, timeout, and cooperative cancellation tests.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    num::NonZeroU64,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use plenora_runtime_core::{Clock, RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{
    CorrelationId, MessageId, MessageMetadata, RetryDecision, RetryPolicy,
};
use plenora_runtime_worker::{
    TaskCancellationReason, TaskCancellationToken, TaskLifecycleEvent, TaskLifecycleEventKind,
    TaskLifecycleObserver, TaskProgress, TaskProgressError, TaskProgressReporter, TaskState,
    WorkerConcurrency, WorkerConfig, WorkerContext, WorkerErrorCategory, WorkerExecutionPhase,
    WorkerExecutor, WorkerHandler, WorkerRemoteEffect,
};
use tokio::sync::{Notify, Semaphore};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TestError(&'static str);

impl Display for TestError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for TestError {}

#[derive(Clone, Copy, Debug)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

#[derive(Debug, Default)]
struct RecordingObserver {
    events: Mutex<Vec<TaskLifecycleEvent>>,
}

impl RecordingObserver {
    fn events(&self) -> Vec<TaskLifecycleEvent> {
        lock(&self.events).clone()
    }
}

impl TaskLifecycleObserver for RecordingObserver {
    fn record(&self, event: TaskLifecycleEvent) {
        lock(&self.events).push(event);
    }
}

#[derive(Clone, Copy, Debug)]
struct FixedRetry(RetryDecision);

impl RetryPolicy<TestError> for FixedRetry {
    fn decide(&self, _attempt: u32, _error: &TestError) -> RetryDecision {
        self.0
    }
}

#[derive(Debug)]
struct GateState {
    started: AtomicBool,
    changed: Notify,
    release: Semaphore,
    reporter: Mutex<Option<TaskProgressReporter>>,
}

#[derive(Debug)]
struct StartSignal {
    started: AtomicBool,
    changed: Notify,
}

impl StartSignal {
    fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
            changed: Notify::new(),
        }
    }

    fn mark_started(&self) {
        self.started.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    async fn wait(&self) {
        loop {
            let changed = self.changed.notified();
            if self.started.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

impl GateState {
    fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
            changed: Notify::new(),
            release: Semaphore::new(0),
            reporter: Mutex::new(None),
        }
    }

    async fn wait_started(&self) {
        loop {
            let changed = self.changed.notified();
            if self.started.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

#[derive(Clone, Debug)]
struct ProgressGateHandler {
    state: Arc<GateState>,
}

#[async_trait]
impl WorkerHandler<u64> for ProgressGateHandler {
    type Error = TestError;

    async fn handle(&self, ctx: WorkerContext, _message: u64) -> Result<(), Self::Error> {
        let progress = TaskProgress::new(2, NonZeroU64::new(10))
            .map_err(|_error| TestError("invalid static progress"))?;
        ctx.report_progress(progress)
            .map_err(|_error| TestError("progress reporting failed"))?;
        *lock(&self.state.reporter) = Some(ctx.progress.clone());
        self.state.started.store(true, Ordering::Release);
        self.state.changed.notify_waiters();
        let permit = self
            .state
            .release
            .acquire()
            .await
            .map_err(|_error| TestError("progress gate closed"))?;
        permit.forget();
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct CancellationHandler {
    observed: Arc<Mutex<Option<TaskCancellationReason>>>,
    started: Arc<StartSignal>,
}

#[async_trait]
impl WorkerHandler<u64> for CancellationHandler {
    type Error = TestError;

    async fn handle(&self, ctx: WorkerContext, _message: u64) -> Result<(), Self::Error> {
        self.started.mark_started();
        let reason = ctx.cancellation.cancelled().await;
        *lock(&self.observed) = Some(reason);
        Ok(())
    }
}

#[tokio::test]
async fn cancellation_token_preserves_the_first_reason() {
    let token = TaskCancellationToken::new();
    assert!(token.cancel(TaskCancellationReason::Requested));
    assert!(!token.cancel(TaskCancellationReason::Timeout));
    assert_eq!(token.cancelled().await, TaskCancellationReason::Requested);
    assert_eq!(token.reason(), Some(TaskCancellationReason::Requested));
}

#[test]
fn progress_is_bounded_and_rejects_completed_above_total() {
    let total = NonZeroU64::new(4);
    assert_eq!(
        TaskProgress::new(5, total),
        Err(TaskProgressError::CompletedExceedsTotal)
    );
}

#[tokio::test(start_paused = true)]
async fn success_emits_ordered_progress_heartbeat_and_terminal_state() -> Result<(), Box<dyn Error>>
{
    let runtime = runtime();
    let observer = Arc::new(RecordingObserver::default());
    let observed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(42);
    let state = Arc::new(GateState::new());
    let config = WorkerConfig::new(WorkerConcurrency::new(1)?, Duration::from_secs(5))
        .with_lifecycle_heartbeat(Duration::from_secs(1))?;
    let executor = WorkerExecutor::with_lifecycle_observer(
        ProgressGateHandler {
            state: Arc::clone(&state),
        },
        FixedRetry(RetryDecision::DoNotRetry),
        config,
        Arc::clone(&observer),
        Arc::new(FixedClock(observed_at)),
    )?;

    let execution = tokio::spawn({
        let executor = executor.clone();
        let context = context(&runtime, TaskCancellationToken::new());
        async move { executor.execute(context, 7).await }
    });
    state.wait_started().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    state.release.add_permits(1);
    execution.await??;

    let events = observer.events();
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    assert!(events.iter().all(|event| event.observed_at == observed_at));
    assert_eq!(
        events.iter().map(|event| event.kind).collect::<Vec<_>>(),
        vec![
            TaskLifecycleEventKind::StateChanged(TaskState::Queued),
            TaskLifecycleEventKind::StateChanged(TaskState::Running),
            TaskLifecycleEventKind::Progress(TaskProgress::new(2, NonZeroU64::new(10))?),
            TaskLifecycleEventKind::Heartbeat(Some(TaskProgress::new(2, NonZeroU64::new(10),)?)),
            TaskLifecycleEventKind::StateChanged(TaskState::Succeeded),
        ]
    );
    let reporter = lock(&state.reporter)
        .clone()
        .ok_or(TestError("handler did not expose its reporter"))?;
    assert_eq!(
        reporter.heartbeat(),
        Err(TaskProgressError::TaskAlreadyTerminal)
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn timeout_signals_cooperative_cleanup_and_returns_configured_retry()
-> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let observer = Arc::new(RecordingObserver::default());
    let cancellation_reason = Arc::new(Mutex::new(None));
    let started = Arc::new(StartSignal::new());
    let config = WorkerConfig::new(WorkerConcurrency::new(1)?, Duration::from_secs(5))
        .with_execution_timeout(
            Duration::from_secs(5),
            Duration::from_secs(2),
            RetryDecision::RetryAfter(Duration::from_secs(3)),
        )?;
    let executor = WorkerExecutor::with_lifecycle_observer(
        CancellationHandler {
            observed: Arc::clone(&cancellation_reason),
            started: Arc::clone(&started),
        },
        FixedRetry(RetryDecision::DoNotRetry),
        config,
        Arc::clone(&observer),
        Arc::new(FixedClock(SystemTime::UNIX_EPOCH)),
    )?;
    let execution = tokio::spawn({
        let executor = executor.clone();
        let context = context(&runtime, TaskCancellationToken::new());
        async move { executor.execute(context, 1).await }
    });
    started.wait().await;

    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    let error = execution
        .await?
        .err()
        .ok_or(TestError("timed execution unexpectedly succeeded"))?;

    assert_eq!(error.category(), WorkerErrorCategory::Timeout);
    assert_eq!(error.phase(), WorkerExecutionPhase::Handling);
    assert_eq!(error.remote_effect(), WorkerRemoteEffect::Unknown);
    assert_eq!(error.execution_timeout(), Some(Duration::from_secs(5)));
    assert_eq!(
        error.retry_decision(),
        Some(RetryDecision::RetryAfter(Duration::from_secs(3)))
    );
    assert_eq!(
        *lock(&cancellation_reason),
        Some(TaskCancellationReason::Timeout)
    );
    assert_eq!(error.admission_reason(), None);
    assert_eq!(error.cancellation_reason(), None);
    assert_eq!(error.source_error(), None);
    assert_eq!(error.to_string(), "worker handler execution timed out");
    assert!(Error::source(&error).is_none());
    assert_eq!(
        observer.events().last().map(|event| event.kind),
        Some(TaskLifecycleEventKind::StateChanged(TaskState::TimedOut))
    );
    assert!(error.into_source().is_none());
    Ok(())
}

#[tokio::test]
async fn external_cancellation_is_observed_by_handler_and_executor() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let observer = Arc::new(RecordingObserver::default());
    let token = TaskCancellationToken::new();
    let cancellation_reason = Arc::new(Mutex::new(None));
    let started = Arc::new(StartSignal::new());
    let executor = WorkerExecutor::with_lifecycle_observer(
        CancellationHandler {
            observed: Arc::clone(&cancellation_reason),
            started: Arc::clone(&started),
        },
        FixedRetry(RetryDecision::DoNotRetry),
        WorkerConfig::new(WorkerConcurrency::new(1)?, Duration::from_secs(5)),
        Arc::clone(&observer),
        Arc::new(FixedClock(SystemTime::UNIX_EPOCH)),
    )?;
    let execution = tokio::spawn({
        let context = context(&runtime, token.clone());
        async move { executor.execute(context, 1).await }
    });
    started.wait().await;
    assert!(token.cancel(TaskCancellationReason::Requested));

    let error = execution
        .await?
        .err()
        .ok_or(TestError("cancelled execution unexpectedly succeeded"))?;
    assert_eq!(
        error.cancellation_reason(),
        Some(TaskCancellationReason::Requested)
    );
    assert_eq!(error.category(), WorkerErrorCategory::Cancelled);
    assert_eq!(error.phase(), WorkerExecutionPhase::Handling);
    assert_eq!(error.remote_effect(), WorkerRemoteEffect::Unknown);
    assert_eq!(error.retry_decision(), None);
    assert_eq!(error.admission_reason(), None);
    assert_eq!(error.execution_timeout(), None);
    assert_eq!(error.source_error(), None);
    assert_eq!(error.to_string(), "worker handler execution was cancelled");
    assert!(Error::source(&error).is_none());
    assert_eq!(
        *lock(&cancellation_reason),
        Some(TaskCancellationReason::Requested)
    );
    assert_eq!(
        observer.events().last().map(|event| event.kind),
        Some(TaskLifecycleEventKind::StateChanged(TaskState::Cancelled(
            TaskCancellationReason::Requested,
        )))
    );
    assert!(error.into_source().is_none());
    Ok(())
}

#[tokio::test]
async fn dropping_execution_notifies_cooperative_children_and_lifecycle()
-> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let observer = Arc::new(RecordingObserver::default());
    let token = TaskCancellationToken::new();
    let child = tokio::spawn({
        let token = token.clone();
        async move { token.cancelled().await }
    });
    let state = Arc::new(GateState::new());
    let executor = WorkerExecutor::with_lifecycle_observer(
        ProgressGateHandler {
            state: Arc::clone(&state),
        },
        FixedRetry(RetryDecision::DoNotRetry),
        WorkerConfig::new(WorkerConcurrency::new(1)?, Duration::from_secs(5)),
        Arc::clone(&observer),
        Arc::new(FixedClock(SystemTime::UNIX_EPOCH)),
    )?;
    let execution = tokio::spawn({
        let context = context(&runtime, token.clone());
        async move { executor.execute(context, 1).await }
    });
    state.wait_started().await;

    execution.abort();
    let _join_result = execution.await;

    assert_eq!(
        token.reason(),
        Some(TaskCancellationReason::ExecutionDropped)
    );
    assert_eq!(child.await?, TaskCancellationReason::ExecutionDropped);
    assert_eq!(
        observer.events().last().map(|event| event.kind),
        Some(TaskLifecycleEventKind::StateChanged(TaskState::Cancelled(
            TaskCancellationReason::ExecutionDropped,
        )))
    );
    Ok(())
}

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(ServiceMetadata::new("lifecycle-test", "0.1.0", "test-1"))
}

fn context(runtime: &RuntimeHandle, cancellation: TaskCancellationToken) -> WorkerContext {
    WorkerContext::new(
        MessageId::random(),
        CorrelationId::random(),
        None,
        1,
        MessageMetadata::new(),
        runtime.shutdown_signal(),
    )
    .with_cancellation(cancellation)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
