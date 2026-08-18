//! Bounded execution, retry delegation, and shutdown tests.

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
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{
    CausationId, CorrelationId, MessageId, MessageMetadata, RetryDecision, RetryPolicy,
};
use plenora_runtime_worker::{
    WorkerAdmissionReason, WorkerAdmissionState, WorkerConcurrency, WorkerConfig, WorkerContext,
    WorkerDrainOutcome, WorkerErrorCategory, WorkerExecutionError, WorkerExecutionPhase,
    WorkerExecutor, WorkerHandler, WorkerRemoteEffect,
};
use tokio::{
    sync::{Notify, Semaphore},
    time::timeout,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HandlerFailure(&'static str);

impl Display for HandlerFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for HandlerFailure {}

#[derive(Clone, Copy, Debug, Default)]
struct DoNotRetry;

impl RetryPolicy<HandlerFailure> for DoNotRetry {
    fn decide(&self, _attempt: u32, _error: &HandlerFailure) -> RetryDecision {
        RetryDecision::DoNotRetry
    }
}

#[derive(Debug)]
struct RecordingPolicy {
    attempts: Arc<Mutex<Vec<u32>>>,
    decision: RetryDecision,
}

impl RetryPolicy<HandlerFailure> for RecordingPolicy {
    fn decide(&self, attempt: u32, _error: &HandlerFailure) -> RetryDecision {
        lock(&self.attempts).push(attempt);
        self.decision
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct FailingHandler;

#[async_trait]
impl WorkerHandler<u64> for FailingHandler {
    type Error = HandlerFailure;

    async fn handle(&self, _ctx: WorkerContext, _message: u64) -> Result<(), Self::Error> {
        Err(HandlerFailure("sensitive handler detail"))
    }
}

#[derive(Debug)]
struct GateState {
    active: AtomicUsize,
    peak: AtomicUsize,
    started_count: AtomicUsize,
    started: Notify,
    releases: Semaphore,
}

impl GateState {
    fn new() -> Self {
        Self {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            started_count: AtomicUsize::new(0),
            started: Notify::new(),
            releases: Semaphore::new(0),
        }
    }

    async fn wait_for_started(&self, target: usize) -> Result<(), tokio::time::error::Elapsed> {
        timeout(Duration::from_secs(1), async {
            loop {
                let started = self.started.notified();
                if self.started_count.load(Ordering::SeqCst) >= target {
                    return;
                }
                started.await;
            }
        })
        .await
    }
}

#[derive(Clone, Debug)]
struct GateHandler {
    state: Arc<GateState>,
}

struct ActiveGuard {
    state: Arc<GateState>,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.state.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl WorkerHandler<u64> for GateHandler {
    type Error = HandlerFailure;

    async fn handle(&self, _ctx: WorkerContext, _message: u64) -> Result<(), Self::Error> {
        let active = self.state.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.state.peak.fetch_max(active, Ordering::SeqCst);
        self.state.started_count.fetch_add(1, Ordering::SeqCst);
        self.state.started.notify_waiters();
        let _active = ActiveGuard {
            state: Arc::clone(&self.state),
        };

        let permit = self
            .state
            .releases
            .acquire()
            .await
            .map_err(|_| HandlerFailure("test gate closed"))?;
        permit.forget();
        Ok(())
    }
}

#[tokio::test]
async fn execution_never_exceeds_configured_concurrency() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let state = Arc::new(GateState::new());
    let executor = WorkerExecutor::new(
        GateHandler {
            state: Arc::clone(&state),
        },
        DoNotRetry,
        config(2, Duration::from_secs(1)),
    )?;

    let first = executor.execute(context(&runtime, 1), 1);
    let second = executor.execute(context(&runtime, 1), 2);
    let third = executor.execute(context(&runtime, 1), 3);
    let control = async {
        state.wait_for_started(2).await?;
        state.releases.add_permits(3);
        Ok::<(), tokio::time::error::Elapsed>(())
    };

    let (first, second, third, control) = tokio::join!(first, second, third, control);
    first?;
    second?;
    third?;
    control?;

    assert_eq!(state.started_count.load(Ordering::SeqCst), 3);
    assert_eq!(state.peak.load(Ordering::SeqCst), 2);
    assert_eq!(executor.in_flight(), 0);

    Ok(())
}

#[tokio::test]
async fn handler_failure_preserves_source_and_delegates_retry_once() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let executor = WorkerExecutor::new(
        FailingHandler,
        RecordingPolicy {
            attempts: Arc::clone(&attempts),
            decision: RetryDecision::RetryAfter(Duration::from_secs(2)),
        },
        config(1, Duration::from_secs(1)),
    )?;

    let result = executor.execute(context(&runtime, 3), 7).await;
    let error = match result {
        Ok(()) => {
            return Err(HandlerFailure("the failing handler unexpectedly succeeded").into());
        }
        Err(error) => error,
    };
    let debug = format!("{error:?}");

    assert_eq!(error.category(), WorkerErrorCategory::Handler);
    assert_eq!(error.phase(), WorkerExecutionPhase::Handling);
    assert_eq!(error.remote_effect(), WorkerRemoteEffect::Unknown);
    assert_eq!(
        error.retry_decision(),
        Some(RetryDecision::RetryAfter(Duration::from_secs(2)))
    );
    assert_eq!(
        error.source_error(),
        Some(&HandlerFailure("sensitive handler detail"))
    );
    assert_eq!(error.admission_reason(), None);
    assert_eq!(error.cancellation_reason(), None);
    assert_eq!(error.execution_timeout(), None);
    assert_eq!(error.to_string(), "worker handler failed");
    assert!(Error::source(&error).is_some());
    assert_eq!(*lock(&attempts), vec![3]);
    assert!(!debug.contains("sensitive handler detail"));
    assert_eq!(
        error.into_source(),
        Some(HandlerFailure("sensitive handler detail"))
    );

    Ok(())
}

#[tokio::test]
async fn runtime_shutdown_rejects_a_job_waiting_for_capacity() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let state = Arc::new(GateState::new());
    let executor = WorkerExecutor::new(
        GateHandler {
            state: Arc::clone(&state),
        },
        DoNotRetry,
        config(1, Duration::from_secs(1)),
    )?;

    let first = executor.execute(context(&runtime, 1), 1);
    let waiting = executor.execute(context(&runtime, 1), 2);
    let control = async {
        state.wait_for_started(1).await?;
        assert!(runtime.request_shutdown());
        state.releases.add_permits(1);
        Ok::<(), tokio::time::error::Elapsed>(())
    };

    let (first, waiting, control) = tokio::join!(first, waiting, control);
    first?;
    control?;
    let waiting_error = match waiting {
        Ok(()) => {
            return Err(HandlerFailure("a waiting job started after shutdown").into());
        }
        Err(error) => error,
    };

    assert_eq!(
        waiting_error.admission_reason(),
        Some(WorkerAdmissionReason::ShutdownRequested)
    );
    assert_eq!(waiting_error.phase(), WorkerExecutionPhase::Admission);
    assert_eq!(
        waiting_error.remote_effect(),
        WorkerRemoteEffect::NotStarted
    );
    assert_eq!(waiting_error.retry_decision(), None);
    assert_eq!(waiting_error.category(), WorkerErrorCategory::Shutdown);
    assert_eq!(waiting_error.cancellation_reason(), None);
    assert_eq!(waiting_error.execution_timeout(), None);
    assert_eq!(waiting_error.source_error(), None);
    assert_eq!(
        waiting_error.to_string(),
        "runtime shutdown was requested before handler admission"
    );
    assert!(Error::source(&waiting_error).is_none());
    assert!(waiting_error.into_source().is_none());
    assert_eq!(state.started_count.load(Ordering::SeqCst), 1);

    Ok(())
}

#[tokio::test]
async fn drain_stops_admission_and_completes_in_flight_work() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let state = Arc::new(GateState::new());
    let executor = WorkerExecutor::new(
        GateHandler {
            state: Arc::clone(&state),
        },
        DoNotRetry,
        config(1, Duration::from_secs(1)),
    )?;

    let first = executor.execute(context(&runtime, 1), 1);
    let control = async {
        state.wait_for_started(1).await?;
        assert!(executor.begin_drain());
        assert!(!executor.begin_drain());

        let rejected = executor.execute(context(&runtime, 1), 2).await;
        assert_eq!(
            rejected
                .as_ref()
                .err()
                .and_then(WorkerExecutionError::admission_reason),
            Some(WorkerAdmissionReason::Draining)
        );
        let rejected = rejected
            .err()
            .ok_or_else(|| std::io::Error::other("draining executor accepted work"))?;
        assert_eq!(rejected.category(), WorkerErrorCategory::Shutdown);
        assert_eq!(rejected.phase(), WorkerExecutionPhase::Admission);
        assert_eq!(rejected.remote_effect(), WorkerRemoteEffect::NotStarted);
        assert_eq!(
            rejected.to_string(),
            "worker is draining and no longer accepts jobs"
        );

        state.releases.add_permits(1);
        Ok::<WorkerDrainOutcome, Box<dyn Error>>(executor.drain().await)
    };

    let (first, control) = tokio::join!(first, control);
    first?;
    let outcome = control?;

    assert_eq!(outcome, WorkerDrainOutcome::Completed);
    assert!(!executor.is_accepting());
    assert_eq!(executor.in_flight(), 0);
    assert_eq!(state.started_count.load(Ordering::SeqCst), 1);

    Ok(())
}

#[tokio::test]
async fn admission_pause_is_reversible_but_drain_is_terminal() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let executor = WorkerExecutor::new(
        FailingHandler,
        DoNotRetry,
        config(1, Duration::from_secs(1)),
    )?;

    assert_eq!(executor.admission_state(), WorkerAdmissionState::Accepting);
    assert!(executor.pause_admission());
    assert!(!executor.pause_admission());
    assert_eq!(executor.admission_state(), WorkerAdmissionState::Paused);

    let paused = executor.execute(context(&runtime, 1), 1).await;
    let paused = paused
        .err()
        .ok_or_else(|| std::io::Error::other("paused executor accepted work"))?;
    assert_eq!(
        paused.admission_reason(),
        Some(WorkerAdmissionReason::Paused)
    );
    assert_eq!(paused.category(), WorkerErrorCategory::Capacity);
    assert_eq!(paused.remote_effect(), WorkerRemoteEffect::NotStarted);

    assert!(executor.resume_admission());
    assert!(!executor.resume_admission());
    assert_eq!(executor.admission_state(), WorkerAdmissionState::Accepting);
    assert!(executor.execute(context(&runtime, 1), 2).await.is_err());

    assert!(executor.begin_drain());
    assert_eq!(executor.admission_state(), WorkerAdmissionState::Draining);
    assert!(!executor.resume_admission());
    assert_eq!(executor.admission_state(), WorkerAdmissionState::Draining);

    Ok(())
}

#[tokio::test]
async fn admission_pause_rejects_new_work_without_cancelling_active_handler()
-> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let state = Arc::new(GateState::new());
    let executor = WorkerExecutor::new(
        GateHandler {
            state: Arc::clone(&state),
        },
        DoNotRetry,
        config(1, Duration::from_secs(1)),
    )?;

    let active = executor.execute(context(&runtime, 1), 1);
    let control = async {
        state.wait_for_started(1).await?;
        assert!(executor.pause_admission());
        assert_eq!(executor.in_flight(), 1);
        let rejected = executor.execute(context(&runtime, 1), 2).await;
        assert_eq!(
            rejected
                .as_ref()
                .err()
                .and_then(WorkerExecutionError::admission_reason),
            Some(WorkerAdmissionReason::Paused)
        );
        assert_eq!(executor.in_flight(), 1);
        state.releases.add_permits(1);
        Ok::<(), Box<dyn Error>>(())
    };

    let (active, control) = tokio::join!(active, control);
    active?;
    control?;
    assert_eq!(state.started_count.load(Ordering::SeqCst), 1);
    assert_eq!(executor.in_flight(), 0);
    assert!(executor.resume_admission());
    Ok(())
}

#[tokio::test]
async fn drain_timeout_reports_remaining_work_without_unbounded_wait() -> Result<(), Box<dyn Error>>
{
    let runtime = runtime();
    let state = Arc::new(GateState::new());
    let executor = WorkerExecutor::new(
        GateHandler {
            state: Arc::clone(&state),
        },
        DoNotRetry,
        config(1, Duration::from_millis(20)),
    )?;

    let first = executor.execute(context(&runtime, 1), 1);
    let control = async {
        state.wait_for_started(1).await?;
        let outcome = executor.drain().await;
        state.releases.add_permits(1);
        Ok::<WorkerDrainOutcome, tokio::time::error::Elapsed>(outcome)
    };

    let (first, control) = tokio::join!(first, control);
    first?;
    let outcome = control?;

    assert_eq!(
        outcome,
        WorkerDrainOutcome::TimedOut {
            remaining_in_flight: 1,
        }
    );
    assert_eq!(executor.in_flight(), 0);

    Ok(())
}

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(ServiceMetadata::new("worker-test", "0.1.0", "test-1"))
}

fn context(runtime: &RuntimeHandle, attempt: u32) -> WorkerContext {
    WorkerContext::new(
        MessageId::random(),
        CorrelationId::random(),
        Some(CausationId::random()),
        attempt,
        MessageMetadata::new(),
        runtime.shutdown_signal(),
    )
}

fn config(max_in_flight: usize, shutdown_grace_period: Duration) -> WorkerConfig {
    WorkerConfig::new(WorkerConcurrency { max_in_flight }, shutdown_grace_period)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
