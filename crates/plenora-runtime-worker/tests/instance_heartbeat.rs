//! Worker-instance heartbeat and capacity-sampling tests.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
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
    WorkerConcurrency, WorkerConfig, WorkerContext, WorkerExecutor, WorkerHandler,
    WorkerInstanceHeartbeat, WorkerInstanceHeartbeatConfig, WorkerInstanceHeartbeatConfigError,
    WorkerInstanceHeartbeatError, WorkerInstanceHeartbeatObserver, WorkerInstanceIdentity,
    WorkerInstanceStatus,
};
use tokio::sync::{Notify, Semaphore};

#[derive(Clone, Copy, Debug)]
struct TestError;

impl Display for TestError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("test failure")
    }
}

impl Error for TestError {}

#[derive(Clone, Copy, Debug)]
struct FixedRetry;

impl RetryPolicy<TestError> for FixedRetry {
    fn decide(&self, _attempt: u32, _error: &TestError) -> RetryDecision {
        RetryDecision::DoNotRetry
    }
}

#[derive(Debug)]
struct StartSignal {
    started: AtomicBool,
    changed: Notify,
}

impl StartSignal {
    const fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
            changed: Notify::const_new(),
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

#[derive(Clone, Debug)]
struct GateHandler {
    started: Arc<StartSignal>,
    release: Arc<Semaphore>,
}

#[async_trait]
impl WorkerHandler<u8> for GateHandler {
    type Error = TestError;

    async fn handle(&self, _context: WorkerContext, _message: u8) -> Result<(), Self::Error> {
        self.started.mark_started();
        let permit = self.release.acquire().await.map_err(|_error| TestError)?;
        permit.forget();
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

#[derive(Debug, Default)]
struct RecordingObserver {
    heartbeats: Mutex<Vec<WorkerInstanceHeartbeat>>,
}

impl RecordingObserver {
    fn heartbeats(&self) -> Vec<WorkerInstanceHeartbeat> {
        lock(&self.heartbeats).clone()
    }
}

impl WorkerInstanceHeartbeatObserver for RecordingObserver {
    fn record(&self, heartbeat: WorkerInstanceHeartbeat) {
        lock(&self.heartbeats).push(heartbeat);
    }
}

#[tokio::test]
async fn reporter_tracks_capacity_and_monotonic_lifecycle() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(ServiceMetadata::new(
        "heartbeat-test",
        "0.1.0",
        "instance-1",
    ));
    let started = Arc::new(StartSignal::new());
    let release = Arc::new(Semaphore::new(0));
    let executor = WorkerExecutor::new(
        GateHandler {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        },
        FixedRetry,
        WorkerConfig::new(WorkerConcurrency::new(2)?, Duration::from_secs(2)),
    )?;
    let observer = Arc::new(RecordingObserver::default());
    let reporter = executor.instance_heartbeat_reporter(
        WorkerInstanceIdentity::new(runtime.metadata(), "jobs"),
        Arc::clone(&observer),
        Arc::new(FixedClock(SystemTime::UNIX_EPOCH)),
    );

    let starting = reporter.heartbeat()?;
    let ready = reporter.mark_ready()?;
    let execution = tokio::spawn({
        let executor = executor.clone();
        let context = context(&runtime);
        async move { executor.execute(context, 1).await }
    });
    started.wait().await;
    let busy = reporter.heartbeat()?;
    assert!(executor.begin_drain());
    let draining = reporter.mark_draining()?;
    release.add_permits(1);
    execution.await??;
    let stopped = reporter.mark_stopped()?;

    assert_eq!(starting.status, WorkerInstanceStatus::Starting);
    assert_eq!(ready.status, WorkerInstanceStatus::Ready);
    assert_eq!(busy.sequence, 3);
    assert_eq!(busy.max_in_flight, 2);
    assert_eq!(busy.in_flight, 1);
    assert_eq!(busy.available_slots, 1);
    assert_eq!(draining.status, WorkerInstanceStatus::Draining);
    assert_eq!(stopped.status, WorkerInstanceStatus::Stopped);
    assert_eq!(stopped.in_flight, 0);
    assert_eq!(stopped.available_slots, 2);
    assert_eq!(stopped.identity.instance_id.as_ref(), "instance-1");
    assert_eq!(observer.heartbeats().len(), 5);
    assert_eq!(
        reporter.mark_ready(),
        Err(WorkerInstanceHeartbeatError::InvalidTransition {
            from: WorkerInstanceStatus::Stopped,
            to: WorkerInstanceStatus::Ready,
        })
    );
    Ok(())
}

#[test]
fn interval_rejects_a_busy_loop() {
    assert_eq!(
        WorkerInstanceHeartbeatConfig::new(Duration::ZERO),
        Err(WorkerInstanceHeartbeatConfigError::ZeroInterval)
    );
    assert_eq!(
        WorkerInstanceHeartbeatConfig::default().interval(),
        Duration::from_secs(10)
    );
}

fn context(runtime: &RuntimeHandle) -> WorkerContext {
    WorkerContext::new(
        MessageId::random(),
        CorrelationId::random(),
        None,
        1,
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
