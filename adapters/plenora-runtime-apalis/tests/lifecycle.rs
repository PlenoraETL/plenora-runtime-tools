//! Apalis engine and graceful shutdown integration tests.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use apalis::prelude::{
    MemoryStorage, MessageQueue, Monitor, WorkerBuilder, WorkerBuilderExt, WorkerFactory,
};
use async_trait::async_trait;
use plenora_runtime_apalis::{
    ApalisAdapterConfig, ApalisDisposition, ApalisJob, ApalisShutdownBridge, ApalisWorkerService,
};
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{
    CorrelationId, MessageId, MessageMetadata, RetryDecision, RetryPolicy,
};
use plenora_runtime_worker::{
    WorkerConcurrency, WorkerConfig, WorkerContext, WorkerDrainOutcome, WorkerHandler,
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
struct NeverRetry;

impl RetryPolicy<TestError> for NeverRetry {
    fn decide(&self, _attempt: u32, _error: &TestError) -> RetryDecision {
        RetryDecision::DoNotRetry
    }
}

#[derive(Clone, Debug)]
struct RecordingHandler {
    handled: Arc<AtomicUsize>,
    completed: Arc<Notify>,
}

#[async_trait]
impl WorkerHandler<u64> for RecordingHandler {
    type Error = TestError;

    async fn handle(&self, _ctx: WorkerContext, _message: u64) -> Result<(), Self::Error> {
        self.handled.fetch_add(1, Ordering::SeqCst);
        self.completed.notify_one();
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct GateHandler {
    started: Arc<Notify>,
    release: Arc<Semaphore>,
}

#[async_trait]
impl WorkerHandler<u64> for GateHandler {
    type Error = TestError;

    async fn handle(&self, _ctx: WorkerContext, _message: u64) -> Result<(), Self::Error> {
        self.started.notify_one();
        let permit = self
            .release
            .acquire()
            .await
            .map_err(|_| TestError("handler gate closed"))?;
        permit.forget();
        Ok(())
    }
}

#[tokio::test]
async fn memory_storage_worker_builder_executes_plenora_service() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let handled = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(Notify::new());
    let config = ApalisAdapterConfig::new("memory-worker", worker_config(2))?;
    let service = ApalisWorkerService::new(
        RecordingHandler {
            handled: Arc::clone(&handled),
            completed: Arc::clone(&completed),
        },
        NeverRetry,
        config.clone(),
    )?;
    let signal_service = service.clone();
    let bridge = ApalisShutdownBridge::new(runtime.shutdown_signal());
    let grace_period = config.worker().shutdown_grace_period;
    let mut storage = MemoryStorage::new();
    storage
        .enqueue(ApalisJob::new(context(&runtime, 1), 11))
        .await
        .map_err(|()| TestError("memory enqueue failed"))?;

    let worker = WorkerBuilder::new(config.worker_name())
        .concurrency(config.worker().concurrency.max_in_flight)
        .catch_panic()
        .backend(storage)
        .build(service);
    let signal = async move {
        completed.notified().await;
        let _started = runtime.request_shutdown();
        let _draining = bridge.wait_and_begin_drain(&signal_service).await;
        Ok::<(), std::io::Error>(())
    };

    timeout(
        Duration::from_secs(2),
        Monitor::new()
            .with_terminator(tokio::time::sleep(grace_period))
            .register(worker)
            .run_with_signal(signal),
    )
    .await??;

    assert_eq!(handled.load(Ordering::SeqCst), 1);

    Ok(())
}

#[tokio::test]
async fn shutdown_bridge_closes_admission_then_drains_active_handler() -> Result<(), Box<dyn Error>>
{
    let runtime = runtime();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let service = ApalisWorkerService::new(
        GateHandler {
            started: Arc::clone(&started),
            release: Arc::clone(&release),
        },
        NeverRetry,
        ApalisAdapterConfig::new("shutdown-worker", worker_config(1))?,
    )?;
    let bridge = ApalisShutdownBridge::new(runtime.shutdown_signal());
    assert!(!bridge.is_cancelled());

    let work = service.execute(ApalisJob::new(context(&runtime, 1), 1));
    let shutdown = async {
        started.notified().await;
        assert!(runtime.request_shutdown());
        assert!(bridge.wait_and_begin_drain(&service).await);
        assert!(!service.is_accepting());

        let rejected = service
            .execute(ApalisJob::new(context(&runtime, 1), 2))
            .await;
        assert!(matches!(
            rejected.disposition(),
            ApalisDisposition::Shutdown(_)
        ));

        release.add_permits(1);
        service.drain().await
    };

    let (work, drain) = tokio::join!(work, shutdown);

    assert_eq!(work.disposition(), ApalisDisposition::Completed);
    assert_eq!(drain, WorkerDrainOutcome::Completed);
    assert_eq!(service.in_flight(), 0);
    assert!(bridge.is_cancelled());

    Ok(())
}

#[tokio::test]
async fn shutdown_bridge_combines_cancellation_admission_and_drain() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let service = ApalisWorkerService::new(
        RecordingHandler {
            handled: Arc::new(AtomicUsize::new(0)),
            completed: Arc::new(Notify::new()),
        },
        NeverRetry,
        ApalisAdapterConfig::new("combined-shutdown-worker", worker_config(1))?,
    )?;
    let bridge = ApalisShutdownBridge::new(runtime.shutdown_signal());
    assert!(runtime.request_shutdown());

    assert_eq!(
        bridge.shutdown(&service).await,
        WorkerDrainOutcome::Completed
    );
    assert!(!service.is_accepting());
    Ok(())
}

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(ServiceMetadata::new(
        "apalis-adapter-test",
        "0.1.0",
        "test-1",
    ))
}

fn context(runtime: &RuntimeHandle, attempt: u32) -> WorkerContext {
    WorkerContext::new(
        MessageId::random(),
        CorrelationId::random(),
        None,
        attempt,
        MessageMetadata::new(),
        runtime.shutdown_signal(),
    )
}

fn worker_config(max_in_flight: usize) -> WorkerConfig {
    WorkerConfig::new(WorkerConcurrency { max_in_flight }, Duration::from_secs(1))
}
