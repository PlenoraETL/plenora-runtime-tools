//! Memory-pressure configuration, hysteresis, admission, and shutdown tests.

use std::{
    collections::VecDeque,
    convert::Infallible,
    error::Error,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use async_trait::async_trait;
use plenora_runtime_core::{HealthRegistry, ReadinessStatus, RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{RetryDecision, RetryPolicy};
use plenora_runtime_resources::{
    MemoryPressureConfig, MemoryPressureConfigError, MemoryPressureMonitor, MemoryPressureState,
    MemorySample, MemorySampleError, MemorySampleErrorKind, MemorySampler,
};
use plenora_runtime_worker::{
    WorkerAdmissionState, WorkerConcurrency, WorkerConfig, WorkerContext, WorkerExecutor,
    WorkerHandler,
};

#[derive(Debug)]
struct ScriptedSampler {
    samples: Mutex<VecDeque<Result<u64, MemorySampleErrorKind>>>,
}

impl ScriptedSampler {
    fn new(samples: impl IntoIterator<Item = Result<u64, MemorySampleErrorKind>>) -> Self {
        Self {
            samples: Mutex::new(samples.into_iter().collect()),
        }
    }
}

impl MemorySampler for ScriptedSampler {
    fn sample(&self) -> Result<MemorySample, MemorySampleError> {
        match lock(&self.samples).pop_front() {
            Some(Ok(resident_bytes)) => Ok(MemorySample { resident_bytes }),
            Some(Err(kind)) => Err(MemorySampleError::new(kind)),
            None => Err(MemorySampleError::new(MemorySampleErrorKind::Unavailable)),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Handler;

#[async_trait]
impl WorkerHandler<()> for Handler {
    type Error = Infallible;

    async fn handle(&self, _context: WorkerContext, (): ()) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct NoRetry;

impl RetryPolicy<Infallible> for NoRetry {
    fn decide(&self, _attempt: u32, error: &Infallible) -> RetryDecision {
        match *error {}
    }
}

#[test]
fn configuration_rejects_unsafe_thresholds() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        MemoryPressureConfig::new(Duration::ZERO, 80, 100, 150),
        Err(MemoryPressureConfigError::ZeroSampleInterval)
    );
    assert_eq!(
        MemoryPressureConfig::new(Duration::from_secs(1), 100, 100, 150),
        Err(MemoryPressureConfigError::ResumeNotBelowSoftLimit)
    );
    assert_eq!(
        MemoryPressureConfig::new(Duration::from_secs(1), 80, 150, 150),
        Err(MemoryPressureConfigError::SoftNotBelowHardLimit)
    );
    assert_eq!(
        config()?.with_confirmation_samples(0, 1),
        Err(MemoryPressureConfigError::ZeroPressureConfirmation)
    );
    assert_eq!(
        config()?.with_confirmation_samples(1, 0),
        Err(MemoryPressureConfigError::ZeroRecoveryConfirmation)
    );
    Ok(())
}

#[test]
fn pressure_pauses_and_hysteresis_resumes_worker_admission() -> Result<(), Box<dyn Error>> {
    let health = HealthRegistry::new();
    let worker = Arc::new(worker()?);
    let sampler = Arc::new(ScriptedSampler::new([
        Ok(60),
        Ok(110),
        Ok(120),
        Ok(85),
        Ok(70),
        Ok(70),
        Ok(160),
    ]));
    let monitor = MemoryPressureMonitor::new(
        config()?.with_confirmation_samples(2, 2)?,
        sampler,
        Arc::clone(&worker),
        health.clone(),
    );

    assert_eq!(worker.admission_state(), WorkerAdmissionState::Paused);
    assert_eq!(
        monitor.sample_once()?.snapshot.state,
        MemoryPressureState::Normal
    );
    assert_eq!(worker.admission_state(), WorkerAdmissionState::Accepting);
    assert_eq!(
        monitor.sample_once()?.snapshot.state,
        MemoryPressureState::Normal
    );
    assert_eq!(
        monitor.sample_once()?.snapshot.state,
        MemoryPressureState::Pressured
    );
    assert_eq!(worker.admission_state(), WorkerAdmissionState::Paused);
    assert_eq!(
        monitor.sample_once()?.snapshot.state,
        MemoryPressureState::Pressured
    );
    assert_eq!(
        monitor.sample_once()?.snapshot.state,
        MemoryPressureState::Pressured
    );
    assert_eq!(
        monitor.sample_once()?.snapshot.state,
        MemoryPressureState::Normal
    );
    assert_eq!(worker.admission_state(), WorkerAdmissionState::Accepting);
    assert_eq!(
        monitor.sample_once()?.snapshot.state,
        MemoryPressureState::Critical
    );
    assert_eq!(worker.admission_state(), WorkerAdmissionState::Paused);
    assert_eq!(health.readiness().status, ReadinessStatus::NotReady);

    Ok(())
}

#[test]
fn sampling_failure_is_fail_closed_and_source_is_redacted() -> Result<(), Box<dyn Error>> {
    let health = HealthRegistry::new();
    let worker = Arc::new(worker()?);
    let sampler = Arc::new(ScriptedSampler::new([
        Ok(60),
        Err(MemorySampleErrorKind::Unavailable),
    ]));
    let monitor =
        MemoryPressureMonitor::new(config()?, sampler, Arc::clone(&worker), health.clone());

    monitor.sample_once()?;
    let error = monitor
        .sample_once()
        .err()
        .ok_or_else(|| std::io::Error::other("scripted sampling failure was ignored"))?;

    assert_eq!(error.kind(), MemorySampleErrorKind::Unavailable);
    assert_eq!(monitor.snapshot().state, MemoryPressureState::Unavailable);
    assert_eq!(worker.admission_state(), WorkerAdmissionState::Paused);
    assert_eq!(health.readiness().status, ReadinessStatus::NotReady);
    assert!(!format!("{error:?}").contains("/proc"));
    Ok(())
}

#[test]
fn sampling_failure_requires_confirmed_recovery_before_reopening() -> Result<(), Box<dyn Error>> {
    let worker = Arc::new(worker()?);
    let sampler = Arc::new(ScriptedSampler::new([
        Err(MemorySampleErrorKind::Unavailable),
        Ok(60),
        Ok(60),
    ]));
    let monitor = MemoryPressureMonitor::new(
        config()?.with_confirmation_samples(1, 2)?,
        sampler,
        Arc::clone(&worker),
        HealthRegistry::new(),
    );

    assert!(monitor.sample_once().is_err());
    assert_eq!(monitor.snapshot().state, MemoryPressureState::Unavailable);
    assert_eq!(
        monitor.sample_once()?.snapshot.state,
        MemoryPressureState::Unavailable
    );
    assert_eq!(worker.admission_state(), WorkerAdmissionState::Paused);
    assert_eq!(
        monitor.sample_once()?.snapshot.state,
        MemoryPressureState::Normal
    );
    assert_eq!(worker.admission_state(), WorkerAdmissionState::Accepting);
    Ok(())
}

#[test]
fn repeated_pressure_and_recovery_cycles_never_leave_admission_stuck() -> Result<(), Box<dyn Error>>
{
    const CYCLES: usize = 1_000;
    let samples = (0..CYCLES).flat_map(|_| [Ok(120), Ok(60)]);
    let worker = Arc::new(worker()?);
    let monitor = MemoryPressureMonitor::new(
        config()?.with_confirmation_samples(1, 1)?,
        Arc::new(ScriptedSampler::new(samples)),
        Arc::clone(&worker),
        HealthRegistry::new(),
    );

    for _cycle in 0..CYCLES {
        assert_eq!(
            monitor.sample_once()?.snapshot.state,
            MemoryPressureState::Pressured
        );
        assert_eq!(worker.admission_state(), WorkerAdmissionState::Paused);
        assert_eq!(
            monitor.sample_once()?.snapshot.state,
            MemoryPressureState::Normal
        );
        assert_eq!(worker.admission_state(), WorkerAdmissionState::Accepting);
    }

    assert_eq!(monitor.snapshot().sequence, (CYCLES * 2) as u64);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn monitor_loop_stops_promptly_on_runtime_shutdown() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(ServiceMetadata::new("resource-test", "0.1.0", "one"));
    let worker = Arc::new(worker()?);
    let sampler = Arc::new(ScriptedSampler::new([Ok(60), Ok(60), Ok(60)]));
    let monitor = Arc::new(MemoryPressureMonitor::new(
        config()?,
        sampler,
        worker,
        HealthRegistry::new(),
    ));
    let task_monitor = Arc::clone(&monitor);
    let shutdown = runtime.shutdown_signal();
    let task = tokio::spawn(async move { task_monitor.run(shutdown).await });

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(runtime.request_shutdown());
    let report = task.await?;

    assert!(report.samples >= 1);
    assert_eq!(report.failures, 0);
    assert_eq!(report.final_state, MemoryPressureState::Normal);
    Ok(())
}

fn config() -> Result<MemoryPressureConfig, MemoryPressureConfigError> {
    MemoryPressureConfig::new(Duration::from_secs(1), 80, 100, 150)
}

fn worker() -> Result<WorkerExecutor<Handler, NoRetry>, Box<dyn Error>> {
    Ok(WorkerExecutor::new(
        Handler,
        NoRetry,
        WorkerConfig::new(WorkerConcurrency::new(1)?, Duration::from_secs(1)),
    )?)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(value) => value,
        Err(poisoned) => poisoned.into_inner(),
    }
}
