//! Public resource-monitor contract and diagnostic coverage.

use std::{
    error::Error,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use plenora_runtime_core::{HealthRegistry, RuntimeHandle, ServiceMetadata};
use plenora_runtime_resources::{
    MemoryPressureConfig, MemoryPressureConfigError, MemoryPressureMonitor,
    MemoryPressureObservation, MemoryPressureObserver, MemorySample, MemorySampleError,
    MemorySampleErrorKind, MemorySampler, ProcessMemorySampler,
};
use plenora_runtime_worker::{WorkerAdmissionControl, WorkerAdmissionState};

#[derive(Debug, Default)]
struct Admission {
    paused: Mutex<bool>,
}

impl WorkerAdmissionControl for Admission {
    fn pause_admission(&self) -> bool {
        let mut paused = lock(&self.paused);
        let changed = !*paused;
        *paused = true;
        changed
    }

    fn resume_admission(&self) -> bool {
        let mut paused = lock(&self.paused);
        let changed = *paused;
        *paused = false;
        changed
    }

    fn admission_state(&self) -> WorkerAdmissionState {
        if *lock(&self.paused) {
            WorkerAdmissionState::Paused
        } else {
            WorkerAdmissionState::Accepting
        }
    }
}

#[derive(Debug)]
struct ConstantSampler(u64);

impl MemorySampler for ConstantSampler {
    fn sample(&self) -> Result<MemorySample, MemorySampleError> {
        Ok(MemorySample {
            resident_bytes: self.0,
        })
    }
}

#[derive(Debug, Default)]
struct RecordingObserver {
    observations: Mutex<Vec<MemoryPressureObservation>>,
}

impl MemoryPressureObserver for RecordingObserver {
    fn record(&self, observation: MemoryPressureObservation) {
        lock(&self.observations).push(observation);
    }
}

#[test]
fn every_configuration_error_and_getter_is_observable() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        MemoryPressureConfig::new(Duration::from_secs(1), 0, 10, 20),
        Err(MemoryPressureConfigError::ZeroResumeThreshold)
    );
    let variants = [
        MemoryPressureConfigError::ZeroSampleInterval,
        MemoryPressureConfigError::ZeroResumeThreshold,
        MemoryPressureConfigError::ResumeNotBelowSoftLimit,
        MemoryPressureConfigError::SoftNotBelowHardLimit,
        MemoryPressureConfigError::ZeroPressureConfirmation,
        MemoryPressureConfigError::ZeroRecoveryConfirmation,
    ];
    for variant in variants {
        assert!(!variant.to_string().is_empty());
    }

    let config = MemoryPressureConfig::new(Duration::from_secs(3), 10, 20, 30)?
        .with_confirmation_samples(4, 5)?;
    assert_eq!(config.sample_interval(), Duration::from_secs(3));
    assert_eq!(config.resume_below_bytes(), 10);
    assert_eq!(config.soft_limit_bytes(), 20);
    assert_eq!(config.hard_limit_bytes(), 30);
    assert_eq!(config.pressure_confirmation_samples(), 4);
    assert_eq!(config.recovery_confirmation_samples(), 5);
    assert_eq!(config.validate(), Ok(()));
    Ok(())
}

#[test]
fn every_sample_error_is_stable_and_sources_are_redacted() {
    let variants = [
        MemorySampleErrorKind::UnsupportedPlatform,
        MemorySampleErrorKind::ReadFailed,
        MemorySampleErrorKind::MissingResidentSet,
        MemorySampleErrorKind::InvalidResidentSet,
        MemorySampleErrorKind::ResidentSetOverflow,
        MemorySampleErrorKind::Unavailable,
    ];
    for kind in variants {
        let error = MemorySampleError::new(kind);
        assert_eq!(error.kind(), kind);
        assert!(!error.to_string().is_empty());
        assert!(Error::source(&error).is_none());
    }

    let sourced = MemorySampleError::with_source(
        MemorySampleErrorKind::ReadFailed,
        std::io::Error::other("private sampler source"),
    );
    assert!(Error::source(&sourced).is_some());
    assert!(!format!("{sourced:?}").contains("private sampler source"));
}

#[test]
fn explicit_observer_receives_successful_state_change() -> Result<(), Box<dyn Error>> {
    let config = MemoryPressureConfig::new(Duration::from_secs(1), 10, 20, 30)?;
    let admission = Arc::new(Admission::default());
    let observer = Arc::new(RecordingObserver::default());
    let monitor = MemoryPressureMonitor::with_observer(
        config,
        Arc::new(ConstantSampler(5)),
        Arc::clone(&admission),
        Arc::clone(&observer),
        HealthRegistry::new(),
    );

    assert_eq!(monitor.config(), config);
    assert_eq!(monitor.snapshot().sequence, 0);
    let observation = monitor.sample_once()?;
    assert!(observation.changed);
    assert!(observation.sample_succeeded);
    assert_eq!(lock(&observer.observations).as_slice(), &[observation]);
    assert_eq!(admission.admission_state(), WorkerAdmissionState::Accepting);
    Ok(())
}

#[test]
fn process_sampler_never_fabricates_a_zero_sample() {
    let result = ProcessMemorySampler.sample();
    match result {
        Ok(sample) => assert!(sample.resident_bytes > 0),
        Err(error) => assert_eq!(error.kind(), MemorySampleErrorKind::UnsupportedPlatform),
    }
}

#[tokio::test(start_paused = true)]
async fn pre_cancelled_monitor_run_returns_without_sampling() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(ServiceMetadata::new("resource-contract", "0.1.0", "one"));
    assert!(runtime.request_shutdown());
    let monitor = MemoryPressureMonitor::new(
        MemoryPressureConfig::new(Duration::from_secs(1), 10, 20, 30)?,
        Arc::new(ConstantSampler(5)),
        Arc::new(Admission::default()),
        HealthRegistry::new(),
    );
    let report = monitor.run(runtime.shutdown_signal()).await;
    assert_eq!(report.samples, 0);
    assert_eq!(report.failures, 0);
    Ok(())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(value) => value,
        Err(poisoned) => poisoned.into_inner(),
    }
}
