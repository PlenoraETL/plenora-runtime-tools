//! Cross-platform subprocess containment contracts.

use std::{env, error::Error, path::PathBuf, sync::Arc, thread, time::Duration};

use plenora_runtime_subprocess::{
    MAX_ARGUMENT_BYTES, MAX_ARGUMENT_COUNT, MAX_CAPTURE_BYTES, MAX_CONCURRENT_SUBPROCESSES,
    MAX_ENVIRONMENT_BYTES, MAX_ENVIRONMENT_ENTRIES, ProcessTreeMode, SubprocessCancellationPhase,
    SubprocessConfigError, SubprocessErrorKind, SubprocessSpec, SubprocessSpecError,
    SubprocessSupervisor, SubprocessSupervisorConfig, SubprocessTermination,
};
use tokio::sync::Notify;

const FIXTURE_MODE: &str = "PLENORA_SUBPROCESS_FIXTURE_MODE";

#[test]
#[ignore = "executed only as a child by the subprocess supervisor tests"]
fn subprocess_fixture_child() {
    match env::var(FIXTURE_MODE).as_deref() {
        Ok("output") => {
            for _index in 0..512 {
                print!("stdout-bounded;");
                eprint!("stderr-bounded;");
            }
        }
        Ok("sleep") => thread::sleep(Duration::from_secs(30)),
        Ok("memory") => {
            let mut allocation = vec![0_u8; 32 * 1024 * 1024];
            for byte in allocation.iter_mut().step_by(4096) {
                *byte = 1;
            }
            thread::sleep(Duration::from_secs(30));
            std::hint::black_box(allocation);
        }
        Ok("success") => print!("fixture-success"),
        _ => std::process::abort(),
    }
}

#[test]
fn configuration_is_bounded_and_errors_are_stable() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        SubprocessSupervisorConfig::new(0, Duration::from_secs(1)),
        Err(SubprocessConfigError::ZeroConcurrency)
    );
    assert_eq!(
        SubprocessSupervisorConfig::new(MAX_CONCURRENT_SUBPROCESSES + 1, Duration::from_secs(1)),
        Err(SubprocessConfigError::ConcurrencyAboveMaximum)
    );
    assert_eq!(
        SubprocessSupervisorConfig::new(1, Duration::ZERO),
        Err(SubprocessConfigError::ZeroExecutionTimeout)
    );
    let config = SubprocessSupervisorConfig::new(2, Duration::from_secs(3))?
        .with_output_limits(128, 256)?
        .with_termination_timeouts(
            Duration::from_millis(100),
            Duration::from_secs(1),
            Duration::from_secs(1),
        )?
        .with_process_tree_mode(ProcessTreeMode::DirectChild);
    assert_eq!(config.max_concurrent(), 2);
    assert_eq!(config.execution_timeout(), Duration::from_secs(3));
    assert_eq!(
        config.with_output_limits(0, 1),
        Err(SubprocessConfigError::ZeroCaptureLimit)
    );
    assert_eq!(
        config.with_output_limits(MAX_CAPTURE_BYTES + 1, 1),
        Err(SubprocessConfigError::CaptureLimitAboveMaximum)
    );
    assert_eq!(
        config.with_termination_timeouts(
            Duration::ZERO,
            Duration::from_secs(1),
            Duration::from_secs(1)
        ),
        Err(SubprocessConfigError::ZeroTerminationTimeout)
    );
    assert_eq!(
        config.with_resident_memory_limit(0, Duration::from_secs(1)),
        Err(SubprocessConfigError::ZeroMemoryLimit)
    );
    assert_eq!(
        config.with_resident_memory_limit(u64::MAX, Duration::from_secs(1)),
        Err(SubprocessConfigError::MemoryLimitAboveMaximum)
    );
    assert_eq!(
        config.with_resident_memory_limit(1, Duration::ZERO),
        Err(SubprocessConfigError::ZeroMemorySampleInterval)
    );
    for error in [
        SubprocessConfigError::ZeroConcurrency,
        SubprocessConfigError::ConcurrencyAboveMaximum,
        SubprocessConfigError::ZeroExecutionTimeout,
        SubprocessConfigError::ZeroCaptureLimit,
        SubprocessConfigError::CaptureLimitAboveMaximum,
        SubprocessConfigError::ZeroTerminationTimeout,
        SubprocessConfigError::ZeroMemoryLimit,
        SubprocessConfigError::MemoryLimitAboveMaximum,
        SubprocessConfigError::ZeroMemorySampleInterval,
        SubprocessConfigError::MemoryLimitUnsupported,
    ] {
        assert!(!error.to_string().is_empty());
    }

    Ok(())
}

#[test]
fn specification_is_bounded_and_redacted() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        SubprocessSpec::new(PathBuf::new()),
        Err(SubprocessSpecError::EmptyExecutable)
    );
    let spec = fixture_spec("sensitive-value")?;
    let diagnostics = format!("{spec:?}");
    assert!(!diagnostics.contains("sensitive-value"));
    assert!(!diagnostics.contains(FIXTURE_MODE));
    assert_eq!(spec.executable(), env::current_exe()?.as_path());
    assert_eq!(
        SubprocessSpec::new(env::current_exe()?)?.with_environment("BAD=KEY", "value"),
        Err(SubprocessSpecError::InvalidEnvironmentEntry)
    );
    assert_eq!(
        SubprocessSpec::new(env::current_exe()?)?.with_environment("KEY", "bad\0value"),
        Err(SubprocessSpecError::InvalidEnvironmentEntry)
    );

    let oversized_argument = "x".repeat(MAX_ARGUMENT_BYTES.saturating_add(1));
    assert_eq!(
        SubprocessSpec::new(env::current_exe()?)?.with_argument(oversized_argument),
        Err(SubprocessSpecError::ArgumentsTooLarge)
    );
    let mut argument_result = SubprocessSpec::new(env::current_exe()?);
    for _index in 0..=MAX_ARGUMENT_COUNT {
        argument_result = argument_result.and_then(|spec| spec.with_argument("x"));
    }
    assert_eq!(argument_result, Err(SubprocessSpecError::TooManyArguments));

    let oversized_environment = "x".repeat(MAX_ENVIRONMENT_BYTES.saturating_add(1));
    assert_eq!(
        SubprocessSpec::new(env::current_exe()?)?.with_environment("KEY", oversized_environment),
        Err(SubprocessSpecError::EnvironmentTooLarge)
    );
    let mut environment_result = SubprocessSpec::new(env::current_exe()?);
    for index in 0..=MAX_ENVIRONMENT_ENTRIES {
        environment_result =
            environment_result.and_then(|spec| spec.with_environment(format!("KEY_{index}"), "x"));
    }
    assert_eq!(
        environment_result,
        Err(SubprocessSpecError::TooManyEnvironmentEntries)
    );
    let inherited = SubprocessSpec::new(env::current_exe()?)?
        .with_current_directory(env::temp_dir())
        .with_inherited_environment();
    let inherited_debug = format!("{inherited:?}");
    assert!(inherited_debug.contains("current_directory: true"));
    assert!(inherited_debug.contains("clear_environment: false"));
    for error in [
        SubprocessSpecError::EmptyExecutable,
        SubprocessSpecError::TooManyArguments,
        SubprocessSpecError::ArgumentsTooLarge,
        SubprocessSpecError::InvalidEnvironmentEntry,
        SubprocessSpecError::TooManyEnvironmentEntries,
        SubprocessSpecError::EnvironmentTooLarge,
    ] {
        assert!(!error.to_string().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn spawn_failure_is_source_preserving_redacted_and_releases_capacity()
-> Result<(), Box<dyn Error>> {
    let supervisor =
        SubprocessSupervisor::new(SubprocessSupervisorConfig::new(1, Duration::from_secs(1))?);
    let private_path = "/definitely/not/a/real/plenora-executable";
    let error = supervisor
        .run_to_completion(&SubprocessSpec::new(private_path)?)
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("missing executable unexpectedly spawned"))?;
    assert_eq!(error.kind(), SubprocessErrorKind::Spawn);
    assert!(Error::source(&error).is_some());
    assert!(!error.to_string().contains(private_path));
    assert!(!format!("{error:?}").contains(private_path));
    let snapshot = supervisor.snapshot();
    assert_eq!(snapshot.spawn_failures, 1);
    assert_eq!(snapshot.started, 0);
    assert_eq!(snapshot.available, 1);
    Ok(())
}

#[tokio::test]
async fn captures_output_with_hard_retention_bounds() -> Result<(), Box<dyn Error>> {
    let supervisor = SubprocessSupervisor::new(
        SubprocessSupervisorConfig::new(1, Duration::from_secs(5))?.with_output_limits(128, 96)?,
    );
    let report = supervisor
        .run_to_completion(&fixture_spec("output")?)
        .await?;
    assert!(report.success());
    assert_eq!(report.stdout.bytes().len(), 128);
    assert_eq!(report.stderr.bytes().len(), 96);
    assert!(report.stdout.truncated());
    assert!(report.stderr.truncated());
    assert!(!report.stdout.read_failed());
    assert!(!report.stderr.read_failed());
    let snapshot = supervisor.snapshot();
    assert_eq!(snapshot.capacity, 1);
    assert_eq!(snapshot.in_flight, 0);
    assert_eq!(snapshot.available, 1);
    assert_eq!(snapshot.started, 1);
    assert_eq!(snapshot.completed, 1);
    Ok(())
}

#[tokio::test]
async fn timeout_reaps_child_and_releases_capacity() -> Result<(), Box<dyn Error>> {
    let supervisor = SubprocessSupervisor::new(
        SubprocessSupervisorConfig::new(1, Duration::from_millis(100))?.with_termination_timeouts(
            Duration::from_millis(100),
            Duration::from_secs(2),
            Duration::from_secs(1),
        )?,
    );
    let report = supervisor
        .run_to_completion(&fixture_spec("sleep")?)
        .await?;
    assert_eq!(report.termination, SubprocessTermination::TimedOut);
    assert!(!report.success());
    assert_eq!(supervisor.snapshot().available, 1);
    assert_eq!(supervisor.snapshot().timeouts, 1);

    let recovered = supervisor
        .run_to_completion(&fixture_spec("success")?)
        .await?;
    assert!(recovered.success());
    Ok(())
}

#[tokio::test]
async fn queued_and_running_cancellation_are_distinct_and_bounded() -> Result<(), Box<dyn Error>> {
    let supervisor = Arc::new(SubprocessSupervisor::new(
        SubprocessSupervisorConfig::new(1, Duration::from_secs(10))?.with_termination_timeouts(
            Duration::from_millis(100),
            Duration::from_secs(2),
            Duration::from_secs(1),
        )?,
    ));
    let running_cancellation = Arc::new(Notify::new());
    let running_supervisor = Arc::clone(&supervisor);
    let running_signal = Arc::clone(&running_cancellation);
    let running_spec = fixture_spec("sleep")?;
    let running = tokio::spawn(async move {
        running_supervisor
            .run(&running_spec, running_signal.notified())
            .await
    });
    wait_until(|| supervisor.snapshot().in_flight == 1).await?;

    let queued = supervisor
        .run(&fixture_spec("success")?, std::future::ready(()))
        .await?;
    assert_eq!(
        queued.termination,
        SubprocessTermination::Cancelled(SubprocessCancellationPhase::Queued)
    );
    running_cancellation.notify_one();
    let running = running.await??;
    assert_eq!(
        running.termination,
        SubprocessTermination::Cancelled(SubprocessCancellationPhase::Running)
    );
    assert_eq!(supervisor.snapshot().cancellations, 2);
    assert_eq!(supervisor.snapshot().available, 1);
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn linux_rss_limit_terminates_and_reaps_process_tree() -> Result<(), Box<dyn Error>> {
    let supervisor = SubprocessSupervisor::new(
        SubprocessSupervisorConfig::new(1, Duration::from_secs(10))?
            .with_resident_memory_limit(1024 * 1024, Duration::from_millis(10))?
            .with_termination_timeouts(
                Duration::from_millis(100),
                Duration::from_secs(2),
                Duration::from_secs(1),
            )?,
    );
    let report = supervisor
        .run_to_completion(&fixture_spec("memory")?)
        .await?;
    assert!(matches!(
        report.termination,
        SubprocessTermination::ResidentMemoryLimitExceeded { .. }
    ));
    assert_eq!(supervisor.snapshot().memory_terminations, 1);
    assert_eq!(supervisor.snapshot().available, 1);
    Ok(())
}

fn fixture_spec(mode: &str) -> Result<SubprocessSpec, SubprocessSpecError> {
    SubprocessSpec::new(env::current_exe().map_err(|_error| SubprocessSpecError::EmptyExecutable)?)?
        .with_argument("--exact")?
        .with_argument("subprocess_fixture_child")?
        .with_argument("--ignored")?
        .with_argument("--nocapture")?
        .with_environment(FIXTURE_MODE, mode)
}

async fn wait_until<F>(predicate: F) -> Result<(), Box<dyn Error>>
where
    F: Fn() -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if predicate() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("subprocess condition was not observed".into());
        }
        tokio::task::yield_now().await;
    }
}
