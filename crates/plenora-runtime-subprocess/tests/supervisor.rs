//! Cross-platform subprocess containment contracts.

use std::{env, error::Error, fs, path::PathBuf, process, sync::Arc, thread, time::Duration};

use plenora_runtime_subprocess::{
    MAX_ARGUMENT_BYTES, MAX_ARGUMENT_COUNT, MAX_CAPTURE_BYTES, MAX_CONCURRENT_SUBPROCESSES,
    MAX_ENVIRONMENT_BYTES, MAX_ENVIRONMENT_ENTRIES, ProcessTreeMode, SubprocessCancellationPhase,
    SubprocessConfigError, SubprocessErrorKind, SubprocessSpec, SubprocessSpecError,
    SubprocessSupervisor, SubprocessSupervisorConfig, SubprocessTermination,
};
use tokio::sync::Notify;

const FIXTURE_MODE: &str = "PLENORA_SUBPROCESS_FIXTURE_MODE";
const FIXTURE_PID_FILE: &str = "PLENORA_SUBPROCESS_FIXTURE_PID_FILE";

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
        Ok("sleep") => {
            if let Ok(path) = env::var(FIXTURE_PID_FILE) {
                let _written = std::fs::write(path, std::process::id().to_string());
            }
            thread::sleep(Duration::from_secs(30));
        }
        Ok("descendant") => {
            let executable = env::current_exe().unwrap_or_default();
            let spawned = std::process::Command::new(executable)
                .args([
                    "--exact",
                    "subprocess_fixture_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env(FIXTURE_MODE, "sleep")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn();
            if let Ok(child) = spawned {
                print!("descendant-pid={};", child.id());
            }
            let _flushed = std::io::Write::flush(&mut std::io::stdout());
            thread::sleep(Duration::from_secs(30));
        }
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

#[tokio::test]
async fn a_dropped_execution_future_releases_capacity_without_leaking_the_child()
-> Result<(), Box<dyn Error>> {
    let supervisor = SubprocessSupervisor::new(
        SubprocessSupervisorConfig::new(1, Duration::from_secs(30))?.with_termination_timeouts(
            Duration::from_millis(100),
            Duration::from_secs(2),
            Duration::from_secs(1),
        )?,
    );
    let pid_file = env::temp_dir().join(format!("plenora-subprocess-drop-{}.pid", process::id()));
    let _cleared = fs::remove_file(&pid_file);
    let spec = fixture_spec("sleep")?
        .with_environment(FIXTURE_PID_FILE, pid_file.to_string_lossy().into_owned())?;

    let mut run = Box::pin(supervisor.run_to_completion(&spec));
    let started = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                _terminal = &mut run => return false,
                () = tokio::time::sleep(Duration::from_millis(10)) => {
                    if pid_file.exists() {
                        return true;
                    }
                }
            }
        }
    })
    .await?;
    assert!(started);
    let child = fs::read_to_string(&pid_file)?.trim().parse::<u32>()?;
    drop(run);

    // Dropping the future must return the slot, terminate the child, and leave no pipe reader
    // behind, so the next invocation is admitted immediately.
    wait_until(|| supervisor.snapshot().available == 1).await?;
    if cfg!(target_os = "linux") {
        wait_until(|| !PathBuf::from(format!("/proc/{child}")).exists()).await?;
    }
    let recovered = supervisor
        .run_to_completion(&fixture_spec("success")?)
        .await?;
    assert!(recovered.success());
    assert_eq!(supervisor.snapshot().started, 2);
    let _cleared = fs::remove_file(&pid_file);
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn timeout_terminates_descendants_or_reports_degraded_containment()
-> Result<(), Box<dyn Error>> {
    let supervisor = SubprocessSupervisor::new(
        SubprocessSupervisorConfig::new(1, Duration::from_millis(500))?
            .with_output_limits(256, 256)?
            .with_termination_timeouts(
                Duration::from_millis(100),
                Duration::from_secs(2),
                Duration::from_secs(1),
            )?,
    );

    let report = supervisor
        .run_to_completion(&fixture_spec("descendant")?)
        .await?;
    assert_eq!(report.termination, SubprocessTermination::TimedOut);

    let descendant = descendant_pid(report.stdout.bytes())
        .ok_or("fixture did not report a descendant identifier")?;
    if supervisor.snapshot().tree_termination_failures == 0 {
        // Tree containment was delivered, so the descendant must not survive the parent.
        wait_until(|| !std::path::Path::new(&format!("/proc/{descendant}")).exists()).await?;
    }
    assert_eq!(supervisor.snapshot().available, 1);
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn abandoning_an_execution_force_kills_the_whole_isolated_tree() -> Result<(), Box<dyn Error>>
{
    let supervisor =
        SubprocessSupervisor::new(SubprocessSupervisorConfig::new(1, Duration::from_secs(30))?);
    let pid_file = env::temp_dir().join(format!("plenora-subprocess-tree-{}.pid", process::id()));
    let _cleared = fs::remove_file(&pid_file);
    // The leader and its descendant both ignore SIGTERM, so only the forced group kill issued
    // from the abandonment drop path can remove them.
    let spec = SubprocessSpec::new(PathBuf::from("/bin/sh"))?
        .with_argument("-c")?
        .with_argument(
            "sh -c \"trap '' TERM; sleep 30\" & echo -n \"$!\" > \"$PID_FILE\"; \
             trap '' TERM; sleep 30",
        )?
        .with_environment("PID_FILE", pid_file.to_string_lossy().into_owned())?;

    let mut run = Box::pin(supervisor.run_to_completion(&spec));
    let started = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                _terminal = &mut run => return false,
                () = tokio::time::sleep(Duration::from_millis(10)) => {
                    if pid_file.exists() {
                        return true;
                    }
                }
            }
        }
    })
    .await?;
    assert!(started);
    let descendant = fs::read_to_string(&pid_file)?.trim().parse::<u32>()?;
    drop(run);

    if supervisor.snapshot().tree_termination_failures == 0 {
        wait_until(|| !PathBuf::from(format!("/proc/{descendant}")).exists()).await?;
    }
    wait_until(|| supervisor.snapshot().available == 1).await?;
    let _cleared = fs::remove_file(&pid_file);
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_stubborn_tree_is_force_killed_while_the_leader_is_still_terminable()
-> Result<(), Box<dyn Error>> {
    let supervisor = SubprocessSupervisor::new(
        SubprocessSupervisorConfig::new(1, Duration::from_millis(500))?
            .with_output_limits(256, 256)?
            .with_termination_timeouts(
                Duration::from_millis(100),
                Duration::from_secs(2),
                Duration::from_secs(1),
            )?,
    );
    // Both the leader and its descendant ignore SIGTERM, so only the forced group kill that runs
    // while the leader is still unreaped can contain the tree.
    let spec = SubprocessSpec::new(PathBuf::from("/bin/sh"))?
        .with_argument("-c")?
        .with_argument(
            "sh -c \"trap '' TERM; sleep 30\" & echo \"descendant-pid=$!;\"; trap '' TERM; sleep 30",
        )?;

    let report = supervisor.run_to_completion(&spec).await?;
    assert_eq!(report.termination, SubprocessTermination::TimedOut);

    let descendant = descendant_pid(report.stdout.bytes())
        .ok_or("fixture did not report a descendant identifier")?;
    if report.tree_termination_degraded {
        assert!(supervisor.snapshot().tree_termination_failures > 0);
    } else {
        wait_until(|| !PathBuf::from(format!("/proc/{descendant}")).exists()).await?;
    }
    assert_eq!(supervisor.snapshot().available, 1);
    Ok(())
}

#[cfg(target_os = "linux")]
fn descendant_pid(stdout: &[u8]) -> Option<u32> {
    const MARKER: &str = "descendant-pid=";
    let text = std::str::from_utf8(stdout).ok()?;
    let start = text.find(MARKER)?.checked_add(MARKER.len())?;
    text.get(start..)?
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()
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
