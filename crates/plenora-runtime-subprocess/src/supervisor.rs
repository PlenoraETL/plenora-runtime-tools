use std::{
    future::Future,
    io,
    process::{ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::{
    process::{Child, Command},
    sync::Semaphore,
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};

use crate::{
    CapturedOutput, ProcessTreeMode, SubprocessError, SubprocessErrorKind, SubprocessSpec,
    SubprocessSupervisorConfig,
    output::{SharedCapture, drain_bounded},
};

/// Phase at which cancellation prevented or stopped a child.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubprocessCancellationPhase {
    /// Cancellation won while waiting for bounded capacity; no child was created.
    Queued,
    /// Cancellation stopped a running child.
    Running,
}

/// Stable reason one supervised invocation ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubprocessTermination {
    /// The child exited on its own, successfully or unsuccessfully.
    Exited,
    /// The execution deadline expired and the child was terminated.
    TimedOut,
    /// Caller cancellation prevented or stopped execution.
    Cancelled(SubprocessCancellationPhase),
    /// Linux RSS exceeded the configured limit.
    ResidentMemoryLimitExceeded {
        /// Last RSS sample observed before termination.
        observed_bytes: u64,
        /// Configured RSS limit.
        limit_bytes: u64,
    },
}

/// Payload-safe report for one invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubprocessReport {
    /// Stable termination classification.
    pub termination: SubprocessTermination,
    /// Platform exit code, when the operating system exposes one.
    pub exit_code: Option<i32>,
    /// Bounded stdout capture.
    pub stdout: CapturedOutput,
    /// Bounded stderr capture.
    pub stderr: CapturedOutput,
    /// Wall-clock time including bounded capacity waiting.
    pub elapsed: Duration,
}

impl SubprocessReport {
    /// Returns true only for a normal zero-code exit.
    #[must_use]
    pub fn success(&self) -> bool {
        matches!(self.termination, SubprocessTermination::Exited) && self.exit_code == Some(0)
    }

    fn cancelled_while_queued(elapsed: Duration) -> Self {
        Self {
            termination: SubprocessTermination::Cancelled(SubprocessCancellationPhase::Queued),
            exit_code: None,
            stdout: CapturedOutput::empty(),
            stderr: CapturedOutput::empty(),
            elapsed,
        }
    }
}

/// Current bounded capacity and cumulative lifecycle counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubprocessSnapshot {
    /// Maximum concurrent child processes.
    pub capacity: usize,
    /// Currently occupied process slots.
    pub in_flight: usize,
    /// Immediately available slots.
    pub available: usize,
    /// Successfully spawned children.
    pub started: u64,
    /// Reaped children, including cancelled and timed-out children.
    pub completed: u64,
    /// Spawn failures.
    pub spawn_failures: u64,
    /// Running or queued cancellations.
    pub cancellations: u64,
    /// Execution timeouts.
    pub timeouts: u64,
    /// RSS-enforced terminations.
    pub memory_terminations: u64,
}

#[derive(Default)]
struct Counters {
    started: AtomicU64,
    completed: AtomicU64,
    spawn_failures: AtomicU64,
    cancellations: AtomicU64,
    timeouts: AtomicU64,
    memory_terminations: AtomicU64,
}

/// Cloneable bounded supervisor. It owns no application protocol and never invokes a shell.
#[derive(Clone)]
pub struct SubprocessSupervisor {
    config: SubprocessSupervisorConfig,
    permits: Arc<Semaphore>,
    counters: Arc<Counters>,
}

impl SubprocessSupervisor {
    /// Creates a supervisor with the validated configuration.
    #[must_use]
    pub fn new(config: SubprocessSupervisorConfig) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(config.max_concurrent())),
            config,
            counters: Arc::new(Counters::default()),
        }
    }

    /// Returns validated supervisor limits.
    #[must_use]
    pub const fn config(&self) -> SubprocessSupervisorConfig {
        self.config
    }

    /// Returns a bounded operational snapshot.
    #[must_use]
    pub fn snapshot(&self) -> SubprocessSnapshot {
        let available = self.permits.available_permits();
        SubprocessSnapshot {
            capacity: self.config.max_concurrent(),
            in_flight: self.config.max_concurrent().saturating_sub(available),
            available,
            started: self.counters.started.load(Ordering::Relaxed),
            completed: self.counters.completed.load(Ordering::Relaxed),
            spawn_failures: self.counters.spawn_failures.load(Ordering::Relaxed),
            cancellations: self.counters.cancellations.load(Ordering::Relaxed),
            timeouts: self.counters.timeouts.load(Ordering::Relaxed),
            memory_terminations: self.counters.memory_terminations.load(Ordering::Relaxed),
        }
    }

    /// Runs a child while observing a caller-owned cancellation future.
    ///
    /// The future may be `WorkerContext::cancelled()`, a shutdown signal, or any application-owned
    /// cancellation source. Arguments are passed directly to the executable and never through a
    /// command shell.
    ///
    /// # Errors
    ///
    /// Returns a redaction-safe infrastructure error if spawning, waiting, RSS sampling, or
    /// termination fails. A nonzero child exit is a successful report, not an infrastructure error.
    #[allow(
        clippy::too_many_lines,
        reason = "process admission, cancellation, timeout, RSS enforcement, and pipe ownership must remain in one cancellation-safe lifecycle"
    )]
    pub async fn run<F>(
        &self,
        spec: &SubprocessSpec,
        cancellation: F,
    ) -> Result<SubprocessReport, SubprocessError>
    where
        F: Future<Output = ()>,
    {
        let started_at = Instant::now();
        tokio::pin!(cancellation);
        let permit = tokio::select! {
            biased;
            () = &mut cancellation => {
                self.counters.cancellations.fetch_add(1, Ordering::Relaxed);
                return Ok(SubprocessReport::cancelled_while_queued(started_at.elapsed()));
            }
            permit = Arc::clone(&self.permits).acquire_owned() => permit.map_err(|error| {
                SubprocessError::new(
                    SubprocessErrorKind::Spawn,
                    "subprocess admission control is closed",
                    io::Error::other(error.to_string()),
                )
            })?,
        };

        let mut command = build_command(spec, self.config.process_tree_mode());
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                self.counters.spawn_failures.fetch_add(1, Ordering::Relaxed);
                return Err(SubprocessError::new(
                    SubprocessErrorKind::Spawn,
                    "subprocess could not be started",
                    error,
                ));
            }
        };
        self.counters.started.fetch_add(1, Ordering::Relaxed);
        let pid = child.id().ok_or_else(|| {
            SubprocessError::new(
                SubprocessErrorKind::Spawn,
                "subprocess identifier is unavailable",
                io::Error::other("child process did not expose an identifier"),
            )
        })?;
        let stdout = SharedCapture::new(self.config.max_stdout_bytes());
        let stderr = SharedCapture::new(self.config.max_stderr_bytes());
        let mut stdout_task = spawn_capture(child.stdout.take(), Arc::clone(&stdout));
        let mut stderr_task = spawn_capture(child.stderr.take(), Arc::clone(&stderr));
        let deadline = sleep(self.config.execution_timeout());
        tokio::pin!(deadline);
        let memory_tick = sleep(self.config.memory_sample_interval());
        tokio::pin!(memory_tick);

        let (termination, status) = loop {
            tokio::select! {
                biased;
                () = &mut cancellation => {
                    self.counters.cancellations.fetch_add(1, Ordering::Relaxed);
                    let status = terminate_and_reap(&mut child, pid, self.config).await?;
                    break (
                        SubprocessTermination::Cancelled(SubprocessCancellationPhase::Running),
                        status,
                    );
                }
                () = &mut deadline => {
                    self.counters.timeouts.fetch_add(1, Ordering::Relaxed);
                    let status = terminate_and_reap(&mut child, pid, self.config).await?;
                    break (SubprocessTermination::TimedOut, status);
                }
                result = child.wait() => {
                    let status = result.map_err(|error| SubprocessError::new(
                        SubprocessErrorKind::Wait,
                        "subprocess wait failed",
                        error,
                    ))?;
                    break (SubprocessTermination::Exited, status);
                }
                () = &mut memory_tick, if self.config.resident_memory_limit_bytes().is_some() => {
                    let limit = self.config.resident_memory_limit_bytes().unwrap_or_default();
                    let observed = resident_memory_bytes(pid).await.map_err(|error| {
                        SubprocessError::new(
                            SubprocessErrorKind::MemorySample,
                            "subprocess RSS sampling failed while enforcement was active",
                            error,
                        )
                    })?;
                    if observed > limit {
                        self.counters.memory_terminations.fetch_add(1, Ordering::Relaxed);
                        let status = terminate_and_reap(&mut child, pid, self.config).await?;
                        break (
                            SubprocessTermination::ResidentMemoryLimitExceeded {
                                observed_bytes: observed,
                                limit_bytes: limit,
                            },
                            status,
                        );
                    }
                    memory_tick.as_mut().reset(Instant::now() + self.config.memory_sample_interval());
                }
            }
        };

        drain_capture_tasks(
            &mut stdout_task,
            &mut stderr_task,
            self.config.output_drain_timeout(),
        )
        .await;
        drop(permit);
        self.counters.completed.fetch_add(1, Ordering::Relaxed);
        Ok(SubprocessReport {
            termination,
            exit_code: status.code(),
            stdout: stdout.snapshot(),
            stderr: stderr.snapshot(),
            elapsed: started_at.elapsed(),
        })
    }

    /// Runs a child without an external cancellation source.
    ///
    /// # Errors
    ///
    /// Returns the same infrastructure errors as [`Self::run`].
    pub async fn run_to_completion(
        &self,
        spec: &SubprocessSpec,
    ) -> Result<SubprocessReport, SubprocessError> {
        self.run(spec, std::future::pending()).await
    }
}

fn build_command(spec: &SubprocessSpec, tree_mode: ProcessTreeMode) -> Command {
    let mut command = Command::new(spec.executable());
    command
        .args(spec.arguments())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if spec.clear_environment() {
        command.env_clear();
    }
    command.envs(spec.environment());
    if let Some(directory) = spec.current_directory() {
        command.current_dir(directory);
    }
    configure_process_tree(&mut command, tree_mode);
    command
}

#[cfg(unix)]
fn configure_process_tree(command: &mut Command, mode: ProcessTreeMode) {
    if mode == ProcessTreeMode::IsolatedTree {
        command.process_group(0);
    }
}

#[cfg(windows)]
fn configure_process_tree(command: &mut Command, mode: ProcessTreeMode) {
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    if mode == ProcessTreeMode::IsolatedTree {
        command.creation_flags(CREATE_NEW_PROCESS_GROUP);
    }
}

#[cfg(not(any(unix, windows)))]
fn configure_process_tree(_command: &mut Command, _mode: ProcessTreeMode) {}

fn spawn_capture<R>(reader: Option<R>, capture: Arc<SharedCapture>) -> JoinHandle<()>
where
    R: tokio::io::AsyncRead + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        if let Some(reader) = reader {
            drain_bounded(reader, capture).await;
        }
    })
}

async fn drain_capture_tasks(
    stdout: &mut JoinHandle<()>,
    stderr: &mut JoinHandle<()>,
    drain_timeout: Duration,
) {
    let drained = timeout(drain_timeout, async {
        let _stdout = (&mut *stdout).await;
        let _stderr = (&mut *stderr).await;
    })
    .await;
    if drained.is_err() {
        stdout.abort();
        stderr.abort();
        let _stdout = stdout.await;
        let _stderr = stderr.await;
    }
}

async fn terminate_and_reap(
    child: &mut Child,
    pid: u32,
    config: SubprocessSupervisorConfig,
) -> Result<ExitStatus, SubprocessError> {
    if let Some(status) = child.try_wait().map_err(|error| {
        SubprocessError::new(
            SubprocessErrorKind::Termination,
            "subprocess status could not be inspected during termination",
            error,
        )
    })? {
        return Ok(status);
    }

    let graceful_sent = signal_process(pid, config.process_tree_mode(), false)
        .await
        .is_ok();
    if graceful_sent {
        match timeout(config.graceful_termination(), child.wait()).await {
            Ok(Ok(status)) => return Ok(status),
            Ok(Err(error)) => {
                return Err(SubprocessError::new(
                    SubprocessErrorKind::Termination,
                    "subprocess wait failed during graceful termination",
                    error,
                ));
            }
            Err(_elapsed) => {}
        }
    }

    let _forced_tree = signal_process(pid, config.process_tree_mode(), true).await;
    child.start_kill().map_err(|error| {
        SubprocessError::new(
            SubprocessErrorKind::Termination,
            "subprocess hard termination could not be requested",
            error,
        )
    })?;
    timeout(config.hard_kill_timeout(), child.wait())
        .await
        .map_err(|error| {
            SubprocessError::new(
                SubprocessErrorKind::Termination,
                "subprocess did not exit before the hard termination deadline",
                io::Error::new(io::ErrorKind::TimedOut, error),
            )
        })?
        .map_err(|error| {
            SubprocessError::new(
                SubprocessErrorKind::Termination,
                "subprocess wait failed after hard termination",
                error,
            )
        })
}

#[cfg(unix)]
async fn signal_process(pid: u32, mode: ProcessTreeMode, force: bool) -> io::Result<()> {
    let target = if mode == ProcessTreeMode::IsolatedTree {
        let child_group = process_group_id(pid).await?;
        let supervisor_group = process_group_id(std::process::id()).await?;
        isolated_process_group_target(pid, child_group, supervisor_group)?
    } else {
        pid.to_string()
    };
    let mut command = Command::new("/bin/kill");
    command.arg(if force { "-KILL" } else { "-TERM" });
    if mode == ProcessTreeMode::IsolatedTree {
        command.arg("--");
    }
    let status = command
        .arg(target)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other("process signal command failed"))
    }
}

#[cfg(unix)]
async fn process_group_id(pid: u32) -> io::Result<u32> {
    let output = Command::new("/bin/ps")
        .arg("-o")
        .arg("pgid=")
        .arg("-p")
        .arg(pid.to_string())
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .await?;
    if !output.status.success() {
        return Err(io::Error::other("process group lookup failed"));
    }
    let value = std::str::from_utf8(&output.stdout)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    value
        .trim()
        .parse()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[cfg(unix)]
fn isolated_process_group_target(
    pid: u32,
    child_group: u32,
    supervisor_group: u32,
) -> io::Result<String> {
    if child_group != pid || child_group == supervisor_group {
        return Err(io::Error::other(
            "subprocess process group is not safely isolated",
        ));
    }
    Ok(format!("-{child_group}"))
}

#[cfg(windows)]
async fn signal_process(pid: u32, mode: ProcessTreeMode, force: bool) -> io::Result<()> {
    let mut command = Command::new("taskkill.exe");
    command.arg("/PID").arg(pid.to_string());
    if mode == ProcessTreeMode::IsolatedTree {
        command.arg("/T");
    }
    if force {
        command.arg("/F");
    }
    let status = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other("process termination command failed"))
    }
}

#[cfg(not(any(unix, windows)))]
async fn signal_process(_pid: u32, _mode: ProcessTreeMode, _force: bool) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "process signalling is unsupported on this platform",
    ))
}

#[cfg(target_os = "linux")]
async fn resident_memory_bytes(pid: u32) -> io::Result<u64> {
    let status = tokio::fs::read_to_string(format!("/proc/{pid}/status")).await?;
    let line = status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "VmRSS is absent"))?;
    let kibibytes = line
        .split_ascii_whitespace()
        .nth(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "VmRSS value is absent"))?
        .parse::<u64>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    kibibytes
        .checked_mul(1024)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "VmRSS byte count overflowed"))
}

#[cfg(not(target_os = "linux"))]
// Keep every platform implementation compatible with the shared awaited call site.
#[allow(clippy::unused_async)]
async fn resident_memory_bytes(_pid: u32) -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "RSS sampling is unsupported on this platform",
    ))
}

#[cfg(all(test, unix))]
mod tests {
    use super::isolated_process_group_target;

    #[test]
    fn isolated_group_target_must_be_distinct_and_led_by_the_child() {
        assert!(matches!(
            isolated_process_group_target(42, 42, 7).as_deref(),
            Ok("-42")
        ));
        assert!(isolated_process_group_target(42, 7, 7).is_err());
        assert!(isolated_process_group_target(42, 42, 42).is_err());
    }
}
