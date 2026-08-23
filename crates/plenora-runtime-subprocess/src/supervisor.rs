use std::{
    future::Future,
    io,
    pin::Pin,
    process::{ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
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
    /// Whether process-tree containment could not be established for this invocation.
    ///
    /// True means either that a forced tree signal was refused, or that the leader was already
    /// gone before any tree signal could be sent, so descendants may have survived. False only
    /// means no such failure was observed: it is not proof that no descendant outlived the child,
    /// because a leader that exits on its own or within the graceful window is reaped before any
    /// forced tree signal is required. It is always false for [`ProcessTreeMode::DirectChild`],
    /// which makes no tree claim.
    pub tree_termination_degraded: bool,
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
            tree_termination_degraded: false,
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
    /// Forced process-tree terminations that could not be proven delivered.
    ///
    /// This counts both a forced tree signal the platform refused and a cleanup that found the
    /// leader already gone before any tree signal could be sent. A nonzero value means containment
    /// degraded to the direct child, so descendants may have survived.
    pub tree_termination_failures: u64,
}

#[derive(Default)]
struct Counters {
    started: AtomicU64,
    completed: AtomicU64,
    spawn_failures: AtomicU64,
    cancellations: AtomicU64,
    timeouts: AtomicU64,
    memory_terminations: AtomicU64,
    tree_termination_failures: AtomicU64,
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
            tree_termination_failures: self
                .counters
                .tree_termination_failures
                .load(Ordering::Relaxed),
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
        let pid = child.id();
        let stdout = SharedCapture::new(self.config.max_stdout_bytes());
        let stderr = SharedCapture::new(self.config.max_stderr_bytes());
        // The guard aborts both pipe readers if this future is dropped before a terminal result.
        let mut capture = CaptureTasks {
            stdout: spawn_capture(child.stdout.take(), Arc::clone(&stdout)),
            stderr: spawn_capture(child.stderr.take(), Arc::clone(&stderr)),
        };
        // Declared after `child` so it runs before the child is dropped and reaped: while the
        // leader is still unreaped its process-group identifier cannot have been recycled, which
        // is the only moment a group signal is safe on an abandoned execution.
        // A wait that fails leaves the leader in an unknown state, so the identifier stops being
        // pinned and no group signal may target it again.
        let leader_pinned = Arc::new(AtomicBool::new(true));
        let mut tree_guard = AbandonedTreeGuard::new(
            pid,
            self.config.process_tree_mode(),
            Arc::clone(&self.counters),
            Arc::clone(&leader_pinned),
        );

        let mut tree_degraded = false;
        let supervised = self
            .supervise(
                &mut child,
                pid,
                cancellation.as_mut(),
                &mut tree_degraded,
                &leader_pinned,
            )
            .await;
        let (termination, status) = match supervised {
            Ok(outcome) => {
                // The leader was reaped inside supervision and no await separates that reap from
                // this disarm, so the guard can never fire on a released identifier. The reap is
                // also counted here, before the next cancellation point.
                tree_guard.disarm();
                self.counters.completed.fetch_add(1, Ordering::Relaxed);
                outcome
            }
            Err(error) => {
                // Supervision returns its failures with the leader still owned, so the guard stays
                // armed across this cleanup: abandoning the future here must still terminate the
                // tree.
                let cleanup = terminate_and_reap(
                    &mut child,
                    pid,
                    self.config,
                    &self.counters,
                    &mut tree_degraded,
                    &leader_pinned,
                )
                .await;
                let reaped = cleanup.is_ok();
                if reaped {
                    // Only a confirmed reap releases the guard and counts the child; a failed
                    // cleanup keeps the guard armed so the tree is still signalled while the
                    // identifier remains pinned.
                    tree_guard.disarm();
                    self.counters.completed.fetch_add(1, Ordering::Relaxed);
                }
                capture.drain(self.config.output_drain_timeout()).await;
                drop(permit);
                // The original supervision failure stays the reported cause, but the caller must
                // be able to see when either direct reaping or tree termination is unproven.
                return Err(if reaped && !tree_degraded {
                    error
                } else {
                    error.with_containment_failure()
                });
            }
        };

        capture.drain(self.config.output_drain_timeout()).await;
        drop(permit);
        Ok(SubprocessReport {
            termination,
            exit_code: status.code(),
            stdout: stdout.snapshot(),
            stderr: stderr.snapshot(),
            elapsed: started_at.elapsed(),
            tree_termination_degraded: tree_degraded,
        })
    }

    /// Drives one spawned child until a terminal classification or an infrastructure failure.
    ///
    /// Every failure returned here leaves the child owned by the caller, which is responsible for
    /// terminating and reaping it.
    async fn supervise<F>(
        &self,
        child: &mut Child,
        pid: Option<u32>,
        mut cancellation: Pin<&mut F>,
        tree_degraded: &mut bool,
        leader_pinned: &AtomicBool,
    ) -> Result<(SubprocessTermination, ExitStatus), SubprocessError>
    where
        F: Future<Output = ()>,
    {
        let Some(pid) = pid else {
            return Err(SubprocessError::new(
                SubprocessErrorKind::Spawn,
                "subprocess identifier is unavailable",
                io::Error::other("child process did not expose an identifier"),
            ));
        };
        let deadline = sleep(self.config.execution_timeout());
        tokio::pin!(deadline);
        let memory_tick = sleep(self.config.memory_sample_interval());
        tokio::pin!(memory_tick);

        loop {
            tokio::select! {
                biased;
                () = cancellation.as_mut() => {
                    self.counters.cancellations.fetch_add(1, Ordering::Relaxed);
                    let status = terminate_and_reap(child, Some(pid), self.config, &self.counters, tree_degraded, leader_pinned).await?;
                    return Ok((
                        SubprocessTermination::Cancelled(SubprocessCancellationPhase::Running),
                        status,
                    ));
                }
                () = &mut deadline => {
                    self.counters.timeouts.fetch_add(1, Ordering::Relaxed);
                    let status = terminate_and_reap(child, Some(pid), self.config, &self.counters, tree_degraded, leader_pinned).await?;
                    return Ok((SubprocessTermination::TimedOut, status));
                }
                result = child.wait() => {
                    let status = result.map_err(|error| {
                        leader_pinned.store(false, Ordering::Release);
                        SubprocessError::new(
                            SubprocessErrorKind::Wait,
                            "subprocess wait failed",
                            error,
                        )
                    })?;
                    return Ok((SubprocessTermination::Exited, status));
                }
                () = &mut memory_tick, if self.config.resident_memory_limit_bytes().is_some() => {
                    let limit = self.config.resident_memory_limit_bytes().unwrap_or_default();
                    let observed = match resident_memory_bytes(pid).await {
                        Ok(observed) => observed,
                        Err(error) => {
                            // Sampling reads per-process state that disappears the moment the
                            // child exits, so an ordinary exit must not be reported as a sampling
                            // failure.
                            if let Some(status) = poll_reaped(child, leader_pinned) {
                                return Ok((SubprocessTermination::Exited, status));
                            }
                            return Err(SubprocessError::new(
                                SubprocessErrorKind::MemorySample,
                                "subprocess RSS sampling failed while enforcement was active",
                                error,
                            ));
                        }
                    };
                    if observed > limit {
                        self.counters.memory_terminations.fetch_add(1, Ordering::Relaxed);
                        let status = terminate_and_reap(child, Some(pid), self.config, &self.counters, tree_degraded, leader_pinned).await?;
                        return Ok((
                            SubprocessTermination::ResidentMemoryLimitExceeded {
                                observed_bytes: observed,
                                limit_bytes: limit,
                            },
                            status,
                        ));
                    }
                    memory_tick.as_mut().reset(Instant::now() + self.config.memory_sample_interval());
                }
            }
        }
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

/// Pipe readers owned by one invocation; dropping the guard releases both tasks.
struct CaptureTasks {
    stdout: JoinHandle<()>,
    stderr: JoinHandle<()>,
}

impl CaptureTasks {
    async fn drain(&mut self, drain_timeout: Duration) {
        let drained = timeout(drain_timeout, async {
            let _stdout = (&mut self.stdout).await;
            let _stderr = (&mut self.stderr).await;
        })
        .await;
        if drained.is_err() {
            self.stdout.abort();
            self.stderr.abort();
            let _stdout = (&mut self.stdout).await;
            let _stderr = (&mut self.stderr).await;
        }
    }
}

impl Drop for CaptureTasks {
    fn drop(&mut self) {
        // Aborting an already finished task is a no-op, so the normal drained path is unaffected
        // while a dropped execution future can no longer leave detached readers behind.
        self.stdout.abort();
        self.stderr.abort();
    }
}

async fn terminate_and_reap(
    child: &mut Child,
    pid: Option<u32>,
    config: SubprocessSupervisorConfig,
    counters: &Counters,
    tree_degraded: &mut bool,
    leader_pinned: &AtomicBool,
) -> Result<ExitStatus, SubprocessError> {
    if let Some(status) = poll_reaped(child, leader_pinned) {
        // The leader vanished before any tree signal could be delivered, so nothing proves its
        // descendants ended with it.
        if config.process_tree_mode() == ProcessTreeMode::IsolatedTree {
            *tree_degraded = true;
            counters
                .tree_termination_failures
                .fetch_add(1, Ordering::Relaxed);
        }
        return Ok(status);
    }

    let graceful_sent = match pinned_target(pid, leader_pinned) {
        Some(pid) => signal_tree(pid, config, TreeSignal::Terminate)
            .await
            .is_ok(),
        None => false,
    };
    if graceful_sent {
        // Only a confirmed reap ends termination early; a failed wait unpins the identifier and
        // falls through to the direct-child path instead of abandoning the child.
        match timeout(config.graceful_termination(), child.wait()).await {
            Ok(Ok(status)) => return Ok(status),
            Ok(Err(_error)) => leader_pinned.store(false, Ordering::Release),
            Err(_elapsed) => {}
        }
    }

    let forced_tree_failed = match pinned_target(pid, leader_pinned) {
        Some(pid) => signal_tree(pid, config, TreeSignal::Kill).await.is_err(),
        None => config.process_tree_mode() == ProcessTreeMode::IsolatedTree,
    };
    if forced_tree_failed && config.process_tree_mode() == ProcessTreeMode::IsolatedTree {
        // Containment degraded to the direct child, so the degradation must stay observable per
        // invocation and in the aggregate instead of being silently discarded before the
        // fallback kill.
        *tree_degraded = true;
        counters
            .tree_termination_failures
            .fetch_add(1, Ordering::Relaxed);
    }
    // The forced tree signal reaches the leader too, so it may already have exited. Reaping it
    // here keeps a successful cleanup from being reported as a termination failure.
    if let Some(status) = poll_reaped(child, leader_pinned) {
        return Ok(status);
    }
    if pinned_target(pid, leader_pinned).is_some()
        && let Err(error) = child.start_kill()
    {
        // A kill request can also fail because the child exited in the meantime.
        if let Some(status) = poll_reaped(child, leader_pinned) {
            return Ok(status);
        }
        return Err(SubprocessError::new(
            SubprocessErrorKind::Termination,
            "subprocess hard termination could not be requested",
            error,
        ));
    }
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
            leader_pinned.store(false, Ordering::Release);
            SubprocessError::new(
                SubprocessErrorKind::Termination,
                "subprocess wait failed after hard termination",
                error,
            )
        })
}

/// Budget for waiting on the blocking tree signal issued from the abandonment drop path.
///
/// `Drop` cannot await, so the helper is bounded by a deliberately small budget rather than the
/// configured hard-kill timeout: abandoning one execution must never stall an executor thread for
/// a termination window sized for cooperative shutdown.
///
/// This bounds the waiting only. The drop path launches exactly one helper and every wait it
/// performs has a deadline, but creating that helper is a synchronous operating-system call the
/// crate cannot deadline without a native signalling primitive it deliberately avoids, so the drop
/// path as a whole has no hard time bound.
const ABANDONED_TREE_SIGNAL_BUDGET: Duration = Duration::from_millis(250);

/// Terminates the isolated process tree when an execution future is abandoned.
///
/// The guard is declared after the child and disarmed only once the leader has been reaped, so it
/// can fire exclusively while the leader is unreaped and its process-group identifier therefore
/// cannot have been recycled.
struct AbandonedTreeGuard {
    pid: Option<u32>,
    mode: ProcessTreeMode,
    armed: bool,
    counters: Arc<Counters>,
    leader_pinned: Arc<AtomicBool>,
}

impl AbandonedTreeGuard {
    fn new(
        pid: Option<u32>,
        mode: ProcessTreeMode,
        counters: Arc<Counters>,
        leader_pinned: Arc<AtomicBool>,
    ) -> Self {
        Self {
            pid,
            mode,
            armed: matches!(mode, ProcessTreeMode::IsolatedTree),
            counters,
            leader_pinned,
        }
    }

    const fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for AbandonedTreeGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let delivered = pinned_target(self.pid, &self.leader_pinned)
            .is_some_and(|pid| kill_abandoned_tree(pid, self.mode, ABANDONED_TREE_SIGNAL_BUDGET));
        if !delivered {
            // An undelivered abandonment signal degrades containment exactly like a failed forced
            // tree signal, so it must stay observable instead of being discarded.
            self.counters
                .tree_termination_failures
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Force-kills the abandoned tree and reports whether the signal was delivered.
///
/// The child was spawned with its own process group, so the group identifier equals the pinned
/// leader identifier and no lookup helper is required. That keeps this blocking path to one helper
/// process whose every wait is deadline-bounded, though its creation is not.
#[cfg(unix)]
fn kill_abandoned_tree(pid: u32, mode: ProcessTreeMode, budget: Duration) -> bool {
    let target = if mode == ProcessTreeMode::IsolatedTree {
        format!("-{pid}")
    } else {
        pid.to_string()
    };
    let mut command = std::process::Command::new("/bin/kill");
    command.arg("-KILL");
    if mode == ProcessTreeMode::IsolatedTree {
        command.arg("--");
    }
    let spawned = command
        .arg(target)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
    match spawned {
        Ok(mut helper) => wait_bounded(&mut helper, budget),
        Err(_error) => false,
    }
}

#[cfg(windows)]
fn kill_abandoned_tree(pid: u32, mode: ProcessTreeMode, budget: Duration) -> bool {
    let mut command = std::process::Command::new("taskkill.exe");
    command.arg("/PID").arg(pid.to_string());
    if mode == ProcessTreeMode::IsolatedTree {
        command.arg("/T");
    }
    let spawned = command
        .arg("/F")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
    match spawned {
        Ok(mut helper) => wait_bounded(&mut helper, budget),
        Err(_error) => false,
    }
}

#[cfg(not(any(unix, windows)))]
const fn kill_abandoned_tree(_pid: u32, _mode: ProcessTreeMode, _budget: Duration) -> bool {
    false
}

/// Extra window allowed for reaping a helper that had to be killed.
///
/// Together with [`ABANDONED_TREE_SIGNAL_BUDGET`] this bounds every wait the drop path performs.
#[cfg(any(unix, windows))]
const ABANDONED_TREE_REAP_BUDGET: Duration = Duration::from_millis(50);

/// Waits for a platform helper within an explicit budget, killing it when the budget elapses.
///
/// Every wait in this function is deadline-bounded. A helper that is still not reapable after the
/// kill window has its handle abandoned rather than blocking the dropping thread any further; if
/// the kill also failed, that helper may briefly outlive this call or be reparented.
#[cfg(any(unix, windows))]
fn wait_bounded(helper: &mut std::process::Child, budget: Duration) -> bool {
    if let HelperWait::Exited(delivered) = poll_until(helper, budget) {
        return delivered;
    }
    // Both a timeout and an inspection failure leave the helper possibly running, so both take the
    // kill path rather than abandoning it untouched.
    let _killed = helper.kill();
    let _reaped = poll_until(helper, ABANDONED_TREE_REAP_BUDGET);
    false
}

/// Outcome of waiting for a platform helper within a deadline.
#[cfg(any(unix, windows))]
enum HelperWait {
    /// The helper exited, carrying whether it reported success.
    Exited(bool),
    /// The helper is still running, or its state could no longer be inspected.
    Unresolved,
}

/// Polls a helper until it exits or the deadline elapses.
#[cfg(any(unix, windows))]
fn poll_until(helper: &mut std::process::Child, budget: Duration) -> HelperWait {
    let deadline = std::time::Instant::now() + budget;
    loop {
        match helper.try_wait() {
            Ok(Some(status)) => return HelperWait::Exited(status.success()),
            Ok(None) => {}
            Err(_error) => return HelperWait::Unresolved,
        }
        if std::time::Instant::now() >= deadline {
            return HelperWait::Unresolved;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Returns the child identifier only while it is still guaranteed to be unreaped.
///
/// An unreaped child keeps its identifier reserved on both supported platforms, for different
/// reasons: on Unix the exited child remains a zombie until it is reaped, and on Windows the
/// identifier is not reused while a handle to the process object is open, which the owning `Child`
/// holds until it is dropped. Once a wait has failed that reservation can no longer be assumed, so
/// none of the signals this crate composes itself may target the identifier again.
///
/// Tokio's own `kill_on_drop` handling stays outside this gate: it is scoped to the `Child` handle
/// it was configured on, and the crate keeps it because it is the only thing that stops an
/// abandoned direct child from leaking.
fn pinned_target(pid: Option<u32>, leader_pinned: &AtomicBool) -> Option<u32> {
    if leader_pinned.load(Ordering::Acquire) {
        pid
    } else {
        None
    }
}

/// Polls for a reaped child, releasing the identifier reservation on any inspection failure.
///
/// Every `try_wait` in this module goes through here so no call site can observe a failure without
/// also recording that the identifier can no longer be signalled.
fn poll_reaped(child: &mut Child, leader_pinned: &AtomicBool) -> Option<ExitStatus> {
    match child.try_wait() {
        Ok(status) => status,
        Err(_error) => {
            leader_pinned.store(false, Ordering::Release);
            None
        }
    }
}

/// Signal delivered to a child or to its isolated process tree.
#[derive(Clone, Copy, Eq, PartialEq)]
enum TreeSignal {
    /// Cooperative termination request.
    Terminate,
    /// Unconditional termination.
    Kill,
}

/// Delivers one platform tree signal under an explicit deadline.
///
/// The platform helpers are external processes, so an unresponsive helper must not extend
/// termination beyond the configured hard-kill budget.
async fn signal_tree(
    pid: u32,
    config: SubprocessSupervisorConfig,
    signal: TreeSignal,
) -> io::Result<()> {
    timeout(
        config.hard_kill_timeout(),
        signal_process(pid, config.process_tree_mode(), signal),
    )
    .await
    .map_err(|error| io::Error::new(io::ErrorKind::TimedOut, error))?
}

#[cfg(unix)]
async fn signal_process(pid: u32, mode: ProcessTreeMode, signal: TreeSignal) -> io::Result<()> {
    let target = if mode == ProcessTreeMode::IsolatedTree {
        let child_group = process_group_id(pid).await?;
        let supervisor_group = process_group_id(std::process::id()).await?;
        isolated_process_group_target(pid, child_group, supervisor_group)?
    } else {
        pid.to_string()
    };
    let mut command = Command::new("/bin/kill");
    command.arg(match signal {
        TreeSignal::Terminate => "-TERM",
        TreeSignal::Kill => "-KILL",
    });
    if mode == ProcessTreeMode::IsolatedTree {
        command.arg("--");
    }
    let status = command
        .arg(target)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
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
        .kill_on_drop(true)
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
async fn signal_process(pid: u32, mode: ProcessTreeMode, signal: TreeSignal) -> io::Result<()> {
    let mut command = Command::new("taskkill.exe");
    command.arg("/PID").arg(pid.to_string());
    if mode == ProcessTreeMode::IsolatedTree {
        command.arg("/T");
    }
    if signal == TreeSignal::Kill {
        command.arg("/F");
    }
    let status = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .status()
        .await?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other("process termination command failed"))
    }
}

#[cfg(not(any(unix, windows)))]
async fn signal_process(_pid: u32, _mode: ProcessTreeMode, _signal: TreeSignal) -> io::Result<()> {
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
    fn isolated_group_target_must_be_distinct_and_led_by_the_child() -> std::io::Result<()> {
        if isolated_process_group_target(42, 42, 7)? != "-42" {
            return Err(std::io::Error::other("safe process group was rejected"));
        }
        if isolated_process_group_target(42, 7, 7).is_ok()
            || isolated_process_group_target(42, 42, 42).is_ok()
        {
            return Err(std::io::Error::other("unsafe process group was accepted"));
        }
        Ok(())
    }
}
