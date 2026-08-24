use std::{
    future::Future,
    io,
    pin::Pin,
    process::{ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use process_wrap::tokio::{ChildWrapper, CommandWrap, KillOnDrop};
use tokio::{
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
        // Attaching the containment object reads the identifier of the process that was just
        // created, and the dependency panics rather than returning if that read yields nothing.
        // Only a child that has already been waited to completion reports no identifier, and
        // nothing between the spawn and this line waits, so the condition is unreachable here
        // rather than merely unlikely. It is recorded because it is the one failure in this
        // function that would not arrive as a `SubprocessError`.
        let child = match command.spawn() {
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
        // The child and its containment object are owned together, so abandoning this future
        // terminates the tree from `Drop` with one non-blocking system call and no platform branch.
        let mut child = SupervisedChild::new(
            child,
            self.config.process_tree_mode(),
            Arc::clone(&self.counters),
        );
        let pid = child.id();
        let stdout = SharedCapture::new(self.config.max_stdout_bytes());
        let stderr = SharedCapture::new(self.config.max_stderr_bytes());
        // The guard aborts both pipe readers if this future is dropped before a terminal result.
        let mut capture = CaptureTasks {
            stdout: spawn_capture(child.take_stdout(), Arc::clone(&stdout)),
            stderr: spawn_capture(child.take_stderr(), Arc::clone(&stderr)),
        };

        let mut tree_degraded = false;
        let supervised = self
            .supervise(&mut child, pid, cancellation.as_mut(), &mut tree_degraded)
            .await;
        let (termination, status) = match supervised {
            Ok(outcome) => {
                // The leader was reaped inside supervision and no await separates that reap from
                // this disarm, so the child can never signal a released identifier from `Drop`.
                // The reap is also counted here, before the next cancellation point.
                child.disarm();
                self.counters.completed.fetch_add(1, Ordering::Relaxed);
                outcome
            }
            Err(error) => {
                // Supervision returns its failures with the leader still owned, so the child stays
                // armed across this cleanup: abandoning the future here must still terminate the
                // tree.
                let cleanup = terminate_and_reap(&mut child, self.config, &mut tree_degraded).await;
                let reaped = cleanup.is_ok();
                if reaped {
                    // Only a confirmed reap disarms the child and counts it; a failed cleanup
                    // keeps it armed so the tree is still signalled while the identifier remains
                    // pinned.
                    child.disarm();
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
        child: &mut SupervisedChild,
        pid: Option<u32>,
        mut cancellation: Pin<&mut F>,
        tree_degraded: &mut bool,
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
                    let status = terminate_and_reap(child, self.config, tree_degraded).await?;
                    return Ok((
                        SubprocessTermination::Cancelled(SubprocessCancellationPhase::Running),
                        status,
                    ));
                }
                () = &mut deadline => {
                    self.counters.timeouts.fetch_add(1, Ordering::Relaxed);
                    let status = terminate_and_reap(child, self.config, tree_degraded).await?;
                    return Ok((SubprocessTermination::TimedOut, status));
                }
                result = child.wait_leader() => {
                    let status = result.map_err(|error| {
                        SubprocessError::new(
                            SubprocessErrorKind::Wait,
                            "subprocess wait failed",
                            error,
                        )
                    });
                    let status = match status {
                        Ok(status) => status,
                        Err(error) => {
                            // A failed wait leaves the leader in an unknown state, so the
                            // identifier stops being pinned and no signal may target it again.
                            child.unpin();
                            return Err(error);
                        }
                    };
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
                            if let Some(status) = child.poll_reaped() {
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
                        let status = terminate_and_reap(child, self.config, tree_degraded).await?;
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

fn build_command(spec: &SubprocessSpec, tree_mode: ProcessTreeMode) -> CommandWrap {
    let mut command = CommandWrap::with_new(spec.executable(), |command| {
        command
            .args(spec.arguments())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if spec.clear_environment() {
            command.env_clear();
        }
        command.envs(spec.environment());
        if let Some(directory) = spec.current_directory() {
            command.current_dir(directory);
        }
    });
    // Kill-on-drop is requested through the wrapper rather than on the command, because the
    // containment wrapper reads it back: on Windows it is what makes the job object terminate its
    // members when the last handle closes.
    command.wrap(KillOnDrop);
    if tree_mode == ProcessTreeMode::IsolatedTree {
        wrap_tree_containment(&mut command);
    }
    command
}

/// Attaches the platform containment object that makes descendants addressable as a unit.
///
/// This is the only place where the containment primitive differs by platform, and the difference
/// ends here: every later operation drives the same [`ChildWrapper`] interface, so the
/// supervisor has one termination path rather than one per platform.
#[cfg(unix)]
fn wrap_tree_containment(command: &mut CommandWrap) {
    command.wrap(process_wrap::tokio::ProcessGroup::leader());
}

#[cfg(windows)]
fn wrap_tree_containment(command: &mut CommandWrap) {
    command.wrap(process_wrap::tokio::JobObject);
}

#[cfg(not(any(unix, windows)))]
fn wrap_tree_containment(_command: &mut CommandWrap) {}

/// Whether this target has a containment object at all.
///
/// Where it does not, isolated mode cannot be honoured and every termination must be reported as
/// degraded instead of silently passing as a delivered tree signal.
const TREE_CONTAINMENT_SUPPORTED: bool = cfg!(any(unix, windows));

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

/// One spawned child together with the platform object that contains its descendants.
///
/// Owning both in one place is what removes the per-platform termination code from this crate.
/// `start_kill` is a process-group signal on Unix and a job termination on Windows; on both it is a
/// single non-blocking system call, so abandonment can be handled from `Drop` without creating a
/// helper process and without any wait that could stall the dropping thread.
struct SupervisedChild {
    child: Box<dyn ChildWrapper>,
    mode: ProcessTreeMode,
    counters: Arc<Counters>,
    /// True while the identifier is still guaranteed unreaped, so signalling it stays safe.
    ///
    /// An unreaped child keeps its identifier reserved on both supported platforms, for different
    /// reasons: on Unix the exited child remains a zombie until it is reaped, and on Windows the
    /// identifier is not reused while a handle to the process object is open, which the owning
    /// child holds until it is dropped. Once a wait has failed that reservation can no longer be
    /// assumed, so none of the signals this crate composes may target the identifier again.
    ///
    /// This gate gets applied to the Windows job object too, where it is not strictly required:
    /// a job handle stays valid regardless of what happened to the leader identifier. Gating it
    /// anyway keeps one contract on every platform rather than a stronger one where the primitive
    /// happens to be stronger. The cost is real and larger than a conservative report: on the rare
    /// path where a wait has failed, Windows could still have terminated the job promptly, and
    /// instead this type declines to, waits out the hard-kill budget holding its capacity slot,
    /// reports a termination failure, and never counts the invocation as completed. Only the leak
    /// is recovered, by the job's kill-on-close, when this type is dropped. That path is entered
    /// solely when the operating system fails a wait on our own child, and closing it would mean
    /// tracking identifier safety and containment ownership as two separate per-platform concepts,
    /// which is the platform-conditional termination logic this crate exists to have removed.
    pinned: bool,
    /// True while an abandoned execution must still terminate the tree from `Drop`.
    armed: bool,
}

impl SupervisedChild {
    fn new(child: Box<dyn ChildWrapper>, mode: ProcessTreeMode, counters: Arc<Counters>) -> Self {
        Self {
            child,
            mode,
            counters,
            pinned: true,
            armed: matches!(mode, ProcessTreeMode::IsolatedTree),
        }
    }

    fn id(&self) -> Option<u32> {
        self.child.id()
    }

    fn take_stdout(&mut self) -> Option<tokio::process::ChildStdout> {
        self.child.stdout().take()
    }

    fn take_stderr(&mut self) -> Option<tokio::process::ChildStderr> {
        self.child.stderr().take()
    }

    const fn disarm(&mut self) {
        self.armed = false;
    }

    /// Releases the identifier reservation after an operation left the leader in an unknown state.
    const fn unpin(&mut self) {
        self.pinned = false;
    }

    /// Polls for a reaped leader, releasing the identifier reservation on any inspection failure.
    ///
    /// Every `try_wait` in this module goes through here so no call site can observe a failure
    /// without also recording that the identifier can no longer be signalled.
    ///
    /// This deliberately inspects the leader rather than the containment unit. Every caller is
    /// asking whether the identifier it is about to signal is still valid, which is a question
    /// about the leader alone; the containment unit answers a different question, and answers it
    /// misleadingly here, because a group whose members have all been reaped is indistinguishable
    /// from one that never exited.
    fn poll_reaped(&mut self) -> Option<ExitStatus> {
        match self.child.inner_mut().try_wait() {
            Ok(status) => status,
            Err(_error) => {
                self.pinned = false;
                None
            }
        }
    }

    /// Waits for the leader alone, never for the containment unit.
    ///
    /// Every wait this crate performs is either raced against cancellation or bounded by a
    /// deadline, and a containment-unit wait is unsafe in both positions: on Unix it reaps the
    /// leader first and then keeps draining the unit, so a future dropped after that first step
    /// would leave the identifier released while this type still believed it was pinned, and the
    /// drain it was performing would be stranded on the blocking pool. Little is given up by not
    /// waiting for the unit: descendants are children of the leader, not of this process, so
    /// reaping them here is possible only if the host has made itself a subreaper, which this
    /// crate neither does nor can observe.
    fn wait_leader(&mut self) -> Pin<Box<dyn Future<Output = io::Result<ExitStatus>> + Send + '_>> {
        self.child.inner_mut().wait()
    }

    /// Requests cooperative termination of the containment unit, reporting whether one was sent.
    ///
    /// Unix has a cooperative signal for a process group. Windows has no equivalent for a job
    /// object, so there no request is sent and the caller skips the graceful window instead of
    /// spending it waiting for a request the child never received.
    fn request_graceful(&self) -> bool {
        if !self.pinned {
            return false;
        }
        #[cfg(unix)]
        {
            self.child.signal(libc::SIGTERM).is_ok()
        }
        #[cfg(not(unix))]
        {
            false
        }
    }

    /// Force-terminates the containment unit, reporting whether the request was accepted.
    ///
    /// On a target with no containment object the request still reaches the direct child, but it
    /// proves nothing about descendants, so isolated mode reports it as undelivered rather than
    /// claiming a containment it never had.
    fn force_terminate(&mut self) -> bool {
        if !self.pinned {
            return false;
        }
        let accepted = self.child.start_kill().is_ok();
        accepted && (TREE_CONTAINMENT_SUPPORTED || self.mode == ProcessTreeMode::DirectChild)
    }

    /// Force-terminates only the direct child, bypassing a containment object that refused.
    ///
    /// This is the degraded path: it keeps a failed tree signal from also leaking the leader.
    fn force_terminate_leader(&mut self) -> io::Result<()> {
        self.child.inner_mut().start_kill()
    }

    /// Records that containment could not be proven for this invocation.
    ///
    /// Direct-child mode makes no tree claim, so nothing is recorded for it.
    fn record_containment_gap(&self, tree_degraded: &mut bool) {
        if self.mode == ProcessTreeMode::IsolatedTree {
            *tree_degraded = true;
            self.counters
                .tree_termination_failures
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Drop for SupervisedChild {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if !self.force_terminate() {
            // An undelivered abandonment signal degrades containment exactly like a failed forced
            // tree signal, so it must stay observable instead of being discarded.
            self.counters
                .tree_termination_failures
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn terminate_and_reap(
    child: &mut SupervisedChild,
    config: SubprocessSupervisorConfig,
    tree_degraded: &mut bool,
) -> Result<ExitStatus, SubprocessError> {
    if let Some(status) = child.poll_reaped() {
        // The leader vanished before any tree signal could be delivered, so nothing proves its
        // descendants ended with it.
        child.record_containment_gap(tree_degraded);
        return Ok(status);
    }

    if child.request_graceful() {
        // Only a confirmed reap ends termination early; a failed wait unpins the identifier and
        // falls through to the forced path instead of abandoning the child.
        match timeout(config.graceful_termination(), child.wait_leader()).await {
            Ok(Ok(status)) => return Ok(status),
            Ok(Err(_error)) => child.unpin(),
            Err(_elapsed) => {}
        }
    }

    if !child.force_terminate() {
        // Containment degraded to the direct child, so the degradation must stay observable per
        // invocation and in the aggregate instead of being silently discarded before the fallback
        // kill.
        child.record_containment_gap(tree_degraded);
        // The forced signal may have been refused because the leader exited in the meantime.
        if let Some(status) = child.poll_reaped() {
            return Ok(status);
        }
        if child.pinned
            && let Err(error) = child.force_terminate_leader()
        {
            if let Some(status) = child.poll_reaped() {
                return Ok(status);
            }
            return Err(SubprocessError::new(
                SubprocessErrorKind::Termination,
                "subprocess hard termination could not be requested",
                error,
            ));
        }
    }

    let waited = timeout(config.hard_kill_timeout(), child.wait_leader()).await;
    let status = match waited {
        Ok(status) => status,
        Err(error) => {
            return Err(SubprocessError::new(
                SubprocessErrorKind::Termination,
                "subprocess did not exit before the hard termination deadline",
                io::Error::new(io::ErrorKind::TimedOut, error),
            ));
        }
    };
    status.map_err(|error| {
        child.unpin();
        SubprocessError::new(
            SubprocessErrorKind::Termination,
            "subprocess wait failed after hard termination",
            error,
        )
    })
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

#[cfg(test)]
mod tests {
    use super::{ProcessTreeMode, build_command};
    use crate::{SubprocessSpec, SubprocessSpecError};
    use process_wrap::tokio::KillOnDrop;

    #[cfg(unix)]
    type Containment = process_wrap::tokio::ProcessGroup;
    #[cfg(windows)]
    type Containment = process_wrap::tokio::JobObject;

    #[cfg(any(unix, windows))]
    #[test]
    fn only_isolated_tree_mode_attaches_a_containment_object() -> Result<(), SubprocessSpecError> {
        let spec = SubprocessSpec::new("/nonexistent/never-spawned")?;

        let isolated = build_command(&spec, ProcessTreeMode::IsolatedTree);
        assert!(isolated.has_wrap::<Containment>());

        let direct = build_command(&spec, ProcessTreeMode::DirectChild);
        assert!(!direct.has_wrap::<Containment>());

        Ok(())
    }

    #[test]
    fn kill_on_drop_is_requested_in_every_mode() -> Result<(), SubprocessSpecError> {
        // The job object reads this back to decide whether closing its last handle terminates the
        // members, so requesting it on the command instead of through the wrapper would silently
        // drop Windows containment on abandonment.
        let spec = SubprocessSpec::new("/nonexistent/never-spawned")?;

        assert!(build_command(&spec, ProcessTreeMode::IsolatedTree).has_wrap::<KillOnDrop>());
        assert!(build_command(&spec, ProcessTreeMode::DirectChild).has_wrap::<KillOnDrop>());

        Ok(())
    }
}
