use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    time::Duration,
};

use crate::ProcessTreeMode;

/// Hard upper bound for concurrently supervised child processes.
pub const MAX_CONCURRENT_SUBPROCESSES: usize = 4_096;
/// Hard upper bound retained independently for stdout and stderr.
pub const MAX_CAPTURE_BYTES: usize = 16 * 1024 * 1024;
const MAX_MEMORY_LIMIT_BYTES: u64 = 1024 * 1024 * 1024 * 1024;

/// Validated limits for one subprocess supervisor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubprocessSupervisorConfig {
    max_concurrent: usize,
    execution_timeout: Duration,
    graceful_termination: Duration,
    hard_kill_timeout: Duration,
    output_drain_timeout: Duration,
    max_stdout_bytes: usize,
    max_stderr_bytes: usize,
    process_tree_mode: ProcessTreeMode,
    resident_memory_limit_bytes: Option<u64>,
    memory_sample_interval: Duration,
}

impl SubprocessSupervisorConfig {
    /// Creates a supervisor configuration with conservative output and termination defaults.
    ///
    /// # Errors
    ///
    /// Returns an error for zero or excessive concurrency, or a zero execution timeout.
    pub fn new(
        max_concurrent: usize,
        execution_timeout: Duration,
    ) -> Result<Self, SubprocessConfigError> {
        if max_concurrent == 0 {
            return Err(SubprocessConfigError::ZeroConcurrency);
        }
        if max_concurrent > MAX_CONCURRENT_SUBPROCESSES {
            return Err(SubprocessConfigError::ConcurrencyAboveMaximum);
        }
        if execution_timeout.is_zero() {
            return Err(SubprocessConfigError::ZeroExecutionTimeout);
        }
        Ok(Self {
            max_concurrent,
            execution_timeout,
            graceful_termination: Duration::from_secs(2),
            hard_kill_timeout: Duration::from_secs(5),
            output_drain_timeout: Duration::from_secs(2),
            max_stdout_bytes: 1024 * 1024,
            max_stderr_bytes: 1024 * 1024,
            process_tree_mode: ProcessTreeMode::IsolatedTree,
            resident_memory_limit_bytes: None,
            memory_sample_interval: Duration::from_millis(100),
        })
    }

    /// Sets independent retained-output limits while continuing to drain excess bytes.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero limit or a limit above [`MAX_CAPTURE_BYTES`].
    pub fn with_output_limits(
        mut self,
        max_stdout_bytes: usize,
        max_stderr_bytes: usize,
    ) -> Result<Self, SubprocessConfigError> {
        validate_capture_limit(max_stdout_bytes)?;
        validate_capture_limit(max_stderr_bytes)?;
        self.max_stdout_bytes = max_stdout_bytes;
        self.max_stderr_bytes = max_stderr_bytes;
        Ok(self)
    }

    /// Sets graceful, hard-kill, and pipe-drain deadlines.
    ///
    /// # Errors
    ///
    /// Returns an error when any deadline is zero.
    pub fn with_termination_timeouts(
        mut self,
        graceful: Duration,
        hard_kill: Duration,
        output_drain: Duration,
    ) -> Result<Self, SubprocessConfigError> {
        if graceful.is_zero() || hard_kill.is_zero() || output_drain.is_zero() {
            return Err(SubprocessConfigError::ZeroTerminationTimeout);
        }
        self.graceful_termination = graceful;
        self.hard_kill_timeout = hard_kill;
        self.output_drain_timeout = output_drain;
        Ok(self)
    }

    /// Selects whether termination targets only the child or its isolated process tree.
    #[must_use]
    pub const fn with_process_tree_mode(mut self, mode: ProcessTreeMode) -> Self {
        self.process_tree_mode = mode;
        self
    }

    /// Enables fail-closed Linux RSS enforcement.
    ///
    /// # Errors
    ///
    /// Returns an error for zero/excessive limits, a zero sample interval, or unsupported hosts.
    pub fn with_resident_memory_limit(
        mut self,
        bytes: u64,
        sample_interval: Duration,
    ) -> Result<Self, SubprocessConfigError> {
        if bytes == 0 {
            return Err(SubprocessConfigError::ZeroMemoryLimit);
        }
        if bytes > MAX_MEMORY_LIMIT_BYTES {
            return Err(SubprocessConfigError::MemoryLimitAboveMaximum);
        }
        if sample_interval.is_zero() {
            return Err(SubprocessConfigError::ZeroMemorySampleInterval);
        }
        if !cfg!(target_os = "linux") {
            return Err(SubprocessConfigError::MemoryLimitUnsupported);
        }
        self.resident_memory_limit_bytes = Some(bytes);
        self.memory_sample_interval = sample_interval;
        Ok(self)
    }

    /// Returns the maximum number of simultaneous child processes.
    #[must_use]
    pub const fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }

    /// Returns the maximum wall-clock execution duration.
    #[must_use]
    pub const fn execution_timeout(&self) -> Duration {
        self.execution_timeout
    }

    pub(crate) const fn graceful_termination(&self) -> Duration {
        self.graceful_termination
    }

    pub(crate) const fn hard_kill_timeout(&self) -> Duration {
        self.hard_kill_timeout
    }

    pub(crate) const fn output_drain_timeout(&self) -> Duration {
        self.output_drain_timeout
    }

    pub(crate) const fn max_stdout_bytes(&self) -> usize {
        self.max_stdout_bytes
    }

    pub(crate) const fn max_stderr_bytes(&self) -> usize {
        self.max_stderr_bytes
    }

    pub(crate) const fn process_tree_mode(&self) -> ProcessTreeMode {
        self.process_tree_mode
    }

    pub(crate) const fn resident_memory_limit_bytes(&self) -> Option<u64> {
        self.resident_memory_limit_bytes
    }

    pub(crate) const fn memory_sample_interval(&self) -> Duration {
        self.memory_sample_interval
    }
}

fn validate_capture_limit(limit: usize) -> Result<(), SubprocessConfigError> {
    if limit == 0 {
        Err(SubprocessConfigError::ZeroCaptureLimit)
    } else if limit > MAX_CAPTURE_BYTES {
        Err(SubprocessConfigError::CaptureLimitAboveMaximum)
    } else {
        Ok(())
    }
}

/// Invalid supervisor configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubprocessConfigError {
    /// Concurrency must be positive.
    ZeroConcurrency,
    /// Concurrency exceeded the hard maximum.
    ConcurrencyAboveMaximum,
    /// Execution timeout must be positive.
    ZeroExecutionTimeout,
    /// Output capture limits must be positive.
    ZeroCaptureLimit,
    /// An output capture limit exceeded the hard maximum.
    CaptureLimitAboveMaximum,
    /// Termination and drain timeouts must be positive.
    ZeroTerminationTimeout,
    /// RSS limit must be positive.
    ZeroMemoryLimit,
    /// RSS limit exceeded the defensive maximum.
    MemoryLimitAboveMaximum,
    /// RSS sample interval must be positive.
    ZeroMemorySampleInterval,
    /// In-process RSS enforcement is currently supported only on Linux.
    MemoryLimitUnsupported,
}

impl Display for SubprocessConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroConcurrency => "subprocess concurrency must be positive",
            Self::ConcurrencyAboveMaximum => "subprocess concurrency exceeds the hard maximum",
            Self::ZeroExecutionTimeout => "subprocess execution timeout must be positive",
            Self::ZeroCaptureLimit => "subprocess output capture limit must be positive",
            Self::CaptureLimitAboveMaximum => {
                "subprocess output capture limit exceeds the hard maximum"
            }
            Self::ZeroTerminationTimeout => "subprocess termination timeouts must be positive",
            Self::ZeroMemoryLimit => "subprocess RSS limit must be positive",
            Self::MemoryLimitAboveMaximum => "subprocess RSS limit exceeds the hard maximum",
            Self::ZeroMemorySampleInterval => "subprocess RSS sample interval must be positive",
            Self::MemoryLimitUnsupported => {
                "subprocess RSS enforcement is unsupported on this operating system"
            }
        })
    }
}

impl Error for SubprocessConfigError {}
