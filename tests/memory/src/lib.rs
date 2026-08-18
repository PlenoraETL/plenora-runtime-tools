//! Synthetic, bounded memory workloads used to qualify worker allocation release.

#![forbid(unsafe_code)]

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    fs, hint, io,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use futures_util::future::join_all;
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{
    CorrelationId, MessageId, MessageMetadata, RetryDecision, RetryPolicy,
};
use plenora_runtime_worker::{
    WorkerConcurrency, WorkerConfig, WorkerContext, WorkerErrorCategory, WorkerExecutor,
    WorkerHandler,
};

const PAGE_BYTES: usize = 4_096;
const MIB: usize = 1024 * 1024;

/// Synthetic allocation pattern executed by the long-lived worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryScenario {
    /// Repeatedly allocates and releases one large buffer.
    Plateau,
    /// Allocates the same total through differently sized blocks.
    Fragmentation,
    /// Executes the configured number of allocations concurrently.
    Concurrent,
    /// Returns a handler error after allocating and touching memory.
    Error,
    /// Waits for the worker deadline while retaining the allocation.
    Cancellation,
    /// Deliberately retains each allocation to validate growth detection.
    LeakControl,
}

impl MemoryScenario {
    /// Returns the stable command-line name of the scenario.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Plateau => "plateau",
            Self::Fragmentation => "fragmentation",
            Self::Concurrent => "concurrent",
            Self::Error => "error",
            Self::Cancellation => "cancellation",
            Self::LeakControl => "leak-control",
        }
    }
}

impl Display for MemoryScenario {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<&str> for MemoryScenario {
    type Error = ProbeConfigError;

    fn try_from(value: &str) -> Result<Self, ProbeConfigError> {
        match value {
            "plateau" => Ok(Self::Plateau),
            "fragmentation" => Ok(Self::Fragmentation),
            "concurrent" => Ok(Self::Concurrent),
            "error" => Ok(Self::Error),
            "cancellation" => Ok(Self::Cancellation),
            "leak-control" => Ok(Self::LeakControl),
            _ => Err(ProbeConfigError::UnknownScenario),
        }
    }
}

/// Validated settings for one probe process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProbeConfig {
    /// Synthetic workload pattern.
    pub scenario: MemoryScenario,
    /// Recorded iterations after warm-up.
    pub iterations: usize,
    /// Unrecorded iterations used to warm the allocator.
    pub warmup_iterations: usize,
    /// Committed bytes owned by each handler invocation.
    pub allocation_bytes: usize,
    /// Maximum worker concurrency and concurrent scenario fan-out.
    pub max_in_flight: usize,
    /// Quiet period before each RSS sample.
    pub settle_period: Duration,
    /// Maximum tolerated retained-RSS growth after warm-up.
    pub retained_growth_limit_bytes: usize,
}

impl ProbeConfig {
    /// Creates conservative defaults suitable for a bounded local probe.
    #[must_use]
    pub const fn for_scenario(scenario: MemoryScenario) -> Self {
        Self {
            scenario,
            iterations: 40,
            warmup_iterations: 5,
            allocation_bytes: 32 * MIB,
            max_in_flight: 4,
            settle_period: Duration::from_millis(25),
            retained_growth_limit_bytes: 16 * MIB,
        }
    }

    /// Validates all allocation and iteration bounds.
    ///
    /// # Errors
    ///
    /// Returns a stable configuration error for zero or overflowing values.
    pub fn validate(self) -> Result<Self, ProbeConfigError> {
        if self.iterations < 4 {
            return Err(ProbeConfigError::TooFewIterations);
        }
        if self.allocation_bytes == 0 {
            return Err(ProbeConfigError::ZeroAllocation);
        }
        if self.max_in_flight == 0 {
            return Err(ProbeConfigError::ZeroConcurrency);
        }
        if self.settle_period.is_zero() {
            return Err(ProbeConfigError::ZeroSettlePeriod);
        }
        self.allocation_bytes
            .checked_mul(self.max_in_flight)
            .ok_or(ProbeConfigError::WorkingSetOverflow)?;
        Ok(self)
    }

    fn fan_out(self) -> usize {
        if self.scenario == MemoryScenario::Concurrent {
            self.max_in_flight
        } else {
            1
        }
    }
}

/// Stable invalid-probe category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeConfigError {
    /// Scenario text is not supported.
    UnknownScenario,
    /// At least four samples are required for quartile comparison.
    TooFewIterations,
    /// A task must allocate at least one byte.
    ZeroAllocation,
    /// Worker concurrency must be positive.
    ZeroConcurrency,
    /// Sampling requires a nonzero quiet period.
    ZeroSettlePeriod,
    /// Configured concurrent working-set multiplication overflowed.
    WorkingSetOverflow,
}

impl Display for ProbeConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnknownScenario => "memory probe scenario is unknown",
            Self::TooFewIterations => "memory probe requires at least four recorded iterations",
            Self::ZeroAllocation => "memory probe allocation must be positive",
            Self::ZeroConcurrency => "memory probe concurrency must be positive",
            Self::ZeroSettlePeriod => "memory probe settle period must be positive",
            Self::WorkingSetOverflow => "memory probe concurrent working set overflowed",
        })
    }
}

impl Error for ProbeConfigError {}

/// Current and high-water resident memory reported by Linux `/proc`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessMemorySample {
    /// Current resident bytes.
    pub rss_bytes: u64,
    /// Maximum resident bytes observed by the process.
    pub peak_rss_bytes: u64,
}

/// Stable classification of retained memory after allocator warm-up.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetentionClassification {
    /// Retained growth remained within the configured tolerance.
    Stable,
    /// Retained growth exceeded the configured tolerance.
    Growing,
}

impl RetentionClassification {
    /// Returns the stable CSV value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stable => "stable",
            Self::Growing => "growing",
        }
    }
}

/// Summary of one long-lived memory probe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetentionSummary {
    /// Median RSS of the first recorded quartile.
    pub initial_median_rss_bytes: u64,
    /// Median RSS of the final recorded quartile.
    pub final_median_rss_bytes: u64,
    /// Positive retained growth between those medians.
    pub retained_growth_bytes: u64,
    /// Configured maximum retained growth.
    pub retained_growth_limit_bytes: u64,
    /// Resulting stable/growing classification.
    pub classification: RetentionClassification,
}

/// An allocation accounting snapshot independent of operating-system RSS.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AllocationSnapshot {
    /// Bytes still owned by live synthetic task allocations.
    pub live_bytes: usize,
    /// Maximum simultaneously owned bytes.
    pub peak_live_bytes: usize,
}

/// One recorded post-task probe row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProbeSample {
    /// One-based recorded iteration.
    pub iteration: usize,
    /// Process memory after the settle period.
    pub process: ProcessMemorySample,
    /// Task-owned allocation state after the iteration.
    pub allocations: AllocationSnapshot,
    /// Number of worker invocations still in flight.
    pub worker_in_flight: usize,
    /// Stable expected outcome category.
    pub outcome: &'static str,
}

/// Complete result produced by one probe process.
#[derive(Debug)]
pub struct ProbeReport {
    /// Validated settings used by the probe.
    pub config: ProbeConfig,
    /// Recorded iteration samples.
    pub samples: Vec<ProbeSample>,
    /// Retained-RSS classification.
    pub summary: RetentionSummary,
}

#[derive(Debug, Default)]
struct AllocationTracker {
    live_bytes: AtomicUsize,
    peak_live_bytes: AtomicUsize,
}

impl AllocationTracker {
    fn allocated(&self, bytes: usize) {
        let live = self.live_bytes.fetch_add(bytes, Ordering::AcqRel) + bytes;
        let _previous = self.peak_live_bytes.fetch_max(live, Ordering::AcqRel);
    }

    fn released(&self, bytes: usize) {
        let _previous = self.live_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }

    fn snapshot(&self) -> AllocationSnapshot {
        AllocationSnapshot {
            live_bytes: self.live_bytes.load(Ordering::Acquire),
            peak_live_bytes: self.peak_live_bytes.load(Ordering::Acquire),
        }
    }
}

#[derive(Debug)]
struct TrackedAllocation {
    bytes: Vec<u8>,
    tracked_bytes: usize,
    tracker: Arc<AllocationTracker>,
}

impl TrackedAllocation {
    fn new(bytes: usize, pattern: u8, tracker: Arc<AllocationTracker>) -> Result<Self, ProbeError> {
        let mut allocation = Vec::new();
        allocation
            .try_reserve_exact(bytes)
            .map_err(|_error| ProbeError::AllocationRejected)?;
        allocation.resize(bytes, 0);
        let mut checksum = 0_u64;
        for page in allocation.chunks_mut(PAGE_BYTES) {
            if let Some(first) = page.first_mut() {
                *first = pattern;
                checksum = checksum.wrapping_add(u64::from(*first));
            }
        }
        hint::black_box(checksum);
        tracker.allocated(bytes);
        Ok(Self {
            bytes: allocation,
            tracked_bytes: bytes,
            tracker,
        })
    }

    fn checksum(&self) -> u64 {
        self.bytes
            .chunks(PAGE_BYTES)
            .filter_map(|page| page.first())
            .fold(0_u64, |sum, byte| sum.wrapping_add(u64::from(*byte)))
    }
}

impl Drop for TrackedAllocation {
    fn drop(&mut self) {
        self.tracker.released(self.tracked_bytes);
    }
}

#[derive(Clone, Debug)]
struct MemoryWorkload {
    tracker: Arc<AllocationTracker>,
    leaked: Arc<Mutex<Vec<TrackedAllocation>>>,
}

impl MemoryWorkload {
    fn new(tracker: Arc<AllocationTracker>) -> Self {
        Self {
            tracker,
            leaked: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn allocation(&self, request: MemoryRequest) -> Result<Vec<TrackedAllocation>, ProbeError> {
        if request.scenario != MemoryScenario::Fragmentation {
            return Ok(vec![TrackedAllocation::new(
                request.allocation_bytes,
                request.pattern,
                Arc::clone(&self.tracker),
            )?]);
        }

        let mut remaining = request.allocation_bytes;
        let mut block_bytes = MIB.min(remaining);
        let mut allocations = Vec::new();
        while remaining > 0 {
            let bytes = block_bytes.min(remaining);
            allocations.push(TrackedAllocation::new(
                bytes,
                request.pattern,
                Arc::clone(&self.tracker),
            )?);
            remaining -= bytes;
            block_bytes = block_bytes.saturating_mul(2).clamp(1, 32 * MIB);
        }
        Ok(allocations)
    }

    fn retain(&self, mut allocations: Vec<TrackedAllocation>) {
        self.leaked().append(&mut allocations);
    }

    fn leaked(&self) -> MutexGuard<'_, Vec<TrackedAllocation>> {
        match self.leaked.lock() {
            Ok(leaked) => leaked,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct MemoryRequest {
    scenario: MemoryScenario,
    allocation_bytes: usize,
    pattern: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProbeError {
    AllocationRejected,
    IntentionalHandlerFailure,
}

impl Display for ProbeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AllocationRejected => "synthetic memory allocation was rejected",
            Self::IntentionalHandlerFailure => "synthetic handler failure was requested",
        })
    }
}

impl Error for ProbeError {}

#[async_trait]
impl WorkerHandler<MemoryRequest> for MemoryWorkload {
    type Error = ProbeError;

    async fn handle(
        &self,
        context: WorkerContext,
        request: MemoryRequest,
    ) -> Result<(), Self::Error> {
        let allocations = self.allocation(request)?;
        let checksum = allocations.iter().fold(0_u64, |sum, allocation| {
            sum.wrapping_add(allocation.checksum())
        });
        hint::black_box(checksum);
        tokio::task::yield_now().await;

        match request.scenario {
            MemoryScenario::Error => Err(ProbeError::IntentionalHandlerFailure),
            MemoryScenario::Cancellation => {
                let _reason = context.cancelled().await;
                drop(allocations);
                Ok(())
            }
            MemoryScenario::LeakControl => {
                self.retain(allocations);
                Ok(())
            }
            MemoryScenario::Plateau
            | MemoryScenario::Fragmentation
            | MemoryScenario::Concurrent => Ok(()),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct NoRetry;

impl RetryPolicy<ProbeError> for NoRetry {
    fn decide(&self, _attempt: u32, _error: &ProbeError) -> RetryDecision {
        RetryDecision::DoNotRetry
    }
}

/// Runs a long-lived worker memory probe and samples Linux process RSS after every iteration.
///
/// # Errors
///
/// Returns an error for invalid configuration, allocation failure, unexpected worker outcomes, or
/// unavailable `/proc` process memory information.
pub async fn run_probe(config: ProbeConfig) -> Result<ProbeReport, ProbeRunError> {
    let config = config.validate().map_err(ProbeRunError::Config)?;
    let runtime = RuntimeHandle::new(ServiceMetadata::new(
        "memory-probe",
        env!("CARGO_PKG_VERSION"),
        "local-memory-probe",
    ));
    let tracker = Arc::new(AllocationTracker::default());
    let workload = MemoryWorkload::new(Arc::clone(&tracker));
    let worker_config = worker_config(config)?;
    let executor = WorkerExecutor::new(workload, NoRetry, worker_config)
        .map_err(|error| ProbeRunError::WorkerConfig(Box::new(error)))?;

    for warmup in 0..config.warmup_iterations {
        let pattern = pattern_for(warmup);
        let _outcome = execute_iteration(&executor, &runtime, config, pattern).await?;
    }

    let mut samples = Vec::with_capacity(config.iterations);
    for index in 0..config.iterations {
        let outcome = execute_iteration(&executor, &runtime, config, pattern_for(index)).await?;
        tokio::time::sleep(config.settle_period).await;
        samples.push(ProbeSample {
            iteration: index + 1,
            process: read_linux_process_memory().map_err(ProbeRunError::ProcessMemory)?,
            allocations: tracker.snapshot(),
            worker_in_flight: executor.in_flight(),
            outcome,
        });
    }

    let summary = classify_retention(
        &samples
            .iter()
            .map(|sample| sample.process.rss_bytes)
            .collect::<Vec<_>>(),
        u64::try_from(config.retained_growth_limit_bytes).unwrap_or(u64::MAX),
    )?;
    let _shutdown_started = runtime.request_shutdown();
    let _worker_drain = executor.drain().await;
    let _runtime_drain = runtime.shutdown().await;
    Ok(ProbeReport {
        config,
        samples,
        summary,
    })
}

fn worker_config(config: ProbeConfig) -> Result<WorkerConfig, ProbeRunError> {
    let concurrency = WorkerConcurrency::new(config.max_in_flight)
        .map_err(|error| ProbeRunError::WorkerConfig(Box::new(error)))?;
    let base = WorkerConfig::new(concurrency, Duration::from_secs(5));
    if config.scenario == MemoryScenario::Cancellation {
        base.with_execution_timeout(
            Duration::from_millis(20),
            Duration::from_millis(50),
            RetryDecision::DoNotRetry,
        )
        .map_err(|error| ProbeRunError::WorkerConfig(Box::new(error)))
    } else {
        Ok(base)
    }
}

async fn execute_iteration(
    executor: &WorkerExecutor<MemoryWorkload, NoRetry>,
    runtime: &RuntimeHandle,
    config: ProbeConfig,
    pattern: u8,
) -> Result<&'static str, ProbeRunError> {
    let futures = (0..config.fan_out()).map(|offset| {
        executor.execute(
            worker_context(runtime),
            MemoryRequest {
                scenario: config.scenario,
                allocation_bytes: config.allocation_bytes,
                pattern: pattern.wrapping_add(u8::try_from(offset).unwrap_or(u8::MAX)),
            },
        )
    });
    let results = join_all(futures).await;
    for result in results {
        match (config.scenario, result) {
            (
                MemoryScenario::Plateau
                | MemoryScenario::Fragmentation
                | MemoryScenario::Concurrent
                | MemoryScenario::LeakControl,
                Ok(()),
            ) => {}
            (MemoryScenario::Error, Err(error))
                if error.category() == WorkerErrorCategory::Handler => {}
            (MemoryScenario::Cancellation, Err(error))
                if matches!(
                    error.category(),
                    WorkerErrorCategory::Timeout | WorkerErrorCategory::Cancelled
                ) => {}
            (_scenario, Ok(())) => return Err(ProbeRunError::UnexpectedSuccess),
            (_scenario, Err(_error)) => return Err(ProbeRunError::UnexpectedWorkerFailure),
        }
    }
    Ok(match config.scenario {
        MemoryScenario::Error => "expected-error",
        MemoryScenario::Cancellation => "expected-cancellation",
        MemoryScenario::Plateau
        | MemoryScenario::Fragmentation
        | MemoryScenario::Concurrent
        | MemoryScenario::LeakControl => "success",
    })
}

fn worker_context(runtime: &RuntimeHandle) -> WorkerContext {
    WorkerContext::new(
        MessageId::random(),
        CorrelationId::random(),
        None,
        1,
        MessageMetadata::new(),
        runtime.shutdown_signal(),
    )
}

fn pattern_for(index: usize) -> u8 {
    let reduced = index % usize::from(u8::MAX);
    match u8::try_from(reduced) {
        Ok(value) => value.wrapping_add(1),
        Err(_error) => u8::MAX,
    }
}

/// Parses Linux `/proc/self/status` into current and peak resident bytes.
///
/// # Errors
///
/// Returns an error when either required field is absent, malformed, or overflows bytes.
pub fn parse_linux_process_status(status: &str) -> Result<ProcessMemorySample, io::Error> {
    let rss_kib = status_value_kib(status, "VmRSS:")?;
    let peak_kib = status_value_kib(status, "VmHWM:")?;
    Ok(ProcessMemorySample {
        rss_bytes: rss_kib
            .checked_mul(1024)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "VmRSS byte overflow"))?,
        peak_rss_bytes: peak_kib
            .checked_mul(1024)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "VmHWM byte overflow"))?,
    })
}

/// Reads current Linux process resident memory.
///
/// # Errors
///
/// Returns an I/O or parse error when `/proc/self/status` is unavailable or invalid.
pub fn read_linux_process_memory() -> Result<ProcessMemorySample, io::Error> {
    parse_linux_process_status(&fs::read_to_string("/proc/self/status")?)
}

fn status_value_kib(status: &str, field: &str) -> Result<u64, io::Error> {
    let line = status
        .lines()
        .find(|line| line.starts_with(field))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "memory field is absent"))?;
    let value = line
        .split_ascii_whitespace()
        .nth(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "memory value is absent"))?;
    value
        .parse::<u64>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// Classifies retained RSS using first- and last-quartile medians.
///
/// # Errors
///
/// Returns an error when fewer than four samples are supplied.
pub fn classify_retention(
    rss_samples: &[u64],
    retained_growth_limit_bytes: u64,
) -> Result<RetentionSummary, ProbeRunError> {
    if rss_samples.len() < 4 {
        return Err(ProbeRunError::Config(ProbeConfigError::TooFewIterations));
    }
    let quartile_len = (rss_samples.len() / 4).max(1);
    let initial = median(&rss_samples[..quartile_len]);
    let final_start = rss_samples.len().saturating_sub(quartile_len);
    let final_median = median(&rss_samples[final_start..]);
    let retained_growth_bytes = final_median.saturating_sub(initial);
    let classification = if retained_growth_bytes > retained_growth_limit_bytes {
        RetentionClassification::Growing
    } else {
        RetentionClassification::Stable
    };
    Ok(RetentionSummary {
        initial_median_rss_bytes: initial,
        final_median_rss_bytes: final_median,
        retained_growth_bytes,
        retained_growth_limit_bytes,
        classification,
    })
}

fn median(values: &[u64]) -> u64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let middle = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        sorted[middle - 1].saturating_add(sorted[middle]) / 2
    } else {
        sorted[middle]
    }
}

/// Failure while configuring or running a memory probe.
#[derive(Debug)]
pub enum ProbeRunError {
    /// Probe configuration is invalid.
    Config(ProbeConfigError),
    /// Worker configuration failed.
    WorkerConfig(Box<dyn Error + Send + Sync>),
    /// Linux process memory could not be read.
    ProcessMemory(io::Error),
    /// A scenario expected failure but the worker succeeded.
    UnexpectedSuccess,
    /// A scenario returned a worker failure of the wrong category.
    UnexpectedWorkerFailure,
}

impl Display for ProbeRunError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Config(_) => "memory probe configuration is invalid",
            Self::WorkerConfig(_) => "memory probe worker configuration failed",
            Self::ProcessMemory(_) => "memory probe process RSS is unavailable",
            Self::UnexpectedSuccess => "memory probe scenario unexpectedly succeeded",
            Self::UnexpectedWorkerFailure => "memory probe worker returned an unexpected failure",
        })
    }
}

impl Error for ProbeRunError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Config(error) => Some(error),
            Self::WorkerConfig(error) => Some(error.as_ref()),
            Self::ProcessMemory(error) => Some(error),
            Self::UnexpectedSuccess | Self::UnexpectedWorkerFailure => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_status_parser_reads_kib_fields() -> Result<(), Box<dyn Error>> {
        let sample =
            parse_linux_process_status("Name:\tprobe\nVmHWM:\t2048 kB\nVmRSS:\t1024 kB\n")?;
        assert_eq!(sample.rss_bytes, 1024 * 1024);
        assert_eq!(sample.peak_rss_bytes, 2 * 1024 * 1024);
        Ok(())
    }

    #[test]
    fn classifier_accepts_a_plateau_and_rejects_growth() -> Result<(), Box<dyn Error>> {
        let plateau = classify_retention(&[100, 110, 108, 109, 111, 110, 109, 111], 16)?;
        assert_eq!(plateau.classification, RetentionClassification::Stable);

        let growing = classify_retention(&[100, 110, 120, 130, 180, 190, 200, 210], 16)?;
        assert_eq!(growing.classification, RetentionClassification::Growing);
        assert!(growing.retained_growth_bytes > growing.retained_growth_limit_bytes);
        Ok(())
    }

    #[tokio::test]
    async fn success_error_and_timeout_release_task_owned_allocations() -> Result<(), Box<dyn Error>>
    {
        for scenario in [
            MemoryScenario::Plateau,
            MemoryScenario::Error,
            MemoryScenario::Cancellation,
        ] {
            let runtime = RuntimeHandle::new(ServiceMetadata::new("test", "0.1.0", "instance"));
            let tracker = Arc::new(AllocationTracker::default());
            let workload = MemoryWorkload::new(Arc::clone(&tracker));
            let config = ProbeConfig {
                iterations: 4,
                warmup_iterations: 0,
                allocation_bytes: MIB,
                max_in_flight: 2,
                settle_period: Duration::from_millis(1),
                retained_growth_limit_bytes: MIB,
                scenario,
            };
            let executor = WorkerExecutor::new(workload, NoRetry, worker_config(config)?)?;
            let _outcome = execute_iteration(&executor, &runtime, config, 7).await?;
            assert_eq!(tracker.snapshot().live_bytes, 0);
            assert_eq!(executor.in_flight(), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_scenario_reaches_bound_and_releases_every_allocation()
    -> Result<(), Box<dyn Error>> {
        let runtime = RuntimeHandle::new(ServiceMetadata::new("test", "0.1.0", "instance"));
        let tracker = Arc::new(AllocationTracker::default());
        let workload = MemoryWorkload::new(Arc::clone(&tracker));
        let config = ProbeConfig {
            iterations: 4,
            warmup_iterations: 0,
            allocation_bytes: MIB,
            max_in_flight: 4,
            settle_period: Duration::from_millis(1),
            retained_growth_limit_bytes: MIB,
            scenario: MemoryScenario::Concurrent,
        };
        let executor = WorkerExecutor::new(workload, NoRetry, worker_config(config)?)?;

        let _outcome = execute_iteration(&executor, &runtime, config, 11).await?;

        let snapshot = tracker.snapshot();
        assert_eq!(snapshot.live_bytes, 0);
        assert_eq!(snapshot.peak_live_bytes, 4 * MIB);
        assert_eq!(executor.in_flight(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn leak_control_retains_allocations_for_detector_validation() -> Result<(), Box<dyn Error>>
    {
        let runtime = RuntimeHandle::new(ServiceMetadata::new("test", "0.1.0", "instance"));
        let tracker = Arc::new(AllocationTracker::default());
        let workload = MemoryWorkload::new(Arc::clone(&tracker));
        let config = ProbeConfig {
            iterations: 4,
            warmup_iterations: 0,
            allocation_bytes: MIB,
            max_in_flight: 1,
            settle_period: Duration::from_millis(1),
            retained_growth_limit_bytes: MIB,
            scenario: MemoryScenario::LeakControl,
        };
        let executor = WorkerExecutor::new(workload, NoRetry, worker_config(config)?)?;

        for pattern in 1..=4 {
            let _outcome = execute_iteration(&executor, &runtime, config, pattern).await?;
        }

        assert_eq!(tracker.snapshot().live_bytes, 4 * MIB);
        Ok(())
    }
}
