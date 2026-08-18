//! Bounded microbenchmark with machine-readable output.

use std::{
    error::Error,
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use async_trait::async_trait;
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{
    CorrelationId, MessageId, MessageMetadata, RetryDecision, RetryPolicy,
};
use plenora_runtime_scheduler::{
    FixedIntervalPlan, Schedule, ScheduleDispatchError, ScheduleDispatcher, ScheduleId,
    ScheduledOccurrence, SchedulerBuilder, SchedulerConfig,
};
use plenora_runtime_worker::{
    WorkerConcurrency, WorkerConfig, WorkerContext, WorkerExecutor, WorkerHandler,
};
use tokio::task::JoinSet;

const MAX_ITERATIONS: usize = 1_000_000;
const MAX_CONCURRENCY: usize = 4_096;

#[derive(Clone, Copy, Debug)]
struct NoRetry;

impl RetryPolicy<io::Error> for NoRetry {
    fn decide(&self, _attempt: u32, _error: &io::Error) -> RetryDecision {
        RetryDecision::DoNotRetry
    }
}

#[derive(Clone, Copy, Debug)]
struct ImmediateHandler;

#[async_trait]
impl WorkerHandler<()> for ImmediateHandler {
    type Error = io::Error;

    async fn handle(&self, _context: WorkerContext, _message: ()) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Debug, Default)]
struct CountingDispatcher {
    dispatched: AtomicU64,
}

#[async_trait]
impl ScheduleDispatcher<()> for CountingDispatcher {
    async fn dispatch(
        &self,
        _occurrence: ScheduledOccurrence<()>,
    ) -> Result<(), ScheduleDispatchError> {
        self.dispatched.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct BenchmarkConfig {
    iterations: usize,
    concurrency: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = parse_args()?;
    let worker_elapsed = benchmark_worker(config).await?;
    let scheduler_elapsed = benchmark_scheduler(config.iterations).await?;
    println!(
        "{{\"iterations\":{},\"max_in_flight\":{},\"worker_elapsed_ms\":{:.3},\"worker_jobs_per_second\":{:.3},\"scheduler_elapsed_ms\":{:.3},\"scheduler_dispatches_per_second\":{:.3}}}",
        config.iterations,
        config.concurrency,
        milliseconds(worker_elapsed),
        rate(config.iterations, worker_elapsed),
        milliseconds(scheduler_elapsed),
        rate(config.iterations, scheduler_elapsed),
    );
    Ok(())
}

async fn benchmark_worker(config: BenchmarkConfig) -> Result<Duration, Box<dyn Error>> {
    let runtime = RuntimeHandle::new(ServiceMetadata::new(
        "runtime-benchmark",
        "0.1.0",
        "benchmark",
    ));
    let executor = Arc::new(WorkerExecutor::new(
        ImmediateHandler,
        NoRetry,
        WorkerConfig::new(
            WorkerConcurrency::new(config.concurrency)?,
            Duration::from_secs(5),
        ),
    )?);
    let started_at = Instant::now();
    let mut started = 0_usize;
    let mut completed = 0_usize;
    let mut tasks = JoinSet::new();
    while completed < config.iterations {
        while started < config.iterations && tasks.len() < config.concurrency {
            let executor = Arc::clone(&executor);
            let context = WorkerContext::new(
                MessageId::random(),
                CorrelationId::random(),
                None,
                1,
                MessageMetadata::new(),
                runtime.shutdown_signal(),
            );
            tasks.spawn(async move { executor.execute(context, ()).await });
            started = started.saturating_add(1);
        }
        let result = tasks
            .join_next()
            .await
            .ok_or_else(|| io::Error::other("bounded worker task set ended early"))?;
        result.map_err(|error| io::Error::other(format!("worker task join failed: {error}")))??;
        completed = completed.saturating_add(1);
    }
    let elapsed = started_at.elapsed();
    if executor.in_flight() != 0 || !executor.task_control().active_tasks().is_empty() {
        return Err(io::Error::other("worker benchmark retained active state").into());
    }
    Ok(elapsed)
}

async fn benchmark_scheduler(iterations: usize) -> Result<Duration, Box<dyn Error>> {
    let dispatcher = Arc::new(CountingDispatcher::default());
    let mut builder = SchedulerBuilder::new(SchedulerConfig::new(
        Duration::from_secs(1),
        Duration::from_secs(5),
        Duration::MAX,
        1,
        iterations,
        iterations,
    )?);
    builder.register(Schedule::new(
        ScheduleId::new("benchmark")?,
        (),
        FixedIntervalPlan::new(SystemTime::UNIX_EPOCH, Duration::from_millis(1))?,
    ))?;
    let scheduler = builder.build(Arc::clone(&dispatcher));
    let final_offset = u64::try_from(iterations.saturating_sub(1))?;
    let started_at = Instant::now();
    let report = scheduler
        .tick(SystemTime::UNIX_EPOCH + Duration::from_millis(final_offset))
        .await;
    let elapsed = started_at.elapsed();
    if report.dispatched != iterations
        || dispatcher.dispatched.load(Ordering::Relaxed) != u64::try_from(iterations)?
    {
        return Err(
            io::Error::other("scheduler benchmark did not dispatch every occurrence").into(),
        );
    }
    Ok(elapsed)
}

fn parse_args() -> Result<BenchmarkConfig, io::Error> {
    let mut config = BenchmarkConfig {
        iterations: 20_000,
        concurrency: 32,
    };
    let mut arguments = std::env::args().skip(1);
    while let Some(flag) = arguments.next() {
        let value = arguments
            .next()
            .ok_or_else(|| io::Error::other("benchmark option requires a value"))?;
        match flag.as_str() {
            "--iterations" => {
                config.iterations = value
                    .parse()
                    .map_err(|_error| io::Error::other("iterations must be an integer"))?;
            }
            "--max-in-flight" => {
                config.concurrency = value
                    .parse()
                    .map_err(|_error| io::Error::other("max-in-flight must be an integer"))?;
            }
            _ => return Err(io::Error::other("unknown benchmark option")),
        }
    }
    if !(1..=MAX_ITERATIONS).contains(&config.iterations) {
        return Err(io::Error::other("iterations is outside the bounded range"));
    }
    if !(1..=MAX_CONCURRENCY).contains(&config.concurrency) {
        return Err(io::Error::other(
            "max-in-flight is outside the bounded range",
        ));
    }
    Ok(config)
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn rate(iterations: usize, duration: Duration) -> f64 {
    let iterations = u32::try_from(iterations).map_or(f64::from(u32::MAX), f64::from);
    iterations / duration.as_secs_f64().max(f64::EPSILON)
}
