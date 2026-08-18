//! Bounded, engine-neutral Plenora worker example.

#![forbid(unsafe_code)]

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata, SystemClock};
use plenora_runtime_messaging::{
    ClassifyRetry, CorrelationId, ExponentialBackoff, ExponentialBackoffConfig, MessageId,
    MessageMetadata, RetryErrorClass, RetryExhaustedAction,
};
use plenora_runtime_worker::{
    WorkerConcurrency, WorkerConfig, WorkerContext, WorkerExecutor, WorkerHandler,
    WorkerLifecycleChannelConfig, WorkerLifecycleDispatcher, WorkerLifecycleHealthCriticality,
    WorkerLifecycleHealthReporter, WorkerLifecycleObservation,
};

#[derive(Clone, Copy, Debug)]
struct ExampleHandler;

#[derive(Clone, Copy, Debug)]
struct ExampleHandlerError;

impl Display for ExampleHandlerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("example handler rejected the message")
    }
}

impl Error for ExampleHandlerError {}

impl ClassifyRetry for ExampleHandlerError {
    fn retry_class(&self) -> RetryErrorClass {
        RetryErrorClass::Retryable
    }
}

#[async_trait]
impl WorkerHandler<String> for ExampleHandler {
    type Error = ExampleHandlerError;

    async fn handle(&self, context: WorkerContext, message: String) -> Result<(), Self::Error> {
        if context.shutdown.is_cancelled() || message.trim().is_empty() {
            return Err(ExampleHandlerError);
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(ServiceMetadata::new(
        "worker-basic",
        env!("CARGO_PKG_VERSION"),
        "local-example",
    ));
    let retry_policy = ExponentialBackoff::new(ExponentialBackoffConfig {
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(2),
        multiplier: 2,
        max_attempts: 3,
        max_elapsed: Some(Duration::from_secs(5)),
        jitter: None,
        retry_unknown_outcome: false,
        exhausted_action: RetryExhaustedAction::DeadLetter,
    })?;
    let worker_config = WorkerConfig::new(WorkerConcurrency::new(4)?, Duration::from_secs(5));
    let (lifecycle_dispatcher, mut lifecycle_receiver) =
        WorkerLifecycleDispatcher::channel(WorkerLifecycleChannelConfig::new(16)?);
    let lifecycle_health = WorkerLifecycleHealthReporter::new(
        runtime.health_registry(),
        "worker.lifecycle",
        WorkerLifecycleHealthCriticality::Optional,
    );
    let executor = WorkerExecutor::with_lifecycle_observer(
        ExampleHandler,
        retry_policy,
        worker_config,
        Arc::new(lifecycle_dispatcher.clone()),
        Arc::new(SystemClock),
    )?;

    executor
        .execute(
            worker_context(&runtime, 1),
            String::from("bounded example job"),
        )
        .await?;
    println!("worker job completed");

    if let Err(failure) = executor
        .execute(worker_context(&runtime, 1), String::new())
        .await
    {
        println!(
            "worker failure classified: category={:?}, retry={:?}",
            failure.category(),
            failure.retry_decision()
        );
    }

    let before_drain = lifecycle_dispatcher.snapshot();
    lifecycle_health.refresh(before_drain);
    lifecycle_receiver.close();
    let mut task_observations = 0_u64;
    while let Some(observation) = lifecycle_receiver.recv().await {
        if matches!(observation, WorkerLifecycleObservation::Task(_)) {
            task_observations = task_observations.saturating_add(1);
        }
    }
    let after_drain = lifecycle_dispatcher.snapshot();
    lifecycle_health.refresh(after_drain);
    println!(
        "lifecycle handoff: accepted={}, delivered={}, task_events={}, dropped_full={}",
        before_drain.accepted, after_drain.delivered, task_observations, after_drain.dropped_full
    );
    println!(
        "lifecycle health={:?}, readiness={:?}",
        runtime.health_registry().health().status,
        runtime.health_registry().readiness().status
    );

    let _shutdown_started = runtime.request_shutdown();
    let worker_drain = executor.drain().await;
    let runtime_drain = runtime.shutdown().await;
    println!("shutdown completed: worker={worker_drain:?}, runtime={runtime_drain:?}");

    Ok(())
}

fn worker_context(runtime: &RuntimeHandle, attempt: u32) -> WorkerContext {
    WorkerContext::new(
        MessageId::random(),
        CorrelationId::random(),
        None,
        attempt,
        MessageMetadata::new(),
        runtime.shutdown_signal(),
    )
}
