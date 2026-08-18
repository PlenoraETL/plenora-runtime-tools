//! Repeated lifecycle and concurrent cancellation stability coverage.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{
    CorrelationId, MessageId, MessageMetadata, RetryDecision, RetryPolicy,
};
use plenora_runtime_worker::{
    TaskCancellationReason, WorkerConcurrency, WorkerConfig, WorkerContext, WorkerDrainOutcome,
    WorkerExecutor, WorkerHandler,
};
use tokio::{sync::mpsc, time::timeout};

const CANCELLATION_TASKS: usize = 64;
const LIFECYCLE_CYCLES: usize = 100;
const TASKS_PER_CYCLE: usize = 8;

#[derive(Clone, Copy, Debug)]
struct StabilityError(&'static str);

impl Display for StabilityError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for StabilityError {}

#[derive(Clone, Copy, Debug)]
struct NoRetry;

impl RetryPolicy<StabilityError> for NoRetry {
    fn decide(&self, _attempt: u32, _error: &StabilityError) -> RetryDecision {
        RetryDecision::DoNotRetry
    }
}

#[derive(Debug)]
struct CancellationHandler {
    started: mpsc::Sender<()>,
}

#[async_trait]
impl WorkerHandler<()> for CancellationHandler {
    type Error = StabilityError;

    async fn handle(&self, context: WorkerContext, _message: ()) -> Result<(), Self::Error> {
        self.started
            .send(())
            .await
            .map_err(|_error| StabilityError("start observer closed"))?;
        let _reason = context.cancellation.cancelled().await;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct ImmediateHandler;

#[async_trait]
impl WorkerHandler<()> for ImmediateHandler {
    type Error = StabilityError;

    async fn handle(&self, _context: WorkerContext, _message: ()) -> Result<(), Self::Error> {
        tokio::task::yield_now().await;
        Ok(())
    }
}

#[tokio::test]
async fn concurrent_cancellation_storm_leaves_no_active_task_or_permit_leak()
-> Result<(), Box<dyn Error>> {
    let runtime = runtime("cancellation-storm");
    let (started_tx, mut started_rx) = mpsc::channel(CANCELLATION_TASKS);
    let executor = Arc::new(WorkerExecutor::new(
        CancellationHandler {
            started: started_tx,
        },
        NoRetry,
        worker_config(CANCELLATION_TASKS)?,
    )?);
    let control = executor.task_control();
    let mut executions = Vec::with_capacity(CANCELLATION_TASKS);
    for _index in 0..CANCELLATION_TASKS {
        let executor = Arc::clone(&executor);
        let context = context(&runtime, 1);
        executions.push(tokio::spawn(
            async move { executor.execute(context, ()).await },
        ));
    }

    timeout(Duration::from_secs(10), async {
        for _started in 0..CANCELLATION_TASKS {
            started_rx
                .recv()
                .await
                .ok_or(StabilityError("handler did not start"))?;
        }
        Ok::<(), StabilityError>(())
    })
    .await
    .map_err(|_elapsed| StabilityError("cancellation admission timed out"))??;
    let active = control.active_tasks();
    assert_eq!(active.len(), CANCELLATION_TASKS);
    for task in active {
        assert_eq!(
            control.request_cancellation(task.task_id),
            plenora_runtime_worker::WorkerTaskCancellationOutcome::Requested
        );
    }

    timeout(Duration::from_secs(10), async {
        for execution in executions {
            let result = execution
                .await
                .map_err(|_error| StabilityError("execution task failed to join"))?;
            let reason = result
                .err()
                .and_then(|error| error.cancellation_reason())
                .ok_or(StabilityError("cancelled execution returned no reason"))?;
            if reason != TaskCancellationReason::Requested {
                return Err(StabilityError("unexpected cancellation reason"));
            }
        }
        Ok::<(), StabilityError>(())
    })
    .await
    .map_err(|_elapsed| StabilityError("cancellation completion timed out"))??;

    assert!(control.active_tasks().is_empty());
    assert_eq!(executor.in_flight(), 0);
    assert_eq!(executor.drain().await, WorkerDrainOutcome::Completed);
    Ok(())
}

#[tokio::test]
async fn one_hundred_executor_lifecycles_complete_without_residual_state()
-> Result<(), Box<dyn Error>> {
    timeout(Duration::from_secs(20), async {
        for cycle in 0..LIFECYCLE_CYCLES {
            let runtime = runtime(&format!("cycle-{cycle}"));
            let executor = Arc::new(WorkerExecutor::new(
                ImmediateHandler,
                NoRetry,
                worker_config(TASKS_PER_CYCLE)?,
            )?);
            let control = executor.task_control();
            let mut executions = Vec::with_capacity(TASKS_PER_CYCLE);
            for _index in 0..TASKS_PER_CYCLE {
                let executor = Arc::clone(&executor);
                let context = context(&runtime, 1);
                executions.push(tokio::spawn(
                    async move { executor.execute(context, ()).await },
                ));
            }
            for execution in executions {
                execution
                    .await
                    .map_err(|_error| StabilityError("execution task failed to join"))??;
            }
            if executor.in_flight() != 0 || !control.active_tasks().is_empty() {
                return Err(
                    Box::new(StabilityError("executor retained residual state")) as Box<dyn Error>
                );
            }
            if executor.drain().await != WorkerDrainOutcome::Completed {
                return Err(
                    Box::new(StabilityError("executor drain did not complete")) as Box<dyn Error>
                );
            }
        }
        Ok::<(), Box<dyn Error>>(())
    })
    .await
    .map_err(|_elapsed| StabilityError("repeated lifecycle test timed out"))??;
    Ok(())
}

fn worker_config(max_in_flight: usize) -> Result<WorkerConfig, Box<dyn Error>> {
    Ok(WorkerConfig::new(
        WorkerConcurrency::new(max_in_flight)?,
        Duration::from_secs(2),
    ))
}

fn runtime(instance: &str) -> RuntimeHandle {
    RuntimeHandle::new(ServiceMetadata::new(
        "worker-stability-test",
        "0.1.0",
        instance.to_owned(),
    ))
}

fn context(runtime: &RuntimeHandle, attempt: u32) -> WorkerContext {
    WorkerContext::new(
        MessageId::random(),
        CorrelationId::random(),
        None,
        attempt,
        MessageMetadata::new(),
        runtime.shutdown_signal(),
    )
}
