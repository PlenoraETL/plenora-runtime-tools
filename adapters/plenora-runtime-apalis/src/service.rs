use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use apalis::prelude::{Error as ApalisError, Request};
use plenora_runtime_core::Clock;
use plenora_runtime_messaging::RetryPolicy;
use plenora_runtime_worker::{
    TaskLifecycleObserver, WorkerAdmissionControl, WorkerAdmissionHandle, WorkerAdmissionState,
    WorkerDrainOutcome, WorkerExecutor, WorkerHandler, WorkerInstanceHeartbeatObserver,
    WorkerInstanceHeartbeatReporter, WorkerInstanceIdentity, WorkerTaskControl,
};
use tower::Service;

use crate::{ApalisAdapterConfig, ApalisAdapterConfigError, ApalisExecutionOutcome, ApalisJob};

type BridgeFuture<E, EngineError> =
    Pin<Box<dyn Future<Output = Result<ApalisExecutionOutcome<E>, EngineError>> + Send>>;

/// Cloneable Tower service that delegates typed jobs to a Plenora worker executor.
///
/// The service contains Apalis only at its adapter-specific Tower implementation boundary. Its
/// constructors, configuration, job, outcome, and shutdown methods use Plenora-owned types.
pub struct ApalisWorkerService<H, P> {
    config: ApalisAdapterConfig,
    executor: WorkerExecutor<H, P>,
}

impl<H, P> ApalisWorkerService<H, P> {
    /// Creates a service bridge and its bounded Plenora executor.
    ///
    /// # Errors
    ///
    /// Returns an error when adapter or worker configuration is invalid.
    pub fn new(
        handler: H,
        retry_policy: P,
        config: ApalisAdapterConfig,
    ) -> Result<Self, ApalisAdapterConfigError> {
        config.validate()?;
        let executor = WorkerExecutor::new(handler, retry_policy, config.worker())?;
        Ok(Self { config, executor })
    }

    /// Creates a service with an explicit lifecycle observer and deterministic clock.
    ///
    /// # Errors
    ///
    /// Returns an error when adapter or worker configuration is invalid.
    pub fn new_with_lifecycle<O, C>(
        handler: H,
        retry_policy: P,
        config: ApalisAdapterConfig,
        lifecycle_observer: Arc<O>,
        clock: Arc<C>,
    ) -> Result<Self, ApalisAdapterConfigError>
    where
        O: TaskLifecycleObserver + 'static,
        C: Clock + 'static,
    {
        config.validate()?;
        let executor = WorkerExecutor::with_lifecycle_observer(
            handler,
            retry_policy,
            config.worker(),
            lifecycle_observer,
            clock,
        )?;
        Ok(Self { config, executor })
    }

    /// Returns validated adapter configuration.
    #[must_use]
    pub const fn config(&self) -> &ApalisAdapterConfig {
        &self.config
    }

    /// Returns whether the service still accepts jobs.
    #[must_use]
    pub fn is_accepting(&self) -> bool {
        self.executor.is_accepting()
    }

    /// Returns the current reversible/permanent worker admission state.
    #[must_use]
    pub fn admission_state(&self) -> WorkerAdmissionState {
        self.executor.admission_state()
    }

    /// Temporarily stops admission without cancelling active handlers.
    #[must_use]
    pub fn pause_admission(&self) -> bool {
        self.executor.pause_admission()
    }

    /// Reopens admission after a temporary pause.
    #[must_use]
    pub fn resume_admission(&self) -> bool {
        self.executor.resume_admission()
    }

    /// Returns the number of Plenora handlers currently executing.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.executor.in_flight()
    }

    /// Returns a cloneable handle for listing and cancelling active handler invocations.
    #[must_use]
    pub fn task_control(&self) -> WorkerTaskControl {
        self.executor.task_control()
    }

    /// Returns a cloneable admission and capacity handle.
    #[must_use]
    pub fn admission_control(&self) -> WorkerAdmissionHandle {
        self.executor.admission_control()
    }

    /// Creates a worker-instance reporter backed by the executor's live capacity counters.
    #[must_use]
    pub fn instance_heartbeat_reporter<O, C>(
        &self,
        identity: WorkerInstanceIdentity,
        observer: Arc<O>,
        clock: Arc<C>,
    ) -> WorkerInstanceHeartbeatReporter
    where
        O: WorkerInstanceHeartbeatObserver + 'static,
        C: Clock + 'static,
    {
        self.executor
            .instance_heartbeat_reporter(identity, observer, clock)
    }

    /// Stops admission without cancelling handlers already executing.
    #[must_use]
    pub fn begin_drain(&self) -> bool {
        self.executor.begin_drain()
    }

    /// Waits for admitted handlers up to the configured grace period.
    pub async fn drain(&self) -> WorkerDrainOutcome {
        self.executor.drain().await
    }

    /// Executes one adapter job without exposing Tower or Apalis request types.
    pub async fn execute<T>(&self, job: ApalisJob<T>) -> ApalisExecutionOutcome<H::Error>
    where
        T: Send,
        H: WorkerHandler<T>,
        P: RetryPolicy<H::Error>,
    {
        let (context, message) = job.into_parts();
        ApalisExecutionOutcome::from_worker_result(self.executor.execute(context, message).await)
    }
}

impl<H, P> Clone for ApalisWorkerService<H, P> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            executor: self.executor.clone(),
        }
    }
}

impl<H, P> WorkerAdmissionControl for ApalisWorkerService<H, P>
where
    H: Send + Sync,
    P: Send + Sync,
{
    fn pause_admission(&self) -> bool {
        Self::pause_admission(self)
    }

    fn resume_admission(&self) -> bool {
        Self::resume_admission(self)
    }

    fn admission_state(&self) -> WorkerAdmissionState {
        Self::admission_state(self)
    }
}

impl<T, H, P> Service<ApalisJob<T>> for ApalisWorkerService<H, P>
where
    T: Send + 'static,
    H: WorkerHandler<T> + 'static,
    P: RetryPolicy<H::Error> + 'static,
{
    type Response = ApalisExecutionOutcome<H::Error>;
    type Error = Infallible;
    type Future = BridgeFuture<H::Error, Self::Error>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, job: ApalisJob<T>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move { Ok(service.execute(job).await) })
    }
}

impl<T, H, P> Service<Request<ApalisJob<T>, ()>> for ApalisWorkerService<H, P>
where
    T: Send + 'static,
    H: WorkerHandler<T> + 'static,
    P: RetryPolicy<H::Error> + 'static,
{
    type Response = ApalisExecutionOutcome<H::Error>;
    type Error = ApalisError;
    type Future = BridgeFuture<H::Error, Self::Error>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<ApalisJob<T>, ()>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move { Ok(service.execute(request.args).await) })
    }
}

impl<H, P> std::fmt::Debug for ApalisWorkerService<H, P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApalisWorkerService")
            .field("config", &self.config)
            .field("accepting", &self.is_accepting())
            .field("admission_state", &self.admission_state())
            .field("in_flight", &self.in_flight())
            .finish_non_exhaustive()
    }
}
