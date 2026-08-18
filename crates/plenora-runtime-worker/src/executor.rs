use std::{
    fmt::{self, Debug, Formatter},
    future::{Future, pending},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use plenora_runtime_core::{Clock, SystemClock};
use plenora_runtime_messaging::RetryPolicy;
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore},
    time::{sleep, timeout},
};

use crate::active::ActiveTaskRegistry;
use crate::{
    NoopTaskLifecycleObserver, TaskCancellationReason, TaskCancellationToken,
    TaskLifecycleObserver, TaskProgressReporter, TaskState, WorkerAdmissionReason, WorkerConfig,
    WorkerConfigError, WorkerContext, WorkerExecutionError, WorkerHandler,
    WorkerInstanceHeartbeatObserver, WorkerInstanceHeartbeatReporter, WorkerInstanceIdentity,
    WorkerTaskControl,
};

/// Result of a bounded worker drain attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerDrainOutcome {
    /// Every admitted handler completed within the grace period.
    Completed,
    /// The grace period elapsed while handlers were still active.
    TimedOut {
        /// Number of handlers still active when the timeout was observed.
        remaining_in_flight: usize,
    },
}

#[derive(Debug)]
struct Lifecycle {
    admission: WorkerAdmissionState,
    in_flight: usize,
}

/// Current state of the worker admission gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerAdmissionState {
    /// New work may enter the bounded executor.
    Accepting,
    /// New work is temporarily rejected while active handlers may finish.
    Paused,
    /// Admission is permanently closed for coordinated drain.
    Draining,
}

/// Reversible worker-admission control used by resource and readiness monitors.
pub trait WorkerAdmissionControl: Send + Sync {
    /// Temporarily stops new work without cancelling active handlers.
    #[must_use]
    fn pause_admission(&self) -> bool;

    /// Reopens admission after a temporary pause.
    #[must_use]
    fn resume_admission(&self) -> bool;

    /// Returns the current admission state.
    #[must_use]
    fn admission_state(&self) -> WorkerAdmissionState;
}

/// Payload-free bounded worker capacity snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerCapacitySnapshot {
    /// Current reversible or terminal admission state.
    pub admission: WorkerAdmissionState,
    /// Configured maximum number of active handlers.
    pub capacity: usize,
    /// Currently active handlers.
    pub in_flight: usize,
    /// Capacity not occupied by active handlers.
    pub available: usize,
}

#[derive(Debug)]
struct ExecutorState {
    permits: Arc<Semaphore>,
    lifecycle: Mutex<Lifecycle>,
    in_flight_changed: Notify,
}

/// Cloneable admission and capacity handle that remains usable after a runner is moved.
#[derive(Clone)]
pub struct WorkerAdmissionHandle {
    capacity: usize,
    state: Arc<ExecutorState>,
}

impl WorkerAdmissionHandle {
    /// Returns a payload-free capacity snapshot.
    #[must_use]
    pub fn snapshot(&self) -> WorkerCapacitySnapshot {
        let lifecycle = self.state.lifecycle();
        WorkerCapacitySnapshot {
            admission: lifecycle.admission,
            capacity: self.capacity,
            in_flight: lifecycle.in_flight,
            available: self.capacity.saturating_sub(lifecycle.in_flight),
        }
    }

    /// Temporarily stops new work while allowing active handlers to finish.
    #[must_use]
    pub fn pause_admission(&self) -> bool {
        let mut lifecycle = self.state.lifecycle();
        if lifecycle.admission == WorkerAdmissionState::Accepting {
            lifecycle.admission = WorkerAdmissionState::Paused;
            true
        } else {
            false
        }
    }

    /// Reopens a paused gate. A draining gate remains terminal.
    #[must_use]
    pub fn resume_admission(&self) -> bool {
        let mut lifecycle = self.state.lifecycle();
        if lifecycle.admission == WorkerAdmissionState::Paused {
            lifecycle.admission = WorkerAdmissionState::Accepting;
            true
        } else {
            false
        }
    }

    /// Permanently closes admission and wakes capacity waiters.
    #[must_use]
    pub fn begin_drain(&self) -> bool {
        let changed = {
            let mut lifecycle = self.state.lifecycle();
            if lifecycle.admission == WorkerAdmissionState::Draining {
                false
            } else {
                lifecycle.admission = WorkerAdmissionState::Draining;
                true
            }
        };
        if changed {
            self.state.permits.close();
        }
        changed
    }
}

impl WorkerAdmissionControl for WorkerAdmissionHandle {
    fn pause_admission(&self) -> bool {
        Self::pause_admission(self)
    }

    fn resume_admission(&self) -> bool {
        Self::resume_admission(self)
    }

    fn admission_state(&self) -> WorkerAdmissionState {
        self.snapshot().admission
    }
}

impl Debug for WorkerAdmissionHandle {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerAdmissionHandle")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl ExecutorState {
    fn lifecycle(&self) -> MutexGuard<'_, Lifecycle> {
        match self.lifecycle.lock() {
            Ok(lifecycle) => lifecycle,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn try_start(
        self: &Arc<Self>,
        permit: OwnedSemaphorePermit,
    ) -> Result<InFlightGuard, OwnedSemaphorePermit> {
        let mut lifecycle = self.lifecycle();
        if lifecycle.admission != WorkerAdmissionState::Accepting {
            return Err(permit);
        }
        lifecycle.in_flight = lifecycle.in_flight.saturating_add(1);
        drop(lifecycle);

        Ok(InFlightGuard {
            state: Arc::clone(self),
            _permit: permit,
        })
    }

    fn finish(&self) {
        {
            let mut lifecycle = self.lifecycle();
            lifecycle.in_flight = lifecycle.in_flight.saturating_sub(1);
        }
        self.in_flight_changed.notify_waiters();
    }

    async fn wait_until_idle(&self) {
        loop {
            let changed = self.in_flight_changed.notified();
            if self.lifecycle().in_flight == 0 {
                return;
            }
            changed.await;
        }
    }
}

struct InFlightGuard {
    state: Arc<ExecutorState>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.state.finish();
    }
}

/// Bounded engine-neutral coordinator for typed worker handler invocations.
///
/// The executor owns no queue and spawns no tasks. Callers and concrete engine adapters drive its
/// execute future, while an internal semaphore guarantees the configured in-flight bound.
pub struct WorkerExecutor<H, P> {
    handler: Arc<H>,
    retry_policy: Arc<P>,
    config: WorkerConfig,
    state: Arc<ExecutorState>,
    lifecycle_observer: Arc<dyn TaskLifecycleObserver>,
    clock: Arc<dyn Clock>,
    task_control: WorkerTaskControl,
}

impl<H, P> WorkerExecutor<H, P> {
    /// Creates an executor after validating all worker bounds.
    ///
    /// # Errors
    ///
    /// Returns an error for zero concurrency or a zero drain grace period.
    pub fn new(
        handler: H,
        retry_policy: P,
        config: WorkerConfig,
    ) -> Result<Self, WorkerConfigError> {
        Self::from_shared(Arc::new(handler), Arc::new(retry_policy), config)
    }

    /// Creates an executor from shared handler and policy instances.
    ///
    /// # Errors
    ///
    /// Returns an error for zero concurrency or a zero drain grace period.
    pub fn from_shared(
        handler: Arc<H>,
        retry_policy: Arc<P>,
        config: WorkerConfig,
    ) -> Result<Self, WorkerConfigError> {
        Self::from_shared_with_lifecycle(
            handler,
            retry_policy,
            config,
            Arc::new(NoopTaskLifecycleObserver),
            Arc::new(SystemClock),
        )
    }

    /// Creates an executor with an explicit non-blocking lifecycle observer and clock.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid worker bounds or zero timeouts.
    pub fn with_lifecycle_observer<O, C>(
        handler: H,
        retry_policy: P,
        config: WorkerConfig,
        lifecycle_observer: Arc<O>,
        clock: Arc<C>,
    ) -> Result<Self, WorkerConfigError>
    where
        O: TaskLifecycleObserver + 'static,
        C: Clock + 'static,
    {
        Self::from_shared_with_lifecycle(
            Arc::new(handler),
            Arc::new(retry_policy),
            config,
            lifecycle_observer,
            clock,
        )
    }

    /// Creates an executor from shared handler, retry, lifecycle, and clock components.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid worker bounds or zero timeouts.
    pub fn from_shared_with_lifecycle<O, C>(
        handler: Arc<H>,
        retry_policy: Arc<P>,
        config: WorkerConfig,
        lifecycle_observer: Arc<O>,
        clock: Arc<C>,
    ) -> Result<Self, WorkerConfigError>
    where
        O: TaskLifecycleObserver + 'static,
        C: Clock + 'static,
    {
        config.validate()?;
        let active_tasks = ActiveTaskRegistry::new(config.concurrency.max_in_flight);
        Ok(Self {
            handler,
            retry_policy,
            config,
            state: Arc::new(ExecutorState {
                permits: Arc::new(Semaphore::new(config.concurrency.max_in_flight)),
                lifecycle: Mutex::new(Lifecycle {
                    admission: WorkerAdmissionState::Accepting,
                    in_flight: 0,
                }),
                in_flight_changed: Notify::new(),
            }),
            lifecycle_observer,
            clock,
            task_control: WorkerTaskControl::new(active_tasks),
        })
    }

    /// Returns the validated worker configuration.
    #[must_use]
    pub const fn config(&self) -> WorkerConfig {
        self.config
    }

    /// Returns whether new jobs may still be admitted.
    #[must_use]
    pub fn is_accepting(&self) -> bool {
        self.admission_state() == WorkerAdmissionState::Accepting
    }

    /// Returns the current reversible/permanent admission state.
    #[must_use]
    pub fn admission_state(&self) -> WorkerAdmissionState {
        self.state.lifecycle().admission
    }

    /// Temporarily stops new work without cancelling active handlers.
    ///
    /// Returns true only when this call changes accepting admission to paused admission. A worker
    /// that has begun draining can never be resumed.
    #[must_use]
    pub fn pause_admission(&self) -> bool {
        let mut lifecycle = self.state.lifecycle();
        if lifecycle.admission == WorkerAdmissionState::Accepting {
            lifecycle.admission = WorkerAdmissionState::Paused;
            true
        } else {
            false
        }
    }

    /// Reopens admission after a temporary pause.
    ///
    /// Returns true only when this call changes paused admission to accepting admission.
    #[must_use]
    pub fn resume_admission(&self) -> bool {
        let mut lifecycle = self.state.lifecycle();
        if lifecycle.admission == WorkerAdmissionState::Paused {
            lifecycle.admission = WorkerAdmissionState::Accepting;
            true
        } else {
            false
        }
    }

    /// Returns the number of handlers currently executing.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.state.lifecycle().in_flight
    }

    /// Returns a cloneable, engine-neutral handle for listing and cancelling active tasks.
    #[must_use]
    pub fn task_control(&self) -> WorkerTaskControl {
        self.task_control.clone()
    }

    /// Returns a cloneable admission and capacity handle.
    #[must_use]
    pub fn admission_control(&self) -> WorkerAdmissionHandle {
        WorkerAdmissionHandle {
            capacity: self.config.concurrency.max_in_flight,
            state: Arc::clone(&self.state),
        }
    }

    /// Creates a reporter that samples this executor's live bounded capacity.
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
        let state = Arc::clone(&self.state);
        let sample_in_flight: Arc<dyn Fn() -> usize + Send + Sync> =
            Arc::new(move || state.lifecycle().in_flight);
        let observer: Arc<dyn WorkerInstanceHeartbeatObserver> = observer;
        let clock: Arc<dyn Clock> = clock;
        WorkerInstanceHeartbeatReporter::new(
            identity,
            self.config.concurrency.max_in_flight,
            sample_in_flight,
            observer,
            clock,
        )
    }

    /// Stops admission and wakes jobs waiting for capacity.
    ///
    /// Returns true only for the caller that transitions the executor into drain.
    #[must_use]
    pub fn begin_drain(&self) -> bool {
        let changed = {
            let mut lifecycle = self.state.lifecycle();
            if lifecycle.admission == WorkerAdmissionState::Draining {
                false
            } else {
                lifecycle.admission = WorkerAdmissionState::Draining;
                true
            }
        };

        if changed {
            self.state.permits.close();
        }
        changed
    }

    /// Stops admission and waits for active handlers up to the configured grace period.
    pub async fn drain(&self) -> WorkerDrainOutcome {
        let _started = self.begin_drain();

        if self.in_flight() == 0 {
            return WorkerDrainOutcome::Completed;
        }

        if timeout(
            self.config.shutdown_grace_period,
            self.state.wait_until_idle(),
        )
        .await
        .is_ok()
        {
            return WorkerDrainOutcome::Completed;
        }

        let remaining_in_flight = self.in_flight();
        if remaining_in_flight == 0 {
            WorkerDrainOutcome::Completed
        } else {
            WorkerDrainOutcome::TimedOut {
                remaining_in_flight,
            }
        }
    }

    /// Executes one job under the configured concurrency bound.
    ///
    /// Jobs waiting for capacity are rejected when drain or runtime shutdown begins. Handler
    /// errors preserve their source and carry the decision returned by the injected retry policy.
    ///
    /// # Errors
    ///
    /// Returns an admission error if shutdown or drain prevents handler invocation, or a
    /// structured handler error after delegating to the retry policy.
    pub async fn execute<T>(
        &self,
        mut ctx: WorkerContext,
        message: T,
    ) -> Result<(), WorkerExecutionError<H::Error>>
    where
        T: Send,
        H: WorkerHandler<T>,
        P: RetryPolicy<H::Error>,
    {
        let lifecycle = ctx.install_lifecycle(
            Arc::clone(&self.lifecycle_observer),
            Arc::clone(&self.clock),
        );
        let cancellation = ctx.cancellation.clone();
        let mut execution_guard = ExecutionGuard::new(cancellation.clone(), lifecycle.clone());
        let permit = match self
            .acquire_execution_permit(&ctx, &cancellation, &lifecycle)
            .await
        {
            Ok(permit) => permit,
            Err(error) => {
                execution_guard.complete();
                return Err(error);
            }
        };

        let _in_flight = match self.state.try_start(permit) {
            Ok(in_flight) => in_flight,
            Err(_permit) => {
                let reason = match self.admission_state() {
                    WorkerAdmissionState::Paused => WorkerAdmissionReason::Paused,
                    WorkerAdmissionState::Accepting | WorkerAdmissionState::Draining => {
                        WorkerAdmissionReason::Draining
                    }
                };
                finish_admission(&cancellation, &lifecycle, reason);
                execution_guard.complete();
                return Err(WorkerExecutionError::admission(reason));
            }
        };

        let _active_task = match self.task_control.register(
            ctx.message_id,
            ctx.correlation_id,
            ctx.attempt,
            self.clock.now(),
            cancellation.clone(),
        ) {
            Ok(active_task) => active_task,
            Err(_error) => {
                let reason = WorkerAdmissionReason::ControlCapacityUnavailable;
                finish_admission(&cancellation, &lifecycle, reason);
                execution_guard.complete();
                return Err(WorkerExecutionError::admission(reason));
            }
        };

        self.run_handler(
            ctx,
            message,
            &cancellation,
            &lifecycle,
            &mut execution_guard,
        )
        .await
    }

    async fn acquire_execution_permit<E>(
        &self,
        ctx: &WorkerContext,
        cancellation: &TaskCancellationToken,
        lifecycle: &TaskProgressReporter,
    ) -> Result<OwnedSemaphorePermit, WorkerExecutionError<E>> {
        if let Some(reason) = self.admission_rejection(ctx) {
            finish_admission(cancellation, lifecycle, reason);
            return Err(WorkerExecutionError::admission(reason));
        }

        let permits = Arc::clone(&self.state.permits);
        let permit = tokio::select! {
            biased;
            () = ctx.shutdown.cancelled() => {
                let reason = WorkerAdmissionReason::ShutdownRequested;
                finish_admission(cancellation, lifecycle, reason);
                return Err(WorkerExecutionError::admission(reason));
            }
            reason = cancellation.cancelled() => {
                lifecycle.transition(TaskState::Cancelled(reason));
                return Err(WorkerExecutionError::cancelled(reason));
            }
            permit = permits.acquire_owned() => {
                let Ok(permit) = permit else {
                    let reason = match self.admission_state() {
                        WorkerAdmissionState::Paused => WorkerAdmissionReason::Paused,
                        WorkerAdmissionState::Accepting | WorkerAdmissionState::Draining => {
                            WorkerAdmissionReason::Draining
                        }
                    };
                    finish_admission(cancellation, lifecycle, reason);
                    return Err(WorkerExecutionError::admission(reason));
                };
                permit
            }
        };

        if ctx.shutdown.is_cancelled() {
            let reason = WorkerAdmissionReason::ShutdownRequested;
            finish_admission(cancellation, lifecycle, reason);
            return Err(WorkerExecutionError::admission(reason));
        }
        if let Some(reason) = cancellation.reason() {
            lifecycle.transition(TaskState::Cancelled(reason));
            return Err(WorkerExecutionError::cancelled(reason));
        }
        Ok(permit)
    }

    async fn run_handler<T>(
        &self,
        ctx: WorkerContext,
        message: T,
        cancellation: &TaskCancellationToken,
        lifecycle: &TaskProgressReporter,
        execution_guard: &mut ExecutionGuard,
    ) -> Result<(), WorkerExecutionError<H::Error>>
    where
        T: Send,
        H: WorkerHandler<T>,
        P: RetryPolicy<H::Error>,
    {
        let attempt = ctx.attempt;
        lifecycle.transition(TaskState::Running);
        let handler = self.handler.handle(ctx, message);
        tokio::pin!(handler);
        let deadline = wait_optional(self.config.execution_timeout);
        tokio::pin!(deadline);

        loop {
            let heartbeat = wait_optional(self.config.lifecycle_heartbeat_interval);
            tokio::pin!(heartbeat);
            tokio::select! {
                biased;
                result = &mut handler => {
                    if let Some(reason) = cancellation.reason() {
                        lifecycle.transition(TaskState::Cancelled(reason));
                        execution_guard.complete();
                        return Err(WorkerExecutionError::cancelled(reason));
                    }
                    execution_guard.complete();
                    return match result {
                        Ok(()) => {
                            lifecycle.transition(TaskState::Succeeded);
                            Ok(())
                        }
                        Err(source) => {
                            lifecycle.transition(TaskState::Failed);
                            let retry_decision = self.retry_policy.decide(attempt, &source);
                            Err(WorkerExecutionError::handler(source, retry_decision))
                        }
                    };
                }
                reason = cancellation.cancelled() => {
                    wait_for_cleanup(
                        handler.as_mut(),
                        self.config.task_cancellation_grace_period,
                    ).await;
                    lifecycle.transition(TaskState::Cancelled(reason));
                    execution_guard.complete();
                    return Err(WorkerExecutionError::cancelled(reason));
                }
                timeout = &mut deadline => {
                    let _started = cancellation.cancel(TaskCancellationReason::Timeout);
                    wait_for_cleanup(
                        handler.as_mut(),
                        self.config.task_cancellation_grace_period,
                    ).await;
                    lifecycle.transition(TaskState::TimedOut);
                    execution_guard.complete();
                    return Err(WorkerExecutionError::timed_out(
                        timeout,
                        self.config.timeout_retry_decision,
                    ));
                }
                _interval = &mut heartbeat => {
                    let _heartbeat = lifecycle.heartbeat();
                }
            }
        }
    }

    fn admission_rejection(&self, ctx: &WorkerContext) -> Option<WorkerAdmissionReason> {
        if ctx.shutdown.is_cancelled() {
            Some(WorkerAdmissionReason::ShutdownRequested)
        } else {
            match self.admission_state() {
                WorkerAdmissionState::Accepting => None,
                WorkerAdmissionState::Paused => Some(WorkerAdmissionReason::Paused),
                WorkerAdmissionState::Draining => Some(WorkerAdmissionReason::Draining),
            }
        }
    }
}

impl<H, P> Clone for WorkerExecutor<H, P> {
    fn clone(&self) -> Self {
        Self {
            handler: Arc::clone(&self.handler),
            retry_policy: Arc::clone(&self.retry_policy),
            config: self.config,
            state: Arc::clone(&self.state),
            lifecycle_observer: Arc::clone(&self.lifecycle_observer),
            clock: Arc::clone(&self.clock),
            task_control: self.task_control.clone(),
        }
    }
}

struct ExecutionGuard {
    cancellation: TaskCancellationToken,
    lifecycle: TaskProgressReporter,
    completed: bool,
}

impl ExecutionGuard {
    fn new(cancellation: TaskCancellationToken, lifecycle: TaskProgressReporter) -> Self {
        Self {
            cancellation,
            lifecycle,
            completed: false,
        }
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let reason = if let Some(reason) = self.cancellation.reason() {
            reason
        } else {
            let _started = self
                .cancellation
                .cancel(TaskCancellationReason::ExecutionDropped);
            TaskCancellationReason::ExecutionDropped
        };
        self.lifecycle.transition(TaskState::Cancelled(reason));
    }
}

fn finish_admission(
    token: &TaskCancellationToken,
    lifecycle: &TaskProgressReporter,
    reason: WorkerAdmissionReason,
) {
    let cancellation = match reason {
        WorkerAdmissionReason::Draining => TaskCancellationReason::WorkerDraining,
        WorkerAdmissionReason::ShutdownRequested => TaskCancellationReason::RuntimeShutdown,
        WorkerAdmissionReason::ControlCapacityUnavailable => {
            TaskCancellationReason::ControlCapacityUnavailable
        }
        WorkerAdmissionReason::Paused => TaskCancellationReason::AdmissionPaused,
    };
    let _started = token.cancel(cancellation);
    lifecycle.transition(TaskState::Cancelled(cancellation));
}

async fn wait_optional(duration: Option<Duration>) -> Duration {
    match duration {
        Some(duration) => {
            sleep(duration).await;
            duration
        }
        None => pending::<Duration>().await,
    }
}

async fn wait_for_cleanup<F>(handler: Pin<&mut F>, grace_period: Duration)
where
    F: Future,
{
    let _completed = timeout(grace_period, handler).await;
}

impl<H, P> Debug for WorkerExecutor<H, P> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerExecutor")
            .field("config", &self.config)
            .field("accepting", &self.is_accepting())
            .field("admission_state", &self.admission_state())
            .field("in_flight", &self.in_flight())
            .field("active_tasks", &self.task_control.active_tasks().len())
            .finish_non_exhaustive()
    }
}

impl<H, P> WorkerAdmissionControl for WorkerExecutor<H, P>
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
