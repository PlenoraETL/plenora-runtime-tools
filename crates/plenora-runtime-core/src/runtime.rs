use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    fmt::{self, Debug, Formatter},
    future::Future,
    sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::Duration,
};

use tokio::{
    runtime::Handle,
    sync::{Notify, oneshot},
    task::AbortHandle,
    time::timeout,
};

use crate::{
    ComponentHealth, ComponentReadiness, HealthRegistry, HealthStatus, OptionalTaskFailurePolicy,
    ReadinessStatus, ShutdownSignal, SpawnError, TaskCompletion, TaskCriticality, TaskFailure,
    TaskOutcome, TaskReport, TaskSpec,
    shutdown::{ShutdownController, shutdown_channel},
};

/// Stable process identity propagated through runtime contexts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceMetadata {
    /// Logical service name.
    pub service_name: Arc<str>,
    /// Service build or release version.
    pub service_version: Arc<str>,
    /// Unique process instance identifier.
    pub instance_id: Arc<str>,
    /// Optional deployment environment name.
    pub environment: Option<Arc<str>>,
}

impl ServiceMetadata {
    /// Creates service metadata without an environment label.
    #[must_use]
    pub fn new(
        service_name: impl Into<Arc<str>>,
        service_version: impl Into<Arc<str>>,
        instance_id: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            service_name: service_name.into(),
            service_version: service_version.into(),
            instance_id: instance_id.into(),
            environment: None,
        }
    }

    /// Adds a deployment environment label.
    #[must_use]
    pub fn with_environment(mut self, environment: impl Into<Arc<str>>) -> Self {
        self.environment = Some(environment.into());
        self
    }
}

/// Context shared with runtime-managed services and handlers.
#[derive(Clone, Debug)]
pub struct RuntimeContext {
    /// Stable process identity.
    pub metadata: ServiceMetadata,
    /// Cooperative process shutdown signal.
    pub shutdown: ShutdownSignal,
}

/// Current process lifecycle phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimePhase {
    /// New tasks may be accepted.
    Running,
    /// Shutdown was requested and new tasks are rejected.
    Draining,
    /// The configured drain attempt has finished or timed out.
    Stopped,
}

/// Configurable process lifecycle behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeConfig {
    /// Maximum time allowed for supervised tasks to drain.
    pub shutdown_grace_period: Duration,
    /// Reaction to failure of an optional task.
    pub optional_task_failure: OptionalTaskFailurePolicy,
    /// Maximum number of supervised tasks that may be active concurrently.
    pub max_concurrent_tasks: usize,
    /// Maximum number of completed task reports retained in memory.
    ///
    /// A zero capacity disables report history while completion handles still receive reports.
    pub task_report_capacity: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            shutdown_grace_period: Duration::from_secs(30),
            optional_task_failure: OptionalTaskFailurePolicy::MarkDegraded,
            max_concurrent_tasks: 256,
            task_report_capacity: 1_024,
        }
    }
}

/// Result of a bounded runtime drain attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DrainOutcome {
    /// All supervised tasks completed within the grace period.
    Completed,
    /// The grace period elapsed while tasks were still active.
    TimedOut {
        /// Number of supervised tasks still active when the timeout was observed.
        remaining_tasks: usize,
    },
}

#[derive(Debug)]
struct LifecycleState {
    phase: RuntimePhase,
    active_tasks: usize,
}

struct RuntimeInner {
    metadata: ServiceMetadata,
    config: RuntimeConfig,
    shutdown: ShutdownController,
    shutdown_signal: ShutdownSignal,
    lifecycle: Mutex<LifecycleState>,
    task_change: Notify,
    health: HealthRegistry,
    reports: RwLock<VecDeque<TaskReport>>,
    abort_handles: Mutex<BTreeMap<u64, AbortHandle>>,
    next_task_id: Mutex<u64>,
}

impl RuntimeInner {
    fn lifecycle(&self) -> MutexGuard<'_, LifecycleState> {
        match self.lifecycle.lock() {
            Ok(lifecycle) => lifecycle,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn reports(&self) -> RwLockReadGuard<'_, VecDeque<TaskReport>> {
        match self.reports.read() {
            Ok(reports) => reports,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn reports_mut(&self) -> RwLockWriteGuard<'_, VecDeque<TaskReport>> {
        match self.reports.write() {
            Ok(reports) => reports,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn begin_shutdown(&self) -> bool {
        let changed = {
            let mut lifecycle = self.lifecycle();
            if lifecycle.phase == RuntimePhase::Running {
                lifecycle.phase = RuntimePhase::Draining;
                true
            } else {
                false
            }
        };

        if changed {
            self.shutdown.cancel();
        }

        changed
    }

    fn register_task(&self) -> Result<u64, SpawnError> {
        let mut lifecycle = self.lifecycle();
        if lifecycle.phase != RuntimePhase::Running {
            return Err(SpawnError::RuntimeNotRunning(lifecycle.phase));
        }
        if lifecycle.active_tasks >= self.config.max_concurrent_tasks {
            return Err(SpawnError::TaskCapacityExceeded {
                limit: self.config.max_concurrent_tasks,
            });
        }

        let task_id = {
            let mut next_task_id = match self.next_task_id.lock() {
                Ok(next_task_id) => next_task_id,
                Err(poisoned) => poisoned.into_inner(),
            };
            let task_id = *next_task_id;
            *next_task_id = next_task_id
                .checked_add(1)
                .ok_or(SpawnError::TaskIdentifierExhausted)?;
            task_id
        };

        lifecycle.active_tasks = lifecycle.active_tasks.saturating_add(1);
        Ok(task_id)
    }

    fn register_abort_handle(&self, task_id: u64, abort_handle: AbortHandle) {
        match self.abort_handles.lock() {
            Ok(mut handles) => {
                handles.insert(task_id, abort_handle);
            }
            Err(poisoned) => {
                poisoned.into_inner().insert(task_id, abort_handle);
            }
        }
    }

    fn finish_task(&self, task_id: u64, report: &TaskReport) {
        if let Some(failure) = report.outcome.failure() {
            self.apply_task_failure(&report.spec, failure);
        }

        if self.config.task_report_capacity > 0 {
            let mut reports = self.reports_mut();
            if reports.len() == self.config.task_report_capacity {
                reports.pop_front();
            }
            reports.push_back(report.clone());
        }

        match self.abort_handles.lock() {
            Ok(mut handles) => {
                handles.remove(&task_id);
            }
            Err(poisoned) => {
                poisoned.into_inner().remove(&task_id);
            }
        }

        {
            let mut lifecycle = self.lifecycle();
            lifecycle.active_tasks = lifecycle.active_tasks.saturating_sub(1);
        }

        self.task_change.notify_waiters();
    }

    fn abort_active_tasks(&self) {
        let handles = match self.abort_handles.lock() {
            Ok(handles) => handles,
            Err(poisoned) => poisoned.into_inner(),
        };
        for handle in handles.values() {
            handle.abort();
        }
    }

    fn apply_task_failure(&self, spec: &TaskSpec, failure: &TaskFailure) {
        let component = Arc::<str>::from(format!("runtime.task.{}", spec.name));
        let message = Some(Arc::<str>::from(failure.message()));

        match spec.criticality {
            TaskCriticality::Critical => {
                self.health.set_health(ComponentHealth {
                    component: Arc::clone(&component),
                    status: HealthStatus::Unhealthy,
                    message: message.clone(),
                });
                self.health.set_readiness(ComponentReadiness {
                    component,
                    status: ReadinessStatus::NotReady,
                    message,
                });
                self.begin_shutdown();
            }
            TaskCriticality::Required => {
                self.health.set_health(ComponentHealth {
                    component: Arc::clone(&component),
                    status: HealthStatus::Degraded,
                    message: message.clone(),
                });
                self.health.set_readiness(ComponentReadiness {
                    component,
                    status: ReadinessStatus::NotReady,
                    message,
                });
            }
            TaskCriticality::Optional => match self.config.optional_task_failure {
                OptionalTaskFailurePolicy::Ignore => {}
                OptionalTaskFailurePolicy::MarkDegraded => {
                    self.health.set_health(ComponentHealth {
                        component,
                        status: HealthStatus::Degraded,
                        message,
                    });
                }
                OptionalTaskFailurePolicy::Shutdown => {
                    self.health.set_health(ComponentHealth {
                        component: Arc::clone(&component),
                        status: HealthStatus::Unhealthy,
                        message: message.clone(),
                    });
                    self.health.set_readiness(ComponentReadiness {
                        component,
                        status: ReadinessStatus::NotReady,
                        message,
                    });
                    self.begin_shutdown();
                }
            },
        }
    }

    async fn wait_until_idle(&self) {
        loop {
            let task_changed = self.task_change.notified();
            if self.lifecycle().active_tasks == 0 {
                return;
            }
            task_changed.await;
        }
    }

    fn mark_stopped(&self) {
        self.lifecycle().phase = RuntimePhase::Stopped;
    }
}

/// Cloneable handle for lifecycle coordination and supervised task execution.
#[derive(Clone)]
pub struct RuntimeHandle {
    inner: Arc<RuntimeInner>,
}

impl RuntimeHandle {
    /// Creates a runtime handle with default lifecycle configuration.
    #[must_use]
    pub fn new(metadata: ServiceMetadata) -> Self {
        Self::with_config(metadata, RuntimeConfig::default())
    }

    /// Creates a runtime handle with explicit lifecycle configuration.
    #[must_use]
    pub fn with_config(metadata: ServiceMetadata, config: RuntimeConfig) -> Self {
        let (shutdown, shutdown_signal) = shutdown_channel();
        Self {
            inner: Arc::new(RuntimeInner {
                metadata,
                config,
                shutdown,
                shutdown_signal,
                lifecycle: Mutex::new(LifecycleState {
                    phase: RuntimePhase::Running,
                    active_tasks: 0,
                }),
                task_change: Notify::new(),
                health: HealthRegistry::new(),
                reports: RwLock::new(VecDeque::new()),
                abort_handles: Mutex::new(BTreeMap::new()),
                next_task_id: Mutex::new(1),
            }),
        }
    }

    /// Returns stable process metadata.
    #[must_use]
    pub fn metadata(&self) -> &ServiceMetadata {
        &self.inner.metadata
    }

    /// Creates a context for a runtime-managed service or handler.
    #[must_use]
    pub fn context(&self) -> RuntimeContext {
        RuntimeContext {
            metadata: self.inner.metadata.clone(),
            shutdown: self.inner.shutdown_signal.clone(),
        }
    }

    /// Returns a clone of the cooperative shutdown signal.
    #[must_use]
    pub fn shutdown_signal(&self) -> ShutdownSignal {
        self.inner.shutdown_signal.clone()
    }

    /// Returns the shared health and readiness registry.
    #[must_use]
    pub fn health_registry(&self) -> HealthRegistry {
        self.inner.health.clone()
    }

    /// Returns the current lifecycle phase.
    #[must_use]
    pub fn phase(&self) -> RuntimePhase {
        self.inner.lifecycle().phase
    }

    /// Returns the number of supervised tasks that have not completed.
    #[must_use]
    pub fn active_tasks(&self) -> usize {
        self.inner.lifecycle().active_tasks
    }

    /// Returns all task reports captured so far in completion order.
    #[must_use]
    pub fn task_reports(&self) -> Vec<TaskReport> {
        self.inner.reports().iter().cloned().collect()
    }

    /// Starts coordinated shutdown.
    ///
    /// Returns true only for the caller that transitions the runtime into draining.
    #[must_use]
    pub fn request_shutdown(&self) -> bool {
        self.inner.begin_shutdown()
    }

    /// Starts shutdown and waits for active tasks up to the configured grace period.
    pub async fn shutdown(&self) -> DrainOutcome {
        let _started = self.request_shutdown();

        let outcome = if self.active_tasks() == 0 {
            DrainOutcome::Completed
        } else if let Ok(()) = timeout(
            self.inner.config.shutdown_grace_period,
            self.inner.wait_until_idle(),
        )
        .await
        {
            DrainOutcome::Completed
        } else {
            let remaining_tasks = self.active_tasks();
            if remaining_tasks == 0 {
                DrainOutcome::Completed
            } else {
                self.inner.abort_active_tasks();
                DrainOutcome::TimedOut { remaining_tasks }
            }
        };

        self.inner.mark_stopped();
        outcome
    }

    /// Starts a task and arranges for its failure or panic to be supervised.
    ///
    /// # Errors
    ///
    /// Returns an error when no asynchronous runtime is active, draining has started, the
    /// configured task-admission capacity is full, or internal identifiers are exhausted.
    pub fn spawn<F, E>(&self, spec: TaskSpec, future: F) -> Result<TaskCompletion, SpawnError>
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
        E: Error + Send + Sync + 'static,
    {
        let runtime = Handle::try_current().map_err(|_| SpawnError::NoRuntime)?;
        let task_id = self.inner.register_task()?;

        let (completion_sender, completion_receiver) = oneshot::channel();
        let task = runtime.spawn(future);
        self.inner
            .register_abort_handle(task_id, task.abort_handle());
        let inner = Arc::clone(&self.inner);

        drop(runtime.spawn(async move {
            let outcome = match task.await {
                Ok(Ok(())) => TaskOutcome::Completed,
                Ok(Err(error)) => TaskOutcome::Failed(TaskFailure::from_error(error)),
                Err(join_error) => match join_error.try_into_panic() {
                    Ok(payload) => TaskOutcome::Failed(TaskFailure::from_panic(payload.as_ref())),
                    Err(cancelled) => {
                        TaskOutcome::Failed(TaskFailure::cancelled(cancelled.to_string()))
                    }
                },
            };

            let report = TaskReport { spec, outcome };
            inner.finish_task(task_id, &report);
            drop(completion_sender.send(report));
        }));

        Ok(TaskCompletion::new(completion_receiver))
    }
}

impl Debug for RuntimeHandle {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeHandle")
            .field("metadata", &self.inner.metadata)
            .field("phase", &self.phase())
            .field("active_tasks", &self.active_tasks())
            .finish_non_exhaustive()
    }
}
