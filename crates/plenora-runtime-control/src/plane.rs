use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Display, Formatter},
    sync::Arc,
    time::SystemTime,
};

use plenora_runtime_messaging::MessageId;
use plenora_runtime_scheduler::{
    ManualTriggerOutcome, ScheduleBuildError, ScheduleId, ScheduleSnapshot,
};
use plenora_runtime_worker::{
    WorkerMessageCancellationReport, WorkerTaskCancellationOutcome, WorkerTaskId,
};

use crate::{
    ControlComponent, ControlComponentId, ControlComponentKind, ControlMutationOutcome,
    ControlPlaneError, ControlRegistrationError, MemoryPressureSnapshot, MemorySnapshotSource,
    SchedulerControl, SubprocessSnapshot, SubprocessSnapshotSource, WorkerControlHandle,
    WorkerControlSnapshot,
};

/// Registration limits for heterogeneous runtime components.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControlPlaneConfig {
    workers: usize,
    schedulers: usize,
    memory_monitors: usize,
    subprocess_supervisors: usize,
}

impl ControlPlaneConfig {
    /// Creates strictly positive per-category capacities.
    ///
    /// # Errors
    ///
    /// Returns an error when any category would be unrepresentable.
    pub const fn new(
        max_workers: usize,
        max_schedulers: usize,
        max_memory_monitors: usize,
        max_subprocess_supervisors: usize,
    ) -> Result<Self, ControlPlaneConfigError> {
        if max_workers == 0 {
            return Err(ControlPlaneConfigError::ZeroWorkers);
        }
        if max_schedulers == 0 {
            return Err(ControlPlaneConfigError::ZeroSchedulers);
        }
        if max_memory_monitors == 0 {
            return Err(ControlPlaneConfigError::ZeroMemoryMonitors);
        }
        if max_subprocess_supervisors == 0 {
            return Err(ControlPlaneConfigError::ZeroSubprocessSupervisors);
        }
        Ok(Self {
            workers: max_workers,
            schedulers: max_schedulers,
            memory_monitors: max_memory_monitors,
            subprocess_supervisors: max_subprocess_supervisors,
        })
    }

    /// Returns the worker registration limit.
    #[must_use]
    pub const fn max_workers(self) -> usize {
        self.workers
    }

    /// Returns the scheduler registration limit.
    #[must_use]
    pub const fn max_schedulers(self) -> usize {
        self.schedulers
    }

    /// Returns the memory-monitor registration limit.
    #[must_use]
    pub const fn max_memory_monitors(self) -> usize {
        self.memory_monitors
    }

    /// Returns the subprocess-supervisor registration limit.
    #[must_use]
    pub const fn max_subprocess_supervisors(self) -> usize {
        self.subprocess_supervisors
    }
}

impl Default for ControlPlaneConfig {
    fn default() -> Self {
        Self {
            workers: 32,
            schedulers: 8,
            memory_monitors: 8,
            subprocess_supervisors: 8,
        }
    }
}

/// Invalid control-plane registration bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlPlaneConfigError {
    /// At least one worker must be representable.
    ZeroWorkers,
    /// At least one scheduler must be representable.
    ZeroSchedulers,
    /// At least one memory monitor must be representable.
    ZeroMemoryMonitors,
    /// At least one subprocess supervisor must be representable.
    ZeroSubprocessSupervisors,
}

impl Display for ControlPlaneConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("runtime control-plane capacities must be greater than zero")
    }
}

impl Error for ControlPlaneConfigError {}

/// Bounded registration builder for a heterogeneous runtime control plane.
pub struct ControlPlaneBuilder {
    config: ControlPlaneConfig,
    workers: BTreeMap<ControlComponentId, WorkerControlHandle>,
    schedulers: BTreeMap<ControlComponentId, Arc<dyn SchedulerControl>>,
    memory: BTreeMap<ControlComponentId, Arc<dyn MemorySnapshotSource>>,
    subprocess: BTreeMap<ControlComponentId, Arc<dyn SubprocessSnapshotSource>>,
}

impl ControlPlaneBuilder {
    /// Creates an empty builder with explicit category bounds.
    #[must_use]
    pub const fn new(config: ControlPlaneConfig) -> Self {
        Self {
            config,
            workers: BTreeMap::new(),
            schedulers: BTreeMap::new(),
            memory: BTreeMap::new(),
            subprocess: BTreeMap::new(),
        }
    }

    /// Registers one worker endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate identity or exhausted worker capacity.
    pub fn register_worker(
        &mut self,
        id: ControlComponentId,
        handle: WorkerControlHandle,
    ) -> Result<(), ControlRegistrationError> {
        insert_bounded(
            &mut self.workers,
            id,
            handle,
            self.config.max_workers(),
            ControlComponentKind::Worker,
        )
    }

    /// Registers one type-erased scheduler endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate identity or exhausted scheduler capacity.
    pub fn register_scheduler<S>(
        &mut self,
        id: ControlComponentId,
        scheduler: Arc<S>,
    ) -> Result<(), ControlRegistrationError>
    where
        S: SchedulerControl + 'static,
    {
        let scheduler: Arc<dyn SchedulerControl> = scheduler;
        insert_bounded(
            &mut self.schedulers,
            id,
            scheduler,
            self.config.max_schedulers(),
            ControlComponentKind::Scheduler,
        )
    }

    /// Registers one read-only memory-pressure source.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate identity or exhausted memory-monitor capacity.
    pub fn register_memory<M>(
        &mut self,
        id: ControlComponentId,
        monitor: Arc<M>,
    ) -> Result<(), ControlRegistrationError>
    where
        M: MemorySnapshotSource + 'static,
    {
        let monitor: Arc<dyn MemorySnapshotSource> = monitor;
        insert_bounded(
            &mut self.memory,
            id,
            monitor,
            self.config.max_memory_monitors(),
            ControlComponentKind::Memory,
        )
    }

    /// Registers one read-only subprocess-capacity source.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate identity or exhausted supervisor capacity.
    pub fn register_subprocess<S>(
        &mut self,
        id: ControlComponentId,
        supervisor: Arc<S>,
    ) -> Result<(), ControlRegistrationError>
    where
        S: SubprocessSnapshotSource + 'static,
    {
        let supervisor: Arc<dyn SubprocessSnapshotSource> = supervisor;
        insert_bounded(
            &mut self.subprocess,
            id,
            supervisor,
            self.config.max_subprocess_supervisors(),
            ControlComponentKind::Subprocess,
        )
    }

    /// Freezes registration and returns a cloneable control plane.
    #[must_use]
    pub fn build(self) -> ControlPlane {
        ControlPlane {
            inner: Arc::new(ControlPlaneInner {
                workers: self.workers,
                schedulers: self.schedulers,
                memory: self.memory,
                subprocess: self.subprocess,
            }),
        }
    }
}

struct ControlPlaneInner {
    workers: BTreeMap<ControlComponentId, WorkerControlHandle>,
    schedulers: BTreeMap<ControlComponentId, Arc<dyn SchedulerControl>>,
    memory: BTreeMap<ControlComponentId, Arc<dyn MemorySnapshotSource>>,
    subprocess: BTreeMap<ControlComponentId, Arc<dyn SubprocessSnapshotSource>>,
}

/// Cloneable payload-free view and mutation gateway for runtime-owned components.
#[derive(Clone)]
pub struct ControlPlane {
    inner: Arc<ControlPlaneInner>,
}

impl ControlPlane {
    /// Lists all registered components in stable category/identity order.
    #[must_use]
    pub fn components(&self) -> Vec<ControlComponent> {
        let mut components = Vec::with_capacity(
            self.inner
                .workers
                .len()
                .saturating_add(self.inner.schedulers.len())
                .saturating_add(self.inner.memory.len())
                .saturating_add(self.inner.subprocess.len()),
        );
        append_components(
            &mut components,
            self.inner.workers.keys(),
            ControlComponentKind::Worker,
        );
        append_components(
            &mut components,
            self.inner.schedulers.keys(),
            ControlComponentKind::Scheduler,
        );
        append_components(
            &mut components,
            self.inner.memory.keys(),
            ControlComponentKind::Memory,
        );
        append_components(
            &mut components,
            self.inner.subprocess.keys(),
            ControlComponentKind::Subprocess,
        );
        components
    }

    /// Returns one bounded worker snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown worker identity.
    pub fn worker_snapshot(
        &self,
        id: &ControlComponentId,
    ) -> Result<WorkerControlSnapshot, ControlPlaneError> {
        Ok(self.worker(id)?.snapshot())
    }

    /// Temporarily pauses one worker.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown worker identity.
    pub fn pause_worker(
        &self,
        id: &ControlComponentId,
    ) -> Result<ControlMutationOutcome, ControlPlaneError> {
        Ok(self.worker(id)?.pause())
    }

    /// Resumes one reversibly paused worker.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown worker identity.
    pub fn resume_worker(
        &self,
        id: &ControlComponentId,
    ) -> Result<ControlMutationOutcome, ControlPlaneError> {
        Ok(self.worker(id)?.resume())
    }

    /// Permanently starts one worker drain.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown worker identity.
    pub fn drain_worker(
        &self,
        id: &ControlComponentId,
    ) -> Result<ControlMutationOutcome, ControlPlaneError> {
        Ok(self.worker(id)?.drain())
    }

    /// Requests cooperative cancellation for one active task.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown worker identity.
    pub fn cancel_worker_task(
        &self,
        id: &ControlComponentId,
        task_id: WorkerTaskId,
    ) -> Result<WorkerTaskCancellationOutcome, ControlPlaneError> {
        Ok(self.worker(id)?.cancel_task(task_id))
    }

    /// Requests cancellation for all active attempts of one broker message.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown worker identity.
    pub fn cancel_worker_message(
        &self,
        id: &ControlComponentId,
        message_id: MessageId,
    ) -> Result<WorkerMessageCancellationReport, ControlPlaneError> {
        Ok(self.worker(id)?.cancel_message(message_id))
    }

    /// Returns one memory-pressure snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown memory-monitor identity.
    pub fn memory_snapshot(
        &self,
        id: &ControlComponentId,
    ) -> Result<MemoryPressureSnapshot, ControlPlaneError> {
        self.inner
            .memory
            .get(id)
            .map(|source| source.snapshot())
            .ok_or_else(|| unknown(ControlComponentKind::Memory, id))
    }

    /// Returns one subprocess-supervisor snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown subprocess-supervisor identity.
    pub fn subprocess_snapshot(
        &self,
        id: &ControlComponentId,
    ) -> Result<SubprocessSnapshot, ControlPlaneError> {
        self.inner
            .subprocess
            .get(id)
            .map(|source| source.snapshot())
            .ok_or_else(|| unknown(ControlComponentKind::Subprocess, id))
    }

    /// Returns one scheduler's bounded cursor snapshots.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown scheduler identity.
    pub async fn scheduler_snapshots(
        &self,
        id: &ControlComponentId,
    ) -> Result<Vec<ScheduleSnapshot>, ControlPlaneError> {
        Ok(self.scheduler(id)?.snapshots().await)
    }

    /// Pauses one schedule.
    ///
    /// # Errors
    ///
    /// Separates component lookup failure from scheduler transition failure.
    pub async fn pause_schedule(
        &self,
        component: &ControlComponentId,
        schedule: &ScheduleId,
    ) -> Result<Result<(), ScheduleBuildError>, ControlPlaneError> {
        Ok(self.scheduler(component)?.pause(schedule).await)
    }

    /// Resumes one schedule.
    ///
    /// # Errors
    ///
    /// Separates component lookup failure from scheduler transition failure.
    pub async fn resume_schedule(
        &self,
        component: &ControlComponentId,
        schedule: &ScheduleId,
    ) -> Result<Result<(), ScheduleBuildError>, ControlPlaneError> {
        Ok(self.scheduler(component)?.resume(schedule).await)
    }

    /// Performs one bounded manual scheduled invocation.
    ///
    /// # Errors
    ///
    /// Separates component lookup failure from scheduler transition failure.
    pub async fn trigger_schedule(
        &self,
        component: &ControlComponentId,
        schedule: &ScheduleId,
        triggered_at: SystemTime,
    ) -> Result<Result<ManualTriggerOutcome, ScheduleBuildError>, ControlPlaneError> {
        Ok(self
            .scheduler(component)?
            .trigger(schedule, triggered_at)
            .await)
    }

    fn worker(&self, id: &ControlComponentId) -> Result<&WorkerControlHandle, ControlPlaneError> {
        self.inner
            .workers
            .get(id)
            .ok_or_else(|| unknown(ControlComponentKind::Worker, id))
    }

    fn scheduler(
        &self,
        id: &ControlComponentId,
    ) -> Result<&Arc<dyn SchedulerControl>, ControlPlaneError> {
        self.inner
            .schedulers
            .get(id)
            .ok_or_else(|| unknown(ControlComponentKind::Scheduler, id))
    }
}

impl Default for ControlPlaneBuilder {
    fn default() -> Self {
        Self::new(ControlPlaneConfig::default())
    }
}

fn insert_bounded<T>(
    entries: &mut BTreeMap<ControlComponentId, T>,
    id: ControlComponentId,
    value: T,
    limit: usize,
    kind: ControlComponentKind,
) -> Result<(), ControlRegistrationError> {
    if entries.contains_key(&id) {
        return Err(ControlRegistrationError::Duplicate { kind, id });
    }
    if entries.len() >= limit {
        return Err(ControlRegistrationError::CapacityExceeded { kind, limit });
    }
    let _previous = entries.insert(id, value);
    Ok(())
}

fn append_components<'a>(
    target: &mut Vec<ControlComponent>,
    ids: impl Iterator<Item = &'a ControlComponentId>,
    kind: ControlComponentKind,
) {
    target.extend(ids.map(|id| ControlComponent {
        id: id.clone(),
        kind,
    }));
}

fn unknown(kind: ControlComponentKind, id: &ControlComponentId) -> ControlPlaneError {
    ControlPlaneError::UnknownComponent {
        kind,
        id: id.clone(),
    }
}
