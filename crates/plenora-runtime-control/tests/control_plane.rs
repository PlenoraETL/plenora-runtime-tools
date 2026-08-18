//! Registration, discovery, snapshots, and mutation coverage.

use std::{
    error::Error,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use plenora_runtime_control::{
    ControlComponentId, ControlComponentIdError, ControlComponentKind, ControlMutationOutcome,
    ControlPlaneBuilder, ControlPlaneConfig, ControlPlaneConfigError, ControlPlaneError,
    ControlRegistrationError, MAX_CONTROL_COMPONENT_ID_BYTES, ManualTriggerOutcome,
    MemoryPressureSnapshot, MemorySnapshotSource, WorkerControlHandle, WorkerTaskId,
};
use plenora_runtime_messaging::MessageId;
use plenora_runtime_resources::MemoryPressureState;
use plenora_runtime_scheduler::{
    FixedIntervalPlan, Schedule, ScheduleDispatchError, ScheduleDispatcher, ScheduleId,
    ScheduleStatus, ScheduledOccurrence, SchedulerBuilder, SchedulerConfig,
};
use plenora_runtime_subprocess::{SubprocessSupervisor, SubprocessSupervisorConfig};
use plenora_runtime_worker::{
    WorkerConcurrency, WorkerConfig, WorkerExecutor, WorkerTaskCancellationOutcome,
};

#[derive(Debug)]
struct ConfirmingDispatcher;

#[async_trait]
impl ScheduleDispatcher<&'static str> for ConfirmingDispatcher {
    async fn dispatch(
        &self,
        _occurrence: ScheduledOccurrence<&'static str>,
    ) -> Result<(), ScheduleDispatchError> {
        Ok(())
    }
}

#[derive(Debug)]
struct FixedMemory;

impl MemorySnapshotSource for FixedMemory {
    fn snapshot(&self) -> MemoryPressureSnapshot {
        MemoryPressureSnapshot {
            sequence: 3,
            state: MemoryPressureState::Normal,
            resident_bytes: Some(64),
        }
    }
}

#[tokio::test]
async fn heterogeneous_control_is_bounded_discoverable_and_operable() -> Result<(), Box<dyn Error>>
{
    let worker_id = ControlComponentId::new("worker.main")?;
    let scheduler_id = ControlComponentId::new("scheduler.main")?;
    let memory_id = ControlComponentId::new("memory.process")?;
    let subprocess_id = ControlComponentId::new("subprocess.tools")?;
    let executor = WorkerExecutor::new(
        (),
        (),
        WorkerConfig::new(WorkerConcurrency::new(2)?, Duration::from_secs(5)),
    )?;
    let worker = WorkerControlHandle::new(executor.admission_control(), executor.task_control());

    let recurring_id = ScheduleId::new("daily")?;
    let mut scheduler_builder = SchedulerBuilder::new(SchedulerConfig::new(
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::ZERO,
        2,
        1,
        1,
    )?);
    scheduler_builder.register(Schedule::new(
        recurring_id.clone(),
        "payload",
        FixedIntervalPlan::new(SystemTime::UNIX_EPOCH, Duration::from_secs(10))?,
    ))?;
    let scheduler = Arc::new(scheduler_builder.build(Arc::new(ConfirmingDispatcher)));

    let mut builder = ControlPlaneBuilder::new(ControlPlaneConfig::new(1, 1, 1, 1)?);
    builder.register_worker(worker_id.clone(), worker)?;
    builder.register_scheduler(scheduler_id.clone(), scheduler)?;
    builder.register_memory(memory_id.clone(), Arc::new(FixedMemory))?;
    builder.register_subprocess(
        subprocess_id.clone(),
        Arc::new(SubprocessSupervisor::new(SubprocessSupervisorConfig::new(
            4,
            Duration::from_secs(5),
        )?)),
    )?;
    let control = builder.build();

    let components = control.components();
    assert_eq!(components.len(), 4);
    assert_eq!(components[0].kind, ControlComponentKind::Worker);
    assert_eq!(components[1].kind, ControlComponentKind::Scheduler);
    assert_eq!(components[2].kind, ControlComponentKind::Memory);
    assert_eq!(components[3].kind, ControlComponentKind::Subprocess);

    assert_eq!(control.worker_snapshot(&worker_id)?.capacity.capacity, 2);
    assert_eq!(
        control.pause_worker(&worker_id)?,
        ControlMutationOutcome::Applied
    );
    assert_eq!(
        control.pause_worker(&worker_id)?,
        ControlMutationOutcome::AlreadyInState
    );
    assert_eq!(
        control.resume_worker(&worker_id)?,
        ControlMutationOutcome::Applied
    );
    assert_eq!(
        control.cancel_worker_task(&worker_id, WorkerTaskId::from_str("1")?)?,
        WorkerTaskCancellationOutcome::NotFound
    );
    assert_eq!(
        control
            .cancel_worker_message(&worker_id, MessageId::random())?
            .matched,
        0
    );

    control
        .pause_schedule(&scheduler_id, &recurring_id)
        .await??;
    assert_eq!(
        control.scheduler_snapshots(&scheduler_id).await?[0].status,
        ScheduleStatus::Paused
    );
    assert_eq!(
        control
            .trigger_schedule(&scheduler_id, &recurring_id, SystemTime::UNIX_EPOCH)
            .await??,
        ManualTriggerOutcome::Confirmed
    );
    control
        .resume_schedule(&scheduler_id, &recurring_id)
        .await??;
    assert_eq!(
        control.memory_snapshot(&memory_id)?.resident_bytes,
        Some(64)
    );
    assert_eq!(control.subprocess_snapshot(&subprocess_id)?.available, 4);

    assert_eq!(
        control.drain_worker(&worker_id)?,
        ControlMutationOutcome::Applied
    );
    assert_eq!(
        control.resume_worker(&worker_id)?,
        ControlMutationOutcome::Rejected
    );
    Ok(())
}

#[tokio::test]
async fn validation_capacity_and_unknown_component_errors_are_explicit()
-> Result<(), Box<dyn Error>> {
    assert_eq!(
        ControlComponentId::new(""),
        Err(ControlComponentIdError::Empty)
    );
    assert_eq!(
        ControlComponentId::new("Invalid"),
        Err(ControlComponentIdError::InvalidCharacter)
    );
    assert_eq!(
        ControlComponentId::new("x".repeat(MAX_CONTROL_COMPONENT_ID_BYTES + 1)),
        Err(ControlComponentIdError::TooLong)
    );
    let rendered_id = ControlComponentId::new("worker.rendered")?;
    assert_eq!(rendered_id.as_str(), "worker.rendered");
    assert_eq!(rendered_id.to_string(), "worker.rendered");
    for error in [
        ControlComponentIdError::Empty,
        ControlComponentIdError::TooLong,
        ControlComponentIdError::InvalidCharacter,
    ] {
        assert!(!error.to_string().is_empty());
    }
    assert_eq!(
        ControlPlaneConfig::new(0, 1, 1, 1),
        Err(ControlPlaneConfigError::ZeroWorkers)
    );
    assert_eq!(
        ControlPlaneConfig::new(1, 0, 1, 1),
        Err(ControlPlaneConfigError::ZeroSchedulers)
    );
    assert_eq!(
        ControlPlaneConfig::new(1, 1, 0, 1),
        Err(ControlPlaneConfigError::ZeroMemoryMonitors)
    );
    assert_eq!(
        ControlPlaneConfig::new(1, 1, 1, 0),
        Err(ControlPlaneConfigError::ZeroSubprocessSupervisors)
    );
    for error in [
        ControlPlaneConfigError::ZeroWorkers,
        ControlPlaneConfigError::ZeroSchedulers,
        ControlPlaneConfigError::ZeroMemoryMonitors,
        ControlPlaneConfigError::ZeroSubprocessSupervisors,
    ] {
        assert!(!error.to_string().is_empty());
    }
    let defaults = ControlPlaneConfig::default();
    assert_eq!(defaults.max_workers(), 32);
    assert_eq!(defaults.max_schedulers(), 8);
    assert_eq!(defaults.max_memory_monitors(), 8);
    assert_eq!(defaults.max_subprocess_supervisors(), 8);

    let id = ControlComponentId::new("worker.one")?;
    let executor = WorkerExecutor::new((), (), WorkerConfig::default())?;
    let worker = WorkerControlHandle::new(executor.admission_control(), executor.task_control());
    let mut builder = ControlPlaneBuilder::new(ControlPlaneConfig::new(1, 1, 1, 1)?);
    builder.register_worker(id.clone(), worker.clone())?;
    assert_eq!(
        builder.register_worker(id.clone(), worker.clone()),
        Err(ControlRegistrationError::Duplicate {
            kind: ControlComponentKind::Worker,
            id: id.clone(),
        })
    );
    let duplicate_error = ControlRegistrationError::Duplicate {
        kind: ControlComponentKind::Worker,
        id: id.clone(),
    };
    assert!(!duplicate_error.to_string().is_empty());
    let capacity_error = ControlRegistrationError::CapacityExceeded {
        kind: ControlComponentKind::Worker,
        limit: 1,
    };
    assert!(!capacity_error.to_string().is_empty());
    assert_eq!(
        builder.register_worker(ControlComponentId::new("worker.two")?, worker),
        Err(ControlRegistrationError::CapacityExceeded {
            kind: ControlComponentKind::Worker,
            limit: 1,
        })
    );
    let control = builder.build();
    let missing = ControlComponentId::new("worker.missing")?;
    let unknown_worker = ControlPlaneError::UnknownComponent {
        kind: ControlComponentKind::Worker,
        id: missing.clone(),
    };
    assert_eq!(
        control.worker_snapshot(&missing),
        Err(unknown_worker.clone())
    );
    assert!(!unknown_worker.to_string().is_empty());
    assert!(control.memory_snapshot(&missing).is_err());
    assert!(control.subprocess_snapshot(&missing).is_err());
    assert!(control.scheduler_snapshots(&missing).await.is_err());
    Ok(())
}
