use std::time::{SystemTime, UNIX_EPOCH};

use plenora_runtime_control::{
    ActiveWorkerTask, ControlComponent, ControlComponentKind, ControlMutationOutcome,
    ManualTriggerOutcome, MemoryPressureSnapshot, ScheduleSnapshot, SubprocessSnapshot,
    WorkerControlSnapshot, WorkerTaskCancellationOutcome,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub(crate) struct TriggerRequestDto {
    pub triggered_at_unix_ms: u64,
}

#[derive(Serialize)]
pub(crate) struct ErrorDto {
    pub code: &'static str,
}

#[derive(Serialize)]
pub(crate) struct ComponentsDto {
    pub components: Vec<ComponentDto>,
}

#[derive(Serialize)]
pub(crate) struct ComponentDto {
    pub id: String,
    pub kind: &'static str,
}

impl From<ControlComponent> for ComponentDto {
    fn from(component: ControlComponent) -> Self {
        Self {
            id: component.id.to_string(),
            kind: component_kind(component.kind),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct WorkerDto {
    pub admission: &'static str,
    pub capacity: usize,
    pub in_flight: usize,
    pub available: usize,
    pub active_tasks: Vec<TaskDto>,
}

impl From<WorkerControlSnapshot> for WorkerDto {
    fn from(snapshot: WorkerControlSnapshot) -> Self {
        Self {
            admission: worker_admission(snapshot.capacity.admission),
            capacity: snapshot.capacity.capacity,
            in_flight: snapshot.capacity.in_flight,
            available: snapshot.capacity.available,
            active_tasks: snapshot
                .active_tasks
                .into_iter()
                .map(TaskDto::from)
                .collect(),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct TaskDto {
    pub task_id: String,
    pub message_id: String,
    pub correlation_id: String,
    pub attempt: u32,
    pub started_at_unix_ms: Option<u64>,
    pub cancellation_reason: Option<&'static str>,
}

impl From<ActiveWorkerTask> for TaskDto {
    fn from(task: ActiveWorkerTask) -> Self {
        Self {
            task_id: task.task_id.to_string(),
            message_id: task.message_id.to_string(),
            correlation_id: task.correlation_id.to_string(),
            attempt: task.attempt,
            started_at_unix_ms: unix_millis(task.started_at),
            cancellation_reason: task.cancellation_reason.map(cancellation_reason),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct SchedulesDto {
    pub schedules: Vec<ScheduleDto>,
}

#[derive(Serialize)]
pub(crate) struct ScheduleDto {
    pub id: String,
    pub status: &'static str,
    pub next_due_at_unix_ms: Option<u64>,
}

impl From<ScheduleSnapshot> for ScheduleDto {
    fn from(snapshot: ScheduleSnapshot) -> Self {
        Self {
            id: snapshot.id.to_string(),
            status: schedule_status(snapshot.status),
            next_due_at_unix_ms: snapshot.next_due_at.and_then(unix_millis),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct MemoryDto {
    pub sequence: u64,
    pub state: &'static str,
    pub resident_bytes: Option<u64>,
}

impl From<MemoryPressureSnapshot> for MemoryDto {
    fn from(snapshot: MemoryPressureSnapshot) -> Self {
        Self {
            sequence: snapshot.sequence,
            state: memory_state(snapshot.state),
            resident_bytes: snapshot.resident_bytes,
        }
    }
}

#[derive(Serialize)]
pub(crate) struct SubprocessDto {
    pub capacity: usize,
    pub in_flight: usize,
    pub available: usize,
    pub started: u64,
    pub completed: u64,
    pub spawn_failures: u64,
    pub cancellations: u64,
    pub timeouts: u64,
    pub memory_terminations: u64,
}

impl From<SubprocessSnapshot> for SubprocessDto {
    fn from(snapshot: SubprocessSnapshot) -> Self {
        Self {
            capacity: snapshot.capacity,
            in_flight: snapshot.in_flight,
            available: snapshot.available,
            started: snapshot.started,
            completed: snapshot.completed,
            spawn_failures: snapshot.spawn_failures,
            cancellations: snapshot.cancellations,
            timeouts: snapshot.timeouts,
            memory_terminations: snapshot.memory_terminations,
        }
    }
}

#[derive(Serialize)]
pub(crate) struct OutcomeDto {
    pub outcome: &'static str,
}

impl From<ControlMutationOutcome> for OutcomeDto {
    fn from(outcome: ControlMutationOutcome) -> Self {
        Self {
            outcome: match outcome {
                ControlMutationOutcome::Applied => "applied",
                ControlMutationOutcome::AlreadyInState => "already_in_state",
                ControlMutationOutcome::Rejected => "rejected",
            },
        }
    }
}

impl From<WorkerTaskCancellationOutcome> for OutcomeDto {
    fn from(outcome: WorkerTaskCancellationOutcome) -> Self {
        Self {
            outcome: match outcome {
                WorkerTaskCancellationOutcome::Requested => "requested",
                WorkerTaskCancellationOutcome::AlreadyRequested(_) => "already_requested",
                WorkerTaskCancellationOutcome::NotFound => "not_found",
            },
        }
    }
}

impl From<ManualTriggerOutcome> for OutcomeDto {
    fn from(outcome: ManualTriggerOutcome) -> Self {
        Self {
            outcome: match outcome {
                ManualTriggerOutcome::Confirmed => "confirmed",
                ManualTriggerOutcome::NotStarted => "not_started",
                ManualTriggerOutcome::OutcomeUnknown => "outcome_unknown",
                ManualTriggerOutcome::TimedOut => "timed_out",
                ManualTriggerOutcome::Saturated => "saturated",
                ManualTriggerOutcome::ReconciliationRequired => "reconciliation_required",
            },
        }
    }
}

const fn component_kind(kind: ControlComponentKind) -> &'static str {
    match kind {
        ControlComponentKind::Worker => "worker",
        ControlComponentKind::Scheduler => "scheduler",
        ControlComponentKind::Memory => "memory",
        ControlComponentKind::Subprocess => "subprocess",
    }
}

const fn worker_admission(state: plenora_runtime_worker::WorkerAdmissionState) -> &'static str {
    match state {
        plenora_runtime_worker::WorkerAdmissionState::Accepting => "accepting",
        plenora_runtime_worker::WorkerAdmissionState::Paused => "paused",
        plenora_runtime_worker::WorkerAdmissionState::Draining => "draining",
    }
}

const fn cancellation_reason(
    reason: plenora_runtime_worker::TaskCancellationReason,
) -> &'static str {
    match reason {
        plenora_runtime_worker::TaskCancellationReason::Timeout => "timeout",
        plenora_runtime_worker::TaskCancellationReason::LeaseLost => "lease_lost",
        plenora_runtime_worker::TaskCancellationReason::Requested => "requested",
        plenora_runtime_worker::TaskCancellationReason::ExecutionDropped => "execution_dropped",
        plenora_runtime_worker::TaskCancellationReason::RuntimeShutdown => "runtime_shutdown",
        plenora_runtime_worker::TaskCancellationReason::WorkerDraining => "worker_draining",
        plenora_runtime_worker::TaskCancellationReason::ControlCapacityUnavailable => {
            "control_capacity_unavailable"
        }
        plenora_runtime_worker::TaskCancellationReason::AdmissionPaused => "admission_paused",
    }
}

const fn schedule_status(status: plenora_runtime_scheduler::ScheduleStatus) -> &'static str {
    match status {
        plenora_runtime_scheduler::ScheduleStatus::Active => "active",
        plenora_runtime_scheduler::ScheduleStatus::Paused => "paused",
        plenora_runtime_scheduler::ScheduleStatus::Completed => "completed",
        plenora_runtime_scheduler::ScheduleStatus::ReconciliationRequired => {
            "reconciliation_required"
        }
    }
}

const fn memory_state(state: plenora_runtime_resources::MemoryPressureState) -> &'static str {
    match state {
        plenora_runtime_resources::MemoryPressureState::Initializing => "initializing",
        plenora_runtime_resources::MemoryPressureState::Normal => "normal",
        plenora_runtime_resources::MemoryPressureState::Pressured => "pressured",
        plenora_runtime_resources::MemoryPressureState::Critical => "critical",
        plenora_runtime_resources::MemoryPressureState::Unavailable => "unavailable",
    }
}

fn unix_millis(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
}
