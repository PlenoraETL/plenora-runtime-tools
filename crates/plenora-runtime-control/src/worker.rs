use plenora_runtime_worker::{
    ActiveWorkerTask, WorkerAdmissionHandle, WorkerCapacitySnapshot,
    WorkerMessageCancellationReport, WorkerTaskCancellationOutcome, WorkerTaskControl,
    WorkerTaskId,
};

/// Result of an idempotent component-state mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlMutationOutcome {
    /// The call changed component state.
    Applied,
    /// The component was already in the requested state.
    AlreadyInState,
    /// A terminal or incompatible state rejected the transition.
    Rejected,
}

/// Payload-free worker state and bounded active-task list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerControlSnapshot {
    /// Current admission and capacity counters.
    pub capacity: WorkerCapacitySnapshot,
    /// Active tasks, bounded by worker capacity.
    pub active_tasks: Vec<ActiveWorkerTask>,
}

/// Cloneable engine-neutral worker control endpoint.
#[derive(Clone, Debug)]
pub struct WorkerControlHandle {
    admission: WorkerAdmissionHandle,
    tasks: WorkerTaskControl,
}

impl WorkerControlHandle {
    /// Combines handles exposed by one worker executor or adapter.
    #[must_use]
    pub const fn new(admission: WorkerAdmissionHandle, tasks: WorkerTaskControl) -> Self {
        Self { admission, tasks }
    }

    /// Returns one payload-free bounded snapshot.
    #[must_use]
    pub fn snapshot(&self) -> WorkerControlSnapshot {
        WorkerControlSnapshot {
            capacity: self.admission.snapshot(),
            active_tasks: self.tasks.active_tasks(),
        }
    }

    /// Temporarily pauses new task admission.
    #[must_use]
    pub fn pause(&self) -> ControlMutationOutcome {
        if self.admission.pause_admission() {
            ControlMutationOutcome::Applied
        } else if self.admission.snapshot().admission
            == plenora_runtime_worker::WorkerAdmissionState::Paused
        {
            ControlMutationOutcome::AlreadyInState
        } else {
            ControlMutationOutcome::Rejected
        }
    }

    /// Resumes a reversibly paused worker.
    #[must_use]
    pub fn resume(&self) -> ControlMutationOutcome {
        if self.admission.resume_admission() {
            ControlMutationOutcome::Applied
        } else if self.admission.snapshot().admission
            == plenora_runtime_worker::WorkerAdmissionState::Accepting
        {
            ControlMutationOutcome::AlreadyInState
        } else {
            ControlMutationOutcome::Rejected
        }
    }

    /// Permanently begins worker drain.
    #[must_use]
    pub fn drain(&self) -> ControlMutationOutcome {
        if self.admission.begin_drain() {
            ControlMutationOutcome::Applied
        } else {
            ControlMutationOutcome::AlreadyInState
        }
    }

    /// Requests cooperative cancellation for one executor-local task.
    #[must_use]
    pub fn cancel_task(&self, task_id: WorkerTaskId) -> WorkerTaskCancellationOutcome {
        self.tasks.request_cancellation(task_id)
    }

    /// Requests cooperative cancellation for every active attempt of one message.
    #[must_use]
    pub fn cancel_message(
        &self,
        message_id: plenora_runtime_messaging::MessageId,
    ) -> WorkerMessageCancellationReport {
        self.tasks.request_message_cancellation(message_id)
    }
}
