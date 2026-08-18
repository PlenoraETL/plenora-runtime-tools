//! Bounded, backend-neutral operational control over runtime components.
//!
//! This crate exposes payload-free snapshots and explicit control operations. It owns no HTTP,
//! authentication, database, broker, or application-library policy.

#![forbid(unsafe_code)]

mod component;
mod error;
mod plane;
mod scheduler;
mod snapshot;
mod worker;

pub use component::{
    ControlComponent, ControlComponentId, ControlComponentIdError, ControlComponentKind,
    MAX_CONTROL_COMPONENT_ID_BYTES,
};
pub use error::{ControlPlaneError, ControlRegistrationError};
pub use plane::{ControlPlane, ControlPlaneBuilder, ControlPlaneConfig, ControlPlaneConfigError};
pub use scheduler::SchedulerControl;
pub use snapshot::{MemorySnapshotSource, SubprocessSnapshotSource};
pub use worker::{ControlMutationOutcome, WorkerControlHandle, WorkerControlSnapshot};

pub use plenora_runtime_resources::MemoryPressureSnapshot;
pub use plenora_runtime_scheduler::{
    ManualTriggerOutcome, ScheduleBuildError, ScheduleId, ScheduleSnapshot,
};
pub use plenora_runtime_subprocess::SubprocessSnapshot;
pub use plenora_runtime_worker::{
    ActiveWorkerTask, WorkerMessageCancellationReport, WorkerTaskCancellationOutcome, WorkerTaskId,
};
