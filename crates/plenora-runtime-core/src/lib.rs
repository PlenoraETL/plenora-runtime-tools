//! Process lifecycle, shutdown, supervision, health, and shared runtime primitives.

#![forbid(unsafe_code)]

mod clock;
mod health;
mod runtime;
mod shutdown;
mod supervision;

pub use clock::{Clock, SystemClock};
pub use health::{
    ComponentHealth, ComponentReadiness, HealthRegistry, HealthSnapshot, HealthStatus,
    ReadinessSnapshot, ReadinessStatus,
};
pub use runtime::{
    DrainOutcome, RuntimeConfig, RuntimeContext, RuntimeHandle, RuntimePhase, ServiceMetadata,
};
pub use shutdown::ShutdownSignal;
pub use supervision::{
    OptionalTaskFailurePolicy, SpawnError, TaskCompletion, TaskCompletionError, TaskCriticality,
    TaskFailure, TaskFailureKind, TaskOutcome, TaskReport, TaskSpec,
};
