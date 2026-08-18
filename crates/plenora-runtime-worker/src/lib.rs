//! Worker execution contracts and bounded runtime coordination.

#![forbid(unsafe_code)]

mod active;
mod cancellation;
mod config;
mod context;
mod decoder;
mod dispatch;
mod error;
mod executor;
mod handler;
mod instance;
mod lifecycle;

pub use active::{
    ActiveWorkerTask, WorkerMessageCancellationReport, WorkerTaskCancellationOutcome,
    WorkerTaskControl, WorkerTaskId,
};
pub use cancellation::{TaskCancellationReason, TaskCancellationToken, WorkerCancellationReason};
pub use config::{
    DEFAULT_TASK_CANCELLATION_GRACE_PERIOD, WorkerConcurrency, WorkerConfig, WorkerConfigError,
};
pub use context::{WorkerContext, WorkerContextIdentity};
pub use decoder::{
    DecodedWorkerMessage, MetadataMessageDecodeError, MetadataMessageDecoder, WorkerMessageDecoder,
};
pub use dispatch::{
    DEFAULT_WORKER_LIFECYCLE_CHANNEL_CAPACITY, MAX_WORKER_LIFECYCLE_CHANNEL_CAPACITY,
    WorkerLifecycleChannelConfig, WorkerLifecycleChannelConfigError,
    WorkerLifecycleDispatchSnapshot, WorkerLifecycleDispatchState, WorkerLifecycleDispatcher,
    WorkerLifecycleHealthCriticality, WorkerLifecycleHealthReporter, WorkerLifecycleObservation,
    WorkerLifecycleReceiver,
};
pub use error::{
    WorkerAdmissionReason, WorkerErrorCategory, WorkerExecutionError, WorkerExecutionPhase,
    WorkerRemoteEffect,
};
pub use executor::{
    WorkerAdmissionControl, WorkerAdmissionHandle, WorkerAdmissionState, WorkerCapacitySnapshot,
    WorkerDrainOutcome, WorkerExecutor,
};
pub use handler::WorkerHandler;
pub use instance::{
    DEFAULT_WORKER_INSTANCE_HEARTBEAT_INTERVAL, NoopWorkerInstanceHeartbeatObserver,
    WorkerInstanceHeartbeat, WorkerInstanceHeartbeatConfig, WorkerInstanceHeartbeatConfigError,
    WorkerInstanceHeartbeatError, WorkerInstanceHeartbeatObserver, WorkerInstanceHeartbeatReporter,
    WorkerInstanceIdentity, WorkerInstanceStatus,
};
pub use lifecycle::{
    NoopTaskLifecycleObserver, TaskLifecycleEvent, TaskLifecycleEventKind, TaskLifecycleObserver,
    TaskProgress, TaskProgressError, TaskProgressReporter, TaskState,
};
