//! Apalis adapter for Plenora worker contracts.

#![forbid(unsafe_code)]

mod broker;
mod config;
mod job;
mod outcome;
mod service;
mod shutdown;

pub use broker::{
    BrokerDeliveryService, BrokerWorkerError, BrokerWorkerErrorKind, BrokerWorkerLifecycle,
    BrokerWorkerRunError, BrokerWorkerRunErrorKind, BrokerWorkerRunner,
};
pub use config::{ApalisAdapterConfig, ApalisAdapterConfigError};
pub use job::ApalisJob;
pub use outcome::{
    ApalisDisposition, ApalisExecutionOutcome, ApalisFailure, DEFAULT_PAUSED_ADMISSION_RETRY_DELAY,
};
pub use service::ApalisWorkerService;
pub use shutdown::ApalisShutdownBridge;
