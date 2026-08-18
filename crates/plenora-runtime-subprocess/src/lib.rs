//! Bounded, protocol-neutral child-process containment.
//!
//! Applications own executable selection and command/result serialization. This crate owns only
//! process admission, lifecycle, cancellation, timeouts, bounded output capture, and optional
//! Linux RSS enforcement.

#![forbid(unsafe_code)]

mod config;
mod error;
mod output;
mod spec;
mod supervisor;

pub use config::{
    MAX_CAPTURE_BYTES, MAX_CONCURRENT_SUBPROCESSES, SubprocessConfigError,
    SubprocessSupervisorConfig,
};
pub use error::{SubprocessError, SubprocessErrorKind};
pub use output::CapturedOutput;
pub use spec::{
    MAX_ARGUMENT_BYTES, MAX_ARGUMENT_COUNT, MAX_ENVIRONMENT_BYTES, MAX_ENVIRONMENT_ENTRIES,
    ProcessTreeMode, SubprocessSpec, SubprocessSpecError,
};
pub use supervisor::{
    SubprocessCancellationPhase, SubprocessReport, SubprocessSnapshot, SubprocessSupervisor,
    SubprocessTermination,
};
