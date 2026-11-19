//! Generic process resource-pressure monitoring and bounded worker admission control.

#![forbid(unsafe_code)]

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
compile_error!("plenora-runtime supports only Linux and Windows");

mod config;
mod monitor;
mod sampler;

pub use config::{MemoryPressureConfig, MemoryPressureConfigError};
pub use monitor::{
    MemoryPressureMonitor, MemoryPressureObservation, MemoryPressureObserver,
    MemoryPressureRunReport, MemoryPressureSnapshot, MemoryPressureState,
    NoopMemoryPressureObserver,
};
pub use sampler::{
    MemorySample, MemorySampleError, MemorySampleErrorKind, MemorySampler, ProcessMemorySampler,
};
