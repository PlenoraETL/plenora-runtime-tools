//! Generic process resource-pressure monitoring and bounded worker admission control.

#![forbid(unsafe_code)]

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
