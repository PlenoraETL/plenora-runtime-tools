use std::time::Duration;

use plenora_runtime_core::{
    DrainOutcome, RuntimeConfig, RuntimeHandle, RuntimePhase, ServiceMetadata, ShutdownSignal,
};

/// Small harness for initiating and observing coordinated runtime shutdown.
#[derive(Clone, Debug)]
pub struct ShutdownHarness {
    runtime: RuntimeHandle,
}

impl ShutdownHarness {
    /// Creates a harness with stable test metadata and the requested grace period.
    #[must_use]
    pub fn new(grace_period: Duration) -> Self {
        Self::with_metadata(
            ServiceMetadata::new("test-service", "test", "test-instance"),
            grace_period,
        )
    }

    /// Creates a harness with caller-selected process metadata.
    #[must_use]
    pub fn with_metadata(metadata: ServiceMetadata, grace_period: Duration) -> Self {
        Self {
            runtime: RuntimeHandle::with_config(
                metadata,
                RuntimeConfig {
                    shutdown_grace_period: grace_period,
                    ..RuntimeConfig::default()
                },
            ),
        }
    }

    /// Returns the runtime managed by this harness.
    #[must_use]
    pub const fn runtime(&self) -> &RuntimeHandle {
        &self.runtime
    }

    /// Returns a clone of the cooperative shutdown signal.
    #[must_use]
    pub fn signal(&self) -> ShutdownSignal {
        self.runtime.shutdown_signal()
    }

    /// Starts draining without waiting for active tasks.
    #[must_use]
    pub fn trigger(&self) -> bool {
        self.runtime.request_shutdown()
    }

    /// Returns the current lifecycle phase.
    #[must_use]
    pub fn phase(&self) -> RuntimePhase {
        self.runtime.phase()
    }

    /// Starts shutdown and waits for the configured bounded drain.
    pub async fn shutdown(&self) -> DrainOutcome {
        self.runtime.shutdown().await
    }
}
