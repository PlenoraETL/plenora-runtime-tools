use plenora_runtime_core::ShutdownSignal;
use plenora_runtime_worker::WorkerDrainOutcome;

use crate::ApalisWorkerService;

/// Cooperative bridge from a Plenora shutdown signal to an Apalis worker service.
#[derive(Clone, Debug)]
pub struct ApalisShutdownBridge {
    shutdown: ShutdownSignal,
}

impl ApalisShutdownBridge {
    /// Creates a shutdown bridge.
    #[must_use]
    pub const fn new(shutdown: ShutdownSignal) -> Self {
        Self { shutdown }
    }

    /// Returns whether runtime shutdown was already requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    /// Waits for shutdown and atomically closes new worker admission.
    ///
    /// The returned boolean is true only for the caller that begins drain.
    pub async fn wait_and_begin_drain<H, P>(&self, service: &ApalisWorkerService<H, P>) -> bool {
        self.shutdown.cancelled().await;
        service.begin_drain()
    }

    /// Waits for shutdown, closes admission, and drains within `WorkerConfig` grace bounds.
    pub async fn shutdown<H, P>(&self, service: &ApalisWorkerService<H, P>) -> WorkerDrainOutcome {
        self.shutdown.cancelled().await;
        let _started = service.begin_drain();
        service.drain().await
    }
}
