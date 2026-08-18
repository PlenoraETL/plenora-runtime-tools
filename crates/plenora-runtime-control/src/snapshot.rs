use plenora_runtime_resources::{
    MemoryPressureMonitor, MemoryPressureObserver, MemoryPressureSnapshot, MemorySampler,
};
use plenora_runtime_subprocess::{SubprocessSnapshot, SubprocessSupervisor};
use plenora_runtime_worker::WorkerAdmissionControl;

/// Type-erased read-only memory-pressure view.
pub trait MemorySnapshotSource: Send + Sync {
    /// Returns the latest bounded process-memory sample.
    fn snapshot(&self) -> MemoryPressureSnapshot;
}

impl<S, A, O> MemorySnapshotSource for MemoryPressureMonitor<S, A, O>
where
    S: MemorySampler + 'static,
    A: WorkerAdmissionControl + 'static,
    O: MemoryPressureObserver + 'static,
{
    fn snapshot(&self) -> MemoryPressureSnapshot {
        MemoryPressureMonitor::snapshot(self)
    }
}

/// Type-erased read-only subprocess-capacity view.
pub trait SubprocessSnapshotSource: Send + Sync {
    /// Returns current bounded child-process capacity and counters.
    fn snapshot(&self) -> SubprocessSnapshot;
}

impl SubprocessSnapshotSource for SubprocessSupervisor {
    fn snapshot(&self) -> SubprocessSnapshot {
        SubprocessSupervisor::snapshot(self)
    }
}
