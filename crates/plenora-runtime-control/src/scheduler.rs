use async_trait::async_trait;
use plenora_runtime_scheduler::{
    ManualTriggerOutcome, ScheduleBuildError, ScheduleDispatcher, ScheduleId, ScheduleSnapshot,
    Scheduler,
};
use std::time::SystemTime;

/// Type-erased scheduler operations used by the runtime control plane.
#[async_trait]
pub trait SchedulerControl: Send + Sync {
    /// Returns the bounded registered schedule snapshot.
    async fn snapshots(&self) -> Vec<ScheduleSnapshot>;

    /// Pauses one recurring schedule.
    async fn pause(&self, id: &ScheduleId) -> Result<(), ScheduleBuildError>;

    /// Resumes one recurring schedule.
    async fn resume(&self, id: &ScheduleId) -> Result<(), ScheduleBuildError>;

    /// Performs one bounded manual dispatch without advancing the recurring cursor.
    async fn trigger(
        &self,
        id: &ScheduleId,
        triggered_at: SystemTime,
    ) -> Result<ManualTriggerOutcome, ScheduleBuildError>;
}

#[async_trait]
impl<T, D> SchedulerControl for Scheduler<T, D>
where
    T: Clone + Send + Sync + 'static,
    D: ScheduleDispatcher<T> + 'static,
{
    async fn snapshots(&self) -> Vec<ScheduleSnapshot> {
        Scheduler::snapshots(self).await
    }

    async fn pause(&self, id: &ScheduleId) -> Result<(), ScheduleBuildError> {
        self.pause_schedule(id).await
    }

    async fn resume(&self, id: &ScheduleId) -> Result<(), ScheduleBuildError> {
        self.resume_schedule(id).await
    }

    async fn trigger(
        &self,
        id: &ScheduleId,
        triggered_at: SystemTime,
    ) -> Result<ManualTriggerOutcome, ScheduleBuildError> {
        self.trigger_schedule(id, triggered_at).await
    }
}
