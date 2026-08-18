//! Pause, resume, and bounded manual-dispatch coverage.

use std::{
    error::Error,
    future::pending,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use plenora_runtime_scheduler::{
    FixedIntervalPlan, ManualTriggerOutcome, Schedule, ScheduleBuildError, ScheduleDispatchError,
    ScheduleDispatcher, ScheduleId, ScheduleStatus, ScheduledOccurrence, SchedulerBuilder,
    SchedulerConfig,
};
use tokio::sync::{Notify, Semaphore};

#[derive(Debug, Default)]
struct ConfirmingDispatcher;

#[async_trait]
impl ScheduleDispatcher<&'static str> for ConfirmingDispatcher {
    async fn dispatch(
        &self,
        _occurrence: ScheduledOccurrence<&'static str>,
    ) -> Result<(), ScheduleDispatchError> {
        Ok(())
    }
}

#[derive(Debug)]
struct GatedDispatcher {
    started: Arc<Semaphore>,
    release: Arc<Notify>,
}

#[async_trait]
impl ScheduleDispatcher<&'static str> for GatedDispatcher {
    async fn dispatch(
        &self,
        _occurrence: ScheduledOccurrence<&'static str>,
    ) -> Result<(), ScheduleDispatchError> {
        self.started.add_permits(1);
        self.release.notified().await;
        Ok(())
    }
}

#[derive(Debug)]
struct HangingDispatcher;

#[async_trait]
impl ScheduleDispatcher<&'static str> for HangingDispatcher {
    async fn dispatch(
        &self,
        _occurrence: ScheduledOccurrence<&'static str>,
    ) -> Result<(), ScheduleDispatchError> {
        pending().await
    }
}

#[tokio::test]
async fn pause_preserves_cursor_manual_trigger_is_independent_and_resume_dispatches()
-> Result<(), Box<dyn Error>> {
    let id = ScheduleId::new("pausable")?;
    let mut builder = SchedulerBuilder::new(config(2, 1, Duration::from_secs(1))?);
    builder.register(Schedule::new(
        id.clone(),
        "payload",
        FixedIntervalPlan::new(SystemTime::UNIX_EPOCH, Duration::from_mins(1))?,
    ))?;
    let scheduler = builder.build(Arc::new(ConfirmingDispatcher));

    scheduler.pause_schedule(&id).await?;
    scheduler.pause_schedule(&id).await?;
    let paused = scheduler.snapshots().await;
    assert_eq!(paused[0].status, ScheduleStatus::Paused);
    assert_eq!(paused[0].next_due_at, Some(SystemTime::UNIX_EPOCH));
    assert_eq!(scheduler.tick(SystemTime::UNIX_EPOCH).await.dispatched, 0);
    assert_eq!(
        scheduler
            .trigger_schedule(&id, SystemTime::UNIX_EPOCH)
            .await?,
        ManualTriggerOutcome::Confirmed
    );
    assert_eq!(scheduler.snapshots().await[0], paused[0]);

    scheduler.resume_schedule(&id).await?;
    scheduler.resume_schedule(&id).await?;
    assert_eq!(scheduler.tick(SystemTime::UNIX_EPOCH).await.dispatched, 1);
    assert_eq!(
        scheduler.snapshots().await[0].status,
        ScheduleStatus::Active
    );
    Ok(())
}

#[tokio::test]
async fn manual_trigger_saturates_immediately_and_reuses_capacity() -> Result<(), Box<dyn Error>> {
    let id = ScheduleId::new("bounded-manual")?;
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Notify::new());
    let dispatcher = Arc::new(GatedDispatcher {
        started: Arc::clone(&started),
        release: Arc::clone(&release),
    });
    let mut builder = SchedulerBuilder::new(config(1, 1, Duration::from_secs(10))?);
    builder.register(Schedule::new(
        id.clone(),
        "payload",
        FixedIntervalPlan::new(SystemTime::UNIX_EPOCH, Duration::from_mins(1))?,
    ))?;
    let scheduler = Arc::new(builder.build(dispatcher));
    let first_scheduler = Arc::clone(&scheduler);
    let first_id = id.clone();
    let first = tokio::spawn(async move {
        first_scheduler
            .trigger_schedule(&first_id, SystemTime::UNIX_EPOCH)
            .await
    });

    let started_permit = started.acquire().await?;
    assert_eq!(
        scheduler
            .trigger_schedule(&id, SystemTime::UNIX_EPOCH + Duration::from_secs(1))
            .await?,
        ManualTriggerOutcome::Saturated
    );
    drop(started_permit);
    release.notify_one();
    assert_eq!(first.await??, ManualTriggerOutcome::Confirmed);

    let third_scheduler = Arc::clone(&scheduler);
    let third_id = id.clone();
    let third = tokio::spawn(async move {
        third_scheduler
            .trigger_schedule(&third_id, SystemTime::UNIX_EPOCH + Duration::from_secs(2))
            .await
    });
    let third_started = started.acquire().await?;
    drop(third_started);
    release.notify_one();
    assert_eq!(third.await??, ManualTriggerOutcome::Confirmed);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn manual_timeout_is_reported_as_uncertain() -> Result<(), Box<dyn Error>> {
    let id = ScheduleId::new("manual-timeout")?;
    let mut builder = SchedulerBuilder::new(config(1, 1, Duration::from_secs(5))?);
    builder.register(Schedule::new(
        id.clone(),
        "payload",
        FixedIntervalPlan::new(SystemTime::UNIX_EPOCH, Duration::from_mins(1))?,
    ))?;
    let scheduler = Arc::new(builder.build(Arc::new(HangingDispatcher)));
    let task_scheduler = Arc::clone(&scheduler);
    let task_id = id.clone();
    let task = tokio::spawn(async move {
        task_scheduler
            .trigger_schedule(&task_id, SystemTime::UNIX_EPOCH)
            .await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(task.await??, ManualTriggerOutcome::TimedOut);

    assert_eq!(
        scheduler.pause_schedule(&ScheduleId::new("missing")?).await,
        Err(ScheduleBuildError::UnknownSchedule(ScheduleId::new(
            "missing"
        )?))
    );
    Ok(())
}

fn config(
    schedules: usize,
    dispatches: usize,
    dispatch_timeout: Duration,
) -> Result<SchedulerConfig, plenora_runtime_scheduler::SchedulerConfigError> {
    SchedulerConfig::new(
        Duration::from_secs(1),
        dispatch_timeout,
        Duration::ZERO,
        schedules,
        dispatches,
        1,
    )
}
