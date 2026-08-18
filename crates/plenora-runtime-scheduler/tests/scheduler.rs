//! Deterministic scheduling, recovery, saturation, and restart tests.

use std::{
    collections::VecDeque,
    error::Error,
    future::pending,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_scheduler::{
    FixedIntervalPlan, MisfirePolicy, OneShotPlan, ReconciliationResolution, Schedule,
    ScheduleDispatchEffect, ScheduleDispatchError, ScheduleDispatcher, ScheduleId, ScheduleIdError,
    ScheduleOccurrenceId, ScheduleRegistrationError, ScheduleStatus, ScheduledOccurrence,
    SchedulerBuilder, SchedulerConfig, SchedulerConfigError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScriptedOutcome {
    Confirmed,
    NotStarted,
    Unknown,
}

#[derive(Debug)]
struct ScriptedDispatcher {
    outcomes: Mutex<VecDeque<ScriptedOutcome>>,
    attempts: Mutex<Vec<ScheduleOccurrenceId>>,
}

impl ScriptedDispatcher {
    fn new(outcomes: impl IntoIterator<Item = ScriptedOutcome>) -> Self {
        Self {
            outcomes: Mutex::new(outcomes.into_iter().collect()),
            attempts: Mutex::new(Vec::new()),
        }
    }

    fn attempts(&self) -> Vec<ScheduleOccurrenceId> {
        lock(&self.attempts).clone()
    }
}

#[async_trait]
impl ScheduleDispatcher<&'static str> for ScriptedDispatcher {
    async fn dispatch(
        &self,
        occurrence: ScheduledOccurrence<&'static str>,
    ) -> Result<(), ScheduleDispatchError> {
        lock(&self.attempts).push(occurrence.id);
        match lock(&self.outcomes)
            .pop_front()
            .unwrap_or(ScriptedOutcome::Confirmed)
        {
            ScriptedOutcome::Confirmed => Ok(()),
            ScriptedOutcome::NotStarted => Err(ScheduleDispatchError::new(
                ScheduleDispatchEffect::NotStarted,
            )),
            ScriptedOutcome::Unknown => Err(ScheduleDispatchError::new(
                ScheduleDispatchEffect::OutcomeUnknown,
            )),
        }
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

#[test]
fn configuration_and_identifiers_are_bounded() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        SchedulerConfig::new(
            Duration::ZERO,
            Duration::from_secs(1),
            Duration::ZERO,
            1,
            1,
            1
        ),
        Err(SchedulerConfigError::ZeroTickInterval)
    );
    assert_eq!(ScheduleId::new(""), Err(ScheduleIdError::Empty));
    assert_eq!(
        ScheduleId::new("Invalid"),
        Err(ScheduleIdError::InvalidCharacter)
    );

    let mut builder = SchedulerBuilder::new(config(1, 1, 1)?);
    builder.register(once("one", UNIX_EPOCH)?)?;
    assert_eq!(
        builder.register(once("two", UNIX_EPOCH)?),
        Err(ScheduleRegistrationError::CapacityExceeded { limit: 1 })
    );
    Ok(())
}

#[tokio::test]
async fn one_shot_dispatches_once_and_completes() -> Result<(), Box<dyn Error>> {
    let dispatcher = Arc::new(ScriptedDispatcher::new([ScriptedOutcome::Confirmed]));
    let mut builder = SchedulerBuilder::new(config(4, 4, 4)?);
    builder.register(once("one", UNIX_EPOCH)?)?;
    let scheduler = builder.build(Arc::clone(&dispatcher));

    let report = scheduler.tick(UNIX_EPOCH).await;
    let second = scheduler.tick(at(1)).await;
    let snapshots = scheduler.snapshots().await;

    assert_eq!(report.dispatched, 1);
    assert_eq!(second.dispatched, 0);
    assert_eq!(dispatcher.attempts().len(), 1);
    assert_eq!(snapshots[0].status, ScheduleStatus::Completed);
    assert_eq!(snapshots[0].next_due_at, None);
    Ok(())
}

#[tokio::test]
async fn runtime_registration_and_removal_preserve_capacity() -> Result<(), Box<dyn Error>> {
    let dispatcher = Arc::new(ScriptedDispatcher::new([]));
    let builder = SchedulerBuilder::new(config(1, 1, 1)?);
    let scheduler = builder.build(dispatcher);
    let first_id = ScheduleId::new("dynamic")?;

    scheduler
        .add_schedule(Schedule::new(
            first_id.clone(),
            "payload",
            OneShotPlan::new(UNIX_EPOCH),
        ))
        .await?;
    assert_eq!(
        scheduler.add_schedule(once("overflow", UNIX_EPOCH)?).await,
        Err(ScheduleRegistrationError::CapacityExceeded { limit: 1 })
    );
    let removed = scheduler
        .remove_schedule(&first_id)
        .await
        .ok_or_else(|| std::io::Error::other("dynamic schedule was not removed"))?;
    assert_eq!(removed.id, first_id);
    assert!(scheduler.snapshots().await.is_empty());

    scheduler
        .add_schedule(once("replacement", UNIX_EPOCH)?)
        .await?;
    assert_eq!(scheduler.snapshots().await.len(), 1);
    Ok(())
}

#[tokio::test]
async fn catch_up_is_bounded_and_drains_across_ticks() -> Result<(), Box<dyn Error>> {
    let dispatcher = Arc::new(ScriptedDispatcher::new([]));
    let mut builder = SchedulerBuilder::new(config(2, 3, 2)?);
    builder
        .register(interval("repeat", UNIX_EPOCH, 1)?.with_misfire_policy(MisfirePolicy::CatchUp))?;
    let scheduler = builder.build(Arc::clone(&dispatcher));

    let first = scheduler.tick(at(5)).await;
    let second = scheduler.tick(at(5)).await;
    let third = scheduler.tick(at(5)).await;

    assert_eq!(first.dispatched, 2);
    assert!(first.saturated);
    assert_eq!(second.dispatched, 2);
    assert!(second.saturated);
    assert_eq!(third.dispatched, 2);
    assert!(!third.saturated);
    assert_eq!(dispatcher.attempts().len(), 6);
    Ok(())
}

#[tokio::test]
async fn crash_before_effect_retries_same_occurrence_then_recovers() -> Result<(), Box<dyn Error>> {
    let dispatcher = Arc::new(ScriptedDispatcher::new([
        ScriptedOutcome::NotStarted,
        ScriptedOutcome::Confirmed,
    ]));
    let mut builder = SchedulerBuilder::new(config(1, 1, 1)?);
    builder.register(once("recover", UNIX_EPOCH)?)?;
    let scheduler = builder.build(Arc::clone(&dispatcher));

    let failed = scheduler.tick(UNIX_EPOCH).await;
    let recovered = scheduler.tick(UNIX_EPOCH).await;
    let attempts = dispatcher.attempts();

    assert_eq!(failed.not_started_failures, 1);
    assert_eq!(recovered.dispatched, 1);
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0], attempts[1]);
    assert_eq!(
        scheduler.snapshots().await[0].status,
        ScheduleStatus::Completed
    );
    Ok(())
}

#[tokio::test]
async fn unknown_outcome_blocks_until_explicit_reconciliation() -> Result<(), Box<dyn Error>> {
    let dispatcher = Arc::new(ScriptedDispatcher::new([ScriptedOutcome::Unknown]));
    let mut builder = SchedulerBuilder::new(config(1, 1, 1)?);
    let id = ScheduleId::new("unknown")?;
    builder.register(Schedule::new(
        id.clone(),
        "payload",
        OneShotPlan::new(UNIX_EPOCH),
    ))?;
    let scheduler = builder.build(Arc::clone(&dispatcher));

    assert_eq!(scheduler.tick(UNIX_EPOCH).await.reconciliation_required, 1);
    assert_eq!(scheduler.tick(at(1)).await.dispatched, 0);
    assert_eq!(dispatcher.attempts().len(), 1);
    assert_eq!(
        scheduler.snapshots().await[0].status,
        ScheduleStatus::ReconciliationRequired
    );

    let blocked_snapshot = scheduler.snapshots().await[0].clone();
    let restarted_dispatcher = Arc::new(ScriptedDispatcher::new([]));
    let mut restarted_builder = SchedulerBuilder::new(config(1, 1, 1)?);
    restarted_builder.register(
        Schedule::new(id.clone(), "payload", OneShotPlan::new(UNIX_EPOCH))
            .restore_from(&blocked_snapshot)?,
    )?;
    let restarted = restarted_builder.build(Arc::clone(&restarted_dispatcher));
    assert_eq!(restarted.tick(at(2)).await.dispatched, 0);
    assert!(restarted_dispatcher.attempts().is_empty());
    assert_eq!(
        restarted.snapshots().await[0].status,
        ScheduleStatus::ReconciliationRequired
    );

    scheduler
        .resolve_reconciliation(&id, ReconciliationResolution::Confirmed)
        .await?;
    assert_eq!(
        scheduler.snapshots().await[0].status,
        ScheduleStatus::Completed
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dispatch_timeout_is_an_unknown_outcome_not_a_blind_retry() -> Result<(), Box<dyn Error>> {
    let mut builder = SchedulerBuilder::new(SchedulerConfig::new(
        Duration::from_secs(1),
        Duration::from_secs(5),
        Duration::ZERO,
        1,
        1,
        1,
    )?);
    builder.register(once("timeout", UNIX_EPOCH)?)?;
    let scheduler = Arc::new(builder.build(Arc::new(HangingDispatcher)));
    let task_scheduler = Arc::clone(&scheduler);
    let tick = tokio::spawn(async move { task_scheduler.tick(UNIX_EPOCH).await });

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    let report = tick.await?;

    assert_eq!(report.reconciliation_required, 1);
    assert_eq!(
        scheduler.snapshots().await[0].status,
        ScheduleStatus::ReconciliationRequired
    );
    Ok(())
}

#[tokio::test]
async fn restart_from_persisted_cursor_does_not_repeat_confirmed_occurrence()
-> Result<(), Box<dyn Error>> {
    let first_dispatcher = Arc::new(ScriptedDispatcher::new([]));
    let mut first_builder = SchedulerBuilder::new(config(1, 1, 1)?);
    first_builder.register(interval("restart", UNIX_EPOCH, 10)?)?;
    let first = first_builder.build(Arc::clone(&first_dispatcher));
    assert_eq!(first.tick(UNIX_EPOCH).await.dispatched, 1);
    let cursor = first.snapshots().await[0].next_due_at;

    let second_dispatcher = Arc::new(ScriptedDispatcher::new([]));
    let mut second_builder = SchedulerBuilder::new(config(1, 1, 1)?);
    second_builder.register(interval("restart", UNIX_EPOCH, 10)?.with_next_due_at(cursor))?;
    let restarted = second_builder.build(Arc::clone(&second_dispatcher));

    assert_eq!(restarted.tick(at(9)).await.dispatched, 0);
    assert_eq!(restarted.tick(at(10)).await.dispatched, 1);
    assert_eq!(second_dispatcher.attempts()[0].due_at, at(10));
    Ok(())
}

#[tokio::test]
async fn large_misfire_backlog_remains_bounded_and_allocates_no_queue() -> Result<(), Box<dyn Error>>
{
    let dispatcher = Arc::new(ScriptedDispatcher::new([]));
    let mut builder = SchedulerBuilder::new(config(1, 8, 128)?);
    builder.register(interval("soak", UNIX_EPOCH, 1)?.with_misfire_policy(MisfirePolicy::Skip))?;
    let scheduler = builder.build(Arc::clone(&dispatcher));
    let now = at(10_000);
    let mut ticks = 0_usize;
    let mut confirmed_total = 0_usize;

    loop {
        ticks = ticks.saturating_add(1);
        let report = scheduler.tick(now).await;
        confirmed_total = confirmed_total.saturating_add(report.dispatched);
        assert!(report.skipped_misfires <= 128);
        if !report.saturated {
            break;
        }
        if ticks > 100 {
            return Err(std::io::Error::other("bounded backlog did not converge").into());
        }
    }

    assert_eq!(confirmed_total, 1);
    assert_eq!(dispatcher.attempts().len(), 1);
    assert_eq!(dispatcher.attempts()[0].due_at, now);
    assert_eq!(scheduler.snapshots().await.len(), 1);
    assert!(ticks > 1);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn scheduler_loop_stops_on_shutdown() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(ServiceMetadata::new("scheduler-test", "0.1.0", "one"));
    let dispatcher = Arc::new(ScriptedDispatcher::new([]));
    let mut builder = SchedulerBuilder::new(config(1, 1, 1)?);
    builder.register(once("loop", UNIX_EPOCH)?)?;
    let scheduler = Arc::new(builder.build(Arc::clone(&dispatcher)));
    let task_scheduler = Arc::clone(&scheduler);
    let shutdown = runtime.shutdown_signal();
    let task = tokio::spawn(async move { task_scheduler.run(shutdown).await });

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(runtime.request_shutdown());
    let report = task.await?;

    assert!(report.ticks >= 1);
    assert_eq!(report.dispatched, 1);
    Ok(())
}

const UNIX_EPOCH: SystemTime = SystemTime::UNIX_EPOCH;

fn at(seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds)
}

fn config(
    max_schedules: usize,
    max_dispatches_per_tick: usize,
    max_catch_up_per_schedule: usize,
) -> Result<SchedulerConfig, SchedulerConfigError> {
    SchedulerConfig::new(
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::ZERO,
        max_schedules,
        max_dispatches_per_tick,
        max_catch_up_per_schedule,
    )
}

fn once(id: &str, due_at: SystemTime) -> Result<Schedule<&'static str>, ScheduleIdError> {
    Ok(Schedule::new(
        ScheduleId::new(id)?,
        "payload",
        OneShotPlan::new(due_at),
    ))
}

fn interval(
    id: &str,
    first_due_at: SystemTime,
    seconds: u64,
) -> Result<Schedule<&'static str>, Box<dyn Error>> {
    Ok(Schedule::new(
        ScheduleId::new(id)?,
        "payload",
        FixedIntervalPlan::new(first_due_at, Duration::from_secs(seconds))?,
    ))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(value) => value,
        Err(poisoned) => poisoned.into_inner(),
    }
}
