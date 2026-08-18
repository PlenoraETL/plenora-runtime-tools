//! Public scheduler contract, diagnostic, observer, and restoration coverage.

use std::{
    error::Error,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use plenora_runtime_core::Clock;
use plenora_runtime_scheduler::{
    FixedIntervalPlan, FixedIntervalPlanError, MisfirePolicy, OneShotPlan,
    ReconciliationResolution, Schedule, ScheduleBuildError, ScheduleDispatchEffect,
    ScheduleDispatchError, ScheduleDispatcher, ScheduleId, ScheduleIdError, ScheduleOccurrenceId,
    SchedulePlan, ScheduleRegistrationError, ScheduleRestoreError, ScheduleSnapshot,
    ScheduleStatus, ScheduledOccurrence, SchedulerBuilder, SchedulerConfig, SchedulerConfigError,
    SchedulerObserver, SchedulerTickObservation,
};

#[derive(Debug)]
struct ConfirmingDispatcher;

#[async_trait]
impl ScheduleDispatcher<u64> for ConfirmingDispatcher {
    async fn dispatch(
        &self,
        _occurrence: ScheduledOccurrence<u64>,
    ) -> Result<(), ScheduleDispatchError> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct FixedClock(SystemTime);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

#[derive(Debug, Default)]
struct RecordingObserver {
    observations: Mutex<Vec<SchedulerTickObservation>>,
}

impl SchedulerObserver for RecordingObserver {
    fn record(&self, observation: SchedulerTickObservation) {
        lock(&self.observations).push(observation);
    }
}

#[test]
fn configuration_identifiers_and_plans_cover_public_failures() -> Result<(), Box<dyn Error>> {
    let invalid_configs = [
        SchedulerConfigError::ZeroTickInterval,
        SchedulerConfigError::ZeroDispatchTimeout,
        SchedulerConfigError::ZeroScheduleCapacity,
        SchedulerConfigError::ZeroDispatchCapacity,
        SchedulerConfigError::ZeroCatchUpCapacity,
    ];
    for error in invalid_configs {
        assert!(!error.to_string().is_empty());
    }
    assert_eq!(
        SchedulerConfig::new(
            Duration::from_secs(1),
            Duration::ZERO,
            Duration::ZERO,
            1,
            1,
            1,
        ),
        Err(SchedulerConfigError::ZeroDispatchTimeout)
    );
    assert_eq!(
        SchedulerConfig::new(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::ZERO,
            0,
            1,
            1,
        ),
        Err(SchedulerConfigError::ZeroScheduleCapacity)
    );
    assert_eq!(
        SchedulerConfig::new(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::ZERO,
            1,
            0,
            1,
        ),
        Err(SchedulerConfigError::ZeroDispatchCapacity)
    );
    assert_eq!(
        SchedulerConfig::new(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::ZERO,
            1,
            1,
            0,
        ),
        Err(SchedulerConfigError::ZeroCatchUpCapacity)
    );

    let config = config()?;
    assert_eq!(config.tick_interval(), Duration::from_secs(2));
    assert_eq!(config.dispatch_timeout(), Duration::from_secs(3));
    assert_eq!(config.misfire_grace(), Duration::from_secs(4));
    assert_eq!(config.max_schedules(), 5);
    assert_eq!(config.max_dispatches_per_tick(), 6);
    assert_eq!(config.max_catch_up_per_schedule(), 7);
    assert_eq!(config.validate(), Ok(()));

    let id = ScheduleId::new("plenora.schedule")?;
    assert_eq!(id.as_str(), "plenora.schedule");
    assert_eq!(id.to_string(), "plenora.schedule");
    assert_eq!(
        ScheduleId::new("x".repeat(129)),
        Err(ScheduleIdError::TooLong)
    );
    for error in [
        ScheduleIdError::Empty,
        ScheduleIdError::TooLong,
        ScheduleIdError::InvalidCharacter,
    ] {
        assert!(!error.to_string().is_empty());
    }

    assert_eq!(
        FixedIntervalPlan::new(SystemTime::UNIX_EPOCH, Duration::ZERO),
        Err(FixedIntervalPlanError::ZeroInterval)
    );
    assert!(!FixedIntervalPlanError::ZeroInterval.to_string().is_empty());
    let interval = FixedIntervalPlan::new(SystemTime::UNIX_EPOCH, Duration::from_secs(9))?;
    assert_eq!(interval.interval(), Duration::from_secs(9));
    assert_eq!(interval.first_due_at(), SystemTime::UNIX_EPOCH);
    assert_eq!(
        interval.next_after(SystemTime::UNIX_EPOCH),
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(9))
    );
    let once = OneShotPlan::new(SystemTime::UNIX_EPOCH);
    assert_eq!(once.first_due_at(), SystemTime::UNIX_EPOCH);
    assert_eq!(once.next_after(SystemTime::UNIX_EPOCH), None);
    Ok(())
}

#[test]
fn dispatch_errors_preserve_effect_and_redact_source() {
    let plain = ScheduleDispatchError::new(ScheduleDispatchEffect::OutcomeUnknown);
    assert_eq!(plain.effect(), ScheduleDispatchEffect::OutcomeUnknown);
    assert!(plain.to_string().contains("unknown"));
    assert!(Error::source(&plain).is_none());

    let sourced = ScheduleDispatchError::with_source(
        ScheduleDispatchEffect::NotStarted,
        std::io::Error::other("private dispatch source"),
    );
    assert_eq!(sourced.effect(), ScheduleDispatchEffect::NotStarted);
    assert!(Error::source(&sourced).is_some());
    assert!(!format!("{sourced:?}").contains("private dispatch source"));
}

#[tokio::test]
async fn observer_duplicate_restore_and_transition_errors_are_explicit()
-> Result<(), Box<dyn Error>> {
    let observer = Arc::new(RecordingObserver::default());
    let mut builder = SchedulerBuilder::new(config()?);
    let id = ScheduleId::new("observed")?;
    let schedule = Schedule::new(id.clone(), 42_u64, OneShotPlan::new(SystemTime::UNIX_EPOCH));
    assert_eq!(schedule.id(), &id);
    builder.register(schedule)?;
    assert_eq!(
        builder.register(Schedule::new(
            id.clone(),
            43,
            OneShotPlan::new(SystemTime::UNIX_EPOCH),
        )),
        Err(ScheduleRegistrationError::DuplicateId(id.clone()))
    );
    let scheduler = builder.build_with_runtime(
        Arc::new(ConfirmingDispatcher),
        Arc::new(FixedClock(SystemTime::UNIX_EPOCH)),
        Arc::clone(&observer),
    );
    let report = scheduler.tick(SystemTime::UNIX_EPOCH).await;
    assert_eq!(lock(&observer.observations)[0].report, report);
    assert!(
        scheduler
            .remove_schedule(&ScheduleId::new("missing")?)
            .await
            .is_none()
    );
    assert_eq!(
        scheduler
            .resolve_reconciliation(
                &ScheduleId::new("missing")?,
                ReconciliationResolution::Retry
            )
            .await,
        Err(ScheduleBuildError::UnknownSchedule(ScheduleId::new(
            "missing"
        )?))
    );
    assert_eq!(
        scheduler
            .resolve_reconciliation(&id, ReconciliationResolution::Retry)
            .await,
        Err(ScheduleBuildError::ReconciliationNotRequired(id.clone()))
    );
    for error in [
        ScheduleBuildError::UnknownSchedule(id.clone()),
        ScheduleBuildError::ReconciliationNotRequired(id.clone()),
    ] {
        assert!(!error.to_string().is_empty());
    }
    for error in [
        ScheduleRegistrationError::CapacityExceeded { limit: 1 },
        ScheduleRegistrationError::DuplicateId(id),
    ] {
        assert!(!error.to_string().is_empty());
    }
    Ok(())
}

#[test]
fn durable_restore_rejects_mismatch_and_inconsistent_cursor() -> Result<(), Box<dyn Error>> {
    let id = ScheduleId::new("restore")?;
    let other = ScheduleId::new("other")?;
    let schedule = || Schedule::new(id.clone(), 1_u64, OneShotPlan::new(SystemTime::UNIX_EPOCH));
    assert!(matches!(
        schedule().restore_from(&ScheduleSnapshot {
            id: other,
            status: ScheduleStatus::Active,
            next_due_at: Some(SystemTime::UNIX_EPOCH),
        }),
        Err(ScheduleRestoreError::IdentityMismatch)
    ));
    assert!(matches!(
        schedule().restore_from(&ScheduleSnapshot {
            id,
            status: ScheduleStatus::Completed,
            next_due_at: Some(SystemTime::UNIX_EPOCH),
        }),
        Err(ScheduleRestoreError::InconsistentCursor)
    ));
    assert!(
        !ScheduleRestoreError::IdentityMismatch
            .to_string()
            .is_empty()
    );
    assert!(
        !ScheduleRestoreError::InconsistentCursor
            .to_string()
            .is_empty()
    );
    let policies = [
        MisfirePolicy::Skip,
        MisfirePolicy::FireOnce,
        MisfirePolicy::CatchUp,
    ];
    assert_eq!(policies.len(), 3);
    let occurrence = ScheduleOccurrenceId {
        schedule_id: ScheduleId::new("identity")?,
        due_at: SystemTime::UNIX_EPOCH,
    };
    assert_eq!(occurrence.due_at, SystemTime::UNIX_EPOCH);
    Ok(())
}

fn config() -> Result<SchedulerConfig, SchedulerConfigError> {
    SchedulerConfig::new(
        Duration::from_secs(2),
        Duration::from_secs(3),
        Duration::from_secs(4),
        5,
        6,
        7,
    )
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(value) => value,
        Err(poisoned) => poisoned.into_inner(),
    }
}
