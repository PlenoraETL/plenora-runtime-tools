use std::{
    collections::BTreeSet,
    error::Error,
    fmt::{self, Display, Formatter},
    sync::Arc,
    time::SystemTime,
};

use plenora_runtime_core::{Clock, ShutdownSignal, SystemClock};
use tokio::{
    sync::{Mutex, Semaphore},
    time::{MissedTickBehavior, interval, timeout},
};

use crate::{
    ScheduleDispatchEffect, ScheduleDispatcher, ScheduleId, ScheduleOccurrenceId, SchedulePlan,
    ScheduledOccurrence, SchedulerConfig,
};

/// Behavior when an occurrence is older than the configured misfire grace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MisfirePolicy {
    /// Drop every overdue occurrence and advance to the first future instant.
    Skip,
    /// Dispatch one overdue occurrence and skip the remaining backlog after confirmation.
    FireOnce,
    /// Dispatch overdue occurrences within configured per-tick bounds.
    CatchUp,
}

/// Current lifecycle state of one registered schedule.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScheduleStatus {
    /// The schedule may dispatch its next occurrence.
    Active,
    /// Automatic dispatch is suspended without changing the durable cursor.
    Paused,
    /// The plan has no future occurrence.
    Completed,
    /// A dispatch outcome was unknown and requires an explicit owner decision.
    ReconciliationRequired,
}

/// Explicit resolution for an unknown dispatch outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconciliationResolution {
    /// The owner confirmed the remote effect, so the cursor advances.
    Confirmed,
    /// The owner confirmed no remote effect, so the same occurrence may retry.
    Retry,
}

/// One application command paired with a reusable scheduling plan.
pub struct Schedule<T> {
    id: ScheduleId,
    payload: T,
    plan: Arc<dyn SchedulePlan>,
    misfire_policy: MisfirePolicy,
    next_due_at: Option<SystemTime>,
    status: ScheduleStatus,
}

impl<T> Schedule<T> {
    /// Creates an active schedule at its plan's first due instant.
    #[must_use]
    pub fn new<P>(id: ScheduleId, payload: T, plan: P) -> Self
    where
        P: SchedulePlan + 'static,
    {
        let next_due_at = Some(plan.first_due_at());
        Self {
            id,
            payload,
            plan: Arc::new(plan),
            misfire_policy: MisfirePolicy::FireOnce,
            next_due_at,
            status: ScheduleStatus::Active,
        }
    }

    /// Sets the overdue-occurrence policy.
    #[must_use]
    pub const fn with_misfire_policy(mut self, policy: MisfirePolicy) -> Self {
        self.misfire_policy = policy;
        self
    }

    /// Restores the next due cursor from application-owned persistence.
    #[must_use]
    pub const fn with_next_due_at(mut self, next_due_at: Option<SystemTime>) -> Self {
        self.next_due_at = next_due_at;
        self.status = if next_due_at.is_some() {
            ScheduleStatus::Active
        } else {
            ScheduleStatus::Completed
        };
        self
    }

    /// Restores a complete durable cursor, including reconciliation-required state.
    ///
    /// # Errors
    ///
    /// Returns an error for a different identity or an inconsistent status/cursor pair.
    pub fn restore_from(
        mut self,
        snapshot: &ScheduleSnapshot,
    ) -> Result<Self, ScheduleRestoreError> {
        if self.id != snapshot.id {
            return Err(ScheduleRestoreError::IdentityMismatch);
        }
        let cursor_is_valid = match snapshot.status {
            ScheduleStatus::Active
            | ScheduleStatus::Paused
            | ScheduleStatus::ReconciliationRequired => snapshot.next_due_at.is_some(),
            ScheduleStatus::Completed => snapshot.next_due_at.is_none(),
        };
        if !cursor_is_valid {
            return Err(ScheduleRestoreError::InconsistentCursor);
        }
        self.status = snapshot.status;
        self.next_due_at = snapshot.next_due_at;
        Ok(self)
    }

    /// Returns the stable schedule identity.
    #[must_use]
    pub const fn id(&self) -> &ScheduleId {
        &self.id
    }
}

struct ScheduleEntry<T> {
    id: ScheduleId,
    payload: T,
    plan: Arc<dyn SchedulePlan>,
    misfire_policy: MisfirePolicy,
    next_due_at: Option<SystemTime>,
    status: ScheduleStatus,
}

/// Immutable schedule state suitable for application-owned persistence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleSnapshot {
    /// Stable schedule identity.
    pub id: ScheduleId,
    /// Current schedule lifecycle state.
    pub status: ScheduleStatus,
    /// Next logical occurrence, including a blocked unknown occurrence.
    pub next_due_at: Option<SystemTime>,
}

/// One bounded scheduler tick report.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SchedulerTickReport {
    /// Active schedules examined.
    pub schedules_examined: usize,
    /// Confirmed dispatches.
    pub dispatched: usize,
    /// Safe-to-retry failures whose effects never started.
    pub not_started_failures: usize,
    /// Occurrences moved into reconciliation-required state.
    pub reconciliation_required: usize,
    /// Overdue occurrences skipped by policy.
    pub skipped_misfires: usize,
    /// Whether a global or per-schedule bound left due work for a later tick.
    pub saturated: bool,
}

/// Redaction-safe tick observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerTickObservation {
    /// Wall-clock instant supplied to the tick.
    pub observed_at: SystemTime,
    /// Bounded tick counters.
    pub report: SchedulerTickReport,
}

/// Non-blocking boundary for scheduler metrics and operational persistence.
pub trait SchedulerObserver: Send + Sync {
    /// Records one tick observation.
    fn record(&self, observation: SchedulerTickObservation);
}

/// Observer that intentionally discards tick observations.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopSchedulerObserver;

impl SchedulerObserver for NoopSchedulerObserver {
    fn record(&self, _observation: SchedulerTickObservation) {}
}

/// Bounded scheduler-loop completion report.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SchedulerRunReport {
    /// Number of ticks completed before shutdown.
    pub ticks: u64,
    /// Aggregate confirmed dispatches.
    pub dispatched: u64,
    /// Aggregate safe-to-retry dispatch failures.
    pub not_started_failures: u64,
    /// Aggregate unknown outcomes requiring reconciliation.
    pub reconciliation_required: u64,
}

/// Result of one bounded manual invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManualTriggerOutcome {
    /// The dispatcher confirmed the invocation.
    Confirmed,
    /// The dispatcher proved that no external effect began.
    NotStarted,
    /// The dispatcher returned an uncertain external effect.
    OutcomeUnknown,
    /// The dispatch deadline elapsed, so the external effect is uncertain.
    TimedOut,
    /// All configured manual-dispatch slots were already in use.
    Saturated,
    /// An unresolved automatic occurrence prevents another invocation.
    ReconciliationRequired,
}

/// Builder that enforces registry capacity and duplicate identities before startup.
pub struct SchedulerBuilder<T> {
    config: SchedulerConfig,
    ids: BTreeSet<ScheduleId>,
    schedules: Vec<Schedule<T>>,
}

impl<T> SchedulerBuilder<T> {
    /// Creates an empty bounded registry builder.
    #[must_use]
    pub const fn new(config: SchedulerConfig) -> Self {
        Self {
            config,
            ids: BTreeSet::new(),
            schedules: Vec::new(),
        }
    }

    /// Registers one schedule.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate identity or exhausted registry capacity.
    pub fn register(&mut self, schedule: Schedule<T>) -> Result<(), ScheduleRegistrationError> {
        if self.schedules.len() >= self.config.max_schedules() {
            return Err(ScheduleRegistrationError::CapacityExceeded {
                limit: self.config.max_schedules(),
            });
        }
        if !self.ids.insert(schedule.id.clone()) {
            return Err(ScheduleRegistrationError::DuplicateId(schedule.id));
        }
        self.schedules.push(schedule);
        Ok(())
    }

    /// Builds a scheduler with the system clock and no-op observer.
    #[must_use]
    pub fn build<D>(self, dispatcher: Arc<D>) -> Scheduler<T, D>
    where
        D: ScheduleDispatcher<T>,
    {
        self.build_with_runtime(
            dispatcher,
            Arc::new(SystemClock),
            Arc::new(NoopSchedulerObserver),
        )
    }

    /// Builds a scheduler with deterministic clock and observer boundaries.
    #[must_use]
    pub fn build_with_runtime<D, C, O>(
        self,
        dispatcher: Arc<D>,
        clock: Arc<C>,
        observer: Arc<O>,
    ) -> Scheduler<T, D>
    where
        D: ScheduleDispatcher<T>,
        C: Clock + 'static,
        O: SchedulerObserver + 'static,
    {
        let entries = self.schedules.into_iter().map(schedule_entry).collect();
        let manual_capacity = self.config.max_dispatches_per_tick();
        Scheduler {
            config: self.config,
            dispatcher,
            clock,
            observer,
            entries: Mutex::new(entries),
            manual_permits: Arc::new(Semaphore::new(manual_capacity)),
        }
    }
}

/// Backend-neutral, bounded scheduler.
pub struct Scheduler<T, D> {
    config: SchedulerConfig,
    dispatcher: Arc<D>,
    clock: Arc<dyn Clock>,
    observer: Arc<dyn SchedulerObserver>,
    entries: Mutex<Vec<ScheduleEntry<T>>>,
    manual_permits: Arc<Semaphore>,
}

impl<T, D> Scheduler<T, D>
where
    T: Clone + Send + Sync + 'static,
    D: ScheduleDispatcher<T>,
{
    /// Adds one schedule at runtime under the same bounded registry rules as startup.
    ///
    /// The application remains responsible for persisting the definition through its database
    /// boundary before treating this in-memory update as durable.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate identity or exhausted registry capacity.
    pub async fn add_schedule(
        &self,
        schedule: Schedule<T>,
    ) -> Result<(), ScheduleRegistrationError> {
        let mut entries = self.entries.lock().await;
        if entries.len() >= self.config.max_schedules() {
            return Err(ScheduleRegistrationError::CapacityExceeded {
                limit: self.config.max_schedules(),
            });
        }
        if entries.iter().any(|entry| entry.id == schedule.id) {
            return Err(ScheduleRegistrationError::DuplicateId(schedule.id));
        }
        entries.push(schedule_entry(schedule));
        Ok(())
    }

    /// Removes one in-memory schedule and returns its final cursor snapshot.
    ///
    /// The application owns any corresponding durable deletion.
    pub async fn remove_schedule(&self, id: &ScheduleId) -> Option<ScheduleSnapshot> {
        let mut entries = self.entries.lock().await;
        let position = entries.iter().position(|entry| &entry.id == id)?;
        let entry = entries.remove(position);
        Some(ScheduleSnapshot {
            id: entry.id,
            status: entry.status,
            next_due_at: entry.next_due_at,
        })
    }

    /// Executes one deterministic wall-clock tick.
    pub async fn tick(&self, now: SystemTime) -> SchedulerTickReport {
        let mut entries = self.entries.lock().await;
        let mut report = SchedulerTickReport::default();
        let mut remaining_global = self.config.max_dispatches_per_tick();

        for entry in &mut *entries {
            if entry.status != ScheduleStatus::Active {
                continue;
            }
            report.schedules_examined = report.schedules_examined.saturating_add(1);
            self.process_entry(entry, now, &mut remaining_global, &mut report)
                .await;
            if remaining_global == 0 {
                report.saturated |= entries_have_due(&entries, now);
                break;
            }
        }

        drop(entries);
        self.observer.record(SchedulerTickObservation {
            observed_at: now,
            report,
        });
        report
    }

    /// Runs periodic ticks until shutdown.
    pub async fn run(&self, shutdown: ShutdownSignal) -> SchedulerRunReport {
        let mut ticker = interval(self.config.tick_interval());
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut run = SchedulerRunReport::default();
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return run,
                _instant = ticker.tick() => {
                    let report = self.tick(self.clock.now()).await;
                    run.ticks = run.ticks.saturating_add(1);
                    run.dispatched = run.dispatched.saturating_add(report.dispatched as u64);
                    run.not_started_failures = run.not_started_failures
                        .saturating_add(report.not_started_failures as u64);
                    run.reconciliation_required = run.reconciliation_required
                        .saturating_add(report.reconciliation_required as u64);
                }
            }
        }
    }

    /// Returns immutable state ordered by registration.
    pub async fn snapshots(&self) -> Vec<ScheduleSnapshot> {
        self.entries
            .lock()
            .await
            .iter()
            .map(|entry| ScheduleSnapshot {
                id: entry.id.clone(),
                status: entry.status,
                next_due_at: entry.next_due_at,
            })
            .collect()
    }

    /// Suspends automatic dispatch without changing the next due cursor.
    ///
    /// Repeating the operation for an already paused schedule is idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown, completed, or reconciliation-blocked schedule.
    pub async fn pause_schedule(&self, id: &ScheduleId) -> Result<(), ScheduleBuildError> {
        let mut entries = self.entries.lock().await;
        let entry = find_entry(&mut entries, id)?;
        match entry.status {
            ScheduleStatus::Active | ScheduleStatus::Paused => {
                entry.status = ScheduleStatus::Paused;
                Ok(())
            }
            ScheduleStatus::Completed | ScheduleStatus::ReconciliationRequired => {
                Err(ScheduleBuildError::TransitionNotAllowed(id.clone()))
            }
        }
    }

    /// Resumes automatic dispatch from the unchanged next due cursor.
    ///
    /// Repeating the operation for an already active schedule is idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown, completed, or reconciliation-blocked schedule.
    pub async fn resume_schedule(&self, id: &ScheduleId) -> Result<(), ScheduleBuildError> {
        let mut entries = self.entries.lock().await;
        let entry = find_entry(&mut entries, id)?;
        match entry.status {
            ScheduleStatus::Active | ScheduleStatus::Paused => {
                entry.status = ScheduleStatus::Active;
                Ok(())
            }
            ScheduleStatus::Completed | ScheduleStatus::ReconciliationRequired => {
                Err(ScheduleBuildError::TransitionNotAllowed(id.clone()))
            }
        }
    }

    /// Dispatches one application-requested occurrence without advancing the schedule cursor.
    ///
    /// Manual dispatch is allowed for active, paused, and completed definitions. It is bounded by
    /// `max_dispatches_per_tick`; saturation is reported immediately instead of queueing an
    /// unbounded number of requests. An unknown or timed-out result must be reconciled by the
    /// application using the deterministic occurrence identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the schedule identity is unknown.
    pub async fn trigger_schedule(
        &self,
        id: &ScheduleId,
        triggered_at: SystemTime,
    ) -> Result<ManualTriggerOutcome, ScheduleBuildError> {
        let (payload, status) = {
            let mut entries = self.entries.lock().await;
            let entry = find_entry(&mut entries, id)?;
            (entry.payload.clone(), entry.status)
        };
        if status == ScheduleStatus::ReconciliationRequired {
            return Ok(ManualTriggerOutcome::ReconciliationRequired);
        }
        let Ok(_permit) = Arc::clone(&self.manual_permits).try_acquire_owned() else {
            return Ok(ManualTriggerOutcome::Saturated);
        };
        let occurrence = ScheduledOccurrence {
            id: ScheduleOccurrenceId {
                schedule_id: id.clone(),
                due_at: triggered_at,
            },
            due_at: triggered_at,
            payload,
        };
        let outcome = match timeout(
            self.config.dispatch_timeout(),
            self.dispatcher.dispatch(occurrence),
        )
        .await
        {
            Ok(Ok(())) => ManualTriggerOutcome::Confirmed,
            Ok(Err(error)) if error.effect() == ScheduleDispatchEffect::NotStarted => {
                ManualTriggerOutcome::NotStarted
            }
            Ok(Err(_)) => ManualTriggerOutcome::OutcomeUnknown,
            Err(_) => ManualTriggerOutcome::TimedOut,
        };
        Ok(outcome)
    }

    /// Resolves one unknown dispatch outcome.
    ///
    /// # Errors
    ///
    /// Returns an error when the schedule is missing or not awaiting reconciliation.
    pub async fn resolve_reconciliation(
        &self,
        id: &ScheduleId,
        resolution: ReconciliationResolution,
    ) -> Result<(), ScheduleBuildError> {
        let mut entries = self.entries.lock().await;
        let entry = entries
            .iter_mut()
            .find(|entry| &entry.id == id)
            .ok_or_else(|| ScheduleBuildError::UnknownSchedule(id.clone()))?;
        if entry.status != ScheduleStatus::ReconciliationRequired {
            return Err(ScheduleBuildError::ReconciliationNotRequired(id.clone()));
        }
        if resolution == ReconciliationResolution::Confirmed {
            advance_once(entry);
        } else {
            entry.status = ScheduleStatus::Active;
        }
        Ok(())
    }

    async fn process_entry(
        &self,
        entry: &mut ScheduleEntry<T>,
        now: SystemTime,
        remaining_global: &mut usize,
        report: &mut SchedulerTickReport,
    ) {
        let mut attempted = 0_usize;
        while entry.status == ScheduleStatus::Active
            && entry.next_due_at.is_some_and(|due| due <= now)
            && *remaining_global > 0
            && attempted < self.config.max_catch_up_per_schedule()
        {
            let Some(due_at) = entry.next_due_at else {
                return;
            };
            let misfired = now
                .duration_since(due_at)
                .is_ok_and(|late| late > self.config.misfire_grace());
            attempted = attempted.saturating_add(1);
            if misfired && entry.misfire_policy == MisfirePolicy::Skip {
                report.skipped_misfires = report.skipped_misfires.saturating_add(1);
                advance_once(entry);
                continue;
            }

            *remaining_global = remaining_global.saturating_sub(1);
            let occurrence = ScheduledOccurrence {
                id: ScheduleOccurrenceId {
                    schedule_id: entry.id.clone(),
                    due_at,
                },
                due_at,
                payload: entry.payload.clone(),
            };
            match timeout(
                self.config.dispatch_timeout(),
                self.dispatcher.dispatch(occurrence),
            )
            .await
            {
                Ok(Ok(())) => {
                    report.dispatched = report.dispatched.saturating_add(1);
                    advance_once(entry);
                    if misfired && entry.misfire_policy == MisfirePolicy::FireOnce {
                        let remaining = self
                            .config
                            .max_catch_up_per_schedule()
                            .saturating_sub(attempted);
                        attempted = attempted
                            .saturating_add(skip_through_now(entry, now, remaining, report));
                        break;
                    }
                }
                Ok(Err(error)) if error.effect() == ScheduleDispatchEffect::NotStarted => {
                    report.not_started_failures = report.not_started_failures.saturating_add(1);
                    break;
                }
                Ok(Err(_)) | Err(_) => {
                    entry.status = ScheduleStatus::ReconciliationRequired;
                    report.reconciliation_required =
                        report.reconciliation_required.saturating_add(1);
                    break;
                }
            }
        }
        if attempted >= self.config.max_catch_up_per_schedule()
            && entry.next_due_at.is_some_and(|due| due <= now)
        {
            report.saturated = true;
        }
    }
}

fn schedule_entry<T>(schedule: Schedule<T>) -> ScheduleEntry<T> {
    ScheduleEntry {
        id: schedule.id,
        payload: schedule.payload,
        plan: schedule.plan,
        misfire_policy: schedule.misfire_policy,
        next_due_at: schedule.next_due_at,
        status: schedule.status,
    }
}

fn advance_once<T>(entry: &mut ScheduleEntry<T>) {
    entry.next_due_at = entry.next_due_at.and_then(|previous| {
        entry
            .plan
            .next_after(previous)
            .filter(|next| *next > previous)
    });
    entry.status = if entry.next_due_at.is_some() {
        ScheduleStatus::Active
    } else {
        ScheduleStatus::Completed
    };
}

fn skip_through_now<T>(
    entry: &mut ScheduleEntry<T>,
    now: SystemTime,
    limit: usize,
    report: &mut SchedulerTickReport,
) -> usize {
    let mut skipped = 0_usize;
    while skipped < limit && entry.next_due_at.is_some_and(|due| due <= now) {
        report.skipped_misfires = report.skipped_misfires.saturating_add(1);
        skipped = skipped.saturating_add(1);
        advance_once(entry);
    }
    skipped
}

fn entries_have_due<T>(entries: &[ScheduleEntry<T>], now: SystemTime) -> bool {
    entries.iter().any(|entry| {
        entry.status == ScheduleStatus::Active && entry.next_due_at.is_some_and(|due| due <= now)
    })
}

/// Schedule registration failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScheduleRegistrationError {
    /// The bounded registry is full.
    CapacityExceeded {
        /// Configured schedule capacity.
        limit: usize,
    },
    /// A stable identity was registered twice.
    DuplicateId(ScheduleId),
}

impl Display for ScheduleRegistrationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapacityExceeded { limit } => {
                write!(formatter, "scheduler capacity {limit} is exhausted")
            }
            Self::DuplicateId(_) => {
                formatter.write_str("schedule identifier is already registered")
            }
        }
    }
}

impl Error for ScheduleRegistrationError {}

/// Invalid scheduler state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScheduleBuildError {
    /// No registered schedule matched the identity.
    UnknownSchedule(ScheduleId),
    /// The schedule has no unknown outcome to resolve.
    ReconciliationNotRequired(ScheduleId),
    /// The schedule lifecycle does not permit the requested transition.
    TransitionNotAllowed(ScheduleId),
}

impl Display for ScheduleBuildError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnknownSchedule(_) => "schedule is not registered",
            Self::ReconciliationNotRequired(_) => "schedule does not require reconciliation",
            Self::TransitionNotAllowed(_) => "schedule state does not allow this transition",
        })
    }
}

impl Error for ScheduleBuildError {}

fn find_entry<'a, T>(
    entries: &'a mut [ScheduleEntry<T>],
    id: &ScheduleId,
) -> Result<&'a mut ScheduleEntry<T>, ScheduleBuildError> {
    entries
        .iter_mut()
        .find(|entry| &entry.id == id)
        .ok_or_else(|| ScheduleBuildError::UnknownSchedule(id.clone()))
}

/// Invalid durable schedule restoration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScheduleRestoreError {
    /// The durable snapshot belongs to another schedule.
    IdentityMismatch,
    /// Active/reconciliation state lacked a cursor or completed state retained one.
    InconsistentCursor,
}

impl Display for ScheduleRestoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::IdentityMismatch => "schedule snapshot identity does not match",
            Self::InconsistentCursor => "schedule snapshot status and cursor are inconsistent",
        })
    }
}

impl Error for ScheduleRestoreError {}
