use std::{
    collections::BTreeSet,
    error::Error,
    fmt::{self, Display, Formatter},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use plenora_runtime_core::{Clock, ShutdownSignal, SystemClock};
use tokio::{
    sync::{Mutex, Semaphore},
    time::{MissedTickBehavior, interval, timeout},
};

use crate::{
    ScheduleDispatchEffect, ScheduleDispatchError, ScheduleDispatcher, ScheduleId,
    ScheduleOccurrenceId, SchedulePlan, ScheduledOccurrence, SchedulerConfig,
};

/// Behavior when an occurrence is older than the configured misfire grace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MisfirePolicy {
    /// Drop every occurrence older than the misfire grace without dispatching it.
    ///
    /// An occurrence that is due but still inside the grace is not overdue, so it is dispatched
    /// normally rather than skipped.
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
    /// An occurrence has not settled, or its outcome was unknown and needs an owner decision.
    ///
    /// The two share one state because both mean the external effect is undetermined. They are
    /// distinguished internally: an attempt counts as unsettled from the moment it is issued until
    /// its outcome has been applied, and reconciling it is refused for that whole window, because
    /// resolving an operation that cannot be cancelled could start a second effect for the same
    /// occurrence. Abandoning the tick ends the attempt for the scheduler without producing an
    /// outcome, which releases that refusal and hands the undetermined effect to the owner.
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
    skip_backlog_through: Option<SystemTime>,
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
            skip_backlog_through: None,
        }
    }

    /// Sets the overdue-occurrence policy.
    #[must_use]
    pub const fn with_misfire_policy(mut self, policy: MisfirePolicy) -> Self {
        self.misfire_policy = policy;
        self
    }

    /// Restores the next due cursor from application-owned persistence.
    ///
    /// Replacing the cursor also discards any abandonment boundary, because that boundary belongs
    /// to the cursor it was recorded against.
    #[must_use]
    pub const fn with_next_due_at(mut self, next_due_at: Option<SystemTime>) -> Self {
        self.next_due_at = next_due_at;
        self.skip_backlog_through = None;
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
        // A suppression boundary without a cursor to suppress is durable state that cannot be
        // reproduced by the scheduler, so it is rejected rather than silently ignored.
        if snapshot.next_due_at.is_none() && snapshot.skip_backlog_through.is_some() {
            return Err(ScheduleRestoreError::InconsistentCursor);
        }
        self.status = snapshot.status;
        self.next_due_at = snapshot.next_due_at;
        self.skip_backlog_through = snapshot.skip_backlog_through;
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
    /// Newest instant whose overdue backlog `FireOnce` already decided to abandon.
    ///
    /// The suppression survives across ticks so a backlog larger than the per-schedule catch-up
    /// bound cannot be dispatched again on a later tick.
    skip_backlog_through: Option<SystemTime>,
    /// Identifies the dispatch currently issued for this entry, when there is one.
    ///
    /// The registry lock is released while an application dispatcher runs, so the outcome is only
    /// applied if this still identifies the same dispatch. Pinning an unresolved identity already
    /// prevents the definition from being replaced underneath a running attempt, so this is the
    /// second line of that defence rather than the first.
    dispatch_token: Option<u64>,
}

/// Registered schedules plus the dispatches issued for them that have not settled yet.
///
/// The running set is deliberately kept beside the entries rather than inside them, so it cannot
/// be lost with the schedule it belongs to. A record stays until the attempt settles for this
/// scheduler: its outcome is applied, or the tick that issued it is abandoned. Settled records are
/// discarded whenever the registry is asked to admit work or change membership, and the set is
/// explicitly capped at the configured schedule count so concurrent ticks cannot grow it without
/// bound.
struct Registry<T> {
    entries: Vec<ScheduleEntry<T>>,
    running: Vec<RunningDispatch>,
}

/// One dispatch that is still executing for a schedule identity.
struct RunningDispatch {
    id: ScheduleId,
    active: Arc<AtomicBool>,
}

impl<T> Registry<T> {
    const fn new(entries: Vec<ScheduleEntry<T>>) -> Self {
        Self {
            entries,
            running: Vec::new(),
        }
    }

    fn find_mut(&mut self, id: &ScheduleId) -> Option<&mut ScheduleEntry<T>> {
        self.entries.iter_mut().find(|entry| &entry.id == id)
    }

    /// Returns whether this scheduler is still driving an occurrence of this identity.
    ///
    /// This tracks the attempt, not the external effect: an abandoned tick stops being counted
    /// even though whatever it started may still be running somewhere.
    fn is_running(&self, id: &ScheduleId) -> bool {
        self.running
            .iter()
            .any(|dispatch| &dispatch.id == id && dispatch.active.load(Ordering::Acquire))
    }

    /// Records a started dispatch.
    fn begin(&mut self, id: ScheduleId, active: Arc<AtomicBool>) {
        self.prune();
        self.running.push(RunningDispatch { id, active });
    }

    /// Discards the records of attempts that have already settled.
    fn prune(&mut self) {
        self.running
            .retain(|dispatch| dispatch.active.load(Ordering::Acquire));
    }

    /// Returns whether another dispatch may be issued without exceeding the configured bound.
    fn has_dispatch_capacity(&self, limit: usize) -> bool {
        self.running.len() < limit
    }
}

/// Clears the liveness flag of an in-flight dispatch, including when the tick future is dropped.
struct DispatchGuard {
    active: Arc<AtomicBool>,
}

impl Drop for DispatchGuard {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
    }
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
    /// Newest instant whose overdue backlog a `FireOnce` dispatch already decided to abandon.
    ///
    /// Persisting this boundary with the cursor prevents a restart in the middle of a large
    /// abandoned backlog from dispatching a second overdue occurrence. It is honoured only by a
    /// `FireOnce` schedule, so it can never cross into a definition rebuilt under another misfire
    /// policy.
    ///
    /// Like `next_due_at`, it is trusted durable state: restoration checks that the cursor fields
    /// are mutually consistent but does not second-guess their values against the wall clock, so a
    /// clock that steps backwards cannot make the scheduler replay an interval it already
    /// abandoned.
    pub skip_backlog_through: Option<SystemTime>,
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
            entries: Mutex::new(Registry::new(entries)),
            manual_permits: Arc::new(Semaphore::new(manual_capacity)),
            dispatch_tokens: AtomicU64::new(0),
        }
    }
}

/// Backend-neutral, bounded scheduler.
pub struct Scheduler<T, D> {
    config: SchedulerConfig,
    dispatcher: Arc<D>,
    clock: Arc<dyn Clock>,
    observer: Arc<dyn SchedulerObserver>,
    entries: Mutex<Registry<T>>,
    manual_permits: Arc<Semaphore>,
    dispatch_tokens: AtomicU64,
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
    /// Returns an error for exhausted registry capacity, or for an identity that is already
    /// registered or whose occurrence this scheduler is still driving. An identity holding an
    /// unresolved occurrence stays registered, because such a schedule cannot be removed, so it is
    /// rejected here as a duplicate until the owner reconciles it.
    pub async fn add_schedule(
        &self,
        schedule: Schedule<T>,
    ) -> Result<(), ScheduleRegistrationError> {
        let mut registry = self.entries.lock().await;
        registry.prune();
        if registry.entries.len() >= self.config.max_schedules() {
            return Err(ScheduleRegistrationError::CapacityExceeded {
                limit: self.config.max_schedules(),
            });
        }
        // An identity whose previous occurrence is still executing is not free: reusing it now
        // would let a fresh definition dispatch the same occurrence alongside the running one.
        if registry.entries.iter().any(|entry| entry.id == schedule.id)
            || registry.is_running(&schedule.id)
        {
            return Err(ScheduleRegistrationError::DuplicateId(schedule.id));
        }
        registry.entries.push(schedule_entry(schedule));
        Ok(())
    }

    /// Removes one in-memory schedule and returns its final cursor snapshot.
    ///
    /// The application owns any corresponding durable deletion.
    ///
    /// A schedule holding an unresolved occurrence cannot be removed. Removing it would discard
    /// the only record that the occurrence is undetermined, after which the identity could be
    /// registered again and dispatch that same occurrence alongside the first effect. Reconcile
    /// first; an attempt this scheduler is still driving settles within its dispatch timeout.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown schedule, or for one awaiting reconciliation.
    pub async fn remove_schedule(
        &self,
        id: &ScheduleId,
    ) -> Result<ScheduleSnapshot, ScheduleBuildError> {
        let mut registry = self.entries.lock().await;
        registry.prune();
        let position = registry
            .entries
            .iter()
            .position(|entry| &entry.id == id)
            .ok_or_else(|| ScheduleBuildError::UnknownSchedule(id.clone()))?;
        if registry.entries[position].status == ScheduleStatus::ReconciliationRequired {
            return Err(ScheduleBuildError::TransitionNotAllowed(id.clone()));
        }
        let entry = registry.entries.remove(position);
        Ok(ScheduleSnapshot {
            id: entry.id,
            status: entry.status,
            next_due_at: entry.next_due_at,
            skip_backlog_through: entry.skip_backlog_through,
        })
    }

    /// Executes one deterministic wall-clock tick.
    pub async fn tick(&self, now: SystemTime) -> SchedulerTickReport {
        self.tick_observing(now, None).await
    }

    /// Executes one tick that starts no further occurrence once it observes shutdown.
    ///
    /// Admission is a check before each dispatch, not an atomic handshake: an occurrence whose
    /// dispatch had already begun when shutdown was requested still runs to its configured
    /// timeout.
    async fn tick_observing(
        &self,
        now: SystemTime,
        shutdown: Option<&ShutdownSignal>,
    ) -> SchedulerTickReport {
        let mut report = SchedulerTickReport::default();
        let mut remaining_global = self.config.max_dispatches_per_tick();
        let identities: Vec<ScheduleId> = self
            .entries
            .lock()
            .await
            .entries
            .iter()
            .map(|entry| entry.id.clone())
            .collect();

        for id in &identities {
            if shutdown.is_some_and(ShutdownSignal::is_cancelled) {
                break;
            }
            self.process_entry(id, now, shutdown, &mut remaining_global, &mut report)
                .await;
            if remaining_global == 0 {
                report.saturated |= entries_have_due(&self.entries.lock().await.entries, now);
                break;
            }
        }

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
                    let report = self.tick_observing(self.clock.now(), Some(&shutdown)).await;
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
            .entries
            .iter()
            .map(|entry| ScheduleSnapshot {
                id: entry.id.clone(),
                status: entry.status,
                next_due_at: entry.next_due_at,
                skip_backlog_through: entry.skip_backlog_through,
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
        let mut registry = self.entries.lock().await;
        registry.prune();
        let entry = find_entry(&mut registry.entries, id)?;
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
        let mut registry = self.entries.lock().await;
        registry.prune();
        let entry = find_entry(&mut registry.entries, id)?;
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
    /// Admission is decided once, atomically with reserving the bounded slot. An automatic
    /// occurrence that enters reconciliation afterwards does not retroactively withdraw a manual
    /// dispatch that was already admitted, so a caller that needs the two to be mutually exclusive
    /// must serialize them itself.
    ///
    /// The occurrence identity is exactly the schedule id paired with `triggered_at`, and it is
    /// not made unique per call. Repeating an instant deliberately reuses the identity, so an
    /// idempotent dispatcher treats the second invocation as the same occurrence and produces no
    /// second effect; the same holds when `triggered_at` matches an automatic due instant. A
    /// caller that wants a distinct effect must pass a distinct instant, and a caller that
    /// reconciles by identity must keep manual instants unique for that reason.
    ///
    /// Dropping this future forfeits the outcome: the external effect may already have begun, and
    /// the scheduler records nothing for a manual occurrence, so an abandoned call must be treated
    /// by the caller as an unknown effect for that occurrence identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the schedule identity is unknown.
    pub async fn trigger_schedule(
        &self,
        id: &ScheduleId,
        triggered_at: SystemTime,
    ) -> Result<ManualTriggerOutcome, ScheduleBuildError> {
        // Status and slot are decided under one lock so a concurrent tick cannot slip a
        // reconciliation transition between the two. The payload is only cloned once the
        // invocation is actually admitted.
        let (payload, _permit) = {
            let mut registry = self.entries.lock().await;
            let entry = find_entry(&mut registry.entries, id)?;
            if entry.status == ScheduleStatus::ReconciliationRequired {
                return Ok(ManualTriggerOutcome::ReconciliationRequired);
            }
            match Arc::clone(&self.manual_permits).try_acquire_owned() {
                Ok(permit) => (entry.payload.clone(), permit),
                Err(_saturated) => return Ok(ManualTriggerOutcome::Saturated),
            }
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
        let mut registry = self.entries.lock().await;
        registry.prune();
        // An unsettled attempt cannot be cancelled, so resolving it now could start a second
        // external effect for the same occurrence. Abandoning the tick does end the attempt as far
        // as this scheduler is concerned, which is what stops an interrupted dispatch from blocking
        // the owner forever; whether its effect happened is then the owner's call to make.
        if registry.is_running(id) {
            return Err(ScheduleBuildError::TransitionNotAllowed(id.clone()));
        }
        let entry = registry
            .entries
            .iter_mut()
            .find(|entry| &entry.id == id)
            .ok_or_else(|| ScheduleBuildError::UnknownSchedule(id.clone()))?;
        if entry.status != ScheduleStatus::ReconciliationRequired {
            return Err(ScheduleBuildError::ReconciliationNotRequired(id.clone()));
        }
        entry.dispatch_token = None;
        if resolution == ReconciliationResolution::Confirmed {
            advance_once(entry);
        } else {
            // The owner asked for the same occurrence to run again, so any abandonment boundary
            // recorded for it must not suppress the retry.
            entry.skip_backlog_through = None;
            entry.status = ScheduleStatus::Active;
        }
        Ok(())
    }

    /// Drives one schedule for this tick, releasing the registry lock around every dispatch.
    ///
    /// Each iteration decides the next action under the lock, awaits the application dispatcher
    /// without it, then reacquires the lock to apply the outcome. The entry is marked
    /// reconciliation-required before the lock is released, so a concurrent tick skips it, an
    /// owner sees an unresolved occurrence, and an aborted tick leaves that state behind.
    async fn process_entry(
        &self,
        id: &ScheduleId,
        now: SystemTime,
        shutdown: Option<&ShutdownSignal>,
        remaining_global: &mut usize,
        report: &mut SchedulerTickReport,
    ) {
        let mut attempted = 0_usize;
        let mut examined = false;
        loop {
            let decision = {
                let mut registry = self.entries.lock().await;
                registry.prune();
                // Unsettled attempts are bounded like every other queue in the runtime. Capacity
                // is passed into the decision rather than checked ahead of it, so an entry that
                // was not going to dispatch anyway never reports saturation.
                let has_capacity = registry.has_dispatch_capacity(self.config.max_schedules());
                let Some(entry) = registry.find_mut(id) else {
                    return;
                };
                if !examined {
                    if entry.status != ScheduleStatus::Active {
                        return;
                    }
                    report.schedules_examined = report.schedules_examined.saturating_add(1);
                    examined = true;
                }
                let decision = self.decide(
                    entry,
                    now,
                    shutdown,
                    DispatchBudget {
                        remaining_global: *remaining_global,
                        attempted,
                        has_capacity,
                    },
                    report,
                );
                if let Decision::Dispatch(dispatch) = &decision {
                    registry.begin(id.clone(), Arc::clone(&dispatch.guard.active));
                }
                decision
            };

            let dispatch = match decision {
                Decision::Stop => break,
                Decision::Saturated => {
                    report.saturated = true;
                    return;
                }
                Decision::Skipped => {
                    attempted = attempted.saturating_add(1);
                    continue;
                }
                Decision::Dispatch(dispatch) => dispatch,
            };
            attempted = attempted.saturating_add(1);
            *remaining_global = remaining_global.saturating_sub(1);

            let outcome = timeout(
                self.config.dispatch_timeout(),
                self.dispatcher.dispatch(dispatch.occurrence),
            )
            .await;
            // The attempt keeps counting as running until its outcome has been applied. Releasing
            // it here would let the identity be removed and re-registered, or reconciled, while
            // this result is still pending.
            let running = dispatch.guard;

            // The dispatcher ran, so its result belongs to this tick's counters whether or not
            // the cursor update below still applies to a registered schedule.
            let outcome = classify(outcome);
            match outcome {
                DispatchOutcome::Confirmed => {
                    report.dispatched = report.dispatched.saturating_add(1);
                }
                DispatchOutcome::NotStarted => {
                    report.not_started_failures = report.not_started_failures.saturating_add(1);
                }
                DispatchOutcome::Unknown => {
                    report.reconciliation_required =
                        report.reconciliation_required.saturating_add(1);
                }
            }

            let mut registry = self.entries.lock().await;
            // Pinning the unresolved identity means neither branch below is reachable through the
            // public operations: the schedule cannot be removed, and therefore cannot be replaced,
            // while this attempt is unresolved. They stay as guards so the outcome can never be
            // applied to a definition that did not issue it.
            let Some(entry) = registry.find_mut(id) else {
                return;
            };
            if entry.dispatch_token != Some(dispatch.token) {
                return;
            }
            entry.dispatch_token = None;
            let keep_going = self.apply_outcome(
                entry,
                outcome,
                dispatch.misfired,
                now,
                &mut attempted,
                report,
            );
            // The outcome is settled, so the identity is free again. Both drops happen while this
            // tick still owns the lock, so no other caller can observe the intermediate state.
            drop(running);
            drop(registry);
            if !keep_going {
                break;
            }
        }

        // Saturation means the per-schedule bound left due cursor work behind, which includes work
        // this tick would only have skipped. An entry stopped by reconciliation or completion has
        // no due cursor to carry forward, so it must not raise the flag.
        let registry = self.entries.lock().await;
        if let Some(entry) = registry.entries.iter().find(|entry| &entry.id == id)
            && entry.status == ScheduleStatus::Active
            && attempted >= self.config.max_catch_up_per_schedule()
            && entry.next_due_at.is_some_and(|due| due <= now)
        {
            report.saturated = true;
        }
    }

    /// Applies one dispatch outcome to the entry it was issued for.
    ///
    /// Returns whether this tick may keep working on the same schedule.
    fn apply_outcome(
        &self,
        entry: &mut ScheduleEntry<T>,
        outcome: DispatchOutcome,
        misfired: bool,
        now: SystemTime,
        attempted: &mut usize,
        report: &mut SchedulerTickReport,
    ) -> bool {
        match outcome {
            DispatchOutcome::Confirmed => {
                // `advance_once` sets the status from the new cursor, so the entry is only
                // released from reconciliation once the application-supplied plan has actually
                // produced that cursor. A plan that panics leaves the conservative state.
                advance_once(entry);
                if misfired && entry.misfire_policy == MisfirePolicy::FireOnce {
                    // A schedule the dispatch just completed has no cursor left to suppress, and a
                    // boundary without a cursor is state restoration rejects.
                    if entry.next_due_at.is_some() {
                        entry.skip_backlog_through = Some(now);
                    }
                    let remaining = self
                        .config
                        .max_catch_up_per_schedule()
                        .saturating_sub(*attempted);
                    // The sweep stops either on the per-schedule bound, which the next decision
                    // honours and reports as saturation, or on an occurrence that is still on
                    // time, which is then dispatched in this tick instead of being deferred into a
                    // later misfire.
                    *attempted = attempted.saturating_add(skip_through_now(
                        entry,
                        now,
                        self.config.misfire_grace(),
                        remaining,
                        report,
                    ));
                }
                true
            }
            DispatchOutcome::NotStarted => {
                // Nothing began, so the same occurrence stays due and must not be suppressed.
                entry.status = ScheduleStatus::Active;
                entry.skip_backlog_through = None;
                false
            }
            // The entry is already reconciliation-required and, under FireOnce, already carries
            // the boundary recorded before the dispatch started, so a later `Confirmed`
            // resolution cannot release the rest of the backlog.
            DispatchOutcome::Unknown => false,
        }
    }

    /// Chooses the next action for one entry and applies every mutation that needs no dispatch.
    fn decide(
        &self,
        entry: &mut ScheduleEntry<T>,
        now: SystemTime,
        shutdown: Option<&ShutdownSignal>,
        budget: DispatchBudget,
        report: &mut SchedulerTickReport,
    ) -> Decision<T> {
        if entry.status != ScheduleStatus::Active
            || budget.remaining_global == 0
            || budget.attempted >= self.config.max_catch_up_per_schedule()
        {
            return Decision::Stop;
        }
        let Some(due_at) = entry.next_due_at.filter(|due| *due <= now) else {
            return Decision::Stop;
        };
        // The boundary abandons only work the owning policy itself decided to abandon, so it can
        // never cross into a definition rebuilt under another misfire policy. It is also measured
        // against the tick that recorded it, so an occurrence that was still inside the misfire
        // grace back then is dispatched instead of being swept away.
        if entry.misfire_policy == MisfirePolicy::FireOnce
            && entry.skip_backlog_through.is_some_and(|through| {
                due_at <= through && is_overdue(through, due_at, self.config.misfire_grace())
            })
        {
            report.skipped_misfires = report.skipped_misfires.saturating_add(1);
            advance_once(entry);
            return Decision::Skipped;
        }
        entry.skip_backlog_through = None;
        let misfired = is_overdue(now, due_at, self.config.misfire_grace());
        if misfired && entry.misfire_policy == MisfirePolicy::Skip {
            report.skipped_misfires = report.skipped_misfires.saturating_add(1);
            advance_once(entry);
            return Decision::Skipped;
        }
        if shutdown.is_some_and(ShutdownSignal::is_cancelled) {
            return Decision::Stop;
        }
        if !budget.has_capacity {
            // This occurrence is due and would have been dispatched, so the exhausted slot really
            // did leave work for a later tick.
            return Decision::Saturated;
        }

        // The dispatcher await is both a cancellation point and a window in which the lock is
        // released. Marking the occurrence uncertain before it starts means an aborted tick leaves
        // an explicit reconciliation instead of a cursor that silently invites a blind retry.
        entry.status = ScheduleStatus::ReconciliationRequired;
        if misfired && entry.misfire_policy == MisfirePolicy::FireOnce {
            entry.skip_backlog_through = Some(now);
        }
        let token = self.dispatch_tokens.fetch_add(1, Ordering::Relaxed);
        let active = Arc::new(AtomicBool::new(true));
        entry.dispatch_token = Some(token);
        Decision::Dispatch(PendingDispatch {
            occurrence: ScheduledOccurrence {
                id: ScheduleOccurrenceId {
                    schedule_id: entry.id.clone(),
                    due_at,
                },
                due_at,
                payload: entry.payload.clone(),
            },
            token,
            misfired,
            guard: DispatchGuard { active },
        })
    }
}

/// One dispatch issued for an entry while the registry lock is released.
struct PendingDispatch<T> {
    occurrence: ScheduledOccurrence<T>,
    token: u64,
    misfired: bool,
    guard: DispatchGuard,
}

/// What one dispatcher call reported.
#[derive(Clone, Copy, Eq, PartialEq)]
enum DispatchOutcome {
    /// The dispatcher confirmed the external effect.
    Confirmed,
    /// The dispatcher proved that no external effect began.
    NotStarted,
    /// The external effect is undetermined.
    Unknown,
}

/// Bounds that apply to one dispatch decision.
#[derive(Clone, Copy)]
struct DispatchBudget {
    remaining_global: usize,
    attempted: usize,
    has_capacity: bool,
}

/// Next action for one entry within a tick.
enum Decision<T> {
    /// Nothing more to do for this entry in this tick.
    Stop,
    /// An occurrence is due but no unsettled-attempt slot is free.
    Saturated,
    /// The cursor advanced without dispatching.
    Skipped,
    /// An occurrence must be dispatched with the lock released.
    Dispatch(PendingDispatch<T>),
}

fn schedule_entry<T>(schedule: Schedule<T>) -> ScheduleEntry<T> {
    ScheduleEntry {
        id: schedule.id,
        payload: schedule.payload,
        plan: schedule.plan,
        misfire_policy: schedule.misfire_policy,
        next_due_at: schedule.next_due_at,
        status: schedule.status,
        skip_backlog_through: schedule.skip_backlog_through,
        dispatch_token: None,
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
    if entry.next_due_at.is_none() {
        // A boundary without a cursor is state the scheduler refuses to restore, so a completed
        // schedule must never be snapshotted holding one.
        entry.skip_backlog_through = None;
    }
}

/// Reduces one dispatcher result to the outcome the scheduler acts on.
fn classify(
    outcome: Result<Result<(), ScheduleDispatchError>, tokio::time::error::Elapsed>,
) -> DispatchOutcome {
    match outcome {
        Ok(Ok(())) => DispatchOutcome::Confirmed,
        Ok(Err(error)) if error.effect() == ScheduleDispatchEffect::NotStarted => {
            DispatchOutcome::NotStarted
        }
        Ok(Err(_)) | Err(_) => DispatchOutcome::Unknown,
    }
}

/// Returns whether an occurrence is older than the misfire grace at a reference instant.
fn is_overdue(reference: SystemTime, due_at: SystemTime, grace: Duration) -> bool {
    reference
        .duration_since(due_at)
        .is_ok_and(|late| late > grace)
}

fn skip_through_now<T>(
    entry: &mut ScheduleEntry<T>,
    now: SystemTime,
    grace: Duration,
    limit: usize,
    report: &mut SchedulerTickReport,
) -> usize {
    let mut skipped = 0_usize;
    // Only genuinely overdue work is abandoned: the sweep stops at the first occurrence still
    // inside the grace so an on-time occurrence is dispatched rather than dropped with the backlog.
    while skipped < limit
        && entry
            .next_due_at
            .is_some_and(|due| due <= now && is_overdue(now, due, grace))
    {
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
    /// The durable fields cannot describe one schedule state.
    ///
    /// Active, paused and reconciliation-required states require a cursor, a completed state must
    /// not carry one, and a suppression boundary is meaningless without a cursor to suppress.
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
