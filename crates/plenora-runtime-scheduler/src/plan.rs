use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule as CronSchedule;

/// Maximum accepted cron expression bytes.
pub const MAX_CRON_EXPRESSION_BYTES: usize = 1024;
/// Maximum accepted IANA timezone name bytes.
pub const MAX_TIMEZONE_NAME_BYTES: usize = 128;

/// Extensible calculation boundary for one-shot, interval, cron, or calendar schedules.
pub trait SchedulePlan: Debug + Send + Sync {
    /// Returns the first due instant.
    fn first_due_at(&self) -> SystemTime;

    /// Returns the next due instant strictly after a previously due occurrence.
    fn next_after(&self, previous_due_at: SystemTime) -> Option<SystemTime>;
}

/// Plan that fires exactly once.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OneShotPlan {
    due_at: SystemTime,
}

impl OneShotPlan {
    /// Creates a one-shot plan.
    #[must_use]
    pub const fn new(due_at: SystemTime) -> Self {
        Self { due_at }
    }
}

impl SchedulePlan for OneShotPlan {
    fn first_due_at(&self) -> SystemTime {
        self.due_at
    }

    fn next_after(&self, _previous_due_at: SystemTime) -> Option<SystemTime> {
        None
    }
}

/// Plan with a fixed wall-clock interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FixedIntervalPlan {
    first_due_at: SystemTime,
    interval: Duration,
}

impl FixedIntervalPlan {
    /// Creates a fixed-interval plan.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero interval.
    pub const fn new(
        first_due_at: SystemTime,
        interval: Duration,
    ) -> Result<Self, FixedIntervalPlanError> {
        if interval.is_zero() {
            return Err(FixedIntervalPlanError::ZeroInterval);
        }
        Ok(Self {
            first_due_at,
            interval,
        })
    }

    /// Returns the fixed cadence.
    #[must_use]
    pub const fn interval(self) -> Duration {
        self.interval
    }
}

impl SchedulePlan for FixedIntervalPlan {
    fn first_due_at(&self) -> SystemTime {
        self.first_due_at
    }

    fn next_after(&self, previous_due_at: SystemTime) -> Option<SystemTime> {
        previous_due_at.checked_add(self.interval)
    }
}

/// Invalid fixed-interval plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FixedIntervalPlanError {
    /// A repeating plan requires a positive interval.
    ZeroInterval,
}

impl Display for FixedIntervalPlanError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("fixed schedule interval must be greater than zero")
    }
}

impl Error for FixedIntervalPlanError {}

/// Cron plan evaluated in an explicit IANA timezone, including daylight-saving transitions.
#[derive(Clone)]
pub struct CronPlan {
    expression: Arc<str>,
    timezone_name: Arc<str>,
    timezone: Tz,
    schedule: CronSchedule,
    first_due_at: SystemTime,
}

impl CronPlan {
    /// Parses a cron expression and returns the first occurrence strictly after `first_after`.
    ///
    /// The parser uses cron's second-aware expression format. The timezone must be an IANA name
    /// such as `UTC` or `Europe/Rome`; daylight-saving gaps and overlaps follow that database.
    ///
    /// # Errors
    ///
    /// Returns a redaction-safe error for empty/oversized input, invalid syntax/timezone, or an
    /// expression with no future occurrence.
    pub fn new(
        expression: impl Into<Arc<str>>,
        timezone_name: impl Into<Arc<str>>,
        first_after: SystemTime,
    ) -> Result<Self, CronPlanError> {
        let expression = expression.into();
        let timezone_name = timezone_name.into();
        if expression.trim().is_empty() {
            return Err(CronPlanError::new(CronPlanErrorKind::EmptyExpression));
        }
        if expression.len() > MAX_CRON_EXPRESSION_BYTES {
            return Err(CronPlanError::new(CronPlanErrorKind::ExpressionTooLarge));
        }
        if timezone_name.trim().is_empty() {
            return Err(CronPlanError::new(CronPlanErrorKind::EmptyTimezone));
        }
        if timezone_name.len() > MAX_TIMEZONE_NAME_BYTES {
            return Err(CronPlanError::new(CronPlanErrorKind::TimezoneTooLarge));
        }
        let timezone = Tz::from_str(&timezone_name)
            .map_err(|_error| CronPlanError::new(CronPlanErrorKind::InvalidTimezone))?;
        let schedule = CronSchedule::from_str(&expression)
            .map_err(|_error| CronPlanError::new(CronPlanErrorKind::InvalidExpression))?;
        let first_due_at = next_after(&schedule, timezone, first_after)
            .ok_or_else(|| CronPlanError::new(CronPlanErrorKind::NoFutureOccurrence))?;
        Ok(Self {
            expression,
            timezone_name,
            timezone,
            schedule,
            first_due_at,
        })
    }

    /// Returns the validated cron expression.
    #[must_use]
    pub const fn expression(&self) -> &Arc<str> {
        &self.expression
    }

    /// Returns the validated IANA timezone name.
    #[must_use]
    pub const fn timezone_name(&self) -> &Arc<str> {
        &self.timezone_name
    }
}

impl SchedulePlan for CronPlan {
    fn first_due_at(&self) -> SystemTime {
        self.first_due_at
    }

    fn next_after(&self, previous_due_at: SystemTime) -> Option<SystemTime> {
        next_after(&self.schedule, self.timezone, previous_due_at)
    }
}

impl Debug for CronPlan {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CronPlan")
            .field("expression_bytes", &self.expression.len())
            .field("timezone", &self.timezone_name)
            .field("first_due_at", &self.first_due_at)
            .finish_non_exhaustive()
    }
}

fn next_after(schedule: &CronSchedule, timezone: Tz, after: SystemTime) -> Option<SystemTime> {
    let after = DateTime::<Utc>::from(after).with_timezone(&timezone);
    schedule
        .after(&after)
        .next()
        .map(|next| SystemTime::from(next.with_timezone(&Utc)))
}

/// Stable cron construction category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CronPlanErrorKind {
    /// Expression is blank.
    EmptyExpression,
    /// Expression exceeds its defensive bound.
    ExpressionTooLarge,
    /// Timezone is blank.
    EmptyTimezone,
    /// Timezone name exceeds its defensive bound.
    TimezoneTooLarge,
    /// Cron syntax is invalid.
    InvalidExpression,
    /// Timezone name is not in the bundled IANA database.
    InvalidTimezone,
    /// The expression has no occurrence after the supplied instant.
    NoFutureOccurrence,
}

/// Redaction-safe invalid cron plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CronPlanError {
    kind: CronPlanErrorKind,
}

impl CronPlanError {
    const fn new(kind: CronPlanErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable construction category.
    #[must_use]
    pub const fn kind(self) -> CronPlanErrorKind {
        self.kind
    }
}

impl Display for CronPlanError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            CronPlanErrorKind::EmptyExpression => "cron expression must not be blank",
            CronPlanErrorKind::ExpressionTooLarge => "cron expression exceeds the byte bound",
            CronPlanErrorKind::EmptyTimezone => "cron timezone must not be blank",
            CronPlanErrorKind::TimezoneTooLarge => "cron timezone exceeds the byte bound",
            CronPlanErrorKind::InvalidExpression => "cron expression is invalid",
            CronPlanErrorKind::InvalidTimezone => "cron timezone is invalid",
            CronPlanErrorKind::NoFutureOccurrence => "cron expression has no future occurrence",
        })
    }
}

impl Error for CronPlanError {}

/// Plan wrapper applying one deterministic delay to every occurrence without cadence drift.
#[derive(Clone, Debug)]
pub struct JitteredPlan<P> {
    inner: P,
    delay: Duration,
}

impl<P> JitteredPlan<P>
where
    P: SchedulePlan,
{
    /// Creates a deterministic jitter wrapper from a maximum delay and stable seed.
    #[must_use]
    pub fn new(inner: P, maximum_delay: Duration, seed: u64) -> Self {
        let maximum_nanos = maximum_delay.as_nanos();
        let sampled = u128::from(mix(seed)) % maximum_nanos.saturating_add(1);
        let delay = duration_from_nanos(sampled);
        Self { inner, delay }
    }

    /// Returns the fixed delay selected for this plan.
    #[must_use]
    pub const fn delay(&self) -> Duration {
        self.delay
    }
}

impl<P> SchedulePlan for JitteredPlan<P>
where
    P: SchedulePlan,
{
    fn first_due_at(&self) -> SystemTime {
        let first_due_at = self.inner.first_due_at();
        match first_due_at.checked_add(self.delay) {
            Some(jittered) => jittered,
            None => first_due_at,
        }
    }

    fn next_after(&self, previous_due_at: SystemTime) -> Option<SystemTime> {
        let base = previous_due_at.checked_sub(self.delay)?;
        self.inner.next_after(base)?.checked_add(self.delay)
    }
}

const fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn duration_from_nanos(nanos: u128) -> Duration {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let seconds = nanos / NANOS_PER_SECOND;
    let subsecond_nanos = nanos % NANOS_PER_SECOND;
    match (u64::try_from(seconds), u32::try_from(subsecond_nanos)) {
        (Ok(seconds), Ok(subsecond_nanos)) => Duration::new(seconds, subsecond_nanos),
        _ => Duration::MAX,
    }
}
