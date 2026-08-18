use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    time::Duration,
};

/// Validated global scheduler bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerConfig {
    tick_interval: Duration,
    dispatch_timeout: Duration,
    misfire_grace: Duration,
    max_schedules: usize,
    max_dispatches_per_tick: usize,
    max_catch_up_per_schedule: usize,
}

impl SchedulerConfig {
    /// Creates a validated bounded scheduler configuration.
    ///
    /// # Errors
    ///
    /// Returns an error for any zero time or capacity bound.
    pub fn new(
        tick_interval: Duration,
        dispatch_timeout: Duration,
        misfire_grace: Duration,
        max_schedules: usize,
        max_dispatches_per_tick: usize,
        max_catch_up_per_schedule: usize,
    ) -> Result<Self, SchedulerConfigError> {
        let config = Self {
            tick_interval,
            dispatch_timeout,
            misfire_grace,
            max_schedules,
            max_dispatches_per_tick,
            max_catch_up_per_schedule,
        };
        config.validate()?;
        Ok(config)
    }

    /// Validates every scheduler bound.
    ///
    /// # Errors
    ///
    /// Returns the first invalid invariant.
    pub const fn validate(self) -> Result<(), SchedulerConfigError> {
        if self.tick_interval.is_zero() {
            return Err(SchedulerConfigError::ZeroTickInterval);
        }
        if self.dispatch_timeout.is_zero() {
            return Err(SchedulerConfigError::ZeroDispatchTimeout);
        }
        if self.max_schedules == 0 {
            return Err(SchedulerConfigError::ZeroScheduleCapacity);
        }
        if self.max_dispatches_per_tick == 0 {
            return Err(SchedulerConfigError::ZeroDispatchCapacity);
        }
        if self.max_catch_up_per_schedule == 0 {
            return Err(SchedulerConfigError::ZeroCatchUpCapacity);
        }
        Ok(())
    }

    /// Returns the polling cadence.
    #[must_use]
    pub const fn tick_interval(self) -> Duration {
        self.tick_interval
    }

    /// Returns the deadline for one dispatch attempt.
    #[must_use]
    pub const fn dispatch_timeout(self) -> Duration {
        self.dispatch_timeout
    }

    /// Returns how late an occurrence may be without being a misfire.
    #[must_use]
    pub const fn misfire_grace(self) -> Duration {
        self.misfire_grace
    }

    /// Returns the registry capacity.
    #[must_use]
    pub const fn max_schedules(self) -> usize {
        self.max_schedules
    }

    /// Returns the global dispatch bound per tick.
    #[must_use]
    pub const fn max_dispatches_per_tick(self) -> usize {
        self.max_dispatches_per_tick
    }

    /// Returns the per-schedule catch-up bound per tick.
    #[must_use]
    pub const fn max_catch_up_per_schedule(self) -> usize {
        self.max_catch_up_per_schedule
    }
}

/// Invalid scheduler configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerConfigError {
    /// Polling requires a positive interval.
    ZeroTickInterval,
    /// Dispatch requires a positive timeout.
    ZeroDispatchTimeout,
    /// At least one schedule must fit in the registry.
    ZeroScheduleCapacity,
    /// At least one dispatch must fit in a tick.
    ZeroDispatchCapacity,
    /// Catch-up must allow at least one occurrence per schedule.
    ZeroCatchUpCapacity,
}

impl Display for SchedulerConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroTickInterval => "scheduler tick interval must be greater than zero",
            Self::ZeroDispatchTimeout => "scheduler dispatch timeout must be greater than zero",
            Self::ZeroScheduleCapacity => "scheduler schedule capacity must be greater than zero",
            Self::ZeroDispatchCapacity => "scheduler dispatch capacity must be greater than zero",
            Self::ZeroCatchUpCapacity => "scheduler catch-up capacity must be greater than zero",
        })
    }
}

impl Error for SchedulerConfigError {}
