//! Checked time admission shared by runtime components.
//!
//! Operational waits are positive and at most one day. Retry delays additionally
//! permit zero (immediate retry). Calendar dates are not operational waits.

use chrono::{DateTime, Utc};
use std::{
    error::Error,
    fmt,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Inclusive maximum for operational waits and broker retry delays.
pub const MAX_WAIT: Duration = Duration::from_hours(24);

/// Redaction-safe temporal rejection, never containing the rejected value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeError {
    /// A timeout or periodic interval must be positive.
    Zero,
    /// The operational duration exceeds one day.
    WaitRange,
    /// The platform cannot represent the computed deadline.
    DeadlineRange,
    /// The clock or calendar date is outside the supported representation.
    CalendarRange,
}

impl fmt::Display for TimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Zero => "time interval must be positive",
            Self::WaitRange => "time interval exceeds the operational range",
            Self::DeadlineRange => "time deadline is not representable",
            Self::CalendarRange => "calendar time is not representable",
        })
    }
}
impl Error for TimeError {}

/// Admits an execution, drain, connection or acknowledgement timeout.
/// # Errors
/// Rejects zero, durations over one day and an unrepresentable deadline.
pub fn timeout(value: Duration) -> Result<(), TimeError> {
    if value.is_zero() {
        return Err(TimeError::Zero);
    }
    retry_delay(value)?;
    deadline(Instant::now(), value).map(|_| ())
}

/// Admits a periodic sampling or heartbeat interval.
/// # Errors
/// Has the same operational domain as [`timeout`].
pub fn interval(value: Duration) -> Result<(), TimeError> {
    timeout(value)
}

/// Admits a broker delay, including an immediate retry, with exact wire nanos.
/// # Errors
/// Rejects durations over one day or outside signed 64-bit wire nanoseconds.
pub fn retry_delay(value: Duration) -> Result<(), TimeError> {
    if value > MAX_WAIT {
        return Err(TimeError::WaitRange);
    }
    i64::try_from(value.as_nanos()).map_err(|_| TimeError::WaitRange)?;
    Ok(())
}

/// Constructs a deadline from an injected monotonic clock without normalizing it.
/// # Errors
/// Rejects an addition outside the platform range.
pub fn deadline(now: Instant, delay: Duration) -> Result<Instant, TimeError> {
    let result = now.checked_add(delay).ok_or(TimeError::DeadlineRange)?;
    if result.checked_duration_since(now) != Some(delay) {
        return Err(TimeError::DeadlineRange);
    }
    Ok(result)
}

/// Converts a wall clock exactly, including fractional seconds before the epoch.
/// # Errors
/// Rejects values outside chrono's calendar domain.
pub fn clock(value: SystemTime) -> Result<DateTime<Utc>, TimeError> {
    let nanos = match value.duration_since(UNIX_EPOCH) {
        Ok(delta) => i128::try_from(delta.as_nanos()).map_err(|_| TimeError::CalendarRange)?,
        Err(error) => {
            -i128::try_from(error.duration().as_nanos()).map_err(|_| TimeError::CalendarRange)?
        }
    };
    let seconds =
        i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| TimeError::CalendarRange)?;
    let fraction =
        u32::try_from(nanos.rem_euclid(1_000_000_000)).map_err(|_| TimeError::CalendarRange)?;
    DateTime::from_timestamp(seconds, fraction).ok_or(TimeError::CalendarRange)
}

/// Converts a calendar date to the platform wall clock exactly.
/// # Errors
/// Rejects leap seconds and dates outside the platform domain.
pub fn calendar(value: DateTime<Utc>) -> Result<SystemTime, TimeError> {
    if value.timestamp_subsec_nanos() >= 1_000_000_000 {
        return Err(TimeError::CalendarRange);
    }
    let nanos =
        i128::from(value.timestamp()) * 1_000_000_000 + i128::from(value.timestamp_subsec_nanos());
    let magnitude = nanos.unsigned_abs();
    let seconds = u64::try_from(magnitude / 1_000_000_000).map_err(|_| TimeError::CalendarRange)?;
    let fraction =
        u32::try_from(magnitude % 1_000_000_000).map_err(|_| TimeError::CalendarRange)?;
    let delta = Duration::new(seconds, fraction);
    let result = if nanos < 0 {
        UNIX_EPOCH.checked_sub(delta)
    } else {
        UNIX_EPOCH.checked_add(delta)
    }
    .ok_or(TimeError::CalendarRange)?;
    if clock(result)? != value {
        return Err(TimeError::CalendarRange);
    }
    Ok(result)
}

/// Constructs a Tokio deadline from an injected clock, including timer resets.
/// # Errors
/// Rejects addition outside the monotonic clock domain.
pub fn timer_deadline(
    now: tokio::time::Instant,
    delay: Duration,
) -> Result<tokio::time::Instant, TimeError> {
    deadline(now.into_std(), delay).map(tokio::time::Instant::from_std)
}

/// Validates a remaining absolute deadline without imposing a relative timeout cap.
/// Remote calendar deadlines preserve their contract horizon; only platform
/// representability is required. Expiration must be rejected by the caller.
/// # Errors
/// Rejects an unrepresentable monotonic deadline.
pub fn absolute_deadline(remaining: Duration) -> Result<tokio::time::Instant, TimeError> {
    timer_deadline(tokio::time::Instant::now(), remaining)
}

/// Resets an existing timer only after its new deadline has been checked.
/// # Errors
/// Rejects an unrepresentable deadline and preserves the existing timer.
pub fn reset_timer(
    mut timer: std::pin::Pin<&mut tokio::time::Sleep>,
    now: tokio::time::Instant,
    delay: Duration,
) -> Result<(), TimeError> {
    let next = timer_deadline(now, delay)?;
    timer.as_mut().reset(next);
    Ok(())
}
