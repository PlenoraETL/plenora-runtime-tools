//! Extremal external time values and exact calendar conversion.
use plenora_runtime_core::{
    DrainOutcome, RuntimeConfig, RuntimeHandle, ServiceMetadata,
    temporal::{self, MAX_WAIT, TimeError},
};
use std::{
    error::Error,
    time::{Duration, Instant, SystemTime},
};

#[test]
fn operational_domains_reject_extremes() -> Result<(), Box<dyn Error>> {
    for value in [
        Duration::MAX,
        Duration::from_secs(u64::MAX),
        MAX_WAIT + Duration::from_nanos(1),
    ] {
        assert_eq!(temporal::timeout(value), Err(TimeError::WaitRange));
        assert_eq!(temporal::interval(value), Err(TimeError::WaitRange));
        assert_eq!(temporal::retry_delay(value), Err(TimeError::WaitRange));
    }
    assert_eq!(temporal::timeout(Duration::ZERO), Err(TimeError::Zero));
    temporal::retry_delay(Duration::ZERO)?;
    for value in [
        Duration::from_nanos(1),
        MAX_WAIT
            .checked_sub(Duration::from_nanos(1))
            .ok_or("invalid lower bound fixture")?,
        MAX_WAIT,
    ] {
        temporal::timeout(value)?;
        temporal::interval(value)?;
        temporal::retry_delay(value)?;
    }
    Ok(())
}

#[test]
fn injected_monotonic_limit_rejects_reset_without_waiting() -> Result<(), Box<dyn Error>> {
    let mut limit = Instant::now();
    let mut step = Duration::MAX;
    while !step.is_zero() {
        if let Some(next) = limit.checked_add(step) {
            limit = next;
        }
        step /= 2;
    }
    while let Some(next) = limit.checked_add(Duration::from_nanos(1)) {
        if next == limit {
            break;
        }
        limit = next;
    }
    assert_eq!(temporal::deadline(limit, Duration::ZERO)?, limit);
    assert_eq!(
        temporal::deadline(limit, Duration::from_nanos(1)),
        Err(TimeError::DeadlineRange)
    );
    let now = tokio::time::Instant::from_std(limit);
    assert_eq!(
        temporal::timer_deadline(now, Duration::from_nanos(1)),
        Err(TimeError::DeadlineRange)
    );
    assert_eq!(
        temporal::timer_deadline(now, Duration::MAX),
        Err(TimeError::DeadlineRange)
    );
    Ok(())
}

#[test]
fn calendar_roundtrips_pre_epoch_and_rejects_extreme_clocks() -> Result<(), Box<dyn Error>> {
    for delta in [
        Duration::ZERO,
        Duration::from_nanos(1),
        Duration::new(1, 999_999_999),
    ] {
        for value in [
            SystemTime::UNIX_EPOCH.checked_add(delta),
            SystemTime::UNIX_EPOCH.checked_sub(delta),
        ]
        .into_iter()
        .flatten()
        {
            assert_eq!(temporal::calendar(temporal::clock(value)?)?, value);
        }
    }
    let beyond = Duration::from_secs(10_000_000_000_000);
    for value in [
        SystemTime::UNIX_EPOCH.checked_add(beyond),
        SystemTime::UNIX_EPOCH.checked_sub(beyond),
    ]
    .into_iter()
    .flatten()
    {
        assert_eq!(temporal::clock(value), Err(TimeError::CalendarRange));
    }
    Ok(())
}

#[tokio::test]
async fn invalid_runtime_drain_is_explicit() {
    let runtime = RuntimeHandle::with_config(
        ServiceMetadata::new("test", "1", "test"),
        RuntimeConfig {
            shutdown_grace_period: Duration::MAX,
            ..RuntimeConfig::default()
        },
    );
    assert_eq!(runtime.shutdown().await, DrainOutcome::InvalidTime);
}

#[test]
fn calendar_conversion_never_loses_platform_precision() -> Result<(), Box<dyn Error>> {
    let value = chrono::DateTime::from_timestamp(0, 1).ok_or("invalid fixture")?;
    match temporal::calendar(value) {
        Ok(result) => assert_eq!(temporal::clock(result)?, value),
        Err(error) => assert_eq!(error, TimeError::CalendarRange),
    }
    let leap = chrono::DateTime::from_timestamp(59, 1_000_000_000).ok_or("invalid leap fixture")?;
    assert_eq!(temporal::calendar(leap), Err(TimeError::CalendarRange));
    Ok(())
}

#[tokio::test]
async fn reset_rejects_extreme_injected_clock_and_preserves_timer() -> Result<(), Box<dyn Error>> {
    let timer = tokio::time::sleep(Duration::from_secs(1));
    tokio::pin!(timer);
    let original = timer.deadline();
    let now = tokio::time::Instant::now();
    assert_eq!(
        temporal::reset_timer(timer.as_mut(), now, Duration::MAX),
        Err(TimeError::DeadlineRange)
    );
    assert_eq!(timer.deadline(), original);
    let mut limit = now.into_std();
    let mut step = Duration::MAX;
    while !step.is_zero() {
        if let Some(next) = limit.checked_add(step) {
            limit = next;
        }
        step /= 2;
    }
    assert_eq!(
        temporal::reset_timer(
            timer.as_mut(),
            tokio::time::Instant::from_std(limit),
            Duration::from_secs(1)
        ),
        Err(TimeError::DeadlineRange)
    );
    assert_eq!(timer.deadline(), original);
    temporal::reset_timer(timer.as_mut(), now, Duration::from_secs(2))?;
    assert_eq!(
        timer.deadline(),
        temporal::timer_deadline(now, Duration::from_secs(2))?
    );
    Ok(())
}
