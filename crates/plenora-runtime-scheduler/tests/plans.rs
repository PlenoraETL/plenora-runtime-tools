//! Cron, timezone, daylight-saving, and deterministic-jitter coverage.

use std::{
    error::Error,
    io,
    time::{Duration, SystemTime},
};

use chrono::{TimeZone, Utc};
use plenora_runtime_scheduler::{
    CronPlan, CronPlanError, CronPlanErrorKind, FixedIntervalPlan, JitteredPlan,
    MAX_CRON_EXPRESSION_BYTES, MAX_TIMEZONE_NAME_BYTES, SchedulePlan,
};

#[test]
fn cron_is_timezone_aware_and_strictly_after_the_anchor() -> Result<(), Box<dyn Error>> {
    let anchor = utc(2026, 1, 1, 2, 29, 59)?;
    let plan = CronPlan::new("0 30 2 * * * *", "UTC", anchor)?;

    assert_eq!(plan.first_due_at(), utc(2026, 1, 1, 2, 30, 0)?);
    assert_eq!(
        plan.next_after(plan.first_due_at()),
        Some(utc(2026, 1, 2, 2, 30, 0)?)
    );
    assert_eq!(plan.expression().as_ref(), "0 30 2 * * * *");
    assert_eq!(plan.timezone_name().as_ref(), "UTC");
    assert!(!format!("{plan:?}").contains("0 30 2"));
    Ok(())
}

#[test]
fn cron_handles_rome_spring_gap_and_autumn_overlap() -> Result<(), Box<dyn Error>> {
    let spring = CronPlan::new("0 30 2 * * * *", "Europe/Rome", utc(2026, 3, 29, 0, 0, 0)?)?;
    assert_eq!(spring.first_due_at(), utc(2026, 3, 30, 0, 30, 0)?);

    let autumn = CronPlan::new("0 30 2 * * * *", "Europe/Rome", utc(2026, 10, 25, 0, 0, 0)?)?;
    assert_eq!(autumn.first_due_at(), utc(2026, 10, 25, 0, 30, 0)?);
    assert_eq!(
        autumn.next_after(autumn.first_due_at()),
        Some(utc(2026, 10, 26, 1, 30, 0)?)
    );
    Ok(())
}

#[test]
fn cron_rejects_invalid_and_oversized_inputs_without_echoing_them() {
    let invalid_expression = CronPlan::new("secret invalid", "UTC", SystemTime::UNIX_EPOCH);
    assert_eq!(
        invalid_expression.err().map(CronPlanError::kind),
        Some(CronPlanErrorKind::InvalidExpression)
    );
    let invalid_timezone = CronPlan::new("0 0 * * * * *", "private/zone", SystemTime::UNIX_EPOCH);
    assert_eq!(
        invalid_timezone.err().map(CronPlanError::kind),
        Some(CronPlanErrorKind::InvalidTimezone)
    );
    assert_eq!(
        CronPlan::new(
            "x".repeat(MAX_CRON_EXPRESSION_BYTES.saturating_add(1)),
            "UTC",
            SystemTime::UNIX_EPOCH,
        )
        .err()
        .map(CronPlanError::kind),
        Some(CronPlanErrorKind::ExpressionTooLarge)
    );
    assert_eq!(
        CronPlan::new(
            "0 0 * * * * *",
            "x".repeat(MAX_TIMEZONE_NAME_BYTES.saturating_add(1)),
            SystemTime::UNIX_EPOCH,
        )
        .err()
        .map(CronPlanError::kind),
        Some(CronPlanErrorKind::TimezoneTooLarge)
    );
}

#[test]
fn deterministic_jitter_is_bounded_and_does_not_drift() -> Result<(), Box<dyn Error>> {
    let inner = FixedIntervalPlan::new(SystemTime::UNIX_EPOCH, Duration::from_secs(10))?;
    let plan = JitteredPlan::new(inner, Duration::from_secs(5), 42);
    let same = JitteredPlan::new(inner, Duration::from_secs(5), 42);
    assert_eq!(plan.delay(), same.delay());
    assert!(plan.delay() <= Duration::from_secs(5));

    let first = plan.first_due_at();
    let second = plan
        .next_after(first)
        .ok_or_else(|| io::Error::other("missing second jittered occurrence"))?;
    let third = plan
        .next_after(second)
        .ok_or_else(|| io::Error::other("missing third jittered occurrence"))?;
    assert_eq!(second.duration_since(first)?, Duration::from_secs(10));
    assert_eq!(third.duration_since(second)?, Duration::from_secs(10));
    Ok(())
}

fn utc(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> Result<SystemTime, io::Error> {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .map(SystemTime::from)
        .ok_or_else(|| io::Error::other("invalid UTC test instant"))
}
