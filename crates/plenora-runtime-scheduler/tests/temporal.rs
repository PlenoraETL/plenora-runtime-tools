//! External operational time bounds.
use std::time::Duration;
fn extremes() -> [Duration; 3] {
    [
        Duration::MAX,
        Duration::from_secs(u64::MAX),
        Duration::from_hours(24) + Duration::from_nanos(1),
    ]
}
use plenora_runtime_scheduler::{SchedulerConfig, SchedulerConfigError};
#[test]
fn scheduler_waits_reject_extreme_time() -> Result<(), Box<dyn std::error::Error>> {
    for value in extremes() {
        assert_eq!(
            SchedulerConfig::new(value, Duration::from_secs(1), Duration::ZERO, 1, 1, 1),
            Err(SchedulerConfigError::TimeRange)
        );
        assert_eq!(
            SchedulerConfig::new(Duration::from_secs(1), value, Duration::ZERO, 1, 1, 1),
            Err(SchedulerConfigError::TimeRange)
        );
    }
    SchedulerConfig::new(
        Duration::from_hours(24),
        Duration::from_hours(24),
        Duration::MAX,
        1,
        1,
        1,
    )?;
    Ok(())
}
