//! External temporal bounds.
use plenora_runtime_subprocess::{SubprocessConfigError, SubprocessSupervisorConfig};
use std::{error::Error, time::Duration};
#[test]
fn every_subprocess_time_field_rejects_extreme_values_before_spawn() -> Result<(), Box<dyn Error>> {
    for value in [
        Duration::MAX,
        Duration::from_secs(u64::MAX),
        Duration::from_hours(24) + Duration::from_nanos(1),
    ] {
        assert_eq!(
            SubprocessSupervisorConfig::new(1, value),
            Err(SubprocessConfigError::TimeRange)
        );
        let valid = SubprocessSupervisorConfig::new(1, Duration::from_secs(1))?;
        for values in [
            (value, Duration::from_secs(1), Duration::from_secs(1)),
            (Duration::from_secs(1), value, Duration::from_secs(1)),
            (Duration::from_secs(1), Duration::from_secs(1), value),
        ] {
            assert_eq!(
                valid.with_termination_timeouts(values.0, values.1, values.2),
                Err(SubprocessConfigError::TimeRange)
            );
        }
        assert_eq!(
            valid.with_resident_memory_limit(1024, value),
            Err(SubprocessConfigError::TimeRange)
        );
    }
    SubprocessSupervisorConfig::new(1, Duration::from_hours(24))?;
    Ok(())
}
