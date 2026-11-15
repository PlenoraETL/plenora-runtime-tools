//! External operational time bounds.
use std::time::Duration;
fn extremes() -> [Duration; 3] {
    [
        Duration::MAX,
        Duration::from_secs(u64::MAX),
        Duration::from_hours(24) + Duration::from_nanos(1),
    ]
}
use plenora_runtime_resources::{MemoryPressureConfig, MemoryPressureConfigError};
#[test]
fn sampling_rejects_extreme_time() -> Result<(), Box<dyn std::error::Error>> {
    for value in extremes() {
        assert_eq!(
            MemoryPressureConfig::new(value, 1, 2, 3),
            Err(MemoryPressureConfigError::TimeRange)
        );
    }
    MemoryPressureConfig::new(Duration::from_hours(24), 1, 2, 3)?;
    Ok(())
}
