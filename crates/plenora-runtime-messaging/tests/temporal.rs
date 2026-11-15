//! External temporal bounds.
use plenora_runtime_messaging::{DeliveryHeartbeatConfig, DeliveryHeartbeatConfigError};
use std::{error::Error, time::Duration};
#[test]
fn delivery_heartbeat_rejects_extreme_values() -> Result<(), Box<dyn Error>> {
    for value in [
        Duration::MAX,
        Duration::from_secs(u64::MAX),
        Duration::from_hours(24) + Duration::from_nanos(1),
    ] {
        assert_eq!(
            DeliveryHeartbeatConfig::new(value, 1),
            Err(DeliveryHeartbeatConfigError::TimeRange)
        );
    }
    assert_eq!(
        DeliveryHeartbeatConfig::new(Duration::ZERO, 1),
        Err(DeliveryHeartbeatConfigError::ZeroInterval)
    );
    DeliveryHeartbeatConfig::new(Duration::from_hours(24), u32::MAX)?;
    Ok(())
}
