//! External operational time bounds.
use std::time::Duration;
fn extremes() -> [Duration; 3] {
    [
        Duration::MAX,
        Duration::from_secs(u64::MAX),
        Duration::from_hours(24) + Duration::from_nanos(1),
    ]
}
use plenora_runtime_http::{HttpServerConfig, HttpServerConfigError};
#[test]
fn http_drain_rejects_extreme_time() -> Result<(), Box<dyn std::error::Error>> {
    for value in extremes() {
        assert_eq!(
            HttpServerConfig::new("127.0.0.1:0".parse()?, value),
            Err(HttpServerConfigError::TimeRange)
        );
    }
    HttpServerConfig::new("127.0.0.1:0".parse()?, Duration::from_hours(24))?;
    Ok(())
}
