//! External temporal bounds.
use plenora_runtime_worker::{
    WorkerConfig, WorkerConfigError, WorkerInstanceHeartbeatConfig,
    WorkerInstanceHeartbeatConfigError,
};
use std::{error::Error, time::Duration};
#[test]
fn worker_time_fields_reject_extremes() -> Result<(), Box<dyn Error>> {
    for value in [
        Duration::MAX,
        Duration::from_secs(u64::MAX),
        Duration::from_hours(24) + Duration::from_nanos(1),
    ] {
        for config in [
            WorkerConfig {
                shutdown_grace_period: value,
                ..WorkerConfig::default()
            },
            WorkerConfig {
                execution_timeout: Some(value),
                ..WorkerConfig::default()
            },
            WorkerConfig {
                task_cancellation_grace_period: value,
                ..WorkerConfig::default()
            },
            WorkerConfig {
                lifecycle_heartbeat_interval: Some(value),
                ..WorkerConfig::default()
            },
        ] {
            assert_eq!(config.validate(), Err(WorkerConfigError::TimeRange));
        }
        assert_eq!(
            WorkerInstanceHeartbeatConfig::new(value),
            Err(WorkerInstanceHeartbeatConfigError::TimeRange)
        );
    }
    WorkerInstanceHeartbeatConfig::new(Duration::from_hours(24))?;
    Ok(())
}
