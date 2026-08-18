//! Tests for the production wall clock.

use std::time::{Duration, SystemTime};

use plenora_runtime_core::{Clock, SystemClock};

#[test]
fn system_clock_returns_current_wall_time() {
    let before = SystemTime::now();
    let observed = SystemClock.now();
    let after = SystemTime::now();

    assert!(
        observed.duration_since(before).unwrap_or(Duration::ZERO)
            <= after.duration_since(before).unwrap_or(Duration::ZERO)
    );
}
