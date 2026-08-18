//! Deterministic clock and fault-sequence tests.

use std::time::{Duration, SystemTime};

use plenora_runtime_core::Clock;
use plenora_runtime_testkit::{FaultSequence, ManualClock};

#[test]
fn manual_clock_is_shared_and_only_moves_explicitly() -> Result<(), Box<dyn std::error::Error>> {
    let start = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let clock = ManualClock::new(start);
    let clone = clock.clone();

    assert_eq!(clock.now(), start);
    assert_eq!(
        clone.advance(Duration::from_secs(5))?,
        start + Duration::from_secs(5)
    );
    assert_eq!(clock.now(), start + Duration::from_secs(5));

    clone.set(SystemTime::UNIX_EPOCH);
    assert_eq!(clock.current(), SystemTime::UNIX_EPOCH);
    Ok(())
}

#[test]
fn fault_sequence_consumes_entries_in_fifo_order() -> Result<(), Box<dyn std::error::Error>> {
    let faults = FaultSequence::from_entries(["first", "second"])?;
    let clone = faults.clone();

    assert_eq!(faults.remaining(), 2);
    assert_eq!(clone.pop(), Some("first"));
    assert_eq!(faults.pop(), Some("second"));
    assert!(faults.is_empty());
    assert_eq!(clone.pop(), None);
    Ok(())
}

#[test]
fn fault_sequence_rejects_entries_beyond_its_explicit_bound() {
    let faults = FaultSequence::with_capacity(1);

    assert!(faults.push("accepted").is_ok());
    let error = faults.push("rejected").err();

    assert!(error.is_some_and(|error| error.capacity() == 1));
    assert_eq!(faults.remaining(), 1);
    assert!(!format!("{faults:?}").contains("accepted"));
}
