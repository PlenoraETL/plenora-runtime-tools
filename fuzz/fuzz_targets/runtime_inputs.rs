#![no_main]

use std::time::{Duration, SystemTime};

use libfuzzer_sys::fuzz_target;
use plenora_runtime_control::ControlComponentId;
use plenora_runtime_scheduler::{
    CronPlan, FixedIntervalPlan, JitteredPlan, ScheduleId, SchedulePlan,
};
use plenora_runtime_subprocess::SubprocessSpec;

fuzz_target!(|data: &[u8]| {
    let midpoint = data.len() / 2;
    let first = String::from_utf8_lossy(&data[..midpoint]).into_owned();
    let second = String::from_utf8_lossy(&data[midpoint..]).into_owned();
    let anchor = SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_nanos(read_u64(data, 0)))
        .unwrap_or(SystemTime::UNIX_EPOCH);

    let _ = ControlComponentId::new(first.clone());
    let _ = ScheduleId::new(first.clone());
    let _ = CronPlan::new(first.clone(), second.clone(), anchor);

    let interval = Duration::from_nanos(read_u64(data, 8).max(1));
    if let Ok(plan) = FixedIntervalPlan::new(anchor, interval) {
        let jitter = JitteredPlan::new(
            plan,
            Duration::from_nanos(read_u64(data, 16)),
            read_u64(data, 24),
        );
        let first_due = jitter.first_due_at();
        let _ = jitter.next_after(first_due);
    }

    if let Ok(spec) = SubprocessSpec::new(first.clone()) {
        let _ = spec
            .with_argument(second.clone())
            .and_then(|spec| spec.with_environment(first, second));
    }
});

fn read_u64(data: &[u8], offset: usize) -> u64 {
    let mut bytes = [0_u8; 8];
    if let Some(source) = data.get(offset..offset.saturating_add(8)) {
        bytes.copy_from_slice(source);
    }
    u64::from_le_bytes(bytes)
}
