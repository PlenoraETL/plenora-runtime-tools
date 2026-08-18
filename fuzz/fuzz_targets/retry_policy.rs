#![no_main]

use std::time::Duration;

use libfuzzer_sys::fuzz_target;
use plenora_runtime_messaging::{
    ClassifyRetry, ExponentialBackoff, ExponentialBackoffConfig, JitterConfig, RetryDecision,
    RetryErrorClass, RetryExhaustedAction, RetryPolicy,
};

#[derive(Clone, Copy)]
struct FuzzError(RetryErrorClass);

impl ClassifyRetry for FuzzError {
    fn retry_class(&self) -> RetryErrorClass {
        self.0
    }
}

fuzz_target!(|data: &[u8]| {
    let initial = Duration::from_nanos(read_u64(data, 0));
    let maximum = Duration::from_nanos(read_u64(data, 8));
    let multiplier = read_u32(data, 16);
    let max_attempts = read_u32(data, 20);
    let attempt = read_u32(data, 24);
    let elapsed = Duration::from_nanos(read_u64(data, 28));
    let jitter = data.get(36).map(|percent| JitterConfig {
        percent: *percent,
        seed: read_u64(data, 37),
    });
    let config = ExponentialBackoffConfig {
        initial_delay: initial,
        max_delay: maximum,
        multiplier,
        max_attempts,
        max_elapsed: data
            .get(45)
            .is_some_and(|value| value & 1 == 1)
            .then_some(Duration::from_nanos(read_u64(data, 46))),
        jitter,
        retry_unknown_outcome: data.get(54).is_some_and(|value| value & 1 == 1),
        exhausted_action: if data.get(55).is_some_and(|value| value & 1 == 1) {
            RetryExhaustedAction::DoNotRetry
        } else {
            RetryExhaustedAction::DeadLetter
        },
    };

    if let Ok(policy) = ExponentialBackoff::new(config) {
        let delay = policy.delay_for_attempt(attempt);
        assert!(delay <= maximum);
        assert_eq!(delay, policy.delay_for_attempt(attempt));

        for class in [
            RetryErrorClass::Retryable,
            RetryErrorClass::Permanent,
            RetryErrorClass::DeadLetter,
            RetryErrorClass::OutcomeUnknown,
        ] {
            let decision = policy.decide_with_elapsed(attempt, elapsed, &FuzzError(class));
            if let RetryDecision::RetryAfter(retry_delay) = decision {
                assert!(retry_delay <= maximum);
            }
        }
    }
});

fn read_u32(data: &[u8], offset: usize) -> u32 {
    let mut bytes = [0_u8; 4];
    if let Some(source) = data.get(offset..offset.saturating_add(4)) {
        bytes.copy_from_slice(source);
    }
    u32::from_le_bytes(bytes)
}

fn read_u64(data: &[u8], offset: usize) -> u64 {
    let mut bytes = [0_u8; 8];
    if let Some(source) = data.get(offset..offset.saturating_add(8)) {
        bytes.copy_from_slice(source);
    }
    u64::from_le_bytes(bytes)
}
