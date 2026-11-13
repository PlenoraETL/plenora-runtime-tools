//! Retry decisions and cumulative accounting share one deterministic reducer.
use crate::{
    Error, budget,
    contracts::{Retry, Step, StepResult},
};

fn delay(retry: &Retry) -> Option<u64> {
    match retry {
        Retry::Variant0(r) if r.kind == "safe" => Some(0),
        Retry::Variant1(r) if r.kind == "after" => Some(r.delay_ms),
        _ => None,
    }
}
pub(crate) fn backoff(step: &StepResult, definition: &Step) -> Option<u64> {
    if step.state != "failed"
        || !matches!(step.remote_effect.as_str(), "none" | "rolled_back")
        || step.attempt >= definition.retry_policy.max_attempts
    {
        return None;
    }
    let mut wait = delay(&step.retry)?.max(definition.retry_policy.backoff_ms);
    for error in step.error.iter().chain(&step.other_errors) {
        if !matches!(error.remote_effect.as_str(), "none" | "rolled_back")
            || error.category == "resource_limit"
        {
            return None;
        }
        wait = wait.max(delay(&error.retry)?);
    }
    Some(wait)
}
pub(crate) fn cumulative(
    prior: &StepResult,
    raw: &StepResult,
    elapsed: u64,
) -> Result<StepResult, Error> {
    if raw.attempt != prior.attempt.checked_add(1).ok_or(Error::ResourceLimit)?
        || raw.counters.max_attempts != 1
    {
        return Err(Error::Recovery);
    }
    let mut result = raw.clone();
    result.counters = budget::add(&prior.counters, &raw.counters)?;
    result.counters.elapsed_ms = result.counters.elapsed_ms.max(elapsed);
    if prior.attempt > 0 {
        result.started_at.clone_from(&prior.started_at);
    }
    Ok(result)
}

pub(crate) fn elapsed(intent: &crate::journal::Intent, event: &[u8]) -> Result<u64, Error> {
    let event = crate::contracts::parse_event(event)?;
    let ms = chrono::DateTime::parse_from_rfc3339(&event.occurred_at)
        .map_err(|_| Error::Invalid)?
        .timestamp_millis();
    u64::try_from(ms)
        .map_err(|_| Error::Invalid)?
        .checked_sub(intent.first_started.unwrap_or(intent.started))
        .ok_or(Error::Invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{Retry0, Retry1, parse_plan, parse_result};
    #[test]
    fn retry_axes_and_every_secondary_error_are_conservative()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut s = parse_result(include_bytes!(
            "../../../contracts/chains-v2/fixtures/result-recovery-valid.json"
        ))?
        .steps
        .remove(2);
        let mut definition = parse_plan(include_bytes!(
            "../../../contracts/chains-v2/fixtures/plan-valid.json"
        ))?
        .steps
        .remove(2);
        definition.retry_policy.max_attempts = 3;
        definition.retry_policy.backoff_ms = 25;
        s.state = "failed".into();
        for effect in ["none", "rolled_back", "committed", "partial", "unknown"] {
            for kind in [
                "safe",
                "after",
                "never",
                "quarantine",
                "requires_recovery",
                "requires_idempotency_key",
            ] {
                s.remote_effect = effect.into();
                s.retry = if kind == "after" {
                    Retry::Variant1(Retry1 {
                        kind: kind.into(),
                        delay_ms: 40,
                    })
                } else {
                    Retry::Variant0(Retry0 { kind: kind.into() })
                };
                let error = s.error.as_mut().ok_or("error")?;
                error.remote_effect = effect.into();
                error.retry = s.retry.clone();
                let expected = if matches!(effect, "none" | "rolled_back")
                    && matches!(kind, "safe" | "after")
                {
                    Some(if kind == "after" { 40 } else { 25 })
                } else {
                    None
                };
                assert_eq!(backoff(&s, &definition), expected);
            }
        }
        s.remote_effect = "none".into();
        s.retry = Retry::Variant0(Retry0 {
            kind: "safe".into(),
        });
        let error = s.error.as_mut().ok_or("error")?;
        error.remote_effect = "none".into();
        error.retry = s.retry.clone();
        let mut secondary = error.clone();
        secondary.retry = Retry::Variant0(Retry0 {
            kind: "never".into(),
        });
        s.other_errors.push(secondary);
        assert_eq!(backoff(&s, &definition), None);
        s.other_errors.clear();
        s.error.as_mut().ok_or("error")?.category = "resource_limit".into();
        assert_eq!(backoff(&s, &definition), None);
        Ok(())
    }
}
