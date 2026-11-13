//! Consistency checks for terminal evidence; no effects are inferred as successful.
use crate::{
    error::{Checked, Code, ensure},
    plan::ValidationReport,
    types::{
        ChainError, ChainResult, Counters, Event, Input, Plan, Report, Retry, Source, Step,
        StepResult,
    },
};
use std::collections::BTreeMap;

impl Retry {
    pub(crate) fn kind(&self) -> &str {
        match self {
            Self::Variant0(v) => &v.kind,
            Self::Variant1(v) => &v.kind,
        }
    }
}
fn restrictive(retry: &Retry) -> bool {
    matches!(retry.kind(), "never" | "quarantine" | "requires_recovery")
}
fn axes(effect: &str, retry: &Retry, category: Option<&str>) -> Checked<()> {
    ensure(
        !matches!(effect, "committed" | "partial" | "unknown") || restrictive(retry),
        Code::Result,
    )?;
    ensure(
        category != Some("resource_limit") || effect != "none" || retry.kind() == "never",
        Code::Result,
    )
}
fn reports(reports: &[Report]) -> Checked<()> {
    ensure(
        reports
            .iter()
            .all(|r| (r.availability == "available") == r.document.is_some()),
        Code::Result,
    )
}
pub(crate) fn error(error: &ChainError) -> Checked<()> {
    axes(&error.remote_effect, &error.retry, Some(&error.category))?;
    reports(&error.reports)
}
fn reconcile(effect: &str, retry: &Retry, child_effect: &str, child_retry: &Retry) -> Checked<()> {
    let compatible = match child_effect {
        "unknown" => effect == "unknown",
        "partial" => matches!(effect, "partial" | "unknown"),
        "committed" => matches!(effect, "committed" | "partial" | "unknown"),
        _ => true,
    };
    ensure(compatible, Code::Result)?;
    if restrictive(retry) {
        return Ok(());
    }
    ensure(!restrictive(child_retry), Code::Result)?;
    if child_retry.kind() == "requires_idempotency_key" {
        ensure(retry.kind() == child_retry.kind(), Code::Result)?;
    }
    if let Retry::Variant1(original) = child_retry {
        let Retry::Variant1(summary) = retry else {
            return Err(Code::Result.into());
        };
        ensure(summary.delay_ms >= original.delay_ms, Code::Result)?;
    }
    Ok(())
}
fn times(start: Option<&str>, end: Option<&str>) -> Checked<()> {
    match (start, end) {
        (None, None) => Ok(()),
        (Some(start), Some(end)) => {
            let parse = |v: &str| {
                chrono::DateTime::parse_from_rfc3339(&v.replace('t', "T").replace('z', "Z"))
                    .map_err(|_| crate::Rejection(Code::Result))
            };
            ensure(parse(start)? <= parse(end)?, Code::Result)
        }
        _ => Err(Code::Result.into()),
    }
}
impl Counters {
    pub(crate) const fn amounts(&self) -> [u64; 9] {
        [
            self.memory_bytes,
            self.elapsed_ms,
            self.rows_read,
            self.rows_produced,
            self.artifact_bytes,
            self.spool_bytes,
            self.request_bytes,
            self.result_bytes,
            self.max_attempts,
        ]
    }
}

pub(crate) fn result(
    result: &ChainResult,
    plan: &Plan,
    validated: &ValidationReport,
) -> Checked<()> {
    ensure(
        result.plan_id == plan.plan_id
            && result.plan_revision == plan.revision
            && result.plan_digest == validated.plan_digest,
        Code::Conflict,
    )?;
    let mut by_id = BTreeMap::new();
    for step in &result.steps {
        ensure(by_id.insert(&step.step_id, step).is_none(), Code::Duplicate)?;
    }
    let expected: BTreeMap<_, _> = plan.steps.iter().map(|s| (&s.id, s)).collect();
    ensure(expected.keys().eq(by_id.keys()), Code::Result)?;
    axes(&result.remote_effect, &result.retry, None)?;
    times(Some(&result.started_at), Some(&result.finished_at))?;
    let mut terminal_seen = false;
    for id in &validated.order {
        let current = by_id.get(id).ok_or(Code::Result)?;
        ensure(!terminal_seen || current.attempt == 0, Code::Result)?;
        if current.state != "succeeded" {
            terminal_seen = true;
        }
    }
    let mut all_reports: Vec<&Report> = result.reports.iter().collect();
    for step in &result.steps {
        let definition = expected.get(&step.step_id).ok_or(Code::Result)?;
        step_result(step, definition, plan, &by_id)?;
        reconcile(
            &result.remote_effect,
            &result.retry,
            &step.remote_effect,
            &step.retry,
        )?;
        all_reports.extend(&step.reports);
        for error in step.error.iter().chain(&step.other_errors) {
            all_reports.extend(&error.reports);
        }
        for artifact in step.outputs.values() {
            all_reports.extend(&artifact.reports);
        }
    }
    for e in result.error.iter().chain(&result.other_errors) {
        all_reports.extend(&e.reports);
        error(e)?;
        reconcile(
            &result.remote_effect,
            &result.retry,
            &e.remote_effect,
            &e.retry,
        )?;
    }
    aggregate(result, plan, &expected)?;
    for (name, artifact) in &result.outputs {
        let output = plan.outputs.get(name).ok_or(Code::Result)?;
        let Source::Variant1(source) = &output.source else {
            return Err(Code::Result.into());
        };
        ensure(
            by_id
                .get(&source.step)
                .and_then(|s| s.outputs.get(&source.port))
                == Some(artifact),
            Code::Result,
        )?;
    }
    ensure(
        all_reports
            .iter()
            .all(|r| (r.availability == "available") == r.document.is_some()),
        Code::Result,
    )?;
    ensure(
        !result.reports_complete || all_reports.iter().all(|r| r.availability != "unavailable"),
        Code::Result,
    )?;
    ensure(
        result.reports_complete || result.losses != "none",
        Code::Result,
    )?;
    if result.state == "succeeded" {
        ensure(
            result.steps.iter().all(|s| s.state == "succeeded")
                && result.error.is_none()
                && result.other_errors.is_empty()
                && result.reports_complete
                && result.losses != "unavailable"
                && result.outputs.keys().eq(plan.outputs.keys()),
            Code::Result,
        )?;
    }
    if result.state == "cancelled" {
        ensure(
            result
                .steps
                .iter()
                .filter(|s| s.attempt > 0)
                .all(|s| s.stop_confirmed),
            Code::Result,
        )?;
    }
    Ok(())
}

fn step_result(
    step: &StepResult,
    definition: &Step,
    plan: &Plan,
    by_id: &BTreeMap<&String, &StepResult>,
) -> Checked<()> {
    ensure(
        step.operation == definition.operation
            && step.operation_version == definition.operation_version,
        Code::Version,
    )?;
    axes(&step.remote_effect, &step.retry, None)?;
    times(step.started_at.as_deref(), step.finished_at.as_deref())?;
    ensure(
        step.counters.max_attempts == step.attempt
            && step.attempt <= definition.retry_policy.max_attempts,
        Code::Result,
    )?;
    ensure(
        step.counters
            .amounts()
            .iter()
            .zip(definition.budgets.amounts())
            .all(|(n, limit)| *n <= limit),
        Code::Result,
    )?;
    if matches!(step.state.as_str(), "blocked" | "skipped" | "rejected") {
        ensure(
            step.attempt == 0 && step.remote_effect == "none" && step.outputs.is_empty(),
            Code::Result,
        )?;
    }
    if step.attempt > 0 {
        ensure(
            step.input_manifests.keys().eq(definition.inputs.keys()),
            Code::Result,
        )?;
        for (name, binding) in &definition.inputs {
            let manifest = match &binding.source {
                Source::Variant0(source) => {
                    plan.inputs.get(&source.input).ok_or(Code::Result)?.clone()
                }
                Source::Variant1(source) => {
                    let producer = by_id.get(&source.step).ok_or(Code::Result)?;
                    ensure(producer.state == "succeeded", Code::Result)?;
                    let artifact = producer.outputs.get(&source.port).ok_or(Code::Result)?;
                    Input {
                        artifact_ref: artifact.artifact_ref.clone(),
                        port: artifact.port.clone(),
                        bytes: artifact.bytes,
                        sha256: artifact.sha256.clone(),
                    }
                }
            };
            ensure(
                step.input_manifests.get(name) == Some(&manifest),
                Code::Result,
            )?;
        }
    }
    ensure(
        step.state != "cancelled" || step.stop_confirmed,
        Code::Result,
    )?;
    ensure(
        !matches!(
            step.state.as_str(),
            "failed" | "partial" | "requires_recovery" | "rejected"
        ) || step.error.is_some(),
        Code::Result,
    )?;
    ensure(
        step.remote_effect != "unknown" || step.state == "requires_recovery",
        Code::Result,
    )?;
    if step.state == "succeeded" {
        ensure(
            step.error.is_none()
                && step.other_errors.is_empty()
                && !matches!(
                    step.remote_effect.as_str(),
                    "partial" | "unknown" | "rolled_back"
                )
                && step.outputs.keys().eq(definition.outputs.keys())
                && step.attempt > 0
                && step.upstream_result.is_some(),
            Code::Result,
        )?;
    }
    for (port, artifact) in &step.outputs {
        ensure(
            definition.outputs.get(port) == Some(&artifact.port)
                && artifact.producer_step == step.step_id
                && artifact.attempt == step.attempt
                && (step.state != "succeeded" || artifact.publication == "confirmed"),
            Code::Result,
        )?;
    }
    for e in step.error.iter().chain(&step.other_errors) {
        error(e)?;
        reconcile(&step.remote_effect, &step.retry, &e.remote_effect, &e.retry)?;
    }
    Ok(())
}

fn aggregate(
    result: &ChainResult,
    plan: &Plan,
    expected: &BTreeMap<&String, &Step>,
) -> Checked<()> {
    let has_state = |state: &str| result.steps.iter().any(|s| s.state == state);
    let has_effect = |effect: &str| result.steps.iter().any(|s| s.remote_effect == effect);
    if matches!(
        result.state.as_str(),
        "rejected" | "failed" | "partial" | "requires_recovery"
    ) {
        ensure(result.error.is_some(), Code::Result)?;
    }
    match result.state.as_str() {
        "rejected" => ensure(
            result
                .steps
                .iter()
                .all(|s| s.attempt == 0 && s.remote_effect == "none"),
            Code::Result,
        )?,
        "failed" => ensure(has_state("failed") || has_state("rejected"), Code::Result)?,
        "partial" => ensure(
            has_state("partial")
                || (has_state("succeeded") && result.steps.iter().any(|s| s.state != "succeeded")),
            Code::Result,
        )?,
        "cancelled" => ensure(has_state("cancelled"), Code::Result)?,
        _ => {}
    }
    for (index, (amount, limit)) in result
        .counters
        .amounts()
        .iter()
        .zip(plan.budgets.amounts())
        .enumerate()
    {
        ensure(*amount <= limit, Code::Result)?;
        if !matches!(index, 0 | 1 | 5) {
            let total = result.steps.iter().try_fold(0_u64, |sum, step| {
                let n = step
                    .counters
                    .amounts()
                    .get(index)
                    .copied()
                    .ok_or(Code::Result)?;
                sum.checked_add(n).ok_or(Code::Result)
            })?;
            ensure(*amount == total, Code::Result)?;
        }
    }
    let effect = if has_effect("unknown") {
        "unknown"
    } else if has_effect("partial") || (has_effect("committed") && result.state != "succeeded") {
        "partial"
    } else if has_effect("committed") {
        "committed"
    } else {
        "none"
    };
    ensure(
        result.remote_effect == effect && has_effect("unknown") == result.recovery.required,
        Code::Result,
    )?;
    ensure(
        !has_effect("unknown") || result.state == "requires_recovery",
        Code::Result,
    )?;
    ensure(
        result
            .recovery
            .resumable_steps
            .iter()
            .all(|id| expected.contains_key(id)),
        Code::Reference,
    )
}

pub(crate) fn event(event: &Event) -> Checked<()> {
    ensure(
        (event.kind == "step_state") == event.step_id.is_some(),
        Code::Event,
    )?;
    let state = match event.kind.as_str() {
        "accepted" => Some("pending"),
        "cancel_requested" => Some("cancel_requested"),
        "cancel_confirmed" => Some("cancelled"),
        "recovery_required" => Some("requires_recovery"),
        _ => None,
    };
    ensure(state.is_none_or(|s| s == event.state), Code::Event)?;
    ensure(
        matches!(
            event.kind.as_str(),
            "result_persisted" | "delivery_confirmed"
        ) == event.result_ref.is_some(),
        Code::Event,
    )?;
    ensure(
        event.kind == "step_state" || event.attempt == 0,
        Code::Event,
    )?;
    if let Some(e) = &event.error {
        error(e)?;
    }
    Ok(())
}
