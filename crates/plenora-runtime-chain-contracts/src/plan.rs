//! Static plan checks against an explicit fixture registry; no invocation hooks.
use crate::{
    codec,
    error::{Checked, Code, ensure},
    types::{Budget, Plan, Port, Registry, RegistryBoundariesItem, Source, Step},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// A mandatory schema check at the consumer barrier, never an assumed match.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeferredSchema {
    /// Consumer step identity.
    pub step: String,
    /// Consumer input port identity.
    pub port: String,
}
impl std::fmt::Debug for DeferredSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DeferredSchema { [redacted] }")
    }
}

/// Fidelity is not proved by fixture discovery or by a byte digest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FidelityVerdict {
    /// The adapter must prove exact preservation before admitting the operation.
    ExactRequiresEvidence,
    /// Complete fidelity reports are required; DB consent grants no other loss permission.
    ReportedRequiresEvidence,
}

/// Static validation result. This is deliberately not an executable admission token.
#[derive(Clone, PartialEq, Eq)]
pub struct ValidationReport {
    /// Deterministic topological order with lexicographic tie breaking.
    pub order: Vec<String>,
    /// Consumer schema barriers required for dynamic schemas.
    pub deferred: Vec<DeferredSchema>,
    /// Canonical digest of the complete plan, including consent and annotations.
    pub plan_digest: String,
    /// Explicit unproved fidelity requirement to enforce at the real adapter.
    pub fidelity: FidelityVerdict,
}
impl std::fmt::Debug for ValidationReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ValidationReport { reference_only }")
    }
}
impl ValidationReport {
    /// Fixture evidence cannot authorize an invocation.
    #[must_use]
    pub const fn admission(&self) -> &'static str {
        "reference_only"
    }
}

impl Budget {
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

// Keep the ordered admission checks adjacent to their reference algorithm.
#[allow(clippy::too_many_lines)]
pub(crate) fn semantics(plan: &Plan, registry: &Registry) -> Checked<ValidationReport> {
    ensure(plan.components == registry.components, Code::Version)?;
    let claims: BTreeSet<_> = registry
        .components
        .values()
        .flat_map(|c| &c.claims)
        .collect();
    ensure(
        plan.requirements
            .contracts
            .iter()
            .all(|c| claims.contains(c)),
        Code::Version,
    )?;
    ensure(
        plan.budgets.max_attempts >= plan.steps.len() as u64,
        Code::Budget,
    )?;
    let mut operations = BTreeMap::new();
    for descriptor in &registry.operations {
        ensure(
            operations
                .insert(
                    (
                        &descriptor.component,
                        &descriptor.operation,
                        descriptor.version,
                    ),
                    descriptor,
                )
                .is_none(),
            Code::Duplicate,
        )?;
    }
    let mut boundaries = BTreeMap::new();
    for edge in &registry.boundaries {
        ensure(boundaries.insert(&edge.id, edge).is_none(), Code::Duplicate)?;
    }
    let mut steps = BTreeMap::new();
    for step in &plan.steps {
        ensure(steps.insert(&step.id, step).is_none(), Code::Duplicate)?;
    }
    let mut dependencies = BTreeMap::new();
    let mut deferred = Vec::new();
    for step in &plan.steps {
        ensure(
            plan.components.contains_key(&step.component),
            Code::Reference,
        )?;
        let descriptor = operations
            .get(&(&step.component, &step.operation, step.operation_version))
            .ok_or(Code::Version)?;
        ensure(
            step.input_contract == descriptor.input_contract
                && step.output_contract == descriptor.output_contract
                && step.outputs == descriptor.outputs,
            Code::Boundary,
        )?;
        ensure(
            step.inputs.keys().eq(descriptor.inputs.keys()),
            Code::Reference,
        )?;
        ensure(
            step.parameters.contract == step.input_contract,
            Code::Boundary,
        )?;
        ensure(step.inputs.len() + step.outputs.len() <= 64, Code::Limit)?;
        ensure(
            step.budgets
                .amounts()
                .iter()
                .zip(plan.budgets.amounts())
                .all(|(local, global)| *local <= global),
            Code::Budget,
        )?;
        ensure(
            step.retry_policy.max_attempts <= step.budgets.max_attempts
                && step.retry_policy.backoff_ms <= step.budgets.elapsed_ms,
            Code::Budget,
        )?;
        let mut deps: BTreeSet<&String> = BTreeSet::new();
        for dep in &step.depends_on {
            ensure(steps.contains_key(dep), Code::Reference)?;
            deps.insert(dep);
        }
        for (port, binding) in &step.inputs {
            ensure(
                descriptor.inputs.get(port) == Some(&binding.expected),
                Code::Boundary,
            )?;
            let actual = match &binding.source {
                Source::Variant0(source) => {
                    let actual = &plan.inputs.get(&source.input).ok_or(Code::Reference)?.port;
                    ensure(
                        binding.boundary == "plan-input" && actual == &binding.expected,
                        Code::Boundary,
                    )?;
                    actual
                }
                Source::Variant1(source) => {
                    let producer = steps.get(&source.step).ok_or(Code::Reference)?;
                    let actual = producer.outputs.get(&source.port).ok_or(Code::Reference)?;
                    deps.insert(&source.step);
                    let edge = boundaries.get(&binding.boundary).ok_or(Code::Boundary)?;
                    check_edge(edge, producer, step, actual, &binding.expected, registry)?;
                    if actual.primary_geometry {
                        ensure(has_primary(plan, &producer.component), Code::Boundary)?;
                    }
                    actual
                }
            };
            if binding.expected.primary_geometry {
                ensure(
                    plan.requirements.contracts.iter().any(|c| c == PRIMARY)
                        && has_primary(plan, &step.component),
                    Code::Boundary,
                )?;
            }
            if actual.schema_digest.is_none() {
                ensure(binding.schema_barrier, Code::Boundary)?;
                deferred.push(DeferredSchema {
                    step: step.id.clone(),
                    port: port.clone(),
                });
            }
        }
        dependencies.insert(&step.id, deps);
    }
    let mut order = Vec::new();
    while !dependencies.is_empty() {
        let selected = *dependencies
            .iter()
            .find(|(_, deps)| deps.is_empty())
            .map(|(id, _)| id)
            .ok_or(Code::Cycle)?;
        order.push(selected.clone());
        dependencies.remove(selected);
        for deps in dependencies.values_mut() {
            deps.remove(selected);
        }
    }
    for output in plan.outputs.values() {
        let Source::Variant1(source) = &output.source else {
            return Err(Code::Reference.into());
        };
        let producer = steps.get(&source.step).ok_or(Code::Reference)?;
        let actual = producer.outputs.get(&source.port).ok_or(Code::Reference)?;
        ensure(actual == &output.expected, Code::Boundary)?;
        if actual.primary_geometry {
            ensure(
                has_primary(plan, &producer.component)
                    && plan.requirements.contracts.iter().any(|c| c == PRIMARY),
                Code::Boundary,
            )?;
        }
    }
    consent(plan, &steps)?;
    deferred.sort_by(|a, b| (&a.step, &a.port).cmp(&(&b.step, &b.port)));
    Ok(ValidationReport {
        order,
        deferred,
        plan_digest: crate::plan_digest(plan)?,
        fidelity: if plan.requirements.fidelity == "exact" {
            FidelityVerdict::ExactRequiresEvidence
        } else {
            FidelityVerdict::ReportedRequiresEvidence
        },
    })
}

const PRIMARY: &str = "plenora-arrow-primary-geometry-v1";
fn has_primary(plan: &Plan, component: &str) -> bool {
    plan.components
        .get(component)
        .is_some_and(|c| c.claims.iter().any(|claim| claim == PRIMARY))
}
fn check_edge(
    edge: &RegistryBoundariesItem,
    producer: &Step,
    consumer: &Step,
    actual: &Port,
    expected: &Port,
    registry: &Registry,
) -> Checked<()> {
    ensure(
        edge.source_operation == producer.operation
            && edge.source_version == producer.operation_version
            && edge.target_operation == consumer.operation
            && edge.target_version == consumer.operation_version
            && edge.source_contract == actual.contract
            && edge.target_contract == expected.contract
            && edge.content_type == actual.content_type,
        Code::Boundary,
    )?;
    ensure(edge.mode != "provisional", Code::Boundary)?;
    if edge.mode == "adapter_required" {
        ensure(
            edge.adapter
                .as_ref()
                .is_some_and(|a| registry.adapters.contains(a)),
            Code::Boundary,
        )?;
    }
    ensure(edge.mode != "direct" || actual == expected, Code::Boundary)?;
    ensure(
        !actual.primary_geometry || expected.primary_geometry,
        Code::Boundary,
    )
}

fn consent(plan: &Plan, steps: &BTreeMap<&String, &Step>) -> Checked<()> {
    let mut grants = BTreeMap::new();
    for grant in &plan.loss_policy.grants {
        ensure(grants.insert(&grant.step, grant).is_none(), Code::Duplicate)?;
    }
    ensure(grants.keys().all(|s| steps.contains_key(s)), Code::Consent)?;
    for step in &plan.steps {
        let grant = grants.get(&step.id);
        ensure(
            step.loss_consent.unwrap_or(false) == grant.is_some(),
            Code::Consent,
        )?;
        if step.loss_consent.is_some() {
            ensure(
                step.operation == "database.write" && step.operation_version == 2,
                Code::Consent,
            )?;
        }
        if let Some(grant) = grant {
            ensure(
                grant.operation == step.operation
                    && grant.operation_version == step.operation_version
                    && Some(&grant.destination_ref) == step.destination_ref.as_ref()
                    && grant.input_digest == input_digest(step, plan)?
                    && grant.parameters_digest == step.parameters.sha256,
                Code::Consent,
            )?;
        }
    }
    Ok(())
}

pub(crate) fn input_digest(step: &Step, plan: &Plan) -> Checked<String> {
    let mut value = serde_json::Map::new();
    value.insert("bindings".into(), crate::value(&step.inputs)?);
    value.insert("plan_inputs".into(), crate::value(&plan.inputs)?);
    codec::digest(&serde_json::Value::Object(value))
}
