//! Run-owned copies for the declared provider double; immutable fixture templates are not deleted.
use super::ScriptedAdapter;
use plenora_runtime_chains::{
    Error, Invocation, OwnedResource, ResourceLedger, ResourceRequest,
    contracts::{Source, StepResult},
};
use std::collections::BTreeMap;

pub(super) fn scoped(run: &str, reference: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("ref:owned-{run}-{:x}", Sha256::digest(reference.as_bytes()))
}
pub(super) fn owned_run(reference: &str) -> Option<&str> {
    let suffix = reference.strip_prefix("ref:owned-")?;
    if suffix.len() != 101 || suffix.as_bytes().get(36) != Some(&b'-') {
        return None;
    }
    let run = suffix.get(..36)?;
    uuid::Uuid::parse_str(run)
        .ok()
        .filter(|u| u.hyphenated().to_string() == run)
        .map(|_| run)
}
impl ScriptedAdapter {
    pub(super) fn template(&self, reference: &str) -> Result<&Vec<u8>, Error> {
        if let Some(bytes) = self.artifacts.get(reference) {
            return Ok(bytes);
        }
        let run = owned_run(reference).ok_or(Error::NotFound)?;
        self.artifacts
            .iter()
            .find(|(r, _)| scoped(run, r) == reference)
            .map(|(_, bytes)| bytes)
            .ok_or(Error::NotFound)
    }
    pub(super) fn materialize(i: &Invocation, original: &StepResult) -> Result<StepResult, Error> {
        let mut result = original.clone();
        for (name, input) in &mut result.input_manifests {
            if matches!(
                i.step.inputs.get(name).map(|b| &b.source),
                Some(Source::Variant1(_))
            ) {
                let actual = &i.inputs.get(name).ok_or(Error::Invalid)?.artifact_ref;
                if actual != &input.artifact_ref
                    && actual != &scoped(&i.run_id, &input.artifact_ref)
                {
                    return Err(Error::Invalid);
                }
                input.artifact_ref.clone_from(actual);
            }
        }
        for output in result.outputs.values_mut() {
            output.artifact_ref = scoped(&i.run_id, &output.artifact_ref);
        }
        Ok(result)
    }
    pub(super) async fn check_owned_inputs(&self, i: &Invocation) -> Result<(), Error> {
        let owned: Vec<_> = i
            .inputs
            .values()
            .filter(|a| a.artifact_ref.starts_with("ref:owned-"))
            .collect();
        if owned.is_empty() {
            return Ok(());
        }
        for input in owned {
            if owned_run(&input.artifact_ref) != Some(i.run_id.as_str())
                || self
                    .store
                    .read_resource(&i.tenant, &i.run_id, &input.artifact_ref, 1024 * 1024)
                    .await?
                    != *self.template(&input.artifact_ref)?
            {
                return Err(Error::Invalid);
            }
        }
        Ok(())
    }
    pub(super) fn owned_request(
        &self,
        i: &Invocation,
        result: &StepResult,
    ) -> Result<ResourceRequest, Error> {
        let plan = self.authorized.get(&i.plan_digest).ok_or(Error::Invalid)?;
        let mut objects: BTreeMap<String, (&Vec<u8>, Vec<String>, bool)> = BTreeMap::new();
        for (reference, producer) in i.inputs.values().map(|a| (&a.artifact_ref, None)).chain(
            result
                .outputs
                .iter()
                .map(|(port, a)| (&a.artifact_ref, Some(port))),
        ) {
            let bytes = self.template(reference)?;
            let mut consumers = Vec::new();
            for step in &plan.steps {
                if step.inputs.values().any(|binding| match &binding.source {
                    Source::Variant0(source) => plan
                        .inputs
                        .get(&source.input)
                        .is_some_and(|a| &a.artifact_ref == reference),
                    Source::Variant1(source) => producer
                        .is_some_and(|port| source.step == i.step.id && &source.port == port),
                }) {
                    consumers.push(step.id.clone());
                }
            }
            let borrowed = producer.is_none() && owned_run(reference).is_none();
            let key = if borrowed {
                scoped(&i.message_id, reference).replacen("ref:owned-", "ref:borrow-", 1)
            } else {
                reference.clone()
            };
            let entry = objects.entry(key).or_insert((bytes, vec![], borrowed));
            entry.1.extend(consumers);
            entry.1.sort();
            entry.1.dedup();
        }
        // The fake creates an actual run-owned serialized request buffer. It is independent
        // of the provider template and may be discarded only after authenticated stop proof.
        let temporary = format!("ref:temporary-{}", i.message_id);
        objects.insert(
            temporary,
            (
                self.artifacts
                    .get(&i.step.parameters.artifact_ref)
                    .ok_or(Error::NotFound)?,
                vec![],
                true,
            ),
        );
        // A conservative encoding/copy bound is checked before cloning any owned payload.
        let reserved =
            objects
                .iter()
                .try_fold(0_u64, |sum, (reference, (bytes, consumers, _))| {
                    let strings =
                        reference.len() + consumers.iter().map(String::len).sum::<usize>();
                    sum.checked_add(
                        bytes.len() as u64 * 6
                            + strings as u64 * 6
                            + consumers.len() as u64 * 4
                            + 128,
                    )
                    .ok_or(Error::ResourceLimit)
                })?;
        if reserved > i.reservation.limits.memory_bytes
            || reserved > i.reservation.limits.spool_bytes
        {
            return Err(Error::ResourceLimit);
        }
        let objects = objects
            .into_iter()
            .map(|(reference, (bytes, consumers, temporary))| OwnedResource {
                reference,
                bytes: bytes.clone(),
                consumers,
                temporary,
            })
            .collect();
        Ok(ResourceRequest::new(i, plan.budgets.clone(), objects))
    }
    pub(super) async fn reserve_owned(
        &self,
        i: &Invocation,
        result: &StepResult,
    ) -> Result<(), Error> {
        let request = self.owned_request(i, result)?;
        let mut ledger = self
            .store
            .resource_snapshot(&i.tenant, &i.run_id)
            .await?
            .unwrap_or_else(|| ResourceLedger::new(&request.limits));
        ledger.acquire(
            &request,
            i.started_ms,
            i.started_ms
                .checked_add(plenora_runtime_chains::REPORT_MS)
                .ok_or(Error::ResourceLimit)?,
        )?;
        Ok(())
    }
}
