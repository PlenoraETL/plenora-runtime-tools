//! Deliberately nonconforming adapter: tests preservation, never a qualified provider.
use async_trait::async_trait;
use plenora_runtime_chains::{Adapter, Error, Invocation, contracts::*};
pub fn outcome() -> Result<StepResult, Error> {
    let mut step = parse_result(include_bytes!(
        "../../../../contracts/chains-v2/fixtures/result-valid.json"
    ))?
    .steps
    .remove(0);
    let mut artifact = step.outputs.values().next().ok_or(Error::Invalid)?.clone();
    artifact.reports = vec![
        Report {
            kind: "diagnostics".into(),
            availability: "available".into(),
            document: Some(Document {
                contract: "plenora-diagnostics-v1".into(),
                artifact_ref: format!("ref:{}", "x".repeat(120)),
                sha256: "a".repeat(64)
            })
        };
        64
    ];
    step.outputs = (0..24)
        .map(|n| (format!("output{n}"), artifact.clone()))
        .collect();
    let raw = serde_json::to_vec(&step).map_err(|_| Error::Invalid)?;
    parse_step_result(&raw).map_err(Error::from)
}
pub struct Oversized;
#[async_trait]
impl Adapter for Oversized {
    async fn authorize(&self, _: &str, _: &Plan) -> Result<(), Error> {
        Ok(())
    }
    async fn prepare(&self, _: &Invocation) -> Result<(), Error> {
        Ok(())
    } // Deliberate fault injection.
    async fn invoke(&self, _: &Invocation) -> Result<StepResult, Error> {
        outcome()
    }
    async fn request_cancel(&self, _: &Invocation) -> Result<(), Error> {
        Ok(())
    }
    async fn recover(&self, _: &Invocation) -> Result<Option<StepResult>, Error> {
        Ok(None)
    }
}
