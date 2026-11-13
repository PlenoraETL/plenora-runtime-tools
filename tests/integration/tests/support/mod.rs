//! Explicit fixture host for broker lifecycle tests; no durable settlement claim.
use async_trait::async_trait;
use plenora_runtime_boundary::*;
use plenora_runtime_capabilities::*;
use plenora_runtime_core::SystemClock;
use plenora_runtime_messaging::{PublishOutcome, RetryErrorClass, SerializedMessage};
use plenora_runtime_worker::WorkerContext;
use std::{
    error::Error,
    sync::{Arc, Mutex},
};

struct Policy;
impl AdmissionPolicy for Policy {
    fn idempotency_key_limit(&self, _: &str, _: &str, _: &str) -> Option<usize> {
        None
    }
    fn validate_payload(&self, _: &CapabilityRequest) -> Result<(), PlenoraErrorCategory> {
        Ok(())
    }
}
#[derive(Default)]
struct Sink(Mutex<Vec<SerializedMessage>>);
#[async_trait]
impl BoundaryResultSink for Sink {
    async fn publish(
        &self,
        message: SerializedMessage,
    ) -> Result<PublishOutcome, CapabilityResultPublishError> {
        self.0
            .lock()
            .map_err(|_| {
                CapabilityResultPublishError::with_source(std::io::Error::other(
                    "fixture sink lock",
                ))
            })?
            .push(message);
        Ok(PublishOutcome::Confirmed)
    }
}
struct Adapter<H>(H);
#[async_trait]
impl<H: CapabilityHandler> CapabilityHandler for Adapter<H> {
    async fn invoke(
        &self,
        context: WorkerContext,
        request: CapabilityRequest,
    ) -> Result<CapabilityResponse, CapabilityFailure> {
        match self.0.invoke(context, request).await {
            Ok(response) if response.output().is_none() => {
                let contract = ContractId::new("plenora-fixture-output-v1").map_err(|e| {
                    CapabilityFailure::new(
                        RetryErrorClass::Permanent,
                        CapabilityRemoteEffect::NotStarted,
                        e,
                    )
                })?;
                Ok(CapabilityResponse::new(
                    contract,
                    SerializedMessage::new("application/json", "{}"),
                ))
            }
            Ok(response) => Ok(response),
            Err(error) => {
                let retry = match error.retry_classification() {
                    RetryErrorClass::Retryable => PlenoraErrorRetry::Safe,
                    RetryErrorClass::DeadLetter => PlenoraErrorRetry::Quarantine,
                    RetryErrorClass::OutcomeUnknown => PlenoraErrorRetry::RequiresRecovery,
                    RetryErrorClass::Permanent => PlenoraErrorRetry::Never,
                };
                let public = PlenoraError::new(
                    PlenoraErrorCategory::Internal,
                    PlenoraErrorPhase::Prepare,
                    PlenoraErrorRemoteEffect::None,
                    retry,
                    "Fixture handler failed.",
                )
                .map_err(|e| {
                    CapabilityFailure::new(
                        RetryErrorClass::Permanent,
                        CapabilityRemoteEffect::NotStarted,
                        e,
                    )
                })?;
                Err(CapabilityFailure::with_public_error(public, error))
            }
        }
    }
}
pub fn host<H: CapabilityHandler + 'static>(
    capability: &str,
    input: &str,
    operations: &[&str],
    handler: H,
) -> Result<RuntimeBoundary, Box<dyn Error>> {
    let operations: Vec<_> = operations.iter().map(|id| serde_json::json!({
        "id": id, "version": 1, "status": "available", "surfaces": ["runtime"],
        "input": {"contract": input, "content_types": ["application/octet-stream"]},
        "output": {"contract": "plenora-fixture-output-v1", "content_types": ["application/json"]},
        "side_effect": "none", "controls": {"deadline": false, "idempotency_key": false, "cancellation": true}
    })).collect();
    let discovery = serde_json::json!({"schema_version": 2, "component": "plenora-fixture-tools", "component_version": "1.0.0", "interfaces": [{"kind":"runtime", "contract":"plenora-runtime-binding-v1", "version":1,"artifact":capability}], "operations":operations});
    let mut registry = CapabilityRegistryBuilder::default();
    registry.register_discovered(
        CapabilityDiscovery::from_json(&serde_json::to_vec(&discovery)?)?,
        Adapter(handler),
    )?;
    Ok(RuntimeBoundary::new(
        CapabilityDispatcher::new(registry.build(), CapabilityDispatcherConfig::default())?,
        Arc::new(SystemClock),
        Arc::new(Policy),
        Arc::new(Sink::default()),
    ))
}
