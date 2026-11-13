//! Per-invocation identity decoration before the real sink is called.
use async_trait::async_trait;
use plenora_runtime_capabilities::{
    CapabilityResult, CapabilityResultPublishError, CapabilityResultSink,
};
use plenora_runtime_messaging::{
    CAUSATION_ID_METADATA_KEY, MESSAGE_ID_METADATA_KEY, MessageProducer, PublishOutcome,
    SerializedMessage,
};
use serde_json::{Map, Value};
use std::sync::Arc;

/// Destination for a complete result including its new message identity.
#[async_trait]
pub trait BoundaryResultSink: Send + Sync {
    /// Confirms persistence/publication explicitly; uncertainty must remain uncertainty.
    /// # Errors
    /// Returns a sink failure when persistence/publication cannot be confirmed.
    async fn publish(
        &self,
        message: SerializedMessage,
    ) -> Result<PublishOutcome, CapabilityResultPublishError>;
}
#[async_trait]
impl<P: MessageProducer> BoundaryResultSink for P {
    async fn publish(
        &self,
        message: SerializedMessage,
    ) -> Result<PublishOutcome, CapabilityResultPublishError> {
        MessageProducer::publish(self, message)
            .await
            .map_err(CapabilityResultPublishError::with_source)
    }
}

pub(crate) struct IdentitySink {
    pub(crate) sink: Arc<dyn BoundaryResultSink>,
    pub(crate) metadata: Map<String, Value>,
}
#[async_trait]
impl CapabilityResultSink for IdentitySink {
    async fn publish_result(
        &self,
        result: CapabilityResult,
    ) -> Result<PublishOutcome, CapabilityResultPublishError> {
        let mut message = result.into_message();
        for key in [MESSAGE_ID_METADATA_KEY, CAUSATION_ID_METADATA_KEY] {
            let _previous = message.headers.remove(key);
            if let Some(value) = self.metadata.get(key).and_then(Value::as_str) {
                message
                    .headers
                    .insert_text(key, value)
                    .map_err(CapabilityResultPublishError::with_source)?;
            }
        }
        self.sink.publish(message).await
    }
}
