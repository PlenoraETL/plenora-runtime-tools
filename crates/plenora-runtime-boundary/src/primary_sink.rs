//! Confirmation for durable pull results; this never attests external broker delivery.
use crate::{BoundaryResultSink, DurableExecutionStore};
use async_trait::async_trait;
use plenora_runtime_capabilities::CapabilityResultPublishError;
use plenora_runtime_messaging::{PublishOutcome, SerializedMessage};
use std::sync::Arc;
// A local result route confirms only bytes already in the authoritative receipt, not broker delivery.
/// Local pull-result route: confirms only the exact terminal already present on the primary.
pub struct PrimaryResultSink<S> {
    store: Arc<S>,
    scope: String,
}
#[async_trait]
impl<S: DurableExecutionStore> BoundaryResultSink for PrimaryResultSink<S> {
    async fn publish(
        &self,
        message: SerializedMessage,
    ) -> Result<PublishOutcome, CapabilityResultPublishError> {
        let confirmed = async {
            let id = message
                .headers
                .get_text(plenora_runtime_messaging::MESSAGE_ID_METADATA_KEY)
                .ok()
                .flatten()?;
            let prefix = format!("{}:{}:", self.scope.len(), self.scope);
            let (_, j) = self.store.result_receipt(&prefix, id).await.ok()?;
            (j.evidence()?.message().ok()? == message).then_some(())
        }
        .await
        .is_some();
        if confirmed {
            Ok(PublishOutcome::Confirmed)
        } else {
            Err(CapabilityResultPublishError::with_source(
                crate::SettlementError,
            ))
        }
    }
}
impl<S> PrimaryResultSink<S> {
    /// Bind a trusted tenant scope to the authoritative result store.
    /// # Errors
    /// Rejects noncanonical or oversized scope; no destination is selected by message headers.
    pub fn new(store: Arc<S>, scope: &str) -> Result<Self, crate::SettlementError> {
        if scope.is_empty() || scope.len() > 128 || !scope.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(crate::SettlementError);
        }
        Ok(Self {
            store,
            scope: scope.into(),
        })
    }
}
