//! Durable execution around the real dispatcher; publication never authorizes domain replay.
use async_trait::async_trait;
use plenora_runtime_capabilities::{
    CapabilityFailure, CapabilityHandler, CapabilityRemoteEffect, CapabilityRequest,
    CapabilityResponse, PlenoraError, PlenoraErrorPhase, PlenoraErrorRemoteEffect,
};
use plenora_runtime_messaging::{RetryErrorClass, SerializedMessage};
use plenora_runtime_worker::WorkerContext;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, error::Error, fmt};

/// Lossless durable carrier, independent of broker headers and domain payload format.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredMessage {
    content_type: String,
    bytes: Vec<u8>,
    headers: BTreeMap<String, Vec<u8>>,
}
impl StoredMessage {
    /// Encodes an already admitted/result carrier without normalizing metadata.
    /// # Errors
    /// Refuses unresolved raw metadata or transport-limit markers.
    pub fn capture(message: &SerializedMessage) -> Result<Self, SettlementError> {
        if message.raw_metadata().is_some()
            || message.has_unreadable_metadata()
            || message.exceeds_transport_payload_limit()
        {
            return Err(SettlementError);
        }
        Ok(Self {
            content_type: message.content_type.to_string(),
            bytes: message.bytes.to_vec(),
            headers: message
                .headers
                .iter()
                .map(|(k, v)| (k.to_owned(), v.to_vec()))
                .collect(),
        })
    }
    /// Restores the complete carrier within portable metadata bounds.
    /// # Errors
    /// Refuses malformed stored metadata.
    pub fn restore(self) -> Result<SerializedMessage, SettlementError> {
        let mut message = SerializedMessage::new(self.content_type, self.bytes);
        for (k, v) in self.headers {
            message.headers.insert(k, v).map_err(|_| SettlementError)?;
        }
        Ok(message)
    }
    /// Serializes the exact carrier with deterministic map ordering.
    /// # Errors
    /// Returns an explicit serialization error.
    pub fn encode(&self) -> Result<Vec<u8>, SettlementError> {
        serde_json::to_vec(self).map_err(|_| SettlementError)
    }
}

/// Explicit component outcome, including cleanup and remote commit knowledge.
/// Components must finish cleanup before returning; a dropped invocation remains recovery-only.
pub struct ComponentOutcome {
    /// Complete success output or a data-free public failure.
    pub(crate) result: Result<CapabilityResponse, PlenoraError>,
    /// Last phase proved by the component, including cleanup when applicable.
    pub(crate) phase: PlenoraErrorPhase,
    /// Remote effect proved by the component, never inferred from output size.
    pub(crate) effect: PlenoraErrorRemoteEffect,
}
/// Component adapter supplying explicit evidence before dispatcher result validation.
#[async_trait]
pub trait EvidenceComponent: Send + Sync {
    /// Executes domain work once under a durable start authority.
    async fn invoke(
        &self,
        context: WorkerContext,
        request: CapabilityRequest,
        budget: OutcomeBudget,
    ) -> ComponentOutcome;
}
/// Persistence, evidence or settlement was not confirmed; no payload/source data escapes.
#[derive(Debug, Clone, Copy)]
pub struct SettlementError;
impl fmt::Display for SettlementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("durable settlement not confirmed")
    }
}
impl Error for SettlementError {}

pub(crate) struct Unbound;
#[async_trait]
impl CapabilityHandler for Unbound {
    async fn invoke(
        &self,
        _: WorkerContext,
        _: CapabilityRequest,
    ) -> Result<CapabilityResponse, CapabilityFailure> {
        Err(failure())
    }
}
fn failure() -> CapabilityFailure {
    CapabilityFailure::new(
        RetryErrorClass::OutcomeUnknown,
        CapabilityRemoteEffect::Unknown,
        SettlementError,
    )
}

/// Unforgeable per-invocation output reservation, supplied before domain work.
#[derive(Clone, Copy)]
pub struct OutcomeBudget(pub(crate) usize);
impl OutcomeBudget {
    /// Maximum accepted payload bytes. Adapters use this while producing output.
    #[must_use]
    pub const fn bytes(self) -> usize {
        self.0
    }
    /// Constructs a complete success or a component resource-limit outcome, never truncated output.
    /// # Errors
    /// Fails if the static resource-limit mapping is invalid.
    pub fn success(
        self,
        response: CapabilityResponse,
        phase: PlenoraErrorPhase,
        effect: PlenoraErrorRemoteEffect,
    ) -> Result<ComponentOutcome, SettlementError> {
        if response.output().is_some_and(|m| {
            m.len() > self.0
                || m.content_type.len() > plenora_runtime_messaging::MAX_INGRESS_CONTENT_TYPE_BYTES
                || m.raw_metadata().is_some()
                || m.has_unreadable_metadata()
                || m.exceeds_transport_payload_limit()
        }) {
            let error = PlenoraError::new(
                plenora_runtime_capabilities::PlenoraErrorCategory::ResourceLimit,
                phase,
                effect,
                if effect == PlenoraErrorRemoteEffect::None {
                    plenora_runtime_capabilities::PlenoraErrorRetry::Never
                } else {
                    plenora_runtime_capabilities::PlenoraErrorRetry::RequiresRecovery
                },
                "Component output reservation exceeded.",
            )
            .map_err(|_| SettlementError)?;
            return Ok(ComponentOutcome {
                result: Err(error),
                phase,
                effect,
            });
        }
        Ok(ComponentOutcome {
            result: Ok(response),
            phase,
            effect,
        })
    }
    /// Constructs a failure using exactly the component's validated semantic axes.
    #[must_use]
    pub fn failure(self, error: PlenoraError) -> ComponentOutcome {
        ComponentOutcome {
            phase: error.phase(),
            effect: error.remote_effect(),
            result: Err(error),
        }
    }
}
