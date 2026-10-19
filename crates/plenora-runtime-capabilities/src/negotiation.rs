//! Host policy for RT-026/027; context travels outside the runtime envelope.
//!
//! Deviation RT-027: NATS/Apalis do not yet install this policy or a durable store.
//! See docs/contract-alignment.md: a store implementation and transport wiring are
//! required before claiming end-to-end Runtime Binding 2.0 conformance.

use std::{error::Error, fmt};

use plenora_runtime_messaging::MessageId;

use crate::{
    CapabilityDiscovery, CapabilitySurface, PlenoraError, PlenoraErrorCategory, PlenoraErrorPhase,
    PlenoraErrorRemoteEffect, PlenoraErrorRetry, PlenoraErrorValidationError,
};

/// Negotiated contract version, independent of wire version (always 1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeBinding {
    /// Runtime Binding 1.0.
    V1,
    /// Runtime Binding 2.0.
    V2,
}

/// Exact transport context, never embedded in a serialized domain request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NegotiationContext {
    /// Contract the sender requires.
    pub requires: RuntimeBinding,
    /// Sender's explicit permission to use 1.0 instead of 2.0.
    pub accepts_downgrade: bool,
}

impl NegotiationContext {
    /// Parses the two transport header values without normalization.
    ///
    /// # Errors
    /// Rejects a missing half, unknown contract, or noncanonical boolean.
    pub fn from_headers(
        requires: Option<&str>,
        accepts_downgrade: Option<&str>,
    ) -> Result<Option<Self>, NegotiationError> {
        let requires = match (requires, accepts_downgrade) {
            (None, None) => return Ok(None),
            (Some("plenora-runtime-binding-v1"), Some(_)) => RuntimeBinding::V1,
            (Some("plenora-runtime-binding-v2"), Some(_)) => RuntimeBinding::V2,
            _ => return Err(NegotiationError),
        };
        let accepts_downgrade = match accepts_downgrade {
            Some("true") => true,
            Some("false") => false,
            _ => return Err(NegotiationError),
        };
        Ok(Some(Self {
            requires,
            accepts_downgrade,
        }))
    }

    /// Exact values of `plenora.negotiation.requires` and
    /// `plenora.negotiation.accepts_downgrade`, respectively.
    #[must_use]
    pub const fn headers(self) -> (&'static str, &'static str) {
        (
            match self.requires {
                RuntimeBinding::V1 => "plenora-runtime-binding-v1",
                RuntimeBinding::V2 => "plenora-runtime-binding-v2",
            },
            if self.accepts_downgrade {
                "true"
            } else {
                "false"
            },
        )
    }
}

/// Outcome of one authoritative, atomic lookup-or-create.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptanceRecord {
    /// The proposed context was durably created by this reception.
    Created,
    /// A previous reception already fixed this context.
    Existing(NegotiationContext),
}

/// Application-owned durable acceptance storage (RT-027).
///
/// Implementations must serialize lookup-or-create by message identity, commit
/// before returning `Created`, and read authoritatively at every transport hop.
/// `None` never creates a record. An absent record with `None`, an expired record,
/// a stale/unavailable read, or an unconfirmed write returns `NegotiationError`.
/// Retain records for the entire delivery/retry/redrive/resend window declared
/// by the deployment, then retain tombstones so an old identity is never new.
/// An in-memory implementation is suitable only for tests, not this guarantee.
pub trait AcceptanceStore {
    /// Atomically looks up a record or durably creates the proposed context.
    ///
    /// # Errors
    /// Refuses any result whose absence, durability or validity is unproven.
    fn accept(
        &self,
        message_id: &MessageId,
        proposed: Option<NegotiationContext>,
    ) -> Result<AcceptanceRecord, NegotiationError>;
}

/// Accepts a new ingress or verifies a forwarding before destination selection.
/// `external` describes the source, not whether the identity is new. A resend
/// from the sender is still a forwarding when its record already exists.
///
/// # Errors
/// Refuses lost/altered context, unreadable records, an incapable hop, and new
/// requests below the configured ingress minimum. All failures precede sending.
pub fn accept_runtime_request(
    store: &impl AcceptanceStore,
    message_id: &MessageId,
    received: Option<NegotiationContext>,
    external: bool,
    minimum: RuntimeBinding,
    carries_context: bool,
) -> Result<NegotiationContext, NegotiationError> {
    if !carries_context {
        return Err(NegotiationError);
    }
    let candidate = received.unwrap_or(NegotiationContext {
        requires: RuntimeBinding::V1,
        accepts_downgrade: false,
    });
    // Below-minimum receptions may still be forwardings of already accepted
    // requests. Lookup only; do not create a record that ingress would refuse.
    let may_create =
        external && (minimum == RuntimeBinding::V1 || candidate.requires == RuntimeBinding::V2);
    match store.accept(message_id, may_create.then_some(candidate))? {
        AcceptanceRecord::Created if may_create => Ok(candidate),
        AcceptanceRecord::Existing(fixed) if received == Some(fixed) => Ok(fixed),
        AcceptanceRecord::Created | AcceptanceRecord::Existing(_) => Err(NegotiationError),
    }
}

/// Selects the exchange contract from the effective destination's discovery.
/// Catalog surfaces alone are never proof that an artifact is available.
///
/// # Errors
/// Refuses unreadable discovery, invalid interface pairs and implicit downgrade.
pub fn select_runtime_destination(
    context: NegotiationContext,
    destination: Option<&CapabilityDiscovery>,
) -> Result<RuntimeBinding, NegotiationError> {
    let destination = destination.ok_or(NegotiationError)?;
    destination
        .runtime_capability()
        .map_err(|_| NegotiationError)?;
    let interface = destination
        .interfaces()
        .iter()
        .find(|interface| interface.kind() == CapabilitySurface::Runtime)
        .ok_or(NegotiationError)?;
    match interface.version() {
        2 => Ok(RuntimeBinding::V2),
        1 if context.requires == RuntimeBinding::V1 || context.accepts_downgrade => {
            Ok(RuntimeBinding::V1)
        }
        _ => Err(NegotiationError),
    }
}

/// Data-free refusal of negotiation or acceptance storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NegotiationError;

impl NegotiationError {
    /// Builds the exact RT-026/027 public rejection axes.
    ///
    /// # Errors
    /// Returns an error if the static public mapping violates an invariant.
    pub fn public_error(self) -> Result<PlenoraError, PlenoraErrorValidationError> {
        PlenoraError::new(
            PlenoraErrorCategory::Unsupported,
            PlenoraErrorPhase::Validate,
            PlenoraErrorRemoteEffect::None,
            PlenoraErrorRetry::Never,
            "Runtime negotiation refused before sending.",
        )
    }
}

impl fmt::Display for NegotiationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("runtime negotiation refused before sending")
    }
}

impl Error for NegotiationError {}
