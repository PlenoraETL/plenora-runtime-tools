//! Product delivery gate. Context is transport-owned and never reaches domain metadata.
use crate::{AdmittedRequest, RuntimeBoundary};
use plenora_runtime_capabilities::{
    AcceptanceStore, CapabilityDiscovery, NegotiationContext, NegotiationError, RuntimeBinding,
    accept_runtime_request, select_runtime_destination,
};
use plenora_runtime_messaging::{MessageProducer, PublishOutcome, SerializedMessage};
use std::{error::Error, fmt, sync::Arc};

const REQUIRES: &str = "plenora.negotiation.requires";
const DOWNGRADE: &str = "plenora.negotiation.accepts_downgrade";

/// The origin is explicit; retry, redrive and resend can never create acceptance.
#[derive(Clone, Copy, Debug)]
pub enum DeliveryRoute {
    /// A sender's first reception, subject to authoritative lookup-or-create.
    Ingress,
    /// Any subsequent send, including retry, DLQ redrive and sender resend.
    Forward,
}

/// Admitted request and exact external carrier. No public constructor or mutable access.
pub struct NegotiatedDelivery {
    pub(crate) admitted: AdmittedRequest,
    pub(crate) message: SerializedMessage,
}
impl NegotiatedDelivery {
    /// Inspect the admitted request without acquiring invocation authority.
    #[must_use]
    pub const fn admitted(&self) -> &AdmittedRequest {
        &self.admitted
    }
}
impl fmt::Debug for NegotiatedDelivery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NegotiatedDelivery(<redacted>)")
    }
}

/// Minimum-v2 product facade. The store must be authoritative and durable.
pub struct NegotiatedRuntime<S> {
    pub(crate) boundary: RuntimeBoundary,
    pub(crate) store: Arc<S>,
}
impl<S: AcceptanceStore> NegotiatedRuntime<S> {
    /// Couples raw admission to the same authoritative store used on each send.
    pub fn new(boundary: RuntimeBoundary, store: Arc<S>) -> Self {
        Self { boundary, store }
    }

    /// Validates raw input and fixes or verifies its external context before invocation.
    /// # Errors
    /// Rejects malformed input, missing/altered context, v1, downgrade consent and store failures.
    pub fn admit(
        &self,
        message: &SerializedMessage,
        route: DeliveryRoute,
    ) -> Result<NegotiatedDelivery, NegotiatedSendError> {
        let mut input = message.clone();
        let _ = input.headers.remove(REQUIRES);
        let _ = input.headers.remove(DOWNGRADE);
        let admitted = self
            .boundary
            .admit_message(&input)
            .map_err(NegotiatedSendError::Admission)?;
        let context = context(message)?;
        accept_runtime_request(
            self.store.as_ref(),
            &admitted.identity().message_id,
            Some(context),
            matches!(route, DeliveryRoute::Ingress),
            RuntimeBinding::V2,
            true,
        )
        .map_err(|_| NegotiatedSendError::Negotiation)?;
        Ok(NegotiatedDelivery {
            admitted,
            message: message.clone(),
        })
    }

    /// Rechecks authoritative acceptance and the effective destination on EVERY publication.
    /// The same operation covers first send, retry, redrive and resend; no cached permit exists.
    /// # Errors
    /// Refuses before sending if context, retention, store or discovery cannot be proved.
    pub async fn send<P: MessageProducer>(
        &self,
        delivery: &NegotiatedDelivery,
        destination: Option<&CapabilityDiscovery>,
        producer: &P,
    ) -> Result<PublishOutcome, NegotiatedSendError> {
        let current = self.admit(&delivery.message, DeliveryRoute::Forward)?;
        let context = context(&current.message)?;
        accept_runtime_request(
            self.store.as_ref(),
            &delivery.admitted.identity().message_id,
            Some(context),
            false,
            RuntimeBinding::V2,
            true,
        )
        .map_err(|_| NegotiatedSendError::Negotiation)?;
        select_runtime_destination(context, destination)
            .map_err(|_| NegotiatedSendError::Negotiation)?;
        destination
            .ok_or(NegotiatedSendError::Negotiation)?
            .validate_request(delivery.admitted.request())
            .map_err(|_| NegotiatedSendError::Negotiation)?;
        producer
            .publish(delivery.message.clone())
            .await
            .map_err(|_| NegotiatedSendError::Publication)
    }
}

fn context(message: &SerializedMessage) -> Result<NegotiationContext, NegotiatedSendError> {
    if message.raw_metadata().is_some() || message.has_unreadable_metadata() {
        return Err(NegotiatedSendError::Negotiation);
    }
    let parse = || -> Result<_, NegotiationError> {
        NegotiationContext::from_headers(
            message
                .headers
                .get_text(REQUIRES)
                .map_err(|_| NegotiationError)?,
            message
                .headers
                .get_text(DOWNGRADE)
                .map_err(|_| NegotiationError)?,
        )?
        .ok_or(NegotiationError)
    };
    let context = parse().map_err(|_| NegotiatedSendError::Negotiation)?;
    if context.requires != RuntimeBinding::V2 || context.accepts_downgrade {
        return Err(NegotiatedSendError::Negotiation);
    }
    Ok(context)
}

/// Data-free transport failures; a publication failure does not authorize domain replay.
#[derive(Debug)]
pub enum NegotiatedSendError {
    /// Canonical boundary rejection, retaining selective reflection.
    Admission(crate::Rejection),
    /// External context, acceptance or discovery was not confirmed.
    Negotiation,
    /// Publication was not confirmed. No adapter source data escapes.
    Publication,
}
impl fmt::Display for NegotiatedSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("negotiated delivery failed")
    }
}
impl Error for NegotiatedSendError {}
