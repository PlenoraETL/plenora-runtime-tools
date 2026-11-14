//! Product delivery gate. Context is transport-owned and never reaches domain metadata.
use crate::{AdmittedRequest, RuntimeBoundary};
use plenora_runtime_capabilities::{
    AcceptanceRecord, AcceptanceStore, CapabilityDiscovery, NegotiationContext, NegotiationError,
    RuntimeBinding, accept_runtime_request, select_runtime_destination,
};
use plenora_runtime_messaging::{MessageId, MessageProducer, PublishOutcome, SerializedMessage};
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

    /// Synchronous admission; async hosts must use `admit_async` to avoid blocking their executor.
    /// # Errors
    /// Rejects malformed input, missing/altered context, v1, downgrade consent and store failures.
    pub fn admit(
        &self,
        message: &SerializedMessage,
        route: DeliveryRoute,
    ) -> Result<NegotiatedDelivery, NegotiatedSendError> {
        let admitted = self.inspect(message)?;
        let context = context(message)?;
        let record = self
            .store
            .accept(
                &admitted.identity().message_id,
                matches!(route, DeliveryRoute::Ingress).then_some(context),
            )
            .map_err(|_| NegotiatedSendError::Storage)?;
        accept_runtime_request(
            &AcceptanceSnapshot(record),
            &admitted.identity().message_id,
            Some(context),
            matches!(route, DeliveryRoute::Ingress),
            RuntimeBinding::V2,
            true,
        )
        .map_err(|_| NegotiatedSendError::Negotiation)?;
        // Storage can block: recheck deadlines and host policy after its confirmed response.
        let admitted = self.inspect(message)?;
        Ok(NegotiatedDelivery {
            admitted,
            message: message.clone(),
        })
    }

    // Pure canonical admission shared by normal delivery and lookup-only replay inspection.
    pub(crate) fn inspect(
        &self,
        message: &SerializedMessage,
    ) -> Result<AdmittedRequest, NegotiatedSendError> {
        let mut input = message.clone();
        let _ = input.headers.remove(REQUIRES);
        let _ = input.headers.remove(DOWNGRADE);
        let admitted = self
            .boundary
            .admit_message(&input)
            .map_err(NegotiatedSendError::Admission)?;
        context(message)?;
        Ok(admitted)
    }

    /// Performs the same admission on a blocking worker, leaving the async reactor free.
    /// # Errors
    /// Storage failures and worker failures remain recoverable, distinct from protocol rejection.
    pub async fn admit_async(
        &self,
        message: &SerializedMessage,
        route: DeliveryRoute,
    ) -> Result<NegotiatedDelivery, NegotiatedSendError>
    where
        S: Send + Sync + 'static,
    {
        let runtime = Self::new(self.boundary.clone(), self.store.clone());
        let message = message.clone();
        tokio::task::spawn_blocking(move || runtime.admit(&message, route))
            .await
            .map_err(|_| NegotiatedSendError::Storage)?
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
    ) -> Result<PublishOutcome, NegotiatedSendError>
    where
        S: Send + Sync + 'static,
    {
        let current = self
            .admit_async(&delivery.message, DeliveryRoute::Forward)
            .await?;
        let context = context(&current.message)?;
        select_runtime_destination(context, destination)
            .map_err(|_| NegotiatedSendError::Negotiation)?;
        destination
            .ok_or(NegotiatedSendError::Negotiation)?
            .validate_request(current.admitted.request())
            .map_err(|_| NegotiatedSendError::Negotiation)?;
        producer
            .publish(delivery.message.clone())
            .await
            .map_err(|_| NegotiatedSendError::Publication)
    }
}

// A confirmed response is validated by the common contract, never reused across calls.
struct AcceptanceSnapshot(AcceptanceRecord);
impl AcceptanceStore for AcceptanceSnapshot {
    fn accept(
        &self,
        _: &MessageId,
        _: Option<NegotiationContext>,
    ) -> Result<AcceptanceRecord, NegotiationError> {
        Ok(self.0)
    }
}

pub(crate) async fn lookup_acceptance<S: AcceptanceStore + Send + Sync + 'static>(
    store: Arc<S>,
    id: MessageId,
) -> Result<AcceptanceRecord, NegotiatedSendError> {
    tokio::task::spawn_blocking(move || store.accept(&id, None))
        .await
        .map_err(|_| NegotiatedSendError::Storage)?
        .map_err(|_| NegotiatedSendError::Storage)
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
    /// External context, confirmed acceptance or discovery violates the contract.
    Negotiation,
    /// Storage or its blocking worker did not confirm authority; never a durable protocol refusal.
    Storage,
    /// Publication was not confirmed. No adapter source data escapes.
    Publication,
}
impl fmt::Display for NegotiatedSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("negotiated delivery failed")
    }
}
impl Error for NegotiatedSendError {}
