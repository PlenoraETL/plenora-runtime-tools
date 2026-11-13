//! Durable execution around the real dispatcher; publication never authorizes domain replay.
use crate::{
    BoundaryResultSink, CapabilityDispatcher, CapabilityRegistryBuilder, NegotiatedDelivery,
    NegotiatedRuntime,
};
use async_trait::async_trait;
use plenora_runtime_capabilities::{
    AcceptanceStore, CapabilityFailure, CapabilityHandler, CapabilityRemoteEffect,
    CapabilityRequest, CapabilityResponse, CapabilityResultPublishError, ContractId,
    IDEMPOTENCY_KEY_METADATA_KEY, NegotiationContext, PLENORA_ERROR_CONTENT_TYPE, PlenoraError,
    PlenoraErrorCategory, PlenoraErrorPhase, PlenoraErrorRemoteEffect, PlenoraErrorRetry,
    RuntimeBinding, select_runtime_destination,
};
use plenora_runtime_messaging::{
    MESSAGE_ID_METADATA_KEY, MessageId, PublishOutcome, RetryErrorClass, SerializedMessage,
};
use plenora_runtime_worker::WorkerContext;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    sync::{Arc, Mutex},
};

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
    pub result: Result<CapabilityResponse, PlenoraError>,
    /// Last phase proved by the component, including cleanup when applicable.
    pub phase: PlenoraErrorPhase,
    /// Remote effect proved by the component, never inferred from output size.
    pub effect: PlenoraErrorRemoteEffect,
}
/// Component adapter supplying explicit evidence before dispatcher result validation.
#[async_trait]
pub trait EvidenceComponent: Send + Sync {
    /// Executes domain work once under a durable start authority.
    async fn invoke(&self, context: WorkerContext, request: CapabilityRequest) -> ComponentOutcome;
}
/// Store decision; existing work never receives another invocation authority.
pub enum ExecutionDecision<P> {
    /// A confirmed start grants this single authority.
    Start(P),
    /// The terminal result is already in the outbox; only publication may be retried.
    Replay,
    /// An incomplete invocation requires reconciliation, never an automatic domain retry.
    Recovery,
}
/// Durable product ledger. Production implementation is PostgreSQL; doubles belong in tests.
#[async_trait]
pub trait DurableExecutionStore: AcceptanceStore + Send + Sync + 'static {
    /// Nonforgeable fence supplied by the durable backend.
    type Permit: Send + Sync + 'static;
    /// Confirms the invocation intention before any component call.
    /// # Errors
    /// Fails closed on conflict, expiry, unavailable or unconfirmed storage.
    async fn begin(
        &self,
        id: MessageId,
        scope: &str,
        step: &str,
        input: &[u8],
    ) -> Result<ExecutionDecision<Self::Permit>, SettlementError>;
    /// Persists complete evidence before returning control to the dispatcher.
    /// # Errors
    /// An unconfirmed write prevents settlement.
    async fn evidence(&self, permit: &Self::Permit, report: &[u8]) -> Result<(), SettlementError>;
    /// Atomically commits report, result identity, outbox and processed inbox.
    /// # Errors
    /// An unconfirmed commit prevents ACK and does not permit domain replay.
    async fn finalize(
        &self,
        permit: &Self::Permit,
        result_id: MessageId,
        report: &[u8],
        envelope: &[u8],
    ) -> Result<(), SettlementError>;
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
struct Recorded {
    report: Vec<u8>,
    phase: PlenoraErrorPhase,
    effect: PlenoraErrorRemoteEffect,
}
struct Wrapper<S: DurableExecutionStore> {
    store: Arc<S>,
    permit: Arc<S::Permit>,
    component: Arc<dyn EvidenceComponent>,
    recorded: Arc<Mutex<Option<Recorded>>>,
}
#[async_trait]
impl<S: DurableExecutionStore> CapabilityHandler for Wrapper<S> {
    async fn invoke(
        &self,
        context: WorkerContext,
        request: CapabilityRequest,
    ) -> Result<CapabilityResponse, CapabilityFailure> {
        let outcome = self.component.invoke(context, request).await;
        let inconsistent_success = outcome.result.is_ok()
            && !matches!(
                outcome.effect,
                PlenoraErrorRemoteEffect::None | PlenoraErrorRemoteEffect::Committed
            );
        // Public errors cannot disagree with the component's explicit evidence axes.
        if outcome
            .result
            .as_ref()
            .err()
            .is_some_and(|e| e.phase() != outcome.phase || e.remote_effect() != outcome.effect)
        {
            return Err(failure());
        }
        let payload = match &outcome.result {
            Ok(response) => {
                serde_json::json!({"output": response.output().map(StoredMessage::capture).transpose().map_err(|_| failure())?, "contract": response.output_contract().map(ContractId::as_str)})
            }
            Err(error) => serde_json::to_value(error).map_err(|_| failure())?,
        };
        let report = serde_json::to_vec(&serde_json::json!({"phase":outcome.phase,"remote_effect":outcome.effect,"outcome":payload})).map_err(|_| failure())?;
        self.store
            .evidence(&self.permit, &report)
            .await
            .map_err(|_| failure())?;
        *self.recorded.lock().map_err(|_| failure())? = Some(Recorded {
            report,
            phase: outcome.phase,
            effect: outcome.effect,
        });
        if inconsistent_success {
            let error = PlenoraError::new(
                PlenoraErrorCategory::Internal,
                outcome.phase,
                outcome.effect,
                PlenoraErrorRetry::RequiresRecovery,
                "Component success contradicts effect evidence.",
            )
            .map_err(|_| failure())?;
            return Err(CapabilityFailure::with_public_error(error, SettlementError));
        }
        outcome
            .result
            .map_err(|e| CapabilityFailure::with_public_error(e, SettlementError))
    }
}
#[derive(Default)]
struct Capture(Mutex<Option<SerializedMessage>>);
#[async_trait]
impl BoundaryResultSink for Capture {
    async fn publish(
        &self,
        message: SerializedMessage,
    ) -> Result<PublishOutcome, CapabilityResultPublishError> {
        let mut slot = self
            .0
            .lock()
            .map_err(|_| CapabilityResultPublishError::with_source(SettlementError))?;
        if slot.is_some() {
            return Err(CapabilityResultPublishError::with_source(SettlementError));
        }
        *slot = Some(message);
        Ok(PublishOutcome::Confirmed)
    }
}
impl<S: DurableExecutionStore> NegotiatedRuntime<S> {
    /// Executes once through an evidenced registration, then persists its unique terminal result.
    /// `scope` and `step` are host-authorized identities; sink quota excludes the reserved error lane.
    /// ACK is allowed only after this method succeeds. External delivery remains pending in outbox.
    /// # Errors
    /// Fails closed on admission, uncertain storage or a potentially running/cancelled invocation.
    #[allow(
        clippy::too_many_lines,
        reason = "Ordered admission, authority, dispatcher and terminal settlement form one auditable transaction protocol"
    )]
    pub async fn execute(
        &self,
        delivery: &NegotiatedDelivery,
        context: WorkerContext,
        scope: &str,
        step: &str,
        sink_bytes: usize,
    ) -> Result<(), SettlementError> {
        if sink_bytes == 0 || sink_bytes > crate::MAX_CAPABILITY_PAYLOAD_BYTES {
            return Err(SettlementError);
        }
        let current = self
            .admit(&delivery.message, crate::DeliveryRoute::Forward)
            .map_err(|_| SettlementError)?;
        // Scoped key execution needs an authorized tenant/operation scope, not a run scope.
        // The unqualified path refuses it; tracked in docs/contract-alignment.md.
        if current
            .admitted
            .metadata()
            .contains_key(IDEMPOTENCY_KEY_METADATA_KEY)
        {
            return Err(SettlementError);
        }
        if context.message_id != current.admitted.identity().message_id
            || context.correlation_id != current.admitted.identity().correlation_id
            || context.causation_id != current.admitted.identity().causation_id
            || &context.metadata != current.admitted.metadata()
        {
            return Err(SettlementError);
        }
        let registration = self
            .boundary
            .dispatcher
            .registry()
            .registration(current.admitted.request().capability())
            .ok_or(SettlementError)?;
        let component = registration.evidence.clone().ok_or(SettlementError)?;
        let discovery = registration.discovery().cloned().ok_or(SettlementError)?;
        select_runtime_destination(
            NegotiationContext {
                requires: RuntimeBinding::V2,
                accepts_downgrade: false,
            },
            Some(&discovery),
        )
        .map_err(|_| SettlementError)?;
        let canonical = StoredMessage::capture(&delivery.message)?.encode()?;
        let permit = match self
            .store
            .begin(context.message_id, scope, step, &canonical)
            .await?
        {
            ExecutionDecision::Start(p) => Arc::new(p),
            ExecutionDecision::Replay => return Ok(()),
            ExecutionDecision::Recovery => return Err(SettlementError),
        };
        let recorded = Arc::new(Mutex::new(None));
        let mut registry = CapabilityRegistryBuilder::default();
        registry
            .register_discovered(
                discovery,
                Wrapper {
                    store: self.store.clone(),
                    permit: permit.clone(),
                    component,
                    recorded: recorded.clone(),
                },
            )
            .map_err(|_| SettlementError)?;
        let capture = Arc::new(Capture::default());
        let raw = current
            .admitted
            .raw
            .headers
            .iter()
            .map(|(k, v)| {
                Ok((
                    k.to_owned(),
                    serde_json::Value::String(
                        std::str::from_utf8(v)
                            .map_err(|_| SettlementError)?
                            .to_owned(),
                    ),
                ))
            })
            .collect::<Result<serde_json::Map<_, _>, SettlementError>>()?;
        let identity = crate::error::result_metadata(&raw);
        let dispatcher = CapabilityDispatcher::with_result_sink(
            registry.build(),
            self.boundary.dispatcher.config(),
            crate::sink::IdentitySink {
                sink: capture.clone(),
                metadata: identity.clone(),
            },
        )
        .map_err(|_| SettlementError)?
        .with_clock(Arc::new(crate::SharedClock(self.boundary.clock.clone())));
        let result = dispatcher.handle(context, current.admitted.request).await;
        let record = recorded.lock().map_err(|_| SettlementError)?.take();
        let Some(record) = record else {
            if let Err(
                error @ crate::CapabilityDispatchError::DeadlineExceeded {
                    invocation_started: false,
                    ..
                },
            ) = &result
            {
                let rejection = crate::Rejection::from_dispatch(error, &raw);
                let mut message = rejection.to_message().map_err(|_| SettlementError)?;
                for (key, value) in [
                    ("plenora.negotiation.requires", "plenora-runtime-binding-v2"),
                    ("plenora.negotiation.accepts_downgrade", "false"),
                ] {
                    message
                        .headers
                        .insert_text(key, value)
                        .map_err(|_| SettlementError)?;
                }
                let envelope = StoredMessage::capture(&message)?.encode()?;
                let id = message
                    .headers
                    .get_text(MESSAGE_ID_METADATA_KEY)
                    .map_err(|_| SettlementError)?
                    .ok_or(SettlementError)?
                    .parse()
                    .map_err(|_| SettlementError)?;
                self.store.evidence(&permit, &envelope).await?;
                return self.store.finalize(&permit, id, &envelope, &envelope).await;
            }
            return Err(SettlementError);
        };
        let captured = capture.0.lock().map_err(|_| SettlementError)?.take();
        let mut message = match (result, captured) {
            (Ok(()), Some(message)) if message.len() <= sink_bytes => message,
            (result, _) => {
                let public_error = result.as_ref().err().and_then(|e| e.public_error().ok());
                let category = public_error
                    .as_ref()
                    .map_or(PlenoraErrorCategory::ResourceLimit, PlenoraError::category);
                let retry = if category == PlenoraErrorCategory::ResourceLimit
                    && record.effect == PlenoraErrorRemoteEffect::None
                {
                    PlenoraErrorRetry::Never
                } else {
                    public_error
                        .as_ref()
                        .map_or(PlenoraErrorRetry::RequiresRecovery, PlenoraError::retry)
                };
                let public = PlenoraError::new(
                    category,
                    record.phase,
                    record.effect,
                    retry,
                    "Runtime execution terminated.",
                )
                .map_err(|_| SettlementError)?;
                let mut message = SerializedMessage::new(
                    PLENORA_ERROR_CONTENT_TYPE,
                    serde_json::to_vec(&public).map_err(|_| SettlementError)?,
                );
                for (k, v) in &identity {
                    message
                        .headers
                        .insert_text(k.clone(), v.as_str().ok_or(SettlementError)?)
                        .map_err(|_| SettlementError)?;
                }
                message
            }
        };
        // Results inherit context explicitly; their immutable outbox binds original acceptance.
        for (k, v) in [
            ("plenora.negotiation.requires", "plenora-runtime-binding-v2"),
            ("plenora.negotiation.accepts_downgrade", "false"),
        ] {
            message
                .headers
                .insert_text(k, v)
                .map_err(|_| SettlementError)?;
        }
        let id = message
            .headers
            .get_text(MESSAGE_ID_METADATA_KEY)
            .map_err(|_| SettlementError)?
            .ok_or(SettlementError)?
            .parse()
            .map_err(|_| SettlementError)?;
        self.store
            .finalize(
                &permit,
                id,
                &record.report,
                &StoredMessage::capture(&message)?.encode()?,
            )
            .await
    }
}
