//! Canonical raw admission for the chain product, above the generic dispatcher.
//!
//! Order is protocol, unsupported, timeout, then pure domain validation. No handler
//! or resolver runs during admission. Durable acceptance and terminal settlement
//! remain separate gates in docs/qualification.md and docs/contract-alignment.md.
//! Raw candidates cannot be dispatched through the public runtime:
//! ```compile_fail
//! use plenora_runtime_boundary::CapabilityDispatcher;
//! use plenora_runtime_capabilities::CapabilityRequest;
//! use plenora_runtime_worker::{WorkerHandler, WorkerContext};
//! async fn bypass(d: CapabilityDispatcher, c: WorkerContext, r: CapabilityRequest) {
//!     d.handle(c, r).await;
//! }
//! ```
//! Nor can callers manufacture an admitted request:
//! ```compile_fail
//! use plenora_runtime_boundary::AdmittedRequest;
//! fn bypass(r: plenora_runtime_capabilities::CapabilityRequest) -> AdmittedRequest { r.into() }
//! ```
#![forbid(unsafe_code)]

mod codec;
mod dispatcher;
mod registry;
pub use dispatcher::{
    CapabilityDispatchError, CapabilityDispatchErrorCategory, CapabilityDispatcher,
    CapabilityDispatcherConfig, CapabilityDispatcherConfigError, CapabilityResponseRejection,
    MAX_CAPABILITY_PAYLOAD_BYTES,
};
pub use registry::{
    CapabilityRegistry, CapabilityRegistryBuilder, CapabilityRegistryConfig,
    CapabilityRegistryError, MAX_REGISTERED_CAPABILITIES,
};

mod error;
mod lexical;
mod negotiation;
mod raw;
mod settlement;
mod sink;
pub use negotiation::{DeliveryRoute, NegotiatedDelivery, NegotiatedRuntime, NegotiatedSendError};
pub use settlement::{
    ComponentOutcome, EvidenceComponent, OutcomeBudget, SettlementError, StoredMessage,
};

pub use error::{Rejection, ResultEncodingError};
use plenora_runtime_capabilities::{
    CAPABILITY_NAME_METADATA_KEY as NAME, CAPABILITY_OPERATION_METADATA_KEY as OPERATION,
    CAPABILITY_VERSION_METADATA_KEY as WIRE, CapabilityRequest, ContractId,
    EXECUTION_DEADLINE_METADATA_KEY as DEADLINE, IDEMPOTENCY_KEY_METADATA_KEY as KEY,
    INPUT_CONTRACT_METADATA_KEY as CONTRACT, OPERATION_VERSION_METADATA_KEY as VERSION,
    PlenoraErrorCategory as Category,
};
use plenora_runtime_core::Clock;
use plenora_runtime_messaging::{
    CAUSATION_ID_METADATA_KEY as CAUSE, CORRELATION_ID_METADATA_KEY as CORRELATION,
    MAX_METADATA_TOTAL_BYTES, MESSAGE_ID_METADATA_KEY as ID, SerializedMessage,
};
use plenora_runtime_worker::{DecodedWorkerMessage, WorkerContextIdentity, WorkerMessageDecoder};
use serde_json::{Map, Value};
pub use sink::BoundaryResultSink;
use std::{fmt, sync::Arc};

/// Pure input-contract validation supplied by the host for its frozen operations.
/// Implementations must not resolve references, invoke domain functions or perform I/O.
pub trait AdmissionPolicy: Send + Sync {
    /// Operation-specific byte bound. None means no key support for this selector.
    fn idempotency_key_limit(
        &self,
        capability: &str,
        operation: &str,
        version: &str,
    ) -> Option<usize>;
    /// Validate the component schema, including RT-023, after runtime admission.
    /// # Errors
    /// Returns a public category (e.g. `invalid_configuration` for two deadline channels).
    fn validate_payload(&self, request: &CapabilityRequest) -> Result<(), Category>;
    /// Exact semantic input for a supported host key. Defaults to the complete payload.
    /// Overrides may exclude only explicitly nonbinding caller-generated receipt identities.
    /// # Errors
    /// Rejects payloads that cannot be bound without ambiguity.
    fn idempotency_payload(&self, request: &CapabilityRequest) -> Result<Vec<u8>, Category> {
        Ok(request.input().bytes.to_vec())
    }
}

/// Request admitted by this boundary. There is deliberately no public constructor.
pub struct AdmittedRequest {
    request: CapabilityRequest,
    identity: WorkerContextIdentity,
    raw: SerializedMessage,
}
impl AdmittedRequest {
    /// Stable, canonical identity decoded at raw ingress.
    #[must_use]
    pub const fn identity(&self) -> WorkerContextIdentity {
        self.identity
    }
    /// Inspect the admitted routing and opaque bytes without invoking the handler.
    #[must_use]
    pub const fn request(&self) -> &CapabilityRequest {
        &self.request
    }
    /// Returns the exact metadata admitted from the transport for constructing worker context.
    #[must_use]
    pub const fn metadata(&self) -> &plenora_runtime_messaging::MessageMetadata {
        &self.raw.headers
    }
}
impl fmt::Debug for AdmittedRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AdmittedRequest { <redacted> }")
    }
}

/// Immutable admission facade sharing the actual dispatcher's frozen discovery.
#[derive(Clone)]
pub struct RuntimeBoundary {
    dispatcher: CapabilityDispatcher,
    clock: Arc<dyn Clock>,
    policy: Arc<dyn AdmissionPolicy>,
    sink: Arc<dyn BoundaryResultSink>,
    check_deadline: bool,
}
impl RuntimeBoundary {
    /// Creates a boundary; the same injected clock is installed in the dispatcher.
    #[must_use]
    pub fn new<C: Clock + 'static>(
        dispatcher: CapabilityDispatcher,
        clock: Arc<C>,
        policy: Arc<dyn AdmissionPolicy>,
        sink: Arc<dyn BoundaryResultSink>,
    ) -> Self {
        Self {
            dispatcher: dispatcher.with_clock(clock.clone()),
            clock,
            policy,
            sink,
            check_deadline: true,
        }
    }

    /// Admit raw JSON metadata before conversion into the portable metadata map.
    /// Payload bytes remain opaque. Headers must be empty to avoid a second metadata channel.
    /// # Errors
    /// Returns the first applicable rejection with selective reflection and fresh identity.
    pub fn admit_raw(
        &self,
        metadata: &[u8],
        input: SerializedMessage,
    ) -> Result<AdmittedRequest, Rejection> {
        if input.content_type.len() > plenora_runtime_messaging::MAX_INGRESS_CONTENT_TYPE_BYTES
            || metadata.len() > MAX_METADATA_TOTAL_BYTES * 6
        {
            return Err(Rejection::new(Category::Protocol, &Map::new()));
        }
        let raw::Metadata(raw) = serde_json::from_slice(metadata)
            .map_err(|_| Rejection::new(Category::Protocol, &Map::new()))?;
        if input.raw_metadata().is_some() || input.headers.iter().next().is_some() {
            return Err(Rejection::new(Category::Protocol, &raw));
        }
        self.materialize(&raw, input)
    }

    fn materialize(
        &self,
        raw: &Map<String, Value>,
        mut input: SerializedMessage,
    ) -> Result<AdmittedRequest, Rejection> {
        for (key, value) in raw {
            let text = value
                .as_str()
                .ok_or_else(|| Rejection::new(Category::Protocol, raw))?;
            input
                .headers
                .insert_text(key.clone(), text)
                .map_err(|_| Rejection::new(Category::Protocol, raw))?;
        }
        self.admit(raw, input)
    }

    /// Admit the transport-neutral message used by NATS/Apalis and other worker adapters.
    /// Transport decoders must reject duplicate headers before building this map.
    /// # Errors
    /// Returns exactly the same public rejection as the raw JSON metadata facade.
    pub fn admit_message(&self, input: &SerializedMessage) -> Result<AdmittedRequest, Rejection> {
        if input.content_type.len() > plenora_runtime_messaging::MAX_INGRESS_CONTENT_TYPE_BYTES
            || input.has_unreadable_metadata()
        {
            return Err(Rejection::new(Category::Protocol, &Map::new()));
        }
        let mut raw = Map::new();
        if let Some(fields) = input.raw_metadata() {
            let raw_size = fields.iter().fold(0usize, |size, (key, value)| {
                size.saturating_add(key.len()).saturating_add(value.len())
            });
            if raw_size > MAX_METADATA_TOTAL_BYTES * 6 {
                return Err(Rejection::new(Category::Protocol, &Map::new()));
            }
            if !input.headers.is_empty()
                || fields.len() > plenora_runtime_messaging::MAX_METADATA_ENTRIES
            {
                return Err(Rejection::new(Category::Protocol, &Map::new()));
            }
            for (key, value) in fields {
                if raw.contains_key(key) {
                    return Err(Rejection::new(Category::Protocol, &Map::new()));
                }
                raw.insert(
                    key.clone(),
                    std::str::from_utf8(value).map_or(Value::Null, |v| Value::String(v.into())),
                );
            }
            let mut materialized =
                SerializedMessage::new(input.content_type.clone(), input.bytes.clone());
            if input.exceeds_transport_payload_limit() {
                materialized = materialized.with_exceeded_transport_payload_limit();
            }
            return self.materialize(&raw, materialized);
        }
        for (key, value) in input.headers.iter() {
            raw.insert(
                key.into(),
                std::str::from_utf8(value).map_or(Value::Null, |v| Value::String(v.into())),
            );
        }
        self.admit(&raw, input.clone())
    }

    fn admit(
        &self,
        raw: &Map<String, Value>,
        input: SerializedMessage,
    ) -> Result<AdmittedRequest, Rejection> {
        if input.content_type.len() > plenora_runtime_messaging::MAX_INGRESS_CONTENT_TYPE_BYTES
            || input.has_unreadable_metadata()
        {
            return Err(Rejection::new(Category::Protocol, &Map::new()));
        }
        let reject = |category| Rejection::new(category, raw);
        if raw.values().any(|v| !v.is_string()) {
            return Err(reject(Category::Protocol));
        }
        for (key, predicate) in [
            (ID, lexical::uuid as fn(&str) -> bool),
            (CORRELATION, lexical::uuid),
            (NAME, lexical::capability),
            (WIRE, lexical::version),
            (OPERATION, lexical::operation),
            (VERSION, lexical::version),
        ] {
            if !raw.get(key).and_then(Value::as_str).is_some_and(predicate) {
                return Err(reject(Category::Protocol));
            }
        }
        if raw.contains_key(CAUSE)
            && !raw
                .get(CAUSE)
                .and_then(Value::as_str)
                .is_some_and(lexical::uuid)
        {
            return Err(reject(Category::Protocol));
        }
        let text = |key| {
            raw.get(key)
                .and_then(Value::as_str)
                .ok_or_else(|| reject(Category::Protocol))
        };
        ContractId::new(text(CONTRACT)?).map_err(|_| reject(Category::Protocol))?;
        let deadline = raw
            .get(DEADLINE)
            .map(|v| {
                v.as_str()
                    .and_then(lexical::deadline)
                    .ok_or_else(|| reject(Category::Protocol))
            })
            .transpose()?;
        let key_limit =
            self.policy
                .idempotency_key_limit(text(NAME)?, text(OPERATION)?, text(VERSION)?);
        if let Some(key) = raw.get(KEY) {
            let key = key
                .as_str()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| reject(Category::Protocol))?;
            if key_limit.is_some_and(|limit| key.len() > limit) {
                return Err(reject(Category::Protocol));
            }
        }
        // Conversion happens only after all lexical checks. A canonical integer outside
        // the generic u16 representation denotes an unavailable selector, not protocol.
        let request = codec::decode(&input).map_err(|_| reject(Category::Unsupported))?;
        let discovery = self
            .dispatcher
            .registry()
            .discovery(request.capability())
            .ok_or_else(|| reject(Category::Unsupported))?;
        discovery
            .validate_request(&request)
            .map_err(|_| reject(Category::Unsupported))?;
        if raw.contains_key(KEY) && key_limit.is_none() {
            return Err(reject(Category::Unsupported));
        }
        if self.check_deadline
            && deadline.is_some_and(|deadline| {
                deadline <= chrono::DateTime::<chrono::Utc>::from(self.clock.now())
            })
        {
            return Err(reject(Category::Timeout));
        }
        if input.exceeds_transport_payload_limit()
            || input.len() > self.dispatcher.config().max_payload_bytes
        {
            return Err(reject(Category::ResourceLimit));
        }
        self.policy.validate_payload(&request).map_err(reject)?;
        let identity = WorkerContextIdentity::new(
            text(ID)?.parse().map_err(|_| reject(Category::Protocol))?,
            text(CORRELATION)?
                .parse()
                .map_err(|_| reject(Category::Protocol))?,
            raw.get(CAUSE)
                .map(|_| text(CAUSE)?.parse().map_err(|_| reject(Category::Protocol)))
                .transpose()?,
        );
        Ok(AdmittedRequest {
            request,
            identity,
            raw: input,
        })
    }
}

impl RuntimeBoundary {
    // Private lookup-only admission. This value is never exposed as invocation authority.
    fn for_replay(&self) -> Self {
        let mut replay = self.clone();
        replay.check_deadline = false;
        replay
    }
}

struct SharedClock(Arc<dyn Clock>);
impl Clock for SharedClock {
    fn now(&self) -> std::time::SystemTime {
        self.0.now()
    }
}

#[async_trait::async_trait]
impl plenora_runtime_worker::WorkerHandler<AdmittedRequest> for RuntimeBoundary {
    type Error = Rejection;
    async fn handle(
        &self,
        context: plenora_runtime_worker::WorkerContext,
        admitted: AdmittedRequest,
    ) -> Result<(), Self::Error> {
        let raw = admitted
            .raw
            .headers
            .iter()
            .map(|(key, value)| {
                (
                    key.to_owned(),
                    std::str::from_utf8(value).map_or(Value::Null, |v| Value::String(v.into())),
                )
            })
            .collect();
        if context.message_id != admitted.identity.message_id
            || context.correlation_id != admitted.identity.correlation_id
            || context.causation_id != admitted.identity.causation_id
            || context.metadata != admitted.raw.headers
        {
            return Err(Rejection::new(Category::Protocol, &raw));
        }
        // A queued admitted value is not an exemption from deadline checks on delivery.
        // Context protocol ambiguity is checked first, preserving RT-018 precedence.
        let current = self.admit_message(&admitted.raw)?;
        let dispatcher = CapabilityDispatcher::with_result_sink(
            self.dispatcher.registry().clone(),
            self.dispatcher.config(),
            sink::IdentitySink {
                sink: self.sink.clone(),
                metadata: error::result_metadata(&raw),
            },
        )
        .map_err(|_| Rejection::new(Category::Internal, &raw))?
        .with_clock(Arc::new(SharedClock(self.clock.clone())));
        dispatcher
            .handle(context, current.request)
            .await
            .map_err(|e| Rejection::from_dispatch(&e, &raw))
    }
}
impl WorkerMessageDecoder<AdmittedRequest> for RuntimeBoundary {
    type Error = Rejection;
    fn decode(
        &self,
        message: &SerializedMessage,
    ) -> Result<DecodedWorkerMessage<AdmittedRequest>, Self::Error> {
        let admitted = self.admit_message(message)?;
        Ok(DecodedWorkerMessage::new(admitted.identity, admitted))
    }
}

#[cfg(test)]
mod test_registry_dispatch;
#[cfg(test)]
mod test_rest_discovery;

#[cfg(test)]
mod test_contracts;
#[cfg(test)]
mod test_runtime_binding_vectors;
#[cfg(test)]
mod test_codec {
    pub struct Codec;
    impl plenora_runtime_messaging::MessageCodec<plenora_runtime_capabilities::CapabilityRequest>
        for Codec
    {
        type Error = plenora_runtime_capabilities::CapabilityMessageCodecError;
        fn encode(
            &self,
            request: &plenora_runtime_capabilities::CapabilityRequest,
        ) -> Result<plenora_runtime_messaging::SerializedMessage, Self::Error> {
            plenora_runtime_capabilities::CapabilityMessageCodec.encode(request)
        }
        fn decode(
            &self,
            message: &plenora_runtime_messaging::SerializedMessage,
        ) -> Result<plenora_runtime_capabilities::CapabilityRequest, Self::Error> {
            crate::codec::decode(message)
        }
    }
}

#[cfg(test)]
mod test_fake_capability;

mod journal;
pub use journal::{
    DeliveryChange, DeliveryJournal, DeliveryOpen, DeliveryPlan, DeliveryStage,
    DurableExecutionStore, IdempotencyOpen, IdempotencyReceipt, JOURNAL_LIMIT, PreparedResult,
};
mod durable;
pub use durable::{
    DurableRuntime, DurableWorker, NegotiatedForwarder, RawDelivery, RouteTable, SettlementRetry,
};

mod primary_sink;
pub use primary_sink::PrimaryResultSink;
