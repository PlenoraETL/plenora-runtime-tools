//! Product ingress, evidenced invocation, finalization and immutable-route publication.
use crate::{
    BoundaryResultSink, CapabilityDispatcher, CapabilityRegistryBuilder, ComponentOutcome,
    DeliveryChange, DeliveryJournal, DeliveryOpen, DeliveryPlan, DeliveryRoute, DeliveryStage,
    DurableExecutionStore, EvidenceComponent, NegotiatedRuntime, OutcomeBudget, PreparedResult,
    Rejection, RuntimeBoundary, SettlementError, StoredMessage,
};
use async_trait::async_trait;
use plenora_runtime_capabilities::{
    AcceptanceRecord, CapabilityDiscovery, CapabilityFailure, CapabilityHandler, CapabilityRequest,
    CapabilityResponse, CapabilityResultPublishError, ContractId, IDEMPOTENCY_KEY_METADATA_KEY,
    NegotiationContext, PLENORA_ERROR_CONTENT_TYPE, PlenoraError, PlenoraErrorCategory as Category,
    PlenoraErrorPhase as Phase, PlenoraErrorRemoteEffect as Effect, PlenoraErrorRetry as Retry,
    RuntimeBinding, select_runtime_destination,
};
use plenora_runtime_messaging::{
    MESSAGE_ID_METADATA_KEY as ID, MessageId, PublishOutcome, RetryDecision, RetryPolicy,
    SerializedMessage,
};
use plenora_runtime_worker::{
    DecodedWorkerMessage, WorkerContext, WorkerContextIdentity, WorkerHandler, WorkerMessageDecoder,
};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
const REQUIRES: &str = "plenora.negotiation.requires";
const DOWNGRADE: &str = "plenora.negotiation.accepts_downgrade";
const V2: NegotiationContext = NegotiationContext {
    requires: RuntimeBinding::V2,
    accepts_downgrade: false,
};
const DLQ_KEYS: [&str; 4] = [
    "plenora.dead_letter.id",
    "plenora.dead_letter.reason",
    "plenora.dead_letter.attempts",
    "plenora.dead_letter.failed_at",
];

/// Frozen binding from durable route identities to producers and effective discovery.
/// The trusted host binds each identity to its actual endpoint/subject; input cannot select one.
pub struct RouteTable(BTreeMap<String, (CapabilityDiscovery, Arc<dyn BoundaryResultSink>)>);
impl RouteTable {
    /// Validates all destinations and freezes their producer bindings.
    /// # Errors
    /// Rejects absent/duplicate routes or any non-v2 destination.
    pub fn new(
        routes: Vec<(String, CapabilityDiscovery, Arc<dyn BoundaryResultSink>)>,
    ) -> Result<Self, SettlementError> {
        if routes.is_empty() || routes.len() > 256 {
            return Err(SettlementError);
        }
        let mut table = BTreeMap::new();
        for (route, discovery, producer) in routes {
            DeliveryPlan::new(&route, 1, 0, 1, 1)?;
            if select_runtime_destination(V2, Some(&discovery)).map_err(|_| SettlementError)?
                != RuntimeBinding::V2
                || table.insert(route, (discovery, producer)).is_some()
            {
                return Err(SettlementError);
            }
        }
        Ok(Self(table))
    }
    async fn publish(
        &self,
        route: &str,
        message: SerializedMessage,
    ) -> Result<PublishOutcome, SettlementError> {
        let (discovery, producer) = self.0.get(route).ok_or(SettlementError)?;
        if external_context(&message)? != V2 {
            return Err(SettlementError);
        }
        select_runtime_destination(V2, Some(discovery)).map_err(|_| SettlementError)?;
        producer.publish(message).await.map_err(|_| SettlementError)
    }
}
/// Complete product runtime. Construction requires a frozen registry, durable storage and routes.
pub struct DurableRuntime<S> {
    negotiated: NegotiatedRuntime<S>,
    plan: DeliveryPlan,
    routes: RouteTable,
    scope: String,
}
impl<S: DurableExecutionStore> DurableRuntime<S> {
    /// Creates the product profile. Scope and routes are authorized host configuration.
    /// # Errors
    /// Refuses invalid scope or an unbound durable result route.
    pub fn new(
        boundary: RuntimeBoundary,
        store: Arc<S>,
        scope: &str,
        plan: DeliveryPlan,
        routes: RouteTable,
    ) -> Result<Self, SettlementError> {
        if scope.is_empty()
            || scope.len() > 128
            || !scope.bytes().all(|b| b.is_ascii_graphic())
            || !routes.0.contains_key(plan.route())
        {
            return Err(SettlementError);
        }
        Ok(Self {
            negotiated: NegotiatedRuntime::new(boundary, store),
            plan,
            routes,
            scope: scope.into(),
        })
    }
    /// Stable journal key: canonical request identity, otherwise a digest of the exact rejected carrier.
    /// No generated worker identity is reflected into a protocol response.
    #[must_use]
    pub fn key(&self, message: &SerializedMessage) -> String {
        if let Some(id) = message
            .headers
            .get_text(ID)
            .ok()
            .flatten()
            .filter(|v| crate::lexical::uuid(v))
        {
            format!("{}:{}:m:{id}", self.scope.len(), self.scope)
        } else {
            format!(
                "{}:{}:r:{:x}",
                self.scope.len(),
                self.scope,
                Sha256::digest(fingerprint(message))
            )
        }
    }
    /// One entry point for ingress, retries, redrive and resend, including malformed messages.
    /// Returns only a confirmed durable terminal. Execution deadlines never suppress persisted results.
    /// # Errors
    /// Storage uncertainty, an active/unknown invocation, retry delay or expired delivery prevents ACK.
    #[allow(
        clippy::too_many_lines,
        reason = "Single ordered receipt state machine keeps invocation authority and durable transitions auditable together"
    )]
    pub async fn process(
        &self,
        message: &SerializedMessage,
        route: DeliveryRoute,
        context: WorkerContext,
    ) -> Result<SerializedMessage, SettlementError> {
        let mut key = self.key(message);
        let binding = fingerprint(message);
        let (mut journal, mut now, conflict) = match self
            .negotiated
            .store
            .open(&key, binding, &self.plan)
            .await?
        {
            DeliveryOpen::Opened(j, n) => (j, n, false),
            DeliveryOpen::Conflict => {
                let mut hash = Sha256::new();
                hash.update(binding);
                hash.update(serde_json::to_vec(&self.plan).map_err(|_| SettlementError)?);
                key = format!(
                    "{}:{}:c:{:x}",
                    self.scope.len(),
                    self.scope,
                    hash.finalize()
                );
                match self
                    .negotiated
                    .store
                    .open(&key, binding, &self.plan)
                    .await?
                {
                    DeliveryOpen::Opened(j, n) => (j, n, true),
                    DeliveryOpen::Conflict => return Err(SettlementError),
                }
            }
        };
        loop {
            if journal.expired(now) {
                return Err(SettlementError);
            }
            match journal.stage() {
                DeliveryStage::Finished => {
                    return journal.evidence().ok_or(SettlementError)?.message();
                }
                DeliveryStage::Evidence => {
                    (journal, now) = self
                        .negotiated
                        .store
                        .change(&key, journal.revision(), DeliveryChange::Settle)
                        .await?;
                }
                DeliveryStage::Running => return Err(SettlementError),
                DeliveryStage::Ready => {
                    if conflict {
                        let category = if external_context(message).is_err() {
                            Category::Unsupported
                        } else {
                            Category::Conflict
                        };
                        let p = prepared_rejection(
                            Rejection::new(category, &reflection(message)),
                            &journal,
                        )?;
                        (journal, now) = self
                            .negotiated
                            .store
                            .change(&key, journal.revision(), DeliveryChange::Reject(p))
                            .await?;
                        continue;
                    }
                    if journal.retry_exhausted(now) {
                        (journal, now) = self
                            .negotiated
                            .store
                            .change(&key, journal.revision(), DeliveryChange::Settle)
                            .await?;
                        continue;
                    }
                    if now < journal.due() {
                        return Err(SettlementError);
                    }
                    let mut input = message.clone();
                    for k in DLQ_KEYS {
                        let _ = input.headers.remove(k);
                    }
                    let admitted = match self.admit_or_replay(&input, route, &journal).await {
                        Ok(Admission::Invoke(d)) => *d,
                        Ok(Admission::Replay(p, origin)) => {
                            (journal, now) = self
                                .negotiated
                                .store
                                .change(&key, journal.revision(), DeliveryChange::Reuse(*p, origin))
                                .await?;
                            continue;
                        }
                        Err(error) => {
                            let rejection = match error {
                                crate::NegotiatedSendError::Storage => return Err(SettlementError),
                                crate::NegotiatedSendError::Admission(r) => r,
                                _ => Rejection::new(Category::Unsupported, &reflection(message)),
                            };
                            let p = prepared_rejection(rejection, &journal)?;
                            (journal, now) = self
                                .negotiated
                                .store
                                .change(&key, journal.revision(), DeliveryChange::Reject(p))
                                .await?;
                            continue;
                        }
                    };
                    let registration = self
                        .negotiated
                        .boundary
                        .dispatcher
                        .registry()
                        .registration(admitted.admitted().request().capability());
                    let component = registration.and_then(|r| r.evidence.clone());
                    let discovery = registration.and_then(|r| r.discovery().cloned());
                    if component.is_none()
                        || select_runtime_destination(V2, discovery.as_ref()).is_err()
                    {
                        let category = Category::Unsupported;
                        let p = prepared_rejection(
                            Rejection::new(category, &reflection(message)),
                            &journal,
                        )?;
                        (journal, now) = self
                            .negotiated
                            .store
                            .change(&key, journal.revision(), DeliveryChange::Reject(p))
                            .await?;
                        continue;
                    }
                    (journal, _) = self
                        .negotiated
                        .store
                        .change(&key, journal.revision(), DeliveryChange::Start)
                        .await?;
                    let mut invocation = context.clone();
                    let identity = admitted.admitted().identity();
                    invocation.message_id = identity.message_id;
                    invocation.correlation_id = identity.correlation_id;
                    invocation.causation_id = identity.causation_id;
                    invocation.metadata = admitted.admitted().metadata().clone();
                    invocation.attempt = u32::from(journal.attempt());
                    let mut registry = CapabilityRegistryBuilder::default();
                    registry
                        .register_discovered(
                            discovery.clone().ok_or(SettlementError)?,
                            Wrapper {
                                component: component.ok_or(SettlementError)?,
                                store: self.negotiated.store.clone(),
                                key: key.clone(),
                                journal: journal.clone(),
                                dispatcher: self.negotiated.boundary.dispatcher.clone(),
                                reflection: reflection(message),
                            },
                        )
                        .map_err(|_| SettlementError)?;
                    let dispatcher = CapabilityDispatcher::with_result_sink(
                        registry.build(),
                        self.negotiated.boundary.dispatcher.config(),
                        crate::sink::IdentitySink {
                            sink: Arc::new(Capture::default()),
                            metadata: fixed_metadata(&reflection(message), journal.result_id()),
                        },
                    )
                    .map_err(|_| SettlementError)?
                    .with_clock(Arc::new(crate::SharedClock(
                        self.negotiated.boundary.clock.clone(),
                    )));
                    let result = dispatcher
                        .handle(invocation, admitted.admitted.request)
                        .await;
                    // Evidence is the authority, not the in-memory dispatcher return or sink capture.
                    (journal, now) = self.negotiated.store.load(&key).await?;
                    if journal.stage() == DeliveryStage::Running {
                        if let Err(
                            error @ crate::CapabilityDispatchError::DeadlineExceeded {
                                invocation_started: false,
                                ..
                            },
                        ) = result
                        {
                            let p = prepared_rejection(
                                Rejection::from_dispatch(&error, &reflection(message)),
                                &journal,
                            )?;
                            (journal, now) = self
                                .negotiated
                                .store
                                .change(&key, journal.revision(), DeliveryChange::Evidence(p))
                                .await?;
                        } else {
                            return Err(SettlementError);
                        }
                    }
                }
            }
        }
    }
    /// Recover finalization and publish only to the result's persisted route. No domain is invoked.
    /// # Errors
    /// Unknown publication or commit leaves the immutable terminal pending for safe republishing.
    pub async fn relay(&self, key: &str) -> Result<bool, SettlementError> {
        if !key.starts_with(&format!("{}:{}:", self.scope.len(), self.scope)) {
            return Err(SettlementError);
        }
        let (mut journal, mut now) = self.negotiated.store.load(key).await?;
        if journal.expired(now) {
            return Err(SettlementError);
        }
        if journal.stage() == DeliveryStage::Evidence {
            (journal, now) = self
                .negotiated
                .store
                .change(key, journal.revision(), DeliveryChange::Settle)
                .await?;
        }
        if journal.stage() != DeliveryStage::Finished {
            return Err(SettlementError);
        }
        if journal.delivered() {
            return Ok(false);
        }
        if journal.expired(now) {
            return Err(SettlementError);
        }
        let (claimed, _) = self
            .negotiated
            .store
            .change(key, journal.revision(), DeliveryChange::Claim)
            .await?;
        let id = claimed.result_id().parse().map_err(|_| SettlementError)?;
        if crate::negotiation::lookup_acceptance(self.negotiated.store.clone(), id)
            .await
            .map_err(|_| SettlementError)?
            != AcceptanceRecord::Existing(V2)
        {
            return Err(SettlementError);
        }
        let message = claimed.evidence().ok_or(SettlementError)?.message()?;
        let outcome = self.routes.publish(claimed.plan().route(), message).await;
        if !matches!(outcome, Ok(PublishOutcome::Confirmed)) {
            self.negotiated
                .store
                .change(key, claimed.revision(), DeliveryChange::Release)
                .await?;
            return Err(SettlementError);
        }
        self.negotiated
            .store
            .change(key, claimed.revision(), DeliveryChange::Confirm)
            .await?;
        Ok(true)
    }
    /// Execute and confirm the actual terminal receipt, including distinct conflict receipts.
    /// The immutable result route may represent durable local pull delivery rather than a broker.
    /// # Errors
    /// Unconfirmed processing, authoritative readback or route publication prevents success.
    pub async fn process_confirmed(
        &self,
        message: &SerializedMessage,
        route: DeliveryRoute,
        context: WorkerContext,
    ) -> Result<PreparedResult, SettlementError> {
        let terminal = self.process(message, route, context).await?;
        let id = terminal
            .headers
            .get_text(ID)
            .map_err(|_| SettlementError)?
            .ok_or(SettlementError)?;
        let prefix = format!("{}:{}:", self.scope.len(), self.scope);
        let (key, journal) = self.negotiated.store.result_receipt(&prefix, id).await?;
        let prepared = journal.evidence().cloned().ok_or(SettlementError)?;
        if prepared.message()? != terminal {
            return Err(SettlementError);
        }
        self.relay(&key).await?;
        Ok(prepared)
    }

    /// Inspect the original exact receipt before a separately admitted reconciliation read.
    /// # Errors
    /// Missing, changed or expired receipts are never evidence that invocation did not happen.
    pub async fn receipt_state(
        &self,
        message: &SerializedMessage,
    ) -> Result<(DeliveryStage, u16), SettlementError> {
        let (j, now) = self.negotiated.store.load(&self.key(message)).await?;
        j.verify(fingerprint(message), &self.plan)?;
        if j.expired(now) {
            return Err(SettlementError);
        }
        Ok((j.stage(), j.attempt()))
    }

    /// Read complete confirmed evidence for the exact original carrier, without invoking anything.
    /// # Errors
    /// Missing, running, conflicting or expired receipts never authorize a fresh invocation.
    pub async fn evidence(
        &self,
        message: &SerializedMessage,
    ) -> Result<PreparedResult, SettlementError> {
        let (j, now) = self.negotiated.store.load(&self.key(message)).await?;
        j.verify(fingerprint(message), &self.plan)?;
        if j.expired(now) || !matches!(j.stage(), DeliveryStage::Evidence | DeliveryStage::Finished)
        {
            return Err(SettlementError);
        }
        j.evidence().cloned().ok_or(SettlementError)
    }

    #[allow(clippy::too_many_lines)] // Ordered admission and alias lookup share one fail-closed path.
    async fn admit_or_replay(
        &self,
        message: &SerializedMessage,
        route: DeliveryRoute,
        receipt: &DeliveryJournal,
    ) -> Result<Admission, crate::NegotiatedSendError> {
        use crate::{IdempotencyOpen, NegotiatedSendError as E};
        if !message.headers.contains_key(IDEMPOTENCY_KEY_METADATA_KEY)
            && !message.raw_metadata().is_some_and(|fields| {
                fields
                    .iter()
                    .any(|(k, _)| k == IDEMPOTENCY_KEY_METADATA_KEY)
            })
        {
            return self
                .negotiated
                .admit_async(message, route)
                .await
                .map(|d| Admission::Invoke(Box::new(d)));
        }
        // Identical raw/discovery/policy/negotiation checks, but lookup may recover a terminal
        // past its original execution deadline. This admission is never sent to a dispatcher.
        let lookup = NegotiatedRuntime::new(
            self.negotiated.boundary.for_replay(),
            self.negotiated.store.clone(),
        );
        let checked = match lookup.inspect(message) {
            Ok(checked) => checked,
            Err(E::Storage) => return Err(E::Storage),
            Err(_) => {
                return self
                    .negotiated
                    .admit_async(message, route)
                    .await
                    .map(|d| Admission::Invoke(Box::new(d)));
            }
        };
        let request = checked.request();
        let key = checked
            .metadata()
            .get_text(IDEMPOTENCY_KEY_METADATA_KEY)
            .map_err(|_| E::Storage)?
            .ok_or(E::Storage)?;
        let selector = serde_json::to_vec(&(
            &self.scope,
            request.capability().name(),
            request.operation().as_str(),
            request.operation_version().get(),
            key,
        ))
        .map_err(|_| E::Storage)?;
        let alias = format!("{:x}", Sha256::digest(selector));
        let mut semantic = checked.raw.clone();
        semantic.bytes = self
            .negotiated
            .boundary
            .policy
            .idempotency_payload(request)
            .map_err(|category| E::Admission(Rejection::new(category, &reflection(message))))?
            .into();
        for field in [
            ID,
            plenora_runtime_messaging::CORRELATION_ID_METADATA_KEY,
            plenora_runtime_messaging::CAUSATION_ID_METADATA_KEY,
        ] {
            let _ = semantic.headers.remove(field);
        }
        let binding = fingerprint(&semantic);
        let mut selected = self
            .negotiated
            .store
            .idempotency(&alias, binding, None)
            .await
            .map_err(|_| E::Storage)?;
        let mut admitted = None;
        if matches!(selected, IdempotencyOpen::Absent) {
            admitted = Some(self.negotiated.admit_async(message, route).await?);
            selected = self
                .negotiated
                .store
                .idempotency(
                    &alias,
                    binding,
                    Some(crate::IdempotencyReceipt {
                        key: self.key(message),
                        binding: fingerprint(message),
                    }),
                )
                .await
                .map_err(|_| E::Storage)?;
        }
        let original = match selected {
            IdempotencyOpen::Selected(original) => original,
            IdempotencyOpen::Conflict => {
                return Err(E::Admission(Rejection::new(
                    Category::Conflict,
                    &reflection(message),
                )));
            }
            IdempotencyOpen::Absent => return Err(E::Storage),
        };
        if original.key == self.key(message) {
            return match admitted {
                Some(d) => Ok(Admission::Invoke(Box::new(d))),
                None => self
                    .negotiated
                    .admit_async(message, route)
                    .await
                    .map(|d| Admission::Invoke(Box::new(d))),
            };
        }
        // Only a confirmed alias permits deadline-exempt negotiation acceptance for this receipt.
        lookup.admit_async(message, route).await?;
        let original_key = original.key.clone();
        let (mut original_receipt, original_now) = self
            .negotiated
            .store
            .load(&original_key)
            .await
            .map_err(|_| E::Storage)?;
        original_receipt
            .verify(original.binding, &self.plan)
            .map_err(|_| E::Storage)?;
        if original_receipt.expired(original_now) {
            return Err(E::Storage);
        }
        if original_receipt.stage() == DeliveryStage::Evidence {
            (original_receipt, _) = self
                .negotiated
                .store
                .change(
                    &original_key,
                    original_receipt.revision(),
                    DeliveryChange::Settle,
                )
                .await
                .map_err(|_| E::Storage)?;
        }
        if original_receipt.stage() != DeliveryStage::Finished {
            return Err(E::Storage);
        }
        let mut prepared = original_receipt.evidence().cloned().ok_or(E::Storage)?;
        let mut terminal = prepared.message().map_err(|_| E::Storage)?;
        for (k, v) in fixed_metadata(&reflection(message), receipt.result_id()) {
            if matches!(
                k.as_str(),
                ID | plenora_runtime_messaging::CORRELATION_ID_METADATA_KEY
                    | plenora_runtime_messaging::CAUSATION_ID_METADATA_KEY
            ) {
                terminal
                    .headers
                    .insert_text(k, v.as_str().ok_or(E::Storage)?)
                    .map_err(|_| E::Storage)?;
            }
        }
        prepared.terminal = StoredMessage::capture(&terminal).map_err(|_| E::Storage)?;
        prepared.retry_ms = None;
        Ok(Admission::Replay(Box::new(prepared), original))
    }

    /// Bounded recovery of evidence/outbox rows; suitable for the host relay task after restart.
    /// # Errors
    /// Any unconfirmed item remains pending and the caller must report degraded readiness.
    pub async fn relay_pending(&self, limit: usize) -> Result<usize, SettlementError> {
        let keys = self
            .negotiated
            .store
            .pending(
                &format!("{}:{}:", self.scope.len(), self.scope),
                self.plan.route(),
                limit,
            )
            .await?;
        let mut sent = 0;
        for key in keys {
            if self.relay(&key).await? {
                sent += 1;
            }
        }
        Ok(sent)
    }
    /// Forward/retry/redrive a request using the same gate and a frozen effective destination.
    /// # Errors
    /// Refuses missing/changed context, stale acceptance or unsupported discovery before send.
    pub async fn forward(
        &self,
        message: &SerializedMessage,
        route: &str,
    ) -> Result<PublishOutcome, SettlementError> {
        let d = self
            .negotiated
            .admit_async(message, DeliveryRoute::Forward)
            .await
            .map_err(|_| SettlementError)?;
        let (discovery, sink) = self.routes.0.get(route).ok_or(SettlementError)?;
        self.negotiated
            .send(&d, Some(discovery), &Producer(sink.clone()))
            .await
            .map_err(|_| SettlementError)
    }
}
enum Admission {
    Invoke(Box<crate::NegotiatedDelivery>),
    Replay(Box<PreparedResult>, crate::IdempotencyReceipt),
}
struct Wrapper<S: DurableExecutionStore> {
    component: Arc<dyn EvidenceComponent>,
    store: Arc<S>,
    key: String,
    journal: DeliveryJournal,
    dispatcher: CapabilityDispatcher,
    reflection: Map<String, Value>,
}
#[async_trait]
impl<S: DurableExecutionStore> CapabilityHandler for Wrapper<S> {
    async fn invoke(
        &self,
        context: WorkerContext,
        request: CapabilityRequest,
    ) -> Result<CapabilityResponse, CapabilityFailure> {
        let outcome = self
            .component
            .invoke(
                context.clone(),
                request.clone(),
                OutcomeBudget(self.journal.plan().output_bytes()),
            )
            .await;
        let response = outcome.result.clone();
        let prepared = render(
            &self.dispatcher,
            &request,
            context.correlation_id,
            outcome,
            &self.reflection,
            &self.journal,
        )
        .await
        .map_err(|_| failure())?;
        self.store
            .change(
                &self.key,
                self.journal.revision(),
                DeliveryChange::Evidence(prepared),
            )
            .await
            .map_err(|_| failure())?;
        response.map_err(|e| CapabilityFailure::with_public_error(e, SettlementError))
    }
}
fn failure() -> CapabilityFailure {
    CapabilityFailure::new(
        plenora_runtime_messaging::RetryErrorClass::OutcomeUnknown,
        plenora_runtime_capabilities::CapabilityRemoteEffect::Unknown,
        SettlementError,
    )
}
#[derive(Default)]
struct Capture(Mutex<Option<SerializedMessage>>);
#[async_trait]
impl BoundaryResultSink for Capture {
    async fn publish(
        &self,
        m: SerializedMessage,
    ) -> Result<PublishOutcome, CapabilityResultPublishError> {
        let mut s = self
            .0
            .lock()
            .map_err(|_| CapabilityResultPublishError::with_source(SettlementError))?;
        if s.is_some() {
            return Err(CapabilityResultPublishError::with_source(SettlementError));
        }
        *s = Some(m);
        Ok(PublishOutcome::Confirmed)
    }
}
#[allow(
    clippy::too_many_lines,
    reason = "One outcome mapping preserves component evidence across dispatcher and sink limits"
)]
async fn render(
    dispatcher: &CapabilityDispatcher,
    request: &CapabilityRequest,
    correlation: plenora_runtime_messaging::CorrelationId,
    outcome: ComponentOutcome,
    raw: &Map<String, Value>,
    journal: &DeliveryJournal,
) -> Result<PreparedResult, SettlementError> {
    let report = match &outcome.result {
        Ok(r) => {
            serde_json::json!({"phase":outcome.phase,"effect":outcome.effect,"output":r.output().map(StoredMessage::capture).transpose()?,"contract":r.output_contract().map(ContractId::as_str)})
        }
        Err(e) => serde_json::to_value(e).map_err(|_| SettlementError)?,
    };
    let mut retry_ms = None;
    let mut terminal = match outcome.result {
        Ok(response) => {
            let capture = Arc::new(Capture::default());
            let d = CapabilityDispatcher::with_result_sink(
                dispatcher.registry().clone(),
                dispatcher.config(),
                crate::sink::IdentitySink {
                    sink: capture.clone(),
                    metadata: fixed_metadata(raw, journal.result_id()),
                },
            )
            .map_err(|_| SettlementError)?;
            let expected = d
                .registry()
                .discovery(request.capability())
                .and_then(|v| v.operation(request.operation(), request.operation_version()))
                .map(|op| op.output().clone());
            let checked = d
                .complete_response(
                    request.capability().clone(),
                    request.operation().clone(),
                    request.operation_version(),
                    correlation,
                    expected,
                    response,
                )
                .await;
            let message = capture.0.lock().map_err(|_| SettlementError)?.take();
            if matches!(
                outcome.effect,
                Effect::Partial | Effect::Unknown | Effect::RolledBack
            ) {
                error_message(
                    Category::Internal,
                    outcome.phase,
                    outcome.effect,
                    Retry::RequiresRecovery,
                    raw,
                    journal.result_id(),
                )?
            } else if checked.is_ok()
                && message
                    .as_ref()
                    .is_some_and(|m| m.len() <= journal.plan().sink_bytes)
            {
                message.ok_or(SettlementError)?
            } else {
                let category = checked
                    .err()
                    .and_then(|e| e.public_error().ok())
                    .map_or(Category::ResourceLimit, |e| e.category());
                error_message(
                    category,
                    outcome.phase,
                    outcome.effect,
                    if outcome.effect == Effect::None {
                        Retry::Never
                    } else {
                        Retry::RequiresRecovery
                    },
                    raw,
                    journal.result_id(),
                )?
            }
        }
        Err(error) => {
            if matches!(error.remote_effect(), Effect::None | Effect::RolledBack)
                && error.category() != Category::ResourceLimit
            {
                retry_ms = match error.retry() {
                    Retry::Safe => Some(0),
                    Retry::After { delay_ms } => Some(delay_ms),
                    _ => None,
                };
            }
            error_message(
                error.category(),
                error.phase(),
                error.remote_effect(),
                if error.category() == Category::ResourceLimit
                    && error.remote_effect() == Effect::None
                {
                    Retry::Never
                } else {
                    error.retry()
                },
                raw,
                journal.result_id(),
            )?
        }
    };
    if externalize(&mut terminal).is_err() {
        terminal = error_message(
            Category::ResourceLimit,
            outcome.phase,
            outcome.effect,
            if outcome.effect == Effect::None {
                Retry::Never
            } else {
                Retry::RequiresRecovery
            },
            raw,
            journal.result_id(),
        )?;
        externalize(&mut terminal)?;
        retry_ms = None;
    }
    Ok(PreparedResult {
        reuse_origin: None,
        report,
        terminal: StoredMessage::capture(&terminal)?,
        retry_ms,
    })
}
fn error_message(
    category: Category,
    phase: Phase,
    effect: Effect,
    retry: Retry,
    raw: &Map<String, Value>,
    id: &str,
) -> Result<SerializedMessage, SettlementError> {
    let public = PlenoraError::new(
        category,
        phase,
        effect,
        retry,
        "Runtime execution terminated.",
    )
    .map_err(|_| SettlementError)?;
    let mut m = SerializedMessage::new(
        PLENORA_ERROR_CONTENT_TYPE,
        serde_json::to_vec(&public).map_err(|_| SettlementError)?,
    );
    for (k, v) in fixed_metadata(raw, id) {
        m.headers
            .insert_text(k, v.as_str().ok_or(SettlementError)?)
            .map_err(|_| SettlementError)?;
    }
    Ok(m)
}
fn fixed_metadata(raw: &Map<String, Value>, id: &str) -> Map<String, Value> {
    let mut m = crate::error::result_metadata(raw);
    m.insert(ID.into(), id.into());
    m
}
fn prepared_rejection(
    r: Rejection,
    journal: &DeliveryJournal,
) -> Result<PreparedResult, SettlementError> {
    let mut m = r.to_message().map_err(|_| SettlementError)?;
    m.headers
        .insert_text(ID, journal.result_id())
        .map_err(|_| SettlementError)?;
    externalize(&mut m)?;
    Ok(PreparedResult {
        reuse_origin: None,
        report: serde_json::to_value(r).map_err(|_| SettlementError)?,
        terminal: StoredMessage::capture(&m)?,
        retry_ms: None,
    })
}
fn externalize(m: &mut SerializedMessage) -> Result<(), SettlementError> {
    for (k, v) in [
        (REQUIRES, "plenora-runtime-binding-v2"),
        (DOWNGRADE, "false"),
    ] {
        m.headers.insert_text(k, v).map_err(|_| SettlementError)?;
    }
    Ok(())
}
fn external_context(m: &SerializedMessage) -> Result<NegotiationContext, SettlementError> {
    NegotiationContext::from_headers(
        m.headers.get_text(REQUIRES).map_err(|_| SettlementError)?,
        m.headers.get_text(DOWNGRADE).map_err(|_| SettlementError)?,
    )
    .map_err(|_| SettlementError)?
    .ok_or(SettlementError)
}
fn reflection(m: &SerializedMessage) -> Map<String, Value> {
    if m.has_unreadable_metadata() || m.raw_metadata().is_some() {
        return Map::new();
    }
    m.headers
        .iter()
        .map(|(k, v)| {
            (
                k.into(),
                std::str::from_utf8(v).map_or(Value::Null, Into::into),
            )
        })
        .collect()
}
fn fingerprint(m: &SerializedMessage) -> [u8; 32] {
    fn field(h: &mut Sha256, b: &[u8]) {
        h.update((b.len() as u64).to_be_bytes());
        h.update(b);
    }
    let mut hash = Sha256::new();
    field(&mut hash, m.content_type.as_bytes());
    field(&mut hash, &m.bytes);
    for (k, v) in m.headers.iter().filter(|(k, _)| !DLQ_KEYS.contains(k)) {
        field(&mut hash, k.as_bytes());
        field(&mut hash, v);
    }
    hash.update([
        u8::from(m.raw_metadata().is_some()),
        u8::from(m.has_unreadable_metadata()),
        u8::from(m.exceeds_transport_payload_limit()),
    ]);
    if let Some(raw) = m.raw_metadata() {
        for (k, v) in raw {
            field(&mut hash, k.as_bytes());
            field(&mut hash, v);
        }
    }
    hash.finalize().into()
}
/// Raw carrier admitted only inside the durable handler; decoder errors cannot discard refusals.
pub struct RawDelivery(SerializedMessage);
/// Adapter-neutral handler/decoder installed in Apalis, broker runners or direct worker execution.
pub struct DurableWorker<S> {
    runtime: Arc<DurableRuntime<S>>,
    route: DeliveryRoute,
}
impl<S> Clone for DurableWorker<S> {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            route: self.route,
        }
    }
}
impl<S: DurableExecutionStore> DurableWorker<S> {
    /// Binds the receiving endpoint's trusted ingress/forwarding role.
    #[must_use]
    pub fn new(runtime: Arc<DurableRuntime<S>>, route: DeliveryRoute) -> Self {
        Self { runtime, route }
    }
}
impl<S: DurableExecutionStore> WorkerMessageDecoder<RawDelivery> for DurableWorker<S> {
    type Error = SettlementError;
    fn decode(
        &self,
        m: &SerializedMessage,
    ) -> Result<DecodedWorkerMessage<RawDelivery>, SettlementError> {
        // Internal job identities are never reflected into rejected runtime envelopes.
        Ok(DecodedWorkerMessage::new(
            WorkerContextIdentity::new(
                MessageId::random(),
                plenora_runtime_messaging::CorrelationId::random(),
                None,
            ),
            RawDelivery(m.clone()),
        ))
    }
}
#[async_trait]
impl<S: DurableExecutionStore> WorkerHandler<RawDelivery> for DurableWorker<S> {
    type Error = SettlementError;
    async fn handle(&self, c: WorkerContext, m: RawDelivery) -> Result<(), SettlementError> {
        self.runtime.process(&m.0, self.route, c).await.map(|_| ())
    }
}
/// Transport retry is publication/receipt recovery, never permission to repeat domain work.
/// Use with `DurableWorker`: only its durable state machine authorizes component retries.
pub struct SettlementRetry;
impl RetryPolicy<SettlementError> for SettlementRetry {
    fn decide(&self, _: u32, _: &SettlementError) -> RetryDecision {
        RetryDecision::RetryAfter(Duration::from_secs(1))
    }
}
/// Frozen outgoing endpoint used for request resend, broker retry and DLQ publication.
/// The `MessageProducer` blanket `DeadLetterSink` implementation preserves context headers.
pub struct NegotiatedForwarder<S> {
    runtime: Arc<DurableRuntime<S>>,
    route: String,
}
impl<S: DurableExecutionStore> NegotiatedForwarder<S> {
    /// Binds one configured destination; callers cannot select a producer per message.
    /// # Errors
    /// Refuses an unregistered route.
    pub fn new(runtime: Arc<DurableRuntime<S>>, route: &str) -> Result<Self, SettlementError> {
        if !runtime.routes.0.contains_key(route) {
            return Err(SettlementError);
        }
        Ok(Self {
            runtime,
            route: route.into(),
        })
    }
}
#[async_trait]
impl<S: DurableExecutionStore> plenora_runtime_messaging::MessageProducer
    for NegotiatedForwarder<S>
{
    type Error = SettlementError;
    async fn publish(&self, message: SerializedMessage) -> Result<PublishOutcome, SettlementError> {
        self.runtime.forward(&message, &self.route).await
    }
}

struct Producer(Arc<dyn BoundaryResultSink>);
#[async_trait]
impl plenora_runtime_messaging::MessageProducer for Producer {
    type Error = SettlementError;
    async fn publish(&self, m: SerializedMessage) -> Result<PublishOutcome, SettlementError> {
        self.0.publish(m).await.map_err(|_| SettlementError)
    }
}
