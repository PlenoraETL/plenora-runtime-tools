use std::{
    collections::{BTreeMap, VecDeque},
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use plenora_runtime_messaging::{
    AckError, AckOperation, Delivery, DeliveryAcknowledger, DeliveryHeartbeatConfig,
    MessageConsumer, MessageMetadata, MessageProducer, NackReason, PublishOutcome,
    SerializedMessage,
};

use crate::ManualClock;

/// Stable identifier assigned to one logical fake-broker delivery.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FakeDeliveryId(u64);

impl FakeDeliveryId {
    /// Wraps a deterministic numeric identifier.
    #[must_use]
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Returns the deterministic numeric identifier.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Display for FakeDeliveryId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

/// Whether a simulated unknown publish outcome applied its remote effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnknownPublishEffect {
    /// The message reached the broker even though confirmation was lost.
    Applied,
    /// The message did not reach the broker.
    NotApplied,
}

/// Category of an operation failure injected by [`FakeBroker`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FakeBrokerErrorKind {
    /// The broker is currently disconnected.
    Disconnected,
    /// A caller-scripted operation fault was consumed.
    Injected,
    /// A requested delivery identifier is unknown.
    UnknownDelivery,
    /// The deterministic delivery identifier space was exhausted.
    IdentifierExhausted,
    /// A delayed instant cannot be represented by [`SystemTime`].
    DelayOverflow,
    /// Internal portable broker metadata could not be constructed.
    InvalidBrokerMetadata,
    /// A configured queue, catalog, history, or fault-script bound was reached.
    CapacityExceeded,
    /// A message exceeded the configured fake-broker payload bound.
    PayloadTooLarge,
}

/// Redaction-safe error returned by fake broker operations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FakeBrokerError {
    kind: FakeBrokerErrorKind,
    message: Arc<str>,
}

impl FakeBrokerError {
    fn new(kind: FakeBrokerErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Returns the stable error category.
    #[must_use]
    pub const fn kind(&self) -> FakeBrokerErrorKind {
        self.kind
    }

    /// Returns the redaction-safe diagnostic message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for FakeBrokerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for FakeBrokerError {}

/// A recorded acknowledgement-side effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckEvent {
    /// The delivery was acknowledged successfully.
    Acked,
    /// The delivery was negatively acknowledged and scheduled for redelivery.
    Nacked(NackReason),
    /// The operation failed and the fake scheduled conservative redelivery.
    Failed(AckOperation),
}

/// Deterministic acknowledgement record captured by [`FakeBroker`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AckRecord {
    /// Logical delivery identity.
    pub delivery_id: FakeDeliveryId,
    /// Attempt on which the operation was made.
    pub attempt: u32,
    /// Recorded acknowledgement result.
    pub event: AckEvent,
}

/// Result of one deterministic non-terminal heartbeat attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeartbeatEvent {
    /// Broker ownership was renewed.
    Renewed,
    /// Renewal failed without settling the delivery.
    Failed,
}

/// Deterministic heartbeat record captured by [`FakeBroker`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeartbeatRecord {
    /// Logical delivery identity.
    pub delivery_id: FakeDeliveryId,
    /// Attempt on which the heartbeat was made.
    pub attempt: u32,
    /// Recorded heartbeat result.
    pub event: HeartbeatEvent,
}

/// Explicit memory and admission bounds for [`FakeBroker`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FakeBrokerLimits {
    /// Maximum queued plus currently in-flight deliveries.
    pub max_pending_deliveries: usize,
    /// Maximum logical deliveries retained for deterministic duplicate injection.
    pub max_catalog_entries: usize,
    /// Maximum applied-publish records retained in memory.
    pub max_published_history: usize,
    /// Maximum settlement and heartbeat records retained in each history.
    pub max_acknowledgement_history: usize,
    /// Maximum terminal-delivery records retained in memory.
    pub max_terminal_history: usize,
    /// Maximum pending faults per operation-specific script.
    pub max_scripted_faults: usize,
    /// Maximum encoded message payload size accepted by this fake.
    pub max_message_bytes: usize,
}

impl Default for FakeBrokerLimits {
    fn default() -> Self {
        Self {
            max_pending_deliveries: 1_024,
            max_catalog_entries: 4_096,
            max_published_history: 1_024,
            max_acknowledgement_history: 4_096,
            max_terminal_history: 1_024,
            max_scripted_faults: 256,
            max_message_bytes: 1_048_576,
        }
    }
}

/// Point-in-time counters and connectivity for a [`FakeBroker`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FakeBrokerSnapshot {
    /// Whether broker operations are currently accepted.
    pub connected: bool,
    /// Number of queued deliveries, including delayed ones.
    pub pending_deliveries: usize,
    /// Number of publish calls whose simulated remote effect was applied.
    pub applied_publishes: usize,
    /// Number of acknowledgement operations recorded.
    pub acknowledgement_count: usize,
    /// Number of non-terminal heartbeat attempts recorded.
    pub heartbeat_count: usize,
    /// Number of deliveries currently owned by consumers.
    pub in_flight_deliveries: usize,
    /// Number of terminal negative acknowledgements observed.
    pub terminal_delivery_count: usize,
}

#[derive(Clone, Debug)]
struct QueuedDelivery {
    id: FakeDeliveryId,
    message: SerializedMessage,
    attempt: u32,
    available_at: SystemTime,
}

#[derive(Clone, Debug)]
enum PublishFault {
    Error,
    OutcomeUnknown(UnknownPublishEffect),
}

#[derive(Debug)]
struct BrokerState {
    limits: FakeBrokerLimits,
    connected: bool,
    next_id: u64,
    queue: VecDeque<QueuedDelivery>,
    in_flight: usize,
    catalog: BTreeMap<FakeDeliveryId, SerializedMessage>,
    published: VecDeque<SerializedMessage>,
    applied_publish_count: usize,
    acknowledgements: VecDeque<AckRecord>,
    acknowledgement_count: usize,
    heartbeats: VecDeque<HeartbeatRecord>,
    heartbeat_count: usize,
    terminal_deliveries: VecDeque<AckRecord>,
    terminal_delivery_count: usize,
    publish_faults: VecDeque<PublishFault>,
    receive_faults: VecDeque<()>,
    ack_faults: VecDeque<()>,
    nack_faults: VecDeque<()>,
    heartbeat_faults: VecDeque<()>,
}

impl BrokerState {
    fn new(limits: FakeBrokerLimits) -> Self {
        Self {
            limits,
            connected: true,
            next_id: 1,
            queue: VecDeque::new(),
            in_flight: 0,
            catalog: BTreeMap::new(),
            published: VecDeque::new(),
            applied_publish_count: 0,
            acknowledgements: VecDeque::new(),
            acknowledgement_count: 0,
            heartbeats: VecDeque::new(),
            heartbeat_count: 0,
            terminal_deliveries: VecDeque::new(),
            terminal_delivery_count: 0,
            publish_faults: VecDeque::new(),
            receive_faults: VecDeque::new(),
            ack_faults: VecDeque::new(),
            nack_faults: VecDeque::new(),
            heartbeat_faults: VecDeque::new(),
        }
    }
}

/// In-memory broker with deterministic time, redelivery, and fault injection.
#[derive(Clone)]
pub struct FakeBroker {
    state: Arc<Mutex<BrokerState>>,
    clock: ManualClock,
}

impl FakeBroker {
    /// Creates a connected broker controlled by the supplied clock.
    #[must_use]
    pub fn new(clock: ManualClock) -> Self {
        Self::with_limits(clock, FakeBrokerLimits::default())
    }

    /// Creates a connected broker with explicit memory and admission bounds.
    #[must_use]
    pub fn with_limits(clock: ManualClock, limits: FakeBrokerLimits) -> Self {
        Self {
            state: Arc::new(Mutex::new(BrokerState::new(limits))),
            clock,
        }
    }

    /// Returns the broker's shared manual clock.
    #[must_use]
    pub fn clock(&self) -> ManualClock {
        self.clock.clone()
    }

    /// Creates a producer attached to this broker.
    #[must_use]
    pub fn producer(&self) -> FakeProducer {
        FakeProducer::new(self.clone())
    }

    /// Creates a consumer attached to this broker.
    #[must_use]
    pub fn consumer(&self) -> FakeConsumer {
        FakeConsumer::new(self.clone())
    }

    /// Creates a consumer whose deliveries require the supplied heartbeat policy.
    #[must_use]
    pub fn consumer_with_heartbeat(&self, heartbeat: DeliveryHeartbeatConfig) -> FakeConsumer {
        FakeConsumer::new(self.clone()).with_heartbeat(heartbeat)
    }

    /// Enqueues a message for immediate delivery.
    ///
    /// # Errors
    ///
    /// Returns an error when the message or delivery/catalog bounds are exceeded, or when the
    /// deterministic identifier space is exhausted.
    pub fn enqueue(&self, message: SerializedMessage) -> Result<FakeDeliveryId, FakeBrokerError> {
        self.enqueue_at(message, self.clock.current())
    }

    /// Enqueues a message that remains unavailable until the clock advances by `delay`.
    ///
    /// # Errors
    ///
    /// Returns an error if the message or delivery/catalog bounds are exceeded, or if the target
    /// instant or a new delivery identifier cannot be represented.
    pub fn enqueue_delayed(
        &self,
        message: SerializedMessage,
        delay: Duration,
    ) -> Result<FakeDeliveryId, FakeBrokerError> {
        let available_at = self.clock.current().checked_add(delay).ok_or_else(|| {
            FakeBrokerError::new(
                FakeBrokerErrorKind::DelayOverflow,
                "fake delivery delay exceeds the SystemTime range",
            )
        })?;
        self.enqueue_at(message, available_at)
    }

    /// Injects another delivery with the same message bytes as a known logical delivery.
    ///
    /// # Errors
    ///
    /// Returns an error when the source identifier is unknown or identifiers are exhausted.
    pub fn inject_duplicate(
        &self,
        source: FakeDeliveryId,
    ) -> Result<FakeDeliveryId, FakeBrokerError> {
        let now = self.clock.current();
        let mut state = self.lock();
        let message = state.catalog.get(&source).cloned().ok_or_else(|| {
            FakeBrokerError::new(
                FakeBrokerErrorKind::UnknownDelivery,
                "cannot duplicate an unknown fake delivery",
            )
        })?;
        enqueue_locked(&mut state, message, now)
    }

    /// Prevents subsequent broker operations until [`Self::reconnect`] is called.
    pub fn disconnect(&self) {
        self.lock().connected = false;
    }

    /// Restores broker operations after a simulated disconnect.
    pub fn reconnect(&self) {
        self.lock().connected = true;
    }

    /// Returns whether the broker is connected.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.lock().connected
    }

    /// Makes the next publish return an injected adapter error.
    ///
    /// # Errors
    ///
    /// Returns an error when the bounded publish-fault script is full.
    pub fn fail_next_publish(&self, _message: impl Into<Arc<str>>) -> Result<(), FakeBrokerError> {
        let mut state = self.lock();
        push_fault(&mut state, FaultKind::Publish(PublishFault::Error))
    }

    /// Makes the next publish return [`PublishOutcome::OutcomeUnknown`].
    ///
    /// The explicit effect controls whether consumers can receive the message despite the lost
    /// confirmation.
    ///
    /// # Errors
    ///
    /// Returns an error when the bounded publish-fault script is full.
    pub fn return_unknown_for_next_publish(
        &self,
        effect: UnknownPublishEffect,
    ) -> Result<(), FakeBrokerError> {
        let mut state = self.lock();
        push_fault(
            &mut state,
            FaultKind::Publish(PublishFault::OutcomeUnknown(effect)),
        )
    }

    /// Makes the next receive operation return an injected adapter error.
    ///
    /// # Errors
    ///
    /// Returns an error when the bounded receive-fault script is full.
    pub fn fail_next_receive(&self, _message: impl Into<Arc<str>>) -> Result<(), FakeBrokerError> {
        let mut state = self.lock();
        push_fault(&mut state, FaultKind::Receive)
    }

    /// Makes the next positive acknowledgement fail and conservatively redeliver the message.
    ///
    /// # Errors
    ///
    /// Returns an error when the bounded acknowledgement-fault script is full.
    pub fn fail_next_ack(&self, _message: impl Into<Arc<str>>) -> Result<(), FakeBrokerError> {
        let mut state = self.lock();
        push_fault(&mut state, FaultKind::Ack)
    }

    /// Makes the next negative acknowledgement fail and conservatively redeliver the message.
    ///
    /// # Errors
    ///
    /// Returns an error when the bounded negative-acknowledgement fault script is full.
    pub fn fail_next_nack(&self, _message: impl Into<Arc<str>>) -> Result<(), FakeBrokerError> {
        let mut state = self.lock();
        push_fault(&mut state, FaultKind::Nack)
    }

    /// Makes the next non-terminal delivery heartbeat fail without settling the delivery.
    ///
    /// # Errors
    ///
    /// Returns an error when the bounded heartbeat-fault script is full.
    pub fn fail_next_heartbeat(
        &self,
        _message: impl Into<Arc<str>>,
    ) -> Result<(), FakeBrokerError> {
        let mut state = self.lock();
        push_fault(&mut state, FaultKind::Heartbeat)
    }

    /// Returns messages whose simulated remote publish effect was applied, in call order.
    #[must_use]
    pub fn published_messages(&self) -> Vec<SerializedMessage> {
        self.lock().published.iter().cloned().collect()
    }

    /// Returns acknowledgement records in operation order.
    #[must_use]
    pub fn acknowledgement_records(&self) -> Vec<AckRecord> {
        self.lock().acknowledgements.iter().copied().collect()
    }

    /// Returns heartbeat records in operation order.
    #[must_use]
    pub fn heartbeat_records(&self) -> Vec<HeartbeatRecord> {
        self.lock().heartbeats.iter().copied().collect()
    }

    /// Returns retained terminal-delivery records in observation order.
    #[must_use]
    pub fn terminal_delivery_records(&self) -> Vec<AckRecord> {
        self.lock().terminal_deliveries.iter().copied().collect()
    }

    /// Returns a redaction-safe point-in-time broker snapshot.
    #[must_use]
    pub fn snapshot(&self) -> FakeBrokerSnapshot {
        let state = self.lock();
        FakeBrokerSnapshot {
            connected: state.connected,
            pending_deliveries: state.queue.len(),
            applied_publishes: state.applied_publish_count,
            acknowledgement_count: state.acknowledgement_count,
            heartbeat_count: state.heartbeat_count,
            in_flight_deliveries: state.in_flight,
            terminal_delivery_count: state.terminal_delivery_count,
        }
    }

    /// Dequeues the first ready fake delivery without waiting for time to advance.
    ///
    /// # Errors
    ///
    /// Returns a disconnected, injected receive, or internal metadata error.
    pub fn dequeue(&self) -> Result<Option<FakeDelivery>, FakeBrokerError> {
        let now = self.clock.current();
        let queued = {
            let mut state = self.lock();
            if !state.connected {
                return Err(disconnected_error());
            }
            if state.receive_faults.pop_front().is_some() {
                return Err(FakeBrokerError::new(
                    FakeBrokerErrorKind::Injected,
                    "fake broker receive fault was injected",
                ));
            }

            let Some(position) = state
                .queue
                .iter()
                .position(|delivery| delivery.available_at <= now)
            else {
                return Ok(None);
            };
            let queued = state.queue.remove(position);
            if queued.is_some() {
                state.in_flight = state.in_flight.saturating_add(1);
            }
            queued
        };

        match queued {
            Some(queued) => FakeDelivery::new(self.clone(), queued).map(Some),
            None => Ok(None),
        }
    }

    fn enqueue_at(
        &self,
        message: SerializedMessage,
        available_at: SystemTime,
    ) -> Result<FakeDeliveryId, FakeBrokerError> {
        enqueue_locked(&mut self.lock(), message, available_at)
    }

    fn publish(&self, message: SerializedMessage) -> Result<PublishOutcome, FakeBrokerError> {
        let now = self.clock.current();
        let mut state = self.lock();
        if !state.connected {
            return Err(disconnected_error());
        }
        validate_message(&state, &message)?;

        match state.publish_faults.pop_front() {
            Some(PublishFault::Error) => Err(FakeBrokerError::new(
                FakeBrokerErrorKind::Injected,
                "fake broker publish fault was injected",
            )),
            Some(PublishFault::OutcomeUnknown(UnknownPublishEffect::NotApplied)) => {
                Ok(PublishOutcome::OutcomeUnknown)
            }
            Some(PublishFault::OutcomeUnknown(UnknownPublishEffect::Applied)) => {
                let _delivery_id = enqueue_locked(&mut state, message.clone(), now)?;
                record_published(&mut state, message);
                Ok(PublishOutcome::OutcomeUnknown)
            }
            None => {
                let _delivery_id = enqueue_locked(&mut state, message.clone(), now)?;
                record_published(&mut state, message);
                Ok(PublishOutcome::Confirmed)
            }
        }
    }

    fn complete_delivery(&self, queued: QueuedDelivery, event: AckEvent) -> Result<(), AckError> {
        let operation = match event {
            AckEvent::Acked => AckOperation::Ack,
            AckEvent::Nacked(_) => AckOperation::Nack,
            AckEvent::Failed(operation) => operation,
        };
        let now = self.clock.current();
        let mut state = self.lock();
        let failure = if state.connected {
            let injected = match operation {
                AckOperation::Ack => state.ack_faults.pop_front(),
                AckOperation::Nack => state.nack_faults.pop_front(),
                AckOperation::Heartbeat => None,
            };
            injected.map(|()| {
                FakeBrokerError::new(
                    FakeBrokerErrorKind::Injected,
                    "fake broker acknowledgement fault was injected",
                )
            })
        } else {
            Some(disconnected_error())
        };

        if let Some(failure) = failure {
            state.in_flight = state.in_flight.saturating_sub(1);
            record_acknowledgement(
                &mut state,
                AckRecord {
                    delivery_id: queued.id,
                    attempt: queued.attempt,
                    event: AckEvent::Failed(operation),
                },
            );
            state.queue.push_back(redelivery(queued, now));
            return Err(AckError::with_source(
                operation,
                "fake broker acknowledgement failed",
                failure,
            ));
        }

        state.in_flight = state.in_flight.saturating_sub(1);
        let record = AckRecord {
            delivery_id: queued.id,
            attempt: queued.attempt,
            event,
        };
        record_acknowledgement(&mut state, record);
        match event {
            AckEvent::Nacked(
                NackReason::Retryable | NackReason::RetryAfter(_) | NackReason::Shutdown,
            ) => {
                state.queue.push_back(redelivery(queued, now));
            }
            AckEvent::Nacked(NackReason::Permanent | NackReason::ConsumerRejected) => {
                record_terminal_delivery(&mut state, record);
            }
            AckEvent::Acked | AckEvent::Failed(_) => {}
        }
        Ok(())
    }

    fn heartbeat_delivery(&self, queued: &QueuedDelivery) -> Result<(), AckError> {
        let mut state = self.lock();
        let failure = if state.connected {
            state.heartbeat_faults.pop_front().map(|()| {
                FakeBrokerError::new(
                    FakeBrokerErrorKind::Injected,
                    "fake broker heartbeat fault was injected",
                )
            })
        } else {
            Some(disconnected_error())
        };
        let event = if failure.is_some() {
            HeartbeatEvent::Failed
        } else {
            HeartbeatEvent::Renewed
        };
        record_heartbeat(
            &mut state,
            HeartbeatRecord {
                delivery_id: queued.id,
                attempt: queued.attempt,
                event,
            },
        );
        match failure {
            Some(failure) => Err(AckError::with_source(
                AckOperation::Heartbeat,
                "fake broker delivery heartbeat failed",
                failure,
            )),
            None => Ok(()),
        }
    }

    fn abandon_delivery(&self, queued: QueuedDelivery) {
        let now = self.clock.current();
        let mut state = self.lock();
        state.in_flight = state.in_flight.saturating_sub(1);
        record_acknowledgement(
            &mut state,
            AckRecord {
                delivery_id: queued.id,
                attempt: queued.attempt,
                event: AckEvent::Nacked(NackReason::Shutdown),
            },
        );
        state.queue.push_back(redelivery(queued, now));
    }

    fn lock(&self) -> MutexGuard<'_, BrokerState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl Default for FakeBroker {
    fn default() -> Self {
        Self::new(ManualClock::default())
    }
}

impl Debug for FakeBroker {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FakeBroker")
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}

/// Test producer backed by a [`FakeBroker`].
#[derive(Clone, Debug)]
pub struct FakeProducer {
    broker: FakeBroker,
}

impl FakeProducer {
    /// Creates a producer attached to `broker`.
    #[must_use]
    pub const fn new(broker: FakeBroker) -> Self {
        Self { broker }
    }

    /// Returns the attached broker.
    #[must_use]
    pub const fn broker(&self) -> &FakeBroker {
        &self.broker
    }
}

#[async_trait]
impl MessageProducer for FakeProducer {
    type Error = FakeBrokerError;

    async fn publish(&self, message: SerializedMessage) -> Result<PublishOutcome, Self::Error> {
        self.broker.publish(message)
    }
}

/// Test consumer backed by a [`FakeBroker`].
#[derive(Debug)]
pub struct FakeConsumer {
    broker: FakeBroker,
    closed: bool,
    heartbeat: Option<DeliveryHeartbeatConfig>,
}

impl FakeConsumer {
    /// Creates an open consumer attached to `broker`.
    #[must_use]
    pub const fn new(broker: FakeBroker) -> Self {
        Self {
            broker,
            closed: false,
            heartbeat: None,
        }
    }

    /// Attaches a heartbeat policy to subsequently received broker-neutral deliveries.
    #[must_use]
    pub fn with_heartbeat(mut self, heartbeat: DeliveryHeartbeatConfig) -> Self {
        self.heartbeat = Some(heartbeat);
        self
    }

    /// Stops this consumer. Later receives return `None` without touching the broker.
    pub fn close(&mut self) {
        self.closed = true;
    }

    /// Reopens a previously closed consumer.
    pub fn reopen(&mut self) {
        self.closed = false;
    }

    /// Returns whether this consumer is closed.
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.closed
    }

    /// Receives an inspectable fake delivery.
    ///
    /// # Errors
    ///
    /// Returns the same deterministic failures as [`FakeBroker::dequeue`].
    pub fn receive_fake(&mut self) -> Result<Option<FakeDelivery>, FakeBrokerError> {
        if self.closed {
            Ok(None)
        } else {
            self.broker.dequeue()
        }
    }
}

#[async_trait]
impl MessageConsumer for FakeConsumer {
    type Error = FakeBrokerError;

    async fn receive(&mut self) -> Result<Option<Delivery>, Self::Error> {
        let heartbeat = self.heartbeat;
        self.receive_fake().map(|delivery| {
            delivery.map(|delivery| match heartbeat {
                Some(config) => delivery.into_delivery_with_heartbeat(config),
                None => delivery.into_delivery(),
            })
        })
    }
}

/// Inspectable owned delivery produced by [`FakeBroker`].
///
/// Dropping it without ACK/NACK conservatively records a shutdown NACK and requeues it.
pub struct FakeDelivery {
    /// Serialized message received from the fake broker.
    pub message: SerializedMessage,
    /// One-based simulated delivery attempt.
    pub attempt: u32,
    /// Portable metadata containing the fake delivery identifier.
    pub broker_metadata: MessageMetadata,
    id: FakeDeliveryId,
    broker: FakeBroker,
    queued: QueuedDelivery,
    settled: bool,
}

impl FakeDelivery {
    fn new(broker: FakeBroker, queued: QueuedDelivery) -> Result<Self, FakeBrokerError> {
        let mut broker_metadata = MessageMetadata::new();
        broker_metadata
            .insert_text("plenora.testkit.delivery_id", queued.id.to_string())
            .map_err(|_error| {
                FakeBrokerError::new(
                    FakeBrokerErrorKind::InvalidBrokerMetadata,
                    "fake broker could not construct delivery metadata",
                )
            })?;

        Ok(Self {
            message: queued.message.clone(),
            attempt: queued.attempt,
            broker_metadata,
            id: queued.id,
            broker,
            queued,
            settled: false,
        })
    }

    /// Returns the stable logical delivery identifier.
    #[must_use]
    pub const fn id(&self) -> FakeDeliveryId {
        self.id
    }

    /// Converts this inspectable value into the broker-neutral messaging delivery.
    #[must_use]
    pub fn into_delivery(mut self) -> Delivery {
        self.settled = true;
        let acknowledger = FakeAcknowledger {
            broker: self.broker.clone(),
            queued: self.queued.clone(),
            completed: false,
        };
        Delivery::new(
            self.message.clone(),
            self.attempt,
            self.broker_metadata.clone(),
            acknowledger,
        )
    }

    /// Converts this value into a broker-neutral delivery with lease renewal enabled.
    #[must_use]
    pub fn into_delivery_with_heartbeat(mut self, heartbeat: DeliveryHeartbeatConfig) -> Delivery {
        self.settled = true;
        let acknowledger = FakeAcknowledger {
            broker: self.broker.clone(),
            queued: self.queued.clone(),
            completed: false,
        };
        Delivery::new_with_heartbeat(
            self.message.clone(),
            self.attempt,
            self.broker_metadata.clone(),
            heartbeat,
            acknowledger,
        )
    }

    /// Positively acknowledges and consumes this delivery.
    ///
    /// # Errors
    ///
    /// Returns a deterministic acknowledgement error injected by the broker.
    pub async fn ack(self) -> Result<(), AckError> {
        self.into_delivery().ack().await
    }

    /// Negatively acknowledges and consumes this delivery.
    ///
    /// # Errors
    ///
    /// Returns a deterministic acknowledgement error injected by the broker.
    pub async fn nack(self, reason: NackReason) -> Result<(), AckError> {
        self.into_delivery().nack(reason).await
    }
}

impl Drop for FakeDelivery {
    fn drop(&mut self) {
        if !self.settled {
            self.broker.abandon_delivery(self.queued.clone());
        }
    }
}

impl Debug for FakeDelivery {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FakeDelivery")
            .field("message", &self.message)
            .field("attempt", &self.attempt)
            .field("broker_metadata", &self.broker_metadata)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct FakeAcknowledger {
    broker: FakeBroker,
    queued: QueuedDelivery,
    completed: bool,
}

#[async_trait]
impl DeliveryAcknowledger for FakeAcknowledger {
    async fn heartbeat(&mut self) -> Result<(), AckError> {
        self.broker.heartbeat_delivery(&self.queued)
    }

    async fn ack(mut self: Box<Self>) -> Result<(), AckError> {
        self.completed = true;
        self.broker
            .complete_delivery(self.queued.clone(), AckEvent::Acked)
    }

    async fn nack(mut self: Box<Self>, reason: NackReason) -> Result<(), AckError> {
        self.completed = true;
        self.broker
            .complete_delivery(self.queued.clone(), AckEvent::Nacked(reason))
    }
}

impl Drop for FakeAcknowledger {
    fn drop(&mut self) {
        if !self.completed {
            self.broker.abandon_delivery(self.queued.clone());
        }
    }
}

enum FaultKind {
    Publish(PublishFault),
    Receive,
    Ack,
    Nack,
    Heartbeat,
}

fn push_fault(state: &mut BrokerState, fault: FaultKind) -> Result<(), FakeBrokerError> {
    let capacity = state.limits.max_scripted_faults;
    let target_len = match &fault {
        FaultKind::Publish(_) => state.publish_faults.len(),
        FaultKind::Receive => state.receive_faults.len(),
        FaultKind::Ack => state.ack_faults.len(),
        FaultKind::Nack => state.nack_faults.len(),
        FaultKind::Heartbeat => state.heartbeat_faults.len(),
    };
    if target_len >= capacity {
        return Err(capacity_error(
            "fake broker fault-script capacity was reached",
        ));
    }

    match fault {
        FaultKind::Publish(fault) => state.publish_faults.push_back(fault),
        FaultKind::Receive => state.receive_faults.push_back(()),
        FaultKind::Ack => state.ack_faults.push_back(()),
        FaultKind::Nack => state.nack_faults.push_back(()),
        FaultKind::Heartbeat => state.heartbeat_faults.push_back(()),
    }
    Ok(())
}

fn validate_message(
    state: &BrokerState,
    message: &SerializedMessage,
) -> Result<(), FakeBrokerError> {
    if message.len() > state.limits.max_message_bytes {
        Err(FakeBrokerError::new(
            FakeBrokerErrorKind::PayloadTooLarge,
            "fake broker payload exceeds the configured byte limit",
        ))
    } else {
        Ok(())
    }
}

fn validate_delivery_capacity(state: &BrokerState) -> Result<(), FakeBrokerError> {
    if state.queue.len().saturating_add(state.in_flight) >= state.limits.max_pending_deliveries {
        return Err(capacity_error(
            "fake broker pending-delivery capacity was reached",
        ));
    }
    if state.catalog.len() >= state.limits.max_catalog_entries {
        return Err(capacity_error(
            "fake broker duplicate catalog capacity was reached",
        ));
    }
    Ok(())
}

fn record_published(state: &mut BrokerState, message: SerializedMessage) {
    state.applied_publish_count = state.applied_publish_count.saturating_add(1);
    push_bounded(
        &mut state.published,
        message,
        state.limits.max_published_history,
    );
}

fn record_acknowledgement(state: &mut BrokerState, record: AckRecord) {
    state.acknowledgement_count = state.acknowledgement_count.saturating_add(1);
    push_bounded(
        &mut state.acknowledgements,
        record,
        state.limits.max_acknowledgement_history,
    );
}

fn record_heartbeat(state: &mut BrokerState, record: HeartbeatRecord) {
    state.heartbeat_count = state.heartbeat_count.saturating_add(1);
    push_bounded(
        &mut state.heartbeats,
        record,
        state.limits.max_acknowledgement_history,
    );
}

fn record_terminal_delivery(state: &mut BrokerState, record: AckRecord) {
    state.terminal_delivery_count = state.terminal_delivery_count.saturating_add(1);
    push_bounded(
        &mut state.terminal_deliveries,
        record,
        state.limits.max_terminal_history,
    );
}

fn push_bounded<T>(entries: &mut VecDeque<T>, entry: T, capacity: usize) {
    if capacity == 0 {
        return;
    }
    if entries.len() == capacity {
        entries.pop_front();
    }
    entries.push_back(entry);
}

fn enqueue_locked(
    state: &mut BrokerState,
    message: SerializedMessage,
    available_at: SystemTime,
) -> Result<FakeDeliveryId, FakeBrokerError> {
    validate_message(state, &message)?;
    validate_delivery_capacity(state)?;
    let id = FakeDeliveryId(state.next_id);
    state.next_id = state.next_id.checked_add(1).ok_or_else(|| {
        FakeBrokerError::new(
            FakeBrokerErrorKind::IdentifierExhausted,
            "fake broker delivery identifier space is exhausted",
        )
    })?;
    state.catalog.insert(id, message.clone());
    state.queue.push_back(QueuedDelivery {
        id,
        message,
        attempt: 1,
        available_at,
    });
    Ok(id)
}

fn redelivery(mut queued: QueuedDelivery, available_at: SystemTime) -> QueuedDelivery {
    queued.attempt = queued.attempt.saturating_add(1);
    queued.available_at = available_at;
    queued
}

fn disconnected_error() -> FakeBrokerError {
    FakeBrokerError::new(
        FakeBrokerErrorKind::Disconnected,
        "fake broker is disconnected",
    )
}

fn capacity_error(message: &'static str) -> FakeBrokerError {
    FakeBrokerError::new(FakeBrokerErrorKind::CapacityExceeded, message)
}
