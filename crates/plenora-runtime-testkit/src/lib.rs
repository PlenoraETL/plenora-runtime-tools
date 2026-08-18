//! Deterministic fakes and fault-injection support for runtime consumers.

#![forbid(unsafe_code)]

mod broker;
mod capability;
mod clock;
mod fault;
mod retry;
mod shutdown;

pub use broker::{
    AckEvent, AckRecord, FakeBroker, FakeBrokerError, FakeBrokerErrorKind, FakeBrokerLimits,
    FakeBrokerSnapshot, FakeConsumer, FakeDelivery, FakeDeliveryId, FakeProducer, HeartbeatEvent,
    HeartbeatRecord, UnknownPublishEffect,
};
pub use capability::{
    FakeCapabilityConfig, FakeCapabilityError, FakeCapabilityErrorKind, FakeCapabilityHandler,
    FakeCapabilityInvocation, FakeCapabilityOutcome, FakeCapabilitySnapshot,
    MAX_FAKE_CAPABILITY_HISTORY, MAX_FAKE_CAPABILITY_OUTCOMES,
};
pub use clock::{ManualClock, ManualClockError, TestClock};
pub use fault::{DEFAULT_FAULT_SEQUENCE_CAPACITY, FaultSequence, FaultSequenceCapacityError};
pub use plenora_runtime_outbox::{FakeIdempotencyStore, FakeInboxStore, FakeOutboxStore};
pub use retry::{RetryObservation, observe_retry_decisions};
pub use shutdown::ShutdownHarness;
