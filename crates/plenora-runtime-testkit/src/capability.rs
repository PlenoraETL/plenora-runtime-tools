use std::{
    collections::VecDeque,
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    sync::{Arc, Mutex, MutexGuard},
};

use async_trait::async_trait;
use plenora_runtime_capabilities::{
    CapabilityFailure, CapabilityHandler, CapabilityId, CapabilityRemoteEffect, CapabilityRequest,
    OperationName,
};
use plenora_runtime_messaging::{CorrelationId, MessageId, RetryErrorClass};
use plenora_runtime_worker::WorkerContext;

/// Hard upper bound for payload-free fake capability invocation records.
pub const MAX_FAKE_CAPABILITY_HISTORY: usize = 65_536;
/// Hard upper bound for deterministic scripted fake outcomes.
pub const MAX_FAKE_CAPABILITY_OUTCOMES: usize = 65_536;

/// Explicit in-memory bounds for [`FakeCapabilityHandler`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FakeCapabilityConfig {
    /// Maximum payload-free invocation records retained.
    pub invocation_capacity: usize,
    /// Maximum pending scripted outcomes.
    pub outcome_capacity: usize,
}

impl FakeCapabilityConfig {
    /// Creates and validates fake capability bounds.
    ///
    /// # Errors
    ///
    /// Returns an error when a capacity is zero or exceeds its hard upper bound.
    pub const fn new(
        invocation_capacity: usize,
        outcome_capacity: usize,
    ) -> Result<Self, FakeCapabilityError> {
        if invocation_capacity == 0 || outcome_capacity == 0 {
            Err(FakeCapabilityError::new(
                FakeCapabilityErrorKind::ZeroCapacity,
            ))
        } else if invocation_capacity > MAX_FAKE_CAPABILITY_HISTORY
            || outcome_capacity > MAX_FAKE_CAPABILITY_OUTCOMES
        {
            Err(FakeCapabilityError::new(
                FakeCapabilityErrorKind::CapacityAboveMaximum,
            ))
        } else {
            Ok(Self {
                invocation_capacity,
                outcome_capacity,
            })
        }
    }
}

impl Default for FakeCapabilityConfig {
    fn default() -> Self {
        Self {
            invocation_capacity: 1_024,
            outcome_capacity: 256,
        }
    }
}

/// Deterministic FIFO result returned by a fake capability invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FakeCapabilityOutcome {
    /// Invocation succeeds.
    Success,
    /// Invocation returns an explicit failure classification.
    Failure {
        /// Retry class consumed by the worker policy.
        retry_class: RetryErrorClass,
        /// Whether the concrete remote effect is known not to have started.
        remote_effect: CapabilityRemoteEffect,
    },
}

/// Payload-free observation of one fake adapter invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FakeCapabilityInvocation {
    /// Routed versioned capability.
    pub capability: CapabilityId,
    /// Routed capability-local operation.
    pub operation: OperationName,
    /// Canonical worker message identity.
    pub message_id: MessageId,
    /// Canonical correlation identity.
    pub correlation_id: CorrelationId,
    /// One-based delivery attempt.
    pub attempt: u32,
    /// Encoded input size without retained bytes.
    pub payload_bytes: usize,
}

/// Current bounded fake capability state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FakeCapabilitySnapshot {
    /// Number of retained payload-free invocations.
    pub invocation_count: usize,
    /// Number of pending scripted outcomes.
    pub pending_outcomes: usize,
}

#[derive(Debug)]
struct FakeCapabilityState {
    invocations: Vec<FakeCapabilityInvocation>,
    outcomes: VecDeque<FakeCapabilityOutcome>,
}

#[derive(Debug)]
struct FakeCapabilityInner {
    config: FakeCapabilityConfig,
    state: Mutex<FakeCapabilityState>,
}

/// Cloneable deterministic capability adapter for consumer and integration tests.
#[derive(Clone)]
pub struct FakeCapabilityHandler {
    inner: Arc<FakeCapabilityInner>,
}

impl FakeCapabilityHandler {
    /// Creates an empty fake with validated explicit bounds.
    ///
    /// # Errors
    ///
    /// Returns an error when a configured capacity is invalid.
    pub fn new(config: FakeCapabilityConfig) -> Result<Self, FakeCapabilityError> {
        let config =
            FakeCapabilityConfig::new(config.invocation_capacity, config.outcome_capacity)?;
        Ok(Self {
            inner: Arc::new(FakeCapabilityInner {
                config,
                state: Mutex::new(FakeCapabilityState {
                    invocations: Vec::with_capacity(config.invocation_capacity),
                    outcomes: VecDeque::with_capacity(config.outcome_capacity),
                }),
            }),
        })
    }

    /// Appends one deterministic FIFO invocation outcome.
    ///
    /// # Errors
    ///
    /// Returns an error when the outcome script is at capacity.
    pub fn script(&self, outcome: FakeCapabilityOutcome) -> Result<(), FakeCapabilityError> {
        let mut state = self.lock();
        if state.outcomes.len() >= self.inner.config.outcome_capacity {
            return Err(FakeCapabilityError::new(
                FakeCapabilityErrorKind::OutcomeCapacityReached,
            ));
        }
        state.outcomes.push_back(outcome);
        Ok(())
    }

    /// Returns retained payload-free invocation records in call order.
    #[must_use]
    pub fn invocations(&self) -> Vec<FakeCapabilityInvocation> {
        self.lock().invocations.clone()
    }

    /// Returns current bounded fake state counts.
    #[must_use]
    pub fn snapshot(&self) -> FakeCapabilitySnapshot {
        let state = self.lock();
        FakeCapabilitySnapshot {
            invocation_count: state.invocations.len(),
            pending_outcomes: state.outcomes.len(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, FakeCapabilityState> {
        match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl Debug for FakeCapabilityHandler {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FakeCapabilityHandler")
            .field("config", &self.inner.config)
            .field("snapshot", &self.snapshot())
            .field("payloads", &"<not retained>")
            .finish()
    }
}

#[async_trait]
impl CapabilityHandler for FakeCapabilityHandler {
    async fn invoke(
        &self,
        context: WorkerContext,
        request: CapabilityRequest,
    ) -> Result<(), CapabilityFailure> {
        let outcome = {
            let mut state = self.lock();
            if state.invocations.len() >= self.inner.config.invocation_capacity {
                return Err(CapabilityFailure::new(
                    RetryErrorClass::Permanent,
                    CapabilityRemoteEffect::NotStarted,
                    FakeCapabilityError::new(FakeCapabilityErrorKind::InvocationCapacityReached),
                ));
            }
            state.invocations.push(FakeCapabilityInvocation {
                capability: request.capability().clone(),
                operation: request.operation().clone(),
                message_id: context.message_id,
                correlation_id: context.correlation_id,
                attempt: context.attempt,
                payload_bytes: request.input().len(),
            });
            state
                .outcomes
                .pop_front()
                .unwrap_or(FakeCapabilityOutcome::Success)
        };

        match outcome {
            FakeCapabilityOutcome::Success => Ok(()),
            FakeCapabilityOutcome::Failure {
                retry_class,
                remote_effect,
            } => Err(CapabilityFailure::new(
                retry_class,
                remote_effect,
                FakeCapabilityError::new(FakeCapabilityErrorKind::ScriptedFailure),
            )),
        }
    }
}

impl Default for FakeCapabilityHandler {
    fn default() -> Self {
        Self {
            inner: Arc::new(FakeCapabilityInner {
                config: FakeCapabilityConfig::default(),
                state: Mutex::new(FakeCapabilityState {
                    invocations: Vec::new(),
                    outcomes: VecDeque::new(),
                }),
            }),
        }
    }
}

/// Stable fake capability failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FakeCapabilityErrorKind {
    /// A zero bound was rejected.
    ZeroCapacity,
    /// A configured bound exceeds its hard maximum.
    CapacityAboveMaximum,
    /// Invocation history is full.
    InvocationCapacityReached,
    /// Scripted outcome storage is full.
    OutcomeCapacityReached,
    /// A scripted failure was consumed.
    ScriptedFailure,
}

/// Redaction-safe fake capability error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FakeCapabilityError {
    kind: FakeCapabilityErrorKind,
}

impl FakeCapabilityError {
    const fn new(kind: FakeCapabilityErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable fake failure category.
    #[must_use]
    pub const fn kind(self) -> FakeCapabilityErrorKind {
        self.kind
    }
}

impl Display for FakeCapabilityError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            FakeCapabilityErrorKind::ZeroCapacity => "fake capability capacities must be positive",
            FakeCapabilityErrorKind::CapacityAboveMaximum => {
                "fake capability capacity exceeds the hard maximum"
            }
            FakeCapabilityErrorKind::InvocationCapacityReached => {
                "fake capability invocation history is full"
            }
            FakeCapabilityErrorKind::OutcomeCapacityReached => {
                "fake capability outcome script is full"
            }
            FakeCapabilityErrorKind::ScriptedFailure => {
                "fake capability returned a scripted failure"
            }
        })
    }
}

impl Error for FakeCapabilityError {}
