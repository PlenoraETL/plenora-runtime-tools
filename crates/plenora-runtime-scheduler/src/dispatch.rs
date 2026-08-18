use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    time::SystemTime,
};

use async_trait::async_trait;

use crate::ScheduleOccurrenceId;

/// One immutable scheduled invocation passed to an application-owned dispatcher.
#[derive(Clone, Debug)]
pub struct ScheduledOccurrence<T> {
    /// Deterministic idempotency identity.
    pub id: ScheduleOccurrenceId,
    /// Logical due instant copied from the identity for convenient dispatch mapping.
    pub due_at: SystemTime,
    /// Application-owned command or payload.
    pub payload: T,
}

/// Certainty about the external effect of a failed dispatch attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScheduleDispatchEffect {
    /// The dispatcher guarantees that no external effect began.
    NotStarted,
    /// The dispatcher cannot prove whether the occurrence was accepted remotely.
    OutcomeUnknown,
}

/// Source-preserving dispatch failure with explicit effect certainty.
pub struct ScheduleDispatchError {
    effect: ScheduleDispatchEffect,
    source: Option<Box<dyn Error + Send + Sync>>,
}

impl ScheduleDispatchError {
    /// Creates an error without a concrete source.
    #[must_use]
    pub const fn new(effect: ScheduleDispatchEffect) -> Self {
        Self {
            effect,
            source: None,
        }
    }

    /// Creates an error that preserves its concrete source.
    #[must_use]
    pub fn with_source<E>(effect: ScheduleDispatchEffect, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            effect,
            source: Some(Box::new(source)),
        }
    }

    /// Returns external-effect certainty for safe retry or reconciliation.
    #[must_use]
    pub const fn effect(&self) -> ScheduleDispatchEffect {
        self.effect
    }
}

impl Display for ScheduleDispatchError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.effect {
            ScheduleDispatchEffect::NotStarted => "scheduled dispatch did not start",
            ScheduleDispatchEffect::OutcomeUnknown => "scheduled dispatch outcome is unknown",
        })
    }
}

impl Debug for ScheduleDispatchError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScheduleDispatchError")
            .field("effect", &self.effect)
            .field(
                "source",
                &self.source.as_ref().map(|_| "<preserved; redacted>"),
            )
            .finish()
    }
}

impl Error for ScheduleDispatchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

/// Application-owned boundary that publishes or invokes one scheduled occurrence.
#[async_trait]
pub trait ScheduleDispatcher<T>: Send + Sync {
    /// Dispatches one occurrence.
    ///
    /// The occurrence id must be used as an idempotency key by side-effecting implementations.
    ///
    /// # Errors
    ///
    /// Returns an explicitly classified failure; unknown outcomes are never blindly retried.
    async fn dispatch(
        &self,
        occurrence: ScheduledOccurrence<T>,
    ) -> Result<(), ScheduleDispatchError>;
}
