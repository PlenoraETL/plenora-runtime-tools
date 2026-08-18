use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::{self, Display, Formatter},
    sync::{Arc, Mutex, MutexGuard},
};

use async_trait::async_trait;
use plenora_runtime_messaging::MessageId;

use crate::{
    FailureDisposition, IdempotencyDecision, IdempotencyKey, IdempotencyStore, InboxStore,
    OutboxEntry, OutboxId, OutboxStore, PublishFailure, RequestFingerprint,
};

/// Operation that can be failed deterministically by an in-memory store.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MemoryStoreOperation {
    /// Insert an outbox record through the fake-specific helper.
    OutboxInsert,
    /// Claim pending outbox records.
    OutboxClaim,
    /// Mark an outbox record as published.
    OutboxMarkPublished,
    /// Mark an outbox record as failed.
    OutboxMarkFailed,
    /// Read an inbox processed marker.
    InboxContains,
    /// Write an inbox processed marker.
    InboxRecord,
    /// Begin an idempotent operation.
    IdempotencyBegin,
    /// Complete an idempotent operation through the fake-specific helper.
    IdempotencyComplete,
}

/// Deterministic error returned by the in-memory stores.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryStoreError {
    /// A configured one-shot failure was consumed.
    Injected(MemoryStoreOperation),
    /// A mutex was poisoned by a panicking test thread.
    LockPoisoned,
    /// An outbox identity was inserted more than once.
    DuplicateOutbox(OutboxId),
    /// An outbox identity was not found.
    OutboxNotFound(OutboxId),
    /// An outbox update was not valid for its current state.
    InvalidOutboxTransition {
        /// Affected outbox record.
        id: OutboxId,
        /// State observed before the rejected transition.
        state: OutboxEntryState,
    },
    /// An idempotency key was not found by the fake completion helper.
    IdempotencyKeyNotFound,
}

impl Display for MemoryStoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Injected(operation) => write!(formatter, "injected failure for {operation:?}"),
            Self::LockPoisoned => formatter.write_str("in-memory store lock is poisoned"),
            Self::DuplicateOutbox(id) => write!(formatter, "duplicate outbox entry {id:?}"),
            Self::OutboxNotFound(id) => write!(formatter, "outbox entry {id:?} was not found"),
            Self::InvalidOutboxTransition { id, state } => {
                write!(
                    formatter,
                    "outbox entry {id:?} cannot transition from {state:?}"
                )
            }
            Self::IdempotencyKeyNotFound => formatter.write_str("idempotency key was not found"),
        }
    }
}

impl Error for MemoryStoreError {}

#[derive(Debug, Default)]
struct FaultPlan {
    remaining: HashMap<MemoryStoreOperation, usize>,
}

impl FaultPlan {
    fn inject(&mut self, operation: MemoryStoreOperation, times: usize) {
        self.remaining.insert(operation, times);
    }

    fn check(&mut self, operation: MemoryStoreOperation) -> Result<(), MemoryStoreError> {
        let Some(remaining) = self.remaining.get_mut(&operation) else {
            return Ok(());
        };

        if *remaining == 0 {
            return Ok(());
        }

        *remaining -= 1;
        Err(MemoryStoreError::Injected(operation))
    }
}

/// Observable lifecycle state of an in-memory outbox record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxEntryState {
    /// Eligible for a relay claim.
    Pending,
    /// Claimed by a relay run.
    Claimed,
    /// Confirmed by the broker and recorded as published.
    Published,
    /// Removed from automatic relay pending reconciliation.
    AwaitingReconciliation,
    /// Permanently failed.
    Failed,
}

/// Read-only snapshot exposed by the in-memory outbox fake.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxEntrySnapshot {
    /// Current entry value, including its latest attempt counter.
    pub entry: OutboxEntry,
    /// Current lifecycle state.
    pub state: OutboxEntryState,
    /// Most recently recorded failure, if any.
    pub last_failure: Option<PublishFailure>,
}

#[derive(Clone, Debug)]
struct OutboxRecord {
    entry: OutboxEntry,
    state: OutboxEntryState,
    last_failure: Option<PublishFailure>,
}

#[derive(Debug, Default)]
struct OutboxMemoryState {
    records: HashMap<OutboxId, OutboxRecord>,
    faults: FaultPlan,
}

/// Thread-safe deterministic outbox fake with operation-level fault injection.
#[derive(Clone, Debug, Default)]
pub struct InMemoryOutboxStore {
    state: Arc<Mutex<OutboxMemoryState>>,
}

impl InMemoryOutboxStore {
    /// Creates an empty in-memory outbox.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a pending entry, simulating the transactional append owned by a consumer.
    ///
    /// # Errors
    ///
    /// Returns an injected, locking, or duplicate identity error.
    pub fn insert(&self, entry: OutboxEntry) -> Result<(), MemoryStoreError> {
        let mut state = lock(&self.state)?;
        state.faults.check(MemoryStoreOperation::OutboxInsert)?;
        if state.records.contains_key(&entry.id) {
            return Err(MemoryStoreError::DuplicateOutbox(entry.id));
        }
        state.records.insert(
            entry.id,
            OutboxRecord {
                entry,
                state: OutboxEntryState::Pending,
                last_failure: None,
            },
        );
        Ok(())
    }

    /// Returns an immutable snapshot of one record.
    ///
    /// # Errors
    ///
    /// Returns a locking error if the fake state is unavailable.
    pub fn snapshot(&self, id: OutboxId) -> Result<Option<OutboxEntrySnapshot>, MemoryStoreError> {
        let state = lock(&self.state)?;
        Ok(state.records.get(&id).map(|record| OutboxEntrySnapshot {
            entry: record.entry.clone(),
            state: record.state,
            last_failure: record.last_failure,
        }))
    }

    /// Returns the number of records in a lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns a locking error if the fake state is unavailable.
    pub fn count_in_state(&self, expected: OutboxEntryState) -> Result<usize, MemoryStoreError> {
        let state = lock(&self.state)?;
        Ok(state
            .records
            .values()
            .filter(|record| record.state == expected)
            .count())
    }

    /// Makes an operation fail a deterministic number of times.
    ///
    /// Setting times to zero clears the effective failure without changing other plans.
    ///
    /// # Errors
    ///
    /// Returns a locking error if the fake state is unavailable.
    pub fn inject_failure(
        &self,
        operation: MemoryStoreOperation,
        times: usize,
    ) -> Result<(), MemoryStoreError> {
        let mut state = lock(&self.state)?;
        state.faults.inject(operation, times);
        Ok(())
    }
}

#[async_trait]
impl OutboxStore for InMemoryOutboxStore {
    type Error = MemoryStoreError;

    async fn claim_pending(&self, limit: usize) -> Result<Vec<OutboxEntry>, Self::Error> {
        let mut state = lock(&self.state)?;
        state.faults.check(MemoryStoreOperation::OutboxClaim)?;

        let mut identities: Vec<_> = state
            .records
            .iter()
            .filter(|(_, record)| record.state == OutboxEntryState::Pending)
            .map(|(id, record)| (*id, record.entry.created_at))
            .collect();
        identities.sort_unstable_by(|left, right| left.1.cmp(&right.1).then(left.0.cmp(&right.0)));
        identities.truncate(limit);

        let mut claimed = Vec::with_capacity(identities.len());
        for (id, _) in identities {
            if let Some(record) = state.records.get_mut(&id) {
                record.state = OutboxEntryState::Claimed;
                record.entry.attempts = record.entry.attempts.saturating_add(1);
                claimed.push(record.entry.clone());
            }
        }
        Ok(claimed)
    }

    async fn mark_published(&self, id: OutboxId) -> Result<(), Self::Error> {
        let mut state = lock(&self.state)?;
        state
            .faults
            .check(MemoryStoreOperation::OutboxMarkPublished)?;
        let record = state
            .records
            .get_mut(&id)
            .ok_or(MemoryStoreError::OutboxNotFound(id))?;

        match record.state {
            OutboxEntryState::Claimed => {
                record.state = OutboxEntryState::Published;
                record.last_failure = None;
                Ok(())
            }
            OutboxEntryState::Published => Ok(()),
            observed => Err(MemoryStoreError::InvalidOutboxTransition {
                id,
                state: observed,
            }),
        }
    }

    async fn mark_failed(&self, id: OutboxId, failure: PublishFailure) -> Result<(), Self::Error> {
        let mut state = lock(&self.state)?;
        state.faults.check(MemoryStoreOperation::OutboxMarkFailed)?;
        let record = state
            .records
            .get_mut(&id)
            .ok_or(MemoryStoreError::OutboxNotFound(id))?;

        if record.state != OutboxEntryState::Claimed {
            return Err(MemoryStoreError::InvalidOutboxTransition {
                id,
                state: record.state,
            });
        }

        record.state = match failure.disposition() {
            FailureDisposition::Retry => OutboxEntryState::Pending,
            FailureDisposition::Reconcile => OutboxEntryState::AwaitingReconciliation,
            FailureDisposition::Terminal => OutboxEntryState::Failed,
        };
        record.last_failure = Some(failure);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct InboxMemoryState {
    processed: HashSet<MessageId>,
    faults: FaultPlan,
}

/// Thread-safe deterministic inbox fake.
#[derive(Clone, Debug, Default)]
pub struct InMemoryInboxStore {
    state: Arc<Mutex<InboxMemoryState>>,
}

impl InMemoryInboxStore {
    /// Creates an empty in-memory inbox.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of distinct processed message identities.
    ///
    /// # Errors
    ///
    /// Returns a locking error if the fake state is unavailable.
    pub fn processed_count(&self) -> Result<usize, MemoryStoreError> {
        Ok(lock(&self.state)?.processed.len())
    }

    /// Makes an inbox operation fail a deterministic number of times.
    ///
    /// # Errors
    ///
    /// Returns a locking error if the fake state is unavailable.
    pub fn inject_failure(
        &self,
        operation: MemoryStoreOperation,
        times: usize,
    ) -> Result<(), MemoryStoreError> {
        let mut state = lock(&self.state)?;
        state.faults.inject(operation, times);
        Ok(())
    }
}

#[async_trait]
impl InboxStore for InMemoryInboxStore {
    type Error = MemoryStoreError;

    async fn contains(&self, message_id: MessageId) -> Result<bool, Self::Error> {
        let mut state = lock(&self.state)?;
        state.faults.check(MemoryStoreOperation::InboxContains)?;
        Ok(state.processed.contains(&message_id))
    }

    async fn record_processed(&self, message_id: MessageId) -> Result<(), Self::Error> {
        let mut state = lock(&self.state)?;
        state.faults.check(MemoryStoreOperation::InboxRecord)?;
        state.processed.insert(message_id);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IdempotencyState {
    InProgress,
    Completed,
}

#[derive(Clone, Debug)]
struct IdempotencyRecord {
    fingerprint: RequestFingerprint,
    state: IdempotencyState,
}

#[derive(Debug, Default)]
struct IdempotencyMemoryState {
    records: HashMap<IdempotencyKey, IdempotencyRecord>,
    faults: FaultPlan,
}

/// Thread-safe deterministic idempotency fake.
#[derive(Clone, Debug, Default)]
pub struct InMemoryIdempotencyStore {
    state: Arc<Mutex<IdempotencyMemoryState>>,
}

impl InMemoryIdempotencyStore {
    /// Creates an empty in-memory idempotency store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks an in-progress key as completed so subsequent calls return stored-result status.
    ///
    /// This helper changes only fake state because the first public trait intentionally leaves
    /// result storage to a future persistence-adapter API.
    ///
    /// # Errors
    ///
    /// Returns an injected, locking, or missing-key error.
    pub fn complete(&self, key: &IdempotencyKey) -> Result<(), MemoryStoreError> {
        let mut state = lock(&self.state)?;
        state
            .faults
            .check(MemoryStoreOperation::IdempotencyComplete)?;
        let record = state
            .records
            .get_mut(key)
            .ok_or(MemoryStoreError::IdempotencyKeyNotFound)?;
        record.state = IdempotencyState::Completed;
        Ok(())
    }

    /// Makes an idempotency operation fail a deterministic number of times.
    ///
    /// # Errors
    ///
    /// Returns a locking error if the fake state is unavailable.
    pub fn inject_failure(
        &self,
        operation: MemoryStoreOperation,
        times: usize,
    ) -> Result<(), MemoryStoreError> {
        let mut state = lock(&self.state)?;
        state.faults.inject(operation, times);
        Ok(())
    }
}

#[async_trait]
impl IdempotencyStore for InMemoryIdempotencyStore {
    type Error = MemoryStoreError;

    async fn begin(
        &self,
        key: IdempotencyKey,
        fingerprint: RequestFingerprint,
    ) -> Result<IdempotencyDecision, Self::Error> {
        let mut state = lock(&self.state)?;
        state.faults.check(MemoryStoreOperation::IdempotencyBegin)?;

        match state.records.get(&key) {
            Some(record) if record.fingerprint != fingerprint => Ok(IdempotencyDecision::Conflict),
            Some(record) if record.state == IdempotencyState::InProgress => {
                Ok(IdempotencyDecision::InProgress)
            }
            Some(_) => Ok(IdempotencyDecision::ReturnStoredResult),
            None => {
                state.records.insert(
                    key,
                    IdempotencyRecord {
                        fingerprint,
                        state: IdempotencyState::InProgress,
                    },
                );
                Ok(IdempotencyDecision::Execute)
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>, MemoryStoreError> {
    mutex.lock().map_err(|_| MemoryStoreError::LockPoisoned)
}

/// Test-oriented alias for the in-memory outbox store.
pub type FakeOutboxStore = InMemoryOutboxStore;

/// Test-oriented alias for the in-memory inbox store.
pub type FakeInboxStore = InMemoryInboxStore;

/// Test-oriented alias for the in-memory idempotency store.
pub type FakeIdempotencyStore = InMemoryIdempotencyStore;
