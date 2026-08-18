//! Persistence-neutral outbox, inbox, and idempotency contracts.

#![forbid(unsafe_code)]

mod dedup;
mod identifiers;
mod memory;
mod relay;
mod stores;

pub use dedup::{DeduplicationDecision, InboxDeduplicator};
pub use identifiers::{IdempotencyKey, OutboxId, RequestFingerprint};
pub use memory::{
    FakeIdempotencyStore, FakeInboxStore, FakeOutboxStore, InMemoryIdempotencyStore,
    InMemoryInboxStore, InMemoryOutboxStore, MemoryStoreError, MemoryStoreOperation,
    OutboxEntrySnapshot, OutboxEntryState,
};
pub use relay::{
    OutboxRelay, RelayBatchReport, RelayConfig, RelayConfigError, RelayError, RelayPolicy,
    RelayStoreOperation,
};
pub use stores::{
    FailureDisposition, IdempotencyDecision, IdempotencyStore, InboxStore, OutboxEntry,
    OutboxStore, PublishFailure, PublishFailureKind,
};
