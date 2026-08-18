//! Contract tests for the persistence-neutral stores and deterministic fakes.

use std::{error::Error, io};

use chrono::{DateTime, Utc};
use plenora_runtime_messaging::{MessageId, SerializedMessage};
use plenora_runtime_outbox::{
    DeduplicationDecision, FailureDisposition, IdempotencyDecision, IdempotencyKey,
    IdempotencyStore, InMemoryIdempotencyStore, InMemoryInboxStore, InMemoryOutboxStore,
    InboxDeduplicator, InboxStore, MemoryStoreError, MemoryStoreOperation, OutboxEntry,
    OutboxEntryState, OutboxId, OutboxStore, PublishFailure, PublishFailureKind,
    RequestFingerprint,
};
use uuid::Uuid;

fn timestamp(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    Ok(DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc))
}

fn entry(id: u128, created_at: DateTime<Utc>) -> OutboxEntry {
    OutboxEntry {
        id: OutboxId::from_uuid(Uuid::from_u128(id)),
        message: SerializedMessage::new("application/octet-stream", vec![0_u8]),
        created_at,
        attempts: 0,
    }
}

#[tokio::test]
async fn outbox_claims_oldest_first_with_bounded_attempts() -> Result<(), Box<dyn Error>> {
    let store = InMemoryOutboxStore::new();
    let newer = entry(2, timestamp("2026-08-15T10:01:00Z")?);
    let older = entry(1, timestamp("2026-08-15T10:00:00Z")?);
    store.insert(newer.clone())?;
    store.insert(older.clone())?;

    let claimed = store.claim_pending(1).await?;

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, older.id);
    assert_eq!(claimed[0].attempts, 1);
    assert_eq!(
        store
            .snapshot(older.id)?
            .ok_or_else(|| io::Error::other("missing older snapshot"))?
            .state,
        OutboxEntryState::Claimed
    );
    assert_eq!(store.count_in_state(OutboxEntryState::Pending)?, 1);
    Ok(())
}

#[tokio::test]
async fn outbox_fault_injection_is_counted_and_deterministic() -> Result<(), Box<dyn Error>> {
    let store = InMemoryOutboxStore::new();
    store.inject_failure(MemoryStoreOperation::OutboxClaim, 1)?;

    let first = store.claim_pending(10).await;
    let second = store.claim_pending(10).await?;

    assert_eq!(
        first,
        Err(MemoryStoreError::Injected(
            MemoryStoreOperation::OutboxClaim
        ))
    );
    assert!(second.is_empty());
    Ok(())
}

#[tokio::test]
async fn inbox_deduplicator_reports_new_then_duplicate() -> Result<(), Box<dyn Error>> {
    let store = InMemoryInboxStore::new();
    let helper = InboxDeduplicator::new(store.clone());
    let message_id = MessageId::from_uuid(Uuid::from_u128(41));

    assert_eq!(
        helper.check(message_id).await?,
        DeduplicationDecision::Process
    );
    helper.record_processed(message_id).await?;
    assert_eq!(
        helper.check(message_id).await?,
        DeduplicationDecision::Duplicate
    );
    helper.record_processed(message_id).await?;
    assert_eq!(store.processed_count()?, 1);
    Ok(())
}

#[tokio::test]
async fn inbox_fault_does_not_record_processed_identity() -> Result<(), Box<dyn Error>> {
    let store = InMemoryInboxStore::new();
    let helper = InboxDeduplicator::new(store.clone());
    let message_id = MessageId::from_uuid(Uuid::from_u128(42));
    store.inject_failure(MemoryStoreOperation::InboxRecord, 1)?;

    let result = helper.record_processed(message_id).await;

    assert_eq!(
        result,
        Err(MemoryStoreError::Injected(
            MemoryStoreOperation::InboxRecord
        ))
    );
    assert_eq!(store.processed_count()?, 0);
    Ok(())
}

#[tokio::test]
async fn idempotency_store_distinguishes_lifecycle_and_conflicts() -> Result<(), Box<dyn Error>> {
    let store = InMemoryIdempotencyStore::new();
    let key = IdempotencyKey::new("sensitive-operation-key");
    let original = RequestFingerprint::new(Vec::from([1_u8, 2, 3]));
    let different = RequestFingerprint::new(Vec::from([9_u8, 8, 7]));

    assert_eq!(
        store.begin(key.clone(), original.clone()).await?,
        IdempotencyDecision::Execute
    );
    assert_eq!(
        store.begin(key.clone(), original.clone()).await?,
        IdempotencyDecision::InProgress
    );
    assert_eq!(
        store.begin(key.clone(), different).await?,
        IdempotencyDecision::Conflict
    );
    store.complete(&key)?;
    assert_eq!(
        store.begin(key, original).await?,
        IdempotencyDecision::ReturnStoredResult
    );
    Ok(())
}

#[test]
fn idempotency_debug_output_redacts_values() {
    let key = IdempotencyKey::new("never-print-this-key");
    let fingerprint = RequestFingerprint::new(Vec::from([1_u8, 2, 3, 4]));

    assert!(!format!("{key:?}").contains("never-print-this-key"));
    assert!(!format!("{fingerprint:?}").contains("1, 2, 3, 4"));
}

#[test]
fn opaque_identifiers_round_trip_through_adapter_accessors() {
    let uuid = Uuid::from_u128(500);
    let id = OutboxId::from_uuid(uuid);
    assert_eq!(id.as_uuid(), &uuid);
    assert_eq!(id.into_uuid(), uuid);
    assert_eq!(OutboxId::from(uuid), id);
    assert_eq!(Uuid::from(id), uuid);
    assert_ne!(OutboxId::random(), id);

    let key = IdempotencyKey::new("private-key");
    assert_eq!(key.as_str(), "private-key");
    assert_eq!(format!("{key:?}"), "IdempotencyKey(<redacted>)");

    let empty = RequestFingerprint::new(Vec::<u8>::new());
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    let fingerprint = RequestFingerprint::new(Vec::from([1_u8, 2, 3]));
    assert_eq!(fingerprint.as_bytes(), &[1, 2, 3]);
    assert_eq!(fingerprint.len(), 3);
    assert!(!fingerprint.is_empty());
}

#[test]
fn publish_failure_constructors_preserve_kind_and_disposition() {
    let producer = PublishFailure::producer_error(FailureDisposition::Retry);
    assert_eq!(producer.kind(), PublishFailureKind::ProducerError);
    assert_eq!(producer.disposition(), FailureDisposition::Retry);
    let unknown = PublishFailure::outcome_unknown(FailureDisposition::Reconcile);
    assert_eq!(unknown.kind(), PublishFailureKind::OutcomeUnknown);
    assert_eq!(unknown.disposition(), FailureDisposition::Reconcile);
}

#[tokio::test]
async fn invalid_outbox_transitions_and_missing_records_are_explicit() -> Result<(), Box<dyn Error>>
{
    let store = InMemoryOutboxStore::new();
    let pending = entry(600, timestamp("2026-08-15T10:00:00Z")?);
    assert_eq!(store.snapshot(pending.id)?, None);
    store.insert(pending.clone())?;
    let duplicate = store
        .insert(pending.clone())
        .err()
        .ok_or("duplicate outbox entry unexpectedly accepted")?;
    assert!(matches!(duplicate, MemoryStoreError::DuplicateOutbox(id) if id == pending.id));
    assert!(duplicate.to_string().contains("duplicate"));

    let unknown = OutboxId::from_uuid(Uuid::from_u128(601));
    assert!(matches!(
        store.mark_published(unknown).await,
        Err(MemoryStoreError::OutboxNotFound(id)) if id == unknown
    ));
    assert!(matches!(
        store
            .mark_failed(
                unknown,
                PublishFailure::producer_error(FailureDisposition::Retry)
            )
            .await,
        Err(MemoryStoreError::OutboxNotFound(id)) if id == unknown
    ));
    let invalid = store
        .mark_published(pending.id)
        .await
        .err()
        .ok_or("pending entry transitioned directly to published")?;
    assert!(matches!(
        invalid,
        MemoryStoreError::InvalidOutboxTransition {
            state: OutboxEntryState::Pending,
            ..
        }
    ));
    assert!(invalid.to_string().contains("cannot transition"));

    let claimed = store.claim_pending(1).await?;
    assert_eq!(claimed.len(), 1);
    store.mark_published(pending.id).await?;
    assert!(matches!(
        store
            .mark_failed(
                pending.id,
                PublishFailure::outcome_unknown(FailureDisposition::Terminal)
            )
            .await,
        Err(MemoryStoreError::InvalidOutboxTransition {
            state: OutboxEntryState::Published,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn every_memory_store_fault_hook_is_one_shot_and_observable() -> Result<(), Box<dyn Error>> {
    let outbox = InMemoryOutboxStore::new();
    let pending = entry(700, timestamp("2026-08-15T10:00:00Z")?);
    outbox.inject_failure(MemoryStoreOperation::OutboxInsert, 1)?;
    assert_eq!(
        outbox.insert(pending.clone()),
        Err(MemoryStoreError::Injected(
            MemoryStoreOperation::OutboxInsert
        ))
    );
    outbox.insert(pending.clone())?;
    let _claimed = outbox.claim_pending(1).await?;
    outbox.inject_failure(MemoryStoreOperation::OutboxMarkPublished, 1)?;
    assert_eq!(
        outbox.mark_published(pending.id).await,
        Err(MemoryStoreError::Injected(
            MemoryStoreOperation::OutboxMarkPublished
        ))
    );
    outbox.inject_failure(MemoryStoreOperation::OutboxMarkFailed, 1)?;
    assert_eq!(
        outbox
            .mark_failed(
                pending.id,
                PublishFailure::producer_error(FailureDisposition::Retry)
            )
            .await,
        Err(MemoryStoreError::Injected(
            MemoryStoreOperation::OutboxMarkFailed
        ))
    );

    let inbox = InMemoryInboxStore::new();
    let message_id = MessageId::random();
    inbox.inject_failure(MemoryStoreOperation::InboxContains, 1)?;
    assert_eq!(
        inbox.contains(message_id).await,
        Err(MemoryStoreError::Injected(
            MemoryStoreOperation::InboxContains
        ))
    );
    assert!(!inbox.contains(message_id).await?);

    let idempotency = InMemoryIdempotencyStore::new();
    let key = IdempotencyKey::new("operation");
    let fingerprint = RequestFingerprint::new(Vec::from([9_u8]));
    idempotency.inject_failure(MemoryStoreOperation::IdempotencyBegin, 1)?;
    assert_eq!(
        idempotency.begin(key.clone(), fingerprint.clone()).await,
        Err(MemoryStoreError::Injected(
            MemoryStoreOperation::IdempotencyBegin
        ))
    );
    assert_eq!(
        idempotency.begin(key.clone(), fingerprint).await?,
        IdempotencyDecision::Execute
    );
    idempotency.inject_failure(MemoryStoreOperation::IdempotencyComplete, 1)?;
    assert_eq!(
        idempotency.complete(&key),
        Err(MemoryStoreError::Injected(
            MemoryStoreOperation::IdempotencyComplete
        ))
    );
    idempotency.complete(&key)?;
    assert_eq!(
        idempotency.complete(&IdempotencyKey::new("missing")),
        Err(MemoryStoreError::IdempotencyKeyNotFound)
    );
    assert!(
        MemoryStoreError::IdempotencyKeyNotFound
            .to_string()
            .contains("not found")
    );
    Ok(())
}

#[tokio::test]
async fn deduplicator_accessors_return_the_owned_store() -> Result<(), Box<dyn Error>> {
    let helper = InboxDeduplicator::new(InMemoryInboxStore::new());
    assert_eq!(helper.store().processed_count()?, 0);
    let message_id = MessageId::random();
    helper.record_processed(message_id).await?;
    let store = helper.into_inner();
    assert_eq!(store.processed_count()?, 1);
    Ok(())
}
