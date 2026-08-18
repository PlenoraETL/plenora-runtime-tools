//! Deterministic tests for safe outbox publication semantics.

use std::{
    collections::VecDeque,
    error::Error,
    fmt::{self, Display, Formatter},
    io,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use plenora_runtime_messaging::{MessageProducer, PublishOutcome, SerializedMessage};
use plenora_runtime_outbox::{
    FailureDisposition, InMemoryOutboxStore, MemoryStoreOperation, OutboxEntry, OutboxEntryState,
    OutboxId, OutboxRelay, RelayConfig, RelayConfigError, RelayError, RelayPolicy,
    RelayStoreOperation,
};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScriptedProducerError {
    Injected,
    ScriptExhausted,
    LockPoisoned,
}

impl Display for ScriptedProducerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("scripted producer failure")
    }
}

impl Error for ScriptedProducerError {}

#[derive(Clone, Debug)]
struct ScriptedProducer {
    script: Arc<Mutex<VecDeque<Result<PublishOutcome, ScriptedProducerError>>>>,
    published: Arc<Mutex<Vec<SerializedMessage>>>,
}

impl ScriptedProducer {
    fn new(
        script: impl IntoIterator<Item = Result<PublishOutcome, ScriptedProducerError>>,
    ) -> Self {
        Self {
            script: Arc::new(Mutex::new(script.into_iter().collect())),
            published: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn published_count(&self) -> Result<usize, ScriptedProducerError> {
        self.published
            .lock()
            .map(|messages| messages.len())
            .map_err(|_| ScriptedProducerError::LockPoisoned)
    }
}

#[async_trait]
impl MessageProducer for ScriptedProducer {
    type Error = ScriptedProducerError;

    async fn publish(&self, message: SerializedMessage) -> Result<PublishOutcome, Self::Error> {
        self.published
            .lock()
            .map_err(|_| ScriptedProducerError::LockPoisoned)?
            .push(message);
        self.script
            .lock()
            .map_err(|_| ScriptedProducerError::LockPoisoned)?
            .pop_front()
            .ok_or(ScriptedProducerError::ScriptExhausted)?
    }
}

fn timestamp() -> Result<DateTime<Utc>, chrono::ParseError> {
    Ok(DateTime::parse_from_rfc3339("2026-08-15T10:00:00Z")?.with_timezone(&Utc))
}

fn entry(id: u128) -> Result<OutboxEntry, chrono::ParseError> {
    Ok(OutboxEntry {
        id: OutboxId::from_uuid(Uuid::from_u128(id)),
        message: SerializedMessage::new("application/octet-stream", vec![7_u8, 8, 9]),
        created_at: timestamp()?,
        attempts: 0,
    })
}

#[tokio::test]
async fn confirmed_publication_is_marked_published() -> Result<(), Box<dyn Error>> {
    let store = InMemoryOutboxStore::new();
    let pending = entry(1)?;
    store.insert(pending.clone())?;
    let producer = ScriptedProducer::new([Ok(PublishOutcome::Confirmed)]);
    let relay = OutboxRelay::new(store.clone(), producer.clone(), RelayConfig::default());

    let report = relay.run_once().await?;

    assert_eq!(report.claimed, 1);
    assert_eq!(report.published, 1);
    assert_eq!(producer.published_count()?, 1);
    assert_eq!(
        store
            .snapshot(pending.id)?
            .ok_or_else(|| io::Error::other("missing published snapshot"))?
            .state,
        OutboxEntryState::Published
    );
    Ok(())
}

#[tokio::test]
async fn unknown_outcome_defaults_to_reconciliation_without_blind_retry()
-> Result<(), Box<dyn Error>> {
    let store = InMemoryOutboxStore::new();
    let pending = entry(2)?;
    store.insert(pending.clone())?;
    let producer = ScriptedProducer::new([Ok(PublishOutcome::OutcomeUnknown)]);
    let relay = OutboxRelay::new(store.clone(), producer.clone(), RelayConfig::default());

    let first = relay.run_once().await?;
    let second = relay.run_once().await?;

    assert_eq!(first.awaiting_reconciliation, 1);
    assert_eq!(second.claimed, 0);
    assert_eq!(producer.published_count()?, 1);
    assert_eq!(
        store
            .snapshot(pending.id)?
            .ok_or_else(|| io::Error::other("missing reconciliation snapshot"))?
            .state,
        OutboxEntryState::AwaitingReconciliation
    );
    Ok(())
}

#[tokio::test]
async fn unknown_outcome_retry_requires_explicit_policy() -> Result<(), Box<dyn Error>> {
    let store = InMemoryOutboxStore::new();
    let pending = entry(3)?;
    store.insert(pending.clone())?;
    let producer = ScriptedProducer::new([Ok(PublishOutcome::OutcomeUnknown)]);
    let policy = RelayPolicy::new(FailureDisposition::Retry, FailureDisposition::Retry);
    let relay = OutboxRelay::new(store.clone(), producer, RelayConfig::new(10, policy)?);

    let report = relay.run_once().await?;

    assert_eq!(report.retry_scheduled, 1);
    assert_eq!(
        store
            .snapshot(pending.id)?
            .ok_or_else(|| io::Error::other("missing retry snapshot"))?
            .state,
        OutboxEntryState::Pending
    );
    Ok(())
}

#[tokio::test]
async fn producer_error_is_recorded_and_source_is_preserved() -> Result<(), Box<dyn Error>> {
    let store = InMemoryOutboxStore::new();
    let pending = entry(4)?;
    store.insert(pending.clone())?;
    let producer = ScriptedProducer::new([Err(ScriptedProducerError::Injected)]);
    let relay = OutboxRelay::new(store.clone(), producer, RelayConfig::default());

    let result = relay.run_once().await;

    assert!(matches!(
        result,
        Err(RelayError::Publish {
            source: ScriptedProducerError::Injected,
            ..
        })
    ));
    assert_eq!(
        store
            .snapshot(pending.id)?
            .ok_or_else(|| io::Error::other("missing producer-error snapshot"))?
            .state,
        OutboxEntryState::Pending
    );
    Ok(())
}

#[tokio::test]
async fn confirmed_remote_effect_and_store_failure_remain_explicit() -> Result<(), Box<dyn Error>> {
    let store = InMemoryOutboxStore::new();
    let pending = entry(5)?;
    store.insert(pending.clone())?;
    store.inject_failure(MemoryStoreOperation::OutboxMarkPublished, 1)?;
    let producer = ScriptedProducer::new([Ok(PublishOutcome::Confirmed)]);
    let relay = OutboxRelay::new(store.clone(), producer, RelayConfig::default());

    let result = relay.run_once().await;

    assert!(matches!(
        result,
        Err(RelayError::Store {
            operation: RelayStoreOperation::MarkPublished,
            ..
        })
    ));
    assert_eq!(
        store
            .snapshot(pending.id)?
            .ok_or_else(|| io::Error::other("missing claimed snapshot"))?
            .state,
        OutboxEntryState::Claimed
    );
    Ok(())
}

#[test]
fn relay_configuration_and_accessors_are_explicit() -> Result<(), Box<dyn Error>> {
    assert_eq!(RelayConfig::default().batch_size(), 100);
    assert_eq!(
        RelayConfig::default().policy(),
        RelayPolicy::new(FailureDisposition::Retry, FailureDisposition::Reconcile)
    );
    assert_eq!(
        RelayConfig::new(0, RelayPolicy::default()),
        Err(RelayConfigError::ZeroBatchSize)
    );
    assert!(
        RelayConfigError::ZeroBatchSize
            .to_string()
            .contains("positive")
    );

    let policy = RelayPolicy::new(FailureDisposition::Terminal, FailureDisposition::Retry);
    assert_eq!(policy.producer_error(), FailureDisposition::Terminal);
    assert_eq!(policy.outcome_unknown(), FailureDisposition::Retry);
    let config = RelayConfig::new(7, policy)?;
    assert_eq!(config.batch_size(), 7);
    assert_eq!(config.policy(), policy);

    let store = InMemoryOutboxStore::new();
    let producer = ScriptedProducer::new([]);
    let relay = OutboxRelay::new(store.clone(), producer.clone(), config);
    assert_eq!(relay.config(), config);
    assert_eq!(relay.store().count_in_state(OutboxEntryState::Pending)?, 0);
    assert_eq!(relay.producer().published_count()?, 0);
    assert!(format!("{relay:?}").contains("OutboxRelay"));
    Ok(())
}

#[tokio::test]
async fn claim_and_unknown_outcome_store_failures_preserve_operation_and_report()
-> Result<(), Box<dyn Error>> {
    let store = InMemoryOutboxStore::new();
    store.inject_failure(MemoryStoreOperation::OutboxClaim, 1)?;
    let relay = OutboxRelay::new(
        store.clone(),
        ScriptedProducer::new([]),
        RelayConfig::default(),
    );
    let error = relay
        .run_once()
        .await
        .err()
        .ok_or("claim failure unexpectedly succeeded")?;
    assert!(matches!(
        &error,
        RelayError::Store {
            operation: RelayStoreOperation::ClaimPending,
            ..
        }
    ));
    assert_eq!(error.report().claimed, 0);
    assert_eq!(error.outbox_id(), None);
    assert!(error.source().is_some());
    assert!(error.to_string().contains("ClaimPending"));
    assert!(format!("{error:?}").contains("<redacted>"));

    let pending = entry(11)?;
    store.insert(pending.clone())?;
    store.inject_failure(MemoryStoreOperation::OutboxMarkFailed, 1)?;
    let relay = OutboxRelay::new(
        store,
        ScriptedProducer::new([Ok(PublishOutcome::OutcomeUnknown)]),
        RelayConfig::default(),
    );
    let error = relay
        .run_once()
        .await
        .err()
        .ok_or("mark-failed injection unexpectedly succeeded")?;
    assert!(matches!(
        &error,
        RelayError::Store {
            operation: RelayStoreOperation::MarkFailed,
            ..
        }
    ));
    assert_eq!(error.report().claimed, 1);
    assert_eq!(error.outbox_id(), None);
    Ok(())
}

#[tokio::test]
async fn producer_and_store_failure_keeps_both_sources_and_pre_failure_report()
-> Result<(), Box<dyn Error>> {
    let store = InMemoryOutboxStore::new();
    let pending = entry(12)?;
    store.insert(pending.clone())?;
    store.inject_failure(MemoryStoreOperation::OutboxMarkFailed, 1)?;
    let relay = OutboxRelay::new(
        store,
        ScriptedProducer::new([Err(ScriptedProducerError::Injected)]),
        RelayConfig::default(),
    );

    let error = relay
        .run_once()
        .await
        .err()
        .ok_or("combined failure unexpectedly succeeded")?;
    assert_eq!(error.report().claimed, 1);
    assert_eq!(error.outbox_id(), Some(pending.id));
    assert!(error.source().is_some());
    assert!(
        error
            .to_string()
            .contains("publication and failure recording")
    );
    let diagnostics = format!("{error:?}");
    assert!(diagnostics.contains("publish_source"));
    assert!(diagnostics.contains("store_source"));
    assert!(!diagnostics.contains("scripted producer failure"));
    assert!(matches!(
        error,
        RelayError::PublishAndStore {
            publish_source: ScriptedProducerError::Injected,
            ..
        }
    ));
    Ok(())
}

#[tokio::test]
async fn every_failure_disposition_updates_the_matching_report_counter()
-> Result<(), Box<dyn Error>> {
    for (id, disposition, expected_retry, expected_reconcile, expected_terminal) in [
        (20, FailureDisposition::Retry, 1, 0, 0),
        (21, FailureDisposition::Reconcile, 0, 1, 0),
        (22, FailureDisposition::Terminal, 0, 0, 1),
    ] {
        let store = InMemoryOutboxStore::new();
        store.insert(entry(id)?)?;
        let policy = RelayPolicy::new(FailureDisposition::Retry, disposition);
        let relay = OutboxRelay::new(
            store,
            ScriptedProducer::new([Ok(PublishOutcome::OutcomeUnknown)]),
            RelayConfig::new(1, policy)?,
        );
        let report = relay.run_once().await?;
        assert_eq!(report.retry_scheduled, expected_retry);
        assert_eq!(report.awaiting_reconciliation, expected_reconcile);
        assert_eq!(report.terminal_failures, expected_terminal);
    }
    Ok(())
}

#[tokio::test]
async fn recorded_publish_error_exposes_partial_report_without_sensitive_source()
-> Result<(), Box<dyn Error>> {
    let store = InMemoryOutboxStore::new();
    let pending = entry(30)?;
    store.insert(pending.clone())?;
    let policy = RelayPolicy::new(FailureDisposition::Terminal, FailureDisposition::Reconcile);
    let relay = OutboxRelay::new(
        store,
        ScriptedProducer::new([Err(ScriptedProducerError::Injected)]),
        RelayConfig::new(1, policy)?,
    );
    let error = relay
        .run_once()
        .await
        .err()
        .ok_or("producer failure unexpectedly succeeded")?;
    assert_eq!(error.outbox_id(), Some(pending.id));
    assert_eq!(error.report().terminal_failures, 1);
    assert!(error.source().is_some());
    assert!(error.to_string().contains("publication failed"));
    assert!(!format!("{error:?}").contains("scripted producer failure"));
    Ok(())
}
