//! Dead-letter sink contract tests.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::{Arc, Mutex, MutexGuard},
    time::SystemTime,
};

use async_trait::async_trait;
use plenora_runtime_messaging::{
    DEAD_LETTER_ATTEMPTS_METADATA_KEY, DEAD_LETTER_FAILED_AT_METADATA_KEY,
    DEAD_LETTER_ID_METADATA_KEY, DEAD_LETTER_REASON_METADATA_KEY, DeadLetter,
    DeadLetterPublishErrorKind, DeadLetterSink, MESSAGE_ID_METADATA_KEY, MessageMetadata,
    MessageProducer, PublishOutcome, SerializedMessage,
};

#[derive(Clone, Copy, Debug)]
struct TestProducerError;

impl Display for TestProducerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("sensitive producer detail")
    }
}

impl Error for TestProducerError {}

#[derive(Debug)]
struct RecordingProducer {
    result: Result<PublishOutcome, TestProducerError>,
    messages: Mutex<Vec<SerializedMessage>>,
}

impl RecordingProducer {
    const fn new(result: Result<PublishOutcome, TestProducerError>) -> Self {
        Self {
            result,
            messages: Mutex::new(Vec::new()),
        }
    }

    fn messages(&self) -> Vec<SerializedMessage> {
        lock(&self.messages).clone()
    }
}

#[async_trait]
impl MessageProducer for RecordingProducer {
    type Error = TestProducerError;

    async fn publish(&self, message: SerializedMessage) -> Result<PublishOutcome, Self::Error> {
        lock(&self.messages).push(message);
        self.result
    }
}

#[tokio::test]
async fn a_confirmed_sink_adds_bounded_portable_metadata() -> Result<(), Box<dyn Error>> {
    let producer = RecordingProducer::new(Ok(PublishOutcome::Confirmed));
    let outcome = producer
        .publish_dead_letter(dead_letter("handler_failed")?)
        .await?;

    assert_eq!(outcome, PublishOutcome::Confirmed);
    let messages = producer.messages();
    let message = messages.first().ok_or(TestProducerError)?;
    assert_eq!(
        message.headers.get_text(DEAD_LETTER_REASON_METADATA_KEY)?,
        Some("handler_failed")
    );
    assert_eq!(
        message.headers.get_text(DEAD_LETTER_ID_METADATA_KEY)?,
        Some("message-1.dlq")
    );
    assert_eq!(
        message
            .headers
            .get_text(DEAD_LETTER_ATTEMPTS_METADATA_KEY)?,
        Some("3")
    );
    assert!(
        message
            .headers
            .get_text(DEAD_LETTER_FAILED_AT_METADATA_KEY)?
            .is_some()
    );
    assert_eq!(message.bytes.as_ref(), b"payload");
    Ok(())
}

#[tokio::test]
async fn outcome_unknown_is_preserved_for_the_consumer_adapter() -> Result<(), Box<dyn Error>> {
    let producer = RecordingProducer::new(Ok(PublishOutcome::OutcomeUnknown));
    let outcome = producer
        .publish_dead_letter(dead_letter("handler_failed")?)
        .await?;

    assert_eq!(outcome, PublishOutcome::OutcomeUnknown);
    Ok(())
}

#[tokio::test]
async fn producer_error_preserves_source_but_redacts_diagnostics() -> Result<(), Box<dyn Error>> {
    let producer = RecordingProducer::new(Err(TestProducerError));
    let error = producer
        .publish_dead_letter(dead_letter("handler_failed")?)
        .await
        .err()
        .ok_or(TestProducerError)?;
    let debug = format!("{error:?}");

    assert_eq!(error.kind(), DeadLetterPublishErrorKind::Publication);
    assert!(error.source().is_some());
    assert!(!debug.contains("sensitive producer detail"));
    assert!(!error.to_string().contains("sensitive producer detail"));
    Ok(())
}

fn dead_letter(reason: &'static str) -> Result<DeadLetter, Box<dyn Error>> {
    let mut headers = MessageMetadata::new();
    let _previous = headers.insert_text(MESSAGE_ID_METADATA_KEY, "message-1")?;
    Ok(DeadLetter {
        message: SerializedMessage::new("application/octet-stream", "payload")
            .with_headers(headers),
        reason: Arc::from(reason),
        attempts: 3,
        failed_at: SystemTime::UNIX_EPOCH.into(),
    })
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
