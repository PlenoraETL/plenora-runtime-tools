use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    time::Duration,
};

use async_nats::jetstream::{
    self, AckKind,
    consumer::{AckPolicy, DeliverPolicy, PullConsumer, pull},
};
use async_trait::async_trait;
use futures_util::StreamExt as _;
use plenora_runtime_messaging::{
    AckError, AckOperation, Delivery, DeliveryAcknowledger, DeliveryHeartbeatConfig,
    MessageConsumer, MessageMetadata, NackReason,
};

use crate::{
    InfrastructureMode, JetStreamConsumerConfig, NatsAdapterError, NatsErrorCategory,
    NatsOperation, ReplayConsumerConfig, metadata,
};

/// Durable `JetStream` pull consumer.
pub struct JetStreamConsumer {
    messages: pull::Stream,
    shutdown_nak_delay: Duration,
    max_payload_bytes: usize,
    heartbeat: Option<DeliveryHeartbeatConfig>,
    rejected_deliveries: u64,
    last_rejection: Option<NatsAdapterError>,
}

impl JetStreamConsumer {
    /// Returns how many malformed deliveries had a TERM request sent without stopping consumption.
    ///
    /// A protocol-level rejection sends TERM and continues consumption. A failed send is
    /// returned as an error; successful sending does not attest server confirmation.
    #[must_use]
    pub const fn rejected_deliveries(&self) -> u64 {
        self.rejected_deliveries
    }

    /// Returns the most recent redaction-safe rejection reason, when one was observed.
    #[must_use]
    pub const fn last_rejection(&self) -> Option<&NatsAdapterError> {
        self.last_rejection.as_ref()
    }

    pub(crate) async fn operational(
        context: &jetstream::Context,
        config: JetStreamConsumerConfig,
    ) -> Result<Self, NatsAdapterError> {
        config.validate().map_err(config_error)?;
        open(
            context,
            ConsumerSettings {
                stream: &config.stream,
                durable_name: &config.durable_name,
                filter_subject: &config.filter_subject,
                ack_wait: config.ack_wait,
                heartbeat: config.heartbeat,
                max_deliver: config.max_deliver,
                max_ack_pending: config.max_ack_pending,
                max_payload_bytes: config.max_payload_bytes,
                shutdown_nak_delay: config.shutdown_nak_delay,
                infrastructure: &config.infrastructure,
                deliver_policy: DeliverPolicy::All,
            },
        )
        .await
    }

    pub(crate) async fn replay(
        context: &jetstream::Context,
        config: ReplayConsumerConfig,
        deliver_policy: DeliverPolicy,
    ) -> Result<Self, NatsAdapterError> {
        config.validate().map_err(config_error)?;
        open(
            context,
            ConsumerSettings {
                stream: &config.stream,
                durable_name: &config.durable_name,
                filter_subject: &config.filter_subject,
                ack_wait: config.ack_wait,
                heartbeat: config.heartbeat,
                max_deliver: config.max_deliver,
                max_ack_pending: config.max_ack_pending,
                max_payload_bytes: config.max_payload_bytes,
                shutdown_nak_delay: config.shutdown_nak_delay,
                infrastructure: &config.infrastructure,
                deliver_policy,
            },
        )
        .await
    }
}

#[async_trait]
impl MessageConsumer for JetStreamConsumer {
    type Error = NatsAdapterError;

    async fn receive(&mut self) -> Result<Option<Delivery>, Self::Error> {
        loop {
            let Some(received) = self.messages.next().await else {
                return Ok(None);
            };
            let message = received.map_err(|error| {
                NatsAdapterError::with_source(
                    NatsErrorCategory::Connection,
                    NatsOperation::Receive,
                    "JetStream delivery polling failed",
                    error,
                )
            })?;
            match convert_delivery(
                message,
                self.shutdown_nak_delay,
                self.max_payload_bytes,
                self.heartbeat,
            )
            .await?
            {
                ConvertedDelivery::Accepted(delivery) => return Ok(Some(*delivery)),
                ConvertedDelivery::Rejected(rejection) => {
                    self.rejected_deliveries = self.rejected_deliveries.saturating_add(1);
                    self.last_rejection = Some(rejection);
                }
            }
        }
    }
}

struct ConsumerSettings<'a> {
    stream: &'a str,
    durable_name: &'a str,
    filter_subject: &'a str,
    ack_wait: Duration,
    heartbeat: Option<DeliveryHeartbeatConfig>,
    max_deliver: Option<u32>,
    max_ack_pending: Option<usize>,
    max_payload_bytes: usize,
    shutdown_nak_delay: Duration,
    infrastructure: &'a InfrastructureMode,
    deliver_policy: DeliverPolicy,
}

async fn open(
    context: &jetstream::Context,
    settings: ConsumerSettings<'_>,
) -> Result<JetStreamConsumer, NatsAdapterError> {
    let max_deliver = finite_max_deliver(settings.max_deliver)?;
    let max_ack_pending = finite_max_ack_pending(settings.max_ack_pending)?;
    let stream = match settings.infrastructure {
        InfrastructureMode::BindExisting => context
            .get_stream(settings.stream)
            .await
            .map_err(|error| infrastructure("configured JetStream stream is unavailable", error))?,
        InfrastructureMode::CreateIfMissing { stream_subjects } => context
            .get_or_create_stream(jetstream::stream::Config {
                name: settings.stream.to_owned(),
                subjects: stream_subjects.iter().map(ToString::to_string).collect(),
                ..Default::default()
            })
            .await
            .map_err(|error| infrastructure("JetStream stream provisioning failed", error))?,
    };

    let config = pull::Config {
        durable_name: Some(settings.durable_name.to_owned()),
        deliver_policy: settings.deliver_policy,
        ack_policy: AckPolicy::Explicit,
        ack_wait: settings.ack_wait,
        max_deliver,
        filter_subject: settings.filter_subject.to_owned(),
        max_ack_pending,
        ..Default::default()
    };
    let consumer: PullConsumer = match settings.infrastructure {
        InfrastructureMode::BindExisting => stream
            .get_consumer(settings.durable_name)
            .await
            .map_err(|error| {
                infrastructure(
                    "configured durable JetStream consumer is unavailable",
                    error,
                )
            })?,
        InfrastructureMode::CreateIfMissing { .. } => stream
            .get_or_create_consumer(settings.durable_name, config)
            .await
            .map_err(|error| infrastructure("JetStream consumer provisioning failed", error))?,
    };
    validate_bound_consumer(&consumer, &settings)?;
    let messages = consumer
        .messages()
        .await
        .map_err(|error| infrastructure("JetStream pull stream could not be opened", error))?;
    Ok(JetStreamConsumer {
        messages,
        shutdown_nak_delay: settings.shutdown_nak_delay,
        max_payload_bytes: settings.max_payload_bytes,
        heartbeat: settings.heartbeat,
        rejected_deliveries: 0,
        last_rejection: None,
    })
}

fn finite_max_deliver(value: Option<u32>) -> Result<i64, NatsAdapterError> {
    value
        .map(i64::from)
        .ok_or_else(missing_finite_consumer_bound)
}

fn finite_max_ack_pending(value: Option<usize>) -> Result<i64, NatsAdapterError> {
    let value = value.ok_or_else(missing_finite_consumer_bound)?;
    i64::try_from(value).map_err(|error| {
        NatsAdapterError::with_source(
            NatsErrorCategory::Configuration,
            NatsOperation::Bind,
            "NATS consumer bound exceeds the JetStream integer range",
            error,
        )
    })
}

fn missing_finite_consumer_bound() -> NatsAdapterError {
    NatsAdapterError::new(
        NatsErrorCategory::Configuration,
        NatsOperation::Bind,
        "NATS consumer requires finite delivery and pending-ack bounds",
    )
}

fn validate_bound_consumer(
    consumer: &PullConsumer,
    expected: &ConsumerSettings<'_>,
) -> Result<(), NatsAdapterError> {
    let actual = &consumer.cached_info().config;
    let expected_max_deliver = finite_max_deliver(expected.max_deliver)?;
    let expected_max_ack_pending = finite_max_ack_pending(expected.max_ack_pending)?;
    if actual.durable_name.as_deref() != Some(expected.durable_name)
        || actual.filter_subject != expected.filter_subject
        || actual.ack_policy != AckPolicy::Explicit
        || actual.ack_wait != expected.ack_wait
        || actual.max_deliver != expected_max_deliver
        || actual.max_ack_pending != expected_max_ack_pending
        || actual.deliver_policy != expected.deliver_policy
    {
        return Err(NatsAdapterError::new(
            NatsErrorCategory::Infrastructure,
            NatsOperation::Bind,
            "existing JetStream consumer configuration is incompatible with the requested bounds or delivery policy",
        ));
    }
    Ok(())
}

/// Outcome of converting one raw `JetStream` delivery.
enum ConvertedDelivery {
    /// A well-formed delivery ready for handler execution.
    Accepted(Box<Delivery>),
    /// A malformed delivery whose TERM request was sent; consumption continues.
    Rejected(NatsAdapterError),
}

/// Owned `JetStream` delivery facts copied before the message is split.
struct DeliveryFacts {
    attempt: u32,
    stream: String,
    consumer: String,
    stream_sequence: u64,
    consumer_sequence: u64,
    pending: u64,
}

fn delivery_facts(message: &jetstream::Message) -> Result<DeliveryFacts, NatsAdapterError> {
    let info = message.info().map_err(|error| {
        NatsAdapterError::with_source(
            NatsErrorCategory::Protocol,
            NatsOperation::Receive,
            "JetStream delivery metadata is invalid",
            error,
        )
    })?;
    let attempt = u32::try_from(info.delivered).map_err(|error| {
        NatsAdapterError::with_source(
            NatsErrorCategory::Protocol,
            NatsOperation::Receive,
            "JetStream delivery attempt is outside the supported range",
            error,
        )
    })?;
    Ok(DeliveryFacts {
        attempt,
        stream: info.stream.to_owned(),
        consumer: info.consumer.to_owned(),
        stream_sequence: info.stream_sequence,
        consumer_sequence: info.consumer_sequence,
        pending: info.pending,
    })
}

fn broker_metadata(
    subject: &impl ToString,
    facts: &DeliveryFacts,
) -> Result<MessageMetadata, NatsAdapterError> {
    let mut metadata = MessageMetadata::new();
    insert_text(&mut metadata, "plenora.nats.subject", subject)?;
    insert_text(&mut metadata, "plenora.nats.stream", &facts.stream)?;
    insert_text(&mut metadata, "plenora.nats.consumer", &facts.consumer)?;
    insert_text(
        &mut metadata,
        "plenora.nats.stream_sequence",
        &facts.stream_sequence,
    )?;
    insert_text(
        &mut metadata,
        "plenora.nats.consumer_sequence",
        &facts.consumer_sequence,
    )?;
    insert_text(&mut metadata, "plenora.nats.pending", &facts.pending)?;
    Ok(metadata)
}

/// Sends TERM for one malformed delivery before continuing consumption.
///
/// A failed send is escalated. Successful sending is not a server confirmation;
/// durable settlement qualification remains tracked in docs/security-review.md.
async fn reject_delivery(
    acknowledger: impl DeliveryAcknowledger + 'static,
    rejection: NatsAdapterError,
) -> Result<ConvertedDelivery, NatsAdapterError> {
    Box::new(acknowledger)
        .nack(NackReason::ConsumerRejected)
        .await
        .map_err(|error| {
            NatsAdapterError::with_source(
                NatsErrorCategory::Broker,
                NatsOperation::Receive,
                "malformed JetStream delivery could not be terminated",
                error,
            )
        })?;
    Ok(ConvertedDelivery::Rejected(rejection))
}

async fn convert_delivery(
    message: jetstream::Message,
    shutdown_nak_delay: Duration,
    max_payload_bytes: usize,
    heartbeat: Option<DeliveryHeartbeatConfig>,
) -> Result<ConvertedDelivery, NatsAdapterError> {
    let facts = delivery_facts(&message);
    let (message, acker) = message.split();
    convert_parts(
        message,
        facts,
        max_payload_bytes,
        heartbeat,
        NatsAcknowledger {
            acker,
            shutdown_nak_delay,
        },
    )
    .await
}

// Operational and replay consumers validate transport admission before exposing
// a Delivery, independently of the application decoder used by their caller.
async fn convert_parts(
    message: async_nats::Message,
    facts: Result<DeliveryFacts, NatsAdapterError>,
    max_payload_bytes: usize,
    heartbeat: Option<DeliveryHeartbeatConfig>,
    acknowledger: impl DeliveryAcknowledger + 'static,
) -> Result<ConvertedDelivery, NatsAdapterError> {
    let facts = match facts {
        Ok(facts) => facts,
        Err(rejection) => return reject_delivery(acknowledger, rejection).await,
    };
    let broker_metadata = match broker_metadata(&message.subject, &facts) {
        Ok(metadata) => metadata,
        Err(rejection) => return reject_delivery(acknowledger, rejection).await,
    };
    let attempt = facts.attempt;
    let message = match metadata::decode_delivery(
        message.headers.as_ref(),
        message.payload,
        max_payload_bytes,
    ) {
        Ok(message) => message,
        Err(rejection) => return reject_delivery(acknowledger, rejection).await,
    };
    Ok(ConvertedDelivery::Accepted(Box::new(match heartbeat {
        Some(config) => {
            Delivery::new_with_heartbeat(message, attempt, broker_metadata, config, acknowledger)
        }
        None => Delivery::new(message, attempt, broker_metadata, acknowledger),
    })))
}

fn insert_text(
    metadata: &mut MessageMetadata,
    key: &'static str,
    value: &impl ToString,
) -> Result<(), NatsAdapterError> {
    metadata
        .insert_text(key, value.to_string())
        .map_err(|error| {
            NatsAdapterError::with_source(
                NatsErrorCategory::Protocol,
                NatsOperation::Receive,
                "broker metadata key is invalid",
                error,
            )
        })?;
    Ok(())
}

struct NatsAcknowledger {
    acker: jetstream::message::Acker,
    shutdown_nak_delay: Duration,
}

#[async_trait]
impl DeliveryAcknowledger for NatsAcknowledger {
    async fn heartbeat(&mut self) -> Result<(), AckError> {
        self.acker
            .ack_with(AckKind::Progress)
            .await
            .map_err(|error| {
                AckError::with_source(
                    AckOperation::Heartbeat,
                    "JetStream delivery heartbeat failed",
                    BoxedAckSource(error),
                )
            })
    }

    async fn ack(self: Box<Self>) -> Result<(), AckError> {
        self.acker.double_ack().await.map_err(|error| {
            AckError::with_source(
                AckOperation::Ack,
                "JetStream positive acknowledgement failed",
                BoxedAckSource(error),
            )
        })
    }

    async fn nack(self: Box<Self>, reason: NackReason) -> Result<(), AckError> {
        let kind = match reason {
            NackReason::Retryable => AckKind::Nak(None),
            NackReason::RetryAfter(delay) => AckKind::Nak(Some(delay)),
            NackReason::Shutdown => AckKind::Nak(Some(self.shutdown_nak_delay)),
            NackReason::Permanent | NackReason::ConsumerRejected => AckKind::Term,
        };
        self.acker.ack_with(kind).await.map_err(|error| {
            AckError::with_source(
                AckOperation::Nack,
                "JetStream negative acknowledgement failed",
                BoxedAckSource(error),
            )
        })
    }
}

struct BoxedAckSource(Box<dyn Error + Send + Sync + 'static>);

impl Debug for BoxedAckSource {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("BoxedAckSource([REDACTED])")
    }
}

impl Display for BoxedAckSource {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("NATS acknowledgement source error")
    }
}

impl Error for BoxedAckSource {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}

fn config_error(error: crate::NatsConfigError) -> NatsAdapterError {
    NatsAdapterError::with_source(
        NatsErrorCategory::Configuration,
        NatsOperation::Bind,
        "invalid NATS consumer configuration",
        error,
    )
}

fn infrastructure<E>(message: &'static str, source: E) -> NatsAdapterError
where
    E: Into<Box<dyn Error + Send + Sync + 'static>>,
{
    NatsAdapterError::with_source(
        NatsErrorCategory::Infrastructure,
        NatsOperation::Bind,
        message,
        source,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use bytes::Bytes;

    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    struct RecordingAcknowledger {
        events: Arc<Mutex<Vec<&'static str>>>,
        fail: bool,
    }

    #[async_trait]
    impl DeliveryAcknowledger for RecordingAcknowledger {
        async fn ack(self: Box<Self>) -> Result<(), AckError> {
            self.events
                .lock()
                .map_err(|_| AckError::new(AckOperation::Ack, "test event lock failed"))?
                .push("ack");
            Ok(())
        }

        async fn nack(self: Box<Self>, reason: NackReason) -> Result<(), AckError> {
            assert_eq!(reason, NackReason::ConsumerRejected);
            self.events
                .lock()
                .map_err(|_| AckError::new(AckOperation::Nack, "test event lock failed"))?
                .push("term");
            if self.fail {
                Err(AckError::new(
                    AckOperation::Nack,
                    "injected settlement failure",
                ))
            } else {
                Ok(())
            }
        }
    }

    fn facts() -> DeliveryFacts {
        DeliveryFacts {
            attempt: 1,
            stream: "STREAM".into(),
            consumer: "CONSUMER".into(),
            stream_sequence: 1,
            consumer_sequence: 1,
            pending: 0,
        }
    }

    fn wire(payload: Bytes, headers: Option<async_nats::HeaderMap>) -> async_nats::Message {
        async_nats::Message {
            subject: "test.input".into(),
            reply: None,
            length: payload.len(),
            payload,
            headers,
            status: None,
            description: None,
        }
    }

    fn poisoned_deliveries() -> Vec<async_nats::Message> {
        let mut malformed = async_nats::HeaderMap::new();
        malformed.insert("Plenora-Content-Type-B64", "not!valid!base64");
        let mut duplicate = async_nats::HeaderMap::new();
        duplicate.append("Plenora-Content-Type-B64", "eA");
        duplicate.append("Plenora-Content-Type-B64", "eA");
        let mut raw = async_nats::HeaderMap::new();
        raw.insert(
            format!("Plenora-Meta-{}", URL_SAFE_NO_PAD.encode("invalid key")),
            "eA",
        );
        let mut content_type = async_nats::HeaderMap::new();
        content_type.insert(
            "Plenora-Content-Type-B64",
            URL_SAFE_NO_PAD
                .encode("x".repeat(plenora_runtime_messaging::MAX_METADATA_KEY_BYTES + 1)),
        );
        vec![
            wire(Bytes::from(vec![7; 4096]), None),
            wire(Bytes::from_static(b"poison"), Some(malformed)),
            wire(Bytes::from_static(b"poison"), Some(duplicate)),
            wire(Bytes::from_static(b"poison"), Some(raw)),
            wire(Bytes::from_static(b"poison"), Some(content_type)),
        ]
    }

    #[tokio::test]
    async fn consumer_transport_parity_rejects_poison_then_exposes_valid_delivery() -> TestResult {
        for poison in poisoned_deliveries() {
            let events = Arc::new(Mutex::new(Vec::new()));
            let mut accepted = 0;
            let mut rejected = 0;
            // Exercise the same conversion and settlement used by receive/replay,
            // without relying on a downstream boundary decoder to catch poison.
            for message in [poison, wire(Bytes::from_static(b"small-payload"), None)] {
                match convert_parts(
                    message,
                    Ok(facts()),
                    64,
                    None,
                    RecordingAcknowledger {
                        events: events.clone(),
                        fail: false,
                    },
                )
                .await?
                {
                    ConvertedDelivery::Rejected(error) => {
                        assert_eq!(error.category(), NatsErrorCategory::Protocol);
                        rejected += 1;
                    }
                    ConvertedDelivery::Accepted(delivery) => {
                        assert_eq!(delivery.message.bytes.as_ref(), b"small-payload");
                        assert_eq!(
                            *events.lock().map_err(|_| "test event lock failed")?,
                            ["term"]
                        );
                        assert_eq!(delivery.attempt, 1);
                        assert_eq!(
                            delivery.broker_metadata.get_text("plenora.nats.subject")?,
                            Some("test.input")
                        );
                        accepted += 1;
                        delivery.ack().await?;
                    }
                }
            }
            assert_eq!((rejected, accepted), (1, 1));
            assert_eq!(
                *events.lock().map_err(|_| "test event lock failed")?,
                ["term", "ack"]
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn consumer_transport_parity_propagates_failed_termination() -> TestResult {
        for poison in poisoned_deliveries() {
            let events = Arc::new(Mutex::new(Vec::new()));
            let outcome = convert_parts(
                poison,
                Ok(facts()),
                64,
                None,
                RecordingAcknowledger {
                    events: events.clone(),
                    fail: true,
                },
            )
            .await;
            let error = outcome
                .err()
                .ok_or("failed termination reported as success")?;
            assert_eq!(error.category(), NatsErrorCategory::Broker);
            assert_eq!(
                *events.lock().map_err(|_| "test event lock failed")?,
                ["term"]
            );
        }
        Ok(())
    }
}
