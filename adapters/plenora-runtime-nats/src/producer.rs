use std::{str, sync::Arc};

use async_nats::jetstream::{self, context::PublishErrorKind};
use async_trait::async_trait;
use plenora_runtime_messaging::{MessageProducer, PublishOutcome, SerializedMessage};

use crate::{
    JetStreamProducerConfig, NatsAdapterError, NatsErrorCategory, NatsOperation, metadata,
};

/// Fixed-subject `JetStream` producer.
#[derive(Clone)]
pub struct JetStreamProducer {
    context: jetstream::Context,
    config: Arc<JetStreamProducerConfig>,
}

impl JetStreamProducer {
    pub(crate) fn new(
        context: jetstream::Context,
        config: JetStreamProducerConfig,
    ) -> Result<Self, NatsAdapterError> {
        config.validate().map_err(|error| {
            NatsAdapterError::with_source(
                NatsErrorCategory::Configuration,
                NatsOperation::Publish,
                "invalid NATS producer configuration",
                error,
            )
        })?;
        Ok(Self {
            context,
            config: Arc::new(config),
        })
    }
}

#[async_trait]
impl MessageProducer for JetStreamProducer {
    type Error = NatsAdapterError;

    async fn publish(&self, message: SerializedMessage) -> Result<PublishOutcome, Self::Error> {
        if message.len() > self.config.max_payload_bytes {
            return Err(NatsAdapterError::new(
                NatsErrorCategory::Protocol,
                NatsOperation::Publish,
                "message exceeds the configured NATS payload limit",
            ));
        }

        let headers = metadata::encode(&message.content_type, &message.headers)?;
        let mut publication = jetstream::message::PublishMessage::build()
            .payload(message.bytes)
            .headers(headers);
        if let Some(key) = self.config.message_id_metadata_key.as_deref()
            && let Some(value) = message.headers.get(key)
        {
            let message_id = str::from_utf8(value).map_err(|error| {
                NatsAdapterError::with_source(
                    NatsErrorCategory::Protocol,
                    NatsOperation::Publish,
                    "JetStream message ID metadata is not UTF-8",
                    error,
                )
            })?;
            publication = publication.message_id(message_id);
        }

        let acknowledgement = self
            .context
            .send_publish(self.config.subject.to_string(), publication)
            .await
            .map_err(|error| {
                NatsAdapterError::with_source(
                    NatsErrorCategory::Connection,
                    NatsOperation::Publish,
                    "NATS publication could not be sent",
                    error,
                )
            })?;
        match acknowledgement.await {
            Ok(_) => Ok(PublishOutcome::Confirmed),
            Err(error)
                if matches!(
                    error.kind(),
                    PublishErrorKind::TimedOut
                        | PublishErrorKind::BrokenPipe
                        | PublishErrorKind::Other
                ) =>
            {
                Ok(PublishOutcome::OutcomeUnknown)
            }
            Err(error) => Err(NatsAdapterError::with_source(
                NatsErrorCategory::Broker,
                NatsOperation::Publish,
                "JetStream rejected the publication",
                error,
            )),
        }
    }
}
