use plenora_runtime_messaging::{CausationId, CorrelationId, MessageId};
use tracing::{Span, field};

use crate::span_fields;

/// Creates a supervised runtime-task span.
#[must_use]
pub fn runtime_task_span(operation: &str) -> Span {
    tracing::info_span!(
        "plenora.runtime.task",
        "plenora.operation" = operation,
        "otel.kind" = "internal"
    )
}

/// Creates a message-processing consumer span.
#[must_use]
pub fn message_span(
    message_id: MessageId,
    correlation_id: CorrelationId,
    causation_id: Option<CausationId>,
) -> Span {
    let span = tracing::info_span!(
        "plenora.message.process",
        "messaging.message.id" = %message_id,
        "messaging.correlation.id" = %correlation_id,
        "messaging.causation.id" = field::Empty,
        "otel.kind" = "consumer"
    );
    if let Some(causation_id) = causation_id {
        span.record(
            span_fields::CAUSATION_ID,
            tracing::field::display(causation_id),
        );
    }
    span
}

/// Creates a broker operation span.
#[must_use]
pub fn broker_span(operation: &str) -> Span {
    tracing::info_span!(
        "plenora.broker.operation",
        "plenora.operation" = operation,
        "otel.kind" = "client"
    )
}

/// Creates an outbox relay span.
#[must_use]
pub fn outbox_span(operation: &str) -> Span {
    tracing::info_span!(
        "plenora.outbox.relay",
        "plenora.operation" = operation,
        "otel.kind" = "producer"
    )
}

/// Creates a health observation span.
#[must_use]
pub fn health_span(component: &str) -> Span {
    tracing::info_span!(
        "plenora.health.observe",
        "plenora.component" = component,
        "otel.kind" = "internal"
    )
}

#[cfg(test)]
mod tests {
    use plenora_runtime_messaging::{CausationId, CorrelationId, MessageId};

    use super::{broker_span, health_span, message_span, outbox_span, runtime_task_span};

    #[test]
    fn every_span_factory_is_safe_with_and_without_a_subscriber() {
        let message_id = MessageId::random();
        let correlation_id = CorrelationId::random();
        let causation_id = CausationId::random();

        drop(runtime_task_span("drain"));
        drop(message_span(message_id, correlation_id, None));
        drop(message_span(message_id, correlation_id, Some(causation_id)));
        drop(broker_span("publish"));
        drop(outbox_span("relay"));
        drop(health_span("nats"));
    }
}
