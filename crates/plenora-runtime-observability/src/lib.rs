//! Backend-neutral runtime observability conventions and hooks.

#![forbid(unsafe_code)]

mod bridge;
mod conventions;
mod instruments;
mod labels;
mod metrics;
mod propagation;
mod redaction;
mod spans;

pub use bridge::{current_context, set_parent, span_context};
pub use conventions::{metric_names, span_fields, span_names};
pub use instruments::{
    BrokerMetrics, HealthMetrics, HealthState, LifecycleObservationDropReason, MessageFailureKind,
    OutboxMetrics, OutboxPublishDisposition, ReadinessState, RuntimeInstruments, TaskFailureKind,
    WorkerMetrics,
};
pub use labels::{
    LabelError, LabelErrorKind, MAX_LABEL_VALUE_LEN, MAX_LABELS, MetricLabel, MetricLabelKey,
    MetricLabels,
};
pub use metrics::{MetricSink, NoopMetricSink};
pub use propagation::{
    CORRELATION_ID_METADATA_KEY, MessageMetadataExtractor, MessageMetadataInjector,
    PropagationError, PropagationErrorKind, TRACEPARENT_METADATA_KEY, TRACESTATE_METADATA_KEY,
    extract_context, extract_correlation_id, inject_context, inject_correlation_id,
};
pub use redaction::{
    DefaultRedactionPolicy, RedactedText, RedactionPolicy, Sensitive, Sensitivity,
};
pub use spans::{broker_span, health_span, message_span, outbox_span, runtime_task_span};
