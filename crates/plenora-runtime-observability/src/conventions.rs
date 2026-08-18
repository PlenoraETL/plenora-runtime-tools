/// Stable span names emitted by runtime instrumentation.
pub mod span_names {
    /// Supervised runtime task span.
    pub const RUNTIME_TASK: &str = "plenora.runtime.task";
    /// Message processing span.
    pub const MESSAGE_PROCESS: &str = "plenora.message.process";
    /// Broker operation span.
    pub const BROKER_OPERATION: &str = "plenora.broker.operation";
    /// Outbox relay span.
    pub const OUTBOX_RELAY: &str = "plenora.outbox.relay";
    /// Health or readiness observation span.
    pub const HEALTH_OBSERVE: &str = "plenora.health.observe";
}

/// Stable field names for structured spans and events.
pub mod span_fields {
    /// Low-cardinality operation name.
    pub const OPERATION: &str = "plenora.operation";
    /// Runtime component name.
    pub const COMPONENT: &str = "plenora.component";
    /// Message identity. This belongs in spans, never metric labels.
    pub const MESSAGE_ID: &str = "messaging.message.id";
    /// Correlation identity. This belongs in spans, never metric labels.
    pub const CORRELATION_ID: &str = "messaging.correlation.id";
    /// Causation identity. This belongs in spans, never metric labels.
    pub const CAUSATION_ID: &str = "messaging.causation.id";
}

/// Stable backend-neutral metric names.
pub mod metric_names {
    /// Currently active supervised tasks.
    pub const RUNTIME_TASKS_ACTIVE: &str = "runtime_tasks_active";
    /// Failed supervised tasks.
    pub const RUNTIME_TASKS_FAILED_TOTAL: &str = "runtime_tasks_failed_total";
    /// Coordinated shutdown duration in seconds.
    pub const RUNTIME_SHUTDOWN_DURATION: &str = "runtime_shutdown_duration";
    /// Received messages.
    pub const MESSAGES_RECEIVED_TOTAL: &str = "messages_received_total";
    /// Successfully processed messages.
    pub const MESSAGES_PROCESSED_TOTAL: &str = "messages_processed_total";
    /// Failed message-processing attempts.
    pub const MESSAGES_FAILED_TOTAL: &str = "messages_failed_total";
    /// Retried message-processing attempts.
    pub const MESSAGES_RETRIED_TOTAL: &str = "messages_retried_total";
    /// Messages moved to a dead-letter destination.
    pub const MESSAGES_DEAD_LETTERED_TOTAL: &str = "messages_dead_lettered_total";
    /// Message-processing duration in seconds.
    pub const MESSAGE_PROCESSING_DURATION: &str = "message_processing_duration";
    /// Current number of queued worker lifecycle observations.
    pub const WORKER_LIFECYCLE_QUEUE_DEPTH: &str = "worker_lifecycle_queue_depth";
    /// Worker lifecycle observations dropped before delivery.
    pub const WORKER_LIFECYCLE_DROPPED_TOTAL: &str = "worker_lifecycle_dropped_total";
    /// Pending outbox entry count.
    pub const OUTBOX_PENDING: &str = "outbox_pending";
    /// Age of the oldest pending outbox entry in seconds.
    pub const OUTBOX_OLDEST_AGE: &str = "outbox_oldest_age";
    /// Confirmed outbox publications.
    pub const OUTBOX_PUBLISH_TOTAL: &str = "outbox_publish_total";
    /// Failed or uncertain outbox publications.
    pub const OUTBOX_PUBLISH_FAILED_TOTAL: &str = "outbox_publish_failed_total";
    /// Broker connection state, represented as zero or one.
    pub const BROKER_CONNECTED: &str = "broker_connected";
    /// Broker reconnection count.
    pub const BROKER_RECONNECT_TOTAL: &str = "broker_reconnect_total";
    /// Broker consumer lag.
    pub const CONSUMER_LAG: &str = "consumer_lag";
    /// Aggregate component health state.
    pub const RUNTIME_HEALTH: &str = "runtime_health";
    /// Aggregate component readiness state.
    pub const RUNTIME_READINESS: &str = "runtime_readiness";
}
