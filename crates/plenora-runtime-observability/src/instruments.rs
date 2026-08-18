use std::{sync::Arc, time::Duration};

use crate::{MetricLabelKey, MetricLabels, MetricSink, metric_names};

/// Bounded runtime-task failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskFailureKind {
    /// Task returned an error.
    Error,
    /// Task panicked.
    Panic,
    /// Task was cancelled.
    Cancelled,
}

impl TaskFailureKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Panic => "panic",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Bounded message-processing failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageFailureKind {
    /// Message decoding failed.
    Decode,
    /// Handler execution failed.
    Handler,
    /// Broker acknowledgement failed.
    Acknowledge,
    /// Consumer rejected the delivery.
    Rejected,
}

/// Bounded reason for dropping a worker lifecycle observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleObservationDropReason {
    /// The in-memory handoff queue had no available slot.
    Full,
    /// The application-owned receiver was closed.
    Closed,
}

impl LifecycleObservationDropReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Closed => "closed",
        }
    }
}

impl MessageFailureKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Decode => "decode",
            Self::Handler => "handler",
            Self::Acknowledge => "acknowledge",
            Self::Rejected => "rejected",
        }
    }
}

/// Bounded outbox failure disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxPublishDisposition {
    /// Publication will be retried.
    Retry,
    /// Publication awaits explicit reconciliation.
    Reconcile,
    /// Publication failed permanently.
    Terminal,
    /// Remote publication effect is unknown.
    OutcomeUnknown,
}

impl OutboxPublishDisposition {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Retry => "retry",
            Self::Reconcile => "reconcile",
            Self::Terminal => "terminal",
            Self::OutcomeUnknown => "outcome_unknown",
        }
    }
}

/// Backend-neutral aggregate health state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HealthState {
    /// All observed components are healthy.
    Healthy,
    /// At least one observed component is degraded.
    Degraded,
    /// At least one observed component is unhealthy.
    Unhealthy,
}

impl HealthState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unhealthy => "unhealthy",
        }
    }

    const fn gauge(self) -> u64 {
        match self {
            Self::Healthy => 2,
            Self::Degraded => 1,
            Self::Unhealthy => 0,
        }
    }
}

/// Backend-neutral aggregate readiness state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadinessState {
    /// New work may be accepted.
    Ready,
    /// New work must not be accepted.
    NotReady,
}

impl ReadinessState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::NotReady => "not_ready",
        }
    }

    const fn gauge(self) -> u64 {
        match self {
            Self::Ready => 1,
            Self::NotReady => 0,
        }
    }
}

/// Instrumentation wrapper for runtime tasks and message workers.
pub struct WorkerMetrics<S: MetricSink + ?Sized> {
    sink: Arc<S>,
}

impl<S: MetricSink + ?Sized> WorkerMetrics<S> {
    /// Creates a worker instrumentation wrapper.
    #[must_use]
    pub fn new(sink: Arc<S>) -> Self {
        Self { sink }
    }

    /// Records the current active runtime task count.
    pub fn runtime_tasks_active(&self, active: u64) {
        self.sink.record_gauge(
            metric_names::RUNTIME_TASKS_ACTIVE,
            active,
            &MetricLabels::new(),
        );
    }

    /// Records one failed runtime task.
    pub fn runtime_task_failed(&self, kind: TaskFailureKind) {
        self.sink.increment_counter(
            metric_names::RUNTIME_TASKS_FAILED_TOTAL,
            1,
            &MetricLabels::one(MetricLabelKey::Reason, kind.as_str()),
        );
    }

    /// Records a coordinated shutdown duration.
    pub fn shutdown_duration(&self, duration: Duration) {
        self.sink.record_duration(
            metric_names::RUNTIME_SHUTDOWN_DURATION,
            duration,
            &MetricLabels::new(),
        );
    }

    /// Records one received message.
    pub fn message_received(&self) {
        self.sink.increment_counter(
            metric_names::MESSAGES_RECEIVED_TOTAL,
            1,
            &MetricLabels::new(),
        );
    }

    /// Records successful processing and its duration.
    pub fn message_processed(&self, duration: Duration) {
        let labels = MetricLabels::new();
        self.sink
            .increment_counter(metric_names::MESSAGES_PROCESSED_TOTAL, 1, &labels);
        self.sink
            .record_duration(metric_names::MESSAGE_PROCESSING_DURATION, duration, &labels);
    }

    /// Records one message-processing failure.
    pub fn message_failed(&self, kind: MessageFailureKind) {
        self.sink.increment_counter(
            metric_names::MESSAGES_FAILED_TOTAL,
            1,
            &MetricLabels::one(MetricLabelKey::Reason, kind.as_str()),
        );
    }

    /// Records one retry.
    pub fn message_retried(&self) {
        self.sink.increment_counter(
            metric_names::MESSAGES_RETRIED_TOTAL,
            1,
            &MetricLabels::new(),
        );
    }

    /// Records one dead-lettered message.
    pub fn message_dead_lettered(&self) {
        self.sink.increment_counter(
            metric_names::MESSAGES_DEAD_LETTERED_TOTAL,
            1,
            &MetricLabels::new(),
        );
    }

    /// Records current occupancy of the bounded worker lifecycle handoff.
    pub fn lifecycle_queue_depth(&self, queued: usize) {
        self.sink.record_gauge(
            metric_names::WORKER_LIFECYCLE_QUEUE_DEPTH,
            u64::try_from(queued).map_or(u64::MAX, std::convert::identity),
            &MetricLabels::new(),
        );
    }

    /// Adds newly observed lifecycle handoff drops to the monotonic counter.
    ///
    /// `count` must be the delta since the previous observation, not a cumulative snapshot.
    pub fn lifecycle_observations_dropped(
        &self,
        reason: LifecycleObservationDropReason,
        count: u64,
    ) {
        self.sink.increment_counter(
            metric_names::WORKER_LIFECYCLE_DROPPED_TOTAL,
            count,
            &MetricLabels::one(MetricLabelKey::Reason, reason.as_str()),
        );
    }
}

/// Instrumentation wrapper for broker connection and consumer state.
pub struct BrokerMetrics<S: MetricSink + ?Sized> {
    sink: Arc<S>,
}

impl<S: MetricSink + ?Sized> BrokerMetrics<S> {
    /// Creates a broker instrumentation wrapper.
    #[must_use]
    pub fn new(sink: Arc<S>) -> Self {
        Self { sink }
    }

    /// Records broker connectivity as zero or one.
    pub fn connected(&self, connected: bool) {
        self.sink.record_gauge(
            metric_names::BROKER_CONNECTED,
            u64::from(connected),
            &MetricLabels::new(),
        );
    }

    /// Records one successful broker reconnection.
    pub fn reconnected(&self) {
        self.sink.increment_counter(
            metric_names::BROKER_RECONNECT_TOTAL,
            1,
            &MetricLabels::new(),
        );
    }

    /// Records a broker-reported consumer lag.
    pub fn consumer_lag(&self, lag: u64) {
        self.sink
            .record_gauge(metric_names::CONSUMER_LAG, lag, &MetricLabels::new());
    }
}

/// Instrumentation wrapper for outbox relay state.
pub struct OutboxMetrics<S: MetricSink + ?Sized> {
    sink: Arc<S>,
}

impl<S: MetricSink + ?Sized> OutboxMetrics<S> {
    /// Creates an outbox instrumentation wrapper.
    #[must_use]
    pub fn new(sink: Arc<S>) -> Self {
        Self { sink }
    }

    /// Records the pending entry count and optional oldest age.
    pub fn backlog(&self, pending: u64, oldest_age: Option<Duration>) {
        let labels = MetricLabels::new();
        self.sink
            .record_gauge(metric_names::OUTBOX_PENDING, pending, &labels);
        if let Some(age) = oldest_age {
            self.sink
                .record_duration(metric_names::OUTBOX_OLDEST_AGE, age, &labels);
        }
    }

    /// Records confirmed publications.
    pub fn published(&self, count: u64) {
        self.sink.increment_counter(
            metric_names::OUTBOX_PUBLISH_TOTAL,
            count,
            &MetricLabels::new(),
        );
    }

    /// Records failed or uncertain publications by bounded disposition.
    pub fn publish_failed(&self, disposition: OutboxPublishDisposition, count: u64) {
        self.sink.increment_counter(
            metric_names::OUTBOX_PUBLISH_FAILED_TOTAL,
            count,
            &MetricLabels::one(MetricLabelKey::Disposition, disposition.as_str()),
        );
    }
}

/// Instrumentation wrapper for aggregate health and readiness.
pub struct HealthMetrics<S: MetricSink + ?Sized> {
    sink: Arc<S>,
}

impl<S: MetricSink + ?Sized> HealthMetrics<S> {
    /// Creates a health instrumentation wrapper.
    #[must_use]
    pub fn new(sink: Arc<S>) -> Self {
        Self { sink }
    }

    /// Records aggregate health.
    pub fn health(&self, state: HealthState) {
        self.sink.record_gauge(
            metric_names::RUNTIME_HEALTH,
            state.gauge(),
            &MetricLabels::one(MetricLabelKey::Status, state.as_str()),
        );
    }

    /// Records aggregate readiness.
    pub fn readiness(&self, state: ReadinessState) {
        self.sink.record_gauge(
            metric_names::RUNTIME_READINESS,
            state.gauge(),
            &MetricLabels::one(MetricLabelKey::Status, state.as_str()),
        );
    }
}

/// Complete set of backend-neutral runtime instruments sharing one sink.
pub struct RuntimeInstruments<S: MetricSink + ?Sized> {
    /// Worker and lifecycle metrics.
    pub worker: WorkerMetrics<S>,
    /// Broker metrics.
    pub broker: BrokerMetrics<S>,
    /// Outbox metrics.
    pub outbox: OutboxMetrics<S>,
    /// Health and readiness metrics.
    pub health: HealthMetrics<S>,
}

impl<S: MetricSink + ?Sized> RuntimeInstruments<S> {
    /// Creates all instrumentation wrappers over one shared sink.
    #[must_use]
    pub fn new(sink: Arc<S>) -> Self {
        Self {
            worker: WorkerMetrics::new(Arc::clone(&sink)),
            broker: BrokerMetrics::new(Arc::clone(&sink)),
            outbox: OutboxMetrics::new(Arc::clone(&sink)),
            health: HealthMetrics::new(sink),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex, MutexGuard},
        time::Duration,
    };

    use crate::{MetricLabelKey, MetricLabels, MetricSink, metric_names};

    use super::{
        HealthState, LifecycleObservationDropReason, MessageFailureKind, OutboxPublishDisposition,
        ReadinessState, RuntimeInstruments, TaskFailureKind, WorkerMetrics,
    };

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct MetricRecord {
        name: &'static str,
        value: u64,
        reason: Option<String>,
    }

    #[derive(Debug, Default)]
    struct RecordingSink {
        records: Mutex<Vec<MetricRecord>>,
    }

    impl RecordingSink {
        fn records(&self) -> Vec<MetricRecord> {
            lock(&self.records).clone()
        }

        fn push(&self, name: &'static str, value: u64, labels: &MetricLabels) {
            let reason = labels
                .iter()
                .find(|label| label.key() == MetricLabelKey::Reason)
                .map(|label| label.value().to_owned());
            lock(&self.records).push(MetricRecord {
                name,
                value,
                reason,
            });
        }
    }

    impl MetricSink for RecordingSink {
        fn increment_counter(&self, name: &'static str, amount: u64, labels: &MetricLabels) {
            self.push(name, amount, labels);
        }

        fn record_gauge(&self, name: &'static str, value: u64, labels: &MetricLabels) {
            self.push(name, value, labels);
        }

        fn record_duration(&self, _name: &'static str, _value: Duration, _labels: &MetricLabels) {}
    }

    #[test]
    fn worker_metrics_record_lifecycle_depth_and_drop_deltas() {
        let sink = Arc::new(RecordingSink::default());
        let metrics = WorkerMetrics::new(Arc::clone(&sink));

        metrics.lifecycle_queue_depth(7);
        metrics.lifecycle_observations_dropped(LifecycleObservationDropReason::Full, 3);
        metrics.lifecycle_observations_dropped(LifecycleObservationDropReason::Closed, 1);

        assert_eq!(
            sink.records(),
            vec![
                MetricRecord {
                    name: metric_names::WORKER_LIFECYCLE_QUEUE_DEPTH,
                    value: 7,
                    reason: None,
                },
                MetricRecord {
                    name: metric_names::WORKER_LIFECYCLE_DROPPED_TOTAL,
                    value: 3,
                    reason: Some(String::from("full")),
                },
                MetricRecord {
                    name: metric_names::WORKER_LIFECYCLE_DROPPED_TOTAL,
                    value: 1,
                    reason: Some(String::from("closed")),
                },
            ]
        );
    }

    #[test]
    fn complete_runtime_instruments_emit_every_bounded_category() {
        let sink = Arc::new(RecordingSink::default());
        let metrics = RuntimeInstruments::new(Arc::clone(&sink));

        metrics.worker.runtime_tasks_active(3);
        for kind in [
            TaskFailureKind::Error,
            TaskFailureKind::Panic,
            TaskFailureKind::Cancelled,
        ] {
            metrics.worker.runtime_task_failed(kind);
        }
        metrics.worker.shutdown_duration(Duration::from_millis(5));
        metrics.worker.message_received();
        metrics.worker.message_processed(Duration::from_millis(7));
        for kind in [
            MessageFailureKind::Decode,
            MessageFailureKind::Handler,
            MessageFailureKind::Acknowledge,
            MessageFailureKind::Rejected,
        ] {
            metrics.worker.message_failed(kind);
        }
        metrics.worker.message_retried();
        metrics.worker.message_dead_lettered();
        metrics.worker.lifecycle_queue_depth(usize::MAX);
        metrics
            .worker
            .lifecycle_observations_dropped(LifecycleObservationDropReason::Full, 2);
        metrics
            .worker
            .lifecycle_observations_dropped(LifecycleObservationDropReason::Closed, 1);

        metrics.broker.connected(true);
        metrics.broker.connected(false);
        metrics.broker.reconnected();
        metrics.broker.consumer_lag(11);

        metrics.outbox.backlog(4, None);
        metrics.outbox.backlog(4, Some(Duration::from_millis(13)));
        metrics.outbox.published(2);
        for disposition in [
            OutboxPublishDisposition::Retry,
            OutboxPublishDisposition::Reconcile,
            OutboxPublishDisposition::Terminal,
            OutboxPublishDisposition::OutcomeUnknown,
        ] {
            metrics.outbox.publish_failed(disposition, 1);
        }

        for state in [
            HealthState::Healthy,
            HealthState::Degraded,
            HealthState::Unhealthy,
        ] {
            metrics.health.health(state);
        }
        for state in [ReadinessState::Ready, ReadinessState::NotReady] {
            metrics.health.readiness(state);
        }

        assert!(sink.records().len() >= 30);
    }

    fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        match mutex.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}
