use std::time::Duration;

use crate::MetricLabels;

/// Backend-neutral metric recording boundary.
///
/// Implementations must not retain references after a method returns. Metric names are stable
/// library constants and labels have already passed bounded, redaction-safe validation.
pub trait MetricSink: Send + Sync {
    /// Adds a non-negative amount to a monotonic counter.
    fn increment_counter(&self, name: &'static str, amount: u64, labels: &MetricLabels);

    /// Records the current value of a gauge.
    fn record_gauge(&self, name: &'static str, value: u64, labels: &MetricLabels);

    /// Records a duration histogram sample.
    fn record_duration(&self, name: &'static str, value: Duration, labels: &MetricLabels);
}

/// Metric sink that intentionally discards every observation.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopMetricSink;

impl MetricSink for NoopMetricSink {
    fn increment_counter(&self, _name: &'static str, _amount: u64, _labels: &MetricLabels) {}

    fn record_gauge(&self, _name: &'static str, _value: u64, _labels: &MetricLabels) {}

    fn record_duration(&self, _name: &'static str, _value: Duration, _labels: &MetricLabels) {}
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{MetricSink, NoopMetricSink};
    use crate::MetricLabels;

    #[test]
    fn noop_sink_accepts_every_observation_shape() {
        let sink = NoopMetricSink;
        let labels = MetricLabels::new();
        sink.increment_counter("counter", 1, &labels);
        sink.record_gauge("gauge", 2, &labels);
        sink.record_duration("duration", Duration::from_millis(3), &labels);
    }
}
