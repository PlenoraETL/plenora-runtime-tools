use async_nats::jetstream::consumer::DeliverPolicy;
use plenora_runtime_messaging::{BrokerCapabilities, ReplaySource};

/// Returns the conservative capability set guaranteed by this adapter.
#[must_use]
pub const fn capabilities() -> BrokerCapabilities {
    BrokerCapabilities {
        durable_consumers: true,
        replay: true,
        ordered_delivery: false,
        dead_letter_native: false,
        exactly_once_claimed: false,
    }
}

pub(crate) fn delivery_policy(source: &ReplaySource) -> DeliverPolicy {
    match source {
        ReplaySource::All => DeliverPolicy::All,
        ReplaySource::FromSequence(sequence) => DeliverPolicy::ByStartSequence {
            start_sequence: *sequence,
        },
        ReplaySource::FromTimestamp(timestamp) => DeliverPolicy::ByStartTime {
            start_time: *timestamp,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::capabilities;

    #[test]
    fn capabilities_are_conservative() {
        let capabilities = capabilities();
        assert!(capabilities.durable_consumers);
        assert!(capabilities.replay);
        assert!(!capabilities.ordered_delivery);
        assert!(!capabilities.dead_letter_native);
        assert!(!capabilities.exactly_once_claimed);
    }
}
