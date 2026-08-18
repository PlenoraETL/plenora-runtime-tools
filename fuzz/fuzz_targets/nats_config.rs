#![no_main]

use std::{sync::Arc, time::Duration};

use libfuzzer_sys::fuzz_target;
use plenora_runtime_messaging::DeliveryHeartbeatConfig;
use plenora_runtime_nats::{
    InfrastructureMode, JetStreamConsumerConfig, JetStreamProducerConfig, NatsConfig,
    ReplayConsumerConfig,
};

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data).into_owned();
    let number = read_u64(data, 0);
    let count = usize::try_from(number).unwrap_or(usize::MAX);
    let duration = Duration::from_nanos(read_u64(data, 8));
    let optional_count = data
        .get(16)
        .is_some_and(|value| value & 1 == 1)
        .then_some(count);
    let heartbeat = DeliveryHeartbeatConfig::new(duration, read_u32(data, 17)).ok();
    let infrastructure = if data.get(21).is_some_and(|value| value & 1 == 1) {
        InfrastructureMode::CreateIfMissing {
            stream_subjects: vec![Arc::from(text.clone())],
        }
    } else {
        InfrastructureMode::BindExisting
    };

    let mut connection = NatsConfig::default();
    connection.servers = if data.first().is_some_and(|value| value & 1 == 1) {
        vec![Arc::from(text.clone())]
    } else {
        Vec::new()
    };
    connection.connect_timeout = duration;
    connection.request_timeout = Duration::from_nanos(read_u64(data, 22));
    connection.client_capacity = count;
    connection.subscription_capacity = usize::try_from(read_u64(data, 30)).unwrap_or(usize::MAX);
    connection.health_component = Arc::from(text.clone());
    let _ = connection.validate();

    let producer = JetStreamProducerConfig {
        subject: Arc::from(text.clone()),
        max_payload_bytes: count,
        message_id_metadata_key: data
            .get(38)
            .is_some_and(|value| value & 1 == 1)
            .then(|| Arc::from(text.clone())),
    };
    let _ = producer.validate();

    let consumer = JetStreamConsumerConfig {
        stream: Arc::from(text.clone()),
        durable_name: Arc::from(text.clone()),
        filter_subject: Arc::from(text.clone()),
        ack_wait: duration,
        heartbeat,
        max_deliver: data
            .get(39)
            .is_some_and(|value| value & 1 == 1)
            .then_some(read_u32(data, 40)),
        max_ack_pending: optional_count,
        max_payload_bytes: count,
        shutdown_nak_delay: duration,
        infrastructure: infrastructure.clone(),
    };
    let _ = consumer.validate();

    let replay = ReplayConsumerConfig {
        stream: Arc::from(text.clone()),
        durable_name: Arc::from(text.clone()),
        operational_durable_name: if data.get(44).is_some_and(|value| value & 1 == 1) {
            Arc::from(format!("{text}-operational"))
        } else {
            Arc::from(text.clone())
        },
        filter_subject: Arc::from(text),
        ack_wait: duration,
        heartbeat,
        max_deliver: Some(read_u32(data, 45)),
        max_ack_pending: optional_count,
        max_payload_bytes: count,
        shutdown_nak_delay: duration,
        infrastructure,
    };
    let _ = replay.validate();
});

fn read_u32(data: &[u8], offset: usize) -> u32 {
    let mut bytes = [0_u8; 4];
    if let Some(source) = data.get(offset..offset.saturating_add(4)) {
        bytes.copy_from_slice(source);
    }
    u32::from_le_bytes(bytes)
}

fn read_u64(data: &[u8], offset: usize) -> u64 {
    let mut bytes = [0_u8; 8];
    if let Some(source) = data.get(offset..offset.saturating_add(8)) {
        bytes.copy_from_slice(source);
    }
    u64::from_le_bytes(bytes)
}
