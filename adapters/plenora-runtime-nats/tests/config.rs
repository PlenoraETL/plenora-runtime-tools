//! Public configuration and redaction contract tests.

use std::{error::Error, path::PathBuf, sync::Arc, time::Duration};

use plenora_runtime_messaging::DeliveryHeartbeatConfig;

use plenora_runtime_nats::{
    ClientCertificate, InfrastructureMode, JetStreamConsumerConfig, JetStreamProducerConfig,
    NatsAdapterError, NatsConfig, NatsConnection, NatsCredentials, NatsErrorCategory,
    NatsOperation, NatsTlsConfig, ReplayConsumerConfig, SecretString, TlsMode,
};

#[test]
fn defaults_require_tls_and_external_provisioning() -> Result<(), Box<dyn Error>> {
    let connection = NatsConfig::default();
    assert_eq!(connection.tls.mode, TlsMode::Required);
    assert_eq!(connection.max_reconnects, Some(10));
    let consumer = consumer_config()?;
    assert_eq!(consumer.infrastructure, InfrastructureMode::BindExisting);
    Ok(())
}

#[test]
fn debug_output_redacts_credentials_and_server_urls() {
    let config = NatsConfig {
        servers: vec![Arc::from("nats://user:secret@broker:4222")],
        credentials: NatsCredentials::Token(SecretString::new("token-value")),
        ..NatsConfig::default()
    };
    let output = format!("{config:?}");
    assert!(!output.contains("secret"));
    assert!(!output.contains("token-value"));
    assert!(!output.contains("broker"));
    assert!(output.contains("[REDACTED]"));
}

#[test]
fn explicit_provisioning_requires_subjects() -> Result<(), Box<dyn Error>> {
    let mut config = consumer_config()?;
    config.infrastructure = InfrastructureMode::CreateIfMissing {
        stream_subjects: Vec::new(),
    };
    let error = config.validate();
    assert!(error.is_err());
    Ok(())
}

#[test]
fn consumer_requires_finite_delivery_pending_ack_and_payload_bounds() -> Result<(), Box<dyn Error>>
{
    let mut config = consumer_config()?;
    config.max_deliver = None;
    assert_eq!(
        config.validate().err().map(|error| error.field()),
        Some("max_deliver")
    );

    config.max_deliver = Some(5);
    config.max_ack_pending = None;
    assert_eq!(
        config.validate().err().map(|error| error.field()),
        Some("max_ack_pending")
    );

    config.max_ack_pending = Some(128);
    config.max_payload_bytes = 0;
    assert_eq!(
        config.validate().err().map(|error| error.field()),
        Some("max_payload_bytes")
    );
    Ok(())
}

#[test]
fn replay_durable_must_differ_from_the_operational_durable() -> Result<(), Box<dyn Error>> {
    let mut config = replay_config()?;
    config.durable_name = Arc::clone(&config.operational_durable_name);

    assert_eq!(
        config.validate().err().map(|error| error.field()),
        Some("durable_name")
    );
    Ok(())
}

#[test]
fn heartbeat_failure_window_must_fit_inside_ack_wait() -> Result<(), Box<dyn Error>> {
    let mut config = consumer_config()?;
    config.heartbeat = Some(DeliveryHeartbeatConfig::new(Duration::from_secs(10), 3)?);

    assert_eq!(
        config.validate().err().map(|error| error.field()),
        Some("heartbeat")
    );
    Ok(())
}

#[test]
fn credential_and_tls_debug_output_redacts_every_secret_bearing_variant() {
    let credentials = [
        NatsCredentials::None,
        NatsCredentials::Token(SecretString::new("private-token")),
        NatsCredentials::UserPassword {
            username: Arc::from("private-user"),
            password: SecretString::new("private-password"),
        },
        NatsCredentials::Nkey(SecretString::new("private-seed")),
        NatsCredentials::Credentials(SecretString::new("private-credentials")),
        NatsCredentials::CredentialsFile(PathBuf::from("private.creds")),
    ];
    for credential in credentials {
        let debug = format!("{credential:?}");
        assert!(debug.starts_with("NatsCredentials::"));
        assert!(!debug.contains("private"));
    }

    let certificate = ClientCertificate {
        certificate: PathBuf::from("private-cert.pem"),
        private_key: PathBuf::from("private-key.pem"),
    };
    assert_eq!(format!("{certificate:?}"), "ClientCertificate([REDACTED])");
    let tls = NatsTlsConfig {
        mode: TlsMode::AllowPlaintext,
        root_certificates: vec![PathBuf::from("private-root.pem")],
        client_certificate: Some(certificate),
        tls_first: true,
    };
    let debug = format!("{tls:?}");
    assert!(debug.contains("root_certificate_count: 1"));
    assert!(debug.contains("has_client_certificate: true"));
    assert!(!debug.contains("private"));
}

#[test]
fn every_connection_and_producer_configuration_bound_is_field_specific()
-> Result<(), Box<dyn Error>> {
    let base = NatsConfig::default();
    let connection_cases = [
        mutated(&base, |config| config.servers.clear()),
        mutated(&base, |config| config.connect_timeout = Duration::ZERO),
        mutated(&base, |config| config.request_timeout = Duration::ZERO),
        mutated(&base, |config| config.client_capacity = 0),
        mutated(&base, |config| config.subscription_capacity = 0),
        mutated(&base, |config| config.health_component = Arc::from(" ")),
        mutated(&base, |config| config.servers = vec![Arc::from(" ")]),
    ];
    let expected_fields = [
        "servers",
        "connect_timeout",
        "request_timeout",
        "client_capacity",
        "subscription_capacity",
        "health_component",
        "servers",
    ];
    for (config, field) in connection_cases.into_iter().zip(expected_fields) {
        let error = config
            .validate()
            .err()
            .ok_or("invalid connection case unexpectedly passed")?;
        assert_eq!(error.field(), field);
        assert!(error.to_string().starts_with("invalid NATS "));
    }

    let producer_cases = [
        JetStreamProducerConfig {
            subject: Arc::from(" "),
            max_payload_bytes: 1,
            message_id_metadata_key: None,
        },
        JetStreamProducerConfig {
            subject: Arc::from("events.test"),
            max_payload_bytes: 0,
            message_id_metadata_key: None,
        },
        JetStreamProducerConfig {
            subject: Arc::from("events.test"),
            max_payload_bytes: 1,
            message_id_metadata_key: Some(Arc::from("not_namespaced")),
        },
    ];
    for (config, field) in
        producer_cases
            .into_iter()
            .zip(["subject", "max_payload_bytes", "message_id_metadata_key"])
    {
        assert_eq!(
            config.validate().err().map(|error| error.field()),
            Some(field)
        );
    }
    Ok(())
}

#[test]
fn every_consumer_bound_and_provisioning_failure_is_rejected() -> Result<(), Box<dyn Error>> {
    let base = consumer_config()?;
    let overflow_heartbeat = DeliveryHeartbeatConfig::new(Duration::MAX, 2)?;
    let excessive_pending = usize::try_from(i64::MAX)?.saturating_add(1);
    let cases = [
        consumer_mutated(&base, |config| config.stream = Arc::from(" ")),
        consumer_mutated(&base, |config| config.durable_name = Arc::from(" ")),
        consumer_mutated(&base, |config| config.filter_subject = Arc::from(" ")),
        consumer_mutated(&base, |config| config.ack_wait = Duration::ZERO),
        consumer_mutated(&base, |config| {
            config.heartbeat = Some(overflow_heartbeat);
        }),
        consumer_mutated(&base, |config| config.max_deliver = Some(0)),
        consumer_mutated(&base, |config| config.max_ack_pending = Some(0)),
        consumer_mutated(&base, |config| {
            config.max_ack_pending = Some(excessive_pending);
        }),
        consumer_mutated(&base, |config| {
            config.infrastructure = InfrastructureMode::CreateIfMissing {
                stream_subjects: vec![Arc::from(" ")],
            };
        }),
    ];
    let expected = [
        "stream",
        "durable_name",
        "filter_subject",
        "ack_wait",
        "heartbeat",
        "max_deliver",
        "max_ack_pending",
        "max_ack_pending",
        "stream_subjects",
    ];
    for (config, field) in cases.into_iter().zip(expected) {
        assert_eq!(
            config.validate().err().map(|error| error.field()),
            Some(field)
        );
    }

    let mut replay = replay_config()?;
    replay.operational_durable_name = Arc::from(" ");
    assert_eq!(
        replay.validate().err().map(|error| error.field()),
        Some("operational_durable_name")
    );
    Ok(())
}

#[test]
fn adapter_error_preserves_public_taxonomy_source_and_redacted_diagnostics() {
    let source_free = NatsAdapterError::new(
        NatsErrorCategory::Protocol,
        NatsOperation::Metadata,
        "safe protocol failure",
    );
    assert_eq!(source_free.category(), NatsErrorCategory::Protocol);
    assert_eq!(source_free.operation(), NatsOperation::Metadata);
    assert_eq!(source_free.message(), "safe protocol failure");
    assert_eq!(source_free.to_string(), "safe protocol failure");
    assert!(source_free.source().is_none());

    let sourced = NatsAdapterError::with_source(
        NatsErrorCategory::Connection,
        NatsOperation::Probe,
        "safe connection failure",
        std::io::Error::other("private source"),
    );
    assert!(sourced.source().is_some());
    let debug = format!("{sourced:?}");
    assert!(debug.contains("has_source: true"));
    assert!(!debug.contains("private source"));
}

#[tokio::test]
async fn connection_maps_validation_failure_before_any_network_effect() -> Result<(), Box<dyn Error>>
{
    let mut config = NatsConfig::default();
    config.servers.clear();
    let error = NatsConnection::connect(config, plenora_runtime_core::HealthRegistry::new())
        .await
        .err()
        .ok_or("invalid connection config unexpectedly passed")?;
    assert_eq!(error.category(), NatsErrorCategory::Configuration);
    assert_eq!(error.operation(), NatsOperation::Connect);
    assert!(error.source().is_some());
    Ok(())
}

fn mutated(base: &NatsConfig, update: impl FnOnce(&mut NatsConfig)) -> NatsConfig {
    let mut config = base.clone();
    update(&mut config);
    config
}

fn consumer_mutated(
    base: &JetStreamConsumerConfig,
    update: impl FnOnce(&mut JetStreamConsumerConfig),
) -> JetStreamConsumerConfig {
    let mut config = base.clone();
    update(&mut config);
    config
}

fn consumer_config() -> Result<JetStreamConsumerConfig, Box<dyn Error>> {
    Ok(JetStreamConsumerConfig {
        stream: Arc::from("EVENTS"),
        durable_name: Arc::from("worker"),
        filter_subject: Arc::from("events.>"),
        ack_wait: Duration::from_secs(30),
        heartbeat: Some(DeliveryHeartbeatConfig::new(Duration::from_secs(5), 3)?),
        max_deliver: Some(5),
        max_ack_pending: Some(128),
        max_payload_bytes: 64 * 1024,
        shutdown_nak_delay: Duration::from_secs(2),
        infrastructure: InfrastructureMode::BindExisting,
    })
}

fn replay_config() -> Result<ReplayConsumerConfig, Box<dyn Error>> {
    Ok(ReplayConsumerConfig {
        stream: Arc::from("EVENTS"),
        durable_name: Arc::from("worker-replay"),
        operational_durable_name: Arc::from("worker"),
        filter_subject: Arc::from("events.>"),
        ack_wait: Duration::from_secs(30),
        heartbeat: Some(DeliveryHeartbeatConfig::new(Duration::from_secs(5), 3)?),
        max_deliver: Some(5),
        max_ack_pending: Some(128),
        max_payload_bytes: 64 * 1024,
        shutdown_nak_delay: Duration::from_secs(2),
        infrastructure: InfrastructureMode::BindExisting,
    })
}
