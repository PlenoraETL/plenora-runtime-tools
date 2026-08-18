//! Dynamically admitted, bounded NATS `JetStream` worker example.

#![forbid(unsafe_code)]

use std::{
    env,
    error::Error,
    fmt::{self, Display, Formatter},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use axum::{Router, routing::get};
use plenora_runtime_apalis::{ApalisAdapterConfig, BrokerWorkerRunner};
use plenora_runtime_capabilities::{
    CapabilityDispatcher, CapabilityDispatcherConfig, CapabilityFailure, CapabilityHandler,
    CapabilityId, CapabilityMessageCodec, CapabilityRegistryBuilder, CapabilityRegistryConfig,
    CapabilityRemoteEffect, CapabilityRequest,
};
use plenora_runtime_core::{RuntimeConfig, RuntimeHandle, ServiceMetadata, SystemClock};
use plenora_runtime_http::{HttpBootstrap, HttpServerConfig};
use plenora_runtime_messaging::{
    DEAD_LETTER_ID_METADATA_KEY, DeliveryHeartbeatConfig, ExponentialBackoff,
    ExponentialBackoffConfig, RetryErrorClass, RetryExhaustedAction,
};
use plenora_runtime_nats::{
    InfrastructureMode, JetStreamConsumerConfig, JetStreamProducerConfig, NatsConfig,
    NatsConnection, NatsCredentials, NatsTlsConfig, SecretString, TlsMode,
};
use plenora_runtime_worker::{
    MetadataMessageDecoder, WorkerConcurrency, WorkerConfig, WorkerContext,
    WorkerInstanceHeartbeat, WorkerInstanceHeartbeatConfig, WorkerInstanceHeartbeatObserver,
};
use tokio::time::timeout;

const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(10);
const DEFAULT_MAX_WORKERS: usize = 32;
const MAX_WORKERS_LIMIT: usize = 4_096;
const MAX_NAME_BYTES: usize = 256;
const MAX_PAYLOAD_BYTES: usize = 1_048_576;
const MAX_SERVER_COUNT: usize = 8;
const MAX_SERVER_URL_BYTES: usize = 2_048;
const MAX_SERVER_LIST_BYTES: usize = MAX_SERVER_COUNT * MAX_SERVER_URL_BYTES;
const MAX_TOKEN_BYTES: usize = 16 * 1_024;

#[derive(Clone, Copy, Debug)]
enum ExampleConfigError {
    ServerListNotUnicode,
    ServerListEmpty,
    ServerListTooLarge,
    TooManyServers,
    ServerUrlTooLarge,
    PlaintextRequiresOptIn,
    PlaintextFlagNotUnicode,
    PlaintextFlagInvalid,
    TokenNotUnicode,
    TokenEmpty,
    TokenTooLarge,
    MaxWorkersNotUnicode,
    MaxWorkersInvalid,
    MaxWorkersOutOfRange,
    StreamNotUnicode,
    StreamInvalid,
    DurableNotUnicode,
    DurableInvalid,
    SubjectNotUnicode,
    SubjectInvalid,
    DeadLetterSubjectNotUnicode,
    DeadLetterSubjectInvalid,
    HttpBindNotUnicode,
    HttpBindInvalid,
    DrainTimedOut,
}

impl Display for ExampleConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ServerListNotUnicode => "PLENORA_NATS_SERVERS must contain Unicode text",
            Self::ServerListEmpty => "PLENORA_NATS_SERVERS must contain non-empty server URLs",
            Self::ServerListTooLarge => {
                "PLENORA_NATS_SERVERS exceeds the example configuration bound"
            }
            Self::TooManyServers => "PLENORA_NATS_SERVERS contains too many server URLs",
            Self::ServerUrlTooLarge => "a configured NATS server URL exceeds the example bound",
            Self::PlaintextRequiresOptIn => "nats:// requires PLENORA_NATS_ALLOW_PLAINTEXT=true",
            Self::PlaintextFlagNotUnicode => {
                "PLENORA_NATS_ALLOW_PLAINTEXT must contain Unicode text"
            }
            Self::PlaintextFlagInvalid => "PLENORA_NATS_ALLOW_PLAINTEXT must be true or false",
            Self::TokenNotUnicode => "PLENORA_NATS_TOKEN must contain Unicode text",
            Self::TokenEmpty => "PLENORA_NATS_TOKEN must not be blank",
            Self::TokenTooLarge => "PLENORA_NATS_TOKEN exceeds the example credential bound",
            Self::MaxWorkersNotUnicode => "PLENORA_MAX_WORKERS must contain Unicode text",
            Self::MaxWorkersInvalid => "PLENORA_MAX_WORKERS must be a positive integer",
            Self::MaxWorkersOutOfRange => "PLENORA_MAX_WORKERS exceeds the example bound",
            Self::StreamNotUnicode => "PLENORA_NATS_STREAM must contain Unicode text",
            Self::StreamInvalid => "PLENORA_NATS_STREAM must be non-empty and bounded",
            Self::DurableNotUnicode => "PLENORA_NATS_DURABLE must contain Unicode text",
            Self::DurableInvalid => "PLENORA_NATS_DURABLE must be non-empty and bounded",
            Self::SubjectNotUnicode => "PLENORA_NATS_SUBJECT must contain Unicode text",
            Self::SubjectInvalid => "PLENORA_NATS_SUBJECT must be non-empty and bounded",
            Self::DeadLetterSubjectNotUnicode => {
                "PLENORA_NATS_DLQ_SUBJECT must contain Unicode text"
            }
            Self::DeadLetterSubjectInvalid => {
                "PLENORA_NATS_DLQ_SUBJECT must be non-empty and bounded"
            }
            Self::HttpBindNotUnicode => "PLENORA_HTTP_BIND must contain Unicode text",
            Self::HttpBindInvalid => "PLENORA_HTTP_BIND must be a valid socket address",
            Self::DrainTimedOut => "NATS drain exceeded the shutdown grace period",
        })
    }
}

impl Error for ExampleConfigError {}

#[derive(Clone, Copy, Debug)]
struct UnsupportedOperation;

impl Display for UnsupportedOperation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("example capability does not support the requested operation")
    }
}

impl Error for UnsupportedOperation {}

#[derive(Clone, Copy, Debug)]
struct ExampleCapabilityHandler;

#[derive(Clone, Copy, Debug)]
struct ConsoleInstanceHeartbeat;

impl WorkerInstanceHeartbeatObserver for ConsoleInstanceHeartbeat {
    fn record(&self, heartbeat: WorkerInstanceHeartbeat) {
        println!(
            "worker heartbeat instance_id={} worker={} status={:?} in_flight={} available={} max={}",
            heartbeat.identity.instance_id,
            heartbeat.identity.worker_name,
            heartbeat.status,
            heartbeat.in_flight,
            heartbeat.available_slots,
            heartbeat.max_in_flight
        );
    }
}

#[async_trait]
impl CapabilityHandler for ExampleCapabilityHandler {
    async fn invoke(
        &self,
        context: WorkerContext,
        request: CapabilityRequest,
    ) -> Result<(), CapabilityFailure> {
        if request.operation().as_str() != "execute" {
            return Err(CapabilityFailure::new(
                RetryErrorClass::DeadLetter,
                CapabilityRemoteEffect::NotStarted,
                UnsupportedOperation,
            ));
        }
        println!(
            "processed capability={} operation={} message_id={} attempt={} payload_bytes={}",
            request.capability(),
            request.operation(),
            context.message_id,
            context.attempt,
            request.input().len(),
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::with_config(
        ServiceMetadata::new("worker-nats", env!("CARGO_PKG_VERSION"), "local-example"),
        RuntimeConfig {
            shutdown_grace_period: SHUTDOWN_GRACE_PERIOD,
            ..RuntimeConfig::default()
        },
    );
    let max_workers = max_workers_from_environment()?;
    let config = nats_config_from_environment()?;
    println!(
        "connecting to {} configured NATS server(s) with policy {:?}",
        config.servers.len(),
        config.tls.mode
    );

    let connection = NatsConnection::connect(config, runtime.health_registry()).await?;
    connection.probe().await?;
    let consumer = connection
        .consumer(consumer_config_from_environment(max_workers)?)
        .await?;
    let dead_letter_producer = connection.producer(dead_letter_config_from_environment()?)?;
    let http_config = http_config_from_environment()?;
    let http_address = http_config.bind_address();
    let http = HttpBootstrap::new(&runtime, http_config)?;
    let application =
        Router::new().route("/", get(|| async { "Plenora consumer runtime is running" }));
    let mut capabilities = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(16)?)?;
    capabilities.register(
        CapabilityId::new("plenora.example-tools", 1)?,
        ExampleCapabilityHandler,
    )?;
    let dispatcher = CapabilityDispatcher::new(
        capabilities.build(),
        CapabilityDispatcherConfig::new(MAX_PAYLOAD_BYTES)?,
    )?;
    let retry_policy = ExponentialBackoff::new(ExponentialBackoffConfig {
        initial_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(30),
        multiplier: 2,
        max_attempts: 5,
        max_elapsed: Some(Duration::from_mins(2)),
        jitter: None,
        retry_unknown_outcome: false,
        exhausted_action: RetryExhaustedAction::DeadLetter,
    })?;
    let runner = BrokerWorkerRunner::new(
        consumer,
        MetadataMessageDecoder::<_, CapabilityRequest>::new(CapabilityMessageCodec),
        dispatcher,
        retry_policy,
        ApalisAdapterConfig::new(
            "worker-nats",
            WorkerConfig::new(WorkerConcurrency::new(max_workers)?, SHUTDOWN_GRACE_PERIOD),
        )?,
        runtime.shutdown_signal(),
    )?
    .with_dead_letter_sink(dead_letter_producer)
    .with_instance_heartbeat(
        runtime.metadata(),
        WorkerInstanceHeartbeatConfig::default(),
        Arc::new(ConsoleInstanceHeartbeat),
        Arc::new(SystemClock),
    );
    println!(
        "consumer ready; max_workers={}; health=http://{http_address}/health; readiness=http://{http_address}/ready; press Ctrl-C to drain",
        runner.max_in_flight(),
    );

    let worker = runner.run();
    let server = http.serve(application);
    tokio::pin!(worker);
    tokio::pin!(server);
    let (worker_result, http_result, signal_result) = tokio::select! {
        result = &mut worker => {
            let _shutdown_started = runtime.request_shutdown();
            (result, server.await, Ok(()))
        }
        result = &mut server => {
            let _shutdown_started = runtime.request_shutdown();
            (worker.await, result, Ok(()))
        }
        result = tokio::signal::ctrl_c() => {
            let _shutdown_started = runtime.request_shutdown();
            let (worker_result, http_result) = tokio::join!(worker, server);
            (worker_result, http_result, result)
        }
    };

    match timeout(SHUTDOWN_GRACE_PERIOD, connection.begin_drain()).await {
        Ok(result) => result?,
        Err(_elapsed) => {
            return Err(Box::new(ExampleConfigError::DrainTimedOut) as Box<dyn Error>);
        }
    }
    let runtime_outcome = runtime.shutdown().await;
    signal_result?;
    worker_result?;
    http_result?;
    println!("NATS drain accepted; runtime={runtime_outcome:?}");

    Ok(())
}

fn http_config_from_environment() -> Result<HttpServerConfig, Box<dyn Error>> {
    let bind_address =
        optional_environment("PLENORA_HTTP_BIND", ExampleConfigError::HttpBindNotUnicode)?
            .unwrap_or_else(|| String::from("127.0.0.1:3001"))
            .parse::<SocketAddr>()
            .map_err(|_error| ExampleConfigError::HttpBindInvalid)?;
    Ok(HttpServerConfig::new(bind_address, SHUTDOWN_GRACE_PERIOD)?)
}

fn max_workers_from_environment() -> Result<usize, Box<dyn Error>> {
    let value = optional_environment(
        "PLENORA_MAX_WORKERS",
        ExampleConfigError::MaxWorkersNotUnicode,
    )?;
    let max_workers = match value {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_error| ExampleConfigError::MaxWorkersInvalid)?,
        None => DEFAULT_MAX_WORKERS,
    };
    if max_workers == 0 {
        return Err(Box::new(ExampleConfigError::MaxWorkersInvalid) as Box<dyn Error>);
    }
    if max_workers > MAX_WORKERS_LIMIT {
        return Err(Box::new(ExampleConfigError::MaxWorkersOutOfRange) as Box<dyn Error>);
    }
    Ok(max_workers)
}

fn consumer_config_from_environment(
    max_workers: usize,
) -> Result<JetStreamConsumerConfig, Box<dyn Error>> {
    let stream = bounded_name_from_environment(
        "PLENORA_NATS_STREAM",
        "PLENORA_WORK",
        ExampleConfigError::StreamNotUnicode,
        ExampleConfigError::StreamInvalid,
    )?;
    let durable_name = bounded_name_from_environment(
        "PLENORA_NATS_DURABLE",
        "plenora-worker-nats",
        ExampleConfigError::DurableNotUnicode,
        ExampleConfigError::DurableInvalid,
    )?;
    let filter_subject = bounded_name_from_environment(
        "PLENORA_NATS_SUBJECT",
        "plenora.work",
        ExampleConfigError::SubjectNotUnicode,
        ExampleConfigError::SubjectInvalid,
    )?;
    let config = JetStreamConsumerConfig {
        stream,
        durable_name,
        filter_subject,
        ack_wait: Duration::from_secs(30),
        heartbeat: Some(DeliveryHeartbeatConfig::new(Duration::from_secs(5), 3)?),
        max_deliver: Some(5),
        max_ack_pending: Some(max_workers),
        max_payload_bytes: MAX_PAYLOAD_BYTES,
        shutdown_nak_delay: Duration::from_secs(1),
        infrastructure: InfrastructureMode::BindExisting,
    };
    config.validate()?;
    Ok(config)
}

fn dead_letter_config_from_environment() -> Result<JetStreamProducerConfig, Box<dyn Error>> {
    let subject = bounded_name_from_environment(
        "PLENORA_NATS_DLQ_SUBJECT",
        "plenora.work.dlq",
        ExampleConfigError::DeadLetterSubjectNotUnicode,
        ExampleConfigError::DeadLetterSubjectInvalid,
    )?;
    let config = JetStreamProducerConfig {
        subject,
        max_payload_bytes: MAX_PAYLOAD_BYTES,
        message_id_metadata_key: Some(Arc::from(DEAD_LETTER_ID_METADATA_KEY)),
    };
    config.validate()?;
    Ok(config)
}

fn bounded_name_from_environment(
    name: &str,
    default: &'static str,
    invalid_unicode: ExampleConfigError,
    invalid_value: ExampleConfigError,
) -> Result<Arc<str>, Box<dyn Error>> {
    let value =
        optional_environment(name, invalid_unicode)?.unwrap_or_else(|| String::from(default));
    if value.trim().is_empty() || value.len() > MAX_NAME_BYTES {
        return Err(Box::new(invalid_value) as Box<dyn Error>);
    }
    Ok(Arc::from(value))
}

fn nats_config_from_environment() -> Result<NatsConfig, Box<dyn Error>> {
    let raw_servers = match optional_environment(
        "PLENORA_NATS_SERVERS",
        ExampleConfigError::ServerListNotUnicode,
    )? {
        Some(servers) => servers,
        None => String::from("tls://127.0.0.1:4222"),
    };
    if raw_servers.len() > MAX_SERVER_LIST_BYTES {
        return Err(Box::new(ExampleConfigError::ServerListTooLarge) as Box<dyn Error>);
    }
    let server_parts = raw_servers.split(',').map(str::trim).collect::<Vec<_>>();
    if server_parts.is_empty() || server_parts.iter().any(|server| server.is_empty()) {
        return Err(Box::new(ExampleConfigError::ServerListEmpty) as Box<dyn Error>);
    }
    if server_parts.len() > MAX_SERVER_COUNT {
        return Err(Box::new(ExampleConfigError::TooManyServers) as Box<dyn Error>);
    }
    if server_parts
        .iter()
        .any(|server| server.len() > MAX_SERVER_URL_BYTES)
    {
        return Err(Box::new(ExampleConfigError::ServerUrlTooLarge) as Box<dyn Error>);
    }

    let allow_plaintext = plaintext_opt_in()?;
    if !allow_plaintext && server_parts.iter().any(|server| is_nats_url(server)) {
        return Err(Box::new(ExampleConfigError::PlaintextRequiresOptIn) as Box<dyn Error>);
    }

    let credentials =
        match optional_environment("PLENORA_NATS_TOKEN", ExampleConfigError::TokenNotUnicode)? {
            Some(token) if token.trim().is_empty() => {
                return Err(Box::new(ExampleConfigError::TokenEmpty) as Box<dyn Error>);
            }
            Some(token) if token.len() > MAX_TOKEN_BYTES => {
                return Err(Box::new(ExampleConfigError::TokenTooLarge) as Box<dyn Error>);
            }
            Some(token) => NatsCredentials::Token(SecretString::new(token)),
            None => NatsCredentials::None,
        };

    let servers = server_parts
        .into_iter()
        .map(Arc::<str>::from)
        .collect::<Vec<_>>();
    let tls = NatsTlsConfig {
        mode: if allow_plaintext {
            TlsMode::AllowPlaintext
        } else {
            TlsMode::Required
        },
        ..NatsTlsConfig::default()
    };
    let config = NatsConfig {
        servers,
        credentials,
        tls,
        max_reconnects: Some(10),
        retry_on_initial_connect: false,
        ..NatsConfig::default()
    };
    config.validate()?;
    Ok(config)
}

fn plaintext_opt_in() -> Result<bool, Box<dyn Error>> {
    let value = optional_environment(
        "PLENORA_NATS_ALLOW_PLAINTEXT",
        ExampleConfigError::PlaintextFlagNotUnicode,
    )?;
    match value.as_deref() {
        None => Ok(false),
        Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        Some(_) => Err(Box::new(ExampleConfigError::PlaintextFlagInvalid) as Box<dyn Error>),
    }
}

fn optional_environment(
    name: &str,
    invalid_unicode: ExampleConfigError,
) -> Result<Option<String>, ExampleConfigError> {
    match env::var_os(name) {
        Some(value) => value
            .into_string()
            .map(Some)
            .map_err(|_value| invalid_unicode),
        None => Ok(None),
    }
}

fn is_nats_url(server: &str) -> bool {
    server
        .get(.."nats://".len())
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("nats://"))
}
