use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use plenora_runtime_messaging::DeliveryHeartbeatConfig;

/// Secret value whose debug output is always redacted.
#[derive(Clone, Eq, PartialEq)]
pub struct SecretString(Arc<str>);

impl SecretString {
    /// Wraps a secret.
    #[must_use]
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl Debug for SecretString {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretString([REDACTED])")
    }
}

/// NATS authentication mechanism.
#[derive(Clone, Default, Eq, PartialEq)]
pub enum NatsCredentials {
    /// No authentication.
    #[default]
    None,
    /// Bearer token.
    Token(SecretString),
    /// Username and password.
    UserPassword {
        /// Public username.
        username: Arc<str>,
        /// Secret password.
        password: SecretString,
    },
    /// `NKey` seed.
    Nkey(SecretString),
    /// Inline NATS credentials.
    Credentials(SecretString),
    /// NATS credentials file path.
    CredentialsFile(PathBuf),
}

impl Debug for NatsCredentials {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::None => "NatsCredentials::None",
            Self::Token(_) => "NatsCredentials::Token([REDACTED])",
            Self::UserPassword { .. } => "NatsCredentials::UserPassword([REDACTED])",
            Self::Nkey(_) => "NatsCredentials::Nkey([REDACTED])",
            Self::Credentials(_) => "NatsCredentials::Credentials([REDACTED])",
            Self::CredentialsFile(_) => "NatsCredentials::CredentialsFile([REDACTED])",
        })
    }
}

/// Transport security policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TlsMode {
    /// Require TLS.
    #[default]
    Required,
    /// Explicitly permit plaintext, intended for local development.
    AllowPlaintext,
}

/// Client certificate and private-key paths.
#[derive(Clone, Eq, PartialEq)]
pub struct ClientCertificate {
    /// PEM certificate chain.
    pub certificate: PathBuf,
    /// PEM private key.
    pub private_key: PathBuf,
}

impl Debug for ClientCertificate {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClientCertificate([REDACTED])")
    }
}

/// NATS TLS configuration.
#[derive(Clone, Eq, PartialEq)]
pub struct NatsTlsConfig {
    /// TLS requirement.
    pub mode: TlsMode,
    /// Additional PEM root certificates.
    pub root_certificates: Vec<PathBuf>,
    /// Optional mTLS identity.
    pub client_certificate: Option<ClientCertificate>,
    /// Negotiate TLS before the NATS INFO frame.
    pub tls_first: bool,
}

impl Default for NatsTlsConfig {
    fn default() -> Self {
        Self {
            mode: TlsMode::Required,
            root_certificates: Vec::new(),
            client_certificate: None,
            tls_first: false,
        }
    }
}

impl Debug for NatsTlsConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NatsTlsConfig")
            .field("mode", &self.mode)
            .field("root_certificate_count", &self.root_certificates.len())
            .field("has_client_certificate", &self.client_certificate.is_some())
            .field("tls_first", &self.tls_first)
            .finish()
    }
}

/// Controls whether missing `JetStream` resources may be provisioned.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum InfrastructureMode {
    /// Bind only to resources provisioned externally.
    #[default]
    BindExisting,
    /// Explicitly create a missing stream and consumer.
    CreateIfMissing {
        /// Subjects retained by a newly created stream.
        stream_subjects: Vec<Arc<str>>,
    },
}

/// Shared NATS connection configuration.
#[derive(Clone, Eq, PartialEq)]
pub struct NatsConfig {
    /// Server URLs; omitted from debug output because URLs can contain secrets.
    pub servers: Vec<Arc<str>>,
    /// Authentication configuration.
    pub credentials: NatsCredentials,
    /// TLS configuration.
    pub tls: NatsTlsConfig,
    /// Initial connection timeout.
    pub connect_timeout: Duration,
    /// Request timeout.
    pub request_timeout: Duration,
    /// Maximum reconnect attempts; None means unbounded.
    pub max_reconnects: Option<usize>,
    /// Retry the initial connection.
    pub retry_on_initial_connect: bool,
    /// Client command-channel capacity.
    pub client_capacity: usize,
    /// Subscription channel capacity.
    pub subscription_capacity: usize,
    /// Health registry component name.
    pub health_component: Arc<str>,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            servers: vec![Arc::from("tls://127.0.0.1:4222")],
            credentials: NatsCredentials::None,
            tls: NatsTlsConfig::default(),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            max_reconnects: Some(10),
            retry_on_initial_connect: false,
            client_capacity: 128,
            subscription_capacity: 128,
            health_component: Arc::from("nats.jetstream"),
        }
    }
}

impl Debug for NatsConfig {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NatsConfig")
            .field("server_count", &self.servers.len())
            .field("credentials", &self.credentials)
            .field("tls", &self.tls)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("max_reconnects", &self.max_reconnects)
            .field("retry_on_initial_connect", &self.retry_on_initial_connect)
            .field("client_capacity", &self.client_capacity)
            .field("subscription_capacity", &self.subscription_capacity)
            .field("health_component", &self.health_component)
            .finish()
    }
}

impl NatsConfig {
    /// Validates the configuration.
    ///
    /// # Errors
    ///
    /// Returns a field-specific error for empty or zero-valued settings.
    pub fn validate(&self) -> Result<(), NatsConfigError> {
        nonzero("servers", self.servers.len())?;
        duration("connect_timeout", self.connect_timeout)?;
        duration("request_timeout", self.request_timeout)?;
        nonzero("client_capacity", self.client_capacity)?;
        nonzero("subscription_capacity", self.subscription_capacity)?;
        text("health_component", &self.health_component)?;
        if self.servers.iter().any(|server| server.trim().is_empty()) {
            return Err(NatsConfigError::new("servers", "contains an empty URL"));
        }
        Ok(())
    }
}

/// Fixed-subject `JetStream` producer configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JetStreamProducerConfig {
    /// Fixed publication subject.
    pub subject: Arc<str>,
    /// Local payload-size limit.
    pub max_payload_bytes: usize,
    /// Optional UTF-8 metadata key used as the de-duplication message ID.
    pub message_id_metadata_key: Option<Arc<str>>,
}

impl JetStreamProducerConfig {
    /// Validates this producer configuration.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty subject, zero limit, or non-namespaced key.
    pub fn validate(&self) -> Result<(), NatsConfigError> {
        text("subject", &self.subject)?;
        nonzero("max_payload_bytes", self.max_payload_bytes)?;
        if self
            .message_id_metadata_key
            .as_deref()
            .is_some_and(|key| key.trim().is_empty() || !key.contains('.'))
        {
            return Err(NatsConfigError::new(
                "message_id_metadata_key",
                "must be a namespaced key",
            ));
        }
        Ok(())
    }
}

/// Durable pull-consumer configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JetStreamConsumerConfig {
    /// Stream name.
    pub stream: Arc<str>,
    /// Durable consumer name.
    pub durable_name: Arc<str>,
    /// Consumer filter subject.
    pub filter_subject: Arc<str>,
    /// Server acknowledgement timeout.
    pub ack_wait: Duration,
    /// Optional lease-renewal policy attached to each operational delivery.
    pub heartbeat: Option<DeliveryHeartbeatConfig>,
    /// Finite delivery limit. `None` is retained for source compatibility but is rejected.
    pub max_deliver: Option<u32>,
    /// Finite unacknowledged-delivery limit. `None` is rejected.
    pub max_ack_pending: Option<usize>,
    /// Maximum encoded payload bytes admitted to a handler.
    pub max_payload_bytes: usize,
    /// Delay used for shutdown NAKs.
    pub shutdown_nak_delay: Duration,
    /// Binding or explicit provisioning mode.
    pub infrastructure: InfrastructureMode,
}

impl JetStreamConsumerConfig {
    /// Validates this consumer configuration.
    ///
    /// # Errors
    ///
    /// Returns a field-specific validation error.
    pub fn validate(&self) -> Result<(), NatsConfigError> {
        consumer(ConsumerValidation {
            stream: &self.stream,
            durable: &self.durable_name,
            filter: &self.filter_subject,
            ack_wait: self.ack_wait,
            heartbeat: self.heartbeat,
            max_deliver: self.max_deliver,
            max_ack_pending: self.max_ack_pending,
            max_payload_bytes: self.max_payload_bytes,
            infrastructure: &self.infrastructure,
        })
    }
}

/// Dedicated pull-consumer configuration for replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayConsumerConfig {
    /// Stream name.
    pub stream: Arc<str>,
    /// Dedicated replay durable name.
    pub durable_name: Arc<str>,
    /// Operational durable whose cursor this replay must not reuse.
    pub operational_durable_name: Arc<str>,
    /// Replay filter subject.
    pub filter_subject: Arc<str>,
    /// Server acknowledgement timeout.
    pub ack_wait: Duration,
    /// Optional lease-renewal policy attached to each replay delivery.
    pub heartbeat: Option<DeliveryHeartbeatConfig>,
    /// Finite delivery limit. `None` is retained for source compatibility but is rejected.
    pub max_deliver: Option<u32>,
    /// Finite unacknowledged-delivery limit. `None` is rejected.
    pub max_ack_pending: Option<usize>,
    /// Maximum encoded payload bytes admitted to a replay handler.
    pub max_payload_bytes: usize,
    /// Delay used for shutdown NAKs.
    pub shutdown_nak_delay: Duration,
    /// Binding or explicit provisioning mode.
    pub infrastructure: InfrastructureMode,
}

impl ReplayConsumerConfig {
    /// Validates this replay configuration.
    ///
    /// # Errors
    ///
    /// Returns a field-specific validation error.
    pub fn validate(&self) -> Result<(), NatsConfigError> {
        text("operational_durable_name", &self.operational_durable_name)?;
        if self.durable_name == self.operational_durable_name {
            return Err(NatsConfigError::new(
                "durable_name",
                "must differ from the operational durable name",
            ));
        }
        consumer(ConsumerValidation {
            stream: &self.stream,
            durable: &self.durable_name,
            filter: &self.filter_subject,
            ack_wait: self.ack_wait,
            heartbeat: self.heartbeat,
            max_deliver: self.max_deliver,
            max_ack_pending: self.max_ack_pending,
            max_payload_bytes: self.max_payload_bytes,
            infrastructure: &self.infrastructure,
        })
    }
}

/// Redaction-safe configuration error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NatsConfigError {
    field: &'static str,
    message: &'static str,
}

impl NatsConfigError {
    const fn new(field: &'static str, message: &'static str) -> Self {
        Self { field, message }
    }

    /// Returns the invalid field.
    #[must_use]
    pub const fn field(&self) -> &'static str {
        self.field
    }
}

impl Display for NatsConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid NATS {}: {}", self.field, self.message)
    }
}

impl Error for NatsConfigError {}

#[derive(Clone, Copy)]
struct ConsumerValidation<'a> {
    stream: &'a str,
    durable: &'a str,
    filter: &'a str,
    ack_wait: Duration,
    heartbeat: Option<DeliveryHeartbeatConfig>,
    max_deliver: Option<u32>,
    max_ack_pending: Option<usize>,
    max_payload_bytes: usize,
    infrastructure: &'a InfrastructureMode,
}

fn consumer(settings: ConsumerValidation<'_>) -> Result<(), NatsConfigError> {
    text("stream", settings.stream)?;
    text("durable_name", settings.durable)?;
    text("filter_subject", settings.filter)?;
    duration("ack_wait", settings.ack_wait)?;
    if let Some(heartbeat) = settings.heartbeat {
        let failure_window = heartbeat
            .interval()
            .checked_mul(heartbeat.max_consecutive_failures())
            .ok_or_else(|| {
                NatsConfigError::new("heartbeat", "failure window exceeds the duration range")
            })?;
        if failure_window >= settings.ack_wait {
            return Err(NatsConfigError::new(
                "heartbeat",
                "failure window must be shorter than ack_wait",
            ));
        }
    }
    match settings.max_deliver {
        Some(value) if value > 0 => {}
        Some(_) => return Err(NatsConfigError::new("max_deliver", "must be nonzero")),
        None => {
            return Err(NatsConfigError::new(
                "max_deliver",
                "must be configured with a finite value",
            ));
        }
    }
    match settings.max_ack_pending {
        Some(value) if value > 0 && i64::try_from(value).is_ok() => {}
        Some(0) => {
            return Err(NatsConfigError::new("max_ack_pending", "must be nonzero"));
        }
        Some(_) => {
            return Err(NatsConfigError::new(
                "max_ack_pending",
                "exceeds the JetStream signed integer range",
            ));
        }
        None => {
            return Err(NatsConfigError::new(
                "max_ack_pending",
                "must be configured with a finite value",
            ));
        }
    }
    nonzero("max_payload_bytes", settings.max_payload_bytes)?;
    if let InfrastructureMode::CreateIfMissing { stream_subjects } = settings.infrastructure {
        nonzero("stream_subjects", stream_subjects.len())?;
        if stream_subjects
            .iter()
            .any(|subject| subject.trim().is_empty())
        {
            return Err(NatsConfigError::new(
                "stream_subjects",
                "contains an empty subject",
            ));
        }
    }
    Ok(())
}

fn text(field: &'static str, value: &str) -> Result<(), NatsConfigError> {
    if value.trim().is_empty() {
        Err(NatsConfigError::new(field, "must not be empty"))
    } else {
        Ok(())
    }
}

fn nonzero(field: &'static str, value: usize) -> Result<(), NatsConfigError> {
    if value == 0 {
        Err(NatsConfigError::new(field, "must be greater than zero"))
    } else {
        Ok(())
    }
}

fn duration(field: &'static str, value: Duration) -> Result<(), NatsConfigError> {
    if value.is_zero() {
        Err(NatsConfigError::new(field, "must be greater than zero"))
    } else {
        Ok(())
    }
}
