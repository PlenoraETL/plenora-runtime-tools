use std::{path::PathBuf, str::FromStr as _, sync::Arc};

use async_nats::{ConnectOptions, Event, ServerAddr, jetstream};
use plenora_runtime_core::HealthRegistry;
use plenora_runtime_messaging::{BrokerCapabilities, ReplayRequest};

use crate::{
    JetStreamConsumer, JetStreamConsumerConfig, JetStreamProducer, JetStreamProducerConfig,
    NatsAdapterError, NatsConfig, NatsCredentials, NatsErrorCategory, NatsOperation,
    ReplayConsumerConfig, TlsMode, capabilities, health::NatsHealthReporter, replay,
};

/// Connected NATS client and `JetStream` adapter factory.
#[derive(Clone)]
pub struct NatsConnection {
    client: async_nats::Client,
    context: jetstream::Context,
    health: NatsHealthReporter,
}

impl NatsConnection {
    /// Connects using validated TLS, credentials, reconnect, and health settings.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration, credentials, server URLs, or connection fail.
    pub async fn connect(
        config: NatsConfig,
        health_registry: HealthRegistry,
    ) -> Result<Self, NatsAdapterError> {
        config.validate().map_err(|error| {
            NatsAdapterError::with_source(
                NatsErrorCategory::Configuration,
                NatsOperation::Connect,
                "invalid NATS connection configuration",
                error,
            )
        })?;
        let health = NatsHealthReporter::new(health_registry, Arc::clone(&config.health_component));
        let callback_health = health.clone();
        let mut options = ConnectOptions::new()
            .require_tls(config.tls.mode == TlsMode::Required)
            .connection_timeout(config.connect_timeout)
            .request_timeout(Some(config.request_timeout))
            .max_reconnects(config.max_reconnects)
            .client_capacity(config.client_capacity)
            .subscription_capacity(config.subscription_capacity)
            .event_callback(move |event| {
                let reporter = callback_health.clone();
                async move {
                    report_event(&reporter, &event);
                }
            });
        if config.retry_on_initial_connect {
            options = options.retry_on_initial_connect();
        }
        if config.tls.tls_first {
            options = options.tls_first();
        }
        for certificate in config.tls.root_certificates {
            options = options.add_root_certificates(certificate);
        }
        if let Some(identity) = config.tls.client_certificate {
            options = options.add_client_certificate(identity.certificate, identity.private_key);
        }
        options = apply_credentials(options, &config.credentials).await?;
        let servers = config
            .servers
            .iter()
            .map(|server| ServerAddr::from_str(server))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                NatsAdapterError::with_source(
                    NatsErrorCategory::Configuration,
                    NatsOperation::Connect,
                    "a configured NATS server URL is invalid",
                    error,
                )
            })?;
        let client = options.connect(servers).await.map_err(|error| {
            health.unhealthy("NATS initial connection failed");
            NatsAdapterError::with_source(
                NatsErrorCategory::Connection,
                NatsOperation::Connect,
                "NATS initial connection failed",
                error,
            )
        })?;
        let context = jetstream::new(client.clone());
        context.query_account().await.map_err(|error| {
            health.degraded("NATS connected but JetStream is unavailable");
            NatsAdapterError::with_source(
                NatsErrorCategory::Connection,
                NatsOperation::Connect,
                "NATS connected but JetStream is unavailable",
                error,
            )
        })?;
        health.ready();
        Ok(Self {
            client,
            context,
            health,
        })
    }

    /// Performs an active `JetStream` health probe and updates health/readiness.
    ///
    /// # Errors
    ///
    /// Returns an error when the account query fails.
    pub async fn probe(&self) -> Result<(), NatsAdapterError> {
        self.context.query_account().await.map_err(|error| {
            self.health.degraded("JetStream health probe failed");
            NatsAdapterError::with_source(
                NatsErrorCategory::Connection,
                NatsOperation::Probe,
                "JetStream health probe failed",
                error,
            )
        })?;
        self.health.ready();
        Ok(())
    }

    /// Forces a reconnect without waiting for the new connection to be established.
    ///
    /// Normal transport failures reconnect automatically. This explicit trigger is useful for
    /// credential rotation, controlled rebalancing, and deterministic integration checks.
    ///
    /// # Errors
    ///
    /// Returns an error when the reconnect command cannot be submitted to the client.
    pub async fn force_reconnect(&self) -> Result<(), NatsAdapterError> {
        self.health
            .degraded("NATS connection is reconnecting by request");
        self.client.force_reconnect().await.map_err(|error| {
            self.health.unhealthy("NATS reconnect request failed");
            NatsAdapterError::with_source(
                NatsErrorCategory::Connection,
                NatsOperation::Reconnect,
                "NATS reconnect request failed",
                error,
            )
        })
    }

    /// Starts draining subscriptions and buffered client operations.
    ///
    /// The underlying client accepts the drain command asynchronously. Callers should continue
    /// polling their consumer streams until they end before completing process shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error when the NATS client cannot accept the graceful drain command.
    pub async fn begin_drain(&self) -> Result<(), NatsAdapterError> {
        self.health.degraded("NATS connection is draining");
        self.client.drain().await.map_err(|error| {
            self.health.unhealthy("NATS connection drain failed");
            NatsAdapterError::with_source(
                NatsErrorCategory::Connection,
                NatsOperation::Drain,
                "NATS connection drain failed",
                error,
            )
        })
    }

    /// Creates a validated fixed-subject producer.
    ///
    /// # Errors
    ///
    /// Returns an error when the producer configuration is invalid.
    pub fn producer(
        &self,
        config: JetStreamProducerConfig,
    ) -> Result<JetStreamProducer, NatsAdapterError> {
        JetStreamProducer::new(self.context.clone(), config)
    }

    /// Binds or explicitly provisions a durable operational pull consumer.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration or unavailable infrastructure.
    pub async fn consumer(
        &self,
        config: JetStreamConsumerConfig,
    ) -> Result<JetStreamConsumer, NatsAdapterError> {
        JetStreamConsumer::operational(&self.context, config).await
    }

    /// Binds or explicitly provisions a dedicated replay consumer.
    ///
    /// The supplied durable name must be separate from the operational durable.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration or unavailable infrastructure.
    pub async fn replay_consumer(
        &self,
        config: ReplayConsumerConfig,
        request: ReplayRequest,
    ) -> Result<JetStreamConsumer, NatsAdapterError> {
        JetStreamConsumer::replay(
            &self.context,
            config,
            replay::delivery_policy(&request.source),
        )
        .await
    }

    /// Returns the capabilities guaranteed by this adapter.
    #[must_use]
    pub const fn capabilities(&self) -> BrokerCapabilities {
        capabilities()
    }

    /// Returns whether the underlying client currently reports a connected state.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        matches!(
            self.client.connection_state(),
            async_nats::connection::State::Connected
        )
    }
}

async fn apply_credentials(
    options: ConnectOptions,
    credentials: &NatsCredentials,
) -> Result<ConnectOptions, NatsAdapterError> {
    match credentials {
        NatsCredentials::None => Ok(options),
        NatsCredentials::Token(token) => Ok(options.token(token.expose().to_owned())),
        NatsCredentials::UserPassword { username, password } => {
            Ok(options.user_and_password(username.to_string(), password.expose().to_owned()))
        }
        NatsCredentials::Nkey(seed) => Ok(options.nkey(seed.expose().to_owned())),
        NatsCredentials::Credentials(credentials) => options
            .credentials(credentials.expose())
            .map_err(credentials_error),
        NatsCredentials::CredentialsFile(path) => options
            .credentials_file(PathBuf::from(path))
            .await
            .map_err(credentials_error),
    }
}

fn credentials_error<E>(source: E) -> NatsAdapterError
where
    E: std::error::Error + Send + Sync + 'static,
{
    NatsAdapterError::with_source(
        NatsErrorCategory::Configuration,
        NatsOperation::Connect,
        "NATS credentials could not be loaded",
        source,
    )
}

fn report_event(health: &NatsHealthReporter, event: &Event) {
    match event {
        Event::Connected => health.ready(),
        Event::Disconnected => health.degraded("NATS connection is reconnecting"),
        Event::Closed => health.unhealthy("NATS connection is closed"),
        Event::LameDuckMode => health.degraded("NATS server entered lame-duck mode"),
        Event::SlowConsumer(_) => health.degraded("NATS slow consumer detected"),
        Event::ServerError(_) | Event::ClientError(_) => {
            health.degraded("NATS client reported an error");
        }
        Event::Draining => health.degraded("NATS connection is draining"),
    }
}
