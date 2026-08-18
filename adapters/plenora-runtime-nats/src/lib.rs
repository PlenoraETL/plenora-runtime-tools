//! NATS `JetStream` adapter for Plenora messaging contracts.

#![forbid(unsafe_code)]

mod config;
mod connection;
mod consumer;
mod error;
mod health;
mod metadata;
mod producer;
mod replay;

pub use config::{
    ClientCertificate, InfrastructureMode, JetStreamConsumerConfig, JetStreamProducerConfig,
    NatsConfig, NatsConfigError, NatsCredentials, NatsTlsConfig, ReplayConsumerConfig,
    SecretString, TlsMode,
};
pub use connection::NatsConnection;
pub use consumer::JetStreamConsumer;
pub use error::{NatsAdapterError, NatsErrorCategory, NatsOperation};
pub use producer::JetStreamProducer;
pub use replay::capabilities;
