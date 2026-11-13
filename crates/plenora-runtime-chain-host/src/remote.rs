//! Explicit scripted request/reply harness. Durable domain settlement remains a separate gate.
use crate::ScriptedHost;
use async_trait::async_trait;
use futures_util::StreamExt;
use plenora_runtime_chains::{Error, Request, Response, Transport};
use std::{sync::Arc, time::Duration};

/// Remote scripted client. Ten seconds is an observation timeout, never a run deadline.
pub struct RemoteClient {
    client: async_nats::Client,
    subject: String,
}
impl RemoteClient {
    /// Bind a host-configured subject; wildcards and whitespace are refused.
    /// # Errors
    /// Returns data-free transport errors, without connection strings or credentials.
    pub async fn connect(server: &str, subject: &str) -> Result<Self, Error> {
        if subject.is_empty()
            || subject.len() > 256
            || subject
                .bytes()
                .any(|b| !b.is_ascii_graphic() || matches!(b, b'*' | b'>'))
        {
            return Err(Error::Invalid);
        }
        let client = tokio::time::timeout(Duration::from_secs(10), async_nats::connect(server))
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Storage)?;
        Ok(Self {
            client,
            subject: subject.into(),
        })
    }
    /// Send exact facade bytes; no domain algorithms or result rewriting live in this client.
    /// # Errors
    /// Missing response is uncertainty, not proof of failed or cancelled execution.
    pub async fn call(&self, bytes: Vec<u8>) -> Result<Response, Error> {
        if bytes.len() > 1024 * 1024 {
            return Err(Error::ResourceLimit);
        }
        let reply = tokio::time::timeout(
            Duration::from_secs(10),
            self.client.request(self.subject.clone(), bytes.into()),
        )
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|_| Error::Storage)?;
        if reply.payload.len() > 1024 * 1024 {
            return Err(Error::ResourceLimit);
        }
        serde_json::from_slice(&reply.payload).map_err(|_| Error::Invalid)
    }
}
#[async_trait]
impl Transport for RemoteClient {
    async fn exchange(&self, request: Request) -> Result<Response, Error> {
        self.call(serde_json::to_vec(&request).map_err(|_| Error::Invalid)?)
            .await
    }
}
impl ScriptedHost {
    /// Serve the explicitly scripted qualification subject with at most sixteen concurrent calls.
    /// # Errors
    /// Publication or worker failure is surfaced; it never causes domain replay.
    pub async fn serve(
        self: Arc<Self>,
        server: &str,
        subject: &str,
        ready: tokio::sync::oneshot::Sender<()>,
    ) -> Result<(), Error> {
        let remote = RemoteClient::connect(server, subject).await?;
        let mut subscription = remote
            .client
            .subscribe(remote.subject)
            .await
            .map_err(|_| Error::Storage)?;
        remote.client.flush().await.map_err(|_| Error::Storage)?;
        ready.send(()).map_err(|()| Error::Storage)?;
        let mut tasks = tokio::task::JoinSet::new();
        while let Some(message) = subscription.next().await {
            if tasks.len() >= 16 {
                tasks
                    .join_next()
                    .await
                    .ok_or(Error::Storage)?
                    .map_err(|_| Error::Storage)??;
            }
            let host = self.clone();
            let client = remote.client.clone();
            tasks.spawn(async move {
                let reply = message.reply.ok_or(Error::Invalid)?;
                let response = host.call(&message.payload).await;
                let mut bytes = serde_json::to_vec(&response).map_err(|_| Error::Invalid)?;
                if bytes.len() > 1024 * 1024 {
                    bytes = serde_json::to_vec(&Response {
                        value: serde_json::Value::Null,
                        error: Some(Error::ResourceLimit),
                    })
                    .map_err(|_| Error::Invalid)?;
                }
                client
                    .publish(reply, bytes.into())
                    .await
                    .map_err(|_| Error::Storage)?;
                client.flush().await.map_err(|_| Error::Storage)
            });
            while let Some(result) = tasks.try_join_next() {
                result.map_err(|_| Error::Storage)??;
            }
        }
        while let Some(result) = tasks.join_next().await {
            result.map_err(|_| Error::Storage)??;
        }
        Err(Error::Storage)
    }
}
