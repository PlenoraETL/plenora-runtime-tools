//! Remote observation of the durable scripted host; local pull settlement is not a broker ack.
use crate::ScriptedHost;
use async_trait::async_trait;
use futures_util::StreamExt;
use plenora_runtime_chains::{Error, Request, Response, Transport};
use std::{sync::Arc, time::Duration};

fn request_failure(kind: async_nats::RequestErrorKind) -> Error {
    use async_nats::RequestErrorKind;
    if std::env::var_os("PLENORA_STORAGE_DIAGNOSTICS").is_some_and(|v| v == "1") {
        // Only the closed driver category, never its source, subject or payload.
        eprintln!("Remote request failure; kind={kind:?}");
    }
    match kind {
        RequestErrorKind::TimedOut => Error::Timeout,
        RequestErrorKind::InvalidSubject => Error::Invalid,
        RequestErrorKind::MaxPayloadExceeded => Error::ResourceLimit,
        RequestErrorKind::NoResponders | RequestErrorKind::Other => Error::Storage,
    }
}

/// Remote scripted client. Ten seconds is an observation timeout, never a run deadline.
pub struct RemoteClient {
    client: async_nats::Client,
    subject: String,
    observation_timeout: Duration,
}
impl RemoteClient {
    /// Bind a host-configured subject; wildcards and whitespace are refused.
    /// # Errors
    /// Returns data-free transport errors, without connection strings or credentials.
    pub async fn connect(server: &str, subject: &str) -> Result<Self, Error> {
        Self::connect_with_timeout(server, subject, 10_000).await
    }
    /// Configure the observation wait in milliseconds, independently of run deadlines.
    /// # Errors
    /// Rejects zero or more than one day; expiry never cancels or retries accepted work.
    pub async fn connect_with_timeout(
        server: &str,
        subject: &str,
        observation_timeout_ms: u64,
    ) -> Result<Self, Error> {
        if !(1..=86_400_000).contains(&observation_timeout_ms) {
            return Err(Error::Invalid);
        }
        if subject.is_empty()
            || subject.len() > 256
            || subject
                .bytes()
                .any(|b| !b.is_ascii_graphic() || matches!(b, b'*' | b'>'))
        {
            return Err(Error::Invalid);
        }
        let client = tokio::time::timeout(
            Duration::from_secs(10),
            async_nats::ConnectOptions::new()
                .request_timeout(None)
                .connect(server),
        )
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|_| Error::Storage)?;
        Ok(Self {
            client,
            subject: subject.into(),
            observation_timeout: Duration::from_millis(observation_timeout_ms),
        })
    }
    /// Send exact facade bytes; no domain algorithms or result rewriting live in this client.
    /// # Errors
    /// Missing response is uncertainty, not proof of failed or cancelled execution.
    pub async fn call(&self, bytes: Vec<u8>) -> Result<Response, Error> {
        if bytes.len() > 1024 * 1024 {
            return Err(Error::ResourceLimit);
        }
        let request: Request = serde_json::from_slice(&bytes).map_err(|_| Error::Invalid)?;
        let message = crate::control::message(request)?;
        let bytes = serde_json::to_vec(&crate::control::Wire::capture(&message))
            .map_err(|_| Error::Invalid)?;
        let reply = tokio::time::timeout(
            self.observation_timeout,
            self.client.request(self.subject.clone(), bytes.into()),
        )
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|error| request_failure(error.kind()))?;
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
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let listener_stop = stopped.clone();
        let worker = async {
            let result = self.work(stopped).await;
            if let Err(error) = &result
                && std::env::var_os("PLENORA_STORAGE_DIAGNOSTICS").is_some_and(|v| v == "1")
            {
                eprintln!("Host worker stopped; category={}", error.code());
            }
            let _closed = stop.send(true);
            result
        };
        let listener = async {
            let result = drive(
                &mut subscription,
                |message: async_nats::Message| {
                    let host = self.clone();
                    let client = remote.client.clone();
                    async move {
                        let reply = message.reply.ok_or(Error::Invalid)?;
                        let response = host.call_wire(&message.payload).await;
                        let mut bytes =
                            serde_json::to_vec(&response).map_err(|_| Error::Invalid)?;
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
                    }
                },
                Some(listener_stop),
            )
            .await;
            let _closed = stop.send(true);
            result
        };
        let (result, worker_result) = tokio::join!(listener, worker);
        if let Err(error) = &result
            && std::env::var_os("PLENORA_STORAGE_DIAGNOSTICS").is_some_and(|v| v == "1")
        {
            eprintln!("Host listener stopped; category={}", error.code());
        }
        let result = result.and(worker_result);
        // No new work is consumed after drive observes an error; all accepted tasks are drained.
        let unsubscribe = subscription.unsubscribe().await.map_err(|_| Error::Storage);
        result.and(unsubscribe)
    }
}

async fn drive<S, F, R>(
    stream: &mut S,
    mut invoke: F,
    mut stop: Option<tokio::sync::watch::Receiver<bool>>,
) -> Result<(), Error>
where
    S: futures_util::Stream + Unpin,
    F: FnMut(S::Item) -> R,
    R: std::future::Future<Output = Result<(), Error>> + Send + 'static,
{
    let mut tasks = tokio::task::JoinSet::new();
    let failure = loop {
        let message = tokio::select! {
            () = async {
                if let Some(receiver) = &mut stop {
                    while !*receiver.borrow() {
                        if receiver.changed().await.is_err() { break; }
                    }
                } else { std::future::pending::<()>().await; }
            } => break None,
            result = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(error) = task_error(result.as_ref()) {
                    break Some(error);
                }
                continue;
            }
            message = stream.next(), if tasks.len() < 16 => {
                let Some(message) = message else { break None };
                message
            }
        };
        tasks.spawn(invoke(message));
    };
    // A failed publication cannot drop JoinSet and abort sibling handlers.
    drain(&mut tasks, failure).await
}

fn task_error(result: Option<&Result<Result<(), Error>, tokio::task::JoinError>>) -> Option<Error> {
    match result {
        Some(Ok(Ok(()))) => None,
        Some(Ok(Err(error))) => Some(*error),
        Some(Err(_)) | None => Some(Error::Storage),
    }
}

async fn drain(
    tasks: &mut tokio::task::JoinSet<Result<(), Error>>,
    mut failure: Option<Error>,
) -> Result<(), Error> {
    while let Some(result) = tasks.join_next().await {
        if let Some(error) = task_error(Some(&result)) {
            failure.get_or_insert(error);
        }
    }
    Err(failure.unwrap_or(Error::Storage))
}

#[cfg(test)]
mod tests {
    #[test]
    fn transport_categories_do_not_hide_timeout_as_storage() {
        use async_nats::RequestErrorKind as K;
        use plenora_runtime_chains::Error as E;
        for (kind, expected) in [
            (K::TimedOut, E::Timeout),
            (K::InvalidSubject, E::Invalid),
            (K::MaxPayloadExceeded, E::ResourceLimit),
            (K::NoResponders, E::Storage),
            (K::Other, E::Storage),
        ] {
            assert_eq!(super::request_failure(kind), expected);
        }
    }

    use super::*;

    #[tokio::test]
    async fn publication_failure_drains_all_accepted_work() {
        let mut stream = futures_util::stream::iter([0, 1]).chain(futures_util::stream::pending());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let gate = release.clone();
        let done = completed.clone();
        let (observed, failure) = tokio::sync::oneshot::channel();
        let mut observed = Some(observed);
        let task = tokio::spawn(async move {
            drive(
                &mut stream,
                |item| {
                    let entered = entered.clone();
                    let release = release.clone();
                    let completed = completed.clone();
                    let observed = if item == 0 { observed.take() } else { None };
                    async move {
                        if item == 0 {
                            entered.notified().await;
                            if let Some(observed) = observed {
                                observed.send(()).map_err(|()| Error::Invalid)?;
                            }
                            Err(Error::Storage)
                        } else {
                            entered.notify_one();
                            release.notified().await;
                            completed.store(true, std::sync::atomic::Ordering::SeqCst);
                            Ok(())
                        }
                    }
                },
                None,
            )
            .await
        });
        assert!(failure.await.is_ok());
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        gate.notify_one();
        assert_eq!(task.await.ok(), Some(Err(Error::Storage)));
        assert!(done.load(std::sync::atomic::Ordering::SeqCst));
    }
}
