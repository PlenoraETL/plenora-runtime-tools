//! Process-shaped acceptance test for an embedding Plenora consumer.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    net::SocketAddr,
    time::Duration,
};

use axum::{Router, routing::get};
use plenora_runtime_apalis::{ApalisAdapterConfig, BrokerWorkerRunner};
use plenora_runtime_capabilities::{
    CapabilityDispatchError, CapabilityDispatcher, CapabilityDispatcherConfig, CapabilityId,
    CapabilityMessageCodec, CapabilityRegistryBuilder, CapabilityRegistryConfig, CapabilityRequest,
    OperationName,
};
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_http::{HttpBootstrap, HttpServeOutcome, HttpServerConfig};
use plenora_runtime_messaging::{
    CORRELATION_ID_METADATA_KEY, CorrelationId, MESSAGE_ID_METADATA_KEY, MessageCodec, MessageId,
    RetryDecision, RetryPolicy, SerializedMessage,
};
use plenora_runtime_testkit::{AckEvent, FakeBroker, FakeCapabilityHandler, ManualClock};
use plenora_runtime_worker::{MetadataMessageDecoder, WorkerConcurrency, WorkerConfig};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

#[derive(Clone, Copy, Debug)]
struct TestError(&'static str);

impl Display for TestError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for TestError {}

#[derive(Clone, Copy, Debug)]
struct NoRetry;

impl RetryPolicy<CapabilityDispatchError> for NoRetry {
    fn decide(&self, _attempt: u32, _error: &CapabilityDispatchError) -> RetryDecision {
        RetryDecision::DoNotRetry
    }
}

#[tokio::test]
async fn consumer_process_serves_readiness_processes_work_and_stops_together()
-> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(ServiceMetadata::new(
        "consumer-process-test",
        "0.1.0",
        "test-instance",
    ));
    let broker = FakeBroker::new(ManualClock::default());
    let _delivery_id = broker.enqueue(worker_message("consumer-poc")?)?;
    let capability = FakeCapabilityHandler::default();
    let mut capabilities = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(1)?)?;
    capabilities.register(
        CapabilityId::new("plenora.example-tools", 1)?,
        capability.clone(),
    )?;
    let dispatcher =
        CapabilityDispatcher::new(capabilities.build(), CapabilityDispatcherConfig::new(1024)?)?;
    let runner = BrokerWorkerRunner::new(
        broker.consumer(),
        MetadataMessageDecoder::<_, CapabilityRequest>::new(CapabilityMessageCodec),
        dispatcher,
        NoRetry,
        ApalisAdapterConfig::new(
            "consumer-process-test",
            WorkerConfig::new(WorkerConcurrency::new(2)?, Duration::from_secs(2)),
        )?,
        runtime.shutdown_signal(),
    )?;

    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let address = listener.local_addr()?;
    let http = HttpBootstrap::new(
        &runtime,
        HttpServerConfig::new(address, Duration::from_secs(2))?,
    )?;
    let application = Router::new().route("/", get(|| async { "consumer-poc" }));

    let control = async {
        wait_for_count(&capability).await?;
        let readiness = http_get(address, "/ready").await?;
        if !readiness.starts_with("HTTP/1.1 200 OK") || !readiness.contains(r#"{"status":"ready"}"#)
        {
            return Err(TestError("readiness endpoint did not report ready"));
        }
        let _shutdown_started = runtime.request_shutdown();
        Ok::<(), TestError>(())
    };

    let (worker_result, http_result, control_result) = tokio::join!(
        runner.run(),
        http.serve_listener(listener, application),
        control,
    );
    worker_result?;
    assert_eq!(http_result?, HttpServeOutcome::GracefulShutdown);
    control_result?;
    let invocations = capability.invocations();
    assert_eq!(invocations.len(), 1);
    assert_eq!(
        invocations
            .first()
            .map(|invocation| invocation.capability.clone()),
        Some(CapabilityId::new("plenora.example-tools", 1)?)
    );
    let acknowledgements = broker.acknowledgement_records();
    assert_eq!(acknowledgements.len(), 1);
    assert_eq!(
        acknowledgements.first().map(|record| record.event),
        Some(AckEvent::Acked)
    );

    Ok(())
}

fn worker_message(payload: &str) -> Result<SerializedMessage, Box<dyn Error>> {
    let request = CapabilityRequest::new(
        CapabilityId::new("plenora.example-tools", 1)?,
        OperationName::new("execute")?,
        SerializedMessage::new("application/octet-stream", payload.to_owned()),
    );
    let mut message = CapabilityMessageCodec.encode(&request)?;
    let _previous = message
        .headers
        .insert_text(MESSAGE_ID_METADATA_KEY, MessageId::random().to_string())?;
    let _previous = message.headers.insert_text(
        CORRELATION_ID_METADATA_KEY,
        CorrelationId::random().to_string(),
    )?;
    Ok(message)
}

async fn wait_for_count(capability: &FakeCapabilityHandler) -> Result<(), TestError> {
    timeout(Duration::from_secs(2), async {
        loop {
            if capability.snapshot().invocation_count >= 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_elapsed| TestError("worker did not complete in time"))
}

async fn http_get(address: SocketAddr, path: &str) -> Result<String, TestError> {
    let mut stream = TcpStream::connect(address)
        .await
        .map_err(|_error| TestError("HTTP connection failed"))?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|_error| TestError("HTTP request failed"))?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .map_err(|_error| TestError("HTTP response failed"))?;
    String::from_utf8(response).map_err(|_error| TestError("HTTP response is not UTF-8"))
}
