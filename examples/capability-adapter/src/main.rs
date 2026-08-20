//! Compile-tested application-owned adapter for an otherwise unrelated Rust library.

#![forbid(unsafe_code)]

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use plenora_runtime_capabilities::{
    CapabilityDispatcher, CapabilityDispatcherConfig, CapabilityFailure, CapabilityHandler,
    CapabilityId, CapabilityRegistryBuilder, CapabilityRegistryConfig, CapabilityRemoteEffect,
    CapabilityRequest, CapabilityResponse, ContractId, OperationName, OperationVersion,
};
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{
    CorrelationId, ExponentialBackoff, ExponentialBackoffConfig, MessageId, MessageMetadata,
    RetryErrorClass, SerializedMessage,
};
use plenora_runtime_worker::{
    TaskCancellationToken, TaskProgress, WorkerConcurrency, WorkerConfig, WorkerContext,
    WorkerExecutor,
};

const CAPABILITY_NAME: &str = "plenora.example-library";
const CAPABILITY_VERSION: u16 = 1;
const PROCESS_OPERATION: &str = "example.process";
const PROCESS_INPUT_CONTRACT: &str = "plenora-example-process-input-v1";
const CONTENT_TYPE: &str = "application/octet-stream";

/// Simulates an unfinished external Rust library owned by another repository.
#[derive(Clone, Debug, Default)]
struct ExampleLibrary {
    invocations: Arc<AtomicUsize>,
}

impl ExampleLibrary {
    async fn process(
        &self,
        input: &[u8],
        cancellation: TaskCancellationToken,
    ) -> Result<(), LibraryError> {
        let _previous = self.invocations.fetch_add(1, Ordering::AcqRel);
        tokio::task::yield_now().await;
        if cancellation.is_cancelled() {
            return Err(LibraryError::Cancelled);
        }
        match input {
            b"busy" => Err(LibraryError::TemporarilyUnavailable),
            b"uncertain" => Err(LibraryError::CommitUncertain),
            [] => Err(LibraryError::InvalidInput),
            _ => Ok(()),
        }
    }

    fn invocation_count(&self) -> usize {
        self.invocations.load(Ordering::Acquire)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LibraryError {
    InvalidInput,
    TemporarilyUnavailable,
    CommitUncertain,
    Cancelled,
}

impl Display for LibraryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidInput => "library input is invalid",
            Self::TemporarilyUnavailable => "library is temporarily unavailable",
            Self::CommitUncertain => "library result is uncertain",
            Self::Cancelled => "library operation was cancelled",
        })
    }
}

impl Error for LibraryError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdapterError {
    UnsupportedOperation,
    UnsupportedContentType,
    CancelledBeforeStart,
    CancelledAfterStart,
    Library(LibraryError),
}

impl Display for AdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnsupportedOperation => "adapter operation is unsupported",
            Self::UnsupportedContentType => "adapter content type is unsupported",
            Self::CancelledBeforeStart => "adapter was cancelled before invocation",
            Self::CancelledAfterStart => "adapter was cancelled during invocation",
            Self::Library(_) => "external library invocation failed",
        })
    }
}

impl Error for AdapterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Library(error) => Some(error),
            Self::UnsupportedOperation
            | Self::UnsupportedContentType
            | Self::CancelledBeforeStart
            | Self::CancelledAfterStart => None,
        }
    }
}

/// Application-owned translation layer. This type belongs in the final consumer.
#[derive(Clone, Debug)]
struct ExampleLibraryAdapter {
    library: ExampleLibrary,
}

impl ExampleLibraryAdapter {
    const fn new(library: ExampleLibrary) -> Self {
        Self { library }
    }

    fn reject_not_started(error: AdapterError, class: RetryErrorClass) -> CapabilityFailure {
        CapabilityFailure::new(class, CapabilityRemoteEffect::NotStarted, error)
    }

    fn map_library_failure(error: LibraryError) -> CapabilityFailure {
        let (class, effect) = match error {
            LibraryError::InvalidInput => (
                RetryErrorClass::Permanent,
                CapabilityRemoteEffect::NotStarted,
            ),
            LibraryError::TemporarilyUnavailable | LibraryError::Cancelled => (
                RetryErrorClass::Retryable,
                CapabilityRemoteEffect::NotStarted,
            ),
            LibraryError::CommitUncertain => (
                RetryErrorClass::OutcomeUnknown,
                CapabilityRemoteEffect::Unknown,
            ),
        };
        CapabilityFailure::new(class, effect, AdapterError::Library(error))
    }
}

#[async_trait]
impl CapabilityHandler for ExampleLibraryAdapter {
    async fn invoke(
        &self,
        context: WorkerContext,
        request: CapabilityRequest,
    ) -> Result<CapabilityResponse, CapabilityFailure> {
        if request.operation().as_str() != PROCESS_OPERATION {
            return Err(Self::reject_not_started(
                AdapterError::UnsupportedOperation,
                RetryErrorClass::DeadLetter,
            ));
        }
        if request.input().content_type.as_ref() != CONTENT_TYPE {
            return Err(Self::reject_not_started(
                AdapterError::UnsupportedContentType,
                RetryErrorClass::DeadLetter,
            ));
        }
        if context.shutdown.is_cancelled() || context.cancellation.is_cancelled() {
            return Err(Self::reject_not_started(
                AdapterError::CancelledBeforeStart,
                RetryErrorClass::Retryable,
            ));
        }

        let initial_progress = TaskProgress::new(0, NonZeroU64::new(1)).map_err(|error| {
            CapabilityFailure::new(
                RetryErrorClass::Permanent,
                CapabilityRemoteEffect::NotStarted,
                error,
            )
        })?;
        let _reported = context.report_progress(initial_progress);
        let input = request.into_input().bytes;
        let operation = self
            .library
            .process(input.as_ref(), context.cancellation.clone());
        tokio::pin!(operation);

        tokio::select! {
            biased;
            _reason = context.cancelled() => Err(CapabilityFailure::new(
                RetryErrorClass::OutcomeUnknown,
                CapabilityRemoteEffect::Unknown,
                AdapterError::CancelledAfterStart,
            )),
            result = &mut operation => {
                result.map_err(Self::map_library_failure)?;
                let completed_progress = TaskProgress::new(1, NonZeroU64::new(1)).map_err(|error| {
                    CapabilityFailure::new(
                        RetryErrorClass::OutcomeUnknown,
                        CapabilityRemoteEffect::Unknown,
                        error,
                    )
                })?;
                let _reported = context.report_progress(completed_progress);
                Ok(CapabilityResponse::acknowledged())
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(ServiceMetadata::new(
        "capability-adapter-example",
        env!("CARGO_PKG_VERSION"),
        "local-example",
    ));
    let library = ExampleLibrary::default();
    let dispatcher = dispatcher(library.clone())?;
    let executor = WorkerExecutor::new(
        dispatcher,
        ExponentialBackoff::new(ExponentialBackoffConfig::default())?,
        WorkerConfig::new(WorkerConcurrency::new(4)?, Duration::from_secs(5)),
    )?;

    executor
        .execute(
            worker_context(&runtime),
            request(PROCESS_OPERATION, CONTENT_TYPE, b"work-item")?,
        )
        .await?;
    println!(
        "generic capability completed; library_invocations={}",
        library.invocation_count()
    );

    let _shutdown_started = runtime.request_shutdown();
    let _worker_drain = executor.drain().await;
    let _runtime_drain = runtime.shutdown().await;
    Ok(())
}

fn dispatcher(library: ExampleLibrary) -> Result<CapabilityDispatcher, Box<dyn Error>> {
    let mut registry = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(4)?)?;
    registry.register(
        CapabilityId::new(CAPABILITY_NAME, CAPABILITY_VERSION)?,
        ExampleLibraryAdapter::new(library),
    )?;
    Ok(CapabilityDispatcher::new(
        registry.build(),
        CapabilityDispatcherConfig::new(1024 * 1024)?,
    )?)
}

fn request(
    operation: &str,
    content_type: &str,
    bytes: &'static [u8],
) -> Result<CapabilityRequest, Box<dyn Error>> {
    Ok(CapabilityRequest::new(
        CapabilityId::new(CAPABILITY_NAME, CAPABILITY_VERSION)?,
        OperationName::new(operation)?,
        OperationVersion::new(1)?,
        ContractId::new(PROCESS_INPUT_CONTRACT)?,
        SerializedMessage::new(content_type, bytes),
    ))
}

fn worker_context(runtime: &RuntimeHandle) -> WorkerContext {
    WorkerContext::new(
        MessageId::random(),
        CorrelationId::random(),
        None,
        1,
        MessageMetadata::new(),
        runtime.shutdown_signal(),
    )
}

#[cfg(test)]
mod tests {
    use plenora_runtime_messaging::{ClassifyRetry as _, RetryDecision, RetryPolicy as _};
    use plenora_runtime_worker::WorkerHandler as _;

    use super::*;

    #[tokio::test]
    async fn success_reaches_the_library_once() -> Result<(), Box<dyn Error>> {
        let runtime = RuntimeHandle::new(ServiceMetadata::new("test", "0.1.0", "instance"));
        let library = ExampleLibrary::default();
        let dispatcher = dispatcher(library.clone())?;

        dispatcher
            .handle(
                worker_context(&runtime),
                request(PROCESS_OPERATION, CONTENT_TYPE, b"valid")?,
            )
            .await?;

        assert_eq!(library.invocation_count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn invalid_routing_is_rejected_before_library_invocation() -> Result<(), Box<dyn Error>> {
        let runtime = RuntimeHandle::new(ServiceMetadata::new("test", "0.1.0", "instance"));
        let library = ExampleLibrary::default();
        let dispatcher = dispatcher(library.clone())?;

        let failure = dispatcher
            .handle(
                worker_context(&runtime),
                request("example.unsupported", CONTENT_TYPE, b"valid")?,
            )
            .await
            .err()
            .ok_or("unsupported operation unexpectedly succeeded")?;

        assert_eq!(failure.retry_class(), RetryErrorClass::DeadLetter);
        assert_eq!(failure.remote_effect(), CapabilityRemoteEffect::NotStarted);
        assert_eq!(library.invocation_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn failure_mapping_is_explicit_and_fail_closed() -> Result<(), Box<dyn Error>> {
        let runtime = RuntimeHandle::new(ServiceMetadata::new("test", "0.1.0", "instance"));
        let library = ExampleLibrary::default();
        let dispatcher = dispatcher(library)?;

        let transient = dispatcher
            .handle(
                worker_context(&runtime),
                request(PROCESS_OPERATION, CONTENT_TYPE, b"busy")?,
            )
            .await
            .err()
            .ok_or("transient failure unexpectedly succeeded")?;
        assert_eq!(transient.retry_class(), RetryErrorClass::Retryable);
        assert_eq!(
            transient.remote_effect(),
            CapabilityRemoteEffect::NotStarted
        );

        let uncertain = dispatcher
            .handle(
                worker_context(&runtime),
                request(PROCESS_OPERATION, CONTENT_TYPE, b"uncertain")?,
            )
            .await
            .err()
            .ok_or("uncertain failure unexpectedly succeeded")?;
        assert_eq!(uncertain.retry_class(), RetryErrorClass::OutcomeUnknown);
        assert_eq!(uncertain.remote_effect(), CapabilityRemoteEffect::Unknown);

        let retry = ExponentialBackoff::new(ExponentialBackoffConfig::default())?;
        assert_eq!(retry.decide(1, &uncertain), RetryDecision::DoNotRetry);
        Ok(())
    }
}
