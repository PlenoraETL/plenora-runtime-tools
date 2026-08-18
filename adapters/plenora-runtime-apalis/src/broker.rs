use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll},
};

use apalis::prelude::{
    BoxDynError, Error as ApalisError, Monitor, Request, RequestStream, WorkerBuilder,
    WorkerBuilderExt, WorkerFactory,
};
use futures_util::{StreamExt as _, stream};
use plenora_runtime_core::{Clock, ServiceMetadata, ShutdownSignal, SystemClock};
use plenora_runtime_messaging::{
    AckError, DeadLetter, DeadLetterPublishError, DeadLetterSink, Delivery,
    DeliveryHeartbeatConfig, MessageConsumer, MessageMetadata, MetadataKeyError, NackReason,
    PublishOutcome, RetryPolicy,
};
use plenora_runtime_worker::{
    TaskCancellationReason, TaskCancellationToken, TaskLifecycleObserver, WorkerAdmissionHandle,
    WorkerContext, WorkerHandler, WorkerInstanceHeartbeatConfig, WorkerInstanceHeartbeatObserver,
    WorkerInstanceHeartbeatReporter, WorkerInstanceIdentity, WorkerMessageDecoder,
    WorkerTaskControl,
};
use tokio::sync::Notify;
use tokio::time::{Instant, sleep};
use tower::Service;

use crate::{
    ApalisAdapterConfig, ApalisAdapterConfigError, ApalisDisposition, ApalisExecutionOutcome,
    ApalisJob, ApalisWorkerService,
};

type DeliveryFuture<HandlerError> =
    Pin<Box<dyn Future<Output = Result<ApalisExecutionOutcome<HandlerError>, ApalisError>> + Send>>;

/// Stable category for a broker-to-worker bridge failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerWorkerErrorKind {
    /// The serialized delivery could not be decoded into a typed worker message.
    Decode,
    /// Application and broker metadata could not be combined within portable bounds.
    Metadata,
    /// ACK, delayed NAK, or terminal settlement failed.
    Settlement,
    /// Decode failed and the subsequent terminal settlement also failed.
    DecodeSettlement,
    /// Metadata composition failed and the subsequent terminal settlement also failed.
    MetadataSettlement,
    /// Consecutive heartbeat failures exhausted the delivery's renewal budget.
    Heartbeat,
    /// Heartbeat renewal failed and the subsequent retryable settlement also failed.
    HeartbeatSettlement,
    /// A handler requested dead-letter routing but no sink was configured.
    DeadLetterUnavailable,
    /// The configured dead-letter sink rejected publication.
    DeadLetterPublish,
    /// The broker could not determine whether dead-letter publication took effect.
    DeadLetterOutcomeUnknown,
    /// Dead-letter publication was confirmed but terminating the original delivery failed.
    DeadLetterSettlement,
}

/// Stable category for a broker worker monitor failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerWorkerRunErrorKind {
    /// The concrete consumer returned a terminal polling error.
    Consumer,
    /// The Apalis monitor could not complete its lifecycle.
    Monitor,
}

/// Source-preserving failure returned by [`BrokerWorkerRunner::run`].
pub enum BrokerWorkerRunError<E> {
    /// Terminal error returned by the concrete broker consumer.
    Consumer(Arc<E>),
    /// Lifecycle error returned by the Apalis monitor.
    Monitor(std::io::Error),
}

impl<E> BrokerWorkerRunError<E> {
    /// Returns the stable failure category.
    #[must_use]
    pub const fn kind(&self) -> BrokerWorkerRunErrorKind {
        match self {
            Self::Consumer(_) => BrokerWorkerRunErrorKind::Consumer,
            Self::Monitor(_) => BrokerWorkerRunErrorKind::Monitor,
        }
    }

    /// Returns the concrete consumer source when polling failed.
    #[must_use]
    pub fn consumer_error(&self) -> Option<&E> {
        match self {
            Self::Consumer(error) => Some(error),
            Self::Monitor(_) => None,
        }
    }
}

impl<E> Debug for BrokerWorkerRunError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrokerWorkerRunError")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}

impl<E> Display for BrokerWorkerRunError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "broker worker runner failed during {:?}",
            self.kind()
        )
    }
}

impl<E> Error for BrokerWorkerRunError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Consumer(error) => Some(error.as_ref()),
            Self::Monitor(error) => Some(error),
        }
    }
}

/// Source-preserving and payload-redacted broker worker failure.
pub struct BrokerWorkerError<E> {
    kind: BrokerWorkerErrorKind,
    decoder: Option<E>,
    metadata: Option<MetadataKeyError>,
    heartbeat: Option<AckError>,
    dead_letter: Option<DeadLetterPublishError>,
    acknowledgement: Option<AckError>,
}

impl<E> BrokerWorkerError<E> {
    fn decode(error: E) -> Self {
        Self {
            kind: BrokerWorkerErrorKind::Decode,
            decoder: Some(error),
            metadata: None,
            heartbeat: None,
            dead_letter: None,
            acknowledgement: None,
        }
    }

    fn metadata(error: MetadataKeyError) -> Self {
        Self {
            kind: BrokerWorkerErrorKind::Metadata,
            decoder: None,
            metadata: Some(error),
            heartbeat: None,
            dead_letter: None,
            acknowledgement: None,
        }
    }

    fn settlement(error: AckError) -> Self {
        Self {
            kind: BrokerWorkerErrorKind::Settlement,
            decoder: None,
            metadata: None,
            heartbeat: None,
            dead_letter: None,
            acknowledgement: Some(error),
        }
    }

    fn decode_settlement(decoder: E, acknowledgement: AckError) -> Self {
        Self {
            kind: BrokerWorkerErrorKind::DecodeSettlement,
            decoder: Some(decoder),
            metadata: None,
            heartbeat: None,
            dead_letter: None,
            acknowledgement: Some(acknowledgement),
        }
    }

    fn metadata_settlement(metadata: MetadataKeyError, acknowledgement: AckError) -> Self {
        Self {
            kind: BrokerWorkerErrorKind::MetadataSettlement,
            decoder: None,
            metadata: Some(metadata),
            heartbeat: None,
            dead_letter: None,
            acknowledgement: Some(acknowledgement),
        }
    }

    fn heartbeat(error: AckError) -> Self {
        Self {
            kind: BrokerWorkerErrorKind::Heartbeat,
            decoder: None,
            metadata: None,
            heartbeat: Some(error),
            dead_letter: None,
            acknowledgement: None,
        }
    }

    fn heartbeat_settlement(heartbeat: AckError, acknowledgement: AckError) -> Self {
        Self {
            kind: BrokerWorkerErrorKind::HeartbeatSettlement,
            decoder: None,
            metadata: None,
            heartbeat: Some(heartbeat),
            dead_letter: None,
            acknowledgement: Some(acknowledgement),
        }
    }

    fn dead_letter_unavailable() -> Self {
        Self {
            kind: BrokerWorkerErrorKind::DeadLetterUnavailable,
            decoder: None,
            metadata: None,
            heartbeat: None,
            dead_letter: None,
            acknowledgement: None,
        }
    }

    fn dead_letter_publish(error: DeadLetterPublishError) -> Self {
        Self {
            kind: BrokerWorkerErrorKind::DeadLetterPublish,
            decoder: None,
            metadata: None,
            heartbeat: None,
            dead_letter: Some(error),
            acknowledgement: None,
        }
    }

    fn dead_letter_outcome_unknown() -> Self {
        Self {
            kind: BrokerWorkerErrorKind::DeadLetterOutcomeUnknown,
            decoder: None,
            metadata: None,
            heartbeat: None,
            dead_letter: None,
            acknowledgement: None,
        }
    }

    fn dead_letter_settlement(error: AckError) -> Self {
        Self {
            kind: BrokerWorkerErrorKind::DeadLetterSettlement,
            decoder: None,
            metadata: None,
            heartbeat: None,
            dead_letter: None,
            acknowledgement: Some(error),
        }
    }

    /// Returns the stable failure category.
    #[must_use]
    pub const fn kind(&self) -> BrokerWorkerErrorKind {
        self.kind
    }

    /// Returns the decoder source when decoding failed.
    #[must_use]
    pub const fn decoder_error(&self) -> Option<&E> {
        self.decoder.as_ref()
    }

    /// Returns the metadata source when composition failed.
    #[must_use]
    pub const fn metadata_error(&self) -> Option<&MetadataKeyError> {
        self.metadata.as_ref()
    }

    /// Returns the heartbeat source when lease renewal failed.
    #[must_use]
    pub const fn heartbeat_error(&self) -> Option<&AckError> {
        self.heartbeat.as_ref()
    }

    /// Returns the dead-letter publication source when publication failed.
    #[must_use]
    pub const fn dead_letter_error(&self) -> Option<&DeadLetterPublishError> {
        self.dead_letter.as_ref()
    }

    /// Returns the acknowledgement source when settlement failed.
    #[must_use]
    pub const fn acknowledgement_error(&self) -> Option<&AckError> {
        self.acknowledgement.as_ref()
    }
}

impl<E> Debug for BrokerWorkerError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrokerWorkerError")
            .field("kind", &self.kind)
            .field("has_decoder_source", &self.decoder.is_some())
            .field("has_metadata_source", &self.metadata.is_some())
            .field("has_heartbeat_source", &self.heartbeat.is_some())
            .field("has_dead_letter_source", &self.dead_letter.is_some())
            .field(
                "has_acknowledgement_source",
                &self.acknowledgement.is_some(),
            )
            .finish()
    }
}

impl<E> Display for BrokerWorkerError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "broker worker failed during {:?}", self.kind)
    }
}

impl<E> Error for BrokerWorkerError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(error) = self.acknowledgement.as_ref() {
            return Some(error);
        }
        if let Some(error) = self.heartbeat.as_ref() {
            return Some(error);
        }
        if let Some(error) = self.dead_letter.as_ref() {
            return Some(error);
        }
        if let Some(error) = self.metadata.as_ref() {
            return Some(error);
        }
        self.decoder
            .as_ref()
            .map(|error| error as &(dyn Error + 'static))
    }
}

/// Tower service that decodes an owned broker delivery, invokes a typed worker, and settles once.
pub struct BrokerDeliveryService<T, D, H, P> {
    decoder: Arc<D>,
    worker: ApalisWorkerService<H, P>,
    shutdown: ShutdownSignal,
    dead_letter_sink: Option<Arc<dyn DeadLetterSink>>,
    clock: Arc<dyn Clock>,
    message: PhantomData<fn() -> T>,
}

impl<T, D, H, P> BrokerDeliveryService<T, D, H, P> {
    /// Creates a delivery service with a bounded worker executor.
    ///
    /// # Errors
    ///
    /// Returns an error when the Apalis or worker configuration is invalid.
    pub fn new(
        decoder: D,
        handler: H,
        retry_policy: P,
        config: ApalisAdapterConfig,
        shutdown: ShutdownSignal,
    ) -> Result<Self, ApalisAdapterConfigError> {
        Ok(Self {
            decoder: Arc::new(decoder),
            worker: ApalisWorkerService::new(handler, retry_policy, config)?,
            shutdown,
            dead_letter_sink: None,
            clock: Arc::new(SystemClock),
            message: PhantomData,
        })
    }

    /// Creates a delivery service with explicit lifecycle observation.
    ///
    /// # Errors
    ///
    /// Returns an error when the Apalis or worker configuration is invalid.
    pub fn new_with_lifecycle<O, K>(
        decoder: D,
        handler: H,
        retry_policy: P,
        config: ApalisAdapterConfig,
        shutdown: ShutdownSignal,
        lifecycle_observer: Arc<O>,
        clock: Arc<K>,
    ) -> Result<Self, ApalisAdapterConfigError>
    where
        O: TaskLifecycleObserver + 'static,
        K: Clock + 'static,
    {
        let dead_letter_clock: Arc<dyn Clock> = clock.clone();
        Ok(Self {
            decoder: Arc::new(decoder),
            worker: ApalisWorkerService::new_with_lifecycle(
                handler,
                retry_policy,
                config,
                lifecycle_observer,
                clock,
            )?,
            shutdown,
            dead_letter_sink: None,
            clock: dead_letter_clock,
            message: PhantomData,
        })
    }

    /// Attaches a dedicated producer or custom sink for dead-letter records.
    ///
    /// The sink should publish to a destination that is separate from the operational subject.
    /// The original delivery is terminated only after the sink returns `Confirmed`.
    #[must_use]
    pub fn with_dead_letter_sink<S>(mut self, sink: S) -> Self
    where
        S: DeadLetterSink + 'static,
    {
        self.dead_letter_sink = Some(Arc::new(sink));
        self
    }

    /// Returns whether dead-letter routing is configured.
    #[must_use]
    pub const fn has_dead_letter_sink(&self) -> bool {
        self.dead_letter_sink.is_some()
    }

    /// Returns the underlying validated worker configuration.
    #[must_use]
    pub const fn config(&self) -> &ApalisAdapterConfig {
        self.worker.config()
    }

    /// Returns the number of typed handlers currently executing.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.worker.in_flight()
    }

    /// Returns the current reversible/permanent worker admission state.
    #[must_use]
    pub fn admission_state(&self) -> plenora_runtime_worker::WorkerAdmissionState {
        self.worker.admission_state()
    }

    /// Temporarily stops admission without cancelling active handlers.
    #[must_use]
    pub fn pause_admission(&self) -> bool {
        self.worker.pause_admission()
    }

    /// Reopens admission after a temporary pause.
    #[must_use]
    pub fn resume_admission(&self) -> bool {
        self.worker.resume_admission()
    }

    /// Returns a cloneable handle for listing and cancelling active handler invocations.
    #[must_use]
    pub fn task_control(&self) -> WorkerTaskControl {
        self.worker.task_control()
    }

    /// Returns a cloneable admission and capacity handle.
    #[must_use]
    pub fn admission_control(&self) -> WorkerAdmissionHandle {
        self.worker.admission_control()
    }

    /// Stops admission of new handler invocations.
    #[must_use]
    pub fn begin_drain(&self) -> bool {
        self.worker.begin_drain()
    }
}

impl<T, D, H, P> Clone for BrokerDeliveryService<T, D, H, P> {
    fn clone(&self) -> Self {
        Self {
            decoder: Arc::clone(&self.decoder),
            worker: self.worker.clone(),
            shutdown: self.shutdown.clone(),
            dead_letter_sink: self.dead_letter_sink.clone(),
            clock: Arc::clone(&self.clock),
            message: PhantomData,
        }
    }
}

impl<T, D, H, P> plenora_runtime_worker::WorkerAdmissionControl
    for BrokerDeliveryService<T, D, H, P>
where
    T: Send + Sync,
    D: Send + Sync,
    H: Send + Sync,
    P: Send + Sync,
{
    fn pause_admission(&self) -> bool {
        Self::pause_admission(self)
    }

    fn resume_admission(&self) -> bool {
        Self::resume_admission(self)
    }

    fn admission_state(&self) -> plenora_runtime_worker::WorkerAdmissionState {
        Self::admission_state(self)
    }
}

impl<T, D, H, P> Debug for BrokerDeliveryService<T, D, H, P> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrokerDeliveryService")
            .field("config", self.config())
            .field("in_flight", &self.in_flight())
            .field("admission_state", &self.admission_state())
            .field("has_dead_letter_sink", &self.has_dead_letter_sink())
            .finish_non_exhaustive()
    }
}

impl<T, D, H, P> Service<Request<Delivery, ()>> for BrokerDeliveryService<T, D, H, P>
where
    T: Send + 'static,
    D: WorkerMessageDecoder<T> + 'static,
    H: WorkerHandler<T> + 'static,
    P: RetryPolicy<H::Error> + 'static,
{
    type Response = ApalisExecutionOutcome<H::Error>;
    type Error = ApalisError;
    type Future = DeliveryFuture<H::Error>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Delivery, ()>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            service
                .execute(request.args)
                .await
                .map_err(broker_service_error)
        })
    }
}

impl<T, D, H, P> BrokerDeliveryService<T, D, H, P>
where
    T: Send + 'static,
    D: WorkerMessageDecoder<T> + 'static,
    H: WorkerHandler<T> + 'static,
    P: RetryPolicy<H::Error> + 'static,
{
    /// Decodes, invokes, and settles one delivery outside the Apalis monitor.
    ///
    /// # Errors
    ///
    /// Returns a source-preserving error for decode, metadata, or settlement failures.
    pub async fn execute(
        &self,
        mut delivery: Delivery,
    ) -> Result<ApalisExecutionOutcome<H::Error>, BrokerWorkerError<D::Error>> {
        let decoded = match self.decoder.decode(&delivery.message) {
            Ok(decoded) => decoded,
            Err(error) => return reject_decode(delivery, error).await,
        };
        let metadata = match combined_metadata(&delivery) {
            Ok(metadata) => metadata,
            Err(error) => return reject_metadata(delivery, error).await,
        };
        let cancellation = TaskCancellationToken::new();
        let context = WorkerContext::from_identity(
            decoded.identity,
            delivery.attempt,
            metadata,
            self.shutdown.clone(),
        )
        .with_cancellation(cancellation.clone());
        let execution = self
            .worker
            .execute(ApalisJob::new(context, decoded.message));
        let outcome = match delivery.heartbeat_config() {
            Some(config) => {
                match execute_with_heartbeat(&mut delivery, execution, config, &cancellation).await
                {
                    Ok(outcome) => outcome,
                    Err(error) => return reject_heartbeat(delivery, error).await,
                }
            }
            None => execution.await,
        };
        self.settle_delivery(delivery, outcome.disposition())
            .await?;
        Ok(outcome)
    }

    async fn settle_delivery(
        &self,
        delivery: Delivery,
        disposition: ApalisDisposition,
    ) -> Result<(), BrokerWorkerError<D::Error>> {
        match disposition {
            ApalisDisposition::Completed => {
                delivery.ack().await.map_err(BrokerWorkerError::settlement)
            }
            ApalisDisposition::RetryAfter(delay) => delivery
                .nack(NackReason::RetryAfter(delay))
                .await
                .map_err(BrokerWorkerError::settlement),
            ApalisDisposition::DoNotRetry => delivery
                .nack(NackReason::Permanent)
                .await
                .map_err(BrokerWorkerError::settlement),
            ApalisDisposition::Shutdown(_) => delivery
                .nack(NackReason::Shutdown)
                .await
                .map_err(BrokerWorkerError::settlement),
            ApalisDisposition::DeadLetter => self.publish_dead_letter(delivery).await,
        }
    }

    async fn publish_dead_letter(
        &self,
        delivery: Delivery,
    ) -> Result<(), BrokerWorkerError<D::Error>> {
        let Some(sink) = self.dead_letter_sink.as_ref() else {
            return Err(BrokerWorkerError::dead_letter_unavailable());
        };
        let dead_letter = DeadLetter {
            message: delivery.message.clone(),
            reason: Arc::from("handler_failed"),
            attempts: delivery.attempt,
            failed_at: self.clock.now().into(),
        };
        match sink.publish_dead_letter(dead_letter).await {
            Ok(PublishOutcome::Confirmed) => delivery
                .nack(NackReason::Permanent)
                .await
                .map_err(BrokerWorkerError::dead_letter_settlement),
            Ok(PublishOutcome::OutcomeUnknown) => {
                Err(BrokerWorkerError::dead_letter_outcome_unknown())
            }
            Err(error) => Err(BrokerWorkerError::dead_letter_publish(error)),
        }
    }
}

async fn execute_with_heartbeat<F, T>(
    delivery: &mut Delivery,
    execution: F,
    config: DeliveryHeartbeatConfig,
    cancellation: &TaskCancellationToken,
) -> Result<T, AckError>
where
    F: Future<Output = T>,
{
    let execution = execution;
    tokio::pin!(execution);
    let timer = sleep(config.interval());
    tokio::pin!(timer);
    let mut consecutive_failures = 0_u32;

    loop {
        tokio::select! {
            biased;
            outcome = &mut execution => return Ok(outcome),
            () = &mut timer => {
                match delivery.heartbeat().await {
                    Ok(()) => consecutive_failures = 0,
                    Err(error) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        if consecutive_failures >= config.max_consecutive_failures() {
                            let _started = cancellation.cancel(TaskCancellationReason::LeaseLost);
                            return Err(error);
                        }
                    }
                }
                timer.as_mut().reset(Instant::now() + config.interval());
            }
        }
    }
}

/// Apalis monitor that dynamically admits broker deliveries up to the configured concurrency.
pub struct BrokerWorkerRunner<T, C, D, H, P> {
    consumer: C,
    service: BrokerDeliveryService<T, D, H, P>,
    shutdown: ShutdownSignal,
    instance_heartbeat: Option<ActiveWorkerInstanceHeartbeat>,
}

#[derive(Clone, Debug)]
struct ActiveWorkerInstanceHeartbeat {
    config: WorkerInstanceHeartbeatConfig,
    reporter: WorkerInstanceHeartbeatReporter,
}

/// Lifecycle dependencies used by a broker-backed worker runner.
pub struct BrokerWorkerLifecycle<O, K> {
    observer: Arc<O>,
    clock: Arc<K>,
}

impl<O, K> BrokerWorkerLifecycle<O, K> {
    /// Groups an observer and clock for lifecycle-aware runner construction.
    #[must_use]
    pub const fn new(observer: Arc<O>, clock: Arc<K>) -> Self {
        Self { observer, clock }
    }

    fn into_parts(self) -> (Arc<O>, Arc<K>) {
        (self.observer, self.clock)
    }
}

impl<T, C, D, H, P> BrokerWorkerRunner<T, C, D, H, P> {
    /// Creates a broker-backed Apalis worker runner.
    ///
    /// # Errors
    ///
    /// Returns an error when the Apalis or worker configuration is invalid.
    pub fn new(
        consumer: C,
        decoder: D,
        handler: H,
        retry_policy: P,
        config: ApalisAdapterConfig,
        shutdown: ShutdownSignal,
    ) -> Result<Self, ApalisAdapterConfigError> {
        let service =
            BrokerDeliveryService::new(decoder, handler, retry_policy, config, shutdown.clone())?;
        Ok(Self {
            consumer,
            service,
            shutdown,
            instance_heartbeat: None,
        })
    }

    /// Creates a broker-backed runner with explicit lifecycle observation.
    ///
    /// # Errors
    ///
    /// Returns an error when the Apalis or worker configuration is invalid.
    pub fn new_with_lifecycle<O, K>(
        consumer: C,
        decoder: D,
        handler: H,
        retry_policy: P,
        config: ApalisAdapterConfig,
        shutdown: ShutdownSignal,
        lifecycle: BrokerWorkerLifecycle<O, K>,
    ) -> Result<Self, ApalisAdapterConfigError>
    where
        O: TaskLifecycleObserver + 'static,
        K: Clock + 'static,
    {
        let (lifecycle_observer, clock) = lifecycle.into_parts();
        let service = BrokerDeliveryService::new_with_lifecycle(
            decoder,
            handler,
            retry_policy,
            config,
            shutdown.clone(),
            lifecycle_observer,
            clock,
        )?;
        Ok(Self {
            consumer,
            service,
            shutdown,
            instance_heartbeat: None,
        })
    }

    /// Attaches periodic, payload-free worker-instance heartbeat observation.
    #[must_use]
    pub fn with_instance_heartbeat<O, K>(
        mut self,
        metadata: &ServiceMetadata,
        config: WorkerInstanceHeartbeatConfig,
        observer: Arc<O>,
        clock: Arc<K>,
    ) -> Self
    where
        O: WorkerInstanceHeartbeatObserver + 'static,
        K: Clock + 'static,
    {
        let identity =
            WorkerInstanceIdentity::new(metadata, Arc::from(self.service.config().worker_name()));
        let reporter = self
            .service
            .worker
            .instance_heartbeat_reporter(identity, observer, clock);
        self.instance_heartbeat = Some(ActiveWorkerInstanceHeartbeat { config, reporter });
        self
    }

    /// Attaches a dedicated producer or custom sink for dead-letter records.
    #[must_use]
    pub fn with_dead_letter_sink<S>(mut self, sink: S) -> Self
    where
        S: DeadLetterSink + 'static,
    {
        self.service = self.service.with_dead_letter_sink(sink);
        self
    }

    /// Returns the configured maximum number of concurrent handler invocations.
    #[must_use]
    pub const fn max_in_flight(&self) -> usize {
        self.service.config().worker().concurrency.max_in_flight
    }

    /// Returns the number of handler invocations currently executing.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.service.in_flight()
    }

    /// Returns a control handle that remains usable while this runner owns the service.
    #[must_use]
    pub fn task_control(&self) -> WorkerTaskControl {
        self.service.task_control()
    }

    /// Returns a cloneable admission and capacity handle that survives moving the runner.
    #[must_use]
    pub fn admission_control(&self) -> WorkerAdmissionHandle {
        self.service.admission_control()
    }

    /// Returns whether dead-letter routing is configured.
    #[must_use]
    pub const fn has_dead_letter_sink(&self) -> bool {
        self.service.has_dead_letter_sink()
    }
}

impl<T, C, D, H, P> Debug for BrokerWorkerRunner<T, C, D, H, P> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrokerWorkerRunner")
            .field("service", &self.service)
            .field("has_instance_heartbeat", &self.instance_heartbeat.is_some())
            .finish_non_exhaustive()
    }
}

impl<T, C, D, H, P> BrokerWorkerRunner<T, C, D, H, P>
where
    T: Send + 'static,
    C: MessageConsumer + 'static,
    D: WorkerMessageDecoder<T> + 'static,
    H: WorkerHandler<T> + 'static,
    P: RetryPolicy<H::Error> + 'static,
{
    /// Polls only when Apalis has capacity, runs handlers, and settles each owned delivery once.
    ///
    /// Runtime shutdown stops broker polling before the configured bounded drain period begins.
    ///
    /// # Errors
    ///
    /// Returns a source-preserving error if broker polling or the Apalis monitor fails.
    pub async fn run(self) -> Result<(), BrokerWorkerRunError<C::Error>> {
        let Self {
            consumer,
            service,
            shutdown,
            instance_heartbeat,
        } = self;
        if let Some(instance) = instance_heartbeat.as_ref() {
            let _starting = instance.reporter.heartbeat();
        }
        if shutdown.is_cancelled() {
            let _started = service.begin_drain();
            if let Some(instance) = instance_heartbeat.as_ref() {
                let _stopped = instance.reporter.mark_stopped();
            }
            return Ok(());
        }

        let config = service.config().clone();
        let max_in_flight = config.worker().concurrency.max_in_flight;
        let grace_period = config.worker().shutdown_grace_period;
        let consumer_failure = Arc::new(ConsumerFailure::new());
        let signal_failure = Arc::clone(&consumer_failure);
        let signal_service = service.clone();
        let signal_reporter = instance_heartbeat
            .as_ref()
            .map(|instance| instance.reporter.clone());
        let signal = async move {
            tokio::select! {
                () = shutdown.cancelled() => {}
                () = signal_failure.failed() => {}
            }
            let _started = signal_service.begin_drain();
            if let Some(reporter) = signal_reporter {
                let _draining = reporter.mark_draining();
            }
            Ok::<(), std::io::Error>(())
        };
        let worker = WorkerBuilder::new(config.worker_name())
            .concurrency(max_in_flight)
            .catch_panic()
            .backend(delivery_stream(consumer, Arc::clone(&consumer_failure)))
            .build(service);

        if let Some(instance) = instance_heartbeat.as_ref() {
            let _ready = instance.reporter.mark_ready();
        }
        let monitor = Monitor::new()
            .with_terminator(tokio::time::sleep(grace_period))
            .register(worker)
            .run_with_signal(signal);
        let monitor_result = match instance_heartbeat.as_ref() {
            Some(instance) => {
                run_with_instance_heartbeats(
                    monitor,
                    instance.reporter.clone(),
                    instance.config.interval(),
                )
                .await
            }
            None => monitor.await,
        };
        if let Some(instance) = instance_heartbeat.as_ref() {
            let _stopped = instance.reporter.mark_stopped();
        }
        monitor_result.map_err(BrokerWorkerRunError::Monitor)?;
        if let Some(error) = consumer_failure.take() {
            Err(BrokerWorkerRunError::Consumer(error))
        } else {
            Ok(())
        }
    }
}

async fn run_with_instance_heartbeats<F, T>(
    monitor: F,
    reporter: WorkerInstanceHeartbeatReporter,
    interval: std::time::Duration,
) -> T
where
    F: Future<Output = T>,
{
    tokio::pin!(monitor);
    loop {
        tokio::select! {
            result = &mut monitor => return result,
            () = sleep(interval) => {
                let _heartbeat = reporter.heartbeat();
            }
        }
    }
}

struct ConsumerFailure<E> {
    error: Mutex<Option<Arc<E>>>,
    changed: Notify,
}

impl<E> ConsumerFailure<E> {
    const fn new() -> Self {
        Self {
            error: Mutex::new(None),
            changed: Notify::const_new(),
        }
    }

    fn record(&self, error: E) {
        let _previous = self.lock().replace(Arc::new(error));
        self.changed.notify_waiters();
    }

    fn take(&self) -> Option<Arc<E>> {
        self.lock().take()
    }

    async fn failed(&self) {
        loop {
            let changed = self.changed.notified();
            if self.lock().is_some() {
                return;
            }
            changed.await;
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<Arc<E>>> {
        match self.error.lock() {
            Ok(error) => error,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

fn delivery_stream<C>(
    consumer: C,
    failure: Arc<ConsumerFailure<C::Error>>,
) -> RequestStream<Request<Delivery, ()>>
where
    C: MessageConsumer + 'static,
{
    stream::unfold((Some(consumer), failure), |(state, failure)| async move {
        let mut consumer = state?;
        match consumer.receive().await {
            Ok(Some(delivery)) => {
                Some((Ok(Some(Request::new(delivery))), (Some(consumer), failure)))
            }
            Ok(None) => None,
            Err(error) => {
                failure.record(error);
                None
            }
        }
    })
    .boxed()
}

fn broker_service_error<E>(error: BrokerWorkerError<E>) -> ApalisError
where
    E: Error + Send + Sync + 'static,
{
    let error: BoxDynError = Box::new(error);
    ApalisError::ServiceError(Arc::new(error))
}

fn combined_metadata(delivery: &Delivery) -> Result<MessageMetadata, MetadataKeyError> {
    let mut metadata = delivery.message.headers.clone();
    for (key, value) in delivery.broker_metadata.iter() {
        let _replaced = metadata.insert(key, value.clone())?;
    }
    Ok(metadata)
}

async fn reject_decode<E, H>(
    delivery: Delivery,
    error: E,
) -> Result<ApalisExecutionOutcome<H>, BrokerWorkerError<E>> {
    match delivery.nack(NackReason::ConsumerRejected).await {
        Ok(()) => Err(BrokerWorkerError::decode(error)),
        Err(acknowledgement) => Err(BrokerWorkerError::decode_settlement(error, acknowledgement)),
    }
}

async fn reject_metadata<E, H>(
    delivery: Delivery,
    error: MetadataKeyError,
) -> Result<ApalisExecutionOutcome<H>, BrokerWorkerError<E>> {
    match delivery.nack(NackReason::ConsumerRejected).await {
        Ok(()) => Err(BrokerWorkerError::metadata(error)),
        Err(acknowledgement) => Err(BrokerWorkerError::metadata_settlement(
            error,
            acknowledgement,
        )),
    }
}

async fn reject_heartbeat<E, H>(
    delivery: Delivery,
    heartbeat: AckError,
) -> Result<ApalisExecutionOutcome<H>, BrokerWorkerError<E>> {
    match delivery.nack(NackReason::Retryable).await {
        Ok(()) => Err(BrokerWorkerError::heartbeat(heartbeat)),
        Err(acknowledgement) => Err(BrokerWorkerError::heartbeat_settlement(
            heartbeat,
            acknowledgement,
        )),
    }
}
