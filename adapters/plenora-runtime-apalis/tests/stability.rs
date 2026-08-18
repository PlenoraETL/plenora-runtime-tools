//! Deterministic bounded-load stability coverage for the broker worker runner.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use plenora_runtime_apalis::{ApalisAdapterConfig, BrokerWorkerRunner};
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{
    CORRELATION_ID_METADATA_KEY, CorrelationId, MESSAGE_ID_METADATA_KEY, MessageCodec, MessageId,
    MessageMetadata, RetryDecision, RetryPolicy, SerializedMessage,
};
use plenora_runtime_testkit::{AckEvent, FakeBroker, FakeBrokerLimits, ManualClock};
use plenora_runtime_worker::{
    MetadataMessageDecoder, WorkerConcurrency, WorkerConfig, WorkerContext, WorkerHandler,
};
use tokio::{
    sync::{Notify, Semaphore},
    time::timeout,
};

const JOBS: usize = 2_048;
const MAX_IN_FLIGHT: usize = 32;
const STABILITY_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug)]
struct StabilityError(&'static str);

impl Display for StabilityError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for StabilityError {}

#[derive(Clone, Copy, Debug)]
struct TextCodec;

impl MessageCodec<String> for TextCodec {
    type Error = StabilityError;

    fn encode(&self, value: &String) -> Result<SerializedMessage, Self::Error> {
        Ok(SerializedMessage::new("text/plain", value.clone()))
    }

    fn decode(&self, message: &SerializedMessage) -> Result<String, Self::Error> {
        String::from_utf8(message.bytes.to_vec())
            .map_err(|_error| StabilityError("message is not UTF-8"))
    }
}

#[derive(Clone, Copy, Debug)]
struct NoRetry;

impl RetryPolicy<StabilityError> for NoRetry {
    fn decide(&self, _attempt: u32, _error: &StabilityError) -> RetryDecision {
        RetryDecision::DoNotRetry
    }
}

#[derive(Clone, Debug)]
struct GateHandler {
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    changed: Arc<Notify>,
    release: Arc<Semaphore>,
}

#[async_trait]
impl WorkerHandler<String> for GateHandler {
    type Error = StabilityError;

    async fn handle(&self, _context: WorkerContext, _message: String) -> Result<(), Self::Error> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        self.changed.notify_waiters();
        let _active = ActiveGuard {
            active: Arc::clone(&self.active),
            changed: Arc::clone(&self.changed),
        };
        let permit = self
            .release
            .acquire()
            .await
            .map_err(|_error| StabilityError("release gate closed"))?;
        permit.forget();
        self.completed.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_waiters();
        Ok(())
    }
}

struct ActiveGuard {
    active: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the stability scenario keeps admission, saturation, release, and final invariants together"
)]
async fn sustained_load_never_exceeds_capacity_or_loses_a_delivery() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(ServiceMetadata::new(
        "broker-stability-test",
        "0.1.0",
        "stability-instance",
    ));
    let broker = FakeBroker::with_limits(
        ManualClock::default(),
        FakeBrokerLimits {
            max_pending_deliveries: JOBS,
            max_catalog_entries: JOBS,
            max_acknowledgement_history: JOBS,
            max_terminal_history: JOBS,
            ..FakeBrokerLimits::default()
        },
    );
    for index in 0..JOBS {
        let _delivery_id = broker.enqueue(worker_message(index)?)?;
    }

    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let changed = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let runner = BrokerWorkerRunner::new(
        broker.consumer(),
        MetadataMessageDecoder::<_, String>::new(TextCodec),
        GateHandler {
            active: Arc::clone(&active),
            peak: Arc::clone(&peak),
            completed: Arc::clone(&completed),
            changed: Arc::clone(&changed),
            release: Arc::clone(&release),
        },
        NoRetry,
        ApalisAdapterConfig::new(
            "broker-stability-test",
            WorkerConfig::new(
                WorkerConcurrency::new(MAX_IN_FLIGHT)?,
                Duration::from_secs(1),
            ),
        )?,
        runtime.shutdown_signal(),
    )?;
    let task_control = runner.task_control();

    let control = async {
        wait_for(&active, &changed, MAX_IN_FLIGHT).await?;
        let saturated = broker.snapshot();
        if saturated.in_flight_deliveries != MAX_IN_FLIGHT
            || saturated.pending_deliveries != JOBS - MAX_IN_FLIGHT
        {
            return Err(StabilityError(
                "broker did not preserve bounded backpressure",
            ));
        }
        if task_control.active_tasks().len() != MAX_IN_FLIGHT {
            return Err(StabilityError(
                "task registry did not match active handlers",
            ));
        }

        release.add_permits(JOBS);
        wait_for(&completed, &changed, JOBS).await?;
        let _shutdown_started = runtime.request_shutdown();
        Ok::<(), StabilityError>(())
    };

    let (run_result, control_result) = timeout(STABILITY_TIMEOUT, async {
        tokio::join!(runner.run(), control)
    })
    .await
    .map_err(|_elapsed| StabilityError("bounded load scenario timed out"))?;
    run_result?;
    control_result?;

    let final_snapshot = broker.snapshot();
    assert_eq!(completed.load(Ordering::SeqCst), JOBS);
    assert_eq!(peak.load(Ordering::SeqCst), MAX_IN_FLIGHT);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(final_snapshot.pending_deliveries, 0);
    assert_eq!(final_snapshot.in_flight_deliveries, 0);
    assert_eq!(final_snapshot.acknowledgement_count, JOBS);
    assert!(task_control.active_tasks().is_empty());
    assert!(
        broker
            .acknowledgement_records()
            .iter()
            .all(|record| record.event == AckEvent::Acked)
    );
    Ok(())
}

async fn wait_for(
    counter: &AtomicUsize,
    changed: &Notify,
    expected: usize,
) -> Result<(), StabilityError> {
    timeout(Duration::from_secs(10), async {
        loop {
            let notified = changed.notified();
            if counter.load(Ordering::SeqCst) >= expected {
                return;
            }
            notified.await;
        }
    })
    .await
    .map_err(|_elapsed| StabilityError("worker progress timed out"))
}

fn worker_message(index: usize) -> Result<SerializedMessage, Box<dyn Error>> {
    let mut metadata = MessageMetadata::new();
    let _previous =
        metadata.insert_text(MESSAGE_ID_METADATA_KEY, MessageId::random().to_string())?;
    let _previous = metadata.insert_text(
        CORRELATION_ID_METADATA_KEY,
        CorrelationId::random().to_string(),
    )?;
    Ok(SerializedMessage::new("text/plain", format!("job-{index}")).with_headers(metadata))
}
