//! Bounded, non-blocking lifecycle handoff tests.

use std::{error::Error, time::SystemTime};

use plenora_runtime_core::{HealthRegistry, HealthStatus, ReadinessStatus, ServiceMetadata};
use plenora_runtime_messaging::{CorrelationId, MessageId};
use plenora_runtime_worker::{
    MAX_WORKER_LIFECYCLE_CHANNEL_CAPACITY, TaskLifecycleEvent, TaskLifecycleEventKind,
    TaskLifecycleObserver, TaskState, WorkerInstanceHeartbeat, WorkerInstanceHeartbeatObserver,
    WorkerInstanceIdentity, WorkerInstanceStatus, WorkerLifecycleChannelConfig,
    WorkerLifecycleChannelConfigError, WorkerLifecycleDispatchState, WorkerLifecycleDispatcher,
    WorkerLifecycleHealthCriticality, WorkerLifecycleHealthReporter, WorkerLifecycleObservation,
};

#[test]
fn lifecycle_channel_rejects_zero_and_excessive_memory_bounds() {
    assert_eq!(
        WorkerLifecycleChannelConfig::new(0),
        Err(WorkerLifecycleChannelConfigError::ZeroCapacity)
    );
    assert_eq!(
        WorkerLifecycleChannelConfig::new(MAX_WORKER_LIFECYCLE_CHANNEL_CAPACITY + 1),
        Err(WorkerLifecycleChannelConfigError::CapacityTooLarge {
            capacity: MAX_WORKER_LIFECYCLE_CHANNEL_CAPACITY + 1,
            maximum: MAX_WORKER_LIFECYCLE_CHANNEL_CAPACITY,
        })
    );
}

#[tokio::test]
async fn required_dispatch_health_tracks_open_saturated_recovered_and_closed()
-> Result<(), Box<dyn Error>> {
    let registry = HealthRegistry::new();
    let reporter = WorkerLifecycleHealthReporter::new(
        registry.clone(),
        "worker.lifecycle",
        WorkerLifecycleHealthCriticality::Required,
    );
    let (dispatcher, mut receiver) =
        WorkerLifecycleDispatcher::channel(WorkerLifecycleChannelConfig::new(1)?);

    reporter.refresh(dispatcher.snapshot());
    assert_eq!(registry.health().status, HealthStatus::Healthy);
    assert_eq!(registry.readiness().status, ReadinessStatus::Ready);

    TaskLifecycleObserver::record(&dispatcher, task_event(1));
    reporter.refresh(dispatcher.snapshot());
    assert_eq!(registry.health().status, HealthStatus::Degraded);
    assert_eq!(registry.readiness().status, ReadinessStatus::NotReady);

    let _observation = receiver.recv().await;
    reporter.refresh(dispatcher.snapshot());
    assert_eq!(registry.health().status, HealthStatus::Healthy);
    assert_eq!(registry.readiness().status, ReadinessStatus::Ready);

    receiver.close();
    reporter.refresh(dispatcher.snapshot());
    assert_eq!(registry.health().status, HealthStatus::Unhealthy);
    assert_eq!(registry.readiness().status, ReadinessStatus::NotReady);

    reporter.remove();
    assert_eq!(registry.health().status, HealthStatus::Healthy);
    assert_eq!(registry.readiness().status, ReadinessStatus::Ready);
    Ok(())
}

#[tokio::test]
async fn dispatcher_preserves_order_and_reports_full_then_closed_drops()
-> Result<(), Box<dyn Error>> {
    let (dispatcher, mut receiver) =
        WorkerLifecycleDispatcher::channel(WorkerLifecycleChannelConfig::new(2)?);
    let task = task_event(1);
    let instance = instance_heartbeat(1);

    TaskLifecycleObserver::record(&dispatcher, task);
    WorkerInstanceHeartbeatObserver::record(&dispatcher, instance.clone());
    TaskLifecycleObserver::record(&dispatcher, task_event(2));

    assert_eq!(receiver.capacity(), 2);
    assert_eq!(
        dispatcher.snapshot(),
        plenora_runtime_worker::WorkerLifecycleDispatchSnapshot {
            capacity: 2,
            queued: 2,
            accepted: 2,
            delivered: 0,
            dropped_full: 1,
            dropped_closed: 0,
            state: WorkerLifecycleDispatchState::Saturated,
        }
    );
    assert_eq!(
        receiver.recv().await,
        Some(WorkerLifecycleObservation::Task(task))
    );
    assert_eq!(
        receiver.recv().await,
        Some(WorkerLifecycleObservation::Instance(instance))
    );
    let drained = dispatcher.snapshot();
    assert_eq!(drained.queued, 0);
    assert_eq!(drained.delivered, 2);
    assert_eq!(drained.state, WorkerLifecycleDispatchState::Open);

    receiver.close();
    TaskLifecycleObserver::record(&dispatcher, task_event(3));
    let closed = dispatcher.snapshot();
    assert_eq!(closed.accepted, 2);
    assert_eq!(closed.dropped_full, 1);
    assert_eq!(closed.dropped_closed, 1);
    assert_eq!(closed.state, WorkerLifecycleDispatchState::Closed);
    assert_eq!(receiver.recv().await, None);
    Ok(())
}

fn task_event(sequence: u64) -> TaskLifecycleEvent {
    TaskLifecycleEvent {
        message_id: MessageId::random(),
        correlation_id: CorrelationId::random(),
        attempt: 1,
        sequence,
        observed_at: SystemTime::UNIX_EPOCH,
        kind: TaskLifecycleEventKind::StateChanged(TaskState::Running),
    }
}

fn instance_heartbeat(sequence: u64) -> WorkerInstanceHeartbeat {
    WorkerInstanceHeartbeat {
        identity: WorkerInstanceIdentity::new(
            &ServiceMetadata::new("dispatch-test", "0.1.0", "dispatch-instance"),
            "dispatch-worker",
        ),
        sequence,
        observed_at: SystemTime::UNIX_EPOCH,
        status: WorkerInstanceStatus::Ready,
        max_in_flight: 2,
        in_flight: 1,
        available_slots: 1,
    }
}
