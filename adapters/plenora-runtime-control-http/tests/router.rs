//! Authorization, read-only default, mutation opt-in, and redacted response coverage.

use std::{
    error::Error,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use plenora_runtime_control::{
    ControlComponentId, ControlPlane, ControlPlaneBuilder, MemoryPressureSnapshot,
    MemorySnapshotSource, SubprocessSnapshot, SubprocessSnapshotSource, WorkerControlHandle,
};
use plenora_runtime_control_http::{
    ControlAuthorizationRequest, ControlHttpAdapter, ControlRequestAuthorizer,
};
use plenora_runtime_resources::MemoryPressureState;
use plenora_runtime_scheduler::{
    OneShotPlan, Schedule, ScheduleDispatchError, ScheduleDispatcher, ScheduleId,
    ScheduledOccurrence, SchedulerBuilder, SchedulerConfig,
};
use plenora_runtime_worker::{WorkerAdmissionState, WorkerExecutor};
use tower::ServiceExt;

#[derive(Debug)]
struct HeaderAuthorizer;

#[derive(Debug)]
struct ConfirmingDispatcher;

#[derive(Debug)]
struct FixedMemory;

impl MemorySnapshotSource for FixedMemory {
    fn snapshot(&self) -> MemoryPressureSnapshot {
        MemoryPressureSnapshot {
            sequence: 9,
            state: MemoryPressureState::Normal,
            resident_bytes: Some(4_096),
        }
    }
}

#[derive(Debug)]
struct FixedSubprocess;

impl SubprocessSnapshotSource for FixedSubprocess {
    fn snapshot(&self) -> SubprocessSnapshot {
        SubprocessSnapshot {
            capacity: 3,
            in_flight: 1,
            available: 2,
            started: 5,
            completed: 4,
            spawn_failures: 1,
            cancellations: 2,
            timeouts: 3,
            memory_terminations: 4,
        }
    }
}

#[async_trait]
impl ScheduleDispatcher<()> for ConfirmingDispatcher {
    async fn dispatch(
        &self,
        _occurrence: ScheduledOccurrence<()>,
    ) -> Result<(), ScheduleDispatchError> {
        Ok(())
    }
}

impl ControlRequestAuthorizer for HeaderAuthorizer {
    fn authorize(&self, request: &ControlAuthorizationRequest<'_>) -> bool {
        request
            .headers
            .get("x-control-token")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == "allowed")
    }
}

#[tokio::test]
async fn every_read_is_authorized_and_responses_are_payload_free() -> Result<(), Box<dyn Error>> {
    let (control, _) = fixture()?;
    let router = ControlHttpAdapter::read_only(control, Arc::new(HeaderAuthorizer)).router();

    let denied = router
        .clone()
        .oneshot(Request::get("/runtime/control/components").body(Body::empty())?)
        .await?;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);

    let allowed = router
        .oneshot(
            Request::get("/runtime/control/workers/worker.main")
                .header("x-control-token", "allowed")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(allowed.status(), StatusCode::OK);
    let body = to_bytes(allowed.into_body(), 16 * 1024).await?;
    let body = std::str::from_utf8(&body)?;
    assert!(body.contains("\"capacity\":1"));
    assert!(body.contains("\"active_tasks\":[]"));
    assert!(!body.contains("payload"));
    Ok(())
}

#[tokio::test]
async fn mutation_routes_do_not_exist_until_explicitly_enabled() -> Result<(), Box<dyn Error>> {
    let (control, worker) = fixture()?;
    let read_only = ControlHttpAdapter::read_only(control.clone(), Arc::new(HeaderAuthorizer));
    let response = read_only
        .router()
        .oneshot(
            Request::post("/runtime/control/workers/worker.main/pause")
                .header("x-control-token", "allowed")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(worker.snapshot().admission, WorkerAdmissionState::Accepting);

    let mutable = ControlHttpAdapter::read_only(control, Arc::new(HeaderAuthorizer))
        .enable_mutations()
        .router();
    let denied = mutable
        .clone()
        .oneshot(Request::post("/runtime/control/workers/worker.main/pause").body(Body::empty())?)
        .await?;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    assert_eq!(worker.snapshot().admission, WorkerAdmissionState::Accepting);

    let applied = mutable
        .oneshot(
            Request::post("/runtime/control/workers/worker.main/pause")
                .header("x-control-token", "allowed")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(applied.status(), StatusCode::OK);
    assert_eq!(worker.snapshot().admission, WorkerAdmissionState::Paused);
    Ok(())
}

#[tokio::test]
async fn malformed_and_unknown_identifiers_have_stable_errors() -> Result<(), Box<dyn Error>> {
    let (control, _) = fixture()?;
    let router = ControlHttpAdapter::read_only(control, Arc::new(HeaderAuthorizer)).router();
    let malformed = router
        .clone()
        .oneshot(
            Request::get("/runtime/control/workers/INVALID")
                .header("x-control-token", "allowed")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);
    let missing = router
        .oneshot(
            Request::get("/runtime/control/workers/worker.missing")
                .header("x-control-token", "allowed")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn manual_trigger_requires_bounded_explicit_idempotency_time() -> Result<(), Box<dyn Error>> {
    let (control, _) = fixture()?;
    let router = ControlHttpAdapter::read_only(control, Arc::new(HeaderAuthorizer))
        .enable_mutations()
        .router();
    let path = "/runtime/control/schedulers/scheduler.main/schedules/manual/trigger";

    let missing = router
        .clone()
        .oneshot(
            Request::post(path)
                .header("x-control-token", "allowed")
                .header("content-type", "application/json")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(missing.status(), StatusCode::BAD_REQUEST);

    let oversized = router
        .clone()
        .oneshot(
            Request::post(path)
                .header("x-control-token", "allowed")
                .header("content-type", "application/json")
                .body(Body::from("x".repeat(2_048)))?,
        )
        .await?;
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let confirmed = router
        .oneshot(
            Request::post(path)
                .header("x-control-token", "allowed")
                .header("content-type", "application/json")
                .body(Body::from("{\"triggered_at_unix_ms\":123}"))?,
        )
        .await?;
    assert_eq!(confirmed.status(), StatusCode::OK);
    let body = to_bytes(confirmed.into_body(), 1_024).await?;
    assert!(std::str::from_utf8(&body)?.contains("confirmed"));
    Ok(())
}

#[tokio::test]
async fn every_read_model_and_error_path_is_transport_stable() -> Result<(), Box<dyn Error>> {
    let (control, _) = fixture()?;
    let router = ControlHttpAdapter::read_only(control, Arc::new(HeaderAuthorizer)).router();

    for (path, expected_fragment) in [
        ("/runtime/control/components", "worker.main"),
        ("/runtime/control/schedulers/scheduler.main", "manual"),
        ("/runtime/control/memory/memory.process", "4096"),
        (
            "/runtime/control/subprocess/subprocess.tools",
            "\"capacity\":3",
        ),
    ] {
        let response = authorized_request(&router, "GET", path, Body::empty()).await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 16 * 1024).await?;
        assert!(std::str::from_utf8(&body)?.contains(expected_fragment));
    }

    for path in [
        "/runtime/control/schedulers/scheduler.missing",
        "/runtime/control/memory/memory.missing",
        "/runtime/control/subprocess/subprocess.missing",
    ] {
        let response = authorized_request(&router, "GET", path, Body::empty()).await?;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
    for path in [
        "/runtime/control/schedulers/INVALID",
        "/runtime/control/memory/INVALID",
        "/runtime/control/subprocess/INVALID",
    ] {
        let response = authorized_request(&router, "GET", path, Body::empty()).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    Ok(())
}

#[tokio::test]
async fn enabled_mutations_cover_resume_drain_cancel_and_schedule_transitions()
-> Result<(), Box<dyn Error>> {
    let (control, _) = fixture()?;
    let router = ControlHttpAdapter::read_only(control, Arc::new(HeaderAuthorizer))
        .enable_mutations()
        .router();

    for path in [
        "/runtime/control/workers/worker.main/pause",
        "/runtime/control/workers/worker.main/resume",
        "/runtime/control/schedulers/scheduler.main/schedules/manual/pause",
        "/runtime/control/schedulers/scheduler.main/schedules/manual/resume",
    ] {
        let response = authorized_request(&router, "POST", path, Body::empty()).await?;
        assert_eq!(response.status(), StatusCode::OK);
    }

    let missing_task = authorized_request(
        &router,
        "POST",
        "/runtime/control/workers/worker.main/tasks/1/cancel",
        Body::empty(),
    )
    .await?;
    assert_eq!(missing_task.status(), StatusCode::NOT_FOUND);
    let invalid_task = authorized_request(
        &router,
        "POST",
        "/runtime/control/workers/worker.main/tasks/not-a-number/cancel",
        Body::empty(),
    )
    .await?;
    assert_eq!(invalid_task.status(), StatusCode::BAD_REQUEST);

    let unknown_schedule = authorized_request(
        &router,
        "POST",
        "/runtime/control/schedulers/scheduler.main/schedules/missing/pause",
        Body::empty(),
    )
    .await?;
    assert_eq!(unknown_schedule.status(), StatusCode::NOT_FOUND);
    let invalid_schedule = authorized_request(
        &router,
        "POST",
        "/runtime/control/schedulers/scheduler.main/schedules/INVALID/pause",
        Body::empty(),
    )
    .await?;
    assert_eq!(invalid_schedule.status(), StatusCode::BAD_REQUEST);

    let drain = authorized_request(
        &router,
        "POST",
        "/runtime/control/workers/worker.main/drain",
        Body::empty(),
    )
    .await?;
    assert_eq!(drain.status(), StatusCode::OK);
    let rejected_resume = authorized_request(
        &router,
        "POST",
        "/runtime/control/workers/worker.main/resume",
        Body::empty(),
    )
    .await?;
    assert_eq!(rejected_resume.status(), StatusCode::CONFLICT);
    Ok(())
}

fn fixture() -> Result<(ControlPlane, plenora_runtime_worker::WorkerAdmissionHandle), Box<dyn Error>>
{
    let executor = WorkerExecutor::new((), (), plenora_runtime_worker::WorkerConfig::default())?;
    let admission = executor.admission_control();
    let worker = WorkerControlHandle::new(admission.clone(), executor.task_control());
    let mut builder = ControlPlaneBuilder::default();
    builder.register_worker(ControlComponentId::new("worker.main")?, worker)?;
    let mut scheduler_builder = SchedulerBuilder::new(SchedulerConfig::new(
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::ZERO,
        1,
        1,
        1,
    )?);
    scheduler_builder.register(Schedule::new(
        ScheduleId::new("manual")?,
        (),
        OneShotPlan::new(SystemTime::UNIX_EPOCH),
    ))?;
    builder.register_scheduler(
        ControlComponentId::new("scheduler.main")?,
        Arc::new(scheduler_builder.build(Arc::new(ConfirmingDispatcher))),
    )?;
    builder.register_memory(
        ControlComponentId::new("memory.process")?,
        Arc::new(FixedMemory),
    )?;
    builder.register_subprocess(
        ControlComponentId::new("subprocess.tools")?,
        Arc::new(FixedSubprocess),
    )?;
    Ok((builder.build(), admission))
}

async fn authorized_request(
    router: &axum::Router,
    method: &'static str,
    path: &'static str,
    body: Body,
) -> Result<axum::response::Response, Box<dyn Error>> {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("x-control-token", "allowed")
        .body(body)?;
    Ok(router.clone().oneshot(request).await?)
}
