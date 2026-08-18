use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use plenora_runtime_control::{
    ControlComponentId, ControlMutationOutcome, ControlPlane, ControlPlaneError,
    ScheduleBuildError, ScheduleId, WorkerTaskCancellationOutcome, WorkerTaskId,
};

use crate::{
    ControlAction, ControlAuthorizationRequest, ControlRequestAuthorizer,
    dto::{
        ComponentDto, ComponentsDto, ErrorDto, MemoryDto, OutcomeDto, ScheduleDto, SchedulesDto,
        SubprocessDto, TriggerRequestDto, WorkerDto,
    },
};

#[derive(Clone)]
struct HttpState {
    control: ControlPlane,
    authorizer: Arc<dyn ControlRequestAuthorizer>,
}

/// Builder for an authenticated, optionally mutable runtime-control router.
pub struct ControlHttpAdapter {
    state: HttpState,
    mutations_enabled: bool,
}

impl ControlHttpAdapter {
    /// Creates a read-only adapter that still authorizes every request.
    #[must_use]
    pub fn read_only<A>(control: ControlPlane, authorizer: Arc<A>) -> Self
    where
        A: ControlRequestAuthorizer + 'static,
    {
        let authorizer: Arc<dyn ControlRequestAuthorizer> = authorizer;
        Self {
            state: HttpState {
                control,
                authorizer,
            },
            mutations_enabled: false,
        }
    }

    /// Explicitly adds state-changing routes to the generated router.
    #[must_use]
    pub const fn enable_mutations(mut self) -> Self {
        self.mutations_enabled = true;
        self
    }

    /// Builds a composable Axum router rooted at `/runtime/control`.
    pub fn router(&self) -> Router {
        let read_only = Router::new()
            .route("/runtime/control/components", get(components))
            .route("/runtime/control/workers/{component}", get(worker))
            .route("/runtime/control/schedulers/{component}", get(scheduler))
            .route("/runtime/control/memory/{component}", get(memory))
            .route("/runtime/control/subprocess/{component}", get(subprocess));
        let router = if self.mutations_enabled {
            read_only
                .route(
                    "/runtime/control/workers/{component}/pause",
                    post(pause_worker),
                )
                .route(
                    "/runtime/control/workers/{component}/resume",
                    post(resume_worker),
                )
                .route(
                    "/runtime/control/workers/{component}/drain",
                    post(drain_worker),
                )
                .route(
                    "/runtime/control/workers/{component}/tasks/{task}/cancel",
                    post(cancel_task),
                )
                .route(
                    "/runtime/control/schedulers/{component}/schedules/{schedule}/pause",
                    post(pause_schedule),
                )
                .route(
                    "/runtime/control/schedulers/{component}/schedules/{schedule}/resume",
                    post(resume_schedule),
                )
                .route(
                    "/runtime/control/schedulers/{component}/schedules/{schedule}/trigger",
                    post(trigger_schedule),
                )
                .layer(DefaultBodyLimit::max(1_024))
        } else {
            read_only
        };
        router.with_state(self.state.clone())
    }
}

async fn components(State(state): State<HttpState>, headers: HeaderMap) -> Response {
    if !authorized(&state, &headers, ControlAction::ViewComponents, None, None) {
        return forbidden();
    }
    Json(ComponentsDto {
        components: state
            .control
            .components()
            .into_iter()
            .map(ComponentDto::from)
            .collect(),
    })
    .into_response()
}

async fn worker(
    State(state): State<HttpState>,
    Path(component): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Ok(component) = ControlComponentId::new(component) else {
        return invalid("invalid_component");
    };
    if !authorized(
        &state,
        &headers,
        ControlAction::ViewWorker,
        Some(&component),
        None,
    ) {
        return forbidden();
    }
    match state.control.worker_snapshot(&component) {
        Ok(snapshot) => Json(WorkerDto::from(snapshot)).into_response(),
        Err(error) => control_error(error),
    }
}

async fn scheduler(
    State(state): State<HttpState>,
    Path(component): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Ok(component) = ControlComponentId::new(component) else {
        return invalid("invalid_component");
    };
    if !authorized(
        &state,
        &headers,
        ControlAction::ViewScheduler,
        Some(&component),
        None,
    ) {
        return forbidden();
    }
    match state.control.scheduler_snapshots(&component).await {
        Ok(snapshots) => Json(SchedulesDto {
            schedules: snapshots.into_iter().map(ScheduleDto::from).collect(),
        })
        .into_response(),
        Err(error) => control_error(error),
    }
}

async fn memory(
    State(state): State<HttpState>,
    Path(component): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Ok(component) = ControlComponentId::new(component) else {
        return invalid("invalid_component");
    };
    if !authorized(
        &state,
        &headers,
        ControlAction::ViewMemory,
        Some(&component),
        None,
    ) {
        return forbidden();
    }
    match state.control.memory_snapshot(&component) {
        Ok(snapshot) => Json(MemoryDto::from(snapshot)).into_response(),
        Err(error) => control_error(error),
    }
}

async fn subprocess(
    State(state): State<HttpState>,
    Path(component): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Ok(component) = ControlComponentId::new(component) else {
        return invalid("invalid_component");
    };
    if !authorized(
        &state,
        &headers,
        ControlAction::ViewSubprocess,
        Some(&component),
        None,
    ) {
        return forbidden();
    }
    match state.control.subprocess_snapshot(&component) {
        Ok(snapshot) => Json(SubprocessDto::from(snapshot)).into_response(),
        Err(error) => control_error(error),
    }
}

async fn pause_worker(
    State(state): State<HttpState>,
    Path(component): Path<String>,
    headers: HeaderMap,
) -> Response {
    worker_mutation(
        &state,
        &headers,
        component,
        ControlAction::PauseWorker,
        ControlPlane::pause_worker,
    )
}

async fn resume_worker(
    State(state): State<HttpState>,
    Path(component): Path<String>,
    headers: HeaderMap,
) -> Response {
    worker_mutation(
        &state,
        &headers,
        component,
        ControlAction::ResumeWorker,
        ControlPlane::resume_worker,
    )
}

async fn drain_worker(
    State(state): State<HttpState>,
    Path(component): Path<String>,
    headers: HeaderMap,
) -> Response {
    worker_mutation(
        &state,
        &headers,
        component,
        ControlAction::DrainWorker,
        ControlPlane::drain_worker,
    )
}

fn worker_mutation(
    state: &HttpState,
    headers: &HeaderMap,
    component: String,
    action: ControlAction,
    operation: impl FnOnce(
        &ControlPlane,
        &ControlComponentId,
    ) -> Result<ControlMutationOutcome, ControlPlaneError>,
) -> Response {
    let Ok(component) = ControlComponentId::new(component) else {
        return invalid("invalid_component");
    };
    if !authorized(state, headers, action, Some(&component), None) {
        return forbidden();
    }
    match operation(&state.control, &component) {
        Ok(outcome) => mutation_response(outcome),
        Err(error) => control_error(error),
    }
}

async fn cancel_task(
    State(state): State<HttpState>,
    Path((component, task)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let Ok(component) = ControlComponentId::new(component) else {
        return invalid("invalid_component");
    };
    let Ok(task_id) = WorkerTaskId::from_str(&task) else {
        return invalid("invalid_task");
    };
    if !authorized(
        &state,
        &headers,
        ControlAction::CancelWorkerTask,
        Some(&component),
        Some(&task),
    ) {
        return forbidden();
    }
    match state.control.cancel_worker_task(&component, task_id) {
        Ok(WorkerTaskCancellationOutcome::NotFound) => not_found("task_not_found"),
        Ok(outcome) => Json(OutcomeDto::from(outcome)).into_response(),
        Err(error) => control_error(error),
    }
}

async fn pause_schedule(
    State(state): State<HttpState>,
    Path((component, schedule)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    schedule_mutation(
        state,
        headers,
        component,
        schedule,
        ControlAction::PauseSchedule,
        ScheduleOperation::Pause,
    )
    .await
}

async fn resume_schedule(
    State(state): State<HttpState>,
    Path((component, schedule)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    schedule_mutation(
        state,
        headers,
        component,
        schedule,
        ControlAction::ResumeSchedule,
        ScheduleOperation::Resume,
    )
    .await
}

async fn trigger_schedule(
    State(state): State<HttpState>,
    Path((component, schedule)): Path<(String, String)>,
    headers: HeaderMap,
    request: Result<Json<TriggerRequestDto>, JsonRejection>,
) -> Response {
    let Ok(component) = ControlComponentId::new(component) else {
        return invalid("invalid_component");
    };
    let Ok(schedule_id) = ScheduleId::new(schedule.as_str()) else {
        return invalid("invalid_schedule");
    };
    if !authorized(
        &state,
        &headers,
        ControlAction::TriggerSchedule,
        Some(&component),
        Some(&schedule),
    ) {
        return forbidden();
    }
    let request = match request {
        Ok(Json(request)) => request,
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            return response(StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large");
        }
        Err(_) => return invalid("invalid_trigger_request"),
    };
    let Some(triggered_at) =
        UNIX_EPOCH.checked_add(Duration::from_millis(request.triggered_at_unix_ms))
    else {
        return invalid("invalid_trigger_time");
    };
    match state
        .control
        .trigger_schedule(&component, &schedule_id, triggered_at)
        .await
    {
        Ok(Ok(outcome)) => Json(OutcomeDto::from(outcome)).into_response(),
        Ok(Err(error)) => schedule_error(&error),
        Err(error) => control_error(error),
    }
}

#[derive(Clone, Copy)]
enum ScheduleOperation {
    Pause,
    Resume,
}

async fn schedule_mutation(
    state: HttpState,
    headers: HeaderMap,
    component: String,
    schedule: String,
    action: ControlAction,
    operation: ScheduleOperation,
) -> Response {
    let Ok(component) = ControlComponentId::new(component) else {
        return invalid("invalid_component");
    };
    let Ok(schedule_id) = ScheduleId::new(schedule.as_str()) else {
        return invalid("invalid_schedule");
    };
    if !authorized(&state, &headers, action, Some(&component), Some(&schedule)) {
        return forbidden();
    }
    match operation {
        ScheduleOperation::Pause => {
            match state.control.pause_schedule(&component, &schedule_id).await {
                Ok(Ok(())) => mutation_response(ControlMutationOutcome::Applied),
                Ok(Err(error)) => schedule_error(&error),
                Err(error) => control_error(error),
            }
        }
        ScheduleOperation::Resume => match state
            .control
            .resume_schedule(&component, &schedule_id)
            .await
        {
            Ok(Ok(())) => mutation_response(ControlMutationOutcome::Applied),
            Ok(Err(error)) => schedule_error(&error),
            Err(error) => control_error(error),
        },
    }
}

fn authorized(
    state: &HttpState,
    headers: &HeaderMap,
    action: ControlAction,
    component: Option<&ControlComponentId>,
    target: Option<&str>,
) -> bool {
    state.authorizer.authorize(&ControlAuthorizationRequest {
        headers,
        action,
        component,
        target,
    })
}

fn mutation_response(outcome: ControlMutationOutcome) -> Response {
    let status = if outcome == ControlMutationOutcome::Rejected {
        StatusCode::CONFLICT
    } else {
        StatusCode::OK
    };
    (status, Json(OutcomeDto::from(outcome))).into_response()
}

fn control_error(_error: ControlPlaneError) -> Response {
    not_found("component_not_found")
}

fn schedule_error(error: &ScheduleBuildError) -> Response {
    match error {
        ScheduleBuildError::UnknownSchedule(_) => not_found("schedule_not_found"),
        ScheduleBuildError::ReconciliationNotRequired(_)
        | ScheduleBuildError::TransitionNotAllowed(_) => conflict("invalid_schedule_state"),
    }
}

fn forbidden() -> Response {
    response(StatusCode::FORBIDDEN, "forbidden")
}

fn invalid(code: &'static str) -> Response {
    response(StatusCode::BAD_REQUEST, code)
}

fn not_found(code: &'static str) -> Response {
    response(StatusCode::NOT_FOUND, code)
}

fn conflict(code: &'static str) -> Response {
    response(StatusCode::CONFLICT, code)
}

fn response(status: StatusCode, code: &'static str) -> Response {
    (status, Json(ErrorDto { code })).into_response()
}
