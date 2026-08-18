use axum::http::HeaderMap;
use plenora_runtime_control::ControlComponentId;

/// Stable operation category supplied to application-owned access policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlAction {
    /// List registered runtime components.
    ViewComponents,
    /// Read one worker and its payload-free active tasks.
    ViewWorker,
    /// Read one scheduler and its cursors.
    ViewScheduler,
    /// Read process memory pressure.
    ViewMemory,
    /// Read subprocess capacity and counters.
    ViewSubprocess,
    /// Pause worker admission.
    PauseWorker,
    /// Resume worker admission.
    ResumeWorker,
    /// Permanently begin worker drain.
    DrainWorker,
    /// Request cooperative task cancellation.
    CancelWorkerTask,
    /// Pause one schedule.
    PauseSchedule,
    /// Resume one schedule.
    ResumeSchedule,
    /// Manually invoke one schedule.
    TriggerSchedule,
}

/// Borrowed, non-debuggable HTTP authorization input.
pub struct ControlAuthorizationRequest<'a> {
    /// Incoming request headers. Implementations must avoid logging credential-bearing values.
    pub headers: &'a HeaderMap,
    /// Requested stable operation.
    pub action: ControlAction,
    /// Validated component identity, absent only for discovery.
    pub component: Option<&'a ControlComponentId>,
    /// Validated task or schedule identity for item-level operations.
    pub target: Option<&'a str>,
}

/// Application-owned access-policy boundary for every control-plane HTTP request.
pub trait ControlRequestAuthorizer: Send + Sync {
    /// Returns true only when the request may perform the exact supplied operation.
    ///
    /// Implementations should fail closed and must not log raw header values.
    fn authorize(&self, request: &ControlAuthorizationRequest<'_>) -> bool;
}
