//! Backend-neutral bounded scheduling and programmed dispatch contracts.

#![forbid(unsafe_code)]

mod config;
mod dispatch;
mod identifier;
mod plan;
mod scheduler;

pub use config::{SchedulerConfig, SchedulerConfigError};
pub use dispatch::{
    ScheduleDispatchEffect, ScheduleDispatchError, ScheduleDispatcher, ScheduledOccurrence,
};
pub use identifier::{ScheduleId, ScheduleIdError, ScheduleOccurrenceId};
pub use plan::{
    CronPlan, CronPlanError, CronPlanErrorKind, FixedIntervalPlan, FixedIntervalPlanError,
    JitteredPlan, MAX_CRON_EXPRESSION_BYTES, MAX_TIMEZONE_NAME_BYTES, OneShotPlan, SchedulePlan,
};
pub use scheduler::{
    ManualTriggerOutcome, MisfirePolicy, NoopSchedulerObserver, ReconciliationResolution, Schedule,
    ScheduleBuildError, ScheduleRegistrationError, ScheduleRestoreError, ScheduleSnapshot,
    ScheduleStatus, Scheduler, SchedulerBuilder, SchedulerObserver, SchedulerRunReport,
    SchedulerTickObservation, SchedulerTickReport,
};
