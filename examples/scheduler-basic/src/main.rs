//! Minimal bounded generic scheduler composition.

use std::{error::Error, sync::Arc, time::Duration};

use async_trait::async_trait;
use plenora_runtime_scheduler::{
    OneShotPlan, Schedule, ScheduleDispatchError, ScheduleDispatcher, ScheduleId,
    ScheduledOccurrence, SchedulerBuilder, SchedulerConfig,
};

#[derive(Clone, Copy, Debug)]
struct ExampleCommand {
    dataset_id: u64,
}

#[derive(Debug)]
struct ExampleDispatcher;

#[async_trait]
impl ScheduleDispatcher<ExampleCommand> for ExampleDispatcher {
    async fn dispatch(
        &self,
        occurrence: ScheduledOccurrence<ExampleCommand>,
    ) -> Result<(), ScheduleDispatchError> {
        println!(
            "dispatch schedule={} dataset={} due={:?}",
            occurrence.id.schedule_id, occurrence.payload.dataset_id, occurrence.due_at
        );
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let now = std::time::SystemTime::now();
    let config = SchedulerConfig::new(
        Duration::from_secs(1),
        Duration::from_secs(5),
        Duration::from_secs(2),
        64,
        16,
        4,
    )?;
    let mut builder = SchedulerBuilder::new(config);
    builder.register(Schedule::new(
        ScheduleId::new("plenora.dataset.refresh")?,
        ExampleCommand { dataset_id: 42 },
        OneShotPlan::new(now),
    ))?;
    let scheduler = builder.build(Arc::new(ExampleDispatcher));

    let report = scheduler.tick(now).await;
    println!("confirmed dispatches={}", report.dispatched);
    Ok(())
}
