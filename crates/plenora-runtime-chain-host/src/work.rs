//! Host-owned worker lifecycle; request cancellation does not own persisted work.
use plenora_runtime_boundary::DurableExecutionStore;
use plenora_runtime_chains::{ChainEngine, Error, Store};
use std::{sync::Arc, time::Duration};

pub(crate) struct Worker<S: Store + DurableExecutionStore>(pub Arc<ChainEngine<S>>);
impl<S: Store + DurableExecutionStore> Worker<S> {
    pub async fn run(&self, mut stop: tokio::sync::watch::Receiver<bool>) -> Result<(), Error> {
        let owner = uuid::Uuid::new_v4().to_string();
        loop {
            if *stop.borrow() {
                return Ok(());
            }
            self.0.cleanup_resources().await?;
            // Finish the current receipt even when shutdown is requested; never abort domain work.
            match self.0.work_once(&owner, 30_000).await {
                Ok(true) => continue,
                Ok(false) | Err(Error::Conflict) => {}
                Err(error) => return Err(error),
            }
            tokio::select! {
                changed = stop.changed() => { if changed.is_err() { return Ok(()); } },
                () = tokio::time::sleep(Duration::from_millis(25)) => {},
            }
        }
    }
}
