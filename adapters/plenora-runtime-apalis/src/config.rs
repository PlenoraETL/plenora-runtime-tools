use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use plenora_runtime_worker::{WorkerConfig, WorkerConfigError};

/// Plenora-owned configuration used to construct an Apalis worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApalisAdapterConfig {
    worker_name: Arc<str>,
    worker: WorkerConfig,
}

impl ApalisAdapterConfig {
    /// Creates and validates adapter configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the worker name is blank or the worker bounds are invalid.
    pub fn new(
        worker_name: impl Into<Arc<str>>,
        worker: WorkerConfig,
    ) -> Result<Self, ApalisAdapterConfigError> {
        let config = Self {
            worker_name: worker_name.into(),
            worker,
        };
        config.validate()?;
        Ok(config)
    }

    /// Returns the stable name passed to the concrete worker engine.
    #[must_use]
    pub fn worker_name(&self) -> &str {
        &self.worker_name
    }

    /// Returns the engine-neutral worker configuration.
    #[must_use]
    pub const fn worker(&self) -> WorkerConfig {
        self.worker
    }

    /// Validates adapter and engine-neutral configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the worker name is blank or the worker bounds are invalid.
    pub fn validate(&self) -> Result<(), ApalisAdapterConfigError> {
        if self.worker_name.trim().is_empty() {
            return Err(ApalisAdapterConfigError::EmptyWorkerName);
        }
        self.worker
            .validate()
            .map_err(ApalisAdapterConfigError::Worker)
    }
}

/// Invalid Apalis adapter configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApalisAdapterConfigError {
    /// Apalis workers require a stable non-blank name.
    EmptyWorkerName,
    /// An engine-neutral worker bound is invalid.
    Worker(WorkerConfigError),
}

impl Display for ApalisAdapterConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyWorkerName => formatter.write_str("Apalis worker name must not be blank"),
            Self::Worker(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ApalisAdapterConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::EmptyWorkerName => None,
            Self::Worker(error) => Some(error),
        }
    }
}

impl From<WorkerConfigError> for ApalisAdapterConfigError {
    fn from(error: WorkerConfigError) -> Self {
        Self::Worker(error)
    }
}
