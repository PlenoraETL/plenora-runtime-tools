use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    time::Duration,
};

/// Validated process-memory pressure thresholds and sampling cadence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryPressureConfig {
    sample_interval: Duration,
    resume_below_bytes: u64,
    soft_limit_bytes: u64,
    hard_limit_bytes: u64,
    pressure_confirmation_samples: u32,
    recovery_confirmation_samples: u32,
}

impl MemoryPressureConfig {
    /// Creates validated thresholds with one pressure sample and two recovery samples.
    ///
    /// # Errors
    ///
    /// Returns an error unless `resume < soft < hard` and all values are non-zero.
    pub fn new(
        sample_interval: Duration,
        resume_below_bytes: u64,
        soft_limit_bytes: u64,
        hard_limit_bytes: u64,
    ) -> Result<Self, MemoryPressureConfigError> {
        let config = Self {
            sample_interval,
            resume_below_bytes,
            soft_limit_bytes,
            hard_limit_bytes,
            pressure_confirmation_samples: 1,
            recovery_confirmation_samples: 2,
        };
        config.validate()?;
        Ok(config)
    }

    /// Sets bounded consecutive-sample confirmation counts.
    ///
    /// # Errors
    ///
    /// Returns an error when either count is zero.
    pub fn with_confirmation_samples(
        mut self,
        pressure: u32,
        recovery: u32,
    ) -> Result<Self, MemoryPressureConfigError> {
        self.pressure_confirmation_samples = pressure;
        self.recovery_confirmation_samples = recovery;
        self.validate()?;
        Ok(self)
    }

    /// Validates every threshold and confirmation bound.
    ///
    /// # Errors
    ///
    /// Returns a stable configuration error for the first invalid invariant.
    pub const fn validate(&self) -> Result<(), MemoryPressureConfigError> {
        if self.sample_interval.is_zero() {
            return Err(MemoryPressureConfigError::ZeroSampleInterval);
        }
        if self.resume_below_bytes == 0 {
            return Err(MemoryPressureConfigError::ZeroResumeThreshold);
        }
        if self.resume_below_bytes >= self.soft_limit_bytes {
            return Err(MemoryPressureConfigError::ResumeNotBelowSoftLimit);
        }
        if self.soft_limit_bytes >= self.hard_limit_bytes {
            return Err(MemoryPressureConfigError::SoftNotBelowHardLimit);
        }
        if self.pressure_confirmation_samples == 0 {
            return Err(MemoryPressureConfigError::ZeroPressureConfirmation);
        }
        if self.recovery_confirmation_samples == 0 {
            return Err(MemoryPressureConfigError::ZeroRecoveryConfirmation);
        }
        Ok(())
    }

    /// Returns the memory sampling cadence.
    #[must_use]
    pub const fn sample_interval(self) -> Duration {
        self.sample_interval
    }

    /// Returns the threshold below which admission may recover.
    #[must_use]
    pub const fn resume_below_bytes(self) -> u64 {
        self.resume_below_bytes
    }

    /// Returns the threshold that pauses new admission.
    #[must_use]
    pub const fn soft_limit_bytes(self) -> u64 {
        self.soft_limit_bytes
    }

    /// Returns the threshold that marks process health unhealthy.
    #[must_use]
    pub const fn hard_limit_bytes(self) -> u64 {
        self.hard_limit_bytes
    }

    /// Returns the number of consecutive soft-pressure samples required.
    #[must_use]
    pub const fn pressure_confirmation_samples(self) -> u32 {
        self.pressure_confirmation_samples
    }

    /// Returns the number of consecutive recovery samples required.
    #[must_use]
    pub const fn recovery_confirmation_samples(self) -> u32 {
        self.recovery_confirmation_samples
    }
}

/// Invalid memory-pressure monitoring configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryPressureConfigError {
    /// Sampling cannot use a zero interval.
    ZeroSampleInterval,
    /// Recovery must use a positive threshold.
    ZeroResumeThreshold,
    /// Recovery hysteresis must be strictly below the soft limit.
    ResumeNotBelowSoftLimit,
    /// The soft limit must be strictly below the hard limit.
    SoftNotBelowHardLimit,
    /// Pressure confirmation must require at least one sample.
    ZeroPressureConfirmation,
    /// Recovery confirmation must require at least one sample.
    ZeroRecoveryConfirmation,
}

impl Display for MemoryPressureConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroSampleInterval => "memory sample interval must be greater than zero",
            Self::ZeroResumeThreshold => "memory resume threshold must be greater than zero",
            Self::ResumeNotBelowSoftLimit => "memory resume threshold must be below the soft limit",
            Self::SoftNotBelowHardLimit => "memory soft limit must be below the hard limit",
            Self::ZeroPressureConfirmation => {
                "memory pressure confirmation samples must be greater than zero"
            }
            Self::ZeroRecoveryConfirmation => {
                "memory recovery confirmation samples must be greater than zero"
            }
        })
    }
}

impl Error for MemoryPressureConfigError {}
