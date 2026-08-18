use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime},
};

use plenora_runtime_core::Clock;

/// Error returned when a manual clock operation cannot be represented.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManualClockError {
    /// Advancing the clock would exceed the range of [`SystemTime`].
    Overflow,
}

impl Display for ManualClockError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("manual clock advance exceeds the SystemTime range")
    }
}

impl Error for ManualClockError {}

/// Cloneable wall clock advanced explicitly by a test.
#[derive(Clone, Debug)]
pub struct ManualClock {
    now: Arc<Mutex<SystemTime>>,
}

impl ManualClock {
    /// Creates a clock at a caller-selected instant.
    #[must_use]
    pub fn new(now: SystemTime) -> Self {
        Self {
            now: Arc::new(Mutex::new(now)),
        }
    }

    /// Returns the current test instant.
    #[must_use]
    pub fn current(&self) -> SystemTime {
        *self.lock()
    }

    /// Replaces the current instant.
    pub fn set(&self, now: SystemTime) {
        *self.lock() = now;
    }

    /// Advances the current instant by a deterministic duration.
    ///
    /// # Errors
    ///
    /// Returns [`ManualClockError::Overflow`] if the resulting instant cannot be represented.
    pub fn advance(&self, duration: Duration) -> Result<SystemTime, ManualClockError> {
        let mut now = self.lock();
        let advanced = now
            .checked_add(duration)
            .ok_or(ManualClockError::Overflow)?;
        *now = advanced;
        Ok(advanced)
    }

    fn lock(&self) -> MutexGuard<'_, SystemTime> {
        match self.now.lock() {
            Ok(now) => now,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new(SystemTime::UNIX_EPOCH)
    }
}

impl Clock for ManualClock {
    fn now(&self) -> SystemTime {
        self.current()
    }
}

/// Conventional alias for [`ManualClock`].
pub type TestClock = ManualClock;
