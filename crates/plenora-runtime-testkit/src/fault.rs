use std::{
    collections::VecDeque,
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    sync::{Arc, Mutex, MutexGuard},
};

/// Default maximum number of pending entries in a deterministic fault script.
pub const DEFAULT_FAULT_SEQUENCE_CAPACITY: usize = 256;

/// Error returned when a deterministic fault script reaches its configured bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FaultSequenceCapacityError {
    capacity: usize,
}

impl FaultSequenceCapacityError {
    /// Returns the configured maximum number of pending entries.
    #[must_use]
    pub const fn capacity(self) -> usize {
        self.capacity
    }
}

impl Display for FaultSequenceCapacityError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "fault sequence capacity {} was reached",
            self.capacity
        )
    }
}

impl Error for FaultSequenceCapacityError {}

/// Cloneable FIFO script used to inject deterministic outcomes or faults.
#[derive(Clone)]
pub struct FaultSequence<T> {
    entries: Arc<Mutex<VecDeque<T>>>,
    capacity: usize,
}

impl<T> Debug for FaultSequence<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FaultSequence")
            .field("remaining", &self.remaining())
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

impl<T> FaultSequence<T> {
    /// Creates an empty script.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty script with an explicit maximum pending-entry count.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Arc::new(Mutex::new(VecDeque::new())),
            capacity,
        }
    }

    /// Creates a script whose entries are consumed in iterator order.
    ///
    /// # Errors
    ///
    /// Returns an error when the iterator contains more than the default bounded capacity.
    pub fn from_entries(
        entries: impl IntoIterator<Item = T>,
    ) -> Result<Self, FaultSequenceCapacityError> {
        let sequence = Self::default();
        for entry in entries {
            sequence.push(entry)?;
        }
        Ok(sequence)
    }

    /// Appends an entry to the script.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured pending-entry capacity was reached.
    pub fn push(&self, entry: T) -> Result<(), FaultSequenceCapacityError> {
        let mut entries = self.lock();
        if entries.len() >= self.capacity {
            return Err(FaultSequenceCapacityError {
                capacity: self.capacity,
            });
        }
        entries.push_back(entry);
        Ok(())
    }

    /// Removes the next scripted entry.
    #[must_use]
    pub fn pop(&self) -> Option<T> {
        self.lock().pop_front()
    }

    /// Returns the number of entries not yet consumed.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.lock().len()
    }

    /// Returns whether every scripted entry was consumed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> MutexGuard<'_, VecDeque<T>> {
        match self.entries.lock() {
            Ok(entries) => entries,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl<T> Default for FaultSequence<T> {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_FAULT_SEQUENCE_CAPACITY)
    }
}
