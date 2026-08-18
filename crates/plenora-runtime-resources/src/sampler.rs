use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

/// One process-memory observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemorySample {
    /// Resident bytes currently attributed to the process.
    pub resident_bytes: u64,
}

/// Synchronous boundary for obtaining process memory without choosing a metrics backend.
pub trait MemorySampler: Send + Sync {
    /// Obtains the current resident-set sample.
    ///
    /// # Errors
    ///
    /// Returns a categorized source-preserving error when sampling is unavailable.
    fn sample(&self) -> Result<MemorySample, MemorySampleError>;
}

/// Portable sampler backed by `/proc/self/status` on Linux.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessMemorySampler;

impl MemorySampler for ProcessMemorySampler {
    fn sample(&self) -> Result<MemorySample, MemorySampleError> {
        process_memory_sample()
    }
}

#[cfg(target_os = "linux")]
fn process_memory_sample() -> Result<MemorySample, MemorySampleError> {
    let status = std::fs::read_to_string("/proc/self/status").map_err(|source| {
        MemorySampleError::with_source(MemorySampleErrorKind::ReadFailed, source)
    })?;
    parse_proc_status(&status)
}

#[cfg(not(target_os = "linux"))]
fn process_memory_sample() -> Result<MemorySample, MemorySampleError> {
    Err(MemorySampleError::new(
        MemorySampleErrorKind::UnsupportedPlatform,
    ))
}

#[cfg(target_os = "linux")]
fn parse_proc_status(status: &str) -> Result<MemorySample, MemorySampleError> {
    let line = status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .ok_or_else(|| MemorySampleError::new(MemorySampleErrorKind::MissingResidentSet))?;
    let kibibytes = line
        .split_ascii_whitespace()
        .nth(1)
        .ok_or_else(|| MemorySampleError::new(MemorySampleErrorKind::InvalidResidentSet))?
        .parse::<u64>()
        .map_err(|source| {
            MemorySampleError::with_source(MemorySampleErrorKind::InvalidResidentSet, source)
        })?;
    let resident_bytes = kibibytes
        .checked_mul(1_024)
        .ok_or_else(|| MemorySampleError::new(MemorySampleErrorKind::ResidentSetOverflow))?;
    Ok(MemorySample { resident_bytes })
}

/// Stable process-memory sampling failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemorySampleErrorKind {
    /// No portable process sampler exists for the current target.
    UnsupportedPlatform,
    /// The operating-system status source could not be read.
    ReadFailed,
    /// The status source omitted resident memory.
    MissingResidentSet,
    /// The resident-memory value was malformed.
    InvalidResidentSet,
    /// Conversion to bytes overflowed.
    ResidentSetOverflow,
    /// An injected or application-owned sampler was temporarily unavailable.
    Unavailable,
}

/// Source-preserving process-memory sample error with redacted debug output.
pub struct MemorySampleError {
    kind: MemorySampleErrorKind,
    source: Option<Box<dyn Error + Send + Sync>>,
}

impl MemorySampleError {
    /// Creates a categorized error without a concrete source.
    #[must_use]
    pub const fn new(kind: MemorySampleErrorKind) -> Self {
        Self { kind, source: None }
    }

    /// Creates a categorized error that preserves its concrete source.
    #[must_use]
    pub fn with_source<E>(kind: MemorySampleErrorKind, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            kind,
            source: Some(Box::new(source)),
        }
    }

    /// Returns the stable error category.
    #[must_use]
    pub const fn kind(&self) -> MemorySampleErrorKind {
        self.kind
    }
}

impl Display for MemorySampleError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            MemorySampleErrorKind::UnsupportedPlatform => {
                "process memory sampling is unsupported on this platform"
            }
            MemorySampleErrorKind::ReadFailed => "process memory status could not be read",
            MemorySampleErrorKind::MissingResidentSet => {
                "process memory status omitted resident memory"
            }
            MemorySampleErrorKind::InvalidResidentSet => "process resident memory was malformed",
            MemorySampleErrorKind::ResidentSetOverflow => {
                "process resident memory overflowed byte conversion"
            }
            MemorySampleErrorKind::Unavailable => "process memory sampling is unavailable",
        })
    }
}

impl Debug for MemorySampleError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemorySampleError")
            .field("kind", &self.kind)
            .field(
                "source",
                &self.source.as_ref().map(|_| "<preserved; redacted>"),
            )
            .finish()
    }
}

impl Error for MemorySampleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

#[cfg(test)]
#[cfg(target_os = "linux")]
mod tests {
    use super::{MemorySampleErrorKind, parse_proc_status};

    #[test]
    fn parses_linux_resident_memory() -> Result<(), Box<dyn std::error::Error>> {
        let sample = parse_proc_status("Name:\ttest\nVmRSS:\t2048 kB\n")?;
        assert_eq!(sample.resident_bytes, 2_097_152);
        Ok(())
    }

    #[test]
    fn rejects_missing_or_malformed_linux_values() {
        assert_eq!(
            parse_proc_status("Name:\ttest\n")
                .err()
                .map(|error| error.kind()),
            Some(MemorySampleErrorKind::MissingResidentSet)
        );
        assert_eq!(
            parse_proc_status("VmRSS:\tinvalid kB\n")
                .err()
                .map(|error| error.kind()),
            Some(MemorySampleErrorKind::InvalidResidentSet)
        );
    }
}
