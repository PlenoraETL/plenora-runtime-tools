use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    io,
    sync::Arc,
};

/// Stable phase for a supervisor infrastructure failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubprocessErrorKind {
    /// Child creation failed.
    Spawn,
    /// Waiting for the child failed.
    Wait,
    /// The process could not be terminated and reaped within its hard deadline.
    Termination,
    /// Fail-closed RSS sampling failed while a memory limit was active.
    MemorySample,
}

/// Source-preserving error that does not expose executable, arguments, environment, or output.
pub struct SubprocessError {
    kind: SubprocessErrorKind,
    message: &'static str,
    source: Arc<io::Error>,
}

impl SubprocessError {
    pub(crate) fn new(kind: SubprocessErrorKind, message: &'static str, source: io::Error) -> Self {
        Self {
            kind,
            message,
            source: Arc::new(source),
        }
    }

    /// Returns the stable failure phase.
    #[must_use]
    pub const fn kind(&self) -> SubprocessErrorKind {
        self.kind
    }
}

impl Display for SubprocessError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl Debug for SubprocessError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SubprocessError")
            .field("kind", &self.kind)
            .field("message", &self.message)
            .field("source", &"<preserved; redacted>")
            .finish()
    }
}

impl Error for SubprocessError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}
