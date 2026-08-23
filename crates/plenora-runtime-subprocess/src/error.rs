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
    containment_failed: bool,
}

impl SubprocessError {
    pub(crate) fn new(kind: SubprocessErrorKind, message: &'static str, source: io::Error) -> Self {
        Self {
            kind,
            message,
            source: Arc::new(source),
            containment_failed: false,
        }
    }

    pub(crate) const fn with_containment_failure(mut self) -> Self {
        self.containment_failed = true;
        self
    }

    /// Returns the stable failure phase.
    #[must_use]
    pub const fn kind(&self) -> SubprocessErrorKind {
        self.kind
    }

    /// Returns whether bounded termination and reaping could not be completed.
    ///
    /// A true value means the operating system may still hold the child or its descendants, so the
    /// caller must treat process containment for this invocation as unproven.
    #[must_use]
    pub const fn containment_failed(&self) -> bool {
        self.containment_failed
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
            .field("containment_failed", &self.containment_failed)
            .field("source", &"<preserved; redacted>")
            .finish()
    }
}

impl Error for SubprocessError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}
