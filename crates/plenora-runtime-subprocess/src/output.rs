use std::{
    fmt::{self, Debug, Formatter},
    sync::{Arc, Mutex, MutexGuard},
};

use tokio::io::{AsyncRead, AsyncReadExt};

/// Retained bounded output from one child pipe.
#[derive(Clone, Eq, PartialEq)]
pub struct CapturedOutput {
    bytes: Vec<u8>,
    truncated: bool,
    read_failed: bool,
}

impl CapturedOutput {
    /// Returns retained bytes, which never exceed the configured bound.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns whether additional bytes were drained but not retained.
    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.truncated
    }

    /// Returns whether reading the pipe ended with an I/O error.
    #[must_use]
    pub const fn read_failed(&self) -> bool {
        self.read_failed
    }

    pub(crate) const fn empty() -> Self {
        Self {
            bytes: Vec::new(),
            truncated: false,
            read_failed: false,
        }
    }
}

impl Debug for CapturedOutput {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapturedOutput")
            .field("retained_bytes", &self.bytes.len())
            .field("truncated", &self.truncated)
            .field("read_failed", &self.read_failed)
            .finish()
    }
}

pub(crate) struct SharedCapture {
    limit: usize,
    state: Mutex<CapturedOutput>,
}

impl SharedCapture {
    pub(crate) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            state: Mutex::new(CapturedOutput {
                bytes: Vec::with_capacity(limit.min(8 * 1024)),
                truncated: false,
                read_failed: false,
            }),
        })
    }

    pub(crate) fn snapshot(&self) -> CapturedOutput {
        self.lock().clone()
    }

    fn lock(&self) -> MutexGuard<'_, CapturedOutput> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

pub(crate) async fn drain_bounded<R>(mut reader: R, capture: Arc<SharedCapture>)
where
    R: AsyncRead + Unpin,
{
    let mut chunk = [0_u8; 8 * 1024];
    loop {
        match reader.read(&mut chunk).await {
            Ok(0) => return,
            Ok(read) => {
                let mut state = capture.lock();
                let available = capture.limit.saturating_sub(state.bytes.len());
                let retained = available.min(read);
                state.bytes.extend_from_slice(&chunk[..retained]);
                if retained < read {
                    state.truncated = true;
                }
            }
            Err(_error) => {
                capture.lock().read_failed = true;
                return;
            }
        }
    }
}
