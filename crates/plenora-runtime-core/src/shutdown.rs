use tokio::sync::watch;

/// A cloneable, read-only signal that resolves when runtime shutdown begins.
#[derive(Clone, Debug)]
pub struct ShutdownSignal {
    receiver: watch::Receiver<bool>,
}

impl ShutdownSignal {
    /// Returns whether shutdown has already been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.receiver.borrow()
    }

    /// Waits until shutdown is requested.
    ///
    /// The method is cancellation-safe and returns immediately when the signal was already set.
    pub async fn cancelled(&self) {
        let mut receiver = self.receiver.clone();

        if *receiver.borrow_and_update() {
            return;
        }

        loop {
            match receiver.changed().await {
                Ok(()) if *receiver.borrow_and_update() => return,
                Ok(()) => {}
                Err(_) => return,
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct ShutdownController {
    sender: watch::Sender<bool>,
}

impl ShutdownController {
    pub(crate) fn cancel(&self) {
        self.sender.send_replace(true);
    }
}

pub(crate) fn shutdown_channel() -> (ShutdownController, ShutdownSignal) {
    let (sender, receiver) = watch::channel(false);
    (ShutdownController { sender }, ShutdownSignal { receiver })
}
