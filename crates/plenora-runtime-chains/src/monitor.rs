//! Provider supervision: polling cadence never limits a primary operation.
use crate::{Error, ResourceLease, Store};
use plenora_runtime_core::Clock;
use std::time::{Duration, UNIX_EPOCH};

const POLL: Duration = Duration::from_millis(100);

/// Why a provider must be asked to stop. None of these proves termination.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderStop {
    /// The primary confirmed the caller's stop request.
    Requested,
    /// The original authorized deadline elapsed.
    Deadline,
    /// Primary authority could not be confirmed before lease expiry, or failed.
    AuthorityLost,
}

fn now(clock: &dyn Clock) -> Result<u64, Error> {
    u64::try_from(
        clock
            .now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Invalid)?
            .as_millis(),
    )
    .map_err(|_| Error::Invalid)
}

async fn reaches(clock: &dyn Clock, limit: u64) -> Result<(), Error> {
    loop {
        if now(clock)? >= limit {
            return Ok(());
        }
        tokio::time::sleep(POLL).await;
    }
}

async fn renew<S: Store>(
    store: &S,
    clock: &dyn Clock,
    tenant: &str,
    run: &str,
    mut lease: ResourceLease,
) {
    loop {
        if reaches(
            clock,
            lease
                .until
                .saturating_sub(crate::resources::RESOURCE_LEASE_MS / 2),
        )
        .await
        .is_err()
        {
            return;
        }
        let next = {
            let renewed = store.renew_resources(tenant, run, &lease);
            tokio::pin!(renewed);
            tokio::select! {
                biased;
                _ = reaches(clock, lease.until) => return,
                result = &mut renewed => result,
            }
        };
        let Ok(next) = next else { return };
        // A late reply cannot revive expired authority, nor substitute another attempt.
        if now(clock).map_or(true, |time| time >= lease.until || next.until <= time)
            || next.message_id != lease.message_id
            || next.until <= lease.until
        {
            return;
        }
        lease = next;
    }
}

fn cancellation_observation(observed: Result<bool, Error>) -> Option<ProviderStop> {
    match observed {
        Ok(false) => None,
        Ok(true) => Some(ProviderStop::Requested),
        Err(_) => Some(ProviderStop::AuthorityLost),
    }
}

async fn cancellation<S: Store>(store: &S, tenant: &str, run: &str) -> ProviderStop {
    loop {
        let observation = store
            .load(tenant, run)
            .await
            .map(|journal| journal.cancellation_requested());
        if let Some(reason) = cancellation_observation(observation) {
            return reason;
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Own supervision until provider completion or a stop request, then release it.
/// No suspended primary transaction survives into draining or evidence publication.
/// A stop request always drains the original provider; it never authorizes replay.
pub async fn drain_supervised<F, M, C>(provider: F, monitor: M, stop: C) -> F::Output
where
    F: std::future::Future,
    M: std::future::Future<Output = ProviderStop>,
    C: FnOnce(ProviderStop),
{
    tokio::pin!(provider);
    let reason = {
        tokio::pin!(monitor);
        tokio::select! {
            biased;
            result = &mut provider => return result,
            reason = &mut monitor => reason,
        }
    };
    stop(reason);
    provider.await
}

/// Observe primary cancellation, renew the resource lease and enforce the authorized deadline.
/// Pending I/O survives polling ticks. The caller must keep polling and drain its provider
/// after requesting stop; this monitor never publishes evidence or authorizes replay.
pub async fn monitor_provider<S: Store>(
    store: &S,
    clock: &dyn Clock,
    tenant: &str,
    run: &str,
    deadline: u64,
    lease: ResourceLease,
) -> ProviderStop {
    let reason = tokio::select! {
        biased;
        result = reaches(clock, deadline) => if result.is_ok() { ProviderStop::Deadline } else { ProviderStop::AuthorityLost },
        () = renew(store, clock, tenant, run, lease) => ProviderStop::AuthorityLost,
        reason = cancellation(store, tenant, run) => reason,
    };
    if crate::diagnostics::enabled() {
        eprintln!("Chain provider stop requested: reason={reason:?}");
    }
    reason
}

#[cfg(test)]
mod tests {
    //! Pending-primary regression tests with independent injected authority and scheduler clocks.
    use super::*;
    use crate::{Change, Journal, Submission};
    use async_trait::async_trait;
    use plenora_runtime_testkit::ManualClock;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Primary {
        clock: ManualClock,
        mode: u8,
        reads: AtomicUsize,
        renewals: AtomicUsize,
    }
    #[async_trait]
    impl Store for Primary {
        async fn lookup(
            &self,
            _: &str,
            _: &str,
            _: Option<&str>,
        ) -> Result<Option<Journal>, Error> {
            Err(Error::Storage)
        }
        async fn create(&self, _: Submission) -> Result<Journal, Error> {
            Err(Error::Storage)
        }
        async fn compare_exchange(&self, _: u64, _: Change) -> Result<(), Error> {
            Err(Error::Storage)
        }
        async fn load(&self, _: &str, _: &str) -> Result<Journal, Error> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self.mode == 6 {
                return Err(Error::Storage);
            }
            std::future::pending().await
        }
        async fn renew_resources(
            &self,
            _: &str,
            _: &str,
            lease: &ResourceLease,
        ) -> Result<ResourceLease, Error> {
            self.renewals.fetch_add(1, Ordering::SeqCst);
            match self.mode {
                0 => std::future::pending().await,
                2 => Err(Error::Storage),
                mode => {
                    if mode == 5 {
                        self.clock
                            .set(UNIX_EPOCH + Duration::from_millis(lease.until));
                    }
                    Ok(ResourceLease {
                        message_id: if mode == 3 {
                            "other".into()
                        } else {
                            lease.message_id.clone()
                        },
                        until: if mode == 4 {
                            lease.until
                        } else {
                            now(&self.clock)? + crate::resources::RESOURCE_LEASE_MS
                        },
                    })
                }
            }
        }
    }
    fn primary(mode: u8) -> Arc<Primary> {
        Arc::new(Primary {
            clock: ManualClock::new(UNIX_EPOCH),
            mode,
            reads: AtomicUsize::new(0),
            renewals: AtomicUsize::new(0),
        })
    }
    fn start(store: Arc<Primary>, deadline: u64) -> tokio::task::JoinHandle<ProviderStop> {
        tokio::spawn(async move {
            monitor_provider(
                store.as_ref(),
                &store.clock,
                "tenant",
                "run",
                deadline,
                ResourceLease {
                    message_id: "attempt".into(),
                    until: 30_000,
                },
            )
            .await
        })
    }
    async fn tick(store: &Primary, ms: u64) {
        store.clock.set(UNIX_EPOCH + Duration::from_millis(ms));
        tokio::time::advance(POLL).await;
        tokio::task::yield_now().await;
    }

    #[tokio::test(start_paused = true)]
    async fn pending_reads_survive_ticks_while_lease_renews()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = primary(1);
        let task = start(store.clone(), 60_000);
        tokio::task::yield_now().await;
        for ms in [100, 500, 15_000, 15_500, 30_000, 45_000] {
            tick(&store, ms).await;
            assert!(!task.is_finished());
            assert_eq!(store.reads.load(Ordering::SeqCst), 1);
        }
        assert_eq!(store.renewals.load(Ordering::SeqCst), 3);
        tick(&store, 60_000).await;
        assert_eq!(task.await?, ProviderStop::Deadline);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn pending_renewal_survives_ticks_but_not_authority_expiry()
    -> Result<(), Box<dyn std::error::Error>> {
        for (deadline, expected) in [
            (25_000, ProviderStop::Deadline),
            (60_000, ProviderStop::AuthorityLost),
        ] {
            let store = primary(0);
            let task = start(store.clone(), deadline);
            tokio::task::yield_now().await;
            for ms in [15_000, 15_500, 24_999] {
                tick(&store, ms).await;
                assert!(!task.is_finished());
                assert_eq!(store.renewals.load(Ordering::SeqCst), 1);
            }
            tick(&store, deadline.min(30_000)).await;
            assert_eq!(task.await?, expected);
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn enumerate_failed_wrong_stale_late_primary_and_invalid_clock()
    -> Result<(), Box<dyn std::error::Error>> {
        for mode in 2..=6 {
            let store = primary(mode);
            let task = start(store.clone(), 60_000);
            tokio::task::yield_now().await;
            tick(&store, 15_000).await;
            assert_eq!(task.await?, ProviderStop::AuthorityLost, "mode {mode}");
        }
        let store = primary(0);
        store.clock.set(UNIX_EPOCH - Duration::from_secs(1));
        assert_eq!(start(store, 60_000).await?, ProviderStop::AuthorityLost);
        Ok(())
    }

    #[test]
    fn enumerate_primary_cancellation_observations() {
        for (observation, expected) in [
            (Ok(false), None),
            (Ok(true), Some(ProviderStop::Requested)),
            (Err(Error::Storage), Some(ProviderStop::AuthorityLost)),
            (Err(Error::Expired), Some(ProviderStop::AuthorityLost)),
            (Err(Error::NotFound), Some(ProviderStop::AuthorityLost)),
        ] {
            assert_eq!(cancellation_observation(observation), expected);
        }
    }

    // Model a primary transaction held by a suspended monitor. Completion must release
    // it before either a stop callback, provider drain, or the caller's publication.
    #[tokio::test]
    async fn supervision_releases_authority_on_every_exit() -> Result<(), Box<dyn std::error::Error>>
    {
        for reason in [
            None,
            Some(ProviderStop::Requested),
            Some(ProviderStop::Deadline),
            Some(ProviderStop::AuthorityLost),
        ] {
            let authority = tokio::sync::Mutex::new(());
            let (entered, observed) = tokio::sync::oneshot::channel();
            let stopped = AtomicUsize::new(0);
            let monitor = async {
                let _guard = authority.lock().await;
                assert!(entered.send(()).is_ok());
                match reason {
                    Some(reason) => reason,
                    None => std::future::pending().await,
                }
            };
            let provider = async {
                observed.await?;
                if reason.is_some() {
                    assert!(
                        authority.try_lock().is_ok(),
                        "monitor retained authority during drain"
                    );
                    assert_eq!(stopped.load(Ordering::SeqCst), 1);
                }
                Ok::<_, tokio::sync::oneshot::error::RecvError>(17)
            };
            let result = drain_supervised(provider, monitor, |actual| {
                assert_eq!(Some(actual), reason);
                assert!(
                    authority.try_lock().is_ok(),
                    "monitor retained authority at stop"
                );
                stopped.fetch_add(1, Ordering::SeqCst);
            })
            .await?;
            assert_eq!(result, 17);
            assert!(
                authority.try_lock().is_ok(),
                "monitor retained authority at publication"
            );
            assert_eq!(
                stopped.load(Ordering::SeqCst),
                usize::from(reason.is_some())
            );
        }
        Ok(())
    }
}
