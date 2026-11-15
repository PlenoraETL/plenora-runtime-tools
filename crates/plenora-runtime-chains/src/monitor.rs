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
#[path = "monitor_tests.rs"]
mod tests;
