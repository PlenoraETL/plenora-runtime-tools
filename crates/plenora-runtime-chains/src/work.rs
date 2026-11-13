//! Fenced ownership of coordination; expired ownership never proves a domain call stopped.
use crate::{Error, Journal};

/// Primary-issued coordination lease. Its token is monotone per run, never reset by release.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunLease {
    tenant: String,
    run_id: String,
    owner: String,
    token: u64,
    until: u64,
}
impl RunLease {
    /// Construct a receipt only after the primary confirms its lease transaction.
    /// # Errors
    /// Refuses noncanonical identities, zero fences and overflowing database counters.
    pub fn new(
        tenant: &str,
        run_id: &str,
        owner: &str,
        token: u64,
        until: u64,
    ) -> Result<Self, Error> {
        if !crate::scope(tenant)
            || !crate::uuid(run_id)
            || !crate::uuid(owner)
            || token == 0
            || token > i64::MAX as u64
            || until == 0
            || until > i64::MAX as u64
        {
            return Err(Error::Invalid);
        }
        Ok(Self {
            tenant: tenant.into(),
            run_id: run_id.into(),
            owner: owner.into(),
            token,
            until,
        })
    }
    /// Authenticated tenant.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
    /// Selected run identity.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }
    /// Worker identity.
    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }
    /// Monotone fencing token.
    #[must_use]
    pub const fn token(&self) -> u64 {
        self.token
    }
    /// Primary lease expiry, not permission to repeat effects.
    #[must_use]
    pub const fn until(&self) -> u64 {
        self.until
    }
}
impl Journal {
    /// Eligibility for bounded worker discovery; known terminal failures are not reopened.
    #[must_use]
    pub fn work_due(&self, now: u64) -> bool {
        !self.terminal
            && !self.waiting_recovery
            && now >= self.created
            && now - self.created < crate::DELIVERY_MS
            && self
                .retry_due
                .is_none_or(|due| due <= now || self.cancelled || now >= self.deadline)
    }
}
