//! Typed remote facade over a host-authorized transport; no orchestration in the client.
use crate::{Error, Request, Response};
use async_trait::async_trait;
use std::sync::Arc;

/// A transport must preserve negotiation, identity and authentication across every delivery.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Exchange one bounded call; transport uncertainty is distinct from a domain result.
    async fn exchange(&self, request: Request) -> Result<Response, Error>;
}
/// Remote client sharing the same request/response types as the native engine facade.
pub struct ChainClient<T> {
    transport: Arc<T>,
}
impl<T: Transport> ChainClient<T> {
    /// Bind an already authenticated transport without changing its policy.
    #[must_use]
    pub const fn new(transport: Arc<T>) -> Self {
        Self { transport }
    }
    /// Call any catalog function through the shared closed protocol.
    /// # Errors
    /// Returns transport uncertainty separately from the response's domain error and full result.
    pub async fn execute(&self, request: Request) -> Result<Response, Error> {
        self.transport.exchange(request).await
    }

    /// Consume one validated event page, using the same cursor as native bindings.
    /// # Errors
    /// Transport/domain failures or discontinuous pages leave the cursor unchanged.
    pub async fn next_events(
        &self,
        cursor: &mut EventCursor,
    ) -> Result<Vec<crate::contracts::Event>, Error> {
        let reply = self.execute(cursor.request()).await?;
        if let Some(error) = reply.error {
            return Err(error);
        }
        cursor.accept(&reply.value)
    }
}

/// Finite event-page cursor shared by Rust, CLI and native Python iterators.
/// An empty page ends the current enumeration; it does not assert run completion.
pub struct EventCursor {
    run_id: String,
    sequence: u64,
    limit: usize,
}
impl EventCursor {
    /// Start after the last consumed durable sequence.
    /// # Errors
    /// Rejects noncanonical run identity and page sizes outside 1..=256.
    pub fn new(run_id: &str, sequence: u64, limit: usize) -> Result<Self, Error> {
        if !crate::uuid(run_id) || !(1..=256).contains(&limit) {
            return Err(Error::Invalid);
        }
        Ok(Self {
            run_id: run_id.into(),
            sequence,
            limit,
        })
    }
    /// Request the next page without modifying the cursor.
    #[must_use]
    pub fn request(&self) -> Request {
        Request::Events {
            run_id: self.run_id.clone(),
            cursor: self.sequence,
            limit: self.limit,
        }
    }
    /// Validate the entire page before advancing; gaps, foreign runs and oversized pages fail.
    /// # Errors
    /// Returns a data-free protocol rejection without consuming any part of a malformed page.
    pub fn accept(
        &mut self,
        value: &serde_json::Value,
    ) -> Result<Vec<crate::contracts::Event>, Error> {
        let page = value.as_array().ok_or(Error::Invalid)?;
        if page.len() > self.limit {
            return Err(Error::ResourceLimit);
        }
        let mut sequence = self.sequence;
        let mut events = Vec::with_capacity(page.len());
        for value in page {
            let event = crate::contracts::parse_event(&crate::encode(value)?)?;
            crate::contracts::validate_event(&event)?;
            sequence = sequence.checked_add(1).ok_or(Error::ResourceLimit)?;
            if event.run_id != self.run_id || event.sequence != sequence {
                return Err(Error::Conflict);
            }
            events.push(event);
        }
        self.sequence = sequence;
        Ok(events)
    }
}
