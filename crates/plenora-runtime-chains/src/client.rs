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
}
