//! Opt-in HTTP transport for the backend-neutral runtime control plane.

#![forbid(unsafe_code)]

mod authorization;
mod dto;
mod router;

pub use authorization::{ControlAction, ControlAuthorizationRequest, ControlRequestAuthorizer};
pub use router::ControlHttpAdapter;
