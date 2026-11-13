//! Data-free admission errors. No source parser error is retained.
use std::fmt;

/// Fixed rejection code from CHAIN-PLAN-2.0.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Code {
    /// Noncanonical encoding or number.
    Document,
    /// Closed structural contract violated.
    Schema,
    /// Unavailable exact version or claim.
    Version,
    /// Duplicate member or identity.
    Duplicate,
    /// Missing graph reference.
    Reference,
    /// Data or control cycle.
    Cycle,
    /// Incompatible boundary or missing barrier.
    Boundary,
    /// Consent outside its exact scope.
    Consent,
    /// Inconsistent finite budgets.
    Budget,
    /// Document or graph limit exceeded.
    Limit,
    /// Contradictory result.
    Result,
    /// Contradictory event.
    Event,
    /// Conflicting plan identity.
    Conflict,
}

impl Code {
    /// Stable wire code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Document => "CHAIN_DOCUMENT",
            Self::Schema => "CHAIN_SCHEMA",
            Self::Version => "CHAIN_VERSION",
            Self::Duplicate => "CHAIN_DUPLICATE",
            Self::Reference => "CHAIN_REFERENCE",
            Self::Cycle => "CHAIN_CYCLE",
            Self::Boundary => "CHAIN_BOUNDARY",
            Self::Consent => "CHAIN_CONSENT",
            Self::Budget => "CHAIN_BUDGET",
            Self::Limit => "CHAIN_LIMIT",
            Self::Result => "CHAIN_RESULT",
            Self::Event => "CHAIN_EVENT",
            Self::Conflict => "CHAIN_CONFLICT",
        }
    }
    /// Common error category.
    #[must_use]
    pub const fn category(self) -> &'static str {
        match self {
            Self::Document | Self::Result | Self::Event => "protocol",
            Self::Schema | Self::Duplicate | Self::Reference | Self::Cycle => "invalid_plan",
            Self::Version | Self::Boundary => "unsupported",
            Self::Consent => "authorization",
            Self::Budget | Self::Limit => "resource_limit",
            Self::Conflict => "conflict",
        }
    }
}

/// Rejection before any invoke; contains only a fixed code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rejection(pub Code);
impl Rejection {
    /// Fixed phase: no operation has started.
    #[must_use]
    pub const fn phase(self) -> &'static str {
        "validate"
    }
    /// No effects occur inside this validator.
    #[must_use]
    pub const fn remote_effect(self) -> &'static str {
        "none"
    }
    /// Invalid documents cannot be retried unchanged.
    #[must_use]
    pub const fn retry(self) -> &'static str {
        "never"
    }
    /// Error category from the specification.
    #[must_use]
    pub const fn category(self) -> &'static str {
        self.0.category()
    }
    /// Error code from the specification.
    #[must_use]
    pub const fn code(self) -> &'static str {
        self.0.as_str()
    }
}
impl fmt::Display for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0.as_str())
    }
}
impl std::error::Error for Rejection {}
impl From<Code> for Rejection {
    fn from(code: Code) -> Self {
        Self(code)
    }
}
pub(crate) type Checked<T> = Result<T, Rejection>;
pub(crate) fn ensure(condition: bool, code: Code) -> Checked<()> {
    if condition { Ok(()) } else { Err(code.into()) }
}
