use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    sync::Arc,
};

use plenora_runtime_messaging::SerializedMessage;
use serde::Serialize;
use serde_json::{Map, Value};

/// Public error contract identifier used by Runtime Binding 1.0 failures.
pub const PLENORA_ERROR_CONTRACT: &str = "plenora-error-v1";
/// Public error envelope content type.
pub const PLENORA_ERROR_CONTENT_TYPE: &str = "application/vnd.plenora.error+json";

const MAX_ERROR_BYTES: usize = 512 * 1024;
const MAX_DETAILS_BYTES: usize = 256 * 1024;
const MAX_DETAILS_DEPTH: usize = 8;
const MAX_DETAILS_COLLECTION_ITEMS: usize = 128;
const MAX_DETAILS_STRING_BYTES: usize = 4_096;
const MAX_DETAILS_NODES: usize = 2_048;
const MAX_MESSAGE_BYTES: usize = 2_048;
const MAX_CODE_BYTES: usize = 64;
const MAX_PROVIDER_BYTES: usize = 64;
const MAX_EXECUTION_ID_BYTES: usize = 128;
const MAX_RETRY_DELAY_MS: u64 = 86_400_000;

/// Stable public category describing what failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PlenoraErrorCategory {
    /// An execution plan is invalid.
    InvalidPlan,
    /// Public configuration is invalid.
    InvalidConfiguration,
    /// Schema validation or compatibility failed.
    Schema,
    /// Data could not be mapped.
    DataMapping,
    /// Coordinate reference system processing failed.
    Crs,
    /// The requested feature or version is unsupported.
    Unsupported,
    /// A requested resource was not found.
    NotFound,
    /// The request conflicts with current state.
    Conflict,
    /// Concurrent modification prevented completion.
    ConcurrentModification,
    /// Authentication failed.
    Authentication,
    /// Authorization failed.
    Authorization,
    /// An execution deadline elapsed.
    Timeout,
    /// Execution was cancelled.
    Cancelled,
    /// A bounded resource limit was exceeded.
    ResourceLimit,
    /// Input/output failed.
    Io,
    /// A public protocol contract was violated.
    Protocol,
    /// A transient dependency failure occurred.
    Transient,
    /// Operation execution failed.
    Execution,
    /// An internal invariant failed.
    Internal,
}

/// Stable public phase identifying when failure occurred.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PlenoraErrorPhase {
    /// Input validation.
    Validate,
    /// Connection establishment.
    Connect,
    /// Connectivity probing.
    Probe,
    /// Execution preparation.
    Prepare,
    /// Reading.
    Read,
    /// Writing.
    Write,
    /// Finalization.
    Finalize,
    /// Commit.
    Commit,
    /// Rollback.
    Rollback,
    /// Cleanup.
    Cleanup,
}

/// Conservative public knowledge about externally visible effects.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PlenoraErrorRemoteEffect {
    /// No remote effect began.
    None,
    /// Remote effects were rolled back.
    RolledBack,
    /// Some remote effect was committed.
    Partial,
    /// The intended remote effect was committed.
    Committed,
    /// The remote effect cannot be proven.
    Unknown,
}

/// Public retry disposition; it is never inferred from diagnostic text or HTTP status.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PlenoraErrorRetry {
    /// Never retry automatically.
    Never,
    /// Move the request to quarantine/dead-letter handling.
    Quarantine,
    /// Automatic retry is safe.
    Safe,
    /// Retry only when an idempotency key is present and supported.
    RequiresIdempotencyKey,
    /// Reconciliation or recovery is required before another attempt.
    RequiresRecovery,
    /// Retry after an explicit bounded delay.
    After {
        /// Delay in milliseconds.
        delay_ms: u64,
    },
}

/// Bounded, redaction-safe `plenora-error-v1` value.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct PlenoraError {
    category: PlenoraErrorCategory,
    phase: PlenoraErrorPhase,
    remote_effect: PlenoraErrorRemoteEffect,
    retry: PlenoraErrorRetry,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<Arc<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<Arc<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_id: Option<Arc<str>>,
    message: Arc<str>,
    #[serde(skip_serializing_if = "Map::is_empty")]
    details: Map<String, Value>,
}

impl PlenoraError {
    /// Creates a bounded public error with all four required semantic axes.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty/oversized message, an invalid retry delay, or retry semantics
    /// that are unsafe for an unknown remote effect.
    pub fn new(
        category: PlenoraErrorCategory,
        phase: PlenoraErrorPhase,
        remote_effect: PlenoraErrorRemoteEffect,
        retry: PlenoraErrorRetry,
        message: impl Into<Arc<str>>,
    ) -> Result<Self, PlenoraErrorValidationError> {
        let message = message.into();
        if message.is_empty() || message.len() > MAX_MESSAGE_BYTES {
            return Err(PlenoraErrorValidationError::new(
                PlenoraErrorValidationErrorKind::InvalidMessage,
            ));
        }
        validate_retry(remote_effect, retry)?;
        Ok(Self {
            category,
            phase,
            remote_effect,
            retry,
            code: None,
            provider: None,
            execution_id: None,
            message,
            details: Map::new(),
        })
    }

    /// Adds a stable uppercase public error code.
    ///
    /// # Errors
    ///
    /// Returns an error when the code is outside the portable schema vocabulary.
    pub fn with_code(
        mut self,
        code: impl Into<Arc<str>>,
    ) -> Result<Self, PlenoraErrorValidationError> {
        let code = code.into();
        if !is_code(&code) {
            return Err(PlenoraErrorValidationError::new(
                PlenoraErrorValidationErrorKind::InvalidCode,
            ));
        }
        self.code = Some(code);
        Ok(self)
    }

    /// Adds a bounded public provider identifier.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider is not portable lowercase ASCII.
    pub fn with_provider(
        mut self,
        provider: impl Into<Arc<str>>,
    ) -> Result<Self, PlenoraErrorValidationError> {
        let provider = provider.into();
        if !is_provider(&provider) {
            return Err(PlenoraErrorValidationError::new(
                PlenoraErrorValidationErrorKind::InvalidProvider,
            ));
        }
        self.provider = Some(provider);
        Ok(self)
    }

    /// Adds a bounded public execution identifier.
    ///
    /// # Errors
    ///
    /// Returns an error when the identifier is empty or oversized.
    pub fn with_execution_id(
        mut self,
        execution_id: impl Into<Arc<str>>,
    ) -> Result<Self, PlenoraErrorValidationError> {
        let execution_id = execution_id.into();
        if execution_id.is_empty() || execution_id.len() > MAX_EXECUTION_ID_BYTES {
            return Err(PlenoraErrorValidationError::new(
                PlenoraErrorValidationErrorKind::InvalidExecutionId,
            ));
        }
        self.execution_id = Some(execution_id);
        Ok(self)
    }

    /// Adds bounded, component-owned structured details.
    ///
    /// # Errors
    ///
    /// Returns an error when encoded size, depth, collections, strings, or aggregate node count
    /// exceed Typed Errors 1.0 limits.
    pub fn with_details(
        mut self,
        details: Map<String, Value>,
    ) -> Result<Self, PlenoraErrorValidationError> {
        validate_details(&details)?;
        self.details = details;
        let _encoded = self.to_json()?;
        Ok(self)
    }

    /// Returns the public failure category.
    #[must_use]
    pub const fn category(&self) -> PlenoraErrorCategory {
        self.category
    }

    /// Returns the last externally meaningful phase.
    #[must_use]
    pub const fn phase(&self) -> PlenoraErrorPhase {
        self.phase
    }

    /// Returns conservative remote-effect knowledge.
    #[must_use]
    pub const fn remote_effect(&self) -> PlenoraErrorRemoteEffect {
        self.remote_effect
    }

    /// Returns the explicit public retry disposition.
    #[must_use]
    pub const fn retry(&self) -> PlenoraErrorRetry {
        self.retry
    }

    /// Returns the bounded human-readable message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Encodes compact `plenora-error-v1` JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the complete error exceeds its hard byte bound.
    pub fn to_json(&self) -> Result<Vec<u8>, PlenoraErrorValidationError> {
        let encoded = serde_json::to_vec(self).map_err(|_error| {
            PlenoraErrorValidationError::new(PlenoraErrorValidationErrorKind::Serialization)
        })?;
        if encoded.len() > MAX_ERROR_BYTES {
            return Err(PlenoraErrorValidationError::new(
                PlenoraErrorValidationErrorKind::ErrorTooLarge,
            ));
        }
        Ok(encoded)
    }

    /// Encodes a public error envelope without operation routing metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when compact JSON encoding fails or exceeds the hard bound.
    pub fn to_message(&self) -> Result<SerializedMessage, PlenoraErrorValidationError> {
        Ok(SerializedMessage::new(
            PLENORA_ERROR_CONTENT_TYPE,
            self.to_json()?,
        ))
    }
}

impl Debug for PlenoraError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlenoraError")
            .field("category", &self.category)
            .field("phase", &self.phase)
            .field("remote_effect", &self.remote_effect)
            .field("retry", &self.retry)
            .field("message", &"<redacted>")
            .field("details", &"<redacted>")
            .finish_non_exhaustive()
    }
}

/// Stable reason a `plenora-error-v1` value was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlenoraErrorValidationErrorKind {
    /// Message is empty or oversized.
    InvalidMessage,
    /// Error code is outside the portable vocabulary.
    InvalidCode,
    /// Provider identifier is invalid.
    InvalidProvider,
    /// Execution identifier is empty or oversized.
    InvalidExecutionId,
    /// Retry delay exceeds one day.
    InvalidRetryDelay,
    /// Retry semantics are unsafe for an unknown remote effect.
    UnsafeUnknownEffectRetry,
    /// Structured details exceed semantic bounds.
    InvalidDetails,
    /// Compact details JSON exceeds its byte bound.
    DetailsTooLarge,
    /// Compact complete error JSON exceeds its byte bound.
    ErrorTooLarge,
    /// Compact JSON serialization failed.
    Serialization,
}

/// Redaction-safe typed-error validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlenoraErrorValidationError {
    kind: PlenoraErrorValidationErrorKind,
}

impl PlenoraErrorValidationError {
    const fn new(kind: PlenoraErrorValidationErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable validation category.
    #[must_use]
    pub const fn kind(self) -> PlenoraErrorValidationErrorKind {
        self.kind
    }
}

impl Display for PlenoraErrorValidationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("public Plenora error is invalid")
    }
}

impl Error for PlenoraErrorValidationError {}

fn validate_retry(
    effect: PlenoraErrorRemoteEffect,
    retry: PlenoraErrorRetry,
) -> Result<(), PlenoraErrorValidationError> {
    if matches!(retry, PlenoraErrorRetry::After { delay_ms } if delay_ms > MAX_RETRY_DELAY_MS) {
        return Err(PlenoraErrorValidationError::new(
            PlenoraErrorValidationErrorKind::InvalidRetryDelay,
        ));
    }
    if effect == PlenoraErrorRemoteEffect::Unknown
        && !matches!(
            retry,
            PlenoraErrorRetry::Never
                | PlenoraErrorRetry::Quarantine
                | PlenoraErrorRetry::RequiresRecovery
        )
    {
        return Err(PlenoraErrorValidationError::new(
            PlenoraErrorValidationErrorKind::UnsafeUnknownEffectRetry,
        ));
    }
    Ok(())
}

fn is_code(value: &str) -> bool {
    (2..=MAX_CODE_BYTES).contains(&value.len())
        && value.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
        && value
            .bytes()
            .skip(1)
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn is_provider(value: &str) -> bool {
    (1..=MAX_PROVIDER_BYTES).contains(&value.len())
        && value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && value.bytes().skip(1).all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

fn validate_details(details: &Map<String, Value>) -> Result<(), PlenoraErrorValidationError> {
    let encoded = serde_json::to_vec(details).map_err(|_error| {
        PlenoraErrorValidationError::new(PlenoraErrorValidationErrorKind::Serialization)
    })?;
    if encoded.len() > MAX_DETAILS_BYTES {
        return Err(PlenoraErrorValidationError::new(
            PlenoraErrorValidationErrorKind::DetailsTooLarge,
        ));
    }
    let mut nodes = 0;
    validate_detail_value(&Value::Object(details.clone()), 1, &mut nodes)
}

fn validate_detail_value(
    value: &Value,
    depth: usize,
    nodes: &mut usize,
) -> Result<(), PlenoraErrorValidationError> {
    *nodes += 1;
    if depth > MAX_DETAILS_DEPTH || *nodes > MAX_DETAILS_NODES {
        return Err(PlenoraErrorValidationError::new(
            PlenoraErrorValidationErrorKind::InvalidDetails,
        ));
    }
    match value {
        Value::Object(values) => {
            if values.len() > MAX_DETAILS_COLLECTION_ITEMS {
                return Err(PlenoraErrorValidationError::new(
                    PlenoraErrorValidationErrorKind::InvalidDetails,
                ));
            }
            for child in values.values() {
                validate_detail_value(child, depth + 1, nodes)?;
            }
        }
        Value::Array(values) => {
            if values.len() > MAX_DETAILS_COLLECTION_ITEMS {
                return Err(PlenoraErrorValidationError::new(
                    PlenoraErrorValidationErrorKind::InvalidDetails,
                ));
            }
            for child in values {
                validate_detail_value(child, depth + 1, nodes)?;
            }
        }
        Value::String(value) if value.len() > MAX_DETAILS_STRING_BYTES => {
            return Err(PlenoraErrorValidationError::new(
                PlenoraErrorValidationErrorKind::InvalidDetails,
            ));
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}
