//! One redacted public rejection and selective result identity construction.
use crate::lexical;
use plenora_runtime_capabilities::{
    CAPABILITY_OPERATION_METADATA_KEY, OPERATION_VERSION_METADATA_KEY,
    OUTPUT_CONTRACT_METADATA_KEY, PLENORA_ERROR_CONTENT_TYPE, PLENORA_ERROR_CONTRACT,
    PlenoraErrorCategory, PlenoraErrorPhase, PlenoraErrorRemoteEffect, PlenoraErrorRetry,
};
use plenora_runtime_messaging::{
    CAUSATION_ID_METADATA_KEY, CORRELATION_ID_METADATA_KEY, MESSAGE_ID_METADATA_KEY, MessageId,
    SerializedMessage,
};
use serde::Serialize;
use serde_json::{Map, Value};
use std::{error::Error, fmt};

/// Public runtime failure with one identity and all four semantic axes.
/// Diagnostic formatting never includes request data.
#[derive(Clone, Serialize)]
pub struct Rejection {
    category: PlenoraErrorCategory,
    phase: PlenoraErrorPhase,
    remote_effect: PlenoraErrorRemoteEffect,
    retry: PlenoraErrorRetry,
    message: &'static str,
    #[serde(skip)]
    metadata: Map<String, Value>,
}
impl Rejection {
    pub(crate) fn new(category: PlenoraErrorCategory, raw: &Map<String, Value>) -> Self {
        Self {
            category,
            phase: PlenoraErrorPhase::Validate,
            remote_effect: PlenoraErrorRemoteEffect::None,
            retry: PlenoraErrorRetry::Never,
            message: "Runtime request rejected before invocation.",
            metadata: result_metadata(raw),
        }
    }
    pub(crate) fn from_dispatch(
        error: &plenora_runtime_capabilities::CapabilityDispatchError,
        raw: &Map<String, Value>,
    ) -> Self {
        use plenora_runtime_capabilities::CapabilityDispatchError as E;
        if matches!(
            error,
            E::UnknownCapability { .. }
                | E::PayloadTooLarge { .. }
                | E::IncompatibleRequest { .. }
                | E::DeadlineExceeded {
                    invocation_started: false,
                    ..
                }
        ) {
            return Self::new(
                error
                    .public_error()
                    .map_or(PlenoraErrorCategory::Internal, |e| e.category()),
                raw,
            );
        }
        let mut result = Self::new(PlenoraErrorCategory::Internal, raw);
        result.message = "Runtime dispatch failed.";
        result.phase = PlenoraErrorPhase::Finalize;
        result.remote_effect = PlenoraErrorRemoteEffect::Unknown;
        result.retry = PlenoraErrorRetry::RequiresRecovery;
        if let Ok(public) = error.public_error() {
            result.category = public.category();
            result.phase = public.phase();
            result.remote_effect = public.remote_effect();
            result.retry = public.retry();
        }
        result
    }
    /// Returns the public category.
    #[must_use]
    pub const fn category(&self) -> PlenoraErrorCategory {
        self.category
    }
    /// Returns the last known phase.
    #[must_use]
    pub const fn phase(&self) -> PlenoraErrorPhase {
        self.phase
    }
    /// Returns proven effect knowledge.
    #[must_use]
    pub const fn remote_effect(&self) -> PlenoraErrorRemoteEffect {
        self.remote_effect
    }
    /// Returns the public retry disposition.
    #[must_use]
    pub const fn retry(&self) -> PlenoraErrorRetry {
        self.retry
    }
    /// Returns the selective reflected metadata and the identity created once for this result.
    #[must_use]
    pub const fn metadata(&self) -> &Map<String, Value> {
        &self.metadata
    }
    /// Serializes the error using the common media type and contract.
    /// Repeated serialization preserves the already allocated result identity.
    /// # Errors
    /// Returns an explicit encoding failure; an unpublished result is never reported as delivered.
    pub fn to_message(&self) -> Result<SerializedMessage, ResultEncodingError> {
        let bytes = serde_json::to_vec(self).map_err(|_| ResultEncodingError)?;
        let mut message = SerializedMessage::new(PLENORA_ERROR_CONTENT_TYPE, bytes);
        for (key, value) in &self.metadata {
            message
                .headers
                .insert_text(key.clone(), value.as_str().ok_or(ResultEncodingError)?)
                .map_err(|_| ResultEncodingError)?;
        }
        Ok(message)
    }
}
impl fmt::Debug for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Rejection")
            .field("category", &self.category)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message)
    }
}
impl Error for Rejection {}
impl plenora_runtime_messaging::ClassifyRetry for Rejection {
    fn retry_class(&self) -> plenora_runtime_messaging::RetryErrorClass {
        use plenora_runtime_messaging::RetryErrorClass as Class;
        match self.retry {
            PlenoraErrorRetry::Safe | PlenoraErrorRetry::After { .. } => Class::Retryable,
            PlenoraErrorRetry::Quarantine => Class::DeadLetter,
            PlenoraErrorRetry::RequiresRecovery | PlenoraErrorRetry::RequiresIdempotencyKey => {
                Class::OutcomeUnknown
            }
            PlenoraErrorRetry::Never => Class::Permanent,
        }
    }
}

/// An error result could not be encoded within the portable metadata bounds.
#[derive(Debug)]
pub struct ResultEncodingError;
impl fmt::Display for ResultEncodingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("runtime result encoding failed")
    }
}
impl Error for ResultEncodingError {}

pub(crate) fn result_metadata(raw: &Map<String, Value>) -> Map<String, Value> {
    let mut metadata = Map::new();
    for (key, predicate) in [
        (
            CAPABILITY_OPERATION_METADATA_KEY,
            lexical::operation as fn(&str) -> bool,
        ),
        (OPERATION_VERSION_METADATA_KEY, lexical::version),
        (CORRELATION_ID_METADATA_KEY, lexical::uuid),
    ] {
        if let Some(value) = raw.get(key).and_then(Value::as_str).filter(|v| {
            predicate(v) && v.len() <= plenora_runtime_messaging::MAX_METADATA_VALUE_BYTES
        }) {
            metadata.insert(key.into(), value.into());
        }
    }
    let cause = raw
        .get(MESSAGE_ID_METADATA_KEY)
        .and_then(Value::as_str)
        .filter(|v| lexical::uuid(v));
    let mut id = MessageId::random().to_string();
    while cause == Some(id.as_str()) {
        id = MessageId::random().to_string();
    }
    metadata.insert(MESSAGE_ID_METADATA_KEY.into(), id.into());
    if let Some(cause) = cause {
        metadata.insert(CAUSATION_ID_METADATA_KEY.into(), cause.into());
    }
    metadata.insert(
        OUTPUT_CONTRACT_METADATA_KEY.into(),
        PLENORA_ERROR_CONTRACT.into(),
    );
    metadata
}
