//! Typed Errors 1.0 bounds and adapter mapping contracts.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

use plenora_runtime_capabilities::{
    CapabilityFailure, CapabilityRemoteEffect, PLENORA_ERROR_CONTENT_TYPE, PlenoraError,
    PlenoraErrorCategory, PlenoraErrorPhase, PlenoraErrorRemoteEffect, PlenoraErrorRetry,
    PlenoraErrorValidationErrorKind,
};
use plenora_runtime_messaging::{ClassifyRetry, RetryErrorClass};
use serde_json::{Map, Value};

#[derive(Clone, Copy, Debug)]
struct AdapterError;

impl Display for AdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("private adapter diagnostic")
    }
}

impl Error for AdapterError {}

#[test]
fn public_error_serializes_all_four_axes_and_redacts_diagnostics() -> Result<(), Box<dyn Error>> {
    let mut details = Map::new();
    details.insert(
        String::from("contract"),
        Value::String(String::from("plenora-rest-error-details-v1")),
    );
    details.insert(
        String::from("private"),
        Value::String(String::from("secret-value")),
    );
    let error = PlenoraError::new(
        PlenoraErrorCategory::Transient,
        PlenoraErrorPhase::Connect,
        PlenoraErrorRemoteEffect::None,
        PlenoraErrorRetry::After { delay_ms: 250 },
        "redacted public diagnostic",
    )?
    .with_code("REST_CONNECT_RETRY")?
    .with_provider("example_provider")?
    .with_execution_id("execution-1")?
    .with_details(details)?;

    let encoded = error.to_json()?;
    let value: Value = serde_json::from_slice(&encoded)?;
    assert_eq!(value["category"], "transient");
    assert_eq!(value["phase"], "connect");
    assert_eq!(value["remote_effect"], "none");
    assert_eq!(value["retry"]["kind"], "after");
    assert_eq!(value["retry"]["delay_ms"], 250);
    assert_eq!(value["code"], "REST_CONNECT_RETRY");
    assert_eq!(value["provider"], "example_provider");
    assert_eq!(
        error.to_message()?.content_type.as_ref(),
        PLENORA_ERROR_CONTENT_TYPE
    );
    let diagnostics = format!("{error:?}");
    assert!(!diagnostics.contains("redacted public diagnostic"));
    assert!(!diagnostics.contains("secret-value"));
    Ok(())
}

#[test]
fn public_error_rejects_unsafe_retry_and_unbounded_details() -> Result<(), Box<dyn Error>> {
    let unsafe_retry = PlenoraError::new(
        PlenoraErrorCategory::Timeout,
        PlenoraErrorPhase::Write,
        PlenoraErrorRemoteEffect::Unknown,
        PlenoraErrorRetry::Safe,
        "timeout",
    )
    .err()
    .ok_or("unknown remote effect accepted safe retry")?;
    assert_eq!(
        unsafe_retry.kind(),
        PlenoraErrorValidationErrorKind::UnsafeUnknownEffectRetry
    );

    let excessive_delay = PlenoraError::new(
        PlenoraErrorCategory::Transient,
        PlenoraErrorPhase::Connect,
        PlenoraErrorRemoteEffect::None,
        PlenoraErrorRetry::After {
            delay_ms: 86_400_001,
        },
        "retry later",
    )
    .err()
    .ok_or("excessive retry delay was accepted")?;
    assert_eq!(
        excessive_delay.kind(),
        PlenoraErrorValidationErrorKind::InvalidRetryDelay
    );

    for details in [
        nested_details(9),
        single_detail(Value::Array(vec![Value::Null; 129])),
        single_detail(Value::String("x".repeat(4_097))),
    ] {
        let error = base_error()?
            .with_details(details)
            .err()
            .ok_or("unbounded details were accepted")?;
        assert_eq!(
            error.kind(),
            PlenoraErrorValidationErrorKind::InvalidDetails
        );
    }
    Ok(())
}

#[test]
fn adapter_public_mapping_drives_retry_without_status_or_string_inference()
-> Result<(), Box<dyn Error>> {
    let recovery = PlenoraError::new(
        PlenoraErrorCategory::Execution,
        PlenoraErrorPhase::Commit,
        PlenoraErrorRemoteEffect::Unknown,
        PlenoraErrorRetry::RequiresRecovery,
        "remote commit outcome is unknown",
    )?;
    let failure = CapabilityFailure::with_public_error(recovery.clone(), AdapterError);
    assert_eq!(failure.retry_class(), RetryErrorClass::OutcomeUnknown);
    assert_eq!(failure.remote_effect(), CapabilityRemoteEffect::Unknown);
    assert_eq!(failure.public_error(), Some(&recovery));
    assert!(!format!("{failure:?}").contains("private adapter diagnostic"));

    let retryable = PlenoraError::new(
        PlenoraErrorCategory::Transient,
        PlenoraErrorPhase::Connect,
        PlenoraErrorRemoteEffect::None,
        PlenoraErrorRetry::After { delay_ms: 100 },
        "dependency is temporarily unavailable",
    )?;
    let failure = CapabilityFailure::with_public_error(retryable, AdapterError);
    assert_eq!(failure.retry_class(), RetryErrorClass::Retryable);
    assert_eq!(failure.remote_effect(), CapabilityRemoteEffect::NotStarted);
    Ok(())
}

fn base_error() -> Result<PlenoraError, Box<dyn Error>> {
    Ok(PlenoraError::new(
        PlenoraErrorCategory::Internal,
        PlenoraErrorPhase::Finalize,
        PlenoraErrorRemoteEffect::None,
        PlenoraErrorRetry::Never,
        "bounded error",
    )?)
}

fn single_detail(value: Value) -> Map<String, Value> {
    let mut details = Map::new();
    details.insert(String::from("value"), value);
    details
}

fn nested_details(depth: usize) -> Map<String, Value> {
    let mut value = Value::Null;
    for _ in 0..depth {
        value = serde_json::json!({"nested": value});
    }
    single_detail(value)
}
