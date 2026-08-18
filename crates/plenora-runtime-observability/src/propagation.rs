use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    str::FromStr as _,
    sync::Arc,
};

use opentelemetry::{
    Context,
    propagation::{Extractor, Injector, TextMapPropagator},
};
pub use plenora_runtime_messaging::CORRELATION_ID_METADATA_KEY;
use plenora_runtime_messaging::{CorrelationId, MessageMetadata};

/// Metadata key carrying the W3C trace-parent field.
pub const TRACEPARENT_METADATA_KEY: &str = "plenora.trace.traceparent";
/// Metadata key carrying the W3C trace-state field.
pub const TRACESTATE_METADATA_KEY: &str = "plenora.trace.tracestate";
const TRACEPARENT_FIELD: &str = "traceparent";
const TRACESTATE_FIELD: &str = "tracestate";
const MAX_PROPAGATION_VALUE_LEN: usize = 512;

/// Propagation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PropagationErrorKind {
    /// The propagator attempted to emit a field outside W3C trace context.
    UnsupportedField,
    /// A propagated value was empty, oversized, or not safe HTTP-field ASCII.
    InvalidValue,
    /// A stored propagation value was not UTF-8.
    InvalidUtf8,
    /// A metadata key could not be stored.
    Metadata,
    /// The correlation identifier was malformed.
    InvalidCorrelationId,
}

/// Redaction-safe context propagation error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PropagationError {
    kind: PropagationErrorKind,
    field: Arc<str>,
}

impl PropagationError {
    fn new(kind: PropagationErrorKind, field: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            field: field.into(),
        }
    }

    /// Returns the failure category.
    #[must_use]
    pub const fn kind(&self) -> PropagationErrorKind {
        self.kind
    }

    /// Returns the affected propagation field without revealing its value.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }
}

impl Display for PropagationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "propagation field '{}' is invalid: {:?}",
            self.field, self.kind
        )
    }
}

impl Error for PropagationError {}

/// `OpenTelemetry` injector that maps only W3C trace-context fields into message metadata.
pub struct MessageMetadataInjector<'a> {
    metadata: &'a mut MessageMetadata,
    error: Option<PropagationError>,
}

impl<'a> MessageMetadataInjector<'a> {
    /// Creates an injector over mutable message metadata.
    #[must_use]
    pub fn new(metadata: &'a mut MessageMetadata) -> Self {
        Self {
            metadata,
            error: None,
        }
    }

    /// Completes injection and reports any field rejected by the carrier.
    ///
    /// # Errors
    ///
    /// Returns an error for non-W3C fields, unsafe values, or metadata insertion failure.
    pub fn finish(self) -> Result<(), PropagationError> {
        self.error.map_or(Ok(()), Err)
    }

    fn reject(&mut self, error: PropagationError) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }
}

impl Injector for MessageMetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if self.error.is_some() {
            return;
        }
        let Some(metadata_key) = metadata_key(key) else {
            self.reject(PropagationError::new(
                PropagationErrorKind::UnsupportedField,
                "unsupported",
            ));
            return;
        };
        if !valid_propagation_value(&value) {
            self.reject(PropagationError::new(
                PropagationErrorKind::InvalidValue,
                key,
            ));
            return;
        }
        if self.metadata.insert_text(metadata_key, value).is_err() {
            self.reject(PropagationError::new(PropagationErrorKind::Metadata, key));
        }
    }
}

/// `OpenTelemetry` extractor that exposes W3C trace-context fields from message metadata.
pub struct MessageMetadataExtractor<'a> {
    traceparent: Option<&'a str>,
    tracestate: Option<&'a str>,
}

impl<'a> MessageMetadataExtractor<'a> {
    /// Validates and creates an extractor over message metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when a present trace field is not UTF-8 or contains unsafe text.
    pub fn new(metadata: &'a MessageMetadata) -> Result<Self, PropagationError> {
        Ok(Self {
            traceparent: read_field(metadata, TRACEPARENT_METADATA_KEY, TRACEPARENT_FIELD)?,
            tracestate: read_field(metadata, TRACESTATE_METADATA_KEY, TRACESTATE_FIELD)?,
        })
    }
}

impl Extractor for MessageMetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        if key.eq_ignore_ascii_case(TRACEPARENT_FIELD) {
            self.traceparent
        } else if key.eq_ignore_ascii_case(TRACESTATE_FIELD) {
            self.tracestate
        } else {
            None
        }
    }

    fn keys(&self) -> Vec<&str> {
        let mut keys = Vec::with_capacity(2);
        if self.traceparent.is_some() {
            keys.push(TRACEPARENT_FIELD);
        }
        if self.tracestate.is_some() {
            keys.push(TRACESTATE_FIELD);
        }
        keys
    }
}

/// Injects an explicit `OpenTelemetry` context into message metadata.
///
/// The supplied propagator is application-owned; no global propagator is consulted or installed.
///
/// # Errors
///
/// Returns an error when the propagator emits non-W3C fields or unsafe values.
pub fn inject_context(
    propagator: &dyn TextMapPropagator,
    context: &Context,
    metadata: &mut MessageMetadata,
) -> Result<(), PropagationError> {
    inject_transactionally(metadata, |injector| {
        propagator.inject_context(context, injector);
    })
}

/// Extracts an `OpenTelemetry` context from message metadata.
///
/// The supplied propagator is application-owned; no global propagator is consulted or installed.
///
/// # Errors
///
/// Returns an error when a stored propagation field is not valid W3C-safe text.
pub fn extract_context(
    propagator: &dyn TextMapPropagator,
    metadata: &MessageMetadata,
) -> Result<Context, PropagationError> {
    let extractor = MessageMetadataExtractor::new(metadata)?;
    Ok(propagator.extract_with_context(&Context::new(), &extractor))
}

/// Stores a correlation identifier in message metadata.
///
/// # Errors
///
/// Returns an error if the fixed metadata key cannot be inserted.
pub fn inject_correlation_id(
    correlation_id: CorrelationId,
    metadata: &mut MessageMetadata,
) -> Result<(), PropagationError> {
    metadata
        .insert_text(CORRELATION_ID_METADATA_KEY, correlation_id.to_string())
        .map_err(|_error| {
            PropagationError::new(PropagationErrorKind::Metadata, "correlation_id")
        })?;
    Ok(())
}

/// Extracts an optional correlation identifier from message metadata.
///
/// # Errors
///
/// Returns an error when the stored value is not UTF-8 or is not a valid identifier.
pub fn extract_correlation_id(
    metadata: &MessageMetadata,
) -> Result<Option<CorrelationId>, PropagationError> {
    let value = metadata
        .get_text(CORRELATION_ID_METADATA_KEY)
        .map_err(|_error| {
            PropagationError::new(PropagationErrorKind::InvalidUtf8, "correlation_id")
        })?;
    value
        .map(|value| {
            CorrelationId::from_str(value).map_err(|_error| {
                PropagationError::new(PropagationErrorKind::InvalidCorrelationId, "correlation_id")
            })
        })
        .transpose()
}

fn read_field<'a>(
    metadata: &'a MessageMetadata,
    metadata_key: &str,
    field: &'static str,
) -> Result<Option<&'a str>, PropagationError> {
    let value = metadata
        .get_text(metadata_key)
        .map_err(|_error| PropagationError::new(PropagationErrorKind::InvalidUtf8, field))?;
    if value.is_some_and(|value| !valid_propagation_value(value)) {
        Err(PropagationError::new(
            PropagationErrorKind::InvalidValue,
            field,
        ))
    } else {
        Ok(value)
    }
}

fn metadata_key(field: &str) -> Option<&'static str> {
    if field.eq_ignore_ascii_case(TRACEPARENT_FIELD) {
        Some(TRACEPARENT_METADATA_KEY)
    } else if field.eq_ignore_ascii_case(TRACESTATE_FIELD) {
        Some(TRACESTATE_METADATA_KEY)
    } else {
        None
    }
}

fn valid_propagation_value(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_PROPAGATION_VALUE_LEN
        && value.bytes().all(|byte| matches!(byte, 0x20..=0x7e))
}

fn inject_transactionally(
    metadata: &mut MessageMetadata,
    inject: impl FnOnce(&mut dyn Injector),
) -> Result<(), PropagationError> {
    let mut scratch = MessageMetadata::new();
    let mut injector = MessageMetadataInjector::new(&mut scratch);
    inject(&mut injector);
    injector.finish()?;

    let mut staged = metadata.clone();
    drop(staged.remove(TRACEPARENT_METADATA_KEY));
    drop(staged.remove(TRACESTATE_METADATA_KEY));
    copy_field(&scratch, &mut staged, TRACEPARENT_METADATA_KEY)?;
    copy_field(&scratch, &mut staged, TRACESTATE_METADATA_KEY)?;
    *metadata = staged;
    Ok(())
}

fn copy_field(
    source: &MessageMetadata,
    destination: &mut MessageMetadata,
    key: &'static str,
) -> Result<(), PropagationError> {
    if let Some(value) = source.get(key) {
        destination
            .insert(key, value.clone())
            .map_err(|_error| PropagationError::new(PropagationErrorKind::Metadata, key))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use opentelemetry::propagation::text_map_propagator::FieldIter;
    use opentelemetry::{
        Context,
        propagation::{Extractor as _, Injector as _, TextMapPropagator},
    };
    use plenora_runtime_messaging::{CorrelationId, MessageMetadata};

    use super::{
        MessageMetadataExtractor, MessageMetadataInjector, PropagationError, PropagationErrorKind,
        TRACEPARENT_METADATA_KEY, TRACESTATE_METADATA_KEY, extract_context, extract_correlation_id,
        inject_context, inject_correlation_id, inject_transactionally,
    };

    #[derive(Debug)]
    struct TestPropagator {
        fields: Vec<String>,
    }

    impl TestPropagator {
        fn new() -> Self {
            Self {
                fields: vec![String::from("traceparent"), String::from("tracestate")],
            }
        }
    }

    impl TextMapPropagator for TestPropagator {
        fn inject_context(
            &self,
            _context: &Context,
            injector: &mut dyn opentelemetry::propagation::Injector,
        ) {
            injector.set(
                "traceparent",
                String::from("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            );
            injector.set("tracestate", String::from("vendor=value"));
        }

        fn extract_with_context(
            &self,
            context: &Context,
            extractor: &dyn opentelemetry::propagation::Extractor,
        ) -> Context {
            let _traceparent = extractor.get("traceparent");
            let _tracestate = extractor.get("tracestate");
            context.clone()
        }

        fn fields(&self) -> FieldIter<'_> {
            FieldIter::new(&self.fields)
        }
    }

    #[test]
    fn traceparent_maps_to_namespaced_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let mut metadata = MessageMetadata::new();
        let mut injector = MessageMetadataInjector::new(&mut metadata);
        injector.set(
            "traceparent",
            String::from("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );
        injector.finish()?;
        assert!(metadata.contains_key(TRACEPARENT_METADATA_KEY));
        let extractor = MessageMetadataExtractor::new(&metadata)?;
        assert_eq!(
            extractor.get("traceparent"),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );
        Ok(())
    }

    #[test]
    fn unsafe_text_is_rejected_without_echoing_it() {
        let mut metadata = MessageMetadata::new();
        let mut injector = MessageMetadataInjector::new(&mut metadata);
        injector.set("traceparent", String::from("secret\r\nheader"));
        let error = injector.finish();
        assert_eq!(
            error.map_err(|error| error.kind()),
            Err(PropagationErrorKind::InvalidValue)
        );
    }

    #[test]
    fn failed_injection_does_not_mutate_original_metadata() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut metadata = MessageMetadata::new();
        metadata.insert_text("application.kind", "original")?;
        metadata.insert_text(TRACEPARENT_METADATA_KEY, "existing")?;
        let original = metadata.clone();
        let result = inject_transactionally(&mut metadata, |injector| {
            injector.set(
                "traceparent",
                String::from("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            );
            injector.set("baggage", String::from("private=value"));
        });
        assert_eq!(
            result.map_err(|error| error.kind()),
            Err(PropagationErrorKind::UnsupportedField)
        );
        assert_eq!(metadata, original);
        Ok(())
    }

    #[test]
    fn correlation_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let correlation_id = CorrelationId::random();
        let mut metadata = MessageMetadata::new();
        inject_correlation_id(correlation_id, &mut metadata)?;
        assert_eq!(extract_correlation_id(&metadata)?, Some(correlation_id));
        Ok(())
    }

    #[test]
    fn public_context_helpers_round_trip_both_w3c_fields() -> Result<(), Box<dyn std::error::Error>>
    {
        let propagator = TestPropagator::new();
        let mut metadata = MessageMetadata::new();
        inject_context(&propagator, &Context::new(), &mut metadata)?;

        assert!(metadata.contains_key(TRACEPARENT_METADATA_KEY));
        assert!(metadata.contains_key(TRACESTATE_METADATA_KEY));
        drop(extract_context(&propagator, &metadata)?);
        assert_eq!(propagator.fields().count(), 2);
        Ok(())
    }

    #[test]
    fn carrier_reports_unsupported_unsafe_and_invalid_utf8_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut metadata = MessageMetadata::new();
        let mut injector = MessageMetadataInjector::new(&mut metadata);
        injector.set("baggage", String::from("private=value"));
        injector.set("traceparent", String::from("ignored-after-first-error"));
        let error = injector
            .finish()
            .err()
            .ok_or("baggage unexpectedly accepted")?;
        assert_eq!(error.kind(), PropagationErrorKind::UnsupportedField);
        assert_eq!(error.field(), "unsupported");
        assert!(error.to_string().contains("UnsupportedField"));

        let mut empty = MessageMetadata::new();
        let mut injector = MessageMetadataInjector::new(&mut empty);
        injector.set("traceparent", String::new());
        assert_eq!(
            injector.finish().map_err(|error| error.kind()),
            Err(PropagationErrorKind::InvalidValue)
        );

        let mut invalid = MessageMetadata::new();
        invalid.insert(TRACEPARENT_METADATA_KEY, vec![0xff_u8])?;
        assert!(matches!(
            MessageMetadataExtractor::new(&invalid),
            Err(error) if error.kind() == PropagationErrorKind::InvalidUtf8
        ));

        let mut unsafe_value = MessageMetadata::new();
        unsafe_value.insert_text(TRACEPARENT_METADATA_KEY, "line\nbreak")?;
        assert!(matches!(
            MessageMetadataExtractor::new(&unsafe_value),
            Err(error) if error.kind() == PropagationErrorKind::InvalidValue
        ));
        Ok(())
    }

    #[test]
    fn extractor_is_case_insensitive_and_correlation_errors_are_bounded()
    -> Result<(), Box<dyn std::error::Error>> {
        let empty = MessageMetadata::new();
        assert_eq!(extract_correlation_id(&empty)?, None);

        let mut metadata = MessageMetadata::new();
        metadata.insert_text(TRACEPARENT_METADATA_KEY, "parent")?;
        metadata.insert_text(TRACESTATE_METADATA_KEY, "state")?;
        let extractor = MessageMetadataExtractor::new(&metadata)?;
        assert_eq!(extractor.get("TrAcEpArEnT"), Some("parent"));
        assert_eq!(extractor.get("TRACESTATE"), Some("state"));
        assert_eq!(extractor.get("baggage"), None);
        assert_eq!(extractor.keys(), vec!["traceparent", "tracestate"]);

        let mut malformed = MessageMetadata::new();
        malformed.insert_text(super::CORRELATION_ID_METADATA_KEY, "not-a-uuid")?;
        assert_eq!(
            extract_correlation_id(&malformed).map_err(|error| error.kind()),
            Err(PropagationErrorKind::InvalidCorrelationId)
        );
        let mut binary = MessageMetadata::new();
        binary.insert(super::CORRELATION_ID_METADATA_KEY, vec![0xff_u8])?;
        assert_eq!(
            extract_correlation_id(&binary).map_err(|error| error.kind()),
            Err(PropagationErrorKind::InvalidUtf8)
        );
        Ok(())
    }

    #[test]
    fn transactional_injection_preserves_full_destination_on_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut metadata = MessageMetadata::new();
        for suffix in ["a", "b", "c", "d"] {
            metadata.insert(format!("application.{suffix}"), vec![b'x'; 8_100])?;
        }
        let original = metadata.clone();
        let result = inject_transactionally(&mut metadata, |injector| {
            injector.set("traceparent", "x".repeat(512));
        });

        assert_eq!(
            result.map_err(|error| error.kind()),
            Err(PropagationErrorKind::Metadata)
        );
        assert_eq!(metadata, original);
        Ok(())
    }

    #[test]
    fn propagation_error_diagnostics_never_include_values() {
        let error = PropagationError::new(PropagationErrorKind::InvalidValue, "traceparent");
        assert_eq!(error.field(), "traceparent");
        assert!(format!("{error:?}").contains("InvalidValue"));
        assert!(error.to_string().contains("traceparent"));
    }
}
