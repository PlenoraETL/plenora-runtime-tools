use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::Arc,
};

/// Maximum number of labels accepted on one observation.
pub const MAX_LABELS: usize = 8;
/// Maximum UTF-8 byte length of a label value.
pub const MAX_LABEL_VALUE_LEN: usize = 64;

/// Allowlisted low-cardinality label keys.
///
/// Request, message, trace, correlation, tenant, actor, and user identifiers are deliberately
/// absent. They belong in spans or logs rather than metric labels.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum MetricLabelKey {
    /// Bounded runtime or adapter operation.
    Operation,
    /// Bounded result category.
    Outcome,
    /// Bounded failure reason.
    Reason,
    /// Bounded health or readiness status.
    Status,
    /// Broker family, not a server address.
    Broker,
    /// Stable configured component class.
    Component,
    /// Stable message kind, not message identity.
    MessageKind,
    /// Bounded persistence disposition.
    Disposition,
}

impl MetricLabelKey {
    /// Returns the stable backend-facing key.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Operation => "operation",
            Self::Outcome => "outcome",
            Self::Reason => "reason",
            Self::Status => "status",
            Self::Broker => "broker",
            Self::Component => "component",
            Self::MessageKind => "message.kind",
            Self::Disposition => "disposition",
        }
    }
}

/// Validated metric label.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricLabel {
    key: MetricLabelKey,
    value: Arc<str>,
}

impl MetricLabel {
    /// Validates a public, bounded-cardinality label value.
    ///
    /// # Errors
    ///
    /// Returns an error for empty, oversized, or non-portable values.
    pub fn new(key: MetricLabelKey, value: impl Into<Arc<str>>) -> Result<Self, LabelError> {
        let value = value.into();
        validate_value(&value)?;
        Ok(Self { key, value })
    }

    /// Returns the label key.
    #[must_use]
    pub const fn key(&self) -> MetricLabelKey {
        self.key
    }

    /// Returns the validated public value.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// Ordered, unique, bounded metric labels.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetricLabels {
    entries: Vec<MetricLabel>,
}

impl MetricLabels {
    /// Creates an empty label set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Adds one validated public label.
    ///
    /// Values must come from a finite application-defined vocabulary. Length and syntax checks
    /// cannot by themselves prove bounded cardinality.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid value, duplicate key, or excessive label count.
    pub fn try_insert(
        &mut self,
        key: MetricLabelKey,
        value: impl Into<Arc<str>>,
    ) -> Result<(), LabelError> {
        if self.entries.len() >= MAX_LABELS {
            return Err(LabelError::new(LabelErrorKind::TooManyLabels));
        }
        if self.entries.iter().any(|label| label.key == key) {
            return Err(LabelError::new(LabelErrorKind::DuplicateKey));
        }
        self.entries.push(MetricLabel::new(key, value)?);
        self.entries.sort_by_key(MetricLabel::key);
        Ok(())
    }

    /// Iterates over labels in stable key order.
    #[must_use]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &MetricLabel> {
        self.entries.iter()
    }

    /// Returns the number of labels.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether there are no labels.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn one(key: MetricLabelKey, value: &'static str) -> Self {
        Self {
            entries: vec![MetricLabel {
                key,
                value: Arc::from(value),
            }],
        }
    }
}

/// Label validation failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LabelErrorKind {
    /// Label value is empty.
    EmptyValue,
    /// Label value exceeds the documented bound.
    ValueTooLong,
    /// Label value contains characters outside the portable alphabet.
    InvalidCharacter,
    /// The label key is already present.
    DuplicateKey,
    /// The label count exceeds the documented bound.
    TooManyLabels,
}

/// Redaction-safe metric label validation error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LabelError {
    kind: LabelErrorKind,
}

impl LabelError {
    const fn new(kind: LabelErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the validation failure category.
    #[must_use]
    pub const fn kind(self) -> LabelErrorKind {
        self.kind
    }
}

impl Display for LabelError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid metric label: {:?}", self.kind)
    }
}

impl Error for LabelError {}

fn validate_value(value: &str) -> Result<(), LabelError> {
    if value.is_empty() {
        return Err(LabelError::new(LabelErrorKind::EmptyValue));
    }
    if value.len() > MAX_LABEL_VALUE_LEN {
        return Err(LabelError::new(LabelErrorKind::ValueTooLong));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(LabelError::new(LabelErrorKind::InvalidCharacter));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        LabelError, LabelErrorKind, MAX_LABEL_VALUE_LEN, MetricLabel, MetricLabelKey, MetricLabels,
    };

    #[test]
    fn labels_are_sorted_and_unique() -> Result<(), Box<dyn std::error::Error>> {
        let mut labels = MetricLabels::new();
        labels.try_insert(MetricLabelKey::Status, "ready")?;
        labels.try_insert(MetricLabelKey::Operation, "consume")?;
        let keys: Vec<_> = labels.iter().map(MetricLabel::key).collect();
        assert_eq!(
            keys,
            vec![MetricLabelKey::Operation, MetricLabelKey::Status]
        );
        assert_eq!(
            labels
                .try_insert(MetricLabelKey::Status, "healthy")
                .map_err(LabelError::kind),
            Err(LabelErrorKind::DuplicateKey)
        );
        Ok(())
    }

    #[test]
    fn label_values_are_bounded_and_portable() {
        let mut labels = MetricLabels::new();
        let oversized = "a".repeat(MAX_LABEL_VALUE_LEN + 1);
        assert_eq!(
            labels
                .try_insert(MetricLabelKey::Operation, oversized)
                .map_err(LabelError::kind),
            Err(LabelErrorKind::ValueTooLong)
        );
        assert_eq!(
            labels
                .try_insert(MetricLabelKey::Operation, "user@example.com")
                .map_err(LabelError::kind),
            Err(LabelErrorKind::InvalidCharacter)
        );
    }

    #[test]
    fn all_keys_and_validation_failures_are_observable() -> Result<(), Box<dyn std::error::Error>> {
        let keys = [
            MetricLabelKey::Operation,
            MetricLabelKey::Outcome,
            MetricLabelKey::Reason,
            MetricLabelKey::Status,
            MetricLabelKey::Broker,
            MetricLabelKey::Component,
            MetricLabelKey::MessageKind,
            MetricLabelKey::Disposition,
        ];
        let expected = [
            "operation",
            "outcome",
            "reason",
            "status",
            "broker",
            "component",
            "message.kind",
            "disposition",
        ];
        assert_eq!(keys.map(MetricLabelKey::as_str), expected);

        let mut labels = MetricLabels::new();
        assert!(labels.is_empty());
        for key in keys {
            labels.try_insert(key, "bounded")?;
        }
        assert_eq!(labels.len(), super::MAX_LABELS);
        assert_eq!(
            labels
                .try_insert(MetricLabelKey::Operation, "extra")
                .map_err(LabelError::kind),
            Err(LabelErrorKind::TooManyLabels)
        );
        assert_eq!(
            MetricLabel::new(MetricLabelKey::Operation, "").map_err(LabelError::kind),
            Err(LabelErrorKind::EmptyValue)
        );
        assert!(format!("{}", LabelError::new(LabelErrorKind::EmptyValue)).contains("EmptyValue"));
        Ok(())
    }
}
