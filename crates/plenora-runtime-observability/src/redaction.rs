use std::{
    fmt::{self, Debug, Display, Formatter},
    sync::Arc,
};

/// Data sensitivity classification used by redaction hooks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Sensitivity {
    /// Data explicitly approved for diagnostics.
    Public,
    /// Credential, token, key, or other secret.
    Secret,
    /// Personally identifiable information.
    PersonallyIdentifiable,
}

/// Result of applying a redaction policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedactedText {
    value: Arc<str>,
    redacted: bool,
}

impl RedactedText {
    /// Creates diagnostic text explicitly approved for emission.
    #[must_use]
    pub fn public(value: impl Into<Arc<str>>) -> Self {
        Self {
            value: value.into(),
            redacted: false,
        }
    }

    /// Creates the fixed safe redaction marker.
    #[must_use]
    pub fn redacted() -> Self {
        Self {
            value: Arc::from("[REDACTED]"),
            redacted: true,
        }
    }

    /// Returns the policy-approved text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.value
    }

    /// Returns whether the original value was replaced.
    #[must_use]
    pub const fn was_redacted(&self) -> bool {
        self.redacted
    }
}

impl Display for RedactedText {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.value)
    }
}

/// Application-provided redaction decision boundary.
pub trait RedactionPolicy: Send + Sync {
    /// Returns text safe for diagnostic emission.
    fn redact(&self, field: &str, value: &str, sensitivity: Sensitivity) -> RedactedText;
}

/// Conservative policy that emits public values and masks secrets and PII.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultRedactionPolicy;

impl RedactionPolicy for DefaultRedactionPolicy {
    fn redact(&self, _field: &str, value: &str, sensitivity: Sensitivity) -> RedactedText {
        match sensitivity {
            Sensitivity::Public => RedactedText::public(value),
            Sensitivity::Secret | Sensitivity::PersonallyIdentifiable => RedactedText::redacted(),
        }
    }
}

/// Sensitive value whose standard diagnostics never reveal its contents.
pub struct Sensitive<T> {
    value: T,
    sensitivity: Sensitivity,
}

impl<T> Sensitive<T> {
    /// Wraps a sensitive value.
    #[must_use]
    pub const fn new(value: T, sensitivity: Sensitivity) -> Self {
        Self { value, sensitivity }
    }

    /// Returns the sensitivity classification.
    #[must_use]
    pub const fn sensitivity(&self) -> Sensitivity {
        self.sensitivity
    }

    /// Explicitly exposes the contained value to an authorized caller.
    #[must_use]
    pub const fn expose(&self) -> &T {
        &self.value
    }

    /// Consumes the wrapper and explicitly exposes the contained value.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> Debug for Sensitive<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("Sensitive([REDACTED])")
    }
}

impl<T> Display for Sensitive<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

#[cfg(test)]
mod tests {
    use super::{DefaultRedactionPolicy, RedactionPolicy, Sensitive, Sensitivity};

    #[test]
    fn default_policy_masks_secret_and_pii() {
        let policy = DefaultRedactionPolicy;
        for sensitivity in [Sensitivity::Secret, Sensitivity::PersonallyIdentifiable] {
            let output = policy.redact("field", "sensitive-value", sensitivity);
            assert!(output.was_redacted());
            assert_eq!(output.as_str(), "[REDACTED]");
        }
    }

    #[test]
    fn sensitive_diagnostics_never_show_value() {
        let value = Sensitive::new("sensitive-value", Sensitivity::Secret);
        assert_eq!(format!("{value}"), "[REDACTED]");
        assert!(!format!("{value:?}").contains("sensitive-value"));
    }

    #[test]
    fn public_redaction_and_explicit_sensitive_access_are_observable() {
        let policy = DefaultRedactionPolicy;
        let public = policy.redact("field", "safe", Sensitivity::Public);
        assert!(!public.was_redacted());
        assert_eq!(public.as_str(), "safe");
        assert_eq!(public.to_string(), "safe");

        let value = Sensitive::new(
            String::from("internal"),
            Sensitivity::PersonallyIdentifiable,
        );
        assert_eq!(value.sensitivity(), Sensitivity::PersonallyIdentifiable);
        assert_eq!(value.expose(), "internal");
        assert_eq!(value.into_inner(), "internal");
    }
}
