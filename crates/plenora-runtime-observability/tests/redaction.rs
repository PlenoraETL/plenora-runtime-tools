//! External redaction-policy contract tests.

use plenora_runtime_observability::{RedactedText, RedactionPolicy, Sensitivity};

struct ExternalPolicy;

impl RedactionPolicy for ExternalPolicy {
    fn redact(&self, field: &str, value: &str, sensitivity: Sensitivity) -> RedactedText {
        match (field, sensitivity) {
            ("safe", Sensitivity::Public) => RedactedText::public(value),
            _ => RedactedText::redacted(),
        }
    }
}

#[test]
fn external_policy_uses_explicit_public_or_fixed_redacted_text() {
    let policy = ExternalPolicy;
    let public = policy.redact("safe", "visible", Sensitivity::Public);
    let secret = policy.redact("credential", "hidden", Sensitivity::Secret);
    assert_eq!(public.as_str(), "visible");
    assert!(!public.was_redacted());
    assert_eq!(secret.as_str(), "[REDACTED]");
    assert!(secret.was_redacted());
}
