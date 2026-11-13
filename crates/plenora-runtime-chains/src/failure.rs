//! One data-free error projection for native, CLI, Python and remote responses.
use crate::{Error, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Complete public failure axes. The original result stays in Response.value, never in Display.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    /// Stable coordination code; domain code is retained separately.
    pub code: String,
    /// Domain, boundary or coordination; transport uncertainty is a coordination failure.
    pub kind: String,
    /// Contract error category.
    pub category: String,
    /// Proven phase, or dispatch when the remote phase is not known.
    pub phase: String,
    /// Exact known effect; unavailable confirmation is conservatively unknown.
    pub remote_effect: String,
    /// Whether effect describes the known run or the request before a run receipt exists.
    pub effect_scope: String,
    /// Structured retry policy, including backoff when supplied by the domain.
    pub retry: Value,
    /// Canonical durable run identity when a receipt is available.
    pub run_id: Option<String>,
    /// Domain error code without replacing the coordination discriminator.
    pub domain_code: Option<String>,
}
impl std::fmt::Debug for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Failure { structured_context }")
    }
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Chain operation rejected or incomplete.")
    }
}
impl std::error::Error for Failure {}
impl Error {
    /// Canonical code shared by native exceptions and serialized responses.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Invalid => "invalid",
            Self::Schema => "schema",
            Self::Unsupported => "unsupported",
            Self::Conflict => "conflict",
            Self::ResourceLimit => "resource_limit",
            Self::Timeout => "timeout",
            Self::Storage => "storage",
            Self::Pending => "pending",
            Self::Recovery => "recovery",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
            Self::NotFound => "not_found",
        }
    }
}
impl Response {
    /// Project complete failure axes without copying or truncating the original domain result.
    #[must_use]
    pub fn failure(&self) -> Option<Failure> {
        let error = self.error?;
        let uncertain = matches!(error, Error::Recovery | Error::Storage | Error::Timeout);
        let (category, phase) = match error {
            Error::Invalid => ("invalid_configuration", "validate"),
            Error::Schema => ("schema", "validate"),
            Error::Unsupported => ("unsupported", "validate"),
            Error::Conflict => ("conflict", "validate"),
            Error::ResourceLimit => ("resource_limit", "validate"),
            Error::Timeout => ("timeout", "dispatch"),
            Error::Storage => ("storage", "dispatch"),
            Error::Pending | Error::Recovery | Error::Failed => ("execution", "dispatch"),
            Error::Cancelled => ("cancelled", "cancel"),
            Error::Expired => ("expired", "validate"),
            Error::NotFound => ("not_found", "validate"),
        };
        let domain = self
            .value
            .get("error")
            .or_else(|| self.value.get("boundary_error"))
            .filter(|e| e.is_object());
        let text = |name: &str, fallback: &str| {
            domain
                .and_then(|e| e[name].as_str())
                .unwrap_or(fallback)
                .to_owned()
        };
        let effect = self
            .value
            .get("remote_effect")
            .or_else(|| domain.and_then(|e| e.get("remote_effect")))
            .and_then(Value::as_str)
            .unwrap_or(if uncertain { "unknown" } else { "none" });
        Some(Failure {
            code: error.code().into(),
            kind: if self.value.get("boundary_error").is_some() { "boundary" } else if domain.is_some() {
                "domain"
            } else {
                "coordination"
            }
            .into(),
            category: text("category", category),
            phase: text("phase", phase),
            remote_effect: effect.into(),
            effect_scope: if self.value.get("run_id").and_then(Value::as_str).is_some_and(crate::uuid) { "run" } else { "request" }.into(),
            retry: self.value.get("retry").or_else(|| domain.and_then(|e| e.get("retry"))).cloned().unwrap_or_else(
                || json!({"kind": if effect == "unknown" { "requires_recovery" } else { "never" }}),
            ),
            run_id: self
                .value
                .get("run_id")
                .and_then(Value::as_str)
                .filter(|s| crate::uuid(s))
                .map(str::to_owned),
            domain_code: domain.and_then(|e| e["code"].as_str()).map(str::to_owned),
        })
    }
    /// Stable machine exit code; a partial/unknown outcome can never imply success.
    #[must_use]
    pub fn exit_code(&self) -> u8 {
        match self.error {
            None => 0,
            Some(
                Error::Invalid
                | Error::Schema
                | Error::Unsupported
                | Error::Conflict
                | Error::ResourceLimit,
            ) => 2,
            Some(Error::Failed) => 3,
            Some(Error::Recovery) => 4,
            Some(Error::Cancelled) => 5,
            Some(_) => 6,
        }
    }
}
impl Serialize for Response {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Response", 3)?;
        state.serialize_field("value", &self.value)?;
        state.serialize_field("error", &self.error)?;
        state.serialize_field("failure", &self.failure())?;
        state.end()
    }
}
impl<'de> Deserialize<'de> for Response {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            value: Value,
            error: Option<Error>,
            failure: Option<Failure>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let response = Self {
            value: wire.value,
            error: wire.error,
        };
        // Older durable receipts omit this redundant projection; derive it from their retained axes.
        if wire.failure.is_some() && response.failure() != wire.failure {
            return Err(serde::de::Error::custom("inconsistent failure axes"));
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_coordination_error_roundtrips_with_complete_safe_axes()
    -> Result<(), Box<dyn std::error::Error>> {
        for error in [
            Error::Invalid,
            Error::Schema,
            Error::Unsupported,
            Error::Conflict,
            Error::ResourceLimit,
            Error::Timeout,
            Error::Storage,
            Error::Pending,
            Error::Recovery,
            Error::Failed,
            Error::Cancelled,
            Error::Expired,
            Error::NotFound,
        ] {
            let response = Response {
                value: Value::Null,
                error: Some(error),
            };
            let encoded = serde_json::to_value(&response)?;
            assert_eq!(encoded["error"], error.code());
            let failure = response.failure().ok_or("failure")?;
            assert!(!failure.category.is_empty() && !failure.phase.is_empty());
            assert_ne!(response.exit_code(), 0);
            assert_eq!(
                serde_json::from_value::<Response>(encoded.clone())?.failure(),
                Some(failure)
            );
            let mut legacy = encoded.clone();
            legacy.as_object_mut().ok_or("object")?.remove("failure");
            assert_eq!(
                serde_json::from_value::<Response>(legacy)?.failure(),
                response.failure()
            );
            let mut forged = encoded;
            forged["failure"]["remote_effect"] = json!("invented");
            assert!(serde_json::from_value::<Response>(forged).is_err());
        }
        Ok(())
    }
    #[test]
    fn domain_phase_effect_retry_and_complete_result_are_not_replaced()
    -> Result<(), Box<dyn std::error::Error>> {
        for effect in ["none", "committed", "partial", "unknown", "rolled_back"] {
            let response = Response {
                error: Some(Error::Failed),
                value: json!({"run_id":"00000000-0000-0000-0000-000000000001","remote_effect":effect,"error":{"category":"io","phase":"cleanup","code":"CHAIN_UPSTREAM","retry":{"kind":"never"}},"reports":[{"evidence":"retained"}]}),
            };
            let value = serde_json::to_value(&response)?;
            assert_eq!(value["value"], response.value);
            assert_eq!(value["failure"]["phase"], "cleanup");
            assert_eq!(value["failure"]["remote_effect"], effect);
            assert_eq!(response.exit_code(), 3);
        }
        Ok(())
    }
}
