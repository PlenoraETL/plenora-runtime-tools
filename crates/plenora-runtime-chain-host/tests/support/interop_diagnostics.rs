//! Payload-free failure axes for the real surface matrix, including nonterminal handles.
use serde_json::{Value, json};

fn label(value: &Value, allowed: &[&str]) -> Value {
    value
        .as_str()
        .filter(|text| allowed.contains(text))
        .map_or(Value::Null, |text| json!(text))
}
fn state(value: &Value) -> Value {
    label(
        value,
        &[
            "pending",
            "running",
            "cancel_requested",
            "succeeded",
            "failed",
            "rejected",
            "partial",
            "cancelled",
            "requires_recovery",
            "blocked",
            "skipped",
        ],
    )
}
fn effect(value: &Value) -> Value {
    label(
        value,
        &["none", "unknown", "committed", "partial", "rolled_back"],
    )
}
fn error(value: &Value) -> Value {
    json!({
        "category":label(&value["category"], &["protocol", "invalid_configuration", "unsupported",
            "authorization", "schema", "timeout", "resource_limit", "conflict", "cancelled",
            "io", "storage", "execution", "internal", "data_mapping", "crs"]),
        "phase":label(&value["phase"], &["validate", "prepare", "dispatch", "read", "execute",
            "transform", "write", "commit", "finalize", "cancel"]),
        "code":label(&value["code"], &["CHAIN_RECOVERY", "CHAIN_UPSTREAM", "CHAIN_LIMIT",
            "CHAIN_RESULT", "CHAIN_SCHEMA", "CHAIN_STOPPED", "CHAIN_TIMEOUT"]),
        "effect":effect(&value["remote_effect"])
    })
}
pub fn summary(response: &Value) -> Value {
    let value = &response["value"];
    let observed = value.get("observed_result").unwrap_or(value);
    json!({
        "response_error":label(&response["error"], &["invalid", "schema", "unsupported", "conflict",
            "resource_limit", "timeout", "storage", "pending", "recovery", "failed", "cancelled", "expired", "not_found"]),
        "state":state(&value["state"]), "effect":effect(&value["remote_effect"]),
        "has_observed_result":value.get("observed_result").is_some(),
        "observed_state":state(&observed["state"]), "observed_effect":effect(&observed["remote_effect"]),
        "error":error(&observed["error"]),
        "elapsed_ms":observed["counters"]["elapsed_ms"].as_u64(),
        "other_errors":observed["other_errors"].as_array().map(|errors| errors.iter().map(error).collect::<Vec<_>>()),
        "steps":observed["steps"].as_array().map(|steps| steps.iter().enumerate().map(|(index, step)| json!({
            "index":index, "state":state(&step["state"]), "effect":effect(&step["remote_effect"]),
            "attempt":step["attempt"].as_u64(), "elapsed_ms":step["counters"]["elapsed_ms"].as_u64(),
            "stop_confirmed":step["stop_confirmed"].as_bool(), "error":error(&step["error"]),
            "other_errors":step["other_errors"].as_array().map(|errors| errors.iter().map(error).collect::<Vec<_>>())
        })).collect::<Vec<_>>())
    })
}

#[test]
fn diagnostics_distinguish_handles_and_outcomes_without_echoing_data() {
    for (observed, expected) in [(false, false), (true, true)] {
        let result = json!({"state":"requires_recovery","remote_effect":"unknown",
            "error":{"message":"SECRET_SENTINEL","code":"SECRET_SENTINEL"},
            "steps":[{"state":"requires_recovery","attempt":1,"remote_effect":"unknown",
                "outputs":{"secret":"SECRET_SENTINEL"}, "error":{"category":"io","phase":"finalize",
                "code":"CHAIN_RECOVERY","message":"SECRET_SENTINEL"}}]});
        let value = if observed {
            json!({"state":"requires_recovery","observed_result":result})
        } else {
            result
        };
        let facts = summary(&json!({"error":"recovery","value":value}));
        assert!(!facts.to_string().contains("SECRET_SENTINEL"));
        assert_eq!(facts["has_observed_result"], expected);
        assert_eq!(facts["steps"][0]["error"]["code"], "CHAIN_RECOVERY");
        assert_eq!(facts["steps"][0]["attempt"], 1);
    }
}
