//! Offline evaluator for the vocabulary used by the pinned schemas only.
//! Unknown validation keywords fail closed; this is not a general JSON Schema API.
use crate::error::{Checked, Code, ensure};
use regex::Regex;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::OnceLock;

struct Schemas {
    documents: BTreeMap<&'static str, Value>,
    patterns: BTreeMap<String, Regex>,
}
static SCHEMAS: OnceLock<Checked<Schemas>> = OnceLock::new();
const DOCUMENTS: &[(&str, &str)] = &[
    (
        "common.schema.json",
        include_str!("../../../contracts/chains-v2/schemas/common.schema.json"),
    ),
    (
        "chain-plan-v2.schema.json",
        include_str!("../../../contracts/chains-v2/schemas/chain-plan-v2.schema.json"),
    ),
    (
        "chain-result-v2.schema.json",
        include_str!("../../../contracts/chains-v2/schemas/chain-result-v2.schema.json"),
    ),
    (
        "chain-event-v2.schema.json",
        include_str!("../../../contracts/chains-v2/schemas/chain-event-v2.schema.json"),
    ),
    (
        "chain-error-v2.schema.json",
        include_str!("../../../contracts/chains-v2/schemas/chain-error-v2.schema.json"),
    ),
    (
        "reference-context.schema.json",
        include_str!("../../../contracts/chains-v2/schemas/reference-context.schema.json"),
    ),
    (
        "error-v1.schema.json",
        include_str!("../../../contracts/upstream/schemas/error-v1.schema.json"),
    ),
];

fn schemas() -> Checked<&'static Schemas> {
    SCHEMAS
        .get_or_init(|| {
            fn collect(value: &Value, patterns: &mut BTreeMap<String, Regex>) -> Checked<()> {
                if let Some(pattern) = value.get("pattern").and_then(Value::as_str) {
                    // ECMAScript exact-end assertion is equivalent to Rust's \z.
                    let rust = pattern.replace("(?![\\s\\S])", "\\z");
                    patterns.insert(
                        pattern.to_owned(),
                        Regex::new(&rust).map_err(|_| crate::Rejection(Code::Schema))?,
                    );
                }
                match value {
                    Value::Object(map) => {
                        for child in map.values() {
                            collect(child, patterns)?;
                        }
                    }
                    Value::Array(items) => {
                        for child in items {
                            collect(child, patterns)?;
                        }
                    }
                    _ => {}
                }
                Ok(())
            }
            let mut documents = BTreeMap::new();
            let mut patterns = BTreeMap::new();
            for &(name, raw) in DOCUMENTS {
                let value: Value =
                    serde_json::from_str(raw).map_err(|_| crate::Rejection(Code::Schema))?;
                collect(&value, &mut patterns)?;
                documents.insert(name, value);
            }
            Ok(Schemas {
                documents,
                patterns,
            })
        })
        .as_ref()
        .map_err(|error| *error)
}

pub(crate) fn structure(value: &Value, file: &str) -> Checked<()> {
    let schemas = schemas()?;
    let schema = schemas.documents.get(file).ok_or(Code::Schema)?;
    ensure(schemas.matches(value, schema, file), Code::Schema)
}

pub(crate) fn step_result(value: &Value) -> Checked<()> {
    let schemas = schemas()?;
    let schema = schemas
        .documents
        .get("common.schema.json")
        .and_then(|d| d.pointer("/$defs/step_result"))
        .ok_or(Code::Schema)?;
    ensure(
        schemas.matches(value, schema, "common.schema.json"),
        Code::Schema,
    )
}

fn size_valid(schema: &Value, length: usize, min: &str, max: &str) -> bool {
    schema
        .get(min)
        .and_then(Value::as_u64)
        .is_none_or(|n| length as u64 >= n)
        && schema
            .get(max)
            .and_then(Value::as_u64)
            .is_none_or(|n| length as u64 <= n)
}

impl Schemas {
    // The schema recursion is bounded by the trusted acyclic schema set; value
    // recursion has already been limited by the exact JSON decoder.
    #[allow(clippy::too_many_lines)]
    fn matches(&self, value: &Value, schema: &Value, file: &str) -> bool {
        if let Some(allowed) = schema.as_bool() {
            return allowed;
        }
        let Some(object) = schema.as_object() else {
            return false;
        };
        for key in object.keys() {
            if !matches!(
                key.as_str(),
                "$schema"
                    | "$id"
                    | "$comment"
                    | "$defs"
                    | "title"
                    | "description"
                    | "$ref"
                    | "type"
                    | "const"
                    | "enum"
                    | "oneOf"
                    | "allOf"
                    | "if"
                    | "then"
                    | "properties"
                    | "required"
                    | "additionalProperties"
                    | "propertyNames"
                    | "minProperties"
                    | "maxProperties"
                    | "items"
                    | "minItems"
                    | "maxItems"
                    | "uniqueItems"
                    | "pattern"
                    | "format"
                    | "minLength"
                    | "maxLength"
                    | "minimum"
                    | "maximum"
            ) {
                return false;
            }
        }
        if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
            let (path, pointer) = reference.split_once('#').unwrap_or((reference, ""));
            let target = if path.is_empty() {
                file
            } else {
                path.rsplit('/').next().unwrap_or("")
            };
            let Some(document) = self.documents.get(target) else {
                return false;
            };
            let Some(child) = document.pointer(pointer) else {
                return false;
            };
            if !self.matches(value, child, target) {
                return false;
            }
        }
        if let Some(kind) = schema.get("type") {
            let check = |kind: &Value| match kind.as_str() {
                Some("object") => value.is_object(),
                Some("array") => value.is_array(),
                Some("string") => value.is_string(),
                Some("boolean") => value.is_boolean(),
                Some("integer") => value.is_i64() || value.is_u64(),
                Some("null") => value.is_null(),
                _ => false,
            };
            if !kind
                .as_array()
                .map_or_else(|| check(kind), |kinds| kinds.iter().any(check))
            {
                return false;
            }
        }
        if schema
            .get("const")
            .is_some_and(|expected| value != expected)
        {
            return false;
        }
        if schema
            .get("enum")
            .and_then(Value::as_array)
            .is_some_and(|items| !items.contains(value))
        {
            return false;
        }
        if schema
            .get("oneOf")
            .and_then(Value::as_array)
            .is_some_and(|items| {
                items
                    .iter()
                    .filter(|s| self.matches(value, s, file))
                    .count()
                    != 1
            })
        {
            return false;
        }
        if schema
            .get("allOf")
            .and_then(Value::as_array)
            .is_some_and(|items| items.iter().any(|s| !self.matches(value, s, file)))
        {
            return false;
        }
        if let Some(condition) = schema.get("if")
            && self.matches(value, condition, file)
            && schema
                .get("then")
                .is_some_and(|s| !self.matches(value, s, file))
        {
            return false;
        }
        if let Some(map) = value.as_object() {
            if !size_valid(schema, map.len(), "minProperties", "maxProperties") {
                return false;
            }
            if schema
                .get("required")
                .and_then(Value::as_array)
                .is_some_and(|keys| {
                    keys.iter()
                        .any(|k| k.as_str().is_none_or(|k| !map.contains_key(k)))
                })
            {
                return false;
            }
            for (key, child) in map {
                if schema
                    .get("propertyNames")
                    .is_some_and(|s| !self.matches(&Value::String(key.clone()), s, file))
                {
                    return false;
                }
                if let Some(s) = schema.get("properties").and_then(|p| p.get(key)) {
                    if !self.matches(child, s, file) {
                        return false;
                    }
                } else if schema
                    .get("additionalProperties")
                    .is_some_and(|s| !self.matches(child, s, file))
                {
                    return false;
                }
            }
        }
        if let Some(items) = value.as_array() {
            if !size_valid(schema, items.len(), "minItems", "maxItems") {
                return false;
            }
            if schema
                .get("items")
                .is_some_and(|s| items.iter().any(|v| !self.matches(v, s, file)))
            {
                return false;
            }
            if schema.get("uniqueItems") == Some(&Value::Bool(true))
                && items
                    .iter()
                    .enumerate()
                    .any(|(i, v)| items[..i].contains(v))
            {
                return false;
            }
        }
        if let Some(text) = value.as_str() {
            if !size_valid(schema, text.chars().count(), "minLength", "maxLength") {
                return false;
            }
            if let Some(pattern) = schema.get("pattern").and_then(Value::as_str)
                && self.patterns.get(pattern).is_none_or(|p| !p.is_match(text))
            {
                return false;
            }
            if let Some(format) = schema.get("format")
                && (format != "date-time" || !valid_time(text))
            {
                return false;
            }
        }
        if let Some(number) = value.as_i64()
            && (schema
                .get("minimum")
                .and_then(Value::as_i64)
                .is_some_and(|n| number < n)
                || schema
                    .get("maximum")
                    .and_then(Value::as_i64)
                    .is_some_and(|n| number > n))
        {
            return false;
        }
        true
    }
}

fn valid_time(text: &str) -> bool {
    // chrono accepts leap seconds/year zero, whereas the pinned oracle does not.
    if text.get(..4) == Some("0000") || text.get(17..19).is_none_or(|s| s > "59") {
        return false;
    }
    chrono::DateTime::parse_from_rfc3339(&text.replace('t', "T").replace('z', "Z")).is_ok()
}
