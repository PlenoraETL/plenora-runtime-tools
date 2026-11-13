//! Bounded exact JSON decoding and canonical bytes, independent of schemas.
use crate::error::{Checked, Code, ensure};
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::fmt;

pub(crate) const MAX_BYTES: usize = 1_048_576;
const MAX_DEPTH: usize = 64;
const MAX_INTEGER: u64 = 9_007_199_254_740_991;

struct Exact<'a> {
    depth: usize,
    failure: &'a mut Code,
}
impl<'de> DeserializeSeed<'de> for Exact<'_> {
    type Value = Value;
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        if self.depth > MAX_DEPTH {
            *self.failure = Code::Limit;
            return Err(serde::de::Error::custom("CHAIN_LIMIT"));
        }
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Exact<'_> {
    type Value = Value;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("canonical JSON")
    }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Value, E> {
        Ok(value.into())
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Value, E> {
        Ok(value.into())
    }
    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Value, E> {
        Ok(value.into())
    }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Value, E> {
        if value > MAX_INTEGER {
            return Err(E::custom("CHAIN_DOCUMENT"));
        }
        Ok(value.into())
    }
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Value, E> {
        if value.unsigned_abs() > MAX_INTEGER {
            return Err(E::custom("CHAIN_DOCUMENT"));
        }
        Ok(value.into())
    }
    // serde_json routes -0, exponents and decimal points to visit_f64. The
    // default visitor rejects them, even when their numeric value is integral.
    fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = access.next_element_seed(Exact {
            depth: self.depth + 1,
            failure: self.failure,
        })? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        let mut values = Map::new();
        while let Some(key) = access.next_key::<String>()? {
            if self.depth + 1 > MAX_DEPTH {
                *self.failure = Code::Limit;
                return Err(serde::de::Error::custom("CHAIN_LIMIT"));
            }
            if values.contains_key(&key) {
                *self.failure = Code::Duplicate;
                return Err(serde::de::Error::custom("CHAIN_DUPLICATE"));
            }
            let value = access.next_value_seed(Exact {
                depth: self.depth + 1,
                failure: self.failure,
            })?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}

pub(crate) fn parse(raw: &[u8]) -> Checked<Value> {
    ensure(raw.len() <= MAX_BYTES, Code::Limit)?;
    let mut failure = Code::Document;
    let mut decoder = serde_json::Deserializer::from_slice(raw);
    let value = Exact {
        depth: 1,
        failure: &mut failure,
    }
    .deserialize(&mut decoder)
    .map_err(|_| crate::Rejection(failure))?;
    decoder
        .end()
        .map_err(|_| crate::Rejection(Code::Document))?;
    Ok(value)
}

pub(crate) fn canonical(value: &Value) -> Checked<Vec<u8>> {
    // Reparse also enforces limits on DTOs built in memory. Sorting is explicit
    // so feature unification of serde_json/preserve_order cannot change digests.
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let mut pairs: Vec<_> = map.iter().collect();
                pairs.sort_by_key(|(key, _)| *key);
                Value::Object(
                    pairs
                        .into_iter()
                        .map(|(k, v)| (k.clone(), sorted(v)))
                        .collect(),
                )
            }
            Value::Array(items) => Value::Array(items.iter().map(sorted).collect()),
            other => other.clone(),
        }
    }
    // Validate before recursively sorting to bound recursion on constructed values.
    let raw = serde_json::to_vec(value).map_err(|_| crate::Rejection(Code::Document))?;
    parse(&raw)?;
    serde_json::to_vec(&sorted(value)).map_err(|_| Code::Document.into())
}
pub(crate) fn digest(value: &Value) -> Checked<String> {
    Ok(format!("{:x}", Sha256::digest(canonical(value)?)))
}

#[cfg(test)]
mod tests {
    use super::{canonical, digest, parse};

    #[test]
    fn canonical_unicode_escapes_and_sorted_keys() -> Result<(), crate::Rejection> {
        let input = br#"{"z":2,"a":"\u00e9\ud83d\ude00\b\t\n\f\r\u0001/\\\"","b":1}"#;
        assert_eq!(
            canonical(&parse(input)?)?,
            "{\"a\":\"é😀\\b\\t\\n\\f\\r\\u0001/\\\\\\\"\",\"b\":1,\"z\":2}".as_bytes()
        );
        assert_eq!(
            digest(&parse(br#"{"a":1}"#)?)?,
            "015abd7f5cc57a2dd94b7590f04ad8084273905ee33ec5cebeae62276a97f862"
        );
        Ok(())
    }
}
