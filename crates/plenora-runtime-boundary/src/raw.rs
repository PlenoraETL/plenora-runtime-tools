//! Raw JSON metadata parsing. Duplicate members never reach a map that overwrites them.
use serde::{
    Deserialize, Deserializer,
    de::{MapAccess, Visitor},
};
use serde_json::{Map, Value};
use std::fmt;

pub(crate) struct Metadata(pub Map<String, Value>);
impl<'de> Deserialize<'de> for Metadata {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Exact;
        impl<'de> Visitor<'de> for Exact {
            type Value = Metadata;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("unique metadata members")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Metadata, A::Error> {
                let mut result = Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if result.contains_key(&key)
                        || result.len() >= plenora_runtime_messaging::MAX_METADATA_ENTRIES
                    {
                        return Err(serde::de::Error::custom("invalid metadata members"));
                    }
                    result.insert(key, map.next_value()?);
                }
                Ok(Metadata(result))
            }
        }
        deserializer.deserialize_map(Exact)
    }
}
