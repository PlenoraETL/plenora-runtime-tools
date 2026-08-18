#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use plenora_runtime_messaging::{
    MAX_METADATA_ENTRIES, MAX_METADATA_KEY_BYTES, MAX_METADATA_TOTAL_BYTES,
    MAX_METADATA_VALUE_BYTES, MessageMetadata,
};

fuzz_target!(|data: &[u8]| {
    let split = data.first().map_or(0, |value| {
        usize::from(*value).min(data.len().saturating_sub(1))
    });
    let key_bytes = data.get(1..=split).unwrap_or_default();
    let value = data.get(split.saturating_add(1)..).unwrap_or_default();
    let Ok(key) = std::str::from_utf8(key_bytes) else {
        return;
    };

    let mut metadata = MessageMetadata::new();
    let original = metadata.clone();
    let result = metadata.insert(key.to_owned(), Bytes::copy_from_slice(value));

    match result {
        Ok(previous) => {
            assert!(previous.is_none());
            assert!(key.len() <= MAX_METADATA_KEY_BYTES);
            assert!(value.len() <= MAX_METADATA_VALUE_BYTES);
            assert!(metadata.len() <= MAX_METADATA_ENTRIES);
            let total = metadata
                .iter()
                .map(|(stored_key, stored_value)| stored_key.len() + stored_value.len())
                .sum::<usize>();
            assert!(total <= MAX_METADATA_TOTAL_BYTES);
            assert_eq!(metadata.get(key).map(Bytes::as_ref), Some(value));

            let removed = metadata.remove(key);
            assert_eq!(removed.as_deref(), Some(value));
            assert!(metadata.is_empty());
        }
        Err(_) => assert_eq!(metadata, original),
    }
});
