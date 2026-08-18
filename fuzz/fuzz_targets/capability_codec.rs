#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use plenora_runtime_capabilities::{
    CAPABILITY_NAME_METADATA_KEY, CAPABILITY_OPERATION_METADATA_KEY,
    CAPABILITY_VERSION_METADATA_KEY, CapabilityMessageCodec,
};
use plenora_runtime_messaging::{MessageCodec, MessageMetadata, SerializedMessage};

fuzz_target!(|data: &[u8]| {
    let first =
        usize::from(data.first().copied().unwrap_or_default()).min(data.len().saturating_sub(1));
    let remaining = data.len().saturating_sub(first.saturating_add(1));
    let second = usize::from(data.get(1).copied().unwrap_or_default()).min(remaining);
    let name_start = 2_usize.min(data.len());
    let name_end = name_start.saturating_add(first).min(data.len());
    let version_end = name_end.saturating_add(second).min(data.len());

    let mut headers = MessageMetadata::new();
    if headers
        .insert(
            CAPABILITY_NAME_METADATA_KEY,
            Bytes::copy_from_slice(&data[name_start..name_end]),
        )
        .is_err()
    {
        return;
    }
    if headers
        .insert(
            CAPABILITY_VERSION_METADATA_KEY,
            Bytes::copy_from_slice(&data[name_end..version_end]),
        )
        .is_err()
    {
        return;
    }
    if headers
        .insert(
            CAPABILITY_OPERATION_METADATA_KEY,
            Bytes::copy_from_slice(&data[version_end..]),
        )
        .is_err()
    {
        return;
    }

    let message = SerializedMessage::new("application/octet-stream", Bytes::copy_from_slice(data))
        .with_headers(headers);
    let codec = CapabilityMessageCodec;
    if let Ok(request) = codec.decode(&message) {
        assert!(request.capability().version() > 0);
        assert!(!request.capability().name().is_empty());
        assert!(!request.operation().as_str().is_empty());
        assert!(
            !request
                .input()
                .headers
                .contains_key(CAPABILITY_NAME_METADATA_KEY)
        );
        assert!(
            !request
                .input()
                .headers
                .contains_key(CAPABILITY_VERSION_METADATA_KEY)
        );
        assert!(
            !request
                .input()
                .headers
                .contains_key(CAPABILITY_OPERATION_METADATA_KEY)
        );

        let encoded = codec
            .encode(&request)
            .expect("validated request must encode");
        let round_trip = codec
            .decode(&encoded)
            .expect("canonical encoded request must decode");
        assert_eq!(round_trip.capability(), request.capability());
        assert_eq!(round_trip.operation(), request.operation());
        assert_eq!(round_trip.input(), request.input());
    }
});
