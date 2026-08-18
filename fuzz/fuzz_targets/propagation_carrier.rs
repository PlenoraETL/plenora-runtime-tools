#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use opentelemetry::propagation::{Extractor as _, Injector as _};
use plenora_runtime_messaging::{CORRELATION_ID_METADATA_KEY, MessageMetadata};
use plenora_runtime_observability::{MessageMetadataExtractor, MessageMetadataInjector};

fuzz_target!(|data: &[u8]| {
    let selector = data.first().copied().unwrap_or_default() % 4;
    let key = match selector {
        0 => "traceparent",
        1 => "tracestate",
        2 => "baggage",
        _ => "TrAcEpArEnT",
    };
    let value = String::from_utf8_lossy(data.get(1..).unwrap_or_default()).into_owned();
    let mut metadata = MessageMetadata::new();
    let mut injector = MessageMetadataInjector::new(&mut metadata);
    injector.set(key, value);
    if injector.finish().is_ok() {
        let extractor = MessageMetadataExtractor::new(&metadata)
            .expect("successfully injected propagation values must extract");
        let _ = extractor.get("traceparent");
        let _ = extractor.get("tracestate");
        assert!(extractor.keys().len() <= 2);
    }

    let mut correlation = MessageMetadata::new();
    if correlation
        .insert(
            CORRELATION_ID_METADATA_KEY,
            Bytes::copy_from_slice(data.get(1..).unwrap_or_default()),
        )
        .is_ok()
    {
        let _ = plenora_runtime_observability::extract_correlation_id(&correlation);
    }
});
