#![no_main]
use std::sync::Arc;
use libfuzzer_sys::fuzz_target;
use plenora_runtime_boundary::{AdmissionPolicy, CapabilityDispatcher, CapabilityDispatcherConfig, CapabilityRegistryBuilder, RuntimeBoundary};
use plenora_runtime_capabilities::{CapabilityRequest, PlenoraErrorCategory};
use plenora_runtime_messaging::SerializedMessage;
use plenora_runtime_testkit::{FakeBroker, ManualClock};
struct Policy;
impl AdmissionPolicy for Policy {
    fn idempotency_key_limit(&self, _: &str, _: &str, _: &str) -> Option<usize> { None }
    fn validate_payload(&self, _: &CapabilityRequest) -> Result<(), PlenoraErrorCategory> { Ok(()) }
}
fuzz_target!(|data: &[u8]| {
    let broker = FakeBroker::default();
    let dispatcher = CapabilityDispatcher::new(CapabilityRegistryBuilder::default().build(), CapabilityDispatcherConfig::default()).expect("fixed bounds");
    let boundary = RuntimeBoundary::new(dispatcher, Arc::new(ManualClock::default()), Arc::new(Policy), Arc::new(broker.producer()));
    let rejection = boundary.admit_raw(data, SerializedMessage::new("application/octet-stream", Vec::new())).expect_err("empty discovery cannot admit");
    assert_eq!(rejection.to_message().expect("bounded reflection"), rejection.to_message().expect("stable identity"));
});
