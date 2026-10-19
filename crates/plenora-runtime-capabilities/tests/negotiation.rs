//! Public host policy checks; this fake is not evidence of durable storage.
use std::{collections::BTreeMap, error::Error, fs, path::Path, sync::Mutex};

use plenora_runtime_capabilities::{
    AcceptanceRecord, AcceptanceStore, CapabilityDiscovery, CapabilitySideEffect,
    CapabilitySurface, NegotiationContext, NegotiationError, RuntimeBinding,
    accept_runtime_request, select_runtime_destination,
};
use plenora_runtime_messaging::MessageId;
use serde_json::{Value, json};

#[derive(Default)]
struct Store {
    records: Mutex<BTreeMap<String, NegotiationContext>>,
    unreadable: bool,
}

impl AcceptanceStore for Store {
    fn accept(
        &self,
        id: &MessageId,
        proposed: Option<NegotiationContext>,
    ) -> Result<AcceptanceRecord, NegotiationError> {
        if self.unreadable {
            return Err(NegotiationError);
        }
        let mut records = self.records.lock().map_err(|_| NegotiationError)?;
        if let Some(context) = records.get(&id.to_string()) {
            return Ok(AcceptanceRecord::Existing(*context));
        }
        records.insert(id.to_string(), proposed.ok_or(NegotiationError)?);
        Ok(AcceptanceRecord::Created)
    }
}

fn context(value: &Value) -> Result<Option<NegotiationContext>, Box<dyn Error>> {
    if value.is_null() {
        return Ok(None);
    }
    Ok(NegotiationContext::from_headers(
        value["requires"].as_str(),
        Some(
            if value["accepts_downgrade"]
                .as_bool()
                .ok_or("missing boolean")?
            {
                "true"
            } else {
                "false"
            },
        ),
    )?)
}

fn discovery(interface: &Value) -> Result<CapabilityDiscovery, Box<dyn Error>> {
    let mut document: Value = serde_json::from_str(include_str!(
        "../../../contracts/capabilities-v2/rest-tools-v1.json"
    ))?;
    let mut interface = interface.clone();
    interface["artifact"] = json!("plenora.rest-tools");
    document["interfaces"] = json!([interface]);
    Ok(CapabilityDiscovery::from_json(&serde_json::to_vec(
        &document,
    )?)?)
}

#[test]
fn all_pinned_negotiation_sequences_use_public_host_policy() -> Result<(), Box<dyn Error>> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../contracts/runtime-negotiation-v1");
    let mut count = 0;
    for entry in fs::read_dir(dir)? {
        let fixture: Value = serde_json::from_slice(&fs::read(entry?.path())?)?;
        let mut store = Store::default();
        let id: MessageId = "018f3d84-7b2c-7f00-8000-000000000001".parse()?;
        let minimum = if fixture["ingress"]["minimum"] == "plenora-runtime-binding-v2" {
            RuntimeBinding::V2
        } else {
            RuntimeBinding::V1
        };
        let hops = fixture["hops"].as_array().ok_or("missing hops")?;
        let mut refused_at = None;
        let mut selected = None;
        for (index, hop) in hops.iter().enumerate() {
            store.unreadable = hop["record"] == "unreadable";
            let accepted = accept_runtime_request(
                &store,
                &id,
                context(&hop["receives"])?,
                hop["from"] == "sender",
                minimum,
                hop["carries"].as_bool().ok_or("missing carries")?,
            );
            let Ok(accepted) = accepted else {
                refused_at = Some(index);
                break;
            };
            // A mutated forwarding is detected at its receiving hop, as in the
            // contract sequences. The transport is responsible for carrying it.
            if index + 1 == hops.len() {
                let interface = &fixture["destination"]["interface"];
                let document = if interface.is_null() {
                    None
                } else {
                    Some(discovery(interface)?)
                };
                match select_runtime_destination(accepted, document.as_ref()) {
                    Ok(binding) => selected = Some(binding),
                    Err(_) => refused_at = Some(index),
                }
            }
        }
        if fixture["expect"]["decision"] == "refuse" {
            assert_eq!(
                refused_at,
                fixture["expect"]["at_hop"]
                    .as_u64()
                    .map(usize::try_from)
                    .transpose()?
            );
            let error = serde_json::to_value(NegotiationError.public_error()?)?;
            for axis in ["category", "phase", "remote_effect"] {
                assert_eq!(error[axis], fixture["expect"][axis]);
            }
            assert_eq!(error["retry"]["kind"], fixture["expect"]["retry"]);
        } else {
            assert_eq!(refused_at, None);
            let expected = if fixture["expect"]["governed_by"] == "plenora-runtime-binding-v2" {
                RuntimeBinding::V2
            } else {
                RuntimeBinding::V1
            };
            assert_eq!(selected, Some(expected));
        }
        count += 1;
    }
    assert_eq!(count, 21);
    Ok(())
}

#[test]
fn headers_are_exact_and_diagnostics_have_no_input() -> Result<(), Box<dyn Error>> {
    for (requires, downgrade) in [
        (Some("secret"), Some("false")),
        (None, Some("true")),
        (Some("plenora-runtime-binding-v2"), None),
        (Some("plenora-runtime-binding-v2"), Some("True")),
        (Some(" plenora-runtime-binding-v2"), Some("false")),
    ] {
        let error = NegotiationContext::from_headers(requires, downgrade)
            .err()
            .ok_or("accepted invalid headers")?;
        assert!(!format!("{error:?} {error}").contains("secret"));
    }
    for requires in [RuntimeBinding::V1, RuntimeBinding::V2] {
        for accepts_downgrade in [false, true] {
            let context = NegotiationContext {
                requires,
                accepts_downgrade,
            };
            let (a, b) = context.headers();
            assert_eq!(
                NegotiationContext::from_headers(Some(a), Some(b))?,
                Some(context)
            );
        }
    }
    Ok(())
}

#[test]
fn minimum_only_applies_to_new_ingress_and_failed_store_never_defaults()
-> Result<(), Box<dyn Error>> {
    let mut store = Store::default();
    let id: MessageId = "018f3d84-7b2c-7f00-8000-000000000001".parse()?;
    let v1 = accept_runtime_request(&store, &id, None, true, RuntimeBinding::V1, true)?;
    assert_eq!(
        accept_runtime_request(&store, &id, Some(v1), true, RuntimeBinding::V2, true)?,
        v1
    );
    store.unreadable = true;
    assert!(accept_runtime_request(&store, &id, Some(v1), true, RuntimeBinding::V1, true).is_err());
    assert!(accept_runtime_request(&store, &id, None, true, RuntimeBinding::V1, true).is_err());
    Ok(())
}

#[test]
fn concurrent_receptions_fix_one_record_and_resends_cannot_default() -> Result<(), Box<dyn Error>> {
    let store = Store::default();
    let id: MessageId = "018f3d84-7b2c-7f00-8000-000000000001".parse()?;
    let context = NegotiationContext {
        requires: RuntimeBinding::V2,
        accepts_downgrade: false,
    };
    let outcomes = std::thread::scope(|scope| {
        let a = scope.spawn(|| store.accept(&id, Some(context)));
        let b = scope.spawn(|| store.accept(&id, Some(context)));
        (a.join(), b.join())
    });
    let a = outcomes.0.map_err(|_| "thread failed")??;
    let b = outcomes.1.map_err(|_| "thread failed")??;
    assert!(matches!(
        (a, b),
        (AcceptanceRecord::Created, AcceptanceRecord::Existing(_))
            | (AcceptanceRecord::Existing(_), AcceptanceRecord::Created)
    ));
    assert!(accept_runtime_request(&store, &id, None, true, RuntimeBinding::V1, true).is_err());
    assert_eq!(
        accept_runtime_request(&store, &id, Some(context), true, RuntimeBinding::V2, true)?,
        context
    );
    Ok(())
}

#[test]
fn version_pairs_keep_wire_one_and_surface_effects_are_exact() -> Result<(), Box<dyn Error>> {
    for (contract, version, valid) in [
        ("plenora-runtime-binding-v1", 1, true),
        ("plenora-runtime-binding-v2", 2, true),
        ("plenora-runtime-binding-v1", 2, false),
        ("plenora-runtime-binding-v2", 1, false),
    ] {
        let document = discovery(&json!({"kind":"runtime","contract":contract,"version":version}))?;
        let resolved = document.runtime_capability();
        assert_eq!(resolved.is_ok(), valid);
        if let Ok(id) = resolved {
            assert_eq!(id.version(), 1);
        }
    }
    let base: Value = serde_json::from_str(include_str!(
        "../../../contracts/capabilities-v2/rest-tools-v1.json"
    ))?;
    for effect in [json!({"runtime":"local"}), json!({"runtime":"remote"})] {
        let mut value = base.clone();
        value["operations"][0]["side_effect"] = json!("none");
        value["operations"][0]["attributes"]["plenora.surface_side_effects"] = effect.clone();
        let doc = CapabilityDiscovery::from_json(&serde_json::to_vec(&value)?)?;
        let operation = doc
            .operations()
            .find(|op| op.name().as_str() == value["operations"][0]["id"].as_str().unwrap_or(""))
            .ok_or("operation missing")?;
        assert_eq!(
            operation.surface_side_effect(CapabilitySurface::Runtime),
            Some(if effect["runtime"] == "local" {
                CapabilitySideEffect::Local
            } else {
                CapabilitySideEffect::Remote
            })
        );
    }
    for effect in [
        json!(null),
        json!({"runtime":"none"}),
        json!({"runtime":true}),
        json!({"secret":"remote"}),
    ] {
        let mut value = base.clone();
        value["operations"][0]["attributes"]["plenora.surface_side_effects"] = effect;
        assert!(CapabilityDiscovery::from_json(&serde_json::to_vec(&value)?).is_err());
    }
    Ok(())
}
