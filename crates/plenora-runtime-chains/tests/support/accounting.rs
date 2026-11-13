//! Serializer oracle and exhaustive slot accounting; no infrastructure/provider attestation.
use super::*;
use crate::RunOptions;
use std::error::Error as StdError;
fn journal() -> Journal {
    Journal {
        version: 1,
        tenant: "tenant".into(),
        options: RunOptions {
            run_id: "00000000-0000-0000-0000-000000000011".into(),
            idempotency_key: None,
            deadline_ms: None,
        },
        created: 0,
        deadline: u64::MAX,
        revision: 1,
        plan: include_bytes!("../../../../contracts/chains-v2/fixtures/plan-valid.json").to_vec(),
        registry: include_bytes!("../../../../contracts/chains-v2/fixtures/context.json").to_vec(),
        view: vec![],
        active: None,
        cancelled: false,
        terminal: false,
        events: vec![],
        history: vec![],
        evidence: None,
        progress: BTreeMap::new(),
        retry_due: None,
        waiting_recovery: false,
        allocation: Some(Allocation {
            limit: JOURNAL_BYTES,
            events: 8,
            history: 2,
            recovery: None,
            recovery_limit: None,
        }),
    }
}
#[test]
fn every_serialized_field_is_enumerated_and_matches_the_serializer_oracle()
-> Result<(), Box<dyn StdError>> {
    let mut j = journal();
    j.tenant = "quotes\"\\\n\t\u{0} unicode \u{1f600}".into();
    j.options.idempotency_key = Some(j.tenant.clone());
    j.options.deadline_ms = Some(u64::MAX);
    j.revision = u64::MAX;
    j.view = (0..=255).collect();
    j.active = Some(Intent {
        result_limit: Some(u64::MAX),
        index: usize::MAX,
        message_id: "f".repeat(36),
        started: u64::MAX,
        deadline: u64::MAX,
        prior: j.view.clone(),
        first_started: Some(u64::MAX),
        causation: Some("c".repeat(36)),
    });
    j.events = vec![vec![], j.view.clone()];
    j.history = vec![j.view.clone()];
    j.evidence = Some(j.view.clone());
    j.progress.insert(
        "escaped\"\n".into(),
        StepProgress {
            started: u64::MAX,
            deadline: u64::MAX,
            last_message: Some("m".repeat(36)),
        },
    );
    j.retry_due = Some(u64::MAX);
    j.waiting_recovery = true;
    j.cancelled = true;
    j.terminal = true;
    let encoded = serde_json::to_vec(&j)?;
    let parts = fields(&j, false)?;
    let value = serde_json::to_value(&j)?;
    let mut keys: Vec<_> = value
        .as_object()
        .ok_or("object")?
        .keys()
        .map(String::as_str)
        .collect();
    let mut listed: Vec<_> = parts.iter().map(|(name, _)| *name).collect();
    keys.sort_unstable();
    listed.sort_unstable();
    assert_eq!(keys, listed);
    assert_eq!(keys.len(), 19);
    assert_eq!(j.encoded_size()?, encoded.len());
    assert_eq!(total(&parts)?, encoded.len());
    for (name, bytes) in parts {
        assert_eq!(bytes, serde_json::to_vec(&value[name])?.len(), "{name}");
    }
    j.allocation = None;
    assert_eq!(fields(&j, false)?.len(), 18);
    assert_eq!(total(&fields(&j, false)?)?, serde_json::to_vec(&j)?.len());
    Ok(())
}
#[test]
fn byte_array_expansion_and_every_slot_limit_are_enforced() -> Result<(), Box<dyn StdError>> {
    let mut j = journal();
    j.view = vec![255; DOCUMENT];
    j.evidence = Some(vec![255; DOCUMENT]);
    j.history = vec![vec![255; DOCUMENT]; 2];
    j.events = vec![vec![255; EVENT]; 8];
    j.check_allocation()?;
    assert_eq!(size(&j.view)?, 4 * DOCUMENT + 1);
    assert!(j.encoded_size()? <= j.reserved_bytes()?);
    let limit = j.reserved_bytes()?;
    j.allocation.as_mut().ok_or("allocation")?.limit = limit;
    // Reducing the decimal width of the quota cannot increase the reservation.
    j.check_allocation()?;
    let exact = j.reserved_bytes()?;
    j.allocation.as_mut().ok_or("allocation")?.limit = exact - 1;
    assert_eq!(j.check_allocation(), Err(Error::ResourceLimit));
    j.allocation.as_mut().ok_or("allocation")?.limit = JOURNAL_BYTES;
    for kind in [
        "view",
        "evidence",
        "history_bytes",
        "history_count",
        "event_bytes",
        "event_count",
        "prior",
    ] {
        let mut bad = j.clone();
        match kind {
            "view" => bad.view.push(255),
            "evidence" => bad.evidence.as_mut().ok_or("evidence")?.push(255),
            "history_bytes" => bad.history[0].push(255),
            "history_count" => bad.history.push(vec![]),
            "event_bytes" => bad.events[0].push(255),
            "event_count" => bad.events.push(vec![]),
            _ => {
                bad.active = Some(Intent {
                    result_limit: None,
                    index: 0,
                    message_id: "x".repeat(36),
                    started: 1,
                    deadline: 2,
                    prior: vec![0; DOCUMENT + 1],
                    first_started: None,
                    causation: None,
                });
            }
        }
        assert_eq!(bad.check_allocation(), Err(Error::ResourceLimit), "{kind}");
    }
    assert_eq!(blob(usize::MAX), Err(Error::ResourceLimit));
    assert_eq!(array(usize::MAX, 1), Err(Error::ResourceLimit));
    Ok(())
}

#[test]
fn revision_tail_respects_the_contract_integer_ceiling_before_reserving()
-> Result<(), Box<dyn StdError>> {
    let mut j = journal();
    j.revision = 9_007_199_254_740_991 - 5;
    let before = serde_json::to_vec(&j)?;
    assert_eq!(j.reserve_effect(), Err(Error::ResourceLimit));
    assert_eq!(serde_json::to_vec(&j)?, before);
    j.revision -= 1;
    j.reserve_effect()?;
    Ok(())
}
