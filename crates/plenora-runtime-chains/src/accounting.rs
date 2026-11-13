//! Exact JSON accounting and finite preallocated slots for the authoritative run journal.
use crate::{
    Error, JOURNAL_BYTES, Journal,
    contracts::parse_plan,
    journal::{Intent, StepProgress},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) const DOCUMENT: usize = 1024 * 1024;
pub(crate) const EVENT: usize = 4096;
const EVENTS: usize = 16384;
const REPORTS: usize = 1024;
// Leaves half the control output envelope for response and runtime metadata.
pub(crate) const REPORT_WIRE: usize = DOCUMENT / 2;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Allocation {
    pub limit: usize,
    pub events: usize,
    pub history: usize,
    pub recovery: Option<String>,
    pub recovery_limit: Option<u64>,
}
impl Allocation {
    pub fn new(limit: usize) -> Result<Self, Error> {
        if limit == 0 || limit > JOURNAL_BYTES {
            return Err(Error::ResourceLimit);
        }
        Ok(Self {
            limit,
            events: 3,
            history: 0,
            recovery: None,
            recovery_limit: None,
        })
    }
}
#[derive(Default)]
struct Counter(usize);
impl std::io::Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|n| *n <= JOURNAL_BYTES)
            .ok_or_else(|| std::io::Error::other("journal limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub(crate) fn size(value: &impl Serialize) -> Result<usize, Error> {
    let mut out = Counter::default();
    serde_json::to_writer(&mut out, value).map_err(|_| Error::ResourceLimit)?;
    Ok(out.0)
}
fn blob(bytes: usize) -> Result<usize, Error> {
    bytes
        .checked_mul(4)
        .and_then(|n| n.checked_add(2))
        .ok_or(Error::ResourceLimit)
}
fn array(count: usize, element: usize) -> Result<usize, Error> {
    count
        .checked_mul(element.checked_add(1).ok_or(Error::ResourceLimit)?)
        .and_then(|n| n.checked_add(2))
        .ok_or(Error::ResourceLimit)
}
/// Each declared top-level field has one accounting entry. Destructuring without
/// a rest pattern makes adding journal state without accounting a compile error.
fn fields(j: &Journal, reserved: bool) -> Result<Vec<(&'static str, usize)>, Error> {
    let Journal {
        version,
        tenant,
        options,
        created,
        deadline,
        revision,
        plan,
        registry,
        view,
        active,
        cancelled,
        terminal,
        events,
        history,
        evidence,
        progress,
        retry_due,
        waiting_recovery,
        allocation,
    } = j;
    let mut result = Vec::new();
    macro_rules! actual { ($($key:ident),+ $(,)?) => { $(result.push((stringify!($key), size($key)?));)+ }; }
    actual!(version, tenant, options, created, deadline, plan, registry);
    if !reserved {
        actual!(
            revision,
            view,
            active,
            cancelled,
            terminal,
            events,
            history,
            evidence,
            progress,
            retry_due,
            waiting_recovery
        );
        if allocation.is_some() {
            actual!(allocation);
        }
        return Ok(result);
    }
    let a = allocation.as_ref().ok_or(Error::Invalid)?;
    let maximum = u64::MAX;
    let id = "ffffffff-ffff-ffff-ffff-ffffffffffff".to_owned();
    let intent = Intent {
        result_limit: Some(u64::MAX),
        index: usize::MAX,
        message_id: id.clone(),
        started: maximum,
        deadline: maximum,
        prior: vec![],
        first_started: Some(maximum),
        causation: Some(id.clone()),
    };
    let all_progress: BTreeMap<_, _> = parse_plan(plan)?
        .steps
        .into_iter()
        .map(|s| {
            (
                s.id,
                StepProgress {
                    started: maximum,
                    deadline: maximum,
                    last_message: Some(id.clone()),
                },
            )
        })
        .collect();
    let allocated = Allocation {
        limit: a.limit,
        events: EVENTS,
        history: REPORTS,
        recovery: Some(id),
        recovery_limit: Some(u64::MAX),
    };
    result.extend([
        ("revision", size(&maximum)?),
        ("view", blob(DOCUMENT)?),
        (
            "active",
            size(&intent)?
                .checked_add(blob(DOCUMENT)?)
                .ok_or(Error::ResourceLimit)?,
        ),
        ("cancelled", 5),
        ("terminal", 5),
        ("events", array(a.events, blob(EVENT)?)?),
        ("history", array(a.history, blob(DOCUMENT)?)?),
        ("evidence", blob(DOCUMENT)?),
        ("progress", size(&all_progress)?),
        ("retry_due", size(&maximum)?),
        ("waiting_recovery", 5),
        ("allocation", size(&allocated)?),
    ]);
    Ok(result)
}
fn total(fields: &[(&str, usize)]) -> Result<usize, Error> {
    let mut sum = 2_usize; // object braces
    for (index, (name, value)) in fields.iter().enumerate() {
        sum = sum
            .checked_add(size(name)?)
            .and_then(|n| n.checked_add(1))
            .and_then(|n| n.checked_add(*value))
            .and_then(|n| n.checked_add(usize::from(index != 0)))
            .ok_or(Error::ResourceLimit)?;
    }
    Ok(sum)
}
impl Journal {
    /// Actual serialized snapshot bytes, including all JSON array and escape expansion.
    /// # Errors
    /// Fails on overflow or a snapshot beyond the finite host ceiling.
    pub fn encoded_size(&self) -> Result<usize, Error> {
        size(self)
    }
    /// Reserved peak bytes for all preallocated journal slots; legacy state has no reservation.
    /// # Errors
    /// Rejects missing allocation or arithmetic overflow.
    pub fn reserved_bytes(&self) -> Result<usize, Error> {
        total(&fields(self, true)?)
    }
    pub(crate) fn check_allocation(&self) -> Result<(), Error> {
        let actual = fields(self, false)?;
        let measured = total(&actual)?;
        if measured > JOURNAL_BYTES {
            return Err(Error::ResourceLimit);
        }
        let Some(a) = &self.allocation else {
            return Ok(());
        };
        if a.limit == 0
            || a.limit > JOURNAL_BYTES
            || a.events > EVENTS
            || a.history > REPORTS
            || self.events.len() > a.events
            || self.history.len() > a.history
            || a.recovery.is_some() != a.recovery_limit.is_some()
            || a.recovery_limit == Some(0)
            || a.recovery.as_ref().is_some_and(|id| !crate::uuid(id))
            || self.events.iter().any(|b| b.len() > EVENT)
            || self
                .history
                .iter()
                .chain(self.evidence.iter())
                .any(|b| b.len() > DOCUMENT)
            || self.view.len() > DOCUMENT
            || self
                .active
                .as_ref()
                .is_some_and(|i| i.prior.len() > DOCUMENT)
        {
            return Err(Error::ResourceLimit);
        }
        let reserved = fields(self, true)?;
        for (key, bytes) in &actual {
            if reserved
                .iter()
                .find(|(k, _)| k == key)
                .is_none_or(|(_, cap)| bytes > cap)
            {
                return Err(Error::ResourceLimit);
            }
        }
        if total(&reserved)? > a.limit || measured > a.limit {
            return Err(Error::ResourceLimit);
        }
        Ok(())
    }
    pub(crate) fn report_limit(&self, step: &str) -> Result<u64, Error> {
        let mut used = size(&self.history)?;
        if let Some(raw) = &self.evidence {
            used = used
                .checked_add(size(raw)?)
                .and_then(|n| n.checked_add(1))
                .ok_or(Error::ResourceLimit)?;
        }
        let available = REPORT_WIRE
            .checked_sub(used)
            .and_then(|n| n.checked_sub(4))
            .ok_or(Error::ResourceLimit)?
            / 4;
        if available == 0 {
            return Err(Error::ResourceLimit);
        }
        let plan = parse_plan(&self.plan)?;
        let local = plan
            .steps
            .iter()
            .find(|s| s.id == step)
            .ok_or(Error::Invalid)?
            .budgets
            .result_bytes;
        let mut global_used = 0_u64;
        let mut local_used = 0_u64;
        // Ownership comes from the coordinator's finalizing event, never from
        // an untrusted step_id inside the adapter's raw evidence.
        let owners = self
            .events
            .iter()
            .map(|bytes| crate::contracts::parse_event(bytes).map_err(Error::from))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|event| event.kind == "step_state" && event.state == "finalizing")
            .map(|event| event.step_id.ok_or(Error::Invalid))
            .collect::<Result<Vec<_>, _>>()?;
        if owners.len() != self.history.len() + usize::from(self.evidence.is_some()) {
            return Err(Error::Invalid);
        }
        for (raw, owner) in self.history.iter().chain(self.evidence.iter()).zip(owners) {
            let bytes = u64::try_from(raw.len()).map_err(|_| Error::ResourceLimit)?;
            global_used = global_used.checked_add(bytes).ok_or(Error::ResourceLimit)?;
            if owner == step {
                local_used = local_used.checked_add(bytes).ok_or(Error::ResourceLimit)?;
            }
        }
        let limit = u64::try_from(available)
            .map_err(|_| Error::ResourceLimit)?
            .min(
                plan.budgets
                    .result_bytes
                    .checked_sub(global_used)
                    .ok_or(Error::ResourceLimit)?,
            )
            .min(local.checked_sub(local_used).ok_or(Error::ResourceLimit)?);
        if limit == 0 {
            return Err(Error::ResourceLimit);
        }
        Ok(limit)
    }
    /// Allocate one raw receipt (including its previous history slot), six events
    /// for intent/reservation, raw, classification, recovery flag, cancel and tail.
    /// Fixed view/intent/evidence slots remain protected even at history exhaustion.
    pub(crate) fn reserve_effect(&mut self) -> Result<(), Error> {
        self.revision
            .checked_add(6)
            .filter(|n| *n <= 9_007_199_254_740_991)
            .ok_or(Error::ResourceLimit)?;
        let mut a = self
            .allocation
            .clone()
            .unwrap_or(Allocation::new(JOURNAL_BYTES)?);
        a.events = a.events.max(
            self.events
                .len()
                .checked_add(6)
                .ok_or(Error::ResourceLimit)?,
        );
        a.history = a.history.max(
            self.history
                .len()
                .checked_add(1)
                .ok_or(Error::ResourceLimit)?,
        );
        let old = self.allocation.replace(a);
        if let Err(error) = self.check_allocation() {
            self.allocation = old;
            return Err(error);
        }
        Ok(())
    }
}

// Bound the aggregate result as well as its raw receipt. Numeric replacements
// are measured at their maximum width, and timestamp replacements use the
// longest timestamp admitted by the pinned four-digit-year contract.
pub(crate) fn result_limit(
    plan: &crate::contracts::Plan,
    view: &crate::contracts::ChainResult,
    step: &crate::contracts::Step,
) -> Result<u64, Error> {
    let maximum = u64::MAX;
    let counters = crate::contracts::Counters {
        memory_bytes: maximum,
        elapsed_ms: maximum,
        rows_read: maximum,
        rows_produced: maximum,
        artifact_bytes: maximum,
        spool_bytes: maximum,
        request_bytes: maximum,
        result_bytes: maximum,
        max_attempts: maximum,
    };
    let timestamp = "9999-12-31T23:59:59.999999999+00:00";
    let metadata = serde_json::json!({
        "counters": counters, "state_revision": maximum, "finished_at": timestamp,
        "state": "requires_recovery", "remote_effect": "rolled_back",
        "retry": {"kind":"requires_recovery"}, "reports_complete": false, "losses":"unavailable",
        "recovery": {"required":false, "evidence_ref":null,
            "resumable_steps":plan.steps.iter().map(|s| &s.id).collect::<Vec<_>>()}
    });
    let step_metadata = serde_json::json!({"counters":counters, "attempt":maximum,
        "started_at":timestamp,"finished_at":timestamp,"state":"rejected","reason":"upstream_error",
        "retry":{"kind":"never"},"stop_confirmed":false});
    let timeout = crate::engine::problem(Error::Timeout, "rolled_back", Some(&step.id));
    let control = size(&metadata)?
        .checked_add(size(&step_metadata)?)
        .and_then(|n| n.checked_add(size(&timeout).ok()?.checked_mul(2)?))
        .ok_or(Error::ResourceLimit)?;
    // One copy for the step, one for its primary error, one per output alias.
    // Plan bytes cover all alias keys and IDs. A second control envelope remains
    // available for rejecting the next attempt without effects.
    let overhead = size(view)?
        .checked_add(
            crate::contracts::canonical_plan(plan)?
                .len()
                .checked_mul(2)
                .ok_or(Error::ResourceLimit)?,
        )
        .and_then(|n| n.checked_add(control.checked_mul(2)?))
        .ok_or(Error::ResourceLimit)?;
    let copies = plan
        .outputs
        .len()
        .checked_add(2)
        .ok_or(Error::ResourceLimit)?;
    let available = DOCUMENT.checked_sub(overhead).ok_or(Error::ResourceLimit)? / copies;
    if available == 0 {
        return Err(Error::ResourceLimit);
    }
    u64::try_from(available).map_err(|_| Error::ResourceLimit)
}

#[cfg(test)]
#[path = "../tests/support/accounting.rs"]
mod tests;
