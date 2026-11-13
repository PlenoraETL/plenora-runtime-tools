//! One reservation shared by every sequential step, including retained handoffs.
use crate::{
    Error,
    contracts::{Budget, Counters},
};

/// Effective per-attempt limits. Every value is bounded by the remaining global budget.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reservation {
    /// Governed memory, including retained artifacts and adapter-owned copies.
    pub limits: Budget,
    /// Absolute deadline; restart cannot renew it.
    pub deadline_ms: u64,
}
pub(crate) fn amounts(c: &Counters) -> [u64; 9] {
    [
        c.memory_bytes,
        c.elapsed_ms,
        c.rows_read,
        c.rows_produced,
        c.artifact_bytes,
        c.spool_bytes,
        c.request_bytes,
        c.result_bytes,
        c.max_attempts,
    ]
}
fn limits(b: &Budget) -> [u64; 9] {
    [
        b.memory_bytes,
        b.elapsed_ms,
        b.rows_read,
        b.rows_produced,
        b.artifact_bytes,
        b.spool_bytes,
        b.request_bytes,
        b.result_bytes,
        b.max_attempts,
    ]
}
pub(crate) fn zero() -> Counters {
    Counters {
        memory_bytes: 0,
        elapsed_ms: 0,
        rows_read: 0,
        rows_produced: 0,
        artifact_bytes: 0,
        spool_bytes: 0,
        request_bytes: 0,
        result_bytes: 0,
        max_attempts: 0,
    }
}
pub(crate) fn add(a: &Counters, b: &Counters) -> Result<Counters, Error> {
    let mut values = [0; 9];
    for (i, (x, y)) in amounts(a).into_iter().zip(amounts(b)).enumerate() {
        values[i] = if matches!(i, 0 | 5) {
            x.max(y)
        } else {
            x.checked_add(y).ok_or(Error::ResourceLimit)?
        };
    }
    Ok(Counters {
        memory_bytes: values[0],
        elapsed_ms: values[1],
        rows_read: values[2],
        rows_produced: values[3],
        artifact_bytes: values[4],
        spool_bytes: values[5],
        request_bytes: values[6],
        result_bytes: values[7],
        max_attempts: values[8],
    })
}
pub(crate) fn within(c: &Counters, b: &Budget) -> bool {
    amounts(c)
        .into_iter()
        .zip(limits(b))
        .all(|(v, max)| v <= max)
}
impl Reservation {
    pub(crate) fn new(
        global: &Budget,
        step: &Budget,
        used: &Counters,
        retained: u64,
        now: u64,
        deadline: u64,
    ) -> Result<Self, Error> {
        if now >= deadline {
            return Err(Error::Timeout);
        }
        let mut remaining = [0; 9];
        for (i, ((g, s), u)) in limits(global)
            .into_iter()
            .zip(limits(step))
            .zip(amounts(used))
            .enumerate()
        {
            let spent = match i {
                0 | 5 => retained,
                1 => 0,
                _ => u,
            };
            remaining[i] = s.min(g.checked_sub(spent).ok_or(Error::ResourceLimit)?);
        }
        if remaining[0] == 0 || remaining[8] == 0 {
            return Err(Error::ResourceLimit);
        }
        let deadline_ms = deadline.min(
            now.checked_add(step.elapsed_ms)
                .ok_or(Error::ResourceLimit)?,
        );
        remaining[1] = deadline_ms - now;
        Ok(Self {
            limits: Budget {
                memory_bytes: remaining[0],
                elapsed_ms: remaining[1],
                rows_read: remaining[2],
                rows_produced: remaining[3],
                artifact_bytes: remaining[4],
                spool_bytes: remaining[5],
                request_bytes: remaining[6],
                result_bytes: remaining[7],
                max_attempts: remaining[8],
            },
            deadline_ms,
        })
    }
}
