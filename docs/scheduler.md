# Generic scheduler

`plenora-runtime-scheduler` is a bounded, backend-neutral programmed-dispatch engine. It does not
import NATS, Apalis, a database driver, or an application library.

Built-in plans cover one-shot, fixed-interval and second-aware cron execution. `CronPlan` requires an
explicit IANA timezone and uses the bundled timezone database for daylight-saving gaps and overlaps.
At an ambiguous autumn local time it emits the first matching instant once, then advances to the
next local occurrence; a nonexistent spring time is skipped. `JitteredPlan` chooses one bounded,
deterministic delay from a stable seed and removes it before calculating the next occurrence, so
jitter never accumulates cadence drift. `SchedulePlan` remains public for application-specific
calendar rules. Definitions may be registered at startup or added and removed at runtime under the
same explicit registry capacity. Scheduled payloads should contain identifiers and small commands,
never dataframes or large blobs.

Each dispatch carries a deterministic identity composed of the schedule id and logical due time.
The application-owned `ScheduleDispatcher` must use this identity as its idempotency key, and must
not be reentrant: the registry lock is held across the call, so a dispatcher that calls back into
the scheduler only unblocks after its own dispatch has timed out into an unknown outcome. Dispatch
has a deadline and explicit effect certainty:

- `NotStarted` keeps the same occurrence due for a later tick;
- `OutcomeUnknown` blocks the schedule in `ReconciliationRequired` until the owner confirms the
  effect or confirms that retry is safe;
- confirmation advances the cursor.

Misfire policies are `Skip`, `FireOnce`, and bounded `CatchUp`. Global dispatches per tick,
per-schedule catch-up, schedule count, polling cadence, and dispatch time are all finite. No in-memory
occurrence queue is created: overdue work remains represented by one cursor per schedule. A
`FireOnce` decision to abandon the remaining backlog is remembered across ticks, so a backlog larger
than the per-schedule catch-up bound is drained by bounded skipping instead of dispatching a second
overdue occurrence on a later tick.

Automatic dispatch can be paused and resumed without changing `next_due_at`; `Paused` is part of the
durable snapshot. A manual trigger is allowed for active, paused, or completed definitions, never
advances the recurring cursor, and uses a non-waiting semaphore bounded by
`max_dispatches_per_tick`. Saturation is returned immediately. A timeout or unknown external effect
is returned to the caller for application-owned reconciliation and is never retried implicitly.
Manual admission is decided atomically with reserving the bounded slot, but it is decided once: an
automatic occurrence entering reconciliation afterwards does not withdraw an already admitted
manual dispatch, and abandoning the manual future forfeits its outcome, which the caller must then
treat as an unknown effect. A manual occurrence identity is the schedule id paired with the
supplied instant and is not made unique per call, so repeating an instant, or reusing an automatic
due instant, deliberately reuses the identity and an idempotent dispatcher produces no second
effect. Callers that need distinct effects, or that reconcile by identity, must supply distinct
instants.

The scheduler exposes snapshots containing status, `next_due_at`, and `skip_backlog_through`.
Plenora must persist schedule definitions and all three cursor fields through `database-tools`, and
must write them atomically: persisting the cursor without the suppression boundary lets a restart
inside an abandoned `FireOnce` backlog dispatch a second overdue occurrence. Records written before
the boundary existed migrate by defaulting it to `None`, which restores the pre-existing behaviour
of re-evaluating the backlog from the stored cursor. The boundary is defensive by construction: it
is honoured only under `FireOnce`, a boundary without a cursor is rejected at restore, and a
boundary ahead of the current tick is discarded as corrupt instead of silencing the schedule. `restore_from` preserves the next due instant,
the suppression boundary, and an unresolved unknown outcome, preventing a restart from converting
reconciliation into a blind retry. Until that adapter exists, the in-memory scheduler is functional but restart
durability is not claimed.

Application composition should run the scheduler as a critical supervised runtime task. Shutdown
stops new ticks and, inside a tick already in progress, starts no further occurrence once it
observes the signal, so drain is bounded by the dispatch that had already begun and its configured
timeout. Admission is a check before each dispatch rather than an atomic handshake: an occurrence
whose dispatch started in the same instant the signal arrived still runs to that timeout.
