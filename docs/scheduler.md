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
The application-owned `ScheduleDispatcher` must use this identity as its idempotency key. Dispatch
has a deadline and explicit effect certainty:

- `NotStarted` keeps the same occurrence due for a later tick;
- `OutcomeUnknown` blocks the schedule in `ReconciliationRequired` until the owner confirms the
  effect or confirms that retry is safe;
- confirmation advances the cursor.

Misfire policies are `Skip`, `FireOnce`, and bounded `CatchUp`. Global dispatches per tick,
per-schedule catch-up, schedule count, polling cadence, and dispatch time are all finite. No in-memory
occurrence queue is created: overdue work remains represented by one cursor per schedule.

Automatic dispatch can be paused and resumed without changing `next_due_at`; `Paused` is part of the
durable snapshot. A manual trigger is allowed for active, paused, or completed definitions, never
advances the recurring cursor, and uses a non-waiting semaphore bounded by
`max_dispatches_per_tick`. Saturation is returned immediately. A timeout or unknown external effect
is returned to the caller for application-owned reconciliation and is never retried implicitly.

The scheduler exposes snapshots containing status and `next_due_at`. Plenora must persist schedule
definitions and those cursors through `database-tools`. `restore_from` preserves both the next due
instant and an unresolved unknown outcome, preventing a restart from converting reconciliation into
a blind retry. Until that adapter exists, the in-memory scheduler is functional but restart
durability is not claimed.

Application composition should run the scheduler as a critical supervised runtime task. Shutdown
stops new ticks; an in-progress dispatch remains bounded by its configured timeout.
