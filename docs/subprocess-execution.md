# Generic subprocess execution

Subprocess isolation is a containment mode for selected heavy or native-backed operations. When a
child exits or is terminated, the operating system reclaims its address space, including allocator
arenas and memory retained by native libraries.

This is useful when an operation uses a native library that retains process-global pools, can crash
the process, cannot cooperate with cancellation, or needs a hard per-operation memory ceiling. It
is not the default: process startup, serialization, IPC, duplicate-effect recovery, and deployment
packaging add cost. Ordinary async Rust handlers should remain in-process when their memory reaches
a stable plateau and they observe cancellation.

`plenora-runtime-subprocess` implements the protocol-neutral containment layer. A `SubprocessSpec`
selects an executable, argument vector, explicit environment and working directory; it never invokes
a shell and clears inherited environment by default. Argument count, argument bytes, environment
entries and environment bytes are validated before spawn. `Debug` reports sizes and counts rather
than values.

`SubprocessSupervisor` applies explicit maximum concurrency, wall-clock deadline, graceful and
hard-kill deadlines, bounded stdout/stderr retention, and reaping. Pipes are drained even after the
retention prefix is full, preventing a verbose child from blocking on a full OS pipe. Cancellation
distinguishes a request that wins while queued from one that terminates a running child.

Containment is delegated to `process-wrap`, which attaches one platform object making a child's
descendants addressable as a unit: a process group on Unix, a job object on Windows. That choice is
the only platform-dependent line in the crate. Everything after the spawn — graceful request,
forced termination, reaping, abandonment — drives one interface, so there is a single termination
path rather than one per platform, and the native calls it needs stay encapsulated in the
dependency: the crate keeps `#![forbid(unsafe_code)]`. Terminating a tree is now a system call
rather than an external command, which also removes a failure mode that was invisible in
production: `/bin/kill` and `/bin/ps` are absent from ordinary slim container images, so the
previous implementation degraded to killing the direct child in exactly the deployments this crate
targets.

A forced tree termination that cannot be delivered sets `SubprocessReport::tree_termination_degraded`
and increments `tree_termination_failures` instead of being discarded, so a degraded containment is
observable per invocation and in the aggregate. A false flag records only that no delivery failure
was observed; it never proves that no descendant survived. Termination, reaping, and pipe-reader
release run on every post-spawn failure path, and an error whose direct reaping or tree termination
could not be completed reports `containment_failed`. Abandoning the execution future releases both
pipe readers and force-kills the tree from the drop path: that is one non-blocking system call, so
unlike the external helper it replaced the drop path is bounded outright rather than by a waiting
budget. A signal it cannot deliver also increments `tree_termination_failures`.

Two consequences of the platform primitives are documented rather than hidden. The first is that a
destructive signal is only ever delivered while the leader is still unreaped, because a
process-group identifier can be recycled once its leader has been reaped and the crate refuses to
send a signal that could reach an unrelated group. So a descendant that ignores graceful
termination and outlives a leader which exits on its own, or within the graceful window, is not
force-killed. This is a real limit on Unix; on Windows a job object has no such ambiguity, but the
crate deliberately claims the weaker guarantee on both rather than making the contract depend on
where it runs. Containment is a property of the termination and abandonment paths, not of a child
that completes normally while leaving background descendants behind.

The second is that the graceful phase is a Unix-only request. A process group can be sent `SIGTERM`;
a job object has no cooperative equivalent, so on Windows termination is forced immediately instead
of spending the graceful window waiting for a request the child never received. Windows children
are also no longer placed in a new console process group, so a console control event delivered to
the supervisor now reaches them as well; containment itself does not depend on that flag, and no
guarantee here rests on it.

Waiting is always waiting for the leader, never for the containment unit. Every wait the supervisor
performs is either raced against cancellation or bounded by a deadline, and a unit-level wait is
unsafe in both positions: it reaps the leader first and then keeps draining, so a future dropped
after that first step would release the identifier while the supervisor still believed it could
signal it. Little is given up by this, because descendants are children of the leader rather than
of the supervising process, so reaping them here would be possible only in a host that has made
itself a subreaper.

One consequence of holding a single contract across platforms is worth stating plainly rather than
leaving to be discovered. If the operating system fails a wait on the child, the supervisor stops
treating the identifier as safe to signal, and it then declines to terminate the containment unit
even on Windows, where the job handle would still be valid. That invocation waits out its hard-kill
budget holding its capacity slot, reports a termination failure, and is not counted as completed;
only the eventual kill-on-close of the job stops it from leaking. Closing this would require
tracking identifier safety and containment ownership as two separate, per-platform concepts, which
is exactly the platform-conditional termination logic this design removed.

Linux additionally supports a polled per-child RSS ceiling. The supervisor exposes only capacity and
cumulative lifecycle counters to the control plane.

The crate deliberately does not define a command/result wire protocol, idempotency model, library
name, executable path, or payload codec. Each application-owned capability adapter decides whether
to call Rust in-process or serialize a bounded command into a supervised child. The three real
library APIs and representative memory probes are still required before making that choice. An
exit or kill only reclaims process memory; it does not prove a remote database, file, or network
effect did or did not happen.
