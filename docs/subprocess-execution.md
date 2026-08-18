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
distinguishes a request that wins while queued from one that terminates a running child. Unix can
isolate a process group; Windows creates a new process group and uses the platform tree-termination
path. Linux additionally supports a polled per-child RSS ceiling. The supervisor exposes only
capacity and cumulative lifecycle counters to the control plane.

The crate deliberately does not define a command/result wire protocol, idempotency model, library
name, executable path, or payload codec. Each application-owned capability adapter decides whether
to call Rust in-process or serialize a bounded command into a supervised child. The three real
library APIs and representative memory probes are still required before making that choice. An
exit or kill only reclaims process memory; it does not prove a remote database, file, or network
effect did or did not happen.
