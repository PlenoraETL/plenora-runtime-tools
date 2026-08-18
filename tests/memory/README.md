# Memory-retention probe

This package tests whether long-lived worker execution releases task-owned allocations. It uses
synthetic committed buffers, so it validates runtime mechanics and the measurement harness rather
than predicting the future memory behavior of `data-tools`, `database-tools`, or `IO-tools`.

The normal workspace suite runs small deterministic contracts for success, failure, timeout,
concurrency, and the growth classifier. The RSS soak is opt-in and must run through the bounded
Docker wrapper from the workspace root:

```powershell
./scripts/memory-soak.ps1
```

The wrapper builds once, executes each scenario in a network-disabled container with a hard memory
limit, and writes one CSV file per scenario below `target/memory-soak/`. The intentional
`leak-control` scenario must be classified as growing; it proves that the harness detects retained
allocations.

High RSS after the first iteration is not automatically a leak. A stable allocator plateau passes.
Growth is measured between the median of the first and last sample quartiles after warm-up.
