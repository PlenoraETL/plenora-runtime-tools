# Contributing

Treat the implementation specification and the documents under `docs/` as normative.

Before submitting a change:

1. preserve the dependency direction documented in `docs/boundaries.md`;
2. keep domain-specific business types out of the runtime crates;
3. add tests for every implemented requirement;
4. avoid `unwrap`, `expect`, `panic!`, unbounded queues, and unbounded task spawning in production
   runtime paths;
5. document new infrastructure dependencies and material architectural decisions;
6. run formatting, checks, Clippy, tests, documentation, audit, and dependency-policy gates.

Public APIs are not frozen. Prefer small, reviewable changes that complete one work package at a
time.

