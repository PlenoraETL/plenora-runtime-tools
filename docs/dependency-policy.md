# Dependency policy

Dependencies are declared centrally in the workspace whenever they are shared. `Cargo.lock` is
committed, wildcard requirements are forbidden, and duplicate versions are reviewed.

Git dependencies require an ADR, a pinned commit, and a documented reason. New infrastructure
clients require an ADR and must remain behind an adapter or trait boundary. Default features must
be reviewed rather than accepted implicitly.

CI runs pinned releases of `cargo audit` and `cargo deny check`. GitHub Actions and CI tools use
exact release tags rather than floating major-version tags. License exceptions, advisory
exceptions, and source exceptions must be narrow, documented, and time-bounded where possible.

Release qualification is dispatched with an explicit expected commit SHA. All OS, documentation,
dependency-policy, and real-NATS jobs depend on that verification job and therefore qualify the
same checked-out revision on the declared Rust 1.97.1 MSRV.
