# Dependency policy

Dependencies are declared centrally in the workspace whenever they are shared. `Cargo.lock` is
committed, wildcard requirements are forbidden, and duplicate versions are reviewed.

Git dependencies require an ADR, a pinned commit, and a documented reason. New infrastructure
clients require an ADR and must remain behind an adapter or trait boundary. Default features must
be reviewed rather than accepted implicitly.

CI runs pinned releases of `cargo audit` and `cargo deny check` on both lockfiles (the workspace
and the separate `fuzz/` workspace) on every push and pull request, and weekly through
`supply-chain.yml`. GitHub Actions are pinned by full commit SHA with the release tag in a
comment; CI tools are installed at exact versions. Every exact pin in `Cargo.toml` and
`fuzz/Cargo.toml` carries its motivation next to it. License exceptions, advisory
exceptions, and source exceptions must be narrow, documented, and time-bounded where possible.

Release qualification is dispatched with an explicit expected commit SHA. All OS, documentation,
dependency-policy, and real-NATS jobs depend on that verification job and therefore qualify the
same checked-out revision on the pinned Rust 1.98.1 toolchain; the declared MSRV is Rust 1.98
(ADR 0007).
