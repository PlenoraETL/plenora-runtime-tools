# ADR0013 - DATA 5.0.0 candidate Rust adapter

Pin eba11205df585469a154de3ad76ab22a0cb9cad6 of PlenoraETL/plenora-data-tools,
branch rilascio/py314: candidate, not a release. The plenora-cli crate exports
Rust APIs and RuntimeBinding; no CLI process is used. plenora-core validates
Arrow contracts and plenora-pipeline supplies cooperative cancellation.
Dependencies remain in the product adapter, never generic runtime crates.
The host resolves opaque artifacts to bounded immutable bytes, stages output
privately, and only publishes through the journal after validating fidelity.
The closed data.run@3 payload and C-01 localized-effects metadata are distinct
immutable documents. Missing evidence is unavailable, never invented lossless.
No private operation aliases or loss consent contracts. The adapter profile
and qualifications must follow executable tests, not the target catalog.

The resolver stages in memory, but the pinned runtime also uses internal temporary
files without a host-supplied directory/quota. This is a declared qualification
limit, not an OS spool/RSS guarantee (docs/security-review.md). `reported` never
grants DATA loss. Metadata guards use SSA source ancestry for each output;
conflicting surviving field identities are rejected conservatively.
