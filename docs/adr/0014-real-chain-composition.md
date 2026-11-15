# ADR0014 - One real IO/DATA chain coordinator

Compose the existing pinned candidate IO and DATA adapters in the product host,
never in generic runtime crates. One authenticated profile authorizes complete
plans, including parameters, input references, per-step destination references
and contract-scoped consent. Both adapters validate the same full plan against
combined real discovery; each independently verifies its own descriptors.
No subplans, synthetic operation aliases or fallback adapters are introduced.

Handoffs use the current tenant/run resource ledger with exact bytes, digest and
producer/consumer schema barriers. A single coordinator owns stop, cancellation,
budgets, settlement and recovery. Publication without durable evidence remains
recovery-required and never permits a blind second invocation.

Dev-only IO tools/model and plenora-fork-parquet use the existing exact IO
candidate commit 9c087aa7daf5d9ad10843ebf3027ae8858f66b94. They generate real
format fixtures and geometry-free Parquet independently of the geometric IO
writer. tempfile =3.27.0 provides scoped test staging. These dependencies do
not extend the supported production format profile or qualify a release.

The current common chain contract allows loss grants only for database.write@2.
IO/DATA losses remain denied; reported is evidence selection, not consent.
Format qualification must record actual failures, not just catalog availability.

The optional database profile joins the same complete plan and registry with
real database.read@1 / write@2 descriptors. No connection occurs at admission;
connection secrets remain in the trusted profile. Database artifact handoff
also uses the invocation run ledger. Real PG execution is a separate CI gate.
IO loss consent is deferred to the common U4 revision and the future IO API;
S4 retains pre-effect rejection without private grants.
