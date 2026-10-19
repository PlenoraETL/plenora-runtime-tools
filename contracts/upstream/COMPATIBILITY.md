# Public compatibility policy

Compatibility is evaluated from the point of view of a conforming external
consumer.

## Compatible changes

A change is compatible only when an existing conforming consumer can continue
to invoke the same operation and interpret the result with the same meaning.

Typical compatible changes are:

- adding an optional field whose absence preserves the previous behavior;
- exposing a new operation under a new stable identifier;
- adding a new public surface to an existing operation;
- adding an unavailable capability with an explicit reason;
- clarifying prose without changing accepted data or semantics.

## Incompatible changes

The following are incompatible:

- removing or renaming an operation, required field or public symbol;
- changing the meaning, type, units or default of a field;
- changing a success into a partial outcome, or an acknowledged partial
  outcome into success;
- weakening remote-effect or retry information;
- silently replacing one output contract or content type with another;
- changing CLI stream selection or exit-code projection;
- dropping a supported SDK runtime version outside the component's declared
  compatibility policy;
- changing a closed enumeration without an explicit version transition.

An incompatible change requires a new public contract version. A component may
temporarily expose old and new versions together and advertise both through
capability discovery.

## Operation evolution

Operation identity and operation contract version are independent from package
and component release versions. A component patch or minor release may expose a
new compatible operation. It must not change an existing operation contract
incompatibly without incrementing that contract version.

## Consumer behavior

Consumers must select only advertised operations and versions. They must ignore
unknown optional fields where the enclosing schema permits them and must fail
closed on unknown required contract versions.

## Immutability of published documents

A versioned JSON Schema may be changed in place only when the edit cannot alter
whether an existing instance validates. Otherwise a new schema identifier and
file are required.

The machine-readable documents that carry operation identities are fixed in the
same way once published on `main`:

- a public catalog (`catalogs/*-tools-v<N>.json`) may add a new operation
  identity `(id, version)`, add a surface to an existing operation, select a
  target surface that was `undecided` or `not_applicable`, clarify an
  operation `summary`, and move its `status` from `provisional` to
  `normative`. Every other field of a published operation identity, and of
  the catalog itself, is fixed; an identity is never removed;
- an operation registry (`catalogs/*-v<N>.json` with contract
  `plenora-operation-registry-v1`) may add kernels; a listed kernel keeps its
  entry;
- a surface binding map (`bindings/*-v<N>.json`) may name the artifact of a
  component section that had none, add discovery entrypoints and bind new
  operation versions; a published binding keeps its requirement and
  entrypoints, and no section is removed;
- a normative vector (`vectors/**/*.json`) is never changed or removed; a
  corrected or replaced vector is a new file.

A protected document whose kind the gate does not recognize is compared whole.

### Errata before adoption

The only exception is a declared erratum: a schema or another protected
document found wrong before any component adopted it and before any release
used it may be corrected in place once, by a decision that states the defect,
the date and the absence of adopters. The immutability gate (`ERRATA` in
`tools/check_schema_immutability.py`) admits exactly that transition, from the
published content to the corrected one, only while the decision exists and
names the file in its `## Erratum` section, and only against a base that still
published the erroneous content (the declared last base or one of its
ancestors); against a later base the erratum admits nothing. Recorded errata:
`data-execution-result-v3.schema.json` and `data-run-success-v3.json`
([decision 0008](decisions/0008-data-run-runtime.md)).

CI compares the protected documents against three revisions: the base of
the change, the ratified floor recorded in
`tools/check_schema_immutability.py` and the commit where the checked revision
left `origin/main` (`tools/ci_comparison_base.py`). The base of a push to
`main`, forced or not, is the previous tip of `main`; the base of a release
tag `v*` is the tagged commit, which must be a commit of `main`; the base of a
pull request, or of a push to another branch, is the commit where it leaves
`main`, because a branch's previous tip may predate changes that `main` made
legitimately. In addition, every document the checked tree changed since it
left `main` is compared with the current `origin/main`, so a branch cannot
rewrite a document `main` published after the branch forked; a document the
branch did not touch takes `main`'s content when merged and is not compared.
In CI `origin/main` is fetched first and must equal the remote's
`refs/heads/main`; an older ref fails the check instead of hiding what `main`
published since.
The fork point also protects documents published after the floor on the first
push of a new branch. Without
`origin/main` the gate fails; outside CI, `PLENORA_ALLOW_NO_FORK_POINT=1`
waives that revision explicitly and the gate checks the other two (the waiver
is ignored when `CI`, `GITHUB_ACTIONS` or `GITHUB_RUN_ID` is set). Files of
the checked tree are listed by Git, ignored files under the protected
directories included, and each counts only with the exact spelling it has on
disk, so a rename that changes only letter case is a removal on every file
system. It rejects removed documents, changed assertions and identities, and reused schema
identifiers. The guard treats only schema annotations as editable prose; a
property or literal named `description` remains part of the validation rules.

## Contract version transitions

A new version of a contract is a new document and a new identifier next to
the previous ones; the previous text is never edited to carry the new
obligations, and receives at most an informative pointer.

### Public Catalogs and Runtime Binding 1.0 to 2.0

[Public Catalogs 2.0](specs/catalogs/PUBLIC-CATALOGS-2.0.md)
(`plenora-public-catalog-v2`) and
[Runtime Binding 2.0](specs/runtime/RUNTIME-BINDING-2.0.md)
(`plenora-runtime-binding-v2`) add obligations to the 1.0 texts, which stay
unchanged ([decision 0015](decisions/0015-intentional-absences-and-planned-surfaces.md)).

**What changes.**

- Public Catalogs 2.0: catalog surfaces are the target, never availability,
  and a consumer selects surfaces only from the artifact's capability
  document (CAT-001); a surface is never removed and never marked planned,
  and an omitted `conditional` surface needs no deviation (CAT-002); a
  surface a profile declares intentionally absent is never added to that
  operation identity, with a reason of fixed grammar the validator checks
  (CAT-003). Its schema `public-catalog-v2` differs from version 1 only in
  the `contract` value.
- Runtime Binding 2.0: a result beyond a bound of the transport or of the
  component fails with `resource_limit`, never with a truncated success
  (RT-024). RT-025 replaces RT-002: `plenora.capability.version` stays `1`
  and names the wire format, which 2.0 keeps byte for byte; the contract
  version is the `runtime` interface of the capability document (CAP-003).
  Fixtures of RT-024 are in `vectors/runtime-v2`.

**Who adopts.**

- A component whose current catalog declares `plenora-public-catalog-v2`
  follows Public Catalogs 2.0 for it: it keeps the absences its profile
  declares and records a deviation only for an omitted `required` surface.
  Its earlier catalogs of version 1 keep Public Catalogs 1.0.
- A consumer that reads a catalog of version 2 selects operations and
  surfaces only from capability documents (CAT-001).
- A runtime artifact that advertises `plenora-runtime-binding-v2` follows
  RT-024 and RT-025, declares its bounds where its profile says, and
  exercises `vectors/runtime-v2` in addition to `vectors/runtime-v1`.
- runtime-tools, for a claim of Runtime Binding 2.0, declares its transport
  bound in its adoption manifest, applies RT-026 to every destination it
  selects for a request meant for 2.0, and exercises `vectors/runtime-v1`,
  `vectors/runtime-v2` and `vectors/runtime-negotiation-v1`
  ([runtime-tools profile](profiles/runtime-tools.md)).
- A sender that requires Runtime Binding 2.0 selects destinations by RT-026.

**Coexistence.** The two versions are valid at the same time, under
conditions:

- a component may publish catalogs of both versions, each with its profile;
  the operation identities they share keep one meaning;
- an artifact states the runtime binding it follows in its capability
  document. A request carries wire version `1` under both (RT-025), so a
  destination accepts it whatever its contract version, and an artifact of
  2.0 satisfies every rule of 1.0 except RT-002, which RT-025 replaces;
- the contract that governs an exchange is decided by the sender before
  sending (RT-026): a sender of 1.0 may use a destination of 2.0; a sender
  of 2.0 uses a destination of 1.0 only after declaring that it accepts the
  downgrade, and otherwise refuses before sending; a request meant for 2.0
  routed to a destination of 1.0 is detectable only by that selection, so a
  transport that routes such requests checks each destination's capability
  document and refuses when it cannot read it. The sender's requirement and
  its consent to the downgrade travel outside the envelope, from the ingress
  of a new request through every forwarding to the hop that selects the
  destination; an acceptance record per `plenora.message.id` tells a new
  request from a retry, a redrive or a resend, and a hop refuses a lost or
  altered context or one it cannot check (RT-027). The vectors of
  `vectors/runtime-negotiation-v1` illustrate these sequences;
- the guarantee against an implicit downgrade covers only stated
  requirements: a sender of 2.0 that states none is indistinguishable from
  a sender of 1.0, its requirement is taken as 1.0 and the selection does
  not guarantee RT-024 (a destination of 2.0 still applies it), unless the
  ingress is configured with a minimum requirement of 2.0, which then
  refuses it;
- an adoption manifest declares each contract identifier it claims
  (`plenora-public-catalog-v1` or `-v2`, `plenora-runtime-binding-v1` or
  `-v2`), with its own verification.

There is no unconditional compatibility: a sender that states a
requirement of 2.0 and a destination of 1.0 do not exchange without an
explicit downgrade.

**Exit of 1.0.** No 1.0 document, schema, catalog or vector is ever removed
or changed: a pin to a revision that adopts 1.0 stays valid. Version 1.0
stops being a target once every domain component's current catalog
declares `plenora-public-catalog-v2` and the adoption manifests of the five
domain components and of runtime-tools, at their latest pinned revisions,
declare the 2.0 identifiers. From then on new operations and surfaces are
added only to catalogs of version 2, the profiles stop listing the 1.0
texts as applicable, and the CHANGELOG records 1.0 as superseded.

## Repository releases

A repository release is an annotated Git tag `vMAJOR.MINOR.PATCH` on a commit
of `main`, with a matching section in [CHANGELOG.md](CHANGELOG.md); the tag
carries the date. The tag is a readable name for that commit and for the
changes the CHANGELOG lists up to it; it does not replace the pin. An adoption manifest records the full
commit SHA (see [ADOPTION.md](ADOPTION.md)) and may cite the tag next to it.
A tag is never moved or reused.

The repository version follows the documents it publishes:

- MAJOR: a published document, identifier or rule is withdrawn or changes
  meaning, which the immutability rules above admit only through a declared
  erratum or a governance decision;
- MINOR: new contract versions, operations, surfaces, schemas, catalogs or
  vectors, next to the existing ones;
- PATCH: compatible prose clarifications, errata, validator and tooling
  changes that do not alter any published document.

Every commit of `main` remains a valid pin; a release only names one.
