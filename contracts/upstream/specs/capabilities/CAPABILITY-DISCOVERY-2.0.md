# Plenora Capability Discovery Contract 2.0

Status: normative

Contract identifier: `plenora-capabilities-v2`

The machine shape is defined by the
[capabilities v2 schema](../../schemas/capabilities-v2.schema.json).

## 1. Purpose

Capability discovery tells a consumer which public operations a specific
released artifact exposes and how those operations can be invoked. It is not a
roadmap, an internal plugin inventory or an implementation report.

## 2. Artifact identity

**CAP-001** — A capability document MUST identify the component and exact
component version that produced it.

**CAP-002** — The document MUST describe the artifact currently answering the
request, including compile-time or packaging differences that alter its public
surface.

## 3. Interfaces

**CAP-003** — `interfaces` MUST list every discovery-capable public surface
present in the artifact and the contract version implemented by that surface.

**CAP-004** — A listed interface MUST be callable in that artifact. Build intent
or source code that was not packaged is not an exposed interface.

## 4. Operations

**CAP-005** — Every externally invocable operation MUST have exactly one
descriptor for each exposed operation-contract version.

**CAP-006** — The descriptor MUST identify:

- stable operation identifier and contract version;
- availability status and, when unavailable, a reason;
- surfaces through which the operation is reachable;
- versioned input and output contracts;
- accepted and produced content types;
- public side-effect class;
- supported deadline, cancellation and idempotency-key controls.

**CAP-007** — Every surface named by an operation MUST also be present in
`interfaces`.

**CAP-008** — An `available` operation MUST be invocable on every surface it
lists. An operation requiring a provider, credential or runtime resource MAY
still be available when those are ordinary operation inputs.

**CAP-009** — Behavior that is compiled out, unqualified or prohibited by the
artifact MUST be `unavailable` with a bounded reason, or absent if the
operation is not part of that artifact's supported public catalog.

## 5. Status values

- `available`: supported for invocation under the declared contract.
- `unavailable`: known operation not callable in this artifact.
- `experimental`: callable but not covered by stable compatibility promises.
- `deprecated`: callable but scheduled for removal in a future incompatible
  version.

Consumers MUST NOT invoke `unavailable` operations. Consumers use
`experimental` operations only through explicit opt-in.

## 6. Contract references

**CAP-010** — Input and output contract identifiers MUST be immutable,
versioned names. Content types identify their public serialization.

Operation-specific schemas may be component-owned. A consumer does not need
their source location when the surface offers a native typed binding, but the
contract identifier and version remain observable.

## 7. Attributes

`attributes` MAY expose additional structured selection information such as
supported formats, providers, geometry types or transfer limits.

**CAP-011** — A consumer MUST NOT need to parse a human-readable reason to
select an available operation. Selection data belongs in typed attributes.

**CAP-012** — Attributes MUST describe externally observable support. Internal
driver names, dependency versions and private feature topology are out of scope
unless they change consumer-visible compatibility.

**CAP-013** — Attributes used for machine selection MUST conform to an
immutable, versioned component-owned contract named by the component profile.
Untyped attributes are opaque diagnostics: consumers MUST NOT interpret them
for orchestration or compatibility decisions.

Attribute keys beginning with `plenora.` are reserved for shared contracts of
this repository and a component MUST NOT use them for its own data. The only
shared attribute is `plenora.surface_side_effects`
([Surface Bindings 1.0, SB-001](../surfaces/SURFACE-BINDINGS-1.0.md#materializing-a-result-that-the-surface-cannot-return-in-process)),
the effect a surface adds to the operation's `side_effect`.

A component profile MAY instead assign detailed discovery to a versioned public
catalog operation. In that case the operation result is the sole normative
source for those details, and `attributes` MUST NOT duplicate them.

## 8. Ordering and freshness

Array order is not semantically significant. Producers SHOULD emit stable order
to support review and reproducible evidence.

Capability results MAY be cached only for the identified component version and
artifact identity. A provider-specific capability probe MAY return a narrower
document, but MUST NOT claim broader support than the artifact-level document.

## 9. Version 1

`capabilities-v1.schema.json` is retained for historical component-local
adoption. Version 2 replaces feature-only discovery with operation and public
surface discovery. New common adoption uses version 2.
