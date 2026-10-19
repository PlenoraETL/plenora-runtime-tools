# Plenora Public Catalogs Contract 1.0

Status: normative

Contract identifier: `plenora-public-catalog-v1`

The machine shape is defined by
[`public-catalog-v1.schema.json`](../../schemas/public-catalog-v1.schema.json).
The target catalogs are in [`catalogs`](../../catalogs/), named
`<component>-v<N>.json`. The highest version of a component is its current
target; an earlier version stays as an available identity when a later one
changes operations incompatibly.

Informative: [Public Catalogs 2.0](PUBLIC-CATALOGS-2.0.md)
(`plenora-public-catalog-v2`) governs the catalogs whose `contract` is
`plenora-public-catalog-v2`; this text is unchanged and keeps governing the
catalogs of version 1 ([COMPATIBILITY.md](../../COMPATIBILITY.md#public-catalogs-and-runtime-binding-10-to-20)).

## 1. Purpose

A public catalog is the reviewed target boundary of one component. It states
which functions the Plenora ecosystem may depend on and how they are named,
versioned and represented. It does not claim that a particular released
artifact already conforms.

An artifact reports its actual surface through Capability Discovery 2.0. Its
capability document MUST be a truthful implementation of the applicable target
catalog: it may narrow conditional or extension entries, but it may not silently
rename a required operation or change its contracts.

## 2. Requirement levels

- `required`: every conforming artifact containing the applicable surface
  exposes the operation;
- `conditional`: the operation is required when the component publishes that
  function or surface;
- `extension`: the namespace and semantics are standardized, but an artifact
  may omit the extension entirely.

`storage-tools-v1.json` is normative after ratification of its atomicity,
publication, pagination, integrity and artifact-reference semantics. Normative
catalog status fixes identities and testable boundaries; it is not a release
or artifact conformance claim. Storage artifact capabilities may remain
`experimental` until a qualified release exists.

## 3. Operation identity

The pair `(id, version)` is the semantic operation identity. The component
release version, CLI protocol version, runtime binding version and operation
version are independent.

Changing accepted input, output meaning, side-effect classification or control
semantics incompatibly requires a new operation version and new immutable
contract identifiers. Surface-specific convenience spelling does not create a
new operation.

## 4. Payload descriptors

Every input and output declares:

- an operation-specific contract identifier;
- all accepted or produced content types;
- zero or more shared interchange contracts.

The operation-specific identifier owns the domain meaning. For example,
`plenora-database-read-result-v1` identifies the result of `database.read`,
while `plenora-arrow-interchange-v1` states that the tabular payload can cross a
component boundary without translation.

A component-owned schema remains component-owned when no other component needs
its fields. The stable identifier still appears here so callers can reject the
wrong payload before execution. This repository owns a schema when two or more
components must interpret the same fields.

## 5. Surfaces and released capabilities

An operation's `surfaces` array is the target binding set. An adopter MUST:

1. expose the same operation identity on every target surface it implements;
2. publish only the surfaces actually present in its capability document;
3. preserve validation, defaults, output meaning and error axes across those
   surfaces;
4. record temporary deviations in its adoption manifest rather than editing
   the common catalog to match an incomplete implementation.

The exact CLI, Python and runtime spellings are defined by
[Surface Bindings 1.0](../surfaces/SURFACE-BINDINGS-1.0.md).

## 6. Data kernel registry

`data.run` is the externally invocable plan operation. The table and geo
kernels selected inside a plan are not 146 artificial CLI commands. Their
stable identifiers and versions live in the registry the catalog's
`data.catalog` operation names: [`data-kernels-v2.json`](../../catalogs/data-kernels-v2.json)
for the current data-tools catalog, where a kernel's `version` is the version
of its observable semantics, and
[`data-kernels-v1.json`](../../catalogs/data-kernels-v1.json) for version 1.
Their machine shape is
[`operation-registry-v1.schema.json`](../../schemas/operation-registry-v1.schema.json).

Capability discovery for `data.catalog` MUST report only kernels present in the
answering artifact. Kernel parameters and logical result shape are described by
the component-owned kernel descriptor returned by `data.catalog`; the stable
kernel identity and version MUST agree with the common registry. How the
version 2 result reports a registered kernel the artifact cannot execute is
defined by the data-tools profile version 2 (DT-001).

## 7. External verification

Conformance tests treat the catalogs as black-box expectations. They verify
discovery, successful invocation, rejection of wrong versions and contracts,
typed failures and side-effect reporting. They MUST NOT inspect crate modules,
private classes, driver registries or executor topology.
