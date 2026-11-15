# 0023: The primary geometry across components, Arrow Primary Geometry 1.0

Status: accepted
Date: 2026-10-31

## Context

data-tools is adding support for several geometry fields in one table, and
IO-tools already reads and writes them in GeoParquet (plenora-IO-tools#79).
Arrow Geometry Semantics 1.0 admits several `geoarrow.wkb` fields, each with
its own metadata (GEO-012), but nothing says which of them is the table's
primary geometry:

- GeoParquet requires one, `primary_column`. IO-tools #79 keeps it in its
  own contract and records it per field as the native key
  `plenora.geometry.native.geoparquet.primary_column`; its Arrow IPC driver
  still refuses several geometry fields, because the primary would not
  survive the hop.
- data-tools keeps an active geometry internally, falls back to the first
  geometry field, and would have to choose one by position after a
  projection or a join.
- Arrow itself has no standard key for it, so a primary chosen by one
  library is lost, or guessed, in the next.

The owner approved three principles: a common convention for the primary,
defined here, that survives Arrow and Arrow IPC between libraries; no
primary chosen automatically in a join; geometry operations with an
explicit column selector, the primary when it is omitted, and a refusal when
there are several geometries and no primary.

## Decision

A new opt-in contract, **Arrow Primary Geometry 1.0**
(`plenora-arrow-primary-geometry-v1`, PRIM-001 to PRIM-009), for components
that claim Arrow Geometry Semantics 1.0:

1. **One schema key, by identity** (PRIM-001). Schema metadata
   `plenora.primary_geometry` holds the `plenora.field_id` of the primary
   geometry field, or `none`. The identity, not the name or the position,
   because decision 0022 makes it the thing that survives a rename and a
   round trip, and Arrow Vocabulary 1.0 already requires it on every
   geometry field. A schema key, because the primary is a property of the
   table, and a field key would let two fields claim it.
2. **Absence keeps today's meaning** (PRIM-002). One geometry field without
   the key is the primary, as now; several without the key have no primary
   and stay valid for transport. `none` is needed for one case only: one
   geometry field left after the primary was removed, which must not become
   the primary by being alone.
3. **Invalid references are rejected** (PRIM-003): a key outside the
   grammar, in field metadata, on a schema without geometry, naming no field
   or a field that is not a geometry, is `schema`, in the vocabulary class,
   resolved against the schema as sent.
4. **One emitted form** (PRIM-004): the key only where absence would mean
   another state. Every single-geometry schema of today is unchanged on the
   wire, and two components that agree on the state emit the same bytes.
5. **No promotion** (PRIM-005): rename keeps the primary; removing it leaves
   no primary; adding geometries keeps it; a new field becomes primary only
   when there was no geometry, when the request names it, or when the
   operation's output contract says it replaces the primary.
6. **No election in joins** (PRIM-006): geometry fields from two or more
   inputs give no primary unless the request names one; geometry fields from
   one input keep that input's state by provenance: the primary is retained
   only if its field survives, and any reassigned ID is followed through the
   input-to-output mapping. An input without a primary stays without one.
   Appends and unions keep the primary only when every input's primary feeds
   the same output field; equal numeric IDs across inputs do not establish
   that correspondence.
7. **Operand selection** (PRIM-007): the request's selection by identity,
   otherwise the primary; neither is `schema` in the operation-schema class,
   never the first or the only field. An explicit selector is resolved on
   the received schema before completion: an absent identity is `schema`
   in the operation-schema class, in REJ-002 order, even if completion would
   assign that ID. The CRS the operation verifies, its
   support and the values it decodes are the operand's; every other geometry
   field is only checked for input conformance and carried. Two fields in
   two CRS therefore never conflict unless both are operands.
8. **GeoParquet maps exactly** (PRIM-008): `primary_column` becomes the key
   on read and the key becomes `primary_column` on write; a writer refuses a
   table without primary with `unsupported` and never picks one; the native
   key IO records is provenance, never read to decide. Single-geometry sinks
   keep GEO-012 and never write the primary alone.
9. **Vectors** (PRIM-009): 17 schema vectors in `vectors/arrow-primary-v1`,
   14 chains in `vectors/interop-primary-v1` (new schema
   `interop-primary-vector-v1`, which adds the operation class) and 7
   GeoParquet correspondences in `vectors/geoparquet-primary-v1` (new schema
   `geoparquet-primary-vector-v1`). The two geometry fields of the main
   fixture are in EPSG:32632 and EPSG:4326, with the primary not first.
   Another 13 provenance vectors in `vectors/primary-composition-v1` (new
   schema `primary-composition-vector-v1`) cover join, append and union,
   including ID remapping, removed primaries and inputs without a primary.

### Why a separate contract

Arrow Geometry Semantics 1.0 adds no key by its own scope, and components
already pin commits that claim it: rejecting a dangling key under it would
change the meaning of a pinned contract. A successor wire vocabulary
(`plenora.contract.version=2`) would make every consumer of version 1 reject
every schema for one optional key (ARROW-002). As in decision 0013, the rules
are an opt-in contract next to the existing ones, and the repository release
that publishes it is MINOR.

The key is outside the `plenora.geometry.` namespace on purpose: GEO-016
makes any unknown `plenora.geometry.` key a vocabulary defect, so a key under
it would be refused by every component that claims Arrow Geometry Semantics
1.0 but not this contract, instead of being carried.

### Categories

The defects of the key are the vocabulary class of REJ-001, after the field
checks of that class and before the CRS class; the missing operand is the
operation-schema class; the GeoParquet writer's refusal is the support
class. All are `schema` or `unsupported`, never `invalid_plan` (decision
0014).

### Owner resolutions

The owner adopts all six choices as final:

1. Joins with geometry from one input preserve its state by field provenance,
   retaining a primary only if it survives and updating its ID by the mapping.
2. Append and union retain a primary only when every input primary feeds the
   same output field; numeric ID equality between inputs is insufficient.
3. PRIM-004 is unchanged: omit the key for a single primary geometry, and emit
   `none` when the sole remaining geometry is not primary.
4. A missing primary required as operand is `schema`, operation-schema class.
5. GeoParquet writing without a primary is `unsupported`.
6. The name `plenora.primary_geometry` is confirmed, on the stated condition
   that its guarantees hold only between components claiming this contract.
   Carrying the key through a non-claiming component is not that guarantee.

## Alternatives

- **The primary by field name.** A rename would silently move it or leave it
  dangling, and names are data that error messages must not quote (decision
  0021).
- **A field-level marker** (`plenora.geometry.native.primary=true`).
  Two fields can carry it, a projection that drops one keeps the other's
  stale value, and a native key cannot be normative (ARROW-009).
- **The first geometry field as default.** It is the selection by position
  the owner excluded, and it changes when a projection reorders fields.
- **Always write the key.** Every single-geometry output of every component
  would change its bytes, and every published vector its expected table, for
  no new meaning.

## Hazards

A component that claims only Arrow Geometry Semantics 1.0 carries the key as
unknown metadata: if it renumbers field identities the key may name another
geometry field, which nobody can detect; if it drops the primary, a claiming
consumer rejects the dangling key. It also reads one geometry field declared
`none` as the primary. The rules are therefore relied on only between
claiming components (section 9 of the specification).

## Change statement

- **Consumers affected:** components that claim the new contract, the
  interoperability suite, and GeoParquet readers and writers.
- **Before:** several geometry fields valid, no primary expressible on the
  wire; each library chose its own.
- **After:** one schema key by field identity, its form, its preservation
  across operations, operand selection and the GeoParquet correspondence.
- **Compatible:** yes. Nothing published changes meaning; every vector of
  `vectors/arrow-data-v1` and `vectors/interop-v1` keeps its verdict under the
  new rules, which the gate checks for the first and the validator's
  tests for the second.
- **Schemas, examples and profiles:** new specification, three new schemas,
  51 vectors; an informative pointer in Arrow Geometry Semantics 1.0. No
  existing schema, catalog, binding, vector or profile changes.
- **Adoption impact**, for a component that claims the contract:
  - IO-tools: emit the key from `primary_column` on every Arrow surface, IPC
    included, and take `primary_column` from the key; keep the native key
    as provenance only; remove the IPC refusal of several geometry fields.
  - data-tools: keep the primary as field identity with an explicit
    no-primary state, never the first geometry field; carry it through
    every operation by PRIM-005 and PRIM-006; add the selector to the
    geometry operations (PRIM-007) and the explicit change of primary to its
    catalog; read and write the key on IPC, SDK and GeoParquet.
  - database-tools: carry the key and the field identities unchanged
    (decision 0022), or refuse several geometry fields with `unsupported`.
