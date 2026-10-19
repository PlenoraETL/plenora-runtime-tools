# Adopting the public contracts

Adoption demonstrates public behavior. It is not an instruction for organizing
component internals.

## Adoption sequence

1. Select the component profile under `profiles/`.
2. Pin an immutable revision of this repository.
3. Identify the public artifacts covered by the profile: binaries, packages,
   crates or runtime endpoints.
4. Verify each applicable contract through those public artifacts.
5. Record the pin, verification commands and deviations in a component-owned
   adoption manifest that validates against the
   [adoption manifest v4 schema](schemas/adoption-manifest-v4.schema.json).

The adopting project decides how to implement the required behavior.

## Immutable artifact identity

Every manifest v4 artifact records a non-empty release `version` and a
lowercase `sha256:<64-hex>` digest. The digest identifies the exact binary,
crate archive, wheel, package or deployed runtime image exercised by the
verification commands. A branch, mutable tag, rebuilt checkout or version
string without the digest is not sufficient adoption evidence.

## Black-box evidence

Verification should exercise the same boundary used by a real consumer:

- launch the released binary for CLI checks;
- import an installed wheel outside the source checkout for Python SDK checks;
- call only documented public Rust exports for Rust checks;
- send serialized requests through the public transport for runtime checks;
- validate emitted JSON and Arrow artifacts without inspecting private state.

Tests that read private fields or call private helpers are useful component
tests but are not sufficient adoption evidence.

## Capability truthfulness

The capability document must describe the exact artifact under test. An
operation disabled by build features or unavailable for the selected provider
must not be advertised as available.

## Deviations

A deviation records:

- the exact requirement identifier;
- the externally observable difference;
- the affected public artifact or surface, with at least one recorded
  explicitly;
- a tracking reference;
- whether consumers can detect the difference before invocation.

A deviation does not redefine the common contract and does not count as
conformance for that requirement.

A manifest is an unambiguous declaration: a contract cannot be both conforming
and not applicable, and an artifact name cannot identify conflicting artifact
descriptions. Repeated entries with consistent identities and different
verification evidence remain accepted. An artifact-scoped deviation refers to
a declared artifact name. If both artifact and
surface are given, the surface agrees with the named artifact. A surface-only
deviation can describe a missing surface and does not require inventing an
artifact for it. These cross-reference checks supplement structural schema
validation; they do not change the retained JSON Schemas.

## Contract versions

A manifest names each contract by its versioned identifier, and a version
is claimed only by name: `plenora-public-catalog-v2` and
`plenora-runtime-binding-v2` are separate claims from their version 1
([COMPATIBILITY.md](COMPATIBILITY.md#public-catalogs-and-runtime-binding-10-to-20)).
A component whose current catalog is of Public Catalogs 2.0 claims
`plenora-public-catalog-v2` for it; a runtime artifact claims the runtime
binding version its capability document advertises for the `runtime`
interface (CAP-003), and a claim of 2.0 records its result bounds (RT-024).

## No central implementation harness

This repository provides schemas, examples and black-box vectors. Each
component owns the executable harness needed to invoke its artifact because
build systems, provider fixtures and release environments differ.

Manifest v1 remains available for the former CLI/SDK-only scope. Manifests v2
and v3 remain immutable for existing cross-surface declarations. New adoption
uses manifest v4, which preserves Python `api_modes` and adds immutable artifact
identity.
