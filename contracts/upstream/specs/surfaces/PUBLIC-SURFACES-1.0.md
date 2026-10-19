# Plenora Public Surfaces Contract 1.0

Status: normative

Contract identifier: `plenora-public-surfaces-v1`

This document defines how a Plenora component describes functionality exposed
outside its implementation boundary.

## 1. Applicability

This contract applies to every operation intended for callers, orchestrators or
other Plenora components. Private helpers and implementation-only entry points
are outside its scope.

## 2. Component identity

**SURF-001** — A component MUST expose one stable identifier matching
`plenora-<domain>-tools`.

**SURF-002** — Every released artifact MUST expose its component version through
each discovery-capable surface.

The component version identifies the artifact release. It does not replace the
versions of the contracts exposed by that artifact.

## 3. Operation identity

**SURF-003** — Every externally invocable operation MUST have a stable identifier
of the form `<domain>.<action>` or `<domain>.<group>.<action>`.

**SURF-004** — An operation identifier MUST describe observable intent, not an
implementation type or provider. Provider selection, where applicable, is an
explicit input or capability attribute.

**SURF-005** — Renaming an operation or changing its observable meaning is an
incompatible change.

Examples of valid identity families are `database.query`, `geo.buffer`,
`io.read`, `rest.execute` and `storage.get`. Profiles define the required
families without dictating internal APIs; the storage profile standardizes its
seven `storage.*` v1 operations.

## 4. Operation descriptor

**SURF-006** — Before invocation, a consumer MUST be able to obtain for each
public operation:

- operation identifier and contract version;
- availability status;
- public surfaces that expose it;
- input contract identifier;
- output contract identifier;
- accepted and produced content types;
- whether the operation can have a remote side effect;
- whether idempotency keys, deadlines or cancellation are accepted;
- any provider or build constraint required for safe selection.

The machine representation is defined by
[Capability Discovery 2.0](../capabilities/CAPABILITY-DISCOVERY-2.0.md).

`side_effect` represents the conservative maximum-risk class. `remote` may
include local effects in addition to remote effects; `local` guarantees that
the operation cannot produce remote mutations.

## 5. Input and output contracts

**SURF-007** — An operation MUST reject input that does not satisfy its declared
input contract. Unknown fields MUST NOT be silently interpreted as a different
operation.

**SURF-008** — Every successful result MUST identify or unambiguously imply the
declared output contract and content type.

**SURF-009** — A surface-specific convenience type MAY wrap the common semantic
contract, but it MUST preserve required fields, units, defaults and outcome
meaning.

For example, an SDK may return a typed object while a runtime binding transfers
JSON or Arrow IPC. Those representations conform only when they preserve the
same operation contract.

## 6. Observable execution controls

**SURF-010** — If an operation advertises deadline support, expiry MUST be
observable as a typed `timeout` error. Stopping local waiting does not prove a
remote write was rolled back.

**SURF-011** — If an operation advertises cancellation support, cooperative
cancellation MUST be observable as a typed `cancelled` error.

**SURF-012** — If an operation advertises idempotency-key support, reuse of the
same key with the same contract input MUST have the behavior documented by that
operation. Reuse with different input MUST fail closed.

This contract does not prescribe timers, tokens, tasks or storage used to
implement those controls.

## 7. Outcomes and failures

**SURF-013** — An operation MUST distinguish success from failure through the
public surface without requiring message parsing.

**SURF-014** — Partial or ambiguous outcomes MUST NOT be reported as complete
success.

**SURF-015** — Public failures MUST map to the common typed error contract.
Domain-specific codes and details MAY be added without changing the shared
axes.

## 8. Surface bindings

The following surface identifiers are reserved:

| Identifier | Boundary |
|---|---|
| `rust` | documented public Rust API |
| `cli` | public process-level command line |
| `python_sdk` | installed public Python package |
| `runtime` | serialized invocation through runtime-tools |

**SURF-016** — A component MUST advertise only surfaces present in the released
artifact.

**SURF-017** — Multiple surfaces exposing the same operation version MUST have
equivalent validation, result meaning, error axes and security defaults.

Equivalent behavior does not require identical language-level method names or
types.

## 9. Operation-specific specifications

An operation-specific public specification MUST state:

1. stable operation and contract identifiers;
2. accepted input contract and content types;
3. output contract and content types;
4. externally visible validation and defaults;
5. success, partial and failure semantics;
6. error categories and possible remote effects;
7. supported execution controls;
8. compatibility rules.

It MUST NOT mandate an internal trait, module, dependency, scheduler, provider
implementation or algorithm.

Component-owned operation documents SHOULD start from the
[public operation template](../../templates/OPERATION-CONTRACT.md).
