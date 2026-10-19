# Transport consumer profile: runtime-tools

Profile identifier: `plenora-runtime-tools-profile-v1`

Normative runtime selectors: [`runtime-v1.json`](../bindings/runtime-v1.json)

## Role

`runtime-tools` is the common transport consumer for the five domain-library
profiles. It is not a sixth domain library and does not own their operation
schemas, capability truth, domain errors or business semantics.

## Applicable contracts

- [Runtime Binding 1.0](../specs/runtime/RUNTIME-BINDING-1.0.md), or
  [Runtime Binding 2.0](../specs/runtime/RUNTIME-BINDING-2.0.md) for a claim
  of that version (section "Runtime Binding 2.0" below)
- [Runtime Conformance Vectors 1.0](../specs/runtime/RUNTIME-VECTORS-1.0.md)
- [Typed Errors 1.0](../specs/errors/ERRORS-1.0.md), for serialized public failures
- [Public Security 1.0](../specs/security/PUBLIC-SECURITY-1.0.md)
- [Surface Bindings 1.0](../specs/surfaces/SURFACE-BINDINGS-1.0.md)

## Required public behavior

The released Rust and runtime artifacts MUST:

- preserve the request, success and error metadata defined by Runtime Binding
  1.0 without interpreting domain payloads;
- reject malformed routing, operation version, contract identity, content type
  or envelope identity before domain invocation;
- preserve payload bytes, advertised content type, supported execution controls
  and originating correlation identity;
- return serialized output for result-producing operations rather than reducing
  success to an acknowledgement;
- expose terminal public failures as bounded `plenora-error-v1` values while
  keeping retry settlement and remote-effect certainty conservative;
- exercise the pinned runtime vectors through a public codec or transport
  boundary.

`runtime-tools` validates the envelope, routing and compatibility with
registered capabilities. The black-box component validates its own payload and
operation semantics. No provider adapter belongs to `runtime-tools`.

## Public surfaces

- Rust API: required.
- Runtime transport: required for a conformance claim.
- CLI: not required by this profile.
- Python SDK: not required by this profile.

## Adoption

`runtime-tools` records a full immutable `plenora-contracts` revision and
black-box verification commands in adoption manifest v4. It MUST NOT mark
Runtime Binding 1.0 conforming while request, success or terminal-error vector
coverage is incomplete. Temporary gaps are documented as deviations and do not
count as conformance.

## Runtime Binding 2.0

A claim of Runtime Binding 2.0 (`plenora-runtime-binding-v2`) is separate
from a claim of 1.0 and adds, for runtime-tools:

- requests and results of both versions use the wire format of version 1;
  the value `1` is carried only by the request, in
  `plenora.capability.version` (RT-025), and a result carries no wire
  version;
- a request meant for 2.0 is routed only to a destination whose capability
  document advertises `plenora-runtime-binding-v2`, or to one of 1.0 after
  the sender's explicit downgrade, and refused before sending when the
  destination's capability document cannot be read (RT-026);
- it keeps the acceptance record of every request by
  `plenora.message.id`; as ingress of a new request it attaches the context
  the sender states, or the default of 1.0 for a sender that states none
  unless it is configured with a minimum requirement of 2.0, which refuses
  such a request; for a retry, a redrive or a resend it acts as a
  forwarding; it carries the context unchanged outside the envelope, and
  refuses a request whose context is lost or altered, or whose acceptance
  record it cannot read authoritatively (RT-027). Its adoption manifest
  states whether a minimum requirement of 2.0 is configured at its ingress,
  and the window for which it keeps acceptance records;
- a result the transport cannot carry whole fails with `resource_limit` and
  is never delivered truncated as a success (RT-024); the transport's bound
  is declared in its adoption manifest;
- the vectors of [`vectors/runtime-v2`](../vectors/runtime-v2/) and
  [`vectors/runtime-negotiation-v1`](../vectors/runtime-negotiation-v1/) are
  exercised in addition to those of `vectors/runtime-v1`.

A claim of 1.0 alone keeps the obligations above this section.

## Not specified here

This profile does not prescribe worker engines, brokers, queues, handler
registries, retry schedulers, result topics or internal Rust module boundaries.
