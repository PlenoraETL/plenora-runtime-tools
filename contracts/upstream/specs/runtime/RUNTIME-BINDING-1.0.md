# Plenora Runtime Binding Contract 1.0

Status: normative

Contract identifier: `plenora-runtime-binding-v1`

This contract maps public operations to the transport-neutral capability
boundary exposed by runtime-tools. It governs serialized requests and results,
not adapters, handlers, queues or worker implementation.

Informative: [Runtime Binding 2.0](RUNTIME-BINDING-2.0.md)
(`plenora-runtime-binding-v2`) adds RT-024 and keeps the wire format of this
version; this text is unchanged and keeps governing the artifacts that
advertise `plenora-runtime-binding-v1`
([COMPATIBILITY.md](../../COMPATIBILITY.md#public-catalogs-and-runtime-binding-10-to-20)).

## 1. Applicability

An operation follows this contract only when its capability descriptor lists the
`runtime` surface.

## 2. Capability identity

**RT-001** — The runtime capability name for component
`plenora-<domain>-tools` is `plenora.<domain>-tools`.

**RT-002** — Capability version `1` identifies this runtime binding version.
It is independent from component release and operation contract versions.

**RT-003** — The runtime operation selector carries the complete stable public
operation identifier, for example `database.read` or `geo.buffer`.

## 3. Request metadata

A runtime request carries these reserved metadata values:

| Key | Meaning |
|---|---|
| `plenora.capability.name` | runtime capability identity |
| `plenora.capability.version` | runtime binding version |
| `plenora.capability.operation` | stable public operation identifier |
| `plenora.operation.version` | operation contract version |
| `plenora.input.contract` | versioned input contract identifier |
| `plenora.trace.correlation_id` | originating correlation identity |

**RT-004** — Routing and contract metadata MUST agree with one available
operation advertised by Capability Discovery 2.0. Mismatch fails before
invocation with `protocol` or `unsupported`.

**RT-005** — The serialized payload content type MUST be one of the input
content types advertised for that operation version.

The runtime message envelope carries these identity keys:

| Key | Requirement |
|---|---|
| `plenora.message.id` | required unique message identity |
| `plenora.trace.correlation_id` | required originating correlation identity |
| `plenora.message.causation_id` | optional direct-cause message identity |

**RT-012** — Envelope identities MUST use the canonical lowercase hyphenated
UUID representation. Alternate key spellings and non-canonical UUID text are
not compatible aliases. Message and optional causation identities remain
observable to the caller; results preserve the originating correlation UUID.

## 4. Execution controls

When advertised by the operation, the request MAY also carry:

- `plenora.execution.deadline`: an absolute RFC 3339 UTC timestamp;
- `plenora.execution.idempotency_key`: an opaque bounded key.

Cancellation is propagated by the runtime invocation context.

**RT-006** — A control MUST NOT be accepted silently when the operation
descriptor declares it unsupported.

**RT-007** — Deadline, cancellation and idempotency behavior MUST preserve the
semantics of the same operation version on its other public surfaces.

## 5. Artifact-bearing requests and results

**RT-013** — A persistable serialized input that identifies a file or other
artifact MUST carry an opaque artifact reference. Private local paths MUST NOT
cross the runtime boundary. The reference is resolved only through an
authorized runtime resource. When the component contract declares artifact
metadata, it is bounded and keeps content type, byte size and a genuinely
calculated SHA-256 separate from provider-specific identifiers.

**RT-014** — An artifact result MUST preserve its output contract, content
type, originating correlation identity, byte count and checksum algorithm and
value. The concrete transport or storage mechanism for the artifact is
implementation-specific.

**RT-015** — Artifact source and sink resolution belongs to the final
application boundary. A component may expose transport-neutral resolver traits
but MUST NOT require a core runtime-tools crate to depend on the component.

## 6. Success result

A successful runtime invocation returns serialized output with:

- content type advertised by the operation;
- `plenora.operation.version`;
- `plenora.output.contract`;
- the originating correlation identity.

**RT-008** — A result-producing operation MUST return its public result. A
successful acknowledgement with no result is conforming only when the declared
output contract explicitly represents an empty acknowledgement.

**RT-009** — Streaming and artifact-reference outputs MAY use transport-specific
delivery, but every delivered result MUST preserve the advertised content type,
output contract and correlation identity.

## 7. Failure result

**RT-010** — A public runtime failure MUST preserve the common error axes from
`plenora-error-v1`. An adapter-specific retry class is not a substitute for
the public category, phase, remote effect and retry disposition.

A serialized error result uses content type
`application/vnd.plenora.error+json` and output contract
`plenora-error-v1`.

**RT-011** — Unknown operation, version, input contract or content type fails
before invocation with `remote_effect: none`.

## 8. Security

Runtime requests follow
[Public Security 1.0](../security/PUBLIC-SECURITY-1.0.md). Persistable messages carry
connection or secret references and MUST NOT contain raw credentials, tokens,
authorization headers or private key material.

## 9. Compatibility

Adding optional metadata is compatible when its absence preserves previous
behavior. Renaming reserved keys, changing their meaning, changing routing
identity or weakening result/error semantics requires a new runtime binding
version.

## 10. Canonical selectors and vectors

The exact operation selectors for the five component profiles are registered
in [`bindings/runtime-v1.json`](../../bindings/runtime-v1.json). Reusable
request, success and error fixtures are defined by
[Runtime Conformance Vectors 1.0](RUNTIME-VECTORS-1.0.md).

## 11. Rejection before invocation

These rules clarify sections 3, 4 and 7. Each one chooses, among the values
those sections already admit, the one a conforming consumer can rely on; none
admits a request or a result that 1.0 rejected. The reasons and the
compatibility of each are recorded in
[decision 0010](../../decisions/0010-runtime-rejection-and-identity.md).
[`vectors/runtime-probes-v1`](../../vectors/runtime-probes-v1/) states the
expected rejection of each probe as data ([RUNTIME-VECTORS-1.0
§6](RUNTIME-VECTORS-1.0.md#6-rejection-probes)).

**RT-016** — A request that fails RT-004, RT-005, RT-006, RT-011, RT-012 or a
rule of this section is rejected before invocation: no domain functionality
runs. The error has `phase: validate` (ERR-003: no later phase started),
`remote_effect: none` (ERR-004: the absence of an effect is proven) and
`retry.kind: never` (the same message fails the same way again).

**RT-017** — A reserved request key that is required and absent, is not a
JSON string, or does not match its grammar fails with `protocol`. A value
outside the grammar is never normalized into a value inside it: `"01"`,
`" 1"`, `1` and uppercase or braced UUID text are malformed, not aliases.

| Key | Grammar |
|---|---|
| `plenora.capability.name` | `^plenora\.[a-z][a-z0-9-]*-tools$` |
| `plenora.capability.version` | `^[1-9][0-9]*$` |
| `plenora.capability.operation` | `^[a-z][a-z0-9_-]*(\.[a-z][a-z0-9_-]*)+$` |
| `plenora.operation.version` | `^[1-9][0-9]*$` |
| `plenora.input.contract` | `^plenora-[a-z0-9-]+-v[1-9][0-9]*$` |
| `plenora.message.id`, `plenora.trace.correlation_id`, `plenora.message.causation_id` | canonical lowercase hyphenated UUID (RT-012) |
| `plenora.execution.deadline` | RT-021 |
| `plenora.execution.idempotency_key` | RT-022 |

**RT-018** — The category of a rejection is the first that applies:

1. `protocol`, by RT-017;
2. `unsupported`, when every reserved value is well-formed but the capability
   name, binding version, operation, operation version or input contract does
   not agree with one available operation advertised on the runtime surface
   (RT-004, RT-011), the content type is not advertised (RT-005), or a control
   is present that the operation declares unsupported (RT-006);
3. `timeout`, when the deadline has elapsed at entry (RT-021).

A binding version such as `"2"` is well-formed and unsupported; `"01"` is
malformed.

**RT-019** — The result of a rejection carries the content type and output
contract of section 7 and a new `plenora.message.id` (RT-020). It reflects
`plenora.capability.operation`, `plenora.operation.version` and
`plenora.trace.correlation_id` only when the request carried them
well-formed, byte for byte; otherwise it omits them. It never normalizes a
received value and never supplies one the request did not carry. A request
without a canonical correlation has no originating correlation to preserve
(RT-012), so its rejection carries none. The keys the vector schema requires in
a stored error vector describe valid fixtures, not rejections of malformed
requests.

## 12. Result identity

**RT-020** — Every result, success or error, carries a new
`plenora.message.id`, distinct from the request's. A result SHOULD carry
`plenora.message.causation_id`; when present it MUST equal the request's
`plenora.message.id`, and only when that identity is canonical. A result
never copies the request's own `plenora.message.causation_id`, which names
an earlier cause.

## 13. Execution controls on the runtime

**RT-021** — `plenora.execution.deadline` is UTC: a value with a non-zero
offset, or with `-00:00` (an unknown local offset in RFC 3339), fails with
`protocol`. Senders SHOULD write `YYYY-MM-DDTHH:MM:SSZ`, with an optional
fraction of one to nine digits before `Z`, the spelling of every vector;
whether a receiver accepts the other RFC 3339 spellings of UTC is not decided
by 1.0. A deadline that has elapsed at entry, `deadline <= now`, fails with
`timeout` under RT-016. A deadline on an operation that does not advertise it
fails with `unsupported` (RT-006).

**RT-022** — `plenora.execution.idempotency_key`, when present, is a non-empty
string within the bound the operation declares; a JSON `null`, an empty string
or a value over the bound fails with `protocol`. On an operation that does not
advertise the control it fails with `unsupported` (RT-006). Its absence is not
an error: the operation runs without deduplication. Reuse of a key with
different input (SURF-012) fails with `conflict`, `phase: validate`,
`remote_effect: none`, `retry.kind: never`.

**RT-023** — On the runtime surface the deadline travels only as metadata. A
component input contract that also carries a deadline field MUST reject a
runtime request that carries both, even with equal values, with
`invalid_configuration` under RT-016.
