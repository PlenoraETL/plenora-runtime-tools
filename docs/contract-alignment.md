# Common contract alignment

This repository consumes the public runtime boundary defined by
`plenora-contracts`; it is not one of the five domain component profiles. The
immutable source revision and vendored conformance vectors are recorded in
[`contracts/source.json`](../contracts/source.json). CI uses the vendored copies
so validation does not require credentials for the private contracts
repository.

## Current Runtime Binding 1.0 coverage

| Boundary | Status | Evidence or remaining work |
|---|---|---|
| Request routing | Implemented | `CapabilityMessageCodec` requires capability name/version, operation name/version, and immutable input contract. |
| Metadata preservation | Implemented at the public codec boundary | The pinned request vector round-trips content type, payload, deadline, and `plenora.trace.correlation_id`. Routing fields are removed only from the adapter-owned input view and restored on encode. |
| Contract identifiers | Implemented | `ContractId` admits only bounded lowercase `plenora-...-vN` identifiers; `OperationVersion` rejects zero. |
| Success result | Implemented | `CapabilityHandler` returns `CapabilityResponse`; discovered output contract/content type and byte bounds are checked, canonical result metadata preserves operation version, output contract and correlation UUID, and worker success requires confirmed publication through `CapabilityResultSink`. |
| Typed public error | Implemented model and mapping; terminal publication open | `PlenoraError` enforces the four axes and Typed Errors 1.0 bounds. `CapabilityFailure::with_public_error` preserves component mappings without HTTP-status inference; runtime-owned failures have stable mappings. Publishing the final error only after retry exhaustion still belongs with worker settlement. |
| Capability Discovery 2.0 | Implemented | Bounded v2 documents freeze with each registration. Dispatch checks availability, runtime surface, version, input contract, content type and supported controls before invocation. The REST profile additionally requires all five ratified operations and exact output/attribute semantics. |
| Absolute deadline | Implemented | RFC 3339 UTC deadlines are validated before invocation, raced against execution, and propagate `TaskCancellationReason::Timeout`; a post-start timeout remains `remote_effect: unknown`. |
| REST artifacts and secrets | Awaiting component-owned payload schemas | The runtime does not guess fields inside opaque JSON. Exact path/inline-secret rejection and authorized artifact source/sink resolution become implementable when `rest-tools` publishes the immutable schemas named by its profile. Message/result/error diagnostics are already payload-redacted. |

The remaining open row is an explicit release gate. Runtime-tools intentionally
does not invent REST payload fields or parse component-owned JSON heuristically.

## Updating the pin

Do not follow a branch name. Review a full `plenora-contracts` commit, replace
the vector copies from that exact revision, update the 40-character SHA in
`contracts/source.json` and the conformance test, then run the complete
workspace qualification suite.
