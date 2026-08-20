# Generic capability integration

`plenora-runtime-capabilities` is the stable connection point between generic runtime mechanics and
application-owned Rust libraries. Runtime crates do not import `data-tools`, `database-tools`,
`IO-tools`, or future foundation crates.

## Registration model

The embedding consumer constructs a bounded registry during startup and then freezes it. Every
identity contains a lowercase ASCII namespace and a positive wire version, for example
`plenora.data-tools@v1`. Every operation is also the complete namespaced public identifier, such as
`data.run`; local aliases such as `run` are rejected. One handler may route multiple validated
operations for that library.

```rust,ignore
let discovery = CapabilityDiscovery::from_json(component.capabilities_v2()?)?;
let mut builder = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(16)?)?;
builder.register_discovered(discovery, RestToolsAdapter::new(rest_tools))?;

let dispatcher = CapabilityDispatcher::with_result_sink(
    builder.build(),
    CapabilityDispatcherConfig::new(1024 * 1024)?,
    confirmed_result_producer,
)?;
```

`CapabilityDispatcher` implements `WorkerHandler<CapabilityRequest>`, so the same value plugs into
the engine-neutral worker executor or the Apalis broker runner. Registry lookup performs no task
spawn and no runtime mutation. Dynamic worker futures are still admitted only up to the worker's
`max_in_flight` bound.

## Adapter ownership

Concrete libraries do not implement the runtime trait and do not depend on runtime-tools. The
consumer owns the translation wrapper:

For a complete example that is compiled and tested with the workspace, use
[`examples/capability-adapter`](../examples/capability-adapter/README.md). The following abbreviated
fragment only illustrates ownership:

```rust,ignore
struct DataToolsAdapter {
    tools: DataTools,
}

#[async_trait]
impl CapabilityHandler for DataToolsAdapter {
    async fn invoke(
        &self,
        context: WorkerContext,
        request: CapabilityRequest,
    ) -> Result<CapabilityResponse, CapabilityFailure> {
        // Validate operation/content type, decode into data-tools types, forward cancellation and
        // progress, call the library, then classify any concrete error explicitly.
        Ok(CapabilityResponse::new(output_contract, serialized_output))
    }
}
```

The handler returns a serialized public result or a classified failure. The dispatcher validates
the result against frozen discovery metadata, adds canonical Runtime Binding metadata and publishes
it through a broker-neutral `CapabilityResultSink`. A missing sink, incompatible output, failed
publication or ambiguous publication outcome cannot become a successful worker acknowledgement.

Adding a fourth library means adding another wrapper and startup registration. NATS, Apalis, HTTP,
worker execution, heartbeat, retry, DLQ, and shutdown code do not change.

## Wire contract and bounds

`CapabilityMessageCodec` stores the Runtime Binding 1.0 request identity in five
reserved portable metadata keys:

- `plenora.capability.name`;
- `plenora.capability.version`;
- `plenora.capability.operation`;
- `plenora.operation.version`;
- `plenora.input.contract`.

`CapabilityRequest` exposes the operation version through `OperationVersion` and
the immutable input schema through `ContractId`. The codec removes the five reserved keys before
handing the opaque input to an adapter and preserves all other metadata, including
`plenora.trace.correlation_id` and supported execution controls. Payload bytes and content type
remain transport-neutral. The dispatcher rejects
payloads above its configured bound before lookup or invocation. The registry defaults to 64
handlers, has a hard maximum of 4,096, rejects duplicates, and exposes no mutation after `build()`.

Discovery registration is mandatory for `plenora.rest-tools@v1`. Its profile requires
`rest.test`, `rest.generate`, `rest.enrich`, `rest.download` and `rest.upload`, all on the runtime
surface. Requests are rejected before invocation when operation status/version, input contract,
JSON envelope or advertised controls differ. Absolute UTC deadlines cancel the invocation token;
a timeout after invocation may have begun remains an unknown remote outcome.

The request-vector evidence and immutable contracts revision are documented in
[`contract-alignment.md`](contract-alignment.md). Confirmed success-result transport is implemented.
Typed public errors enforce category, phase, remote effect, retry and bounded details; terminal
error publication after retry exhaustion remains coupled to worker settlement and is still a gate.

Unknown capabilities and oversized input are safe dead-letter classifications with
`NotStarted` remote effect. Adapter failures preserve their concrete source while requiring an
explicit retry class and effect certainty. `OutcomeUnknown` therefore retains the existing
fail-closed retry behavior.

Runtime-tools treats payload JSON as component-owned. It will add artifact-reference and
secret-reference extraction only from the released REST payload schemas; it will not scan JSON for
guessed field names. This keeps the runtime a black-box orchestrator rather than an HTTP client.

## Testing

`FakeCapabilityHandler` lives in `plenora-runtime-testkit`. It records only payload-free routing,
identity, attempt, and byte-count observations. Invocation history and FIFO scripted outcomes are
both bounded. Consumers can test new adapters and routing without NATS or any unfinished concrete
library. The per-adapter release checklist is in
[`integrating-a-library.md`](integrating-a-library.md).
