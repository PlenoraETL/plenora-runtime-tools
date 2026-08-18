# Generic capability integration

`plenora-runtime-capabilities` is the stable connection point between generic runtime mechanics and
application-owned Rust libraries. Runtime crates do not import `data-tools`, `database-tools`,
`IO-tools`, or future foundation crates.

## Registration model

The embedding consumer constructs a bounded registry during startup and then freezes it. Every
identity contains a lowercase ASCII namespace and a positive wire version, for example
`plenora.data-tools@v1`. One handler may route multiple validated operations for that library.

```rust,ignore
let mut builder = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(16)?)?;
builder.register(
    CapabilityId::new("plenora.data-tools", 1)?,
    DataToolsAdapter::new(data_tools),
)?;
builder.register(
    CapabilityId::new("plenora.database-tools", 1)?,
    DatabaseToolsAdapter::new(database_tools),
)?;

let dispatcher = CapabilityDispatcher::new(
    builder.build(),
    CapabilityDispatcherConfig::new(1024 * 1024)?,
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
    ) -> Result<(), CapabilityFailure> {
        // Validate operation/content type, decode into data-tools types, forward cancellation and
        // progress, call the library, then classify any concrete error explicitly.
        Ok(())
    }
}
```

The handler is command-oriented and returns success or a classified failure. If a concrete library
produces data, its application adapter also owns the appropriate result side effect—for example an
application result publisher or repository. The generic dispatcher never silently discards a
library result and does not prescribe where application outputs are stored.

Adding a fourth library means adding another wrapper and startup registration. NATS, Apalis, HTTP,
worker execution, heartbeat, retry, DLQ, and shutdown code do not change.

## Wire contract and bounds

`CapabilityMessageCodec` stores routing in three reserved portable metadata keys:

- `plenora.capability.name`;
- `plenora.capability.version`;
- `plenora.capability.operation`.

The codec removes those routing keys before handing the opaque input to an adapter and preserves
all other metadata. Payload bytes and content type remain transport-neutral. The dispatcher rejects
payloads above its configured bound before lookup or invocation. The registry defaults to 64
handlers, has a hard maximum of 4,096, rejects duplicates, and exposes no mutation after `build()`.

Unknown capabilities and oversized input are safe dead-letter classifications with
`NotStarted` remote effect. Adapter failures preserve their concrete source while requiring an
explicit retry class and effect certainty. `OutcomeUnknown` therefore retains the existing
fail-closed retry behavior.

## Testing

`FakeCapabilityHandler` lives in `plenora-runtime-testkit`. It records only payload-free routing,
identity, attempt, and byte-count observations. Invocation history and FIFO scripted outcomes are
both bounded. Consumers can test new adapters and routing without NATS or any unfinished concrete
library. The per-adapter release checklist is in
[`integrating-a-library.md`](integrating-a-library.md).
