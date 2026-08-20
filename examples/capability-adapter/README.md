# Generic Rust library adapter

This compile-tested example is the template for connecting `data-tools`, `database-tools`,
`IO-tools`, or a future Rust library without adding that dependency to a runtime crate.

The simulated `ExampleLibrary` stands in for the concrete library. The application-owned
`ExampleLibraryAdapter`:

- exposes a versioned `CapabilityId`, positive operation version, immutable input contract, and
  validates the operation and content type;
- passes opaque input to the concrete API only after validation;
- bridges task cancellation and reports bounded numeric progress;
- maps concrete failures to an explicit retry class and remote-effect certainty;
- is registered once at startup in an immutable, bounded registry.

Replace `ExampleLibrary` and `LibraryError` only after the real library API is stable. Keep the
adapter in the final Plenora consumer, not in `runtime-tools` and not in the concrete library.

Run it from the workspace root:

```text
cargo run -p plenora-example-capability-adapter
cargo test -p plenora-example-capability-adapter
```

The tests are an initial adapter contract: success reaches the library, invalid operations do not,
transient failures remain retryable, and uncertain external effects are never blindly retried.
The integration checklist in [`docs/integrating-a-library.md`](../../docs/integrating-a-library.md)
defines the additional tests required for a real adapter.
