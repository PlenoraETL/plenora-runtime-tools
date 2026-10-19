# Schemas

These schemas are normative and use JSON Schema draft 2020-12.

| Schema | Purpose |
|---|---|
| `cli-envelope-v2.schema.json` | Common success and error envelope |
| `error-v1.schema.json` | Shared typed error axes |
| `capabilities-v1.schema.json` | Capability result carried by a success envelope |
| `capabilities-v2.schema.json` | Public interfaces and operation-level discovery |
| `row-diagnostics-v1.schema.json` | Bounded row-level evidence shared by data, database and I/O boundaries |
| `adoption-manifest-v1.schema.json` | Component-owned declaration of adoption |
| `adoption-manifest-v2.schema.json` | Adoption across Rust, CLI, Python and runtime surfaces |
| `adoption-manifest-v3.schema.json` | Superseded immutable format that introduced Python sync/async API modes |
| `adoption-manifest-v4.schema.json` | Current adoption format with Python API modes and immutable artifact identity |
| `public-catalog-v1.schema.json` | Normative target operations and boundary contracts per component |
| `runtime-negotiation-vector-v1.schema.json` | The negotiation between Runtime Binding 1.0 and 2.0 at the selection of the destination (RT-026) |
| `public-catalog-v2.schema.json` | The same shape for catalogs of Public Catalogs 2.0 (`contract` `plenora-public-catalog-v2`) |
| `operation-registry-v1.schema.json` | Stable embedded operation identities, including data kernels |
| `surface-bindings-v1.schema.json` | Exact CLI, Python SDK and runtime entrypoint mappings |
| `composition-v1.schema.json` | Cross-component direct and adapter-required handoffs |
| `arrow-metadata-vector-v1.schema.json` | Arrow metadata conformance fixtures |
| `interop-vector-v1.schema.json` | Interoperability vectors: chains of public operations and their exact expectations |
| `arrow-data-vector-v1.schema.json` | Arrow schema fixtures with row values and the verdict the vocabulary gives |
| `runtime-vector-v1.schema.json` | Runtime request, success and error fixtures |
| `runtime-probe-v1.schema.json` | Runtime rejection probes: one request mutation and its expected rejection |
| `rest-arrow-adapter-v1.schema.json` | Declaration of an adapter from a REST execution result to an Arrow table |
| `rest-arrow-adapter-vector-v1.schema.json` | REST-to-Arrow adapter fixtures: declaration, REST result and expected outcome |
| `plan-budget-v1.schema.json` | Plan-format versions and declared memory-budget fragment |
| `data-plan-v1.schema.json` | Data-tools plan format `plenora-data-plan-v1` (Data Plan 1.0) |
| `data-execution-input-v3.schema.json` | Request of data-tools `data.run` version 3: plan, artifact sources and sinks |
| `data-execution-result-v3.schema.json` | Result of data-tools `data.run` version 3: manifest of the published artifacts |

Schema identifiers are immutable. Modify a schema in place only for a change
that cannot alter whether an existing instance validates. Otherwise add a new
version. The single exception is a declared erratum before adoption
([COMPATIBILITY.md](../COMPATIBILITY.md#errata-before-adoption)).

Manifest v1 is retained for the historical CLI/SDK-only scope; manifests v2
and v3 remain immutable. New component adoption uses manifest v4.

The shared CLI and capability version patterns retain their published grammar;
they are not complete SemVer validators. The compatibility constraints and
proposed successor grammar are documented in
[the version-syntax proposal](../proposals/version-syntax.md).
