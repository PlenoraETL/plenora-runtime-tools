# Pinned public contracts

`source.json` pins the immutable `plenora-contracts` revision consumed by this
repository. The files under `runtime-v1/` and `capabilities-v2/` are
byte-independent JSON copies of the complete runtime conformance matrix at
that revision so private-repository network access is not required during CI.

`runtime-tools` is the transport consumer described by the common repository,
not one of the five domain component profiles. It therefore does not claim a
domain adoption manifest. Conformance evidence is owned by the public
capability codec tests and is reported in
[`docs/contract-alignment.md`](../docs/contract-alignment.md).

Do not edit a vendored vector in place. Update `source.json` to a reviewed full
commit SHA, replace the vector copies from that revision, and rerun the complete
qualification suite.
