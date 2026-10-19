# Plenora Public Security Contract 1.0

Status: normative

Contract identifier: `plenora-public-security-v1`

This contract governs security behavior observable at a public boundary. It
does not prescribe secret stores, TLS libraries or internal data structures.

## 1. Secure defaults

**SEC-001** — Network surfaces MUST verify remote identity by default.
Insecure TLS, private-network access, redirects, proxies, custom methods and
local file access MUST be explicit opt-ins when applicable.

**SEC-002** — Unsupported or disallowed access MUST fail closed before the
external side effect starts.

## 2. Secret inputs

**SEC-003** — CLI arguments MUST NOT carry secret values. A CLI accepts a
reference to an environment variable, protected file, standard-input channel or
external secret provider.

**SEC-004** — Serialized runtime requests and persistable plans MUST carry
secret or connection references, not credentials, tokens, authorization
headers, private keys or inline PEM material.

**SEC-005** — An in-process SDK MAY accept a secret value through an explicitly
typed secret parameter. Its public representation, errors and diagnostics MUST
remain redacted.

## 3. Public output

**SEC-006** — Capability documents, results, errors, progress and diagnostics
MUST be safe to persist in CI or orchestration logs.

**SEC-007** — Public output MUST NOT contain credentials, tokens, DSNs,
authorization headers, private key material, bound SQL, source rows, unbounded
remote payloads or private local paths.

**SEC-008** — Public messages and representations MUST be bounded. Payload
previews are absent by default and, when an operation explicitly exposes one,
are size-limited, content-type-aware and redacted.

## 4. Resource selection

**SEC-009** — Access to a local path, network range, database, provider or
storage location MUST be selected through an explicit public input or capability.
It MUST NOT be enabled by hidden fallback.

**SEC-010** — Capability discovery MAY describe that a protected resource class
is supported. It MUST NOT reveal resource addresses, account names or secret
identifiers.

## 5. Equivalent surfaces

**SEC-011** — The same operation version has equivalent security defaults across
Rust, CLI, Python and runtime surfaces. A convenience surface MUST NOT silently
weaken verification or broaden resource access.

## 6. Verification

Black-box conformance includes secret canaries in every accepted secret channel
and verifies that they do not appear in successful output, failures, debug
representations, capability discovery or diagnostics.
