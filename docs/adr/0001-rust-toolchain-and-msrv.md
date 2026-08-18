# ADR 0001: Rust development toolchain and MSRV

- Status: Accepted
- Date: 2026-08-15

## Context

The host does not currently provide Cargo or rustc. Builds must still be reproducible. The selected
v0.1 dependency graph has now been qualified on the pinned toolchain, so releases need an explicit
and testable minimum supported Rust version.

## Decision

Use Rust 1.97.1 as both the pinned development/CI toolchain and the conservative v0.1 MSRV. Local
validation may run through the official `rust:1.97.1-slim-bookworm` container. Every workspace
package inherits `rust-version = "1.97.1"`, and CI qualifies that exact toolchain.

Lowering the MSRV requires a separate same-SHA qualification of all workspace, documentation,
dependency-policy, and real-NATS gates before changing this ADR and package metadata.

## Consequences

Development builds and the supported compiler floor are deterministic. This deliberately makes no
claim that older compilers work; lowering the floor is a future compatibility work item.
