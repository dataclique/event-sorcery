# ADR-0010: One engine with multiple language bindings

## Status

Accepted for 0.4.0.

## Context

The Rust and Haskell repositories currently implement persistence and runtime
semantics independently. That duplicates storage, fencing, job, checkpoint,
schema, and delivery behavior and makes semantic drift a permanent risk. The
Rust workspace is the authoritative project that will carry the combined
implementation.

## Decision

`event-sorcery` becomes a monorepo containing one Rust engine, its native Rust
typed binding, and an idiomatic Haskell binding over a versioned C ABI. The
erased facade is the only production writer path. Framework payloads use the
deterministic CBOR contract and shared conformance corpus defined by `SPEC.md`.

The migration is compulsory. The replacement is the 0.4.0 monorepo stack. The
cutover gate is the complete Rust, ABI, Haskell, Nix, conformance, and benchmark
suite against the real engine. After the replacement stack exists remotely as
draft pull requests and the gate is green, the separate `event-sorcery-hs`
GitHub repository is deleted. The local clone is retained until deletion is
verified and can recreate the remote if rollback is required.

## Consequences

- Engine fixes and optimizations land once and reach every binding.
- The C ABI, deterministic encodings, DDL, and conformance vectors become public
  contracts and require explicit versioning discipline.
- Language bindings own domain folds and orchestration but no production
  persistence decisions.
- 0.4.0 is an intentional breaking release; the pre-0.4 Rust storage surface and
  the standalone Haskell engine are not maintained in parallel.
