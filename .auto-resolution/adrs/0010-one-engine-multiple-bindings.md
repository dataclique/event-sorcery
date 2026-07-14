# ADR-0010: One engine with multiple language bindings

## Status

Accepted for 0.4.0.

## Context

The Rust repository already provides the authoritative implementation on top of
`cqrs-es`, Apalis, `sqlite-es`, SQLx, and SQLite. The separate Haskell
repository reproduces persistence and runtime decisions independently. That
duplicates storage, fencing, job, checkpoint, schema, and delivery behavior and
makes semantic drift a permanent risk.

## Decision

`event-sorcery` becomes a monorepo containing the existing Rust implementation,
its native Rust typed binding, and an idiomatic Haskell binding over a versioned
C ABI.

The shared engine is extracted from the current implementation. It continues to
use `cqrs-es`, Apalis, `sqlite-es`, SQLx, SQLite, and the current job and
dispatch protocols. The extraction must generalize and reuse existing writer
paths; it must not create a parallel event store, job model, queue, or set of
durable decisions.

The erased facade becomes the only production writer path. Both the existing
Rust typed API and the C ABI lower through it. Deterministic CBOR is the ABI
interchange encoding, not a replacement for the existing persistence format. The
shared conformance corpus is defined by `SPEC.md`.

The migration is compulsory. The replacement is the 0.4.0 monorepo stack. The
cutover gate is the complete Rust, ABI, Haskell, Nix, conformance, and benchmark
suite against the real engine. After the replacement stack exists remotely as
draft pull requests and the gate is green, the separate `event-sorcery-hs`
GitHub repository is deleted. Before deletion, its Git refs, issues, pull
requests, releases, repository settings, and protection configuration are
exported, and the restoration runbook is verified against a disposable
repository. The export and local clone are retained until rollback is closed by
an explicit owner decision; a local clone alone is not a remote-repository
backup.

## Consequences

- Engine fixes and optimizations land once and reach every binding.
- The existing Rust public surface remains supported through the extracted
  facade unless a separate, documented 0.4.0 compatibility decision changes it.
- The C ABI, deterministic interchange encodings, generated header, and
  conformance vectors become public contracts and require explicit versioning
  discipline.
- Language bindings own domain types, codecs, pure folds and handlers, job
  bodies, and language-native iteration adapters. They own no production
  persistence or durable transition decisions.
- The Haskell memory and SQLite production backends and duplicated framework
  decisions are removed rather than maintained beside the Rust engine.
- New storage backends, job protocols, and persistence formats remain outside
  this decision and require their own specification.
