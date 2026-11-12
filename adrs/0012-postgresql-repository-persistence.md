# ADR-0012: PostgreSQL persistence through CQRS repositories

## Status

Accepted direction: extend the existing cqrs-es repository architecture.

Issue:
[PostgreSQL command persistence](https://github.com/dataclique/event-sorcery/issues/120).

## Context

The framework must commit aggregate events, snapshots, dispatched-job events and
queue seeds atomically on PostgreSQL while preserving SQLite behavior. The job
event stream remains authoritative; database selection must not duplicate its
lifecycle decisions.

`cqrs-es` separates generic `PersistedEventStore` execution from concrete
`PersistedEventRepository` implementations. Read models use `ViewRepository`
separately. This repository's `sqlite-es` crate follows that architecture, and
`event-sorcery` extends its persistence transaction with library-prepared jobs.

The published `postgres-es` 0.5.0 uses SQLx 0.8, while this workspace uses SQLx
0.9. Upstream revision `ec21997dc182689def488787bd6c9b5352cf9397` uses SQLx 0.9,
but both versions keep transaction-writing helpers crate-private and commit
inside `persist`. Neither exposes a public transaction extension for our job
writes.

## Decision

PostgreSQL extends the existing repository and backend boundaries. Shared
execution remains generic over concrete persistence; database-specific
repositories own their SQL and transactions. Event validation, job preparation
and lifecycle decisions remain shared library code. SQLite constructors and
existing external backend implementations remain compatible.

A PostgreSQL repository must own the complete persistence transaction, including
job writes. Wrapping an independently committed event write with a later queue
write does not satisfy this requirement. Implementing the public cqrs-es
repository contract does not require copying or forking upstream source.

Maintained upstream components may be reused only where their public contracts
preserve that atomic boundary and dependency compatibility. No upstream source
is vendored, and no fork is established by this decision.

PostgreSQL schema migrations belong with PostgreSQL persistence. SQLite's
existing migration files and embedded migrator remain intact. Local integration
tests use devenv's built-in PostgreSQL service; pure Nix checks provision an
isolated database lifecycle independently.

## Alternatives considered

### A database-switching execution engine

- Pros: one runtime-selected entry point could dispatch database operations.
- Cons: introduces another backend-selection mechanism beside the existing
  repository traits and risks duplicating framework decisions.
- Rejected because: concrete repositories already provide the required variation
  without another execution architecture.

### Compose upstream persist with a subsequent enqueue

- Pros: uses the upstream repository without changing its internals.
- Cons: events and snapshots can commit even when job persistence fails.
- Rejected because: two committed transactions cannot provide command-level
  atomicity.

### Copy or maintain a fork of postgres-es

- Pros: its private transaction helpers could be extended directly.
- Cons: creates an upstream maintenance obligation and requires separate fork
  authority; copying also obscures source ownership.
- Rejected because: the public repository contract can be implemented without
  taking ownership of upstream code.

## Consequences

- Domain code and durable job semantics do not change with the database.
- PostgreSQL SQL and migration behavior require real-database tests for atomic
  success, rollback, optimistic conflicts and competing writes.
- Persistence support does not imply worker, startup, recovery or compaction
  support. Backend contracts must not be satisfied with success-returning stubs.
- Framework-owned PostgreSQL persistence code is maintained here; upstream
  postgres-es fixes do not automatically change that code.
