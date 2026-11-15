# 0014. Replayable multi-aggregate projections

- Status: Proposed
- Date: 2026-11-15
- Issues: [#133](https://github.com/dataclique/event-sorcery/issues/133),
  [#134](https://github.com/dataclique/event-sorcery/issues/134),
  [#135](https://github.com/dataclique/event-sorcery/issues/135)

## Context

Aggregate-state materializations currently occupy the public `Projection` name.
They mirror one aggregate instance per row, whereas general read models can
combine events from several aggregate types. Such read models need reproducible
replay without repeating external effects or consulting present-day state.

The event store orders events within an aggregate stream, not across streams. An
aggregate sequence, timestamp or independently allocated database sequence
cannot by itself establish a safe cross-stream consumption checkpoint.

## Decision

### Vocabulary and public API

Rename the existing aggregate-state API to `MaterializedView`, including related
source, examples and documentation. A materialized view is a persisted
projection; an aggregate-state materialized view is one specialization. Make the
rename a breaking release with a package version bump and no aliases or
deprecated old entry points. Preserve physical table names and existing stored
data.

Reserve `Projection` for a separate synchronous, deterministic state-fold trait.
Keep `Reactor` asynchronous for effectful reactions. Do not add an optional
associated projection type or change the return type of `Reactor::react`.

The intended caller-facing shape is:

```rust,ignore
pub trait Projection: Dependent {
    type Payload: Default + Serialize + DeserializeOwned + Send + Sync;
    type Error: std::error::Error + Send + Sync;

    fn apply(
        &self,
        current: &Self::Payload,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<Self::Payload, Self::Error>;
}
```

`Default` defines the empty payload; no separate `initial` method is required.
Payloads requiring initialization can represent that state explicitly. The fold
returns the next payload or a typed failure. Its inputs must include all facts
needed to reproduce the result: no network calls, command dispatch, wall-clock
reads or present-day aggregate queries during replay. Version any configuration
that changes the fold's meaning.

A framework-owned asynchronous adapter loads state, invokes the fold and commits
its output. It may integrate with reactor notifications, but live callback order
is not the durable processing order. Ordinary effectful reactors are never run
as part of a projection rebuild.

### Authoritative event order

Add an ordered companion feed referencing existing event identities without
creating a second event payload authority or changing the existing events-table
schema. Event writes and their ordering entries commit in the same database
transaction through the existing writer.

Serialize feed appends with a database-wide transaction-scoped append lock held
until commit or rollback. PostgreSQL must acquire that lock before allocating
feed positions; SQLite uses its serialized writer transaction. All relevant
writer entry points follow the same lock order. Transaction B cannot expose a
later position while transaction A can still commit an earlier position.
Positions may have gaps; consumers require a stable committed prefix, not dense
numbering. Allocation by a PostgreSQL sequence alone does not provide this
property.

Ordering entries are authoritative event-store metadata, not disposable derived
state. Their references and required event history must survive compaction for
as long as a supported projection depends on them. An incomplete history must
produce a typed refusal rather than an apparently successful partial rebuild.

Existing history has no recorded cross-stream order. Upgrades must require an
explicit historical bootstrap policy or refuse activation; they must not invent
original commit order from aggregate sequences, timestamps or query row order.
The exact bootstrap policy remains a migration decision, not an implementation
default.

### Projection persistence and recovery

Each projection instance has a stable identity and a caller-owned `*_projection`
table containing its opaque JSON payload, projection schema version, optimistic
write version and consumed feed position. These values have separate meanings;
none substitutes for an aggregate sequence or snapshot counter. Physical
metadata representation and instance-key routing are defined by the
implementation contract before coding.

Consume relevant events in feed order. Commit the next payload and checkpoint
atomically, fencing competing processors. Retries cannot apply an already
checkpointed event twice. Transformation errors and failures or cancellation
before commit advance neither payload nor checkpoint. Cancellation or connection
loss during commit can leave the outcome uncertain; reread durable state before
retrying and never report uncertain commit as confirmed failure or success.
Payload and checkpoint always advance atomically.

Catch-up continues from a durable checkpoint. Rebuild folds retained history
from the empty payload through a captured committed feed boundary. Publish the
rebuilt state and its checkpoint atomically; a failed rebuild must not expose
partially rebuilt state as complete. Ordinary reactor side effects are excluded.

## Alternatives Considered

### Put projection payloads on the asynchronous Reactor trait

- Pros: One event-handling trait and familiar dependency declarations.
- Cons: Conflates deterministic replay with arbitrary asynchronous effects and
  changes every ordinary reactor's state and result contract.
- Rejected because: Replay must not repeat external effects or depend on I/O.

### Track only aggregate-local sequences

- Pros: Reuses current stream identities without serializing event appends.
- Cons: Does not define reproducible interleaving across aggregate types.
- Rejected because: Order-sensitive accounting requires repeatable consumption.

### Use timestamps or uncoordinated global sequence allocation

- Pros: Simple allocation and low append contention.
- Cons: Timestamps do not establish a unique commit order; sequence allocation
  can expose a later checkpoint before an earlier transaction commits.
- Rejected because: Consumers could permanently skip committed events.

## Consequences

The public vocabulary distinguishes general projections from aggregate-state
materializations. The synchronous trait keeps replay independent of reactor side
effects, while the existing engine and transaction boundaries remain the only
persistence authority.

Serialized feed appends trade write concurrency for a straightforward ordering
guarantee. Ordering metadata requires backup, migration and retention
protection. Historical activation cannot be automatic without an explicit
bootstrap policy.

This decision does not implement the rename, the feed or the projection runtime.
It does not introduce accounting-specific arithmetic, foreign-binding expansion,
external-effect replay or a separate job queue. SQLite and PostgreSQL need real
storage tests for commit ordering, atomicity, cancellation and recovery.
