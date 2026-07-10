# SPEC.md

System specification for `event-sorcery`. Covers what the library is, what it
guarantees, and the design decisions behind it at a level sufficient to
understand the system without prescribing exact code. For terminology and naming
conventions, see [docs/domain.md](docs/domain.md). For usage examples and
patterns, see [docs/cqrs.md](docs/cqrs.md).

## Background

`cqrs-es` is the de-facto Rust crate for CQRS / event-sourcing. It provides the
basic vocabulary (`Aggregate`, `DomainEvent`, `View`, `Query`, `CqrsFramework`)
and a pluggable persistence layer. In production, it has several sharp edges
that have caused real bugs:

- **Infallible `apply`.** `Aggregate::apply(&mut self, event)` returns nothing.
  Aggregates that need to fail an `apply` (overflow, invariant break) have to
  either panic or stuff the error inside the aggregate state by hand. Every
  aggregate ends up with the same boilerplate.
- **Stringly-typed aggregate IDs.** `cqrs.execute("some-id", cmd)` takes `&str`,
  so passing the wrong ID type compiles fine and fails in production.
- **No schema versioning.** When the shape of an aggregate's state, events, or
  views changes, stale snapshots and views silently mis-deserialize or drift out
  of sync with the event log. There's no built-in way to detect the drift, so
  manual database surgery is required.
- **Flat command handling.** A single `handle` method receives all commands
  regardless of lifecycle state. Implementors hand-match on `(state, command)`
  tuples, making it easy to forget a case or accidentally reference state during
  initialization.

`event-sorcery` is a thin, opinionated layer that closes those gaps without
forking `cqrs-es`. The underlying framework remains the engine; we just present
a safer, more ergonomic public surface.

## Goals

- **Capture invariants in types.** Aggregate identity, lifecycle state, schema
  version — all encoded so the compiler enforces correctness.
- **Make `apply` fallible.** Domain logic that can fail at apply time (overflow,
  invariant violation) returns `Result` and the failure becomes part of the
  persisted lifecycle.
- **Detect schema drift on startup.** Bumping `SCHEMA_VERSION` is enough to
  invalidate stale snapshots/views without touching the database by hand.
- **Don't reinvent persistence.** SQLite-backed event/view repositories live in
  their own crate (`sqlite-es`) so non-event-sorcery consumers can use them too.
- **Stay backend-pluggable.** Storage is abstracted on both sides. Projections
  take a `ViewBackend` parameter; the event store, the `Store`, and the
  durable-job worker take a single `EventBackend` parameter. All default to
  SQLite, so existing call sites are untouched, while tests can swap in
  alternatives and a future Postgres/MySQL backend plugs in by implementing the
  trait — not by editing the worker or `Store` logic.

## Non-goals

- A distributed event store. The library is built around a single-writer SQLite
  database.
- A general-purpose CQRS framework. We bridge to `cqrs-es` rather than competing
  with it.
- Multi-tenancy at the framework level. Tenancy is the consumer's concern.
- Online schema migrations of the events table. Once an event type is emitted,
  its shape is permanent — version up via `event_version()` and add new variants
  instead.

## Workspace

Two crates, no application binaries:

- **`crates/sqlite-es`** — SQLite implementations of `cqrs-es`'s persistence
  traits (`PersistedEventRepository`, `ViewRepository`, plus the `SqliteCqrs<A>`
  glue type). Standalone. Usable wherever a `cqrs-es` backend is needed.
- **`crates/event-sorcery`** — the higher-level layer on top of `sqlite-es`.
  Owns the `EventSourced` trait, the `Lifecycle` adapter that bridges it to
  `cqrs-es`, the typed `Store`, projections, the schema registry, the reactor,
  and the `ViewBackend` GAT.

The canonical SQLite schema for the event/snapshot tables lives at
`migrations/20251016210348_init.sql` at the workspace root. Tests apply it
in-memory via `sqlite_es::testing::create_test_pool()`. Consumers apply the same
migration in their application database.

## Components

### `EventSourced` trait

The core abstraction consumers implement on a domain type. It collects
everything needed to event-source the type:

- `Id` — strongly-typed aggregate identifier (`Display + FromStr`)
- `Event` — domain event type (must implement `DomainEvent`)
- `Command` — input that drives state transitions
- `Error` — domain failure type (`Never` if everything is infallible)
- `Jobs` — type-level list of the durable [`Job`](#durable-jobs) types the
  entity's command handlers may enqueue (`jobs![...]`, or `Nil` for none)
- `Materialized` — `Table` if the entity has a SQLite-backed projection, `Nil`
  otherwise
- `AGGREGATE_TYPE` — stable string identifier used in the event store
- `SCHEMA_VERSION` — bumped when the entity, event, or view shape changes

It splits behavior across two pairs:

- Event-side: `originate` creates initial state from a genesis event; `evolve`
  derives new state from subsequent events. Both are fallible.
- Command-side: `initialize` handles a command when no state exists yet;
  `transition` handles a command against existing state. The split prevents
  accidentally reading "current state" while bootstrapping.

Command handlers are pure `(state, command) -> events` plus enqueues: they
receive a `JobQueue<Self::Jobs>` and may push declared jobs, but perform no side
effects themselves. Pushed jobs flush in the same transaction that commits the
events (see [ADR-0001](adrs/0001-jobs-replace-services.md)).

### `Lifecycle` adapter

`Lifecycle<Entity>` is the `pub(crate)` enum that bridges `EventSourced` to
`cqrs-es::Aggregate`. It encodes the four states an aggregate can be in:
`Uninitialized`, `Live(Entity)`, `Failed { error, last_valid_entity }`, and
intermediate transitions. A blanket
`impl Aggregate for Lifecycle<E> where
E: EventSourced` ties everything
together.

`Lifecycle` never appears in the public API. Where its presence is forced by
type-system mechanics (e.g., `ViewRepository<Lifecycle<E>, Lifecycle<E>>`), it's
hidden behind a higher-kinded-type emulation — see `ViewBackend`.

### `Store<Entity>`

The typed front door for command dispatch. Takes a strongly-typed `Id` and a
`Command`, routes through `Lifecycle` based on current state, and returns a
typed `SendError<Entity>`. Hides `cqrs-es::CqrsFramework` entirely.

### `Projection<Entity, Backend>`

The read-side. A SQLite-backed materialized view that consumers query for entity
state. Operations:

- `load(id)` — single entity by ID
- `load_all()` — every live entity
- `filter(column, value)` — typed filter on a generated column
- `catch_up()` — replay any events missed between persistence and view update
  (crash recovery)
- `rebuild(id)` / `rebuild_all()` — re-derive views from scratch

### `Reactor`

Side-effect handler keyed off events. Used for cross-aggregate orchestration
(e.g., one aggregate's event triggers a command on another). Has automatic
retry-with-backoff on optimistic-lock conflicts.

### `SchemaRegistry`

Tracks `(aggregate_type, schema_version)` tuples in a `schema_registry` table.
On startup, the wiring layer compares the persisted version against the current
`SCHEMA_VERSION` constant and, on mismatch, clears stale snapshots and replays
projections from events. No manual database intervention.

### `ViewBackend` (GAT)

A higher-kinded-type emulation that makes `Projection` generic over its storage
backend without leaking `Lifecycle` into any public bound. The default backend
is `SqliteViewBackend`. Custom backends plug in alternative storage (in-memory
for tests, Postgres in the future).

### `EventBackend`

The write-side equivalent of `ViewBackend`: the single backend a consumer
supplies for the event store, the `Store`, and the durable jobs. It is a cqrs-es
event-repository factory plus exactly two job-shaped primitives cqrs-es cannot
provide — a write-locked `claim` transaction (a generic envelope: the backend
re-reads the row and enacts a crate-side decision, naming no job type) and a
projection-only `renew`. Everything else durable jobs need — the claim/ack
compare-and-swap, the fence, retry, dead-letter, the runnable poll — is ordinary
cqrs-es: jobs are an `EventSourced` aggregate, the ack is a fenced command, and
the `job_queue` is a generic projection. `Store`/`StoreBuilder`, the durable-job
worker (`EventStoreBackend<Job, Backend>`), and `JobRuntime` are all generic
over `EventBackend`. The default is `SqliteBackend`; a Postgres/MySQL backend
implements the one trait (only dialect deltas differ). See
[ADR-0006](adrs/0006-cqrs-native-durable-jobs.md).

### Durable jobs

Side effects run as durable, at-least-once jobs instead of inline in command
handlers. A job is a serializable value implementing the `Job` trait (`Input`
dependency bundle, `Output`, `Error`, a `perform` method). Jobs are themselves
an `EventSourced` aggregate (`aggregate_type = "job"`): enqueue, claim, retry,
defer, and terminal outcomes are ordinary events, committed exactly-once, with
the runnable set materialized in the `job_queue` projection.

- **Enqueue** is transactional from a command handler (`JobQueue::push` flushes
  with the entity's events) or standalone via `JobRuntime::enqueue` (its own
  transaction; see [ADR-0007](adrs/0007-reactor-side-job-enqueue.md)).
- **Execution** happens in supervised apalis workers
  (`build_supervised_worker!`): claim with a lease, renew while running, fenced
  ack on completion. Exhausted retries or claim budgets dead-letter the job;
  terminal rows are retained.
- **Defer** (`JobOutcome::Defer`) reschedules without counting an attempt, for
  polling on an external outcome that is not ready yet.
- **`JobContext`** — every execution receives the job's id and the durable
  attempt number, so external-boundary idempotency keys can derive from stable
  framework identity.

### Durable operations

The lifecycle of a fallible external operation _on an entity_ — request it, run
it durably, feed the outcome back into entity state — is a library primitive,
not consumer plumbing (see
[ADR-0008](adrs/0008-entity-scoped-durable-operations.md)):

- **`Operation<J>`** is a library-owned state machine
  (`Idle -> Requested -> Confirmed(Output) | Failed(Error)`) embedded in entity
  state, with `OperationEvent<J>` / `OperationCommand<J>` wrappers the consumer
  nests in their own event/command enums. Requesting an operation enqueues its
  job in the same transaction that commits the `Requested` event, so there is no
  intent/call crash window. The state guard — ignore an outcome that already
  landed, refuse a second request while one is in flight — lives in the library
  machine.
- **Outcome feed-back.** An operation-driving job declares its origin entity;
  the framework maps the job result to the corresponding `OperationCommand` and
  delivers it to the origin **before** acking the job as succeeded. Failed
  delivery means no ack and redelivery; duplicates are absorbed by the state
  guard. Dead-lettered jobs deliver `Failed`, so an entity never dangles
  in-flight.
- **Submit/reconcile routing.** Whether an execution is the first try or a
  follow-up is a safety invariant for financial operations, so the framework
  routes it: the first claim runs `submit`; every later claim runs `reconcile`,
  which must determine the fate of the earlier attempt and return
  `Settled(outcome)`, `NotSubmitted` (authorizes a resubmit), or `Indeterminate`
  (defers and polls again). The routing is driven by the durable claim count on
  the job aggregate, which over-approximates submissions in the safe direction:
  an execution can never be routed to `submit` while a prior submission might
  exist. Terminal `Confirmed`/`Failed` events carry the attempt count.

### `StoreBuilder<Entity>`

Wires `Store` + `Projection` + reactors at startup using a typed-list encoding
(`Cons` / `Nil`) to enforce exhaustive wiring at compile time. Forgetting a
projection becomes a type error, not silent data staleness.

## Behavior

### Write path

1. Caller invokes `Store::send(&id, command)`.
2. `Store` looks up the aggregate, loads its `Lifecycle`, applies any relevant
   snapshot, replays uncached events.
3. `Lifecycle::handle` routes to `EventSourced::initialize` (no state) or
   `EventSourced::transition` (has state) and produces events; the handler may
   push durable jobs onto its `JobQueue`.
4. `cqrs-es::CqrsFramework` persists events with monotonic sequence numbers; the
   repository flushes any pushed jobs (their `Enqueued` events and `job_queue`
   seed rows) in the same SQL transaction.
5. Reactors registered on this aggregate are notified.

### Read path

Consumers query via `Projection::load(...)`, never by replaying events
themselves. Projections are kept up to date in the same transaction as event
persistence (where possible) or asynchronously via a reactor.

### Schema drift

On startup, `SchemaRegistry::reconcile()` compares the persisted
`schema_version` for every registered aggregate against its current
`SCHEMA_VERSION` constant:

- **Match**: nothing to do.
- **Mismatch**: snapshots are cleared (forces full event replay) and projection
  tables are truncated (rebuilt from events on first read or via `catch_up`).

### Compaction

Per-aggregate `CompactionPolicy` controls whether old events are deleted once
captured by a snapshot:

- `Retain` — events are kept indefinitely. Default; safe.
- `CompactAfterSnapshot` — events at or before the current snapshot sequence may
  be deleted, trading replay latency for storage.

Compaction never deletes events past a snapshot, and snapshots always include
the `last_sequence` they captured so partial replay still works.

## Strictness contract

The library is consumed by financial systems where silent corruption is
catastrophic. Strict invariants:

- **Events are immutable.** Once an event type ships, its shape is permanent.
  Add a new variant or version (`event_version()`); never mutate an existing
  one. Migrations on the `events` table beyond the initial creation are
  forbidden.
- **No direct writes to the events table.** All event emission goes through
  `CqrsFramework::execute` so sequence numbers, ordering, and consistency are
  framework-enforced.
- **Numeric integrity.** Arithmetic in `apply` and projections uses checked
  operations; precision loss surfaces as an error, never a silent truncation.
- **Single framework instance per aggregate** in the consuming application,
  constructed at startup; never per request.
