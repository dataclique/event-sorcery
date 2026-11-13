# Event Sourcing with event-sorcery

Quick reference for event-sourcing patterns in this codebase. The
`event-sorcery` crate provides the primary interface; cqrs-es is an
implementation detail hidden behind it.

## Core Principle: Events Are Immutable

**Persisted events are immutable and cannot be changed or rewritten.** Audit
streams retain their complete history. Only observational aggregates with an
explicit [compaction policy](#event-compaction) may prune snapshot-covered
history through the framework. Changes to domain facts use compensating events,
not edits to past events.

- **Commands**: Can add, remove, or change freely
- **Entities**: Can restructure, add fields, change logic freely
- **Projections**: Can add, drop, restructure freely (just replay from events)
- **Events**: Immutable facts with explicitly selected history retention. Think
  carefully before adding new event types.

## Architecture

```text
Domain type          Adapter             cqrs-es (hidden)
+--------------+     +----------------+  +------------+
| impl         | --> | Lifecycle      |  | Aggregate  |
| EventSourced |     | (blanket impl) |--| trait      |
+--------------+     +----------------+  +------------+
                            |
                     +------+------+
                     | Store       |
                     | (typed IDs, |
                     |  send())    |
                     +-------------+
```

Consumers implement `EventSourced`. `Lifecycle` bridges to cqrs-es
automatically. `Store` provides type-safe command dispatch with strongly-typed
IDs.

## CQRS dependency for external backends

This revision uses a maintained `cqrs-es` fork for explicit snapshot
checkpoints, pinned to `d61e05290bc7b24af1f615bc828a74ae6fc22f25`. Cargo treats
registry and Git packages as different identities even when both have version
`0.5.0`. Mixing them gives different `PersistedEventRepository` traits and
`SerializedEvent` types at the `EventBackend` boundary.

An application with a registry-based 0.5.0 backend must align its dependency
graph in its **root** `Cargo.toml`:

```toml
[dependencies]
cqrs-es = "=0.5.0"

[patch.crates-io]
cqrs-es = { git = "https://github.com/0xgleb/cqrs", rev = "d61e05290bc7b24af1f615bc828a74ae6fc22f25" }
```

A patch inside a dependency's manifest is ignored by the consuming application.
Align any direct Git dependencies to the same revision too, update the
application lockfile, and run `cargo tree -i cqrs-es` from the application root.
The graph must resolve one CQRS package identity across the framework and
backend. Existing backend method signatures do not need to change, but this
manifest migration is required when moving from registry dependencies. The
[simple example](../examples/simple/Cargo.toml) demonstrates that setup.

Package alignment fixes type identity, not a legacy writer's snapshot semantics.
The provided `persist_with_snapshot` hook carries a `SerializedSnapshot`:
`current_sequence` is the covered checkpoint and `current_snapshot` is the
independent snapshot-version counter. Custom repositories must preserve these
fields when implementing the hook. Its default adapter retains the old `persist`
signature but cannot forward the checkpoint through that legacy tuple. The
event-sorcery SQLite and PostgreSQL repositories override the hook; custom
repositories must not assume that the Cargo patch alone makes their snapshots
prefix-aware.

See [ADR-0013](../adrs/0013-snapshot-checkpoint-dependency.md) for the
dependency and compatibility decision.

## Implementing a New Entity

### 1. Define the Domain Type

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MyEntity {
    // domain state
}
```

### 2. Define Events and Commands

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MyEntityEvent {
    Created { /* fields */ },
    Updated { /* fields */ },
}

impl DomainEvent for MyEntityEvent {
    fn event_type(&self) -> String { /* e.g., "MyEntityEvent::Created" */ }
    fn event_version(&self) -> String { "1.0".to_string() }
}

pub enum MyEntityCommand {
    Create { /* fields */ },
    Update { /* fields */ },
}
```

### 3. Implement EventSourced

```rust
#[async_trait]
impl EventSourced for MyEntity {
    type Id = MyEntityId;       // strongly-typed, Display + FromStr
    type Error = Never;         // or a thiserror type
    type Command = MyEntityCommand;
    type Event = MyEntityEvent;
    type Materialized = Table;  // Table for projected, Nil for non-projected
    type Jobs = Nil;            // or jobs![SendEmail, ChargeCard]

    const PROJECTION: Table = Table("my_entity_view");
    const SCHEMA_VERSION: u64 = 1;
    const SNAPSHOT_SIZE: usize = 10;
    const AGGREGATE_TYPE: &'static str = "MyEntity";
    const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;

    // Event-side: reconstruct state from events
    fn originate(event: &Self::Event) -> Option<Self> { /* ... */ }
    fn evolve(entity: &Self, event: &Self::Event)
        -> Result<Option<Self>, Self::Error> { /* ... */ }

    // Command-side: decide -- domain events, or one job dispatch
    async fn initialize(command: Self::Command)
        -> Result<Effect<Self>, Self::Error> { /* ... */ }
    async fn transition(&self, command: Self::Command)
        -> Result<Effect<Self>, Self::Error> { /* ... */ }
}
```

Every entity chooses `SNAPSHOT_SIZE` and `COMPACTION_POLICY` explicitly. The
interval `10` above is a selected value, not a library fallback.

`Jobs` is the type-level list of `Job`s the entity may dispatch -- written with
the `jobs!` macro (`jobs![ChargeCard, SendReceipt]`), or `Nil` for none. A
handler kicks a job off by returning
`Effect::Dispatch(self.field.dispatch(job)?)`; the framework emits the
`Dispatched` event and enqueues in the same transaction, and only its delivery
path can settle the dispatch. See the **Jobs Pattern** and **Running Jobs**
sections below.

**Method naming conventions:**

| Method       | Purpose                                | Theme         |
| ------------ | -------------------------------------- | ------------- |
| `originate`  | Create initial state from first event  | Evolution     |
| `evolve`     | Derive new state from subsequent event | Evolution     |
| `initialize` | Handle command when no state exists    | State machine |
| `transition` | Handle command against existing state  | State machine |

## Key Types

| Type                   | Purpose                                      |
| ---------------------- | -------------------------------------------- |
| `EventSourced`         | Core trait -- implement on domain types      |
| `Store<Entity>`        | Type-safe command dispatch                   |
| `StoreBuilder<Entity>` | Wires reactors/projections, builds Store     |
| `Projection<Entity>`   | Read-side materialized view                  |
| `Reactor<Entity>`      | Event side-effect handler                    |
| `SendError<Entity>`    | Error from `Store::send()`                   |
| `LifecycleError<E>`    | Errors from event application                |
| `Never`                | Error type for infallible entities           |
| `DomainEvent`          | Trait for event serialization (from cqrs-es) |
| `Table`                | Newtype for projection table name            |
| `Nil`                  | Empty type-level list (no projection / jobs) |
| `Effect<Entity>`       | Handler result: events, or one job dispatch  |
| `fx(value)`            | Wraps events/job/dispatch/error into result  |
| `uneventful()`         | No-op outcome: accept command, no events     |
| `Job`                  | Entity-kicked job (submit/reconcile)         |
| `StandaloneJob`        | Origin-less worker job (perform)             |
| `DispatchedJob<J>`     | Entity-embedded state of a kicked-off job    |
| `DispatchEvent<J>`     | Dispatch lifecycle facts on the entity       |
| `DispatchOutcome<J>`   | Sealed verdict delivered by the framework    |
| `JobDispatch<Entity>`  | Opaque kick-off returned in an `Effect`      |
| `Settled<Output>`      | Sealed success payload (output + attempts)   |
| `SettledFailure<E>`    | Sealed failure payload (reason + attempts)   |
| `DispatchRefused`      | Guard refusal (in-flight, mismatch)          |
| `Reconciliation<T>`    | reconcile verdict: settled / not / unknown   |
| `OriginPort<J>`        | Delivery port for verdicts (impl by `Store`) |
| `JobInput<J>`          | Worker input: deps + origin delivery port    |
| `jobs!`                | Macro building the type-level `Job` list     |
| `JobRuntime`           | Wires the worker-side durable-jobs runtime   |

## Sending Commands

```rust
let store: Store<Position> = /* built by StoreBuilder */;

let symbol = Symbol::new("AAPL").unwrap();
store.send(&symbol, PositionCommand::AcknowledgeFill { /* ... */ }).await?;
```

`Store::send()` routes based on lifecycle state:

- Uninitialized -> `Entity::initialize`
- Live -> `Entity::transition`
- Failed -> returns the stored error

## Reading State via Projections

Production code reads entity state through `Projection`, never by loading
aggregates directly:

```rust
// Projection is returned by StoreBuilder::build() for Table entities
let (store, projection) = StoreBuilder::<Position>::new(pool)
    .build()
    .await?;

// Load by typed ID
let position: Option<Position> = projection.load(&symbol).await?;
```

Projections are materialized views stored in SQLite tables (named by
`PROJECTION` constant). `StoreBuilder::build()` automatically creates and wires
projections for entities with `type Materialized = Table`.

### Filtered Queries with Columns

```rust
const STATUS: Column = Column("status");

let pending_orders: Vec<OffchainOrder> = projection
    .load_where(STATUS, "Pending")
    .await?;
```

## Wiring: StoreBuilder

`StoreBuilder` wires reactors to a `Store` at startup and auto-wires projections
based on the entity's `Materialized` type. It uses type-level linked lists
(`Cons`/`Nil`) to ensure all required processors are wired at compile time.

For **projected entities** (`type Materialized = Table`), `build()` returns
`(Arc<Store>, Arc<Projection>)`:

```rust
let (store, projection) = StoreBuilder::<Position>::new(pool)
    .with(rebalancing_trigger)  // wire a reactor
    .build()
    .await?;
```

For **non-projected entities** (`type Materialized = Nil`), `build()` returns
`Arc<Store>`:

```rust
let store = StoreBuilder::<OnChainTrade>::new(pool)
    .build()
    .await?;
```

Projections are created and wired automatically - no manual
`Projection::sqlite()` or `.with(projection)` calls needed. This eliminates a
class of bugs where forgetting to wire a projection causes silent data
staleness.

The `QueryManifest` pattern in `conductor/manifest.rs` ensures exhaustive wiring
by destructuring all processors.

## Reactors

Multi-entity event handlers with compile-time exhaustiveness. Declare
dependencies once with `deps!`, then handle each entity in the `.on()` /
`.exhaustive()` chain:

```rust
deps!(RebalancingTrigger, [Position, TokenizedEquityMint]);

#[async_trait]
impl Reactor for RebalancingTrigger {
    type Error = TriggerError;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|symbol, event| async move {
                self.on_position(symbol, event).await
            })
            .on(|id, event| async move {
                self.on_mint(id, event).await
            })
            .exhaustive()
            .await
    }
}
```

Use `.on_with_fallback(handler, fallback)` instead of `.on()` when a handler
needs a recovery path. If the primary handler returns `Err(error)`, the fallback
receives `(error, id, event)` and can reprocess the event from the errored state
(e.g., force-applying a snapshot that the normal path rejected):

```rust
.on_with_fallback(
    |id, event| async move { self.on_snapshot(event).await },
    |error, id, event| async move {
        self.on_snapshot_recovery(error, event).await
    },
)
```

Wire reactors via `Unwired` + `StoreBuilder::wire()`.

### Retrying on transient SQLite busy errors

`ReactorBridge::dispatch` logs and swallows any `react()` error -- it does not
retry. `Projection` gets its own retry-with-backoff for free (see
`Projection::react`), covering both optimistic-lock conflicts and transient
SQLite busy errors, but a bespoke `Reactor` does not: under SQLite WAL
contention, a `SQLITE_BUSY`/`SQLITE_BUSY_SNAPSHOT` failure is swallowed without
retry and the write outcome is unknown -- an atomic `react()` lost its update,
while one that committed earlier non-transactional work is left partially
applied -- unless the reactor opts in.

Opt in by wrapping the reactor in `RetryOnBusy` and implementing the
`IdempotentReactor` marker trait, which declares that `react()` performs solely
SQLite writes with no side effect (HTTP/RPC call, `Store::send` to another
aggregate, message-queue publish) that would double-fire on retry:

```rust
impl IdempotentReactor for MyReactor {}

let store = StoreBuilder::<MyEntity>::new(pool)
    .with(Arc::new(RetryOnBusy { inner: my_reactor }))
    // ...
```

Only implement `IdempotentReactor` for reactors that are provably pure DB writes
end-to-end -- a reactor that orchestrates across aggregates (like
`RebalancingTrigger` above) must not, unless the downstream command handler is
independently confirmed idempotent under re-invocation. For a reactor whose
`react()` has side effects before its write, call `retry_with_backoff` and
`is_retryable_sqlite_busy` directly around just the write instead, leaving the
earlier side effects outside the retry boundary and outside `IdempotentReactor`
entirely. A `react()` that issues two or more separate, non-transactional SQLite
statements is unsafe to mark too: if the first commits and a later one hits
`SQLITE_BUSY`, the retry replays the whole `react()`, re-running the
already-committed statement. A conforming `react()` must be atomic as a whole --
a single statement, a single transaction, or written so replaying it is safe
(upserts, not bare inserts).

A reactor that does neither still swallows a busy error without retry, leaving
its write outcome unknown, exactly as before this exists -- opting in is a
per-reactor decision, not a blanket fix.

**Latency tradeoff:** `CqrsFramework::execute_with_metadata` awaits every
registered reactor's `dispatch()` synchronously before returning to the command
caller, once per event, so a `RetryOnBusy`-wrapped reactor can block that caller
for up to the full retry budget (~4.3s, see `RETRY_MAX_ATTEMPTS`/
`RETRY_BASE_DELAY_MS`/`RETRY_MAX_DELAY_MS`) _per reacted event_ whenever a
busy/busy-snapshot conflict occurs -- including conflicts caused by an unrelated
writer committing anywhere in the same database file, not just writes to the
aggregate the command touched. A single command that emits multiple events
dispatches the reactor once per event, so the worst-case block for that command
is the retry budget multiplied by the event count, not a flat ~4.3s. Callers
with tight request timeouts need a budget larger than the retry schedule. This
is a real behavioral tradeoff (latency vs. lost updates), not just an
idempotency question -- weigh it before wrapping a reactor in `RetryOnBusy`.

## Jobs Pattern

Command handlers stay pure. A command is the operation being performed; if it
needs the outside world (send an email, charge a card, call a chain), it KICKS
OFF a durable `Job` for a worker -- and the handler signature enforces it
(ADR-0009): a handler returns an `Effect`, either domain events or exactly one
job dispatch. The framework emits the `Dispatched` event (carrying the job value
-- the intent IS the job) and enqueues in the **same transaction**, so there is
no crash window between intent and job, and no accomplished-fact event can
accompany a merely-enqueued effect.

Declare the jobs an entity may dispatch with the `jobs!` macro:

```rust
type Jobs = jobs![NotifyClosed];  // or Nil for an entity that dispatches nothing
```

Embed a `DispatchedJob<J>` field in the entity
(`Idle -> InFlight -> Confirmed | Failed`), nest `DispatchEvent<J>` in the event
enum (conventionally a `Dispatch` variant) and `DispatchOutcome<J>` in the
command enum (conventionally `Settle`), both with `From` impls, and delegate:

```rust
async fn transition(&self, command: Self::Command) -> Result<Effect<Self>, Self::Error> {
    use MyCommand::{AwaitCustomer, Close, Settle};

    match command {
        // Kick off the job; the machine's guard refuses while one is in flight.
        Close => fx(self.notify.dispatch(NotifyClosed {
            ticket: self.ticket,
            subject: self.subject.clone(),
        })?),

        // The framework delivers the sealed verdict; settle folds it in.
        Settle(outcome) => fx(self.notify.settle(outcome)?),

        // Pure domain facts stay plain events.
        AwaitCustomer => fx(MyEvent::AwaitingCustomer),
    }
}
```

`fx(value)` wraps whatever a handler arm produces into the full
`Result<Effect, Error>`: a single event, a `Vec`/array of events, the
`Vec<DispatchEvent<J>>` that `settle` returns, a guarded `JobDispatch`, a bare
`Job` (the infallible kick-off for `initialize`, where there is no dispatch
state to guard), or the entity's domain error (which becomes the `Err` arm). In
`originate`, fold the first `Dispatched` event with
`DispatchedJob::originate(event)`. The empty outcome has its own name:
`uneventful()` accepts the command and records nothing (a bare `fx(vec![])`
cannot infer its element type).

The dispatch compile-checks that the job is declared in `Jobs`, that its
`Origin` is this entity, and that the event enum absorbs `DispatchEvent<J>` --
an unwired job is a compile error. `Confirmed`/`Failed` verdicts are sealed
(framework-constructed only), so a `Confirmed` dispatch in state _proves_ the
job settled. Inputs a handler needs (timestamps, ids) travel on the command,
keeping handlers deterministic.

A `Job` implements `submit`/`reconcile`, never a single `perform`: the framework
runs `submit` only on the first execution and routes every retry through
`reconcile`, which looks up the earlier attempt's fate by its idempotency key
(derive it from `JobContext::job_id`, never the attempt) and returns `Settled` /
`NotSubmitted` / `Indeterminate`. Failures are classified explicitly at the
return site: `JobFailure::Transient` retries with backoff,
`JobFailure::Terminal` dead-letters immediately and settles the origin as
`Failed(Rejected)`.

```rust
impl Job for NotifyClosed {
    type Input = Notifier;  // injected by the worker via JobInput
    type Output = ();
    type Error = Never;
    type Origin = SupportTicket;

    const WORKER_NAME: &'static str = "notify-closed";
    const KIND: &'static str = "notify-closed";

    fn label(&self) -> Label {
        Label::new(format!("notify-closed:{}", self.subject))
    }

    fn origin_id(&self) -> TicketId {
        self.ticket
    }

    async fn submit(
        &self,
        _ctx: &JobContext,
        input: &Notifier,
    ) -> Result<JobOutcome<()>, JobFailure<Never>> {
        input.email_customer(&self.subject).await;
        Ok(JobOutcome::Done(()))
    }

    async fn reconcile(
        &self,
        _ctx: &JobContext,
        _input: &Notifier,
    ) -> Result<Reconciliation<()>, JobFailure<Never>> {
        Ok(Reconciliation::Settled(()))  // a duplicate email is tolerable
    }
}
```

Register the worker input as
`NotifyClosed => JobInput::<NotifyClosed>::new(Notifier, store.clone())` -- the
store is the delivery port verdicts come back through. See **Running Jobs**
below for the worker wiring; the `Desk` entity in `dispatch.rs`'s tests and both
`examples/` are reference implementations.

Origin-less background work (reactor sweeps, pollers, startup recovery)
implements `StandaloneJob` (a plain `perform`) and is enqueued directly with
`JobRuntime::enqueue` (ADR-0007) -- never from a command handler. Each direct
`StandaloneJob` implementation also declares its `TERMINAL_FAILURE_MSG`
explicitly.

## Schema Versioning

Bump `SCHEMA_VERSION` when the entity's state, event, or projection schema
changes. On startup, the wiring infrastructure (via `StoreBuilder::build()`)
detects version mismatches and clears stale snapshots for `Retain` entities. For
current policies that may delete history, it refuses a recorded schema version
change instead; those entities require snapshot-aware migration.

### Adding Optional Fields to Events

When adding a new field to an existing event variant that has a sensible default
(zero, `None`, etc.), use `#[serde(default)]` on the field instead of writing an
upcaster. Old persisted events that lack the field will deserialize with the
default value.

```rust
#[derive(Serialize, Deserialize)]
enum MyEvent {
    Snapshot {
        existing_field: i64,
        #[serde(default)]
        new_optional_field: i64,  // old events -> 0
    },
}
```

**Pitfall**: `#[serde(default)]` only works when the default is semantically
correct for old events. If old events _must_ be distinguished from "field not
present" (e.g., `None` vs `Some(0)`), use `Option<T>` with `#[serde(default)]`
instead. For fields where the default would be misleading, use an upcaster (see
below).

Always bump `SCHEMA_VERSION` after adding the field so view projections are
rebuilt with the new field populated from the first event that carries it.

## Testing

### replay -- reconstruct state from events

```rust
use event_sorcery::replay;

let position = replay::<Position>(vec![
    PositionEvent::Initialized { /* ... */ },
    PositionEvent::FillAcknowledged { /* ... */ },
]).unwrap().unwrap();

assert_eq!(position.net, dec!(100));
```

### TestHarness -- BDD-style command testing

```rust
use event_sorcery::TestHarness;

TestHarness::<Position>::new()
    .given(vec![PositionEvent::Initialized { /* ... */ }])
    .when(PositionCommand::AcknowledgeFill { /* ... */ })
    .await
    .then_expect_events(&[PositionEvent::FillAcknowledged { /* ... */ }]);
```

### TestStore -- in-memory command dispatch

```rust
use event_sorcery::TestStore;

let store = TestStore::<MyEntity>::new();
store.send(&id, MyCommand::Create { /* ... */ }).await.unwrap();

let entity = store.load(&id).await.unwrap().unwrap();
assert_eq!(entity.field, expected);
```

### test_store -- SQLite-backed store without reactors

```rust
use event_sorcery::test_store;

let store = test_store::<VaultRegistry>(pool.clone());
store.send(&id, command).await.unwrap();
```

Use `test_store` when you need SQLite persistence but don't care about
projections or reactors. If you need projection data visible after commands, use
`StoreBuilder` with the projection wired.

### load_aggregate -- test-only aggregate loading

```rust
use event_sorcery::load_aggregate;

let entity: Option<Position> = load_aggregate::<Position>(pool, &symbol)
    .await.unwrap();
```

Gated behind `#[cfg(test)]` / `feature = "test-support"`. Bypasses the CQRS
framework (no reactors dispatched). Production code reads through `Projection`.

## Event Upcasters

When you MUST change event structure (e.g., adding required fields to existing
events), use upcasters to transform old events to the new format at load time:

```rust
use cqrs_es::persist::{EventUpcaster, SemanticVersionEventUpcaster};

fn upcast_v1_to_v2(mut payload: Value) -> Value {
    payload["new_field"] = json!("default");
    payload
}

pub fn create_my_upcaster() -> Box<dyn EventUpcaster> {
    Box::new(SemanticVersionEventUpcaster::new(
        "MyAggregate::MyEvent",  // event_type to match
        "2.0",                    // target version
        Box::new(upcast_v1_to_v2),
    ))
}
```

Register upcasters on the event store:

```rust
let event_store = PersistedEventStore::new(event_repo)
    .with_upcasters(vec![create_my_upcaster()]);
```

Update `event_version()` in your event enum to return the new version for new
events.

## Forbidden Patterns

- **NEVER write directly to the `events` table** -- use `Store::send()` (or
  `CqrsFramework::execute()` in test code) to emit events through commands
  - **FORBIDDEN**: direct INSERTs, manual sequence number management, any path
    that bypasses the framework
  - **WHY**: direct writes break aggregate consistency, event ordering, and
    violate the CQRS pattern. The framework owns persistence, sequence numbers,
    aggregate loading, and consistency guarantees
- **NEVER query the `events` table with raw SQL** -- use framework APIs
- **NEVER query view tables with raw SQL** -- use `GenericQuery::load()`
- **NEVER modify events** -- they're immutable historical facts
- **NEVER delete retained events** -- only explicit compactable observational
  aggregates may prune pre-snapshot events through event-sorcery compaction
- **NEVER add events you don't need yet** -- YAGNI applies especially to events
- **NEVER implement `Aggregate` directly** -- implement `EventSourced`
- **NEVER construct `Lifecycle` in application code** -- it's an internal
  adapter
- **NEVER call `sqlite_cqrs()` or `CqrsFramework::new()` in production code** --
  use `StoreBuilder`. Direct construction is allowed in test helpers, CLI code,
  and migration code
- **NEVER create multiple `Store<Entity>` for the same entity type** -- each
  entity type must have exactly ONE `Store<Entity>` instance, constructed once
  in `Conductor::start` via `StoreBuilder`, then shared
  - **WHY**: multiple instances cause silent production bugs -- events persist
    but query processors registered on other instances never see them, so views
    and projections go stale without warnings
  - Wire all query processors via `StoreBuilder::wire()` before calling
    `build()`; the builder tracks wired queries at the type level so a missing
    required query is a compile-time error

## cqrs-es / sqlite-es Internals Reference

These details are hidden by event-sorcery but documented here for debugging and
migration authoring.

### sqlite-es Table Schemas

All three tables are created in the `event_store` migration.

**Events table:**

```sql
CREATE TABLE IF NOT EXISTS events (
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    sequence       BIGINT NOT NULL,
    event_type     TEXT NOT NULL,
    event_version  TEXT NOT NULL,
    payload        JSON NOT NULL,
    metadata       JSON NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id, sequence)
);
```

- **aggregate_type**: From `EventSourced::AGGREGATE_TYPE` (e.g., `"Position"`)
- **aggregate_id**: Caller-provided ID string
- **sequence**: Auto-incremented per aggregate instance (1, 2, 3, ...)
- **event_type**: From `DomainEvent::event_type()` (e.g.,
  `"PositionEvent::Initialized"`)
- **event_version**: From `DomainEvent::event_version()` (e.g., `"1.0"`)
- **payload**: Event serialized via `serde_json::to_value(&event)`
- **metadata**: Arbitrary JSON metadata passed via `execute_with_metadata()`

**NEVER** write to this table directly. Use `CqrsFramework::execute()`.

### Snapshots Table

```sql
CREATE TABLE IF NOT EXISTS snapshots (
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    last_sequence  BIGINT NOT NULL,
    snapshot_version BIGINT NOT NULL DEFAULT 0,
    payload        JSON NOT NULL,
    timestamp      TEXT NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id)
);
```

- **payload**: Aggregate state serialized via `serde_json::to_value(&aggregate)`
- **last_sequence**: The event sequence at the time of the snapshot
- **snapshot_version**: The cqrs-es snapshot version used to determine the next
  snapshot update
- **timestamp**: ISO 8601 timestamp of when the snapshot was taken

Snapshots are enabled for all aggregates through `StoreBuilder`. They are used
as a replay starting point so hot aggregates do not reload their full event
history on every command.

For `Retain` entities, bump `SCHEMA_VERSION` after changing aggregate struct
layout so startup clears stale snapshots through the schema reconciler. Manual
snapshot deletion is safe only for retained event streams where the full event
history remains available for replay.

```sql
-- Retained streams only: reset snapshots when full event replay is available.
DELETE FROM snapshots WHERE aggregate_type = 'Mint';
```

Do not manually delete snapshots for aggregates using
`CompactionPolicy::CompactAfterSnapshot` or `CompactionPolicy::RetainLast`. For
compacted streams, the snapshot may be the only durable pre-snapshot state.
Those aggregates need a snapshot-aware rebuild path or an external source before
snapshots can be discarded. Under either current policy, incompatible snapshots
return typed deserialization errors and remain unchanged; schema reconciliation
refuses to clear snapshots for a recorded version change.

### Event Compaction

Financial audit aggregates retain every event indefinitely. Observational
aggregates may select either snapshot-bounded compaction policy:

- `CompactionPolicy::CompactAfterSnapshot` deletes events represented by the
  current aggregate snapshot.
- `CompactionPolicy::RetainLast { events: window }` additionally preserves a
  trailing sequence window for each aggregate type and ID. An event is deleted
  only when its sequence is at most both the snapshot's covered sequence and
  `newest_sequence - window` for that stream.

For example, a stream ending at sequence 10 with a snapshot covering sequence 7
and a window of 2 retains sequences 8–10: unsnapshotted events survive even when
there are more of them than the window. Without a snapshot, nothing is deleted.
A zero window deletes only snapshot-covered events. Sparse histories may retain
fewer rows than the numeric window because it measures sequence distance, not a
row count. An unrepresentable SQLite integer window fails before deletion.

Only compact observational aggregates whose older events have no long-term audit
value. Projected entities must use `Retain`; `StoreBuilder` enforces this so
views remain rebuildable from the event log. See
[SPEC.md](../SPEC.md#snapshot-bounded-event-retention) for the retention and
snapshot-recovery contract.

### View Tables (Projections)

```sql
CREATE TABLE IF NOT EXISTS my_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);
```

- **view_id**: The aggregate ID string
- **version**: Event sequence number (used for optimistic locking)
- **payload**: The view serialized via `serde_json::to_value(&view)`

`SqliteViewRepository` stores views with `serde_json::to_value()` and loads them
with `serde_json::from_value()`.

### Lifecycle Serialization in View Payloads

All our aggregates use `Lifecycle<Entity>` (where `Entity: EventSourced`) as
both the aggregate and its own view (via the blanket `View` impl). Serde's
default externally-tagged enum representation means:

- `Lifecycle::Uninitialized` -> `"Uninitialized"`
- `Lifecycle::Live(data)` -> `{"Live": <data>}`
- `Lifecycle::Failed { error, last_valid_state }` -> `{"Failed": {...}}`

When `T` is a **struct** (e.g., `Position`, `OnChainTrade`), `<data>` is a flat
JSON object: `{"Live": {"symbol": "AAPL", "net": "0", ...}}`. JSON paths:
`$.Live.symbol`, `$.Live.net`.

When `T` is an **enum** (e.g., `OffchainOrder`, `UsdcRebalance`), `<data>` is
another tagged enum: `{"Live": {"Pending": {"symbol": "AAPL", ...}}}`. JSON
paths depend on the active variant and are unsuitable for generated columns. Use
`GenericQuery::load()` and deserialize in Rust instead.

### Generated Columns on Views

SQLite generated columns can extract fields from `payload` for indexing and
querying. Only appropriate for **struct-typed views** where the JSON path is
stable:

```sql
CREATE TABLE IF NOT EXISTS position_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL,
    symbol TEXT GENERATED ALWAYS AS (
        json_extract(payload, '$.Live.symbol')
    ) STORED
);
```

Generated columns on enum-typed views (the path changes per variant) should be
avoided in favor of using native cqrs-es tooling, e.g.`GenericQuery::load()`.

## Views and GenericQuery

Views are read-optimized projections built from events. **Never query view
tables directly with raw SQL** -- use `GenericQuery`.

For `EventSourced` entities, `Lifecycle<Entity>` has a blanket `View` impl that
delegates to `originate` and `evolve`, so the entity itself serves as its own
view. Use the `SqliteQuery<Entity>` type alias (defined in `event_sourced.rs`)
for the query type:

```rust
use crate::event_sourced::SqliteQuery;

// SqliteQuery<Position> wraps
// GenericQuery<SqliteViewRepository<Lifecycle<Position>,
//     Lifecycle<Position>>>
let query: Arc<SqliteQuery<Position>> = /* built by StoreBuilder */;

// Load view by aggregate ID
let view: Option<Lifecycle<Position>> =
    query.load(&symbol.to_string()).await;
```

For custom views (not the entity itself), implement the `View` trait on the
cqrs-es `Aggregate` type (`Lifecycle<Entity>`):

```rust
impl View<Lifecycle<MyEntity>> for MyCustomView {
    fn update(&mut self, event: &EventEnvelope<Lifecycle<MyEntity>>) {
        match &event.payload {
            MyEvent::Created { .. } => { /* update view */ }
            MyEvent::Updated { .. } => { /* update view */ }
        }
    }
}
```

## Re-projecting Views with QueryReplay

When you add a new view or need to rebuild an existing one from events, use
`QueryReplay`:

```rust
use cqrs_es::persist::QueryReplay;

pub async fn replay_my_view(pool: Pool<Sqlite>) -> Result<(), MyError> {
    let view_repo = Arc::new(SqliteViewRepository::<MyView, MyAggregate>::new(
        pool.clone(),
        "my_view".to_string(),
    ));
    let query = GenericQuery::new(view_repo);
    let event_repo = SqliteEventRepository::new(pool);

    let replay = QueryReplay::new(event_repo, query);
    replay.replay_all().await?;

    Ok(())
}
```

This replays ALL events through the view's `update()` method, rebuilding the
entire view from scratch. It's idempotent - running it multiple times produces
the same result.

**Call replay at startup** to ensure views are up-to-date with any schema
changes.

## Running Jobs

Dispatching a job (see **Jobs Pattern** above) only writes a pending row; a
worker has to drain it. `JobRuntime::build(pool)` wires the durable-jobs runtime
over the same database, and `build_supervised_worker!` registers one supervised
worker per job type -- entity-kicked jobs get a `JobInput` (their dependencies
plus the origin store verdicts are delivered through), standalone jobs get their
plain input:

```rust
let runtime = JobRuntime::build(pool.clone()).await?;

let monitor = build_supervised_worker!(
    runtime,
    JobWorkerConfig::default(),
    Clock::system(),
    {
        NotifyClosed => JobInput::<NotifyClosed>::new(Notifier, store.clone()),
        SweepStaleTickets => (),
    }
);

monitor.run().await?;  // polls, claims, runs, delivers, and acks jobs
```

Each worker polls the `job_queue_projection` projection for runnable jobs of its
`KIND`, claims one under a lease, runs the routed execution (submit/reconcile
for entity-kicked jobs, `perform` for standalone ones), delivers the verdict,
and durably records the outcome (success, retry with backoff, or dead-letter) --
all fenced so a job runs exactly once per successful claim. `JobWorkerConfig`
tunes poll cadence, concurrency, lease duration, retry budget, backoff, and the
verdict-delivery deferrals (`DeliveryPolicy`); `Clock::system()` is the real
wall clock (use `Clock::from_fn(..)` to drive lease and retry timing
deterministically in tests).

## Testing Aggregates

Prefer the public `TestHarness` for command tests — it hides the cqrs-es
plumbing (sink construction, event collection) behind a BDD-style interface:

```rust
use event_sorcery::testing::TestHarness;

#[tokio::test]
async fn test_my_command() {
    TestHarness::<MyEntity>::new()
        .given(vec![MyEvent::Created { /* ... */ }])
        .when(MyCommand::Update { /* ... */ })
        .await
        .then_expect_events(&[MyEvent::Updated { /* ... */ }]);
}
```

When testing `Lifecycle` directly (inside this crate), the cqrs-es 0.5
`Aggregate::handle` signature takes `&mut self`, the services, and an
`EventSink`; emitted events are collected from the sink rather than returned:

```rust
use cqrs_es::Aggregate;
use cqrs_es::event_sink::EventSink;

#[tokio::test]
async fn test_my_command() {
    let mut aggregate = Lifecycle::<MyEntity>::default();
    aggregate.apply(MyEvent::Created { /* ... */ });

    let sink = EventSink::default();
    aggregate
        .handle(MyCommand::Update { /* ... */ }, &(), &sink)
        .await
        .unwrap();

    let events = sink.collect().await;
    assert!(matches!(events[0], MyEvent::Updated { .. }));
}
```

Note the cqrs-es 0.5 commit contract: `PersistedEventStore::commit` rebuilds the
snapshot by re-applying the sink's events to the aggregate it received from
`handle`. `Lifecycle::handle` therefore leaves `self` at its pre-command state
(events are routed through a throwaway scratch copy) — tests that call `handle`
directly should assert this invariant where it matters (see the `handle_*` tests
in `lifecycle.rs`).

Two consequences of the scratch-copy design worth knowing:

- **Command-time validation.** The scratch copy is checked after each event is
  applied to it, so a command whose events the entity's own `originate`/`evolve`
  rejects now fails at command time with the root-cause `LifecycleError` and
  nothing is persisted. (Under cqrs-es 0.4 such events were committed and
  poisoned the aggregate on its next load.)
- **Snapshot coverage is a separate invariant.** The Lifecycle adapter applies
  events once through `handle -> commit -> snapshot-rebuild`. The native SQLite
  and PostgreSQL repositories also preserve the explicit covered sequence
  supplied by the pinned CQRS dependency. With interval two, a three-event batch
  from an empty stream snapshots through sequence two and reload replays the
  third event. Multi-event commands do not require reducing the snapshot
  interval. Custom repositories must honor the
  [snapshot-aware persistence hook](#cqrs-dependency-for-external-backends)
  rather than infer coverage from the batch end or snapshot-version counter.

For view tests, use `View::update()` with `EventEnvelope`:

```rust
#[test]
fn test_view_updates() {
    let mut view = Lifecycle::<MyEntity>::default();
    view.update(&make_envelope("id", 1, MyEvent::Created { /* ... */ }));

    let Lifecycle::Live(entity) = view else {
        panic!("Expected Live state");
    };
    assert_eq!(entity.field, expected_value);
}
```

For error cases, verify the exact `LifecycleError` variant:

```rust
let sink = EventSink::default();
let error = aggregate.handle(command, &(), &sink).await.unwrap_err();
assert!(matches!(error, LifecycleError::Apply(MyError::SpecificVariant)));
```
