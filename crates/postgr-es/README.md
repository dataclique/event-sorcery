# postgr-es

PostgreSQL persistence integration for `event-sorcery`, built on the maintained
CQRS fork's `postgres-es` adapter. Event, snapshot, replay, and ordinary view
I/O delegate to that adapter. This crate adds atomic job writes, native store
startup and projection recovery, and indexed view queries. Entity definitions
and shared schema and snapshot policy remain in `event-sorcery`.

PostgreSQL can persist job events and queue rows, but this adapter does not yet
run workers or recover the job queue at startup.

## Public API

| Export                    | Purpose                                                                  |
| ------------------------- | ------------------------------------------------------------------------ |
| `PostgresBackend`         | Native persistence over an application-provisioned `PgPool`.             |
| `PostgresStoreRepository` | Policy-aware repository supplied by the native backend.                  |
| `PostgresEventRepository` | Events, snapshots, and replay through the `cqrs-es` repository contract. |
| `PostgresViewRepository`  | Application-owned materialized-view storage and optimistic updates.      |
| `PostgresViewBackend`     | Native view queries and retained-history projection recovery.            |
| `MIGRATOR`                | Canonical PostgreSQL library-storage migrations.                         |

## Native stores

Bootstrap library storage before building stores. The application owns the
connection pool and shares it with the backend:

```rust
use sqlx::PgPool;

use event_sorcery::PersistenceBackend;
use postgr_es::PostgresBackend;

async fn bootstrap(pool: PgPool) -> Result<PostgresBackend, sqlx::Error> {
    let backend = PostgresBackend { pool };
    backend.bootstrap_persistence().await?;
    Ok(backend)
}
```

For an application entity `Note` declaring `Materialized = Nil`, construct its
store with:

```rust
use event_sorcery::StoreBuilder;
use postgr_es::PostgresBackend;

let backend = bootstrap(pool).await?;
let store = StoreBuilder::<Note, PostgresBackend>::with_backend(backend)
    .build()
    .await?;
```

Startup reconciles the entity schema and registers its version in one
transaction before exposing the store. Snapshot recovery respects the entity's
compaction policy. Commands continue through the existing CQRS execution path.

For an application entity `ProjectedNote` declaring `Materialized = Table` and
`PROJECTION = Table("notes_projection")`, apply its view-table migration before
building:

```rust
use event_sorcery::StoreBuilder;
use postgr_es::PostgresBackend;

let backend = bootstrap(pool).await?;
let (store, projection) =
    StoreBuilder::<ProjectedNote, PostgresBackend>::with_backend(backend)
        .build()
        .await?;

let notes = projection.load_all().await?;
```

Projected stores require retained event history. Startup uses the same borrowed
transaction connection for schema reconciliation, projection recovery, and
registry registration, including with a one-connection pool. A changed schema
rebuilds the projection; an unchanged schema catches up missing or lagging rows.
Recovered checkpoints record the captured event maximum, not an update count. A
replay or registry-write failure rolls back the projection and snapshot changes
and does not register the new schema version.

The returned projection supports `load`, `load_all`, typed `filter`, indexed
`find`, `catch_up`, `rebuild`, and `rebuild_all`. Explicit recovery uses its own
transaction. Quiesce command writers during schema upgrades and explicit
rebuilds: the registry lock serializes startup reconciliation, not live
commands.

## Materialized-view storage

Applications provide their own view-table migrations. A table requires
`view_id TEXT PRIMARY KEY`, a positive `BIGINT` `version`, and a non-null
`JSONB` `payload`. Use the `_projection` suffix for projection tables.

Construct storage with
`PostgresViewRepository::<ViewState, AggregateType>::new(pool, "notes_projection")`.
It implements `cqrs_es::persist::ViewRepository`: reads expose the stored view
and optimistic version; updates fence stale writers and preserve additional
columns.

Indexed reads use `indexed_query::IndexedView` with its shared `Predicate`,
`Term`, `Cmp`, `Value`, and `Order` query types. PostgreSQL retains native
argument encoding and quotes table and column names as complete, literal
identifiers. Invalid query inputs and versions are reported through
`ProjectionQueryError` and `ProjectionVersionError`.

Native view versions are aggregate-local event checkpoints. Framework delivery
carries the persisted sequence so an event already included by recovery is not
applied again. Out-of-order delivery retries without advancing past missing
history; `catch_up` repairs a remaining lag. Use framework delivery rather than
mixing direct, unsequenced `Reactor::react` calls into a native projection.

## Standalone durable enqueue

Use `PostgresEventRepository::enqueue` when a job has no application command
transaction to join. Apply the library migrations first. The repository commits
its canonical enqueue event and initial queue row in one transaction:

```rust
use cqrs_es::persist::PersistenceError;
use serde_json::Value;
use sqlx::PgPool;

use event_sorcery::{JobId, JobSeed};
use postgr_es::PostgresEventRepository;

async fn enqueue_job(
    pool: PgPool,
    job_id: JobId,
    kind: String,
    payload: Value,
    run_at_ms: i64,
) -> Result<(), PersistenceError> {
    let repository = PostgresEventRepository { pool };
    repository
        .enqueue(JobSeed::new(job_id, kind, payload, run_at_ms))
        .await
}
```

`run_at_ms` is the scheduled Unix timestamp in milliseconds. Supply a fresh job
identity for a new enqueue; reusing an existing identity returns a persistence
error rather than creating another job. Enqueue records durable intent; it does
not execute the job's external effects.

### Queue recovery contract

The `job` event stream is authoritative; `job_queue_projection` is derived
state. The adapter's PostgreSQL conformance tests exercise the library-owned job
fold through native projection recovery and indexed reads, without starting a
worker. This does not add a public job-queue startup API.

Recovery restores the lifecycle and the scheduled time of pending or claimed
jobs. Catch-up leaves an existing lease column intact; recreating a missing row
leaves that column unset. The deadline recorded in a claim event is audit data,
not a lease that replay can restore. A failed or cancelled recovery does not
publish a partial queue or advance the schema registry.

## Migrations

Use `PostgresBackend::bootstrap_persistence` to apply the library migrations
from a pool. It closes its migration connection on drop, so failure or
cancellation cannot return a connection holding the migration advisory lock to
the pool.

Apply the complete migration set from this crate's `migrations/` directory, as
well as your application's migrations. If one application migrator includes both
sets, run it instead of library bootstrap. If the library and application run
separate migrators on the same database, give them distinct migration history
tables from the first run. For example, leave the library on `_sqlx_migrations`
and configure the application migrator with
`dangerous_set_table_name("_app_sqlx_migrations")`. Sharing the default table
makes either migrator reject versions present only in the other set
(`VersionMissing`), regardless of execution order. Do not rename an existing
history table without preserving its migration records.

The backend crates watch their migration directories so SQL-only changes rebuild
the embedded migrator.

## Testing

Real PostgreSQL tests run in the normal crate suite. Start PostgreSQL and
configure `DATABASE_URL` for a role that can create isolated test databases:

```sh
cargo nextest run -p postgr-es
```

Database tests fail when storage is unavailable; `SQLX_OFFLINE` does not replace
a runtime database.

See [SQLx development notes](../../docs/sqlx.md) for database setup and
[the domain glossary](../../docs/domain.md) for terminology.
