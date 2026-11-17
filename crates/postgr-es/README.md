# postgr-es

PostgreSQL persistence integration for `event-sorcery`, built on the maintained
CQRS fork's `postgres-es` adapter. Event, snapshot, replay, and ordinary view
I/O delegate to that adapter. This crate adds atomic job writes, startup
recovery, and indexed view queries; entity definitions and shared
schema/snapshot policy remain in `event-sorcery`.

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

## Migrations

`PostgresBackend::bootstrap_persistence` applies `MIGRATOR`. Applications may
also run `MIGRATOR.run(&pool).await?` directly. Apply the complete migration set
from this crate's `migrations/` directory; application view tables are separate.

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
