# postgr-es

PostgreSQL repositories, migrations, and native persistence integration for
`event-sorcery`. Database-specific storage lives here; entity definitions,
schema policy, and store construction remain in `event-sorcery`.

## Public API

| Export                    | Purpose                                                                  |
| ------------------------- | ------------------------------------------------------------------------ |
| `PostgresBackend`         | Native persistence over an application-provisioned `PgPool`.             |
| `PostgresStoreRepository` | Policy-aware repository supplied by the native backend.                  |
| `PostgresEventRepository` | Events, snapshots, and replay through the `cqrs-es` repository contract. |
| `PostgresViewRepository`  | Application-owned materialized-view storage and optimistic updates.      |
| `PostgresViewBackend`     | Repository family implementing `event_sorcery::ViewBackend`.             |
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

## Materialized-view storage

Applications provide their own view-table migrations. A table requires
`view_id TEXT PRIMARY KEY`, a positive `BIGINT` `version`, and a non-null
`JSONB` `payload`. Use the `_projection` suffix for projection tables.

Construct storage with
`PostgresViewRepository::<ViewState, AggregateType>::new(pool, "notes_projection")`.
It implements `cqrs_es::persist::ViewRepository`: reads expose the stored view
and optimistic version; updates fence stale writers and preserve additional
columns.

Indexed reads use `sqlite_es::IndexedView` with the shared `Predicate`, `Term`,
`Cmp`, `Value`, and `Order` query types. Despite their crate location, these
query types also serve this PostgreSQL repository. Invalid query inputs and
versions are reported through `ProjectionQueryError` and
`ProjectionVersionError`.

## Migrations

`PostgresBackend::bootstrap_persistence` applies `MIGRATOR`. Applications may
also run `MIGRATOR.run(&pool).await?` directly. Apply the complete migration set
from this crate's `migrations/` directory; application view tables are separate.

The backend crates watch their migration directories so SQL-only changes rebuild
the embedded migrator.

## Testing

From the repository root, run database-independent validation and
query-construction tests with:

```sh
cargo nextest run -p postgr-es --lib
```

With PostgreSQL available and `DATABASE_URL` configured for a role that can
create isolated test databases, run the complete crate suite with:

```sh
cargo nextest run -p postgr-es --all-features
```

The `integration-tests` feature enables real-database tests. Linux flake checks
enable all features and provision PostgreSQL through the test hook. Darwin flake
tests retain the database-dependent test exclusion. Enabled database tests fail
when storage is unavailable; `SQLX_OFFLINE` does not replace a runtime database.

See [SQLx development notes](../../docs/sqlx.md) for database setup and
[the domain glossary](../../docs/domain.md) for terminology.
