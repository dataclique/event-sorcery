//! SQLite store construction, schema reconciliation and maintenance operations.

use cqrs_es::persist::{PersistedEventStore, PersistenceError};
use cqrs_es::{CqrsFramework, EventStore};
use sqlx::{AssertSqlSafe, SqlitePool};
use std::fmt::Debug;
use std::str::FromStr;

#[cfg(any(test, feature = "test-support"))]
use crate::EventBackend;
use crate::job;
use crate::lifecycle::Lifecycle;
use crate::{
    CompactionPolicy, EventSourced, LoadAllIdsError, Nil, Projection, ProjectionError,
    ReconcileError, Reconciler, SchemaReconciliation, SendError, SqliteBackend,
    SqliteEventRepository, SqliteViewBackend, Store, StoreBuilder, Table,
    UnmaterializedStoreBackend,
};

/// SQLite-specific [`Store`] helpers.
impl<Entity: EventSourced> Store<Entity, SqliteBackend> {
    /// Reconstruct an entity's state from events without needing
    /// a full `Store` (no services or CQRS framework required).
    ///
    /// Useful in test/CLI contexts where you only need to read
    /// aggregate state and never send commands.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn load_from_pool(
        pool: SqlitePool,
        id: &Entity::Id,
    ) -> Result<Option<Entity>, SendError<Entity>> {
        let repo = SqliteEventRepository::new(pool, Entity::COMPACTION_POLICY);
        let event_store =
            PersistedEventStore::<SqliteEventRepository, Lifecycle<Entity>>::new_snapshot_store(
                repo,
                Entity::SNAPSHOT_SIZE,
            );
        let context = event_store.load_aggregate(&id.to_string()).await?;

        Ok(context.aggregate.into_result()?)
    }
}

impl<Entity: EventSourced> StoreBuilder<Entity, SqliteBackend> {
    /// Creates a new builder backed by SQLite over `pool`.
    pub fn new(pool: SqlitePool) -> Self {
        Self::with_backend(SqliteBackend::new(pool))
    }
}

impl crate::ProjectedStoreBackend for SqliteBackend {
    type ViewBackend = SqliteViewBackend;

    async fn reconcile_projected<Entity>(
        &self,
    ) -> Result<Projection<Entity, SqliteViewBackend>, ReconcileError>
    where
        Entity: EventSourced<Materialized = Table> + 'static,
    {
        let pool = self.pool().clone();
        let reconciler = Reconciler::new(pool.clone());
        let reconciliation = reconciler.reconcile::<Entity>().await?;

        let projection = Projection::sqlite(pool.clone());

        // A schema version change can leave stored view payloads in an
        // incompatible format. `catch_up` only revisits views that are behind
        // on event sequence, so a view that is current but incompatible (the
        // normal state after a view-schema change with no new events) would
        // never be healed. On a detected schema change, rebuild every view
        // from the event log; otherwise just replay any events the view missed
        // due to a crash between event persistence and view update. Projected
        // entities are guaranteed `CompactionPolicy::Retain` (asserted above),
        // so the full history is always available to rebuild from. Either path
        // runs before registering the projection as a reactor so no concurrent
        // writes can interfere.
        let recovery = match reconciliation {
            SchemaReconciliation::Changed => projection.rebuild_all().await,
            SchemaReconciliation::Unchanged => projection.catch_up().await,
        };
        recovery.map_err(|error| match error {
            ProjectionError::Sqlx(sqlx_error) => ReconcileError::from(sqlx_error),
            ProjectionError::Persistence(persistence_error) => {
                ReconcileError::from(persistence_error)
            }
            other => ReconcileError::Persistence(PersistenceError::UnknownError(Box::new(other))),
        })?;

        // Mark the schema version reconciled only after the snapshot clear and
        // view recovery above have durably completed. A crash before this point
        // leaves the version unadvanced, so the next startup re-runs the whole
        // reconcile -- including rebuild_all -- instead of recording a version
        // whose view rebuild never finished.
        reconciler.record_version::<Entity>().await?;

        Ok(projection)
    }
}

impl UnmaterializedStoreBackend for SqliteBackend {
    async fn reconcile_unmaterialized<Entity: EventSourced<Materialized = Nil>>(
        &self,
    ) -> Result<(), ReconcileError> {
        let reconciler = crate::Reconciler::new(self.pool().clone());
        let _ = reconciler.reconcile::<Entity>().await?;
        reconciler.record_version::<Entity>().await
    }
}

/// Load a single entity by replaying events from the store.
///
/// Creates a lightweight, temporary event store - no CQRS framework, no
/// query processors. Suitable for read-only access from contexts that
/// don't own a [`Store`] (e.g., dashboard transfer loading).
///
/// # Errors
///
/// Returns `SendError` if event store loading or lifecycle
/// reconstruction fails.
pub async fn load_entity<Entity: EventSourced>(
    pool: &SqlitePool,
    id: &Entity::Id,
) -> Result<Option<Entity>, SendError<Entity>> {
    let repo = SqliteEventRepository::new(pool.clone(), Entity::COMPACTION_POLICY);
    let event_store =
        PersistedEventStore::<SqliteEventRepository, Lifecycle<Entity>>::new_snapshot_store(
            repo,
            Entity::SNAPSHOT_SIZE,
        );

    let context = event_store.load_aggregate(&id.to_string()).await?;

    Ok(context.aggregate.into_result()?)
}

/// Execute a single command against an aggregate without a pre-built
/// [`Store`].
///
/// Creates a temporary CQRS framework with no query processors,
/// executes the command (inside a pending-job scope, so any jobs the
/// handler enqueues flush with the events), and discards the framework.
/// Useful in CLI contexts where you need to send a command but don't have
/// (or need) a full server-lifetime Store.
pub async fn send_command<Entity: EventSourced>(
    pool: &SqlitePool,
    id: &Entity::Id,
    command: Entity::Command,
) -> Result<(), SendError<Entity>> {
    let repo = SqliteEventRepository::new(pool.clone(), Entity::COMPACTION_POLICY);
    let store = PersistedEventStore::<SqliteEventRepository, Lifecycle<Entity>>::new_snapshot_store(
        repo,
        Entity::SNAPSHOT_SIZE,
    );

    #[allow(clippy::disallowed_methods)]
    let cqrs = CqrsFramework::new(store, vec![], ());

    job::with_pending_scope(cqrs.execute(&id.to_string(), command)).await
}

/// Delete compactable events that are already represented by snapshots.
///
/// This is a no-op for entities with [`CompactionPolicy::Retain`].
/// [`CompactionPolicy::CompactAfterSnapshot`] removes snapshot-covered events;
/// [`CompactionPolicy::RetainLast`] additionally preserves the trailing sequence
/// window for each aggregate type and id. Neither policy deletes unsnapshotted
/// events or events from streams without snapshots.
///
/// # Errors
///
/// Returns database errors from the delete query or an encoding error when the
/// retention window cannot be represented as a SQLite integer.
pub async fn compact_events<Entity: EventSourced>(pool: &SqlitePool) -> Result<u64, sqlx::Error> {
    let result = match Entity::COMPACTION_POLICY {
        CompactionPolicy::Retain => return Ok(0),
        CompactionPolicy::CompactAfterSnapshot => {
            sqlx::query(
                "DELETE FROM events \
                 WHERE aggregate_type = ?1 \
                   AND sequence <= COALESCE( \
                       (SELECT last_sequence FROM snapshots \
                        WHERE snapshots.aggregate_type = events.aggregate_type \
                          AND snapshots.aggregate_id = events.aggregate_id), \
                       0 \
                   )",
            )
            .bind(Entity::AGGREGATE_TYPE)
            .execute(pool)
            .await?
        }
        CompactionPolicy::RetainLast { events } => {
            let window =
                i64::try_from(events).map_err(|source| sqlx::Error::Encode(Box::new(source)))?;

            sqlx::query(
                "DELETE FROM events \
                 WHERE aggregate_type = ?1 \
                   AND sequence <= COALESCE( \
                       (SELECT last_sequence FROM snapshots \
                        WHERE snapshots.aggregate_type = events.aggregate_type \
                          AND snapshots.aggregate_id = events.aggregate_id), \
                       0 \
                   ) \
                   AND sequence <= ( \
                       SELECT MAX(newest.sequence) FROM events newest \
                       WHERE newest.aggregate_type = events.aggregate_type \
                         AND newest.aggregate_id = events.aggregate_id \
                   ) - ?2",
            )
            .bind(Entity::AGGREGATE_TYPE)
            .bind(window)
            .execute(pool)
            .await?
        }
    };

    Ok(result.rows_affected())
}

/// Reclaim SQLite file space after event compaction.
///
/// Full `VACUUM` can take an exclusive database lock and should be reserved for
/// explicit maintenance windows, not hot runtime loops.
///
/// # Errors
///
/// Returns database errors from SQLite `VACUUM`.
pub async fn vacuum(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query("VACUUM").execute(pool).await?;
    Ok(())
}

/// Reclaim a bounded number of SQLite freelist pages.
///
/// This is intended for periodic background cleanup on databases configured
/// with `auto_vacuum = INCREMENTAL`.
///
/// # Errors
///
/// Returns database errors from SQLite `PRAGMA incremental_vacuum`.
pub async fn incremental_vacuum(pool: &SqlitePool, pages: u32) -> Result<(), sqlx::Error> {
    sqlx::query(AssertSqlSafe(format!("PRAGMA incremental_vacuum({pages})")))
        .execute(pool)
        .await?;
    Ok(())
}

/// Load all aggregate IDs for a given entity type.
///
/// Queries events and snapshots for distinct aggregate IDs. Used by dashboard
/// transfer loading to enumerate all transfer aggregates without requiring
/// access to a [`Store`].
///
/// Returns an error if any stored aggregate ID fails to parse,
/// since that indicates data corruption or a schema mismatch.
///
/// # Errors
///
/// Returns `LoadAllIdsError` on database errors or if stored aggregate
/// IDs fail to parse.
pub async fn load_all_ids<Entity: EventSourced>(
    pool: &SqlitePool,
) -> Result<Vec<Entity::Id>, LoadAllIdsError>
where
    <Entity::Id as FromStr>::Err: Debug,
{
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT aggregate_id FROM ( \
             SELECT aggregate_id FROM events WHERE aggregate_type = ?1 \
             UNION \
             SELECT aggregate_id FROM snapshots WHERE aggregate_type = ?1 \
         ) \
         ORDER BY aggregate_id ASC",
    )
    .bind(Entity::AGGREGATE_TYPE)
    .fetch_all(pool)
    .await?;

    let (ids, invalid) = rows.into_iter().fold(
        (Vec::new(), Vec::new()),
        |(mut ids, mut invalid), (id_str,)| {
            match id_str.parse::<Entity::Id>() {
                Ok(id) => ids.push(id),
                Err(parse_error) => {
                    tracing::warn!(
                        target: "cqrs",
                        aggregate_id = id_str,
                        aggregate_type = Entity::AGGREGATE_TYPE,
                        ?parse_error,
                        "Failed to parse aggregate ID"
                    );
                    invalid.push(id_str);
                }
            }
            (ids, invalid)
        },
    );

    if invalid.is_empty() {
        Ok(ids)
    } else {
        Err(LoadAllIdsError::InvalidIds {
            aggregate_type: Entity::AGGREGATE_TYPE,
            ids: invalid,
        })
    }
}

/// Load aggregate IDs with pagination, newest first (by highest rowid).
///
/// Returns up to `limit` IDs starting from `offset`, ordered by most
/// recently created aggregate first (based on the maximum rowid of each
/// aggregate's events or snapshot).
///
/// # Errors
///
/// Returns `LoadAllIdsError` on database errors or unparseable IDs.
pub async fn load_ids_paginated<Entity: EventSourced>(
    pool: &SqlitePool,
    limit: usize,
    offset: usize,
) -> Result<Vec<Entity::Id>, LoadAllIdsError>
where
    <Entity::Id as FromStr>::Err: Debug,
{
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT aggregate_id FROM ( \
             SELECT aggregate_id, MAX(rowid) AS latest_rowid \
             FROM events \
             WHERE aggregate_type = ?1 \
             GROUP BY aggregate_id \
             UNION ALL \
             SELECT aggregate_id, MAX(rowid) AS latest_rowid \
             FROM snapshots \
             WHERE aggregate_type = ?1 \
             GROUP BY aggregate_id \
         ) \
         GROUP BY aggregate_id \
         ORDER BY MAX(latest_rowid) DESC \
         LIMIT ?2 OFFSET ?3",
    )
    .bind(Entity::AGGREGATE_TYPE)
    .bind(i64::try_from(limit)?)
    .bind(i64::try_from(offset)?)
    .fetch_all(pool)
    .await?;

    let (ids, invalid) = rows.into_iter().fold(
        (Vec::new(), Vec::new()),
        |(mut ids, mut invalid), (id_str,)| {
            match id_str.parse::<Entity::Id>() {
                Ok(id) => ids.push(id),
                Err(parse_error) => {
                    tracing::warn!(
                        target: "cqrs",
                        aggregate_id = id_str,
                        aggregate_type = Entity::AGGREGATE_TYPE,
                        ?parse_error,
                        "Failed to parse aggregate ID (paginated)"
                    );
                    invalid.push(id_str);
                }
            }
            (ids, invalid)
        },
    );

    if invalid.is_empty() {
        Ok(ids)
    } else {
        Err(LoadAllIdsError::InvalidIds {
            aggregate_type: Entity::AGGREGATE_TYPE,
            ids: invalid,
        })
    }
}

/// Count the total number of distinct aggregates of this type.
///
/// # Errors
///
/// Returns `LoadAllIdsError` on database failure or numeric conversion error.
pub async fn count_aggregates<Entity: EventSourced>(
    pool: &SqlitePool,
) -> Result<usize, LoadAllIdsError> {
    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM ( \
             SELECT aggregate_id FROM events WHERE aggregate_type = ?1 \
             UNION \
             SELECT aggregate_id FROM snapshots WHERE aggregate_type = ?1 \
         )",
    )
    .bind(Entity::AGGREGATE_TYPE)
    .fetch_one(pool)
    .await?;

    Ok(usize::try_from(row.0)?)
}

#[cfg(any(test, feature = "test-support"))]
/// Test-only escape hatch for creating CQRS frameworks directly.
///
/// Create a SQLite-backed Store with no reactors, for tests that
/// need persistence but no event processing side-effects.
pub fn test_store<Entity: EventSourced>(pool: sqlx::SqlitePool) -> Store<Entity, SqliteBackend> {
    let backend = SqliteBackend::new(pool);
    let event_store = PersistedEventStore::new_snapshot_store(
        backend.event_repo(Entity::COMPACTION_POLICY),
        Entity::SNAPSHOT_SIZE,
    );
    #[allow(clippy::disallowed_methods)]
    let cqrs = CqrsFramework::new(event_store, vec![], ());
    Store::new(cqrs, &backend)
}
