//! SQLite store construction, schema reconciliation and maintenance operations.

use cqrs_es::persist::{PersistedEventStore, PersistenceError};
use cqrs_es::{CqrsFramework, EventStore};
use sqlx::SqlitePool;
use std::fmt::Debug;
use std::str::FromStr;

#[cfg(any(test, feature = "test-support"))]
use crate::EventBackend;
use crate::job;
use crate::lifecycle::Lifecycle;
use crate::{
    EventSourced, LoadAllIdsError, Nil, Projection, ProjectionError, ReconcileError, Reconciler,
    SchemaReconciliation, SendError, SqliteBackend, SqliteEventRepository, SqliteViewBackend,
    Store, StoreBuilder, Table, UnmaterializedStoreBackend,
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
        let pool = self.pool.clone();
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
        let reconciler = crate::Reconciler::new(self.pool.clone());
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
/// This is a no-op for entities with [`crate::CompactionPolicy::Retain`].
/// [`crate::CompactionPolicy::CompactAfterSnapshot`] removes snapshot-covered events;
/// [`crate::CompactionPolicy::RetainLast`] additionally preserves the trailing sequence
/// window for each aggregate type and id. Neither policy deletes unsnapshotted
/// events or events from streams without snapshots.
///
/// # Errors
///
/// Returns database errors from the delete query or an encoding error when the
/// retention window cannot be represented as a SQLite integer.
pub async fn compact_events<Entity: EventSourced>(pool: &SqlitePool) -> Result<u64, sqlx::Error> {
    sqlite_es::compact_events(pool, Entity::AGGREGATE_TYPE, Entity::COMPACTION_POLICY).await
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
    let rows = sqlite_es::load_all_ids(pool, Entity::AGGREGATE_TYPE).await?;

    let (ids, invalid) = rows.into_iter().fold(
        (Vec::new(), Vec::new()),
        |(mut ids, mut invalid), id_str| {
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

/// Load up to `limit` aggregate IDs starting from `offset`.
///
/// Orders IDs by descending maximum rowid across each aggregate's events and
/// snapshot, not by creation time. An append can change the order; ties have
/// no guaranteed order.
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
    let rows = sqlite_es::load_ids_paginated(
        pool,
        Entity::AGGREGATE_TYPE,
        i64::try_from(limit)?,
        i64::try_from(offset)?,
    )
    .await?;

    let (ids, invalid) = rows.into_iter().fold(
        (Vec::new(), Vec::new()),
        |(mut ids, mut invalid), id_str| {
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
    let count = sqlite_es::count_aggregates(pool, Entity::AGGREGATE_TYPE).await?;
    Ok(usize::try_from(count)?)
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
