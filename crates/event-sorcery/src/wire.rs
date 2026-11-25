//! CQRS framework construction via [`StoreBuilder`].
//!
//! All CQRS framework construction must go through
//! [`StoreBuilder`], which reconciles schema versions and
//! registers reactors. Direct CQRS framework construction is
//! blocked via clippy's `disallowed-methods`; `StoreBuilder`
//! contains the narrow escape hatch.
//!
//! # Registering reactors
//!
//! Use [`.with()`](StoreBuilder::with) to register a reactor
//! with a builder. For single-entity reactors, wrap in
//! `Arc::new()`. For multi-entity reactors, clone the same
//! `Arc` into each builder.
//!
//! # Auto-wired projections
//!
//! `build()` dispatches on `Entity::Materialized` via a type
//! parameter that defaults to `Entity::Materialized`:
//!
//! - `Table` entities: auto-creates and wires a [`Projection`],
//!   returning `(Arc<Store>, Arc<Projection>)`.
//! - `Nil` entities: returns `Arc<Store>`.
//!
//! This eliminates the footgun of forgetting to wire a
//! projection -- if the entity declares a table, the projection
//! is always present.
//!
//! Exhaustive entity handling is enforced by the reactor's
//! [`.on()`](crate::OneOf::on) /
//! [`.exhaustive()`](crate::Fold::exhaustive) chain at compile
//! time, not by the wiring infrastructure.

use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

#[cfg(any(test, feature = "sqlite"))]
use cqrs_es::persist::PersistenceError;
use cqrs_es::persist::{PersistedEventRepository, PersistedEventStore};
use cqrs_es::{CqrsFramework, EventStore, Query};
#[cfg(any(test, feature = "sqlite"))]
use sqlx::SqlitePool;

#[cfg(all(any(test, feature = "test-support"), any(test, feature = "sqlite")))]
use crate::EventBackend;
use crate::Nil;
use crate::dependency::HasEntity;
use crate::lifecycle::{Lifecycle, ReactorBridge};
use crate::projection::{Projection, ProjectionQuery};
use crate::reactor::Reactor;
use crate::schema_registry::ReconcileError;
use crate::{
    CompactionPolicy, EventSourced, PersistenceBackend, ProjectedStoreBackend, SendError, Store,
    Table, UnmaterializedStoreBackend,
};
#[cfg(any(test, feature = "sqlite"))]
use crate::{
    LoadAllIdsError, ProjectionError, Reconciler, SchemaReconciliation, SqliteBackend,
    SqliteViewBackend,
};

/// Builder for a single CQRS framework.
///
/// Parameterized on an [`EventSourced`] entity type. The
/// `Materialized` type parameter defaults to
/// `Entity::Materialized` and determines the `build()` return
/// type: `Table` returns `(Arc<Store>, Arc<Projection>)`, `Nil`
/// returns `Arc<Store>`.
///
/// Register reactors via [`.with()`](Self::with), then call
/// [`.build()`](Self::build) to construct the framework.
pub struct StoreBuilder<
    Entity: EventSourced,
    Backend: PersistenceBackend,
    Materialized = <Entity as EventSourced>::Materialized,
> {
    backend: Backend,
    queries: Vec<Box<dyn Query<Lifecycle<Entity>>>>,
    _materialized: std::marker::PhantomData<Materialized>,
}

impl<Entity: EventSourced, Backend: PersistenceBackend> StoreBuilder<Entity, Backend> {
    /// Creates a new builder over an arbitrary [`PersistenceBackend`].
    pub fn with_backend(backend: Backend) -> Self {
        Self {
            backend,
            queries: vec![],
            _materialized: std::marker::PhantomData,
        }
    }

    /// Registers a reactor with this CQRS framework.
    ///
    /// The reactor must declare `Entity` in its dependency list
    /// (via [`deps!`](crate::deps)). For multi-entity reactors,
    /// clone the same `Arc` into each relevant builder.
    #[must_use]
    pub fn with<R>(mut self, reactor: Arc<R>) -> Self
    where
        R: Reactor + 'static,
        R::Dependencies: HasEntity<Entity>,
        Entity::Id: Clone,
        Entity::Event: Clone,
        <Entity::Id as FromStr>::Err: Debug,
    {
        self.queries.push(Box::new(ReactorBridge { reactor }));
        self
    }
}

pub(crate) fn es_cqrs<Entity: EventSourced, Repository: PersistedEventRepository>(
    store: PersistedEventStore<Repository, Lifecycle<Entity>>,
    queries: Vec<Box<dyn Query<Lifecycle<Entity>>>>,
) -> CqrsFramework<Lifecycle<Entity>, PersistedEventStore<Repository, Lifecycle<Entity>>> {
    // `Lifecycle`'s cqrs-es services are unit -- handlers return an `Effect`
    // instead of calling out through injected services.
    #[allow(clippy::disallowed_methods)]
    CqrsFramework::new(store, queries, ())
}

/// Projected entities: auto-creates and wires a [`Projection`],
/// returning `(Arc<Store>, Arc<Projection>)`.
impl<Entity, Backend> StoreBuilder<Entity, Backend, Table>
where
    Entity: EventSourced<Materialized = Table> + 'static,
    Backend: ProjectedStoreBackend,
    Entity::Id: Clone,
    Entity::Event: Clone,
    <Entity::Id as FromStr>::Err: Debug,
{
    pub async fn build(
        mut self,
    ) -> Result<
        (
            Arc<Store<Entity, Backend>>,
            Arc<Projection<Entity, Backend::ViewBackend>>,
        ),
        ReconcileError,
    > {
        // Projected entities must retain all events so that
        // `catch_up`/`rebuild_all` can replay the full history.
        // Compacted aggregates lose events after snapshot, making
        // projection rebuilds silently incomplete.
        const {
            assert!(
                matches!(Entity::COMPACTION_POLICY, CompactionPolicy::Retain),
                "CompactAfterSnapshot entities must not have table projections -- \
                 rebuild_all only reads the events table and would miss \
                 compacted snapshot-only aggregates"
            );
        }

        let projection = Arc::new(self.backend.reconcile_projected::<Entity>().await?);

        self.queries.push(Box::new(ProjectionQuery {
            projection: projection.clone(),
        }));

        let Self {
            backend, queries, ..
        } = self;
        let event_store = PersistedEventStore::new_snapshot_store(
            backend.persistence_repo(Entity::COMPACTION_POLICY),
            Entity::SNAPSHOT_SIZE,
        );
        let cqrs = es_cqrs(event_store, queries);
        Ok((Arc::new(Store::new(cqrs, &backend)), projection))
    }
}

/// Non-projected entities: returns just `Store`.
impl<Entity: EventSourced<Materialized = Nil>, Backend: UnmaterializedStoreBackend>
    StoreBuilder<Entity, Backend, Nil>
{
    pub async fn build(self) -> Result<Arc<Store<Entity, Backend>>, ReconcileError> {
        self.backend.reconcile_unmaterialized::<Entity>().await?;

        let Self {
            backend, queries, ..
        } = self;
        let event_store = PersistedEventStore::new_snapshot_store(
            backend.persistence_repo(Entity::COMPACTION_POLICY),
            Entity::SNAPSHOT_SIZE,
        );
        let cqrs = es_cqrs(event_store, queries);
        Ok(Arc::new(Store::new(cqrs, &backend)))
    }
}

#[cfg(any(test, feature = "sqlite"))]
impl<Entity: EventSourced> StoreBuilder<Entity, SqliteBackend> {
    /// Creates a new builder backed by SQLite over `pool`.
    pub fn new(pool: SqlitePool) -> Self {
        Self::with_backend(SqliteBackend::new(pool))
    }
}

#[cfg(any(test, feature = "sqlite"))]
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

#[cfg(any(test, feature = "sqlite"))]
impl UnmaterializedStoreBackend for SqliteBackend {
    async fn reconcile_unmaterialized<Entity: EventSourced<Materialized = Nil>>(
        &self,
    ) -> Result<(), ReconcileError> {
        let reconciler = crate::Reconciler::new(self.pool.clone());
        let _ = reconciler.reconcile::<Entity>().await?;
        reconciler.record_version::<Entity>().await
    }
}

/// Reconstructs an entity from its snapshot and events without a [`Store`].
///
/// Returns an error if reading storage or reconstructing the entity fails.
pub async fn load_entity<Entity: EventSourced>(
    backend: &impl PersistenceBackend,
    id: &Entity::Id,
) -> Result<Option<Entity>, SendError<Entity>> {
    let repository = backend.persistence_repo(Entity::COMPACTION_POLICY);
    let store = PersistedEventStore::<_, Lifecycle<Entity>>::new_snapshot_store(
        repository,
        Entity::SNAPSHOT_SIZE,
    );
    let context = store.load_aggregate(&id.to_string()).await?;

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
    backend: &impl PersistenceBackend,
    id: &Entity::Id,
    command: Entity::Command,
) -> Result<(), SendError<Entity>> {
    let repository = backend.persistence_repo(Entity::COMPACTION_POLICY);
    let store = PersistedEventStore::new_snapshot_store(repository, Entity::SNAPSHOT_SIZE);
    let cqrs = es_cqrs::<Entity, _>(store, vec![]);

    crate::job::with_pending_scope(cqrs.execute(&id.to_string(), command)).await
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
#[cfg(any(test, feature = "sqlite"))]
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
#[cfg(any(test, feature = "sqlite"))]
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
#[cfg(any(test, feature = "sqlite"))]
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
#[cfg(any(test, feature = "sqlite"))]
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
#[cfg(any(test, feature = "sqlite"))]
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

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use serde::{Deserialize, Serialize};
    use sqlx::SqlitePool;

    use super::*;
    use crate::dependency::EntityList;
    use crate::deps;
    use crate::lifecycle::{Lifecycle, Never};
    use crate::{Effect, uneventful};

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
    struct AggregateA;

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
    struct AggregateB;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct EventA;

    impl DomainEvent for EventA {
        fn event_type(&self) -> String {
            "EventA".to_string()
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct EventB;

    impl DomainEvent for EventB {
        fn event_type(&self) -> String {
            "EventB".to_string()
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[async_trait]
    impl EventSourced for AggregateA {
        type Id = String;
        type Error = Never;
        type Command = ();
        type Event = EventA;
        type Materialized = Nil;
        type Jobs = Nil;

        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;
        const SNAPSHOT_SIZE: usize = 10;
        const AGGREGATE_TYPE: &'static str = "AggregateA";
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;

        fn originate(_event: &EventA) -> Option<Self> {
            Some(Self)
        }

        fn evolve(_entity: &Self, _event: &EventA) -> Result<Option<Self>, Never> {
            Ok(Some(Self))
        }

        async fn initialize(_command: ()) -> Result<Effect<Self>, Never> {
            uneventful()
        }

        async fn transition(&self, _command: ()) -> Result<Effect<Self>, Never> {
            uneventful()
        }
    }

    #[async_trait]
    impl EventSourced for AggregateB {
        type Id = String;
        type Error = Never;
        type Command = ();
        type Event = EventB;
        type Materialized = Nil;
        type Jobs = Nil;

        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;
        const SNAPSHOT_SIZE: usize = 10;
        const AGGREGATE_TYPE: &'static str = "AggregateB";
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;

        fn originate(_event: &EventB) -> Option<Self> {
            Some(Self)
        }

        fn evolve(_entity: &Self, _event: &EventB) -> Result<Option<Self>, Never> {
            Ok(Some(Self))
        }

        async fn initialize(_command: ()) -> Result<Effect<Self>, Never> {
            uneventful()
        }

        async fn transition(&self, _command: ()) -> Result<Effect<Self>, Never> {
            uneventful()
        }
    }

    struct MultiEntityReactor;

    deps!(MultiEntityReactor, [AggregateA, AggregateB]);

    #[async_trait]
    impl Reactor for MultiEntityReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            event
                .on(|_id, _event| async {})
                .on(|_id, _event| async {})
                .exhaustive()
                .await;
            Ok(())
        }
    }

    struct SingleEntityReactor;

    deps!(SingleEntityReactor, [AggregateA]);

    #[async_trait]
    impl Reactor for SingleEntityReactor {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            let (_id, _event) = event.into_inner();
            Ok(())
        }
    }

    #[tokio::test]
    async fn single_entity_wiring() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlite_es::MIGRATOR.run(&pool).await.unwrap();

        let _store = StoreBuilder::<AggregateA, _>::new(pool.clone())
            .with(Arc::new(SingleEntityReactor))
            .build()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn multi_entity_wiring() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlite_es::MIGRATOR.run(&pool).await.unwrap();

        let multi = Arc::new(MultiEntityReactor);
        let single = Arc::new(SingleEntityReactor);

        let _store_a = StoreBuilder::<AggregateA, _>::new(pool.clone())
            .with(multi.clone())
            .with(single)
            .build()
            .await
            .unwrap();

        let _store_b = StoreBuilder::<AggregateB, _>::new(pool.clone())
            .with(multi)
            .build()
            .await
            .unwrap();
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
    struct Tally {
        count: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum TallyEvent {
        Bumped,
    }

    impl DomainEvent for TallyEvent {
        fn event_type(&self) -> String {
            "TallyEvent::Bumped".to_string()
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[async_trait]
    impl EventSourced for Tally {
        type Id = String;
        type Error = Never;
        type Command = ();
        type Event = TallyEvent;
        type Materialized = Table;
        type Jobs = Nil;

        const PROJECTION: Table = Table("tally_view");
        const SCHEMA_VERSION: u64 = 1;
        const SNAPSHOT_SIZE: usize = 10;
        const AGGREGATE_TYPE: &'static str = "Tally";
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;

        fn originate(event: &TallyEvent) -> Option<Self> {
            match event {
                TallyEvent::Bumped => Some(Self { count: 1 }),
            }
        }

        fn evolve(entity: &Self, event: &TallyEvent) -> Result<Option<Self>, Never> {
            match event {
                TallyEvent::Bumped => Ok(Some(Self {
                    count: entity.count + 1,
                })),
            }
        }

        async fn initialize(_command: ()) -> Result<Effect<Self>, Never> {
            uneventful()
        }

        async fn transition(&self, _command: ()) -> Result<Effect<Self>, Never> {
            uneventful()
        }
    }

    /// A schema-version change (here, the first build, when no stored version
    /// exists) must rebuild views from the event log, healing a view that is
    /// current on event sequence but holds an incompatible payload -- the case
    /// `catch_up` alone never revisits because the view is not behind.
    #[tokio::test]
    async fn schema_change_rebuilds_incompatible_current_view() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlite_es::MIGRATOR.run(&pool).await.unwrap();

        sqlx::query(
            "CREATE TABLE tally_view ( \
                 view_id TEXT NOT NULL PRIMARY KEY, \
                 version BIGINT NOT NULL, \
                 payload TEXT NOT NULL \
             )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let payload = serde_json::to_string(&TallyEvent::Bumped).unwrap();
        for sequence in 1..=3 {
            sqlx::query(
                "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, \
                 event_version, payload, metadata) \
                 VALUES ('Tally', 'tally-1', ?1, 'TallyEvent::Bumped', '1.0', ?2, '{}')",
            )
            .bind(sequence)
            .bind(&payload)
            .execute(&pool)
            .await
            .unwrap();
        }

        // A view that is CURRENT (version == max_seq == 3) but holds an
        // incompatible payload. `catch_up` would never revisit it.
        sqlx::query("INSERT INTO tally_view (view_id, version, payload) VALUES ('tally-1', 3, ?1)")
            .bind(r#"{"Completed": {"count": 0}}"#)
            .execute(&pool)
            .await
            .unwrap();

        let (_store, _projection) = StoreBuilder::<Tally, _>::new(pool.clone())
            .build()
            .await
            .unwrap();

        let healed: String =
            sqlx::query_scalar("SELECT payload FROM tally_view WHERE view_id = 'tally-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        let lifecycle: Lifecycle<Tally> = serde_json::from_str(&healed).unwrap();
        assert!(matches!(lifecycle, Lifecycle::Live(Tally { count: 3 })));

        let version: i64 =
            sqlx::query_scalar("SELECT version FROM tally_view WHERE view_id = 'tally-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(version, 3);
    }

    /// A real version transition (a prior version is already recorded, then the
    /// code's `SCHEMA_VERSION` differs) also rebuilds incompatible current
    /// views. This distinguishes the genuine-mismatch trigger from the
    /// first-registration trigger above: a regression that gated `rebuild_all`
    /// on `stored_version.is_some()` would pass the test above but fail here.
    #[tokio::test]
    async fn schema_version_bump_rebuilds_incompatible_current_view() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlite_es::MIGRATOR.run(&pool).await.unwrap();

        sqlx::query(
            "CREATE TABLE tally_view ( \
                 view_id TEXT NOT NULL PRIMARY KEY, \
                 version BIGINT NOT NULL, \
                 payload TEXT NOT NULL \
             )",
        )
        .execute(&pool)
        .await
        .unwrap();

        // Seed a PRIOR recorded schema version (0) for Tally, so build() sees a
        // genuine 0 -> 1 mismatch rather than a first registration.
        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, \
             event_version, payload, metadata) \
             VALUES ('SchemaRegistry', 'schema', 1, 'SchemaRegistryEvent::VersionUpdated', '1.0', \
             '{\"VersionUpdated\":{\"name\":\"Tally\",\"version\":0}}', '{}')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let payload = serde_json::to_string(&TallyEvent::Bumped).unwrap();
        for sequence in 1..=3 {
            sqlx::query(
                "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, \
                 event_version, payload, metadata) \
                 VALUES ('Tally', 'tally-1', ?1, 'TallyEvent::Bumped', '1.0', ?2, '{}')",
            )
            .bind(sequence)
            .bind(&payload)
            .execute(&pool)
            .await
            .unwrap();
        }

        sqlx::query("INSERT INTO tally_view (view_id, version, payload) VALUES ('tally-1', 3, ?1)")
            .bind(r#"{"Completed": {"count": 0}}"#)
            .execute(&pool)
            .await
            .unwrap();

        let (_store, _projection) = StoreBuilder::<Tally, _>::new(pool.clone())
            .build()
            .await
            .unwrap();

        let healed: String =
            sqlx::query_scalar("SELECT payload FROM tally_view WHERE view_id = 'tally-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        let lifecycle: Lifecycle<Tally> = serde_json::from_str(&healed).unwrap();
        assert!(matches!(lifecycle, Lifecycle::Live(Tally { count: 3 })));

        // The version bookmark must advance to max_seq; otherwise the next
        // catch_up would re-replay every event and double-apply increments.
        let version: i64 =
            sqlx::query_scalar("SELECT version FROM tally_view WHERE view_id = 'tally-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(version, 3);
    }
}
