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

use cqrs_es::persist::{PersistedEventRepository, PersistedEventStore};
use cqrs_es::{CqrsFramework, Query};

use crate::Nil;
use crate::dependency::HasEntity;
use crate::lifecycle::{Lifecycle, ReactorBridge};
use crate::projection::{Projection, Table};
use crate::reactor::Reactor;
use crate::schema_registry::ReconcileError;
use crate::{
    CompactionPolicy, EventSourced, PersistenceBackend, ProjectedStoreBackend, Store,
    UnmaterializedStoreBackend,
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

        self.queries.push(Box::new(ReactorBridge {
            reactor: projection.clone(),
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
