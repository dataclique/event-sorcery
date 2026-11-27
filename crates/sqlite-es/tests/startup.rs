#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use serde::{Deserialize, Serialize};
    use sqlx::SqlitePool;

    use std::sync::Arc;

    use event_sorcery::{
        CompactionPolicy, Effect, EntityList, EventSourced, Never, Nil, Reactor, StoreBuilder,
        Table, deps, uneventful,
    };
    use sqlite_es::SqliteBackend;

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

        let _store =
            StoreBuilder::<AggregateA, _>::with_backend(SqliteBackend { pool: pool.clone() })
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

        let _store_a =
            StoreBuilder::<AggregateA, _>::with_backend(SqliteBackend { pool: pool.clone() })
                .with(multi.clone())
                .with(single)
                .build()
                .await
                .unwrap();

        let _store_b =
            StoreBuilder::<AggregateB, _>::with_backend(SqliteBackend { pool: pool.clone() })
                .with(multi)
                .build()
                .await
                .unwrap();
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
    struct Tally<const VERSION: u64 = 1> {
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
    impl<const VERSION: u64> EventSourced for Tally<VERSION> {
        type Id = String;
        type Error = Never;
        type Command = ();
        type Event = TallyEvent;
        type Materialized = Table;
        type Jobs = Nil;

        const PROJECTION: Table = Table("tally_view");
        const SCHEMA_VERSION: u64 = VERSION;
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

        async fn initialize((): ()) -> Result<Effect<Self>, Never> {
            Ok(Effect::Events(vec![TallyEvent::Bumped]))
        }

        async fn transition(&self, (): ()) -> Result<Effect<Self>, Never> {
            Self::initialize(()).await
        }
    }

    #[tokio::test]
    async fn startup_rebuild_failure_restores_views_and_leaves_registry_unchanged() {
        let pool = sqlite_es::testing::create_test_pool().await.unwrap();
        let backend = SqliteBackend { pool: pool.clone() };
        sqlx::query(
            "CREATE TABLE tally_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload TEXT NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        let (store, _) = StoreBuilder::<Tally, _>::with_backend(backend.clone())
            .build()
            .await
            .unwrap();
        for id in ["a", "z"] {
            store.send(&id.to_owned(), ()).await.unwrap();
        }
        drop(store);

        sqlx::query("UPDATE tally_view SET payload = ?")
            .bind(r#"{"Live":{"count":77}}"#)
            .execute(&pool)
            .await
            .unwrap();
        let before = sqlite_es::projection_rows(&pool, "tally_view")
            .await
            .unwrap();
        let registry = sqlite_es::load_schema_registry_payloads(&pool)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TRIGGER reject_second_rebuild
             BEFORE INSERT ON tally_view
             WHEN NEW.view_id = 'z' AND EXISTS (SELECT 1 FROM tally_view WHERE view_id = 'a')
             BEGIN SELECT RAISE(ABORT, 'second stream rejected'); END",
        )
        .execute(&pool)
        .await
        .unwrap();

        let result = StoreBuilder::<Tally<2>, _>::with_backend(backend.clone())
            .build()
            .await;

        let Err(event_sorcery::ReconcileError::Sqlx(sqlx::Error::Database(error))) = result else {
            panic!("expected the second rebuild write to fail");
        };
        assert_eq!(error.message(), "second stream rejected");
        assert_eq!(
            sqlite_es::projection_rows(&pool, "tally_view")
                .await
                .unwrap(),
            before
        );
        assert_eq!(
            sqlite_es::load_schema_registry_payloads(&pool)
                .await
                .unwrap(),
            registry
        );

        sqlx::query("DROP TRIGGER reject_second_rebuild")
            .execute(&pool)
            .await
            .unwrap();
        let (_, projection) = StoreBuilder::<Tally<2>, _>::with_backend(backend)
            .build()
            .await
            .unwrap();
        assert_eq!(
            projection.load_all().await.unwrap(),
            [
                ("a".to_owned(), Tally { count: 1 }),
                ("z".to_owned(), Tally { count: 1 })
            ]
        );
        let registry = sqlite_es::load_schema_registry_payloads(&pool)
            .await
            .unwrap();
        let registry = event_sorcery::SchemaRegistry::from_payloads(registry)
            .unwrap()
            .unwrap();
        assert_eq!(registry.version_of("Tally"), Some(2));
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

        let (_store, projection) =
            StoreBuilder::<Tally, _>::with_backend(SqliteBackend { pool: pool.clone() })
                .build()
                .await
                .unwrap();

        let healed = projection
            .load(&"tally-1".to_owned())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(healed.count, 3);

        let version: i64 =
            sqlx::query_scalar("SELECT version FROM tally_view WHERE view_id = 'tally-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(version, 3);
    }

    #[tokio::test]
    async fn schema_version_bump_repairs_legacy_revision_before_live_delivery() {
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

        let backend = SqliteBackend { pool: pool.clone() };
        let (old_store, _) = StoreBuilder::<Tally<0>, _>::with_backend(backend.clone())
            .build()
            .await
            .unwrap();
        let id = "tally-1".to_owned();
        old_store.send(&id, ()).await.unwrap();
        old_store.send(&id, ()).await.unwrap();
        drop(old_store);
        // A legacy duplicate delivery advanced the revision, not the event stream.
        sqlx::query("UPDATE tally_view SET version = 3 WHERE view_id = ?")
            .bind(&id)
            .execute(&pool)
            .await
            .unwrap();

        let (store, projection) = StoreBuilder::<Tally, _>::with_backend(backend)
            .build()
            .await
            .unwrap();
        assert_eq!(
            projection.load(&id).await.unwrap(),
            Some(Tally { count: 2 })
        );
        let version_query = "SELECT version FROM tally_view WHERE view_id = ?";
        let version: i64 = sqlx::query_scalar(version_query)
            .bind(&id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, 2);

        store.send(&id, ()).await.unwrap();
        assert_eq!(
            projection.load(&id).await.unwrap(),
            Some(Tally { count: 3 })
        );
        let version: i64 = sqlx::query_scalar(version_query)
            .bind(&id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, 3);
    }
}
