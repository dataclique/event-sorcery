#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use serde::{Deserialize, Serialize};
    use sqlx::SqlitePool;

    use event_sorcery::{
        CompactionPolicy, Effect, EventSourced, LifecycleError, Never, Nil, Projection,
        ProjectionError, ReconcileError, StoreBuilder, StreamIdentity, Table, uneventful,
    };
    use sqlite_es::{SqliteBackend, SqliteViewBackend};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct RecoveryEntity<const ACCEPT_HISTORY: bool>;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum RecoveryEvent {
        Created,
    }

    impl DomainEvent for RecoveryEvent {
        fn event_type(&self) -> String {
            "RecoveryCreated".to_string()
        }

        fn event_version(&self) -> String {
            "1".to_string()
        }
    }

    #[async_trait]
    impl<const ACCEPT_HISTORY: bool> EventSourced for RecoveryEntity<ACCEPT_HISTORY> {
        type Id = String;
        type Command = ();
        type Event = RecoveryEvent;
        type Error = Never;
        type Materialized = Table;
        type Jobs = Nil;

        const AGGREGATE_TYPE: &'static str = "RecoveryEntity";
        const SCHEMA_VERSION: u64 = if ACCEPT_HISTORY { 1 } else { 2 };
        const SNAPSHOT_SIZE: usize = 2;
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;
        const PROJECTION: Table = Table("recovery_projection");

        fn originate(_event: &Self::Event) -> Option<Self> {
            ACCEPT_HISTORY.then_some(Self)
        }

        fn evolve(_entity: &Self, _event: &Self::Event) -> Result<Option<Self>, Never> {
            Ok(None)
        }

        async fn initialize((): ()) -> Result<Effect<Self>, Never> {
            Ok(Effect::Events(vec![RecoveryEvent::Created]))
        }

        async fn transition(&self, (): ()) -> Result<Effect<Self>, Never> {
            uneventful()
        }
    }

    #[tokio::test]
    async fn catch_up_rejects_history_that_enters_a_failed_lifecycle() {
        let pool = sqlite_es::testing::create_test_pool().await.unwrap();
        sqlx::query(
            "CREATE TABLE recovery_projection (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        let (store, _) =
            StoreBuilder::<RecoveryEntity<true>, SqliteBackend>::with_backend(SqliteBackend {
                pool: pool.clone(),
            })
            .build()
            .await
            .unwrap();
        store.send(&"legacy".to_string(), ()).await.unwrap();
        drop(store);
        sqlx::query("DELETE FROM recovery_projection")
            .execute(&pool)
            .await
            .unwrap();
        let projection =
            Projection::<RecoveryEntity<false>, SqliteViewBackend>::with_backend(pool.clone());
        let error = projection.catch_up().await.unwrap_err();
        assert!(matches!(error, ProjectionError::Lifecycle(source)
            if matches!(*source, LifecycleError::EventCantOriginate { event: RecoveryEvent::Created })));
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM recovery_projection")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Counter {
        value: i64,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum CounterEvent {
        Created { initial: i64 },
        Incremented,
    }

    impl DomainEvent for CounterEvent {
        fn event_type(&self) -> String {
            match self {
                Self::Created { .. } => "CounterEvent::Created".to_string(),
                Self::Incremented => "CounterEvent::Incremented".to_string(),
            }
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[async_trait]
    impl EventSourced for Counter {
        type Id = String;
        type Error = Never;
        type Command = usize;
        type Event = CounterEvent;
        type Materialized = Table;
        type Jobs = Nil;

        const PROJECTION: Table = Table("counter_view");
        const SCHEMA_VERSION: u64 = 1;
        const SNAPSHOT_SIZE: usize = 10;
        const AGGREGATE_TYPE: &'static str = "Counter";
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;

        fn originate(event: &CounterEvent) -> Option<Self> {
            match event {
                CounterEvent::Created { initial } => Some(Self { value: *initial }),
                CounterEvent::Incremented => None,
            }
        }

        fn evolve(entity: &Self, event: &CounterEvent) -> Result<Option<Self>, Never> {
            match event {
                CounterEvent::Created { .. } => Ok(None),
                CounterEvent::Incremented => Ok(Some(Self {
                    value: entity.value + 1,
                })),
            }
        }

        async fn initialize(_increments: usize) -> Result<Effect<Self>, Never> {
            Ok(Effect::Events(vec![CounterEvent::Created { initial: 4 }]))
        }

        async fn transition(&self, increments: usize) -> Result<Effect<Self>, Never> {
            Ok(Effect::Events(vec![CounterEvent::Incremented; increments]))
        }
    }

    #[tokio::test]
    async fn native_dispatch_recovers_its_stream_even_if_an_unrelated_stream_cannot_replay() {
        let pool = sqlite_es::testing::create_test_pool().await.unwrap();
        sqlx::query(
            "CREATE TABLE counter_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload TEXT NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        let (store, projection) =
            StoreBuilder::<Counter, SqliteBackend>::with_backend(SqliteBackend {
                pool: pool.clone(),
            })
            .build()
            .await
            .unwrap();
        let current_id = "z-current".to_owned();
        let unrelated_id = "a-unrelated".to_owned();
        store.send(&current_id, 0).await.unwrap();
        store.send(&current_id, 1).await.unwrap();
        store.send(&unrelated_id, 0).await.unwrap();
        sqlx::query("DELETE FROM counter_view")
            .execute(&pool)
            .await
            .unwrap();
        // A distinguishable checkpoint catches accidental replay from sequence zero.
        insert_stale_view(&pool, &current_id, 1, &Counter { value: 10 }).await;
        sqlx::query(
            "CREATE TRIGGER reject_unrelated_recovery
             BEFORE INSERT ON counter_view WHEN NEW.view_id = 'a-unrelated'
             BEGIN SELECT RAISE(ABORT, 'unrelated projection write rejected'); END",
        )
        .execute(&pool)
        .await
        .unwrap();

        store.send(&current_id, 2).await.unwrap();

        assert_eq!(
            projection.load(&current_id).await.unwrap(),
            Some(Counter { value: 13 })
        );
        let checkpoint: i64 =
            sqlx::query_scalar("SELECT version FROM counter_view WHERE view_id = ?")
                .bind(&current_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(checkpoint, 4);
        assert_eq!(projection.load(&unrelated_id).await.unwrap(), None);
    }

    async fn setup_catch_up_db() -> SqlitePool {
        let pool = sqlite_es::testing::create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE counter_view ( \
                 view_id TEXT NOT NULL PRIMARY KEY, \
                 version BIGINT NOT NULL, \
                 payload TEXT NOT NULL \
             )",
        )
        .execute(&pool)
        .await
        .unwrap();

        pool
    }

    async fn insert_event(
        pool: &SqlitePool,
        aggregate_id: &str,
        sequence: i64,
        event: &CounterEvent,
    ) {
        let payload = serde_json::to_string(event).unwrap();
        let event_type = DomainEvent::event_type(event);

        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES (?1, ?2, ?3, ?4, '1.0', ?5, '{}')",
        )
        .bind("Counter")
        .bind(aggregate_id)
        .bind(sequence)
        .bind(&event_type)
        .bind(&payload)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_stale_view(pool: &SqlitePool, view_id: &str, version: i64, counter: &Counter) {
        let payload = serde_json::json!({"Live": counter}).to_string();

        sqlx::query("INSERT INTO counter_view (view_id, version, payload) VALUES (?1, ?2, ?3)")
            .bind(view_id)
            .bind(version)
            .bind(&payload)
            .execute(pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn load_all_orders_views_by_id_not_insertion_order() {
        let pool = setup_catch_up_db().await;
        insert_stale_view(&pool, "z", 1, &Counter { value: 2 }).await;
        insert_stale_view(&pool, "a", 1, &Counter { value: 1 }).await;
        let projection = Projection::<Counter, SqliteViewBackend>::with_backend(pool);

        assert_eq!(
            projection.load_all().await.unwrap(),
            [
                ("a".to_owned(), Counter { value: 1 }),
                ("z".to_owned(), Counter { value: 2 })
            ],
        );
    }

    #[tokio::test]
    async fn catch_up_replays_missed_events() {
        let pool = setup_catch_up_db().await;

        // Insert 3 events: Created(0), Incremented, Incremented
        insert_event(&pool, "counter-1", 1, &CounterEvent::Created { initial: 0 }).await;
        insert_event(&pool, "counter-1", 2, &CounterEvent::Incremented).await;
        insert_event(&pool, "counter-1", 3, &CounterEvent::Incremented).await;

        // View is stale at version 1 (only saw Created)
        insert_stale_view(&pool, "counter-1", 1, &Counter { value: 0 }).await;

        let projection = Projection::<Counter, SqliteViewBackend>::with_backend(pool);

        projection.catch_up().await.unwrap();

        let result = projection.load(&"counter-1".to_string()).await.unwrap();
        assert_eq!(result, Some(Counter { value: 2 }));
    }

    #[tokio::test]
    async fn catch_up_skips_up_to_date_views() {
        let pool = setup_catch_up_db().await;

        insert_event(&pool, "counter-1", 1, &CounterEvent::Created { initial: 5 }).await;

        // View is up to date at version 1
        insert_stale_view(&pool, "counter-1", 1, &Counter { value: 5 }).await;

        let projection = Projection::<Counter, SqliteViewBackend>::with_backend(pool);

        projection.catch_up().await.unwrap();

        let result = projection.load(&"counter-1".to_string()).await.unwrap();
        assert_eq!(result, Some(Counter { value: 5 }));
    }

    #[tokio::test]
    async fn catch_up_with_no_events_is_noop() {
        let pool = setup_catch_up_db().await;

        let projection = Projection::<Counter, SqliteViewBackend>::with_backend(pool);

        projection.catch_up().await.unwrap();
    }

    #[tokio::test]
    async fn catch_up_is_idempotent() {
        let pool = setup_catch_up_db().await;

        insert_event(&pool, "counter-1", 1, &CounterEvent::Created { initial: 0 }).await;
        insert_event(&pool, "counter-1", 2, &CounterEvent::Incremented).await;
        insert_event(&pool, "counter-1", 3, &CounterEvent::Incremented).await;

        insert_stale_view(&pool, "counter-1", 1, &Counter { value: 0 }).await;

        let projection = Projection::<Counter, SqliteViewBackend>::with_backend(pool.clone());

        projection.catch_up().await.unwrap();
        projection.catch_up().await.unwrap();

        let result = projection.load(&"counter-1".to_string()).await.unwrap();
        assert_eq!(result, Some(Counter { value: 2 }));

        // Verify version is correct (should be 3, matching max event sequence)
        let (version,): (i64,) =
            sqlx::query_as("SELECT version FROM counter_view WHERE view_id = 'counter-1'")
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(version, 3);
    }

    #[tokio::test]
    async fn catch_up_rebuilds_missing_view_row() {
        let pool = setup_catch_up_db().await;

        // Events exist but no view row (crash before initial view write)
        insert_event(
            &pool,
            "counter-1",
            1,
            &CounterEvent::Created { initial: 10 },
        )
        .await;
        insert_event(&pool, "counter-1", 2, &CounterEvent::Incremented).await;

        let projection = Projection::<Counter, SqliteViewBackend>::with_backend(pool);

        projection.catch_up().await.unwrap();

        let result = projection.load(&"counter-1".to_string()).await.unwrap();
        assert_eq!(result, Some(Counter { value: 11 }));
    }

    #[tokio::test]
    async fn catch_up_tolerates_sequence_gap() {
        let pool = setup_catch_up_db().await;

        // Insert events with a gap: seq 1 and seq 3 (missing seq 2). A stale
        // view at version 1 expects 2 missed events (3 - 1) but only 1 exists.
        // Catch-up must not abort: it replays the available events in sequence
        // order and advances the view to max_seq.
        insert_event(&pool, "counter-1", 1, &CounterEvent::Created { initial: 0 }).await;
        insert_event(&pool, "counter-1", 3, &CounterEvent::Incremented).await;

        insert_stale_view(&pool, "counter-1", 1, &Counter { value: 0 }).await;

        let projection = Projection::<Counter, SqliteViewBackend>::with_backend(pool.clone());

        projection.catch_up().await.unwrap();

        let result = projection.load(&"counter-1".to_string()).await.unwrap();
        assert_eq!(result, Some(Counter { value: 1 }));

        let (version,): (i64,) =
            sqlx::query_as("SELECT version FROM counter_view WHERE view_id = 'counter-1'")
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(
            version, 3,
            "view advances to max event sequence despite the gap"
        );
    }

    #[tokio::test]
    async fn catch_up_and_rebuild_accept_sparse_unsnapshotted_history() {
        let pool = setup_catch_up_db().await;

        // Some stores assign sequences from a counter shared across aggregates,
        // so a single aggregate's sequences are sparse (never 1..N). With no
        // view row yet, catch-up must build from scratch by replaying every
        // event in sequence order regardless of the gaps between them.
        insert_event(
            &pool,
            "counter-1",
            24,
            &CounterEvent::Created { initial: 0 },
        )
        .await;
        insert_event(&pool, "counter-1", 4559, &CounterEvent::Incremented).await;
        insert_event(&pool, "counter-1", 4560, &CounterEvent::Incremented).await;
        insert_event(&pool, "counter-1", 4561, &CounterEvent::Incremented).await;

        let projection = Projection::<Counter, SqliteViewBackend>::with_backend(pool.clone());

        projection.catch_up().await.unwrap();

        let result = projection.load(&"counter-1".to_string()).await.unwrap();
        assert_eq!(result, Some(Counter { value: 3 }));

        let (version,): (i64,) =
            sqlx::query_as("SELECT version FROM counter_view WHERE view_id = 'counter-1'")
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(version, 4561);

        // Explicit rebuild must replay sparse history too, even when the
        // existing checkpoint would make catch-up skip the row.
        sqlx::query("UPDATE counter_view SET payload = ? WHERE view_id = 'counter-1'")
            .bind(r#"{"Live":{"value":99}}"#)
            .execute(&pool)
            .await
            .unwrap();
        projection.rebuild(&"counter-1".to_owned()).await.unwrap();
        assert_eq!(
            projection.load(&"counter-1".to_owned()).await.unwrap(),
            Some(Counter { value: 3 })
        );
        let rebuilt_version: i64 =
            sqlx::query_scalar("SELECT version FROM counter_view WHERE view_id = 'counter-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(rebuilt_version, 4561);
    }

    #[tokio::test]
    async fn catch_up_recovers_from_incompatible_view_payload() {
        let pool = setup_catch_up_db().await;

        insert_event(&pool, "counter-1", 1, &CounterEvent::Created { initial: 0 }).await;
        insert_event(&pool, "counter-1", 2, &CounterEvent::Incremented).await;
        insert_event(&pool, "counter-1", 3, &CounterEvent::Incremented).await;

        // Pre-Lifecycle wire format: bare entity variant instead of
        // `{"Live": {...}}`.
        sqlx::query("INSERT INTO counter_view (view_id, version, payload) VALUES (?1, ?2, ?3)")
            .bind("counter-1")
            .bind(1_i64)
            .bind(r#"{"Completed": {"value": 0}}"#)
            .execute(&pool)
            .await
            .unwrap();

        let projection = Projection::<Counter, SqliteViewBackend>::with_backend(pool.clone());
        projection.catch_up().await.unwrap();

        let payload: String =
            sqlx::query_scalar("SELECT payload FROM counter_view WHERE view_id = 'counter-1'")
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&payload).unwrap(),
            serde_json::json!({"Live": {"value": 2}})
        );

        // The version bookmark must advance to max_seq after recovery; a
        // regression that left it at the stale version would re-replay every
        // event on each subsequent catch_up and double-apply increments.
        let version: i64 =
            sqlx::query_scalar("SELECT version FROM counter_view WHERE view_id = 'counter-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(version, 3);
    }

    #[tokio::test]
    async fn catch_up_fails_on_malformed_payload() {
        let pool = setup_catch_up_db().await;

        // Insert a valid first event, then a malformed payload at seq 2
        insert_event(&pool, "counter-1", 1, &CounterEvent::Created { initial: 0 }).await;

        sqlx::query(
            "INSERT INTO events \
             (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             VALUES (?1, ?2, ?3, ?4, '1.0', ?5, '{}')",
        )
        .bind("Counter")
        .bind("counter-1")
        .bind(2_i64)
        .bind("CounterEvent::Incremented")
        .bind("not valid json {{{")
        .execute(&pool)
        .await
        .unwrap();

        insert_stale_view(&pool, "counter-1", 1, &Counter { value: 0 }).await;

        let projection = Projection::<Counter, SqliteViewBackend>::with_backend(pool);

        let error = projection.catch_up().await.unwrap_err();
        assert!(
            matches!(error, ProjectionError::Serde { .. }),
            "expected Serde error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn rebuild_refuses_snapshot_only_history_after_switching_to_retain() {
        let pool = sqlite_es::testing::create_test_pool().await.unwrap();
        sqlx::query(
            "CREATE TABLE counter_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload TEXT NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        let engine = sqlite_es::Engine::new(pool.clone());
        let (store, projection) =
            StoreBuilder::<Counter, _>::with_backend(SqliteBackend { pool: pool.clone() })
                .build()
                .await
                .unwrap();
        let id = "compacted".to_owned();
        store.send(&id, 0).await.unwrap();
        let stream = StreamIdentity::new(Counter::AGGREGATE_TYPE, id.clone());
        engine
            .store_snapshot(
                &stream,
                sqlite_es::SnapshotWrite::new(serde_json::json!({"Live": {"value": 4}}), 1),
            )
            .await
            .unwrap();
        assert_eq!(
            sqlite_es::compact_events(
                &pool,
                Counter::AGGREGATE_TYPE,
                CompactionPolicy::CompactAfterSnapshot
            )
            .await
            .unwrap(),
            1
        );
        let snapshot = engine.load_snapshot(&stream).await.unwrap().unwrap();
        let rows = sqlite_es::projection_rows(&pool, "counter_view")
            .await
            .unwrap();

        for result in [
            projection.rebuild(&id).await,
            projection.rebuild_all().await,
        ] {
            assert!(matches!(result,
                Err(ProjectionError::UnprovenSnapshotHistory(ReconcileError::UnprovenSnapshotHistory {
                    stream: rejected, covered_sequence: 1, retained_events: 0,
                })) if rejected == stream
            ));
        }
        assert_eq!(
            sqlite_es::projection_rows(&pool, "counter_view")
                .await
                .unwrap(),
            rows
        );
        assert_eq!(
            projection.load(&id).await.unwrap(),
            Some(Counter { value: 4 })
        );
        assert_eq!(engine.load_snapshot(&stream).await.unwrap(), Some(snapshot));
    }

    #[tokio::test]
    async fn rebuild_replays_all_events_from_scratch() {
        let pool = setup_catch_up_db().await;

        // Insert events and a corrupted view (value should be 2, not 99)
        insert_event(&pool, "counter-1", 1, &CounterEvent::Created { initial: 0 }).await;
        insert_event(&pool, "counter-1", 2, &CounterEvent::Incremented).await;
        insert_event(&pool, "counter-1", 3, &CounterEvent::Incremented).await;

        insert_stale_view(&pool, "counter-1", 3, &Counter { value: 99 }).await;

        let projection = Projection::<Counter, SqliteViewBackend>::with_backend(pool);

        // catch_up would not fix this because version matches max_seq
        projection.rebuild(&"counter-1".to_string()).await.unwrap();

        let result = projection.load(&"counter-1".to_string()).await.unwrap();
        assert_eq!(result, Some(Counter { value: 2 }));
    }

    #[tokio::test]
    async fn rebuild_all_replays_all_aggregates() {
        let pool = setup_catch_up_db().await;

        // Two aggregates with corrupted views
        insert_event(&pool, "counter-1", 1, &CounterEvent::Created { initial: 0 }).await;
        insert_event(&pool, "counter-1", 2, &CounterEvent::Incremented).await;
        insert_event(
            &pool,
            "counter-2",
            1,
            &CounterEvent::Created { initial: 10 },
        )
        .await;

        insert_stale_view(&pool, "counter-1", 2, &Counter { value: 99 }).await;
        insert_stale_view(&pool, "counter-2", 1, &Counter { value: 99 }).await;

        let projection = Projection::<Counter, SqliteViewBackend>::with_backend(pool);

        projection.rebuild_all().await.unwrap();

        let result1 = projection.load(&"counter-1".to_string()).await.unwrap();
        assert_eq!(result1, Some(Counter { value: 1 }));

        let result2 = projection.load(&"counter-2".to_string()).await.unwrap();
        assert_eq!(result2, Some(Counter { value: 10 }));
    }
}
