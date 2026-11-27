//! Repository integration coverage for atomic engine delegation.

#[cfg(test)]
mod tests {
    use cqrs_es::event_sink::EventSink;
    use cqrs_es::persist::{
        EventUpcaster, PersistedEventRepository, PersistedEventStore, PersistenceError,
        SerializedEvent, SerializedSnapshot,
    };
    use cqrs_es::{
        Aggregate, AggregateContext, AggregateError, CqrsFramework, DomainEvent, EventStore,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::collections::HashMap;
    use std::fmt::{self, Display};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    use event_sorcery::{
        CompactionPolicy, Effect, EngineError, EventBackend, EventSourced, JobId, JobSeed, Never,
        Nil, StreamIdentity, buffer_pending_job, with_pending_jobs,
    };
    use sqlite_es::testing::create_test_pool;
    use sqlite_es::{Engine, NativeRepository, SqliteBackend, StoredEventRow, compact_events};
    use sqlx::SqlitePool;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
    struct TestAggregate {
        events: Vec<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    enum TestEvent {
        Created,
    }

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> String {
            "Created".to_string()
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug)]
    struct TestError;

    impl Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "test error")
        }
    }

    impl std::error::Error for TestError {}

    impl Aggregate for TestAggregate {
        const TYPE: &'static str = "TestAggregate";
        type Command = ();
        type Event = TestEvent;
        type Error = TestError;
        type Services = ();

        async fn handle(
            &mut self,
            _command: Self::Command,
            _services: &Self::Services,
            _sink: &EventSink<Self>,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn apply(&mut self, event: Self::Event) {
            self.events.push(event.event_type());
        }
    }

    #[tokio::test]
    async fn repository_rejects_a_snapshot_for_a_different_stream() {
        let pool = create_test_pool().await.unwrap();
        let repository = NativeRepository::new(pool.clone(), CompactionPolicy::Retain);
        let events = covering_events("event-stream", 1);
        let snapshot = SerializedSnapshot {
            aggregate_id: "snapshot-stream".to_owned(),
            aggregate: serde_json::json!({"events": ["Created"]}),
            current_sequence: 1,
            current_snapshot: 1,
        };

        let error = repository
            .persist_with_snapshot::<TestAggregate>(&events, Some(snapshot))
            .await
            .unwrap_err();

        let PersistenceError::UnknownError(source) = error else {
            panic!("expected stream identity mismatch, got {error:?}");
        };
        assert!(
            matches!(source.downcast_ref::<EngineError>(), Some(EngineError::StreamIdentityMismatch { expected, actual })
            if expected == &StreamIdentity::new(TestAggregate::TYPE, "snapshot-stream")
                && actual == &StreamIdentity::new(TestAggregate::TYPE, "event-stream"))
        );
        let counts: (i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM events), (SELECT COUNT(*) FROM snapshots)",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(counts, (0, 0));
    }

    #[tokio::test]
    async fn repository_rejects_gapped_appends_without_writing() {
        let pool = create_test_pool().await.unwrap();
        let repository = NativeRepository::new(pool, CompactionPolicy::Retain);
        let events = covering_events("version-guard", 4);
        for sequence in [2, 0] {
            let mut event = events[0].clone();
            event.sequence = sequence;
            let result = repository.persist::<TestAggregate>(&[event], None).await;
            assert!(
                matches!(result, Err(PersistenceError::OptimisticLockError)),
                "{result:?}"
            );
            assert!(
                repository
                    .get_events::<TestAggregate>("version-guard")
                    .await
                    .unwrap()
                    .is_empty()
            );
        }

        repository
            .persist::<TestAggregate>(&events[..1], None)
            .await
            .unwrap();
        let before = repository
            .get_events::<TestAggregate>("version-guard")
            .await
            .unwrap();
        for sequence in [1, 3] {
            let event = events[sequence - 1].clone();
            let result = repository.persist::<TestAggregate>(&[event], None).await;
            assert!(
                matches!(result, Err(PersistenceError::OptimisticLockError)),
                "{result:?}"
            );
            assert_eq!(
                repository
                    .get_events::<TestAggregate>("version-guard")
                    .await
                    .unwrap(),
                before
            );
        }
        let result = repository
            .persist::<TestAggregate>(&[events[1].clone(), events[3].clone()], None)
            .await;
        assert!(
            matches!(result, Err(PersistenceError::OptimisticLockError)),
            "{result:?}"
        );
        assert_eq!(
            repository
                .get_events::<TestAggregate>("version-guard")
                .await
                .unwrap(),
            before
        );
        repository
            .persist::<TestAggregate>(&events[1..2], None)
            .await
            .unwrap();
        assert_eq!(
            repository
                .get_events::<TestAggregate>("version-guard")
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn snapshot_checkpoint_cannot_move_backwards_or_repeat() {
        let pool = create_test_pool().await.unwrap();
        let repository = NativeRepository::new(pool.clone(), CompactionPolicy::Retain);
        let store = PersistedEventStore::<_, TestAggregate>::new_snapshot_store(repository, 2);
        let context = store.load_aggregate("checkpoint-guard").await.unwrap();
        store
            .commit(vec![TestEvent::Created; 3], context, HashMap::new())
            .await
            .unwrap();
        let repository = NativeRepository::new(pool, CompactionPolicy::Retain);
        let before = repository
            .get_events::<TestAggregate>("checkpoint-guard")
            .await
            .unwrap();
        let mut next = before[0].clone();
        next.sequence = 4;

        for (current_sequence, aggregate) in [
            (1, serde_json::json!({"events": ["Created"]})),
            (2, serde_json::json!({"events": ["Created", "Created"]})),
        ] {
            let result = repository
                .persist_with_snapshot::<TestAggregate>(
                    &[next.clone()],
                    Some(SerializedSnapshot {
                        aggregate_id: "checkpoint-guard".to_string(),
                        aggregate,
                        current_sequence,
                        current_snapshot: 2,
                    }),
                )
                .await;
            assert!(
                matches!(result, Err(PersistenceError::OptimisticLockError)),
                "{result:?}"
            );
            assert_eq!(
                repository
                    .get_events::<TestAggregate>("checkpoint-guard")
                    .await
                    .unwrap(),
                before
            );
            let snapshot = repository
                .get_snapshot::<TestAggregate>("checkpoint-guard")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(snapshot.current_sequence, 2);
            assert_eq!(snapshot.current_snapshot, 1);
            assert_eq!(
                snapshot.aggregate,
                serde_json::json!({"events": ["Created", "Created"]})
            );
        }
    }

    #[tokio::test]
    async fn prefix_snapshot_preserves_suffix_and_rejects_excess_coverage() {
        let pool = create_test_pool().await.unwrap();
        let repository = NativeRepository::new(pool.clone(), CompactionPolicy::Retain);
        let store = PersistedEventStore::<_, TestAggregate>::new_snapshot_store(repository, 2);
        let context = store.load_aggregate("snapshot-prefix").await.unwrap();
        store
            .commit(vec![TestEvent::Created; 3], context, HashMap::new())
            .await
            .unwrap();

        let loaded = store.load_aggregate("snapshot-prefix").await.unwrap();
        assert_eq!(loaded.aggregate.events, ["Created", "Created", "Created"]);
        assert_eq!(loaded.current_sequence, 3);
        let repository = NativeRepository::new(pool, CompactionPolicy::Retain);
        let snapshot = repository
            .get_snapshot::<TestAggregate>("snapshot-prefix")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.current_sequence, 2);
        assert_eq!(snapshot.current_snapshot, 1);
        assert_eq!(
            snapshot.aggregate,
            serde_json::json!({"events": ["Created", "Created"]})
        );

        let events = repository
            .get_events::<TestAggregate>("snapshot-prefix")
            .await
            .unwrap();
        let mut next_event = events[0].clone();
        next_event.sequence = 4;
        let error = repository
            .persist_with_snapshot::<TestAggregate>(
                &[next_event],
                Some(SerializedSnapshot {
                    current_sequence: 5,
                    current_snapshot: 2,
                    ..snapshot
                }),
            )
            .await
            .unwrap_err();
        let PersistenceError::UnknownError(source) = error else {
            panic!("expected a typed excessive-coverage error");
        };
        assert!(matches!(
            source.downcast_ref::<EngineError>().unwrap(),
            EngineError::SnapshotBeyondCurrentVersion {
                proposed: 5,
                current: 4
            }
        ));
        let after = repository
            .get_events::<TestAggregate>("snapshot-prefix")
            .await
            .unwrap();
        assert_eq!(after, events);
        let snapshot = repository
            .get_snapshot::<TestAggregate>("snapshot-prefix")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.current_sequence, 2);
        assert_eq!(snapshot.current_snapshot, 1);
        assert_eq!(
            snapshot.aggregate,
            serde_json::json!({"events": ["Created", "Created"]})
        );
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    struct PreviousSnapshotAggregate {
        events: String,
    }

    impl Aggregate for PreviousSnapshotAggregate {
        const TYPE: &'static str = TestAggregate::TYPE;
        type Command = ();
        type Event = TestEvent;
        type Error = TestError;
        type Services = ();

        async fn handle(
            &mut self,
            _command: Self::Command,
            _services: &Self::Services,
            event_sink: &EventSink<Self>,
        ) -> Result<(), Self::Error> {
            let mut scratch = self.clone();
            event_sink.write(TestEvent::Created, &mut scratch).await;
            Ok(())
        }

        fn apply(&mut self, event: Self::Event) {
            self.events.push_str(&event.event_type());
        }
    }

    #[async_trait::async_trait]
    impl EventSourced for PreviousSnapshotAggregate {
        type Id = String;
        type Error = Never;
        type Command = ();
        type Event = TestEvent;
        type Materialized = Nil;
        type Jobs = Nil;

        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;
        const SNAPSHOT_SIZE: usize = 1;
        const AGGREGATE_TYPE: &'static str = <Self as Aggregate>::TYPE;
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::RetainLast { events: 2 };

        fn originate(event: &TestEvent) -> Option<Self> {
            Some(Self {
                events: event.event_type(),
            })
        }

        fn evolve(entity: &Self, event: &TestEvent) -> Result<Option<Self>, Never> {
            let mut events = entity.events.clone();
            events.push_str(&event.event_type());
            Ok(Some(Self { events }))
        }

        async fn initialize(_command: ()) -> Result<Effect<Self>, Never> {
            Ok(Effect::Events(vec![TestEvent::Created]))
        }

        async fn transition(&self, _command: ()) -> Result<Effect<Self>, Never> {
            Ok(Effect::Events(vec![TestEvent::Created]))
        }
    }

    fn covering_events(aggregate_id: &str, through: usize) -> Vec<SerializedEvent> {
        (1..=through)
            .map(|sequence| SerializedEvent {
                aggregate_type: TestAggregate::TYPE.to_string(),
                aggregate_id: aggregate_id.to_string(),
                sequence,
                event_type: "Created".to_string(),
                event_version: "1.0".to_string(),
                payload: serde_json::json!("Created"),
                metadata: serde_json::json!({}),
            })
            .collect()
    }

    async fn persist_with_pending_job(
        pool: SqlitePool,
        aggregate_id: &str,
        job_id: JobId,
    ) -> Result<(), PersistenceError> {
        let repo = NativeRepository::new(pool, CompactionPolicy::Retain);
        with_pending_jobs(async move {
            buffer_pending_job(
                job_id,
                "test-pending",
                serde_json::json!({"test": "pending"}),
            )
            .unwrap();

            repo.persist::<TestAggregate>(&covering_events(aggregate_id, 1), None)
                .await
        })
        .await
    }

    async fn count_events(pool: &SqlitePool, aggregate_type: &str, aggregate_id: &str) -> i64 {
        sqlx::query_scalar::<_, i64>(
            r"
        SELECT COUNT(*)
        FROM events
        WHERE aggregate_type = ?1 AND aggregate_id = ?2
        ",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn count_queue_rows(pool: &SqlitePool, job_id: &str) -> i64 {
        sqlx::query_scalar::<_, i64>(
            r"
        SELECT COUNT(*)
        FROM job_queue_projection
        WHERE view_id = ?1
        ",
        )
        .bind(job_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn count_snapshots(pool: &SqlitePool, aggregate_type: &str, aggregate_id: &str) -> i64 {
        sqlx::query_scalar::<_, i64>(
            r"
        SELECT COUNT(*)
        FROM snapshots
        WHERE aggregate_type = ?1 AND aggregate_id = ?2
        ",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn queue_row(pool: &SqlitePool, job_id: &str) -> (String, i64, String, Option<i64>) {
        sqlx::query_as::<_, (String, i64, String, Option<i64>)>(
            r"
        SELECT view_id, version, payload, lease_until
        FROM job_queue_projection
        WHERE view_id = ?1
        ",
        )
        .bind(job_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn aggregate_event_job_event_and_queue_seed_commit_together() {
        let pool = create_test_pool().await.unwrap();
        let job_id = JobId::new();

        persist_with_pending_job(pool.clone(), "atomic-success", job_id)
            .await
            .unwrap();

        let aggregate_events = count_events(&pool, TestAggregate::TYPE, "atomic-success").await;
        let job_id = job_id.to_string();
        let job_events = count_events(&pool, "job", &job_id).await;
        let queue_rows = count_queue_rows(&pool, &job_id).await;

        assert_eq!((aggregate_events, job_events, queue_rows), (1, 1, 1));
    }

    #[tokio::test]
    async fn empty_snapshot_update_propagates_without_writing() {
        let pool = create_test_pool().await.unwrap();
        let repo = NativeRepository::new(pool.clone(), CompactionPolicy::Retain);

        let result = repo
            .persist::<TestAggregate>(
                &[],
                Some((
                    "empty-snapshot".to_string(),
                    serde_json::json!({ "state": "invalid" }),
                    1,
                )),
            )
            .await;

        let Err(PersistenceError::UnknownError(source)) = result else {
            panic!("empty snapshot update must propagate the engine error");
        };
        assert!(matches!(
            source.downcast_ref::<EngineError>(),
            Some(EngineError::EmptySnapshotUpdate)
        ));
        assert_eq!(
            count_events(&pool, TestAggregate::TYPE, "empty-snapshot").await,
            0
        );
        assert_eq!(
            count_snapshots(&pool, TestAggregate::TYPE, "empty-snapshot").await,
            0
        );
    }

    #[tokio::test]
    async fn job_identity_conflict_rolls_back_the_aggregate_event() {
        let pool = create_test_pool().await.unwrap();
        let job_id = JobId::new();
        let seed = JobSeed::new(
            job_id,
            "existing-job",
            serde_json::json!({"payload": "existing"}),
            0,
        );
        (SqliteBackend { pool: pool.clone() })
            .enqueue(seed.event().unwrap(), seed.queue_payload().unwrap())
            .await
            .unwrap();
        let job_id_string = job_id.to_string();
        let queue_row_before_conflict = queue_row(&pool, &job_id_string).await;

        let result = persist_with_pending_job(pool.clone(), "atomic-rollback", job_id).await;

        assert!(matches!(result, Err(PersistenceError::OptimisticLockError)));
        let aggregate_events = count_events(&pool, TestAggregate::TYPE, "atomic-rollback").await;
        let existing_job_events = count_events(&pool, "job", &job_id_string).await;
        let queue_row_after_conflict = queue_row(&pool, &job_id_string).await;

        assert_eq!(aggregate_events, 0);
        assert_eq!(existing_job_events, 1);
        assert_eq!(queue_row_after_conflict, queue_row_before_conflict);
    }

    #[tokio::test]
    async fn failed_commit_preserves_the_pending_job_for_retry() {
        let pool = create_test_pool().await.unwrap();
        let repo = NativeRepository::new(pool.clone(), CompactionPolicy::Retain);
        repo.persist::<TestAggregate>(&covering_events("conflict", 1), None)
            .await
            .unwrap();
        let job_id = JobId::new();

        with_pending_jobs(async {
            buffer_pending_job(
                job_id,
                "test-pending",
                serde_json::json!({"test": "pending"}),
            )
            .unwrap();

            let failed = repo
                .persist::<TestAggregate>(&covering_events("conflict", 1), None)
                .await;
            assert!(matches!(failed, Err(PersistenceError::OptimisticLockError)));
            let persisted_id = job_id.to_string();
            assert_eq!(count_events(&pool, "job", &persisted_id).await, 0);
            assert_eq!(count_queue_rows(&pool, &persisted_id).await, 0);

            repo.persist::<TestAggregate>(&covering_events("retry", 1), None)
                .await
                .unwrap();
        })
        .await;

        let job_id = job_id.to_string();
        let retry_events = count_events(&pool, TestAggregate::TYPE, "retry").await;
        let job_events = count_events(&pool, "job", &job_id).await;
        let queue_rows = count_queue_rows(&pool, &job_id).await;
        assert_eq!((retry_events, job_events, queue_rows), (1, 1, 1));
    }

    #[tokio::test]
    async fn event_replay_returns_only_the_requested_stream() {
        let pool = create_test_pool().await.unwrap();
        let repo = NativeRepository::new(pool, CompactionPolicy::Retain);
        repo.persist::<TestAggregate>(&covering_events("requested", 1), None)
            .await
            .unwrap();
        repo.persist::<TestAggregate>(&covering_events("other", 1), None)
            .await
            .unwrap();

        let mut replay = repo
            .stream_events::<TestAggregate>("requested")
            .await
            .unwrap();
        let event = replay.next::<TestAggregate>(&[]).await.unwrap().unwrap();

        assert_eq!(event.aggregate_id, "requested");
        assert_eq!(event.sequence, 1);
        assert_eq!(event.payload, TestEvent::Created);
        assert!(replay.next::<TestAggregate>(&[]).await.is_none());
    }

    #[tokio::test]
    async fn event_replay_returns_all_streams_for_the_aggregate_type() {
        let pool = create_test_pool().await.unwrap();
        let repo = NativeRepository::new(pool, CompactionPolicy::Retain);
        repo.persist::<TestAggregate>(&covering_events("one", 1), None)
            .await
            .unwrap();
        repo.persist::<TestAggregate>(&covering_events("two", 1), None)
            .await
            .unwrap();

        let mut replay = repo.stream_all_events::<TestAggregate>().await.unwrap();
        let first = replay.next::<TestAggregate>(&[]).await.unwrap().unwrap();
        let second = replay.next::<TestAggregate>(&[]).await.unwrap().unwrap();
        let mut aggregate_ids = [first.aggregate_id.as_str(), second.aggregate_id.as_str()];
        aggregate_ids.sort_unstable();

        assert_eq!(aggregate_ids, ["one", "two"]);
        assert!(replay.next::<TestAggregate>(&[]).await.is_none());
    }

    #[tokio::test]
    async fn retained_window_snapshot_guard_preserves_incompatible_state() {
        let pool = create_test_pool().await.unwrap();
        let writer = CqrsFramework::new(
            PersistedEventStore::<_, PreviousSnapshotAggregate>::new_snapshot_store(
                NativeRepository::new(pool.clone(), CompactionPolicy::RetainLast { events: 2 }),
                1,
            ),
            vec![],
            (),
        );
        for _ in 0..3 {
            writer
                .execute("agg-windowed-incompatible", ())
                .await
                .unwrap();
        }

        let engine = Engine::new(pool.clone());
        let stream = StreamIdentity::new(TestAggregate::TYPE, "agg-windowed-incompatible");
        let version = engine.current_version(&stream).await.unwrap();
        assert_eq!(version, 3);
        assert_eq!(
            compact_events(
                &pool,
                PreviousSnapshotAggregate::AGGREGATE_TYPE,
                PreviousSnapshotAggregate::COMPACTION_POLICY
            )
            .await
            .unwrap(),
            1,
        );
        let remaining: Vec<i64> = sqlx::query_scalar(
            "SELECT sequence FROM events \
         WHERE aggregate_type = ?1 AND aggregate_id = ?2 ORDER BY sequence",
        )
        .bind(TestAggregate::TYPE)
        .bind("agg-windowed-incompatible")
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(remaining, vec![2, 3]);

        let repo = NativeRepository::new(pool.clone(), CompactionPolicy::RetainLast { events: 2 });
        let result = repo
            .get_snapshot::<TestAggregate>("agg-windowed-incompatible")
            .await;
        assert!(matches!(
            result,
            Err(PersistenceError::DeserializationError(_))
        ));

        let store = PersistedEventStore::<_, TestAggregate>::new_snapshot_store(repo, 100);
        let result = store.load_aggregate("agg-windowed-incompatible").await;
        assert!(matches!(
            result,
            Err(AggregateError::DeserializationError(_))
        ));

        let snapshot = engine.load_snapshot(&stream).await.unwrap().unwrap();
        assert_eq!(snapshot.current_sequence, 3);
        assert_eq!(snapshot.current_snapshot, 3);
        assert_eq!(
            snapshot.aggregate,
            serde_json::json!({"events": "CreatedCreatedCreated"})
        );
    }

    /// For a `Retain` entity, get_snapshot returns the incompatible snapshot
    /// unchanged (no shape check, no discard); cqrs-es then rebuilds from the
    /// always-complete event history. See the end-to-end rebuild test below.
    #[tokio::test]
    async fn incompatible_snapshot_returned_unchanged_for_retain_entity() {
        let pool = create_test_pool().await.unwrap();
        let repo = NativeRepository::new(pool.clone(), CompactionPolicy::Retain);

        repo.persist::<TestAggregate>(
            &covering_events("agg-replayable", 3),
            Some((
                "agg-replayable".to_string(),
                serde_json::json!({"events": "not-a-list"}),
                1,
            )),
        )
        .await
        .unwrap();

        let snapshot = repo
            .get_snapshot::<TestAggregate>("agg-replayable")
            .await
            .unwrap()
            .expect("Retain returns the snapshot unchanged");
        assert_eq!(snapshot.current_sequence, 3);
        assert_eq!(
            snapshot.aggregate,
            serde_json::json!({"events": "not-a-list"})
        );
    }

    /// For a `CompactAfterSnapshot` entity, the events behind the snapshot may
    /// have been compacted away, so an incompatible snapshot is preserved and an
    /// error surfaced rather than silently rebuilding from an incomplete history.
    /// The policy alone decides this -- no inspection of the event rows.
    #[tokio::test]
    async fn incompatible_snapshot_preserved_for_compactable_entity() {
        for policy in [
            CompactionPolicy::CompactAfterSnapshot,
            CompactionPolicy::RetainLast { events: 2 },
        ] {
            let pool = create_test_pool().await.unwrap();
            let repo = NativeRepository::new(pool.clone(), policy);

            repo.persist::<TestAggregate>(
                &covering_events("agg-compacted", 3),
                Some((
                    "agg-compacted".to_string(),
                    serde_json::json!({"events": "not-a-list"}),
                    1,
                )),
            )
            .await
            .unwrap();

            let result = repo.get_snapshot::<TestAggregate>("agg-compacted").await;
            assert!(matches!(
                result,
                Err(PersistenceError::DeserializationError(_))
            ));

            let remaining: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM snapshots WHERE aggregate_id = 'agg-compacted'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(remaining, 1);
        }
    }

    /// Stamps a marker into the payload so a test can see it ran, and bumps
    /// the version so the NEXT upcaster in the chain recognises its input.
    struct StampVersion {
        from: &'static str,
        to: &'static str,
    }

    impl EventUpcaster for StampVersion {
        fn can_upcast(&self, _event_type: &str, event_version: &str) -> bool {
            event_version == self.from
        }

        fn upcast(&self, mut event: SerializedEvent) -> SerializedEvent {
            if let Value::Object(payload) = &mut event.payload {
                payload.insert(
                    format!("upcast_{}_{}", self.from, self.to),
                    Value::Bool(true),
                );
            }
            event.event_version = self.to.to_string();
            event
        }
    }

    // A chain must carry an event across SEVERAL versions in one load, not
    // one hop per deployment: each upcaster examines the event as the
    // previous one left it, so v1 -> v2 -> v3 resolves in a single read.
    #[tokio::test]
    async fn upcasters_chain_across_versions_on_load() {
        let pool = create_test_pool().await.unwrap();
        let writer = NativeRepository::new(pool.clone(), CompactionPolicy::Retain);
        // An object payload, as a real enum-variant event serializes to --
        // the shared fixture persists a bare JSON string, which an upcaster
        // adding a FIELD has nowhere to put it.
        let persisted = SerializedEvent {
            aggregate_type: TestAggregate::TYPE.to_string(),
            aggregate_id: "agg-upcast".to_string(),
            sequence: 1,
            event_type: "Created".to_string(),
            event_version: "1.0".to_string(),
            payload: serde_json::json!({ "Created": {} }),
            metadata: serde_json::json!({}),
        };
        writer
            .persist::<TestAggregate>(&[persisted], None)
            .await
            .unwrap();

        let reader =
            NativeRepository::new(pool.clone(), CompactionPolicy::Retain).with_upcasters(vec![
                Arc::new(StampVersion {
                    from: "1.0",
                    to: "2.0",
                }),
                Arc::new(StampVersion {
                    from: "2.0",
                    to: "3.0",
                }),
            ]);

        let events = reader
            .get_events::<TestAggregate>("agg-upcast")
            .await
            .unwrap();

        let event = events.first().expect("the persisted event loads");
        assert_eq!(
            event.event_version, "3.0",
            "both hops ran, so the event arrives at the current version"
        );
        assert_eq!(
            event.payload.get("upcast_1.0_2.0"),
            Some(&Value::Bool(true))
        );
        assert_eq!(
            event.payload.get("upcast_2.0_3.0"),
            Some(&Value::Bool(true))
        );
    }

    // A store with no upcasters registered must read exactly what was
    // written -- the mechanism is opt-in and cannot alter existing history.
    #[tokio::test]
    async fn no_upcasters_leaves_the_event_untouched() {
        let pool = create_test_pool().await.unwrap();
        let repo = NativeRepository::new(pool.clone(), CompactionPolicy::Retain);
        repo.persist::<TestAggregate>(&covering_events("agg-plain", 1), None)
            .await
            .unwrap();

        let events = repo.get_events::<TestAggregate>("agg-plain").await.unwrap();

        let event = events.first().expect("the persisted event loads");
        assert_eq!(event.event_version, "1.0");
        assert_eq!(event.payload.get("upcast_1.0_2.0"), None);
    }

    #[tokio::test]
    async fn dropping_replay_releases_the_engine_connection() {
        let pool = create_test_pool().await.unwrap();
        let repository = NativeRepository::new(pool.clone(), CompactionPolicy::Retain);
        repository
            .persist::<TestAggregate>(&covering_events("cancelled", 3), None)
            .await
            .unwrap();
        let started = Arc::new(Notify::new());
        let producer_started = Arc::clone(&started);
        let replay = sqlite_es::replay_events::<StoredEventRow>(
            &pool,
            TestAggregate::TYPE,
            Some("cancelled".to_string()),
            1,
            move |event| {
                producer_started.notify_one();
                event
            },
        );
        timeout(Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        drop(replay);
        let connection = timeout(Duration::from_secs(1), pool.acquire())
            .await
            .unwrap()
            .unwrap();
        drop(connection);
        assert_eq!(
            repository
                .get_events::<TestAggregate>("cancelled")
                .await
                .unwrap()
                .len(),
            3
        );
    }

    struct RenameLegacyEvent {
        from: &'static str,
        to: &'static str,
        payload: &'static str,
    }

    impl EventUpcaster for RenameLegacyEvent {
        fn can_upcast(&self, event_type: &str, event_version: &str) -> bool {
            event_type == "Created" && event_version == self.from
        }

        fn upcast(&self, mut event: SerializedEvent) -> SerializedEvent {
            event.event_version = self.to.to_string();
            event.payload = serde_json::json!(self.payload);
            event
        }
    }

    #[tokio::test]
    async fn numeric_json_remains_readable_before_upcasting_and_after_checkpoints() {
        let pool = create_test_pool().await.unwrap();
        let repository = NativeRepository::new(pool.clone(), CompactionPolicy::Retain);
        let mut events = covering_events("numeric", 2);
        events[1].payload = serde_json::json!(42);
        events[1].metadata = serde_json::json!(3.5);
        repository
            .persist::<TestAggregate>(&events, None)
            .await
            .unwrap();

        assert_eq!(
            repository
                .get_events::<TestAggregate>("numeric")
                .await
                .unwrap(),
            events
        );
        assert_eq!(
            repository
                .get_last_events::<TestAggregate>("numeric", 1)
                .await
                .unwrap(),
            events[1..]
        );
        let engine = Engine::new(pool.clone());
        let stream = StreamIdentity::new(TestAggregate::TYPE, "numeric");
        let page_size = std::num::NonZeroUsize::new(2).unwrap();
        let byte_budget = std::num::NonZeroUsize::new(512).unwrap();
        for (checkpoint, expected) in [(None, events.as_slice()), (Some(1), &events[1..])] {
            assert_eq!(
                engine
                    .load_events_page(&stream, checkpoint, page_size)
                    .await
                    .unwrap(),
                expected
            );
            assert_eq!(
                engine
                    .load_events_page_bounded(&stream, checkpoint, page_size, byte_budget)
                    .await
                    .unwrap(),
                expected
            );
        }

        for aggregate_id in [Some("numeric".to_string()), None] {
            let mut replay = sqlite_es::replay_events::<StoredEventRow>(
                &pool,
                TestAggregate::TYPE,
                aggregate_id,
                1,
                |mut event| {
                    if event.sequence == 2 {
                        assert_eq!(event.payload, serde_json::json!(42));
                        assert_eq!(event.metadata, serde_json::json!(3.5));
                        event.payload = serde_json::json!("Created");
                        event.metadata = serde_json::json!({"upcast": "numeric"});
                    }
                    event
                },
            );
            assert_eq!(
                replay
                    .next::<TestAggregate>(&[])
                    .await
                    .unwrap()
                    .unwrap()
                    .sequence,
                1
            );
            let event = replay.next::<TestAggregate>(&[]).await.unwrap().unwrap();
            assert_eq!(event.sequence, 2);
            assert_eq!(event.payload, TestEvent::Created);
            assert_eq!(
                event.metadata.get("upcast").map(String::as_str),
                Some("numeric")
            );
            assert!(replay.next::<TestAggregate>(&[]).await.is_none());
        }
    }

    #[tokio::test]
    async fn upcasters_apply_to_suffix_and_both_replay_paths_without_rewriting_history() {
        let pool = create_test_pool().await.unwrap();
        let writer = NativeRepository::new(pool.clone(), CompactionPolicy::Retain);
        let mut legacy = covering_events("legacy", 2);
        for event in &mut legacy {
            event.payload = serde_json::json!("LegacyCreated");
        }
        writer
            .persist::<TestAggregate>(&legacy, None)
            .await
            .unwrap();
        let reader = NativeRepository::new(pool, CompactionPolicy::Retain).with_upcasters(vec![
            Arc::new(RenameLegacyEvent {
                from: "1.0",
                to: "2.0",
                payload: "IntermediateCreated",
            }),
            Arc::new(RenameLegacyEvent {
                from: "2.0",
                to: "3.0",
                payload: "Created",
            }),
        ]);

        let suffix = reader
            .get_last_events::<TestAggregate>("legacy", 1)
            .await
            .unwrap();
        assert_eq!(suffix.len(), 1);
        assert_eq!(suffix[0].sequence, 2);
        assert_eq!(suffix[0].event_version, "3.0");
        assert_eq!(suffix[0].payload, serde_json::json!("Created"));

        let mut replay = reader
            .stream_events::<TestAggregate>("legacy")
            .await
            .unwrap();
        let mut all = reader.stream_all_events::<TestAggregate>().await.unwrap();
        for sequence in 1..=2 {
            let event = replay.next::<TestAggregate>(&[]).await.unwrap().unwrap();
            assert_eq!(event.sequence, sequence);
            assert_eq!(event.payload, TestEvent::Created);
            let event = all.next::<TestAggregate>(&[]).await.unwrap().unwrap();
            assert_eq!(event.sequence, sequence);
            assert_eq!(event.payload, TestEvent::Created);
        }
        assert!(replay.next::<TestAggregate>(&[]).await.is_none());
        assert!(all.next::<TestAggregate>(&[]).await.is_none());
        assert_eq!(
            writer.get_events::<TestAggregate>("legacy").await.unwrap(),
            legacy
        );
    }

    /// End-to-end: loading a `Retain` aggregate whose snapshot is incompatible
    /// discards it and reconstructs correct state from the full event history.
    #[tokio::test]
    async fn load_aggregate_rebuilds_after_discard_for_retain_entity() {
        let pool = create_test_pool().await.unwrap();
        let repo = NativeRepository::new(pool.clone(), CompactionPolicy::Retain);

        repo.persist::<TestAggregate>(
            &covering_events("agg-load", 3),
            Some((
                "agg-load".to_string(),
                serde_json::json!({"events": "not-a-list"}),
                1,
            )),
        )
        .await
        .unwrap();

        // load_aggregate must discard the incompatible snapshot and replay the
        // events, reconstructing the correct state rather than Default.
        let store = PersistedEventStore::<_, TestAggregate>::new_snapshot_store(
            NativeRepository::new(pool.clone(), CompactionPolicy::Retain),
            100,
        );
        let mut context = store.load_aggregate("agg-load").await.unwrap();
        assert_eq!(
            *context.aggregate(),
            TestAggregate {
                events: vec![
                    "Created".to_string(),
                    "Created".to_string(),
                    "Created".to_string(),
                ],
            }
        );
    }

    /// End-to-end: loading a `CompactAfterSnapshot` aggregate whose snapshot is
    /// incompatible surfaces a `DeserializationError` through cqrs-es rather than
    /// silently rebuilding from a possibly-incomplete history. This pins the
    /// guard's reliance on cqrs-es propagating the `Err` returned by
    /// `get_snapshot` (instead of falling through to its rebuild-from-events
    /// recovery, which fires only on `Ok(Some(..))`/`Ok(None)`).
    #[tokio::test]
    async fn load_aggregate_errors_for_compactable_entity_with_incompatible_snapshot() {
        for policy in [
            CompactionPolicy::CompactAfterSnapshot,
            CompactionPolicy::RetainLast { events: 2 },
        ] {
            let pool = create_test_pool().await.unwrap();
            let repo = NativeRepository::new(pool.clone(), policy);

            repo.persist::<TestAggregate>(
                &covering_events("agg-compact-load", 3),
                Some((
                    "agg-compact-load".to_string(),
                    serde_json::json!({"events": "not-a-list"}),
                    1,
                )),
            )
            .await
            .unwrap();

            let store = PersistedEventStore::<_, TestAggregate>::new_snapshot_store(
                NativeRepository::new(pool.clone(), policy),
                100,
            );
            let result = store.load_aggregate("agg-compact-load").await;
            assert!(matches!(
                result,
                Err(AggregateError::DeserializationError(_))
            ));
        }
    }
}
