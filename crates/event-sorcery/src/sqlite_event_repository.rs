use cqrs_es::Aggregate;
use cqrs_es::persist::{
    PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot,
};
use serde_json::Value;
use sqlx::SqlitePool;
use tracing::warn;

use crate::CompactionPolicy;
use crate::engine::{CommitRequest, Engine, EngineError, SnapshotUpdate, StreamIdentity};

/// SQLite implementation of the cqrs-es [`PersistedEventRepository`].
///
/// Public so it can be the [`crate::EventBackend::EventRepo`] of
/// [`crate::SqliteBackend`]; consumers obtain it via the backend, not directly.
pub struct SqliteEventRepository {
    engine: Engine,
    compaction_policy: CompactionPolicy,
    stream_channel_size: usize,
}

impl SqliteEventRepository {
    pub(crate) fn new(pool: SqlitePool, compaction_policy: CompactionPolicy) -> Self {
        Self {
            engine: Engine::new(pool),
            compaction_policy,
            stream_channel_size: 1000,
        }
    }

    /// Stream events from the `events` table for replay.
    ///
    /// **Compaction caveat:** This only queries the `events` table.
    /// Fully-compacted aggregates (all events deleted, only a
    /// snapshot remains) will not appear in the stream.
    /// [`load_aggregate`](cqrs_es::persist::PersistedEventStore::load_aggregate)
    /// handles this correctly via snapshot loading, but
    /// stream-based replay will miss snapshot-only entities.
    fn stream_events_impl<A: Aggregate>(&self, aggregate_id: Option<&str>) -> ReplayStream {
        self.engine.stream_events(
            A::TYPE,
            aggregate_id.map(String::from),
            self.stream_channel_size,
        )
    }
}

impl PersistedEventRepository for SqliteEventRepository {
    async fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let stream = StreamIdentity::new(A::TYPE, aggregate_id);
        Ok(self.engine.load_events(&stream, None).await?)
    }

    async fn get_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let stream = StreamIdentity::new(A::TYPE, aggregate_id);
        Ok(self
            .engine
            .load_events(&stream, Some(last_sequence))
            .await?)
    }

    async fn get_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        let stream = StreamIdentity::new(A::TYPE, aggregate_id);
        let snapshot = self.engine.load_snapshot(&stream).await?;
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };

        // Whether an incompatible snapshot must be guarded depends on the
        // entity's compaction policy (ADR-0003).
        match self.compaction_policy {
            // A `Retain` aggregate keeps its full event history, so cqrs-es
            // safely rebuilds from events on a deserialize failure. The snapshot
            // is returned as stored -- no shape check is needed here, and doing
            // one would only duplicate the deserialize cqrs-es already performs.
            CompactionPolicy::Retain => Ok(Some(snapshot)),
            // A `CompactAfterSnapshot` aggregate may have lost the events behind
            // the snapshot, making the snapshot the only durable record of
            // state. An incompatible payload must surface an error rather than
            // letting cqrs-es silently rebuild from an incomplete history.
            CompactionPolicy::CompactAfterSnapshot => match A::deserialize(&snapshot.aggregate) {
                Ok(_) => Ok(Some(snapshot)),
                Err(source) => {
                    warn!(
                        target: "cqrs",
                        aggregate_type = A::TYPE,
                        aggregate_id,
                        %source,
                        "Incompatible snapshot for a compactable aggregate cannot be \
                         safely rebuilt from events; surfacing error"
                    );
                    Err(EngineError::Json(source).into())
                }
            },
        }
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        let aggregate_id = snapshot_update
            .as_ref()
            .map_or_else(
                || events.first().map(|event| event.aggregate_id.as_str()),
                |update| Some(update.0.as_str()),
            )
            .unwrap_or_default();
        let stream = StreamIdentity::new(A::TYPE, aggregate_id);
        let mut pending_jobs = crate::job::prepare_pending().map_err(EngineError::from)?;
        let mut request =
            CommitRequest::new(stream, events).with_jobs(pending_jobs.take_requests());
        if let Some((_, aggregate, snapshot_version)) = snapshot_update {
            request = request.with_snapshot(SnapshotUpdate::new(aggregate, snapshot_version));
        }

        self.engine.commit(request).await?;
        pending_jobs.mark_committed();
        Ok(())
    }

    async fn stream_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<ReplayStream, PersistenceError> {
        Ok(self.stream_events_impl::<A>(Some(aggregate_id)))
    }

    async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
        Ok(self.stream_events_impl::<A>(None))
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::event_sink::EventSink;
    use cqrs_es::persist::PersistedEventStore;
    use cqrs_es::{AggregateContext, AggregateError, DomainEvent, EventStore};
    use serde::{Deserialize, Serialize};
    use std::fmt::{self, Display};

    use sqlite_es::testing::create_test_pool;

    use super::*;
    use crate::SqliteBackend;
    use crate::job::{
        EnqueueRequest, ErasedJob, JobId, JobKind, PendingPush, enqueued_event,
        pending_seed_payload,
    };
    use crate::job_store::EventBackend;

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

    struct TestPendingJob;

    impl ErasedJob for TestPendingJob {
        fn kind(&self) -> JobKind {
            JobKind::new("test-pending")
        }

        fn encode(&self) -> Result<Value, serde_json::Error> {
            Ok(serde_json::json!({ "payload": "test" }))
        }
    }

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
        let repo = SqliteEventRepository::new(pool, CompactionPolicy::Retain);
        crate::job::with_pending_scope(async move {
            crate::job::buffer(PendingPush {
                job_id,
                job: Box::new(TestPendingJob),
                delay: None,
            })
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
            FROM job_queue
            WHERE view_id = ?1
            ",
        )
        .bind(job_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn queue_row(pool: &SqlitePool, job_id: &str) -> (String, i64, String, Option<i64>) {
        sqlx::query_as::<_, (String, i64, String, Option<i64>)>(
            r"
            SELECT view_id, version, payload, lease_until
            FROM job_queue
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
    async fn job_identity_conflict_rolls_back_the_aggregate_event() {
        let pool = create_test_pool().await.unwrap();
        let job_id = JobId::new();
        let request = EnqueueRequest {
            job_id,
            kind: JobKind::new("existing-job"),
            payload: serde_json::json!({ "payload": "existing" }),
            run_at_ms: 0,
        };
        SqliteBackend::new(pool.clone())
            .enqueue(
                enqueued_event(&request).unwrap(),
                pending_seed_payload(&request).unwrap(),
            )
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
        let repo = SqliteEventRepository::new(pool.clone(), CompactionPolicy::Retain);
        repo.persist::<TestAggregate>(&covering_events("conflict", 1), None)
            .await
            .unwrap();
        let job_id = JobId::new();

        crate::job::with_pending_scope(async {
            crate::job::buffer(PendingPush {
                job_id,
                job: Box::new(TestPendingJob),
                delay: None,
            })
            .unwrap();

            let failed = repo
                .persist::<TestAggregate>(&covering_events("conflict", 1), None)
                .await;
            assert!(matches!(failed, Err(PersistenceError::OptimisticLockError)));

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
        let repo = SqliteEventRepository::new(pool, CompactionPolicy::Retain);
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
        let repo = SqliteEventRepository::new(pool, CompactionPolicy::Retain);
        repo.persist::<TestAggregate>(&covering_events("one", 1), None)
            .await
            .unwrap();
        repo.persist::<TestAggregate>(&covering_events("two", 1), None)
            .await
            .unwrap();

        let mut replay = repo.stream_all_events::<TestAggregate>().await.unwrap();
        let first = replay.next::<TestAggregate>(&[]).await.unwrap().unwrap();
        let second = replay.next::<TestAggregate>(&[]).await.unwrap().unwrap();
        let aggregate_ids = [first.aggregate_id.as_str(), second.aggregate_id.as_str()];

        assert!(aggregate_ids.contains(&"one"));
        assert!(aggregate_ids.contains(&"two"));
        assert!(replay.next::<TestAggregate>(&[]).await.is_none());
    }

    /// For a `Retain` entity, get_snapshot returns the incompatible snapshot
    /// unchanged (no shape check, no discard); cqrs-es then rebuilds from the
    /// always-complete event history. See the end-to-end rebuild test below.
    #[tokio::test]
    async fn incompatible_snapshot_returned_unchanged_for_retain_entity() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool.clone(), CompactionPolicy::Retain);

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
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool.clone(), CompactionPolicy::CompactAfterSnapshot);

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

    /// End-to-end: loading a `Retain` aggregate whose snapshot is incompatible
    /// discards it and reconstructs correct state from the full event history.
    #[tokio::test]
    async fn load_aggregate_rebuilds_after_discard_for_retain_entity() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool.clone(), CompactionPolicy::Retain);

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
            SqliteEventRepository::new(pool.clone(), CompactionPolicy::Retain),
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
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool.clone(), CompactionPolicy::CompactAfterSnapshot);

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
            SqliteEventRepository::new(pool.clone(), CompactionPolicy::CompactAfterSnapshot),
            100,
        );
        let result = store.load_aggregate("agg-compact-load").await;
        assert!(matches!(
            result,
            Err(AggregateError::DeserializationError(_))
        ));
    }
}
