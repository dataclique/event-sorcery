//! SQLite repository adapter with ordered read-time event upcasting.

use std::sync::Arc;

use cqrs_es::Aggregate;
use cqrs_es::persist::{
    EventUpcaster, PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent,
    SerializedSnapshot,
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
    /// Applied in registration order to every event on the way out of the
    /// store, so a persisted event whose shape predates the current one is
    /// read as the current shape.
    upcasters: Arc<Vec<Arc<dyn EventUpcaster>>>,
}

impl SqliteEventRepository {
    pub(crate) fn new(pool: SqlitePool, compaction_policy: CompactionPolicy) -> Self {
        Self {
            engine: Engine::new(pool),
            compaction_policy,
            stream_channel_size: 1000,
            upcasters: Arc::new(Vec::new()),
        }
    }

    /// Registers the upcasters every load and replay applies, in order.
    ///
    /// Order is the contract, not an implementation detail: a chain that
    /// carries an event from v1 to v3 must register the v1->v2 step before
    /// the v2->v3 one, because each step sees the event as the previous step
    /// left it.
    #[must_use]
    pub fn with_upcasters(mut self, upcasters: Vec<Arc<dyn EventUpcaster>>) -> Self {
        self.upcasters = Arc::new(upcasters);
        self
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
        let upcasters = Arc::clone(&self.upcasters);
        self.engine.stream_events(
            A::TYPE,
            aggregate_id.map(String::from),
            self.stream_channel_size,
            move |event| upcast_event(event, &upcasters),
        )
    }
}

impl PersistedEventRepository for SqliteEventRepository {
    async fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let stream = StreamIdentity::new(A::TYPE, aggregate_id);
        Ok(self
            .engine
            .load_events(&stream, None)
            .await?
            .into_iter()
            .map(|event| upcast_event(event, &self.upcasters))
            .collect())
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
            .await?
            .into_iter()
            .map(|event| upcast_event(event, &self.upcasters))
            .collect())
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
            // A compacting aggregate (full or windowed) may have lost the
            // events behind the snapshot, making the snapshot the only
            // durable record of state. An incompatible payload must surface
            // an error rather than letting cqrs-es silently rebuild from an
            // incomplete history.
            CompactionPolicy::CompactAfterSnapshot | CompactionPolicy::RetainLast { .. } => {
                match A::deserialize(&snapshot.aggregate) {
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
                }
            }
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
            request = request.with_snapshot(SnapshotUpdate::new(aggregate, snapshot_version))?;
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

/// Walks one event through the registered upcasters, in order.
///
/// Each upcaster re-examines the event AFTER earlier ones have run, so a
/// chain can carry an event forward across several versions in one load
/// (v1 -> v2 -> v3) rather than only one hop per deployment.
///
/// An upcaster changes SHAPE, never facts: it exists because a persisted
/// event predates a field, not because the recorded history was wrong. Data
/// that was never observed cannot be invented here -- a gap in the record is
/// a different problem, and one an upcaster has nothing to work from.
fn upcast_event(
    mut event: SerializedEvent,
    upcasters: &[Arc<dyn EventUpcaster>],
) -> SerializedEvent {
    for upcaster in upcasters {
        if upcaster.can_upcast(&event.event_type, &event.event_version) {
            event = upcaster.upcast(event);
        }
    }
    event
}

#[cfg(test)]
mod tests {
    //! Repository integration coverage for atomic engine delegation.

    use cqrs_es::event_sink::EventSink;
    use cqrs_es::persist::PersistedEventStore;
    use cqrs_es::{AggregateContext, AggregateError, DomainEvent, EventStore};
    use serde::{Deserialize, Serialize};
    use std::fmt::{self, Display};
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::time::timeout;

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
    async fn empty_snapshot_update_propagates_without_writing() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool.clone(), CompactionPolicy::Retain);

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
        let mut aggregate_ids = [first.aggregate_id.as_str(), second.aggregate_id.as_str()];
        aggregate_ids.sort_unstable();

        assert_eq!(aggregate_ids, ["one", "two"]);
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
        for policy in [
            CompactionPolicy::CompactAfterSnapshot,
            CompactionPolicy::RetainLast { events: 2 },
        ] {
            let pool = create_test_pool().await.unwrap();
            let repo = SqliteEventRepository::new(pool.clone(), policy);

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
        let writer = SqliteEventRepository::new(pool.clone(), CompactionPolicy::Retain);
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

        let reader = SqliteEventRepository::new(pool.clone(), CompactionPolicy::Retain)
            .with_upcasters(vec![
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
        let repo = SqliteEventRepository::new(pool.clone(), CompactionPolicy::Retain);
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
        let repository = SqliteEventRepository::new(pool.clone(), CompactionPolicy::Retain);
        repository
            .persist::<TestAggregate>(&covering_events("cancelled", 3), None)
            .await
            .unwrap();
        let started = Arc::new(Notify::new());
        let producer_started = Arc::clone(&started);
        let replay = repository.engine.stream_events(
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
    async fn upcasters_apply_to_suffix_and_both_replay_paths_without_rewriting_history() {
        let pool = create_test_pool().await.unwrap();
        let writer = SqliteEventRepository::new(pool.clone(), CompactionPolicy::Retain);
        let mut legacy = covering_events("legacy", 2);
        for event in &mut legacy {
            event.payload = serde_json::json!("LegacyCreated");
        }
        writer
            .persist::<TestAggregate>(&legacy, None)
            .await
            .unwrap();
        let reader =
            SqliteEventRepository::new(pool, CompactionPolicy::Retain).with_upcasters(vec![
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
        for policy in [
            CompactionPolicy::CompactAfterSnapshot,
            CompactionPolicy::RetainLast { events: 2 },
        ] {
            let pool = create_test_pool().await.unwrap();
            let repo = SqliteEventRepository::new(pool.clone(), policy);

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
                SqliteEventRepository::new(pool.clone(), policy),
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
