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
        let jobs = crate::job::take_pending().map_err(EngineError::from)?;
        let mut request = CommitRequest::new(stream, events).with_jobs(jobs);
        if let Some((_, aggregate, snapshot_version)) = snapshot_update {
            request = request.with_snapshot(SnapshotUpdate {
                aggregate,
                snapshot_version,
            });
        }

        Ok(self.engine.commit(request).await?)
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

    use super::*;

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

    async fn test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlite_es::MIGRATOR.run(&pool).await.unwrap();
        pool
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

    /// For a `Retain` entity, get_snapshot returns the incompatible snapshot
    /// unchanged (no shape check, no discard); cqrs-es then rebuilds from the
    /// always-complete event history. See the end-to-end rebuild test below.
    #[tokio::test]
    async fn incompatible_snapshot_returned_unchanged_for_retain_entity() {
        let pool = test_pool().await;
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
        let pool = test_pool().await;
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
        let pool = test_pool().await;
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
        let pool = test_pool().await;
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
