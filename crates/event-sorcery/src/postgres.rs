//! PostgreSQL persistence through the shared CQRS repository contracts.

use cqrs_es::Aggregate;
use cqrs_es::persist::{
    PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot,
};
use futures_util::TryStreamExt;
use serde_json::Value;
use sqlx::PgConnection;

use crate::engine::{
    CommitRequest, EngineError, JobSeed, SnapshotUpdate, StoredEventRow, StoredSnapshotRow,
    validate_expected_version,
};

/// Pool-backed PostgreSQL repository for the CQRS persistence contract.
///
/// Use it through [`cqrs_es::persist::PersistedEventStore`]. It does not provide
/// a PostgreSQL [`crate::EventBackend`] for [`crate::StoreBuilder`]. The built-in
/// native store and worker integration remain SQLite-only.
pub struct PostgresEventRepository {
    /// Application-provisioned PostgreSQL connection pool.
    pub pool: sqlx::PgPool,
}

/// Framework-owned PostgreSQL schema migrations.
///
/// Applications apply these migrations to their PostgreSQL pool before using
/// framework persistence. Existing SQLite migrations remain in `sqlite-es`.
pub static POSTGRES_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/postgres");

impl PersistedEventRepository for PostgresEventRepository {
    async fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        Ok(self.load_events(A::TYPE, aggregate_id, 0).await?)
    }

    async fn get_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        Ok(self
            .load_events(A::TYPE, aggregate_id, last_sequence)
            .await?)
    }

    async fn get_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        let row = sqlx::query_as::<_, StoredSnapshotRow>(
            r"
            SELECT aggregate_id, last_sequence, snapshot_version, payload::TEXT AS payload
            FROM snapshots
            WHERE aggregate_type = $1 AND aggregate_id = $2
            ",
        )
        .bind(A::TYPE)
        .bind(aggregate_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(EngineError::from)?;
        Ok(row.map(SerializedSnapshot::try_from).transpose()?)
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        let snapshot_update = snapshot_update.map(|(aggregate_id, aggregate, snapshot_version)| {
            (
                aggregate_id,
                SnapshotUpdate::new(aggregate, snapshot_version),
            )
        });
        self.persist_updates::<A>(events, snapshot_update).await
    }

    async fn persist_with_snapshot<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<SerializedSnapshot>,
    ) -> Result<(), PersistenceError> {
        self.persist_updates::<A>(events, snapshot_update.map(SnapshotUpdate::from_serialized))
            .await
    }

    async fn stream_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<ReplayStream, PersistenceError> {
        Ok(self.replay(A::TYPE, Some(aggregate_id.to_string())))
    }

    async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
        Ok(self.replay(A::TYPE, None))
    }
}

impl PostgresEventRepository {
    /// Atomically persists a standalone job's event and queue seed.
    ///
    /// # Errors
    ///
    /// Returns typed persistence errors for invalid jobs, duplicate identities,
    /// and storage failures without committing a partial enqueue.
    pub async fn enqueue(&self, job: JobSeed) -> Result<(), PersistenceError> {
        Box::pin(self.commit(CommitRequest::for_job(job))).await?;
        Ok(())
    }

    async fn persist_updates<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, SnapshotUpdate)>,
    ) -> Result<(), PersistenceError> {
        let (request, pending_jobs) =
            CommitRequest::prepare_repository(A::TYPE, events, snapshot_update)?;
        Box::pin(self.commit(request)).await?;
        pending_jobs.mark_committed();
        Ok(())
    }

    async fn load_events(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, EngineError> {
        let last_sequence = i64::try_from(last_sequence)?;
        let rows = sqlx::query_as::<_, StoredEventRow>(
            r"
            SELECT aggregate_type, aggregate_id, sequence, event_type, event_version,
                   payload::TEXT AS payload, metadata::TEXT AS metadata
            FROM events
            WHERE aggregate_type = $1 AND aggregate_id = $2 AND sequence > $3
            ORDER BY sequence
            ",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(last_sequence)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(SerializedEvent::try_from).collect()
    }

    async fn commit(&self, request: CommitRequest<'_>) -> Result<(), EngineError> {
        request.validate_metadata()?;
        request.validate_stream()?;
        let CommitRequest {
            stream,
            expected_version,
            events,
            snapshot,
            jobs,
        } = request;
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&mut *transaction)
            .await?;

        if let Some(expected_version) = expected_version {
            let actual: i64 = sqlx::query_scalar(
                r"
                SELECT GREATEST(
                    COALESCE((SELECT MAX(sequence) FROM events
                              WHERE aggregate_type = $1 AND aggregate_id = $2), 0),
                    COALESCE((SELECT last_sequence FROM snapshots
                              WHERE aggregate_type = $1 AND aggregate_id = $2), 0)
                )
                ",
            )
            .bind(&stream.aggregate_type)
            .bind(&stream.aggregate_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(write_error)?;
            validate_expected_version(expected_version, usize::try_from(actual)?)?;
        }

        append_events(&mut transaction, events).await?;
        if let Some(snapshot) = snapshot {
            let last_sequence = snapshot.sequence_for(events)?;
            let result = sqlx::query(
                r"
                INSERT INTO snapshots
                    (aggregate_type, aggregate_id, last_sequence, snapshot_version, payload)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (aggregate_type, aggregate_id) DO UPDATE SET
                    last_sequence = EXCLUDED.last_sequence,
                    snapshot_version = EXCLUDED.snapshot_version,
                    payload = EXCLUDED.payload,
                    timestamp = CURRENT_TIMESTAMP
                WHERE snapshots.last_sequence < EXCLUDED.last_sequence
                ",
            )
            .bind(&stream.aggregate_type)
            .bind(&stream.aggregate_id)
            .bind(i64::try_from(last_sequence)?)
            .bind(i64::try_from(snapshot.snapshot_version)?)
            .bind(snapshot.aggregate)
            .execute(&mut *transaction)
            .await
            .map_err(write_error)?;
            if result.rows_affected() != 1 {
                return Err(EngineError::OptimisticLock);
            }
        }

        for request in jobs {
            let event = crate::job::enqueued_event(&request)?;
            append_events(&mut transaction, std::slice::from_ref(&event)).await?;
            let payload = crate::job::pending_seed_payload(&request)?;
            sqlx::query(
                r"
                INSERT INTO job_queue_projection (view_id, version, payload, lease_until)
                VALUES ($1, 1, $2::JSONB, NULL)
                ",
            )
            .bind(request.job_id.to_string())
            .bind(payload)
            .execute(&mut *transaction)
            .await
            .map_err(write_error)?;
        }

        transaction.commit().await.map_err(write_error)?;
        Ok(())
    }

    fn replay(&self, aggregate_type: &'static str, aggregate_id: Option<String>) -> ReplayStream {
        let (mut feed, stream) = ReplayStream::new(1000);
        let pool = self.pool.clone();
        tokio::spawn(async move {
            let mut rows = sqlx::query_as::<_, StoredEventRow>(
                r"
                SELECT aggregate_type, aggregate_id, sequence, event_type, event_version,
                       payload::TEXT AS payload, metadata::TEXT AS metadata
                FROM events
                WHERE aggregate_type = $1 AND ($2::TEXT IS NULL OR aggregate_id = $2)
                ORDER BY aggregate_id, sequence
                ",
            )
            .bind(aggregate_type)
            .bind(aggregate_id)
            .fetch(&pool);

            loop {
                let event = match rows.try_next().await {
                    Ok(Some(row)) => SerializedEvent::try_from(row).map_err(PersistenceError::from),
                    Ok(None) => return,
                    Err(error) => Err(EngineError::Sql(error).into()),
                };
                match event {
                    Ok(event) => {
                        if feed.push(Ok(event)).await.is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        let _ = feed.push(Err(error)).await;
                        return;
                    }
                }
            }
        });
        stream
    }
}

async fn append_events(
    connection: &mut PgConnection,
    events: &[SerializedEvent],
) -> Result<(), EngineError> {
    for event in events {
        sqlx::query(
            r"
            INSERT INTO events
                (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ",
        ).bind(&event.aggregate_type).bind(&event.aggregate_id)
            .bind(i64::try_from(event.sequence)?)
            .bind(&event.event_type).bind(&event.event_version)
            .bind(&event.payload).bind(&event.metadata)
            .execute(&mut *connection).await.map_err(write_error)?;
    }
    Ok(())
}

fn write_error(error: sqlx::Error) -> EngineError {
    use EngineError::{OptimisticLock, Sql};

    match error.as_database_error() {
        Some(source)
            if source.code().as_deref() == Some("40001")
                || source.code().as_deref() == Some("40P01") =>
        {
            OptimisticLock
        }
        Some(source) if source.is_unique_violation() => {
            match (source.table(), source.constraint()) {
                (Some("events"), Some("events_pkey"))
                | (Some("snapshots"), Some("snapshots_pkey"))
                | (Some("job_queue_projection"), Some("job_queue_projection_pkey")) => {
                    OptimisticLock
                }
                _ => Sql(error),
            }
        }
        Some(_) | None => Sql(error),
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use cqrs_es::event_sink::EventSink;
    use cqrs_es::persist::{
        PersistedEventRepository, PersistedEventStore, PersistenceError, SerializedEvent,
    };
    use cqrs_es::{Aggregate, AggregateError, CqrsFramework, DomainEvent, EventStore};
    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};
    use sqlx::PgPool;
    use sqlx::postgres::PgPoolOptions;
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::time::Duration;
    use tokio::time::timeout;

    use super::{POSTGRES_MIGRATOR, PostgresEventRepository};
    use crate::job::{
        ClaimId, DeadReason, ErasedJob, JobEvent, JobId, JobKind, JobState, PendingPush, WorkerId,
    };
    use crate::{EventSourced, JobSeed, Lifecycle};

    #[derive(Default, Debug, Clone, Serialize, Deserialize)]
    struct RecordedValues {
        values: Vec<String>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum ValueEvent {
        Recorded { value: String },
    }

    impl DomainEvent for ValueEvent {
        fn event_type(&self) -> String {
            "Recorded".to_string()
        }

        fn event_version(&self) -> String {
            "1".to_string()
        }
    }

    impl Aggregate for RecordedValues {
        const TYPE: &'static str = "RecordedValues";
        type Command = String;
        type Event = ValueEvent;
        type Error = Infallible;
        type Services = Option<JobId>;

        async fn handle(
            &mut self,
            value: String,
            services: &Option<JobId>,
            sink: &EventSink<Self>,
        ) -> Result<(), Infallible> {
            let mut scratch = self.clone();
            sink.write(ValueEvent::Recorded { value }, &mut scratch)
                .await;
            if let Some(job_id) = services {
                crate::job::buffer(PendingPush {
                    job_id: *job_id,
                    job: Box::new(RecordedJob),
                    delay: None,
                })
                .unwrap();
            }
            Ok(())
        }

        fn apply(&mut self, event: ValueEvent) {
            let ValueEvent::Recorded { value } = event;
            self.values.push(value);
        }
    }

    struct RecordedJob;

    impl ErasedJob for RecordedJob {
        fn kind(&self) -> JobKind {
            JobKind::new("recorded-value")
        }

        fn encode(&self) -> Result<Value, serde_json::Error> {
            Ok(json!({"value": "work"}))
        }
    }

    #[sqlx::test(migrations = false)]
    async fn repository_rejects_gapped_appends_without_writing(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        let repository = PostgresEventRepository { pool };
        let mut event = SerializedEvent {
            aggregate_type: RecordedValues::TYPE.to_string(),
            aggregate_id: "version-guard".to_string(),
            sequence: 1,
            event_type: "Recorded".to_string(),
            event_version: "1".to_string(),
            payload: json!({"Recorded": {"value": "first"}}),
            metadata: json!({}),
        };
        for sequence in [2, 0] {
            event.sequence = sequence;
            let result = repository
                .persist::<RecordedValues>(&[event.clone()], None)
                .await;
            assert!(
                matches!(result, Err(PersistenceError::OptimisticLockError)),
                "{result:?}"
            );
            assert!(
                repository
                    .get_events::<RecordedValues>("version-guard")
                    .await
                    .unwrap()
                    .is_empty()
            );
        }

        event.sequence = 1;
        repository
            .persist::<RecordedValues>(&[event.clone()], None)
            .await
            .unwrap();
        let before = repository
            .get_events::<RecordedValues>("version-guard")
            .await
            .unwrap();
        for sequence in [1, 3] {
            event.sequence = sequence;
            let result = repository
                .persist::<RecordedValues>(&[event.clone()], None)
                .await;
            assert!(
                matches!(result, Err(PersistenceError::OptimisticLockError)),
                "{result:?}"
            );
            assert_eq!(
                repository
                    .get_events::<RecordedValues>("version-guard")
                    .await
                    .unwrap(),
                before
            );
        }
        event.sequence = 2;
        let mut skipped = event.clone();
        skipped.sequence = 4;
        let result = repository
            .persist::<RecordedValues>(&[event.clone(), skipped], None)
            .await;
        assert!(
            matches!(result, Err(PersistenceError::OptimisticLockError)),
            "{result:?}"
        );
        assert_eq!(
            repository
                .get_events::<RecordedValues>("version-guard")
                .await
                .unwrap(),
            before
        );
        repository
            .persist::<RecordedValues>(&[event], None)
            .await
            .unwrap();
        assert_eq!(
            repository
                .get_events::<RecordedValues>("version-guard")
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[sqlx::test(migrations = false)]
    async fn repository_reloads_commands_through_the_generic_cqrs_store(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let store = PersistedEventStore::<_, RecordedValues>::new_event_store(repository);
        let framework = CqrsFramework::new(store, vec![], None);
        framework
            .execute("stream", "first".to_string())
            .await
            .unwrap();
        framework
            .execute("stream", "second".to_string())
            .await
            .unwrap();
        framework
            .execute("other", "isolated".to_string())
            .await
            .unwrap();

        let repository = PostgresEventRepository { pool };
        let store = PersistedEventStore::<_, RecordedValues>::new_event_store(repository);
        let restored = store.load_aggregate("stream").await.unwrap();
        assert_eq!(restored.aggregate.values, ["first", "second"]);
        assert_eq!(restored.current_sequence, 2);
        assert_eq!(restored.current_snapshot, None);

        let other = store.load_aggregate("other").await.unwrap();
        assert_eq!(other.aggregate.values, ["isolated"]);
        assert_eq!(other.current_sequence, 1);
    }

    #[sqlx::test(migrations = false)]
    async fn repository_restores_snapshot_and_suffix_with_distinct_counters(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(repository, 2);
        let framework = CqrsFramework::new(store, vec![], None);
        for value in ["first", "second", "third", "fourth", "fifth"] {
            framework
                .execute("stream", value.to_string())
                .await
                .unwrap();
        }

        let repository = PostgresEventRepository { pool };
        let snapshot = repository
            .get_snapshot::<RecordedValues>("stream")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.current_sequence, 4);
        assert_eq!(snapshot.current_snapshot, 2);
        assert_eq!(
            snapshot.aggregate,
            json!({"values": ["first", "second", "third", "fourth"]})
        );
        let suffix = repository
            .get_last_events::<RecordedValues>("stream", 4)
            .await
            .unwrap();
        assert_eq!(suffix.len(), 1);
        assert_eq!(suffix[0].sequence, 5);
        assert_eq!(suffix[0].payload, json!({"Recorded": {"value": "fifth"}}));

        let store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(repository, 2);
        let restored = store.load_aggregate("stream").await.unwrap();
        assert_eq!(
            restored.aggregate.values,
            ["first", "second", "third", "fourth", "fifth"]
        );
        assert_eq!(restored.current_sequence, 5);
        assert_eq!(restored.current_snapshot, Some(2));
    }

    #[sqlx::test(migrations = false)]
    async fn replay_preserves_stream_order_and_aggregate_isolation(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let store = PersistedEventStore::<_, RecordedValues>::new_event_store(repository);
        let framework = CqrsFramework::new(store, vec![], None);
        for (stream, value) in [
            ("stream", "first"),
            ("other", "isolated"),
            ("stream", "second"),
        ] {
            framework.execute(stream, value.to_string()).await.unwrap();
        }
        let repository = PostgresEventRepository { pool };
        let mut replay = repository
            .stream_events::<RecordedValues>("stream")
            .await
            .unwrap();
        let mut values = Vec::new();
        while let Some(event) = replay.next::<RecordedValues>(&[]).await {
            let event = event.unwrap();
            values.push((event.aggregate_id, event.sequence, event.payload));
        }
        assert_eq!(
            values,
            [
                (
                    "stream".to_string(),
                    1,
                    ValueEvent::Recorded {
                        value: "first".to_string()
                    }
                ),
                (
                    "stream".to_string(),
                    2,
                    ValueEvent::Recorded {
                        value: "second".to_string()
                    }
                ),
            ]
        );
        let mut replay = repository
            .stream_all_events::<RecordedValues>()
            .await
            .unwrap();
        let mut identities = Vec::new();
        while let Some(event) = replay.next::<RecordedValues>(&[]).await {
            let event = event.unwrap();
            identities.push((event.aggregate_id, event.sequence));
        }
        identities.sort();
        assert_eq!(
            identities,
            [
                ("other".to_string(), 1),
                ("stream".to_string(), 1),
                ("stream".to_string(), 2),
            ]
        );
    }

    #[sqlx::test(migrations = false)]
    async fn competing_commands_preserve_one_winning_snapshot(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(repository, 1);
        CqrsFramework::new(store, vec![], None)
            .execute("stream", "seed".to_string())
            .await
            .unwrap();
        let left = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(
            PostgresEventRepository { pool: pool.clone() },
            1,
        );
        let right = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(
            PostgresEventRepository { pool: pool.clone() },
            1,
        );
        let left_context = left.load_aggregate("stream").await.unwrap();
        let right_context = right.load_aggregate("stream").await.unwrap();
        let (left_result, right_result) = tokio::join!(
            left.commit(
                vec![ValueEvent::Recorded {
                    value: "left".to_string()
                }],
                left_context,
                HashMap::new()
            ),
            right.commit(
                vec![ValueEvent::Recorded {
                    value: "right".to_string()
                }],
                right_context,
                HashMap::new()
            ),
        );
        let winner = match (left_result, right_result) {
            (Ok(_), Err(AggregateError::AggregateConflict)) => "left",
            (Err(AggregateError::AggregateConflict), Ok(_)) => "right",
            results => panic!("expected one durable winner and one conflict, got {results:?}"),
        };
        let restored = left.load_aggregate("stream").await.unwrap();
        assert_eq!(restored.aggregate.values, ["seed", winner]);
        assert_eq!(restored.current_sequence, 2);
        assert_eq!(restored.current_snapshot, Some(2));
    }

    #[sqlx::test(migrations = false)]
    async fn unavailable_pool_surfaces_read_and_replay_errors(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        pool.close().await;
        let repository = PostgresEventRepository { pool };
        let error = repository
            .get_events::<RecordedValues>("stream")
            .await
            .unwrap_err();
        let PersistenceError::ConnectionError(source) = error else {
            panic!("expected a connection failure");
        };
        assert!(matches!(
            source.downcast_ref::<sqlx::Error>().unwrap(),
            sqlx::Error::PoolClosed
        ));
        let mut replay = repository
            .stream_all_events::<RecordedValues>()
            .await
            .unwrap();
        let error = replay
            .next::<RecordedValues>(&[])
            .await
            .unwrap()
            .unwrap_err();
        let PersistenceError::ConnectionError(source) = error else {
            panic!("expected a replay connection failure");
        };
        assert!(matches!(
            source.downcast_ref::<sqlx::Error>().unwrap(),
            sqlx::Error::PoolClosed
        ));
        assert_eq!(
            replay
                .next::<RecordedValues>(&[])
                .await
                .map(|event| event.unwrap().sequence),
            None
        );
    }

    #[sqlx::test(migrations = false)]
    async fn snapshot_mid_batch_reloads_every_committed_event(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(repository, 2);
        let context = store.load_aggregate("stream").await.unwrap();
        let events = ["first", "second", "third"]
            .into_iter()
            .map(|value| ValueEvent::Recorded {
                value: value.to_string(),
            })
            .collect();
        store.commit(events, context, HashMap::new()).await.unwrap();
        let repository = PostgresEventRepository { pool };
        let persisted = repository
            .get_events::<RecordedValues>("stream")
            .await
            .unwrap();
        assert_eq!(
            persisted
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
        let snapshot = repository
            .get_snapshot::<RecordedValues>("stream")
            .await
            .unwrap()
            .unwrap();
        let restored = store.load_aggregate("stream").await.unwrap();
        assert_eq!(
            restored.aggregate.values,
            ["first", "second", "third"],
            "snapshot reports sequence {} with payload {}",
            snapshot.current_sequence,
            snapshot.aggregate,
        );
        assert_eq!(restored.current_sequence, 3);
        let suffix = repository
            .get_last_events::<RecordedValues>("stream", snapshot.current_sequence)
            .await
            .unwrap();
        assert_eq!(snapshot.current_sequence, 2);
        assert_eq!(snapshot.current_snapshot, 1);
        assert_eq!(snapshot.aggregate, json!({"values": ["first", "second"]}));
        assert_eq!(suffix.len(), 1);
        assert_eq!(suffix[0].sequence, 3);

        let mut next_event = persisted[0].clone();
        next_event.sequence = 4;
        let error = repository
            .persist_with_snapshot::<RecordedValues>(
                &[next_event],
                Some(cqrs_es::persist::SerializedSnapshot {
                    current_sequence: 5,
                    current_snapshot: snapshot.current_snapshot + 1,
                    ..snapshot
                }),
            )
            .await
            .unwrap_err();
        let PersistenceError::UnknownError(source) = error else {
            panic!("expected a typed excessive-coverage error");
        };
        assert!(matches!(
            source.downcast_ref::<crate::engine::EngineError>().unwrap(),
            crate::engine::EngineError::SnapshotBeyondCurrentVersion {
                proposed: 5,
                current: 4
            }
        ));
        let after = repository
            .get_events::<RecordedValues>("stream")
            .await
            .unwrap();
        assert_eq!(after, persisted);
        let snapshot = repository
            .get_snapshot::<RecordedValues>("stream")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.current_sequence, 2);
        assert_eq!(snapshot.current_snapshot, 1);
        assert_eq!(snapshot.aggregate, json!({"values": ["first", "second"]}));

        for (current_sequence, aggregate) in [
            (1, json!({"values": ["first"]})),
            (2, json!({"values": ["first", "second"]})),
        ] {
            let mut next = persisted[0].clone();
            next.sequence = 4;
            let result = repository
                .persist_with_snapshot::<RecordedValues>(
                    &[next],
                    Some(cqrs_es::persist::SerializedSnapshot {
                        aggregate_id: "stream".to_string(),
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
                    .get_events::<RecordedValues>("stream")
                    .await
                    .unwrap(),
                persisted
            );
            let snapshot = repository
                .get_snapshot::<RecordedValues>("stream")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(snapshot.current_sequence, 2);
            assert_eq!(snapshot.current_snapshot, 1);
            assert_eq!(snapshot.aggregate, json!({"values": ["first", "second"]}));
        }
    }

    #[sqlx::test(migrations = false)]
    async fn dispatched_job_and_snapshot_commit_with_the_domain_event(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        let job_id = JobId::new();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(repository, 1);
        let framework = CqrsFramework::new(store, vec![], Some(job_id));
        crate::job::with_pending_scope(async {
            framework
                .execute("stream", "first".to_string())
                .await
                .unwrap();
        })
        .await;

        let repository = PostgresEventRepository { pool: pool.clone() };
        let events = repository
            .get_events::<RecordedValues>("stream")
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, json!({"Recorded": {"value": "first"}}));
        let snapshot = repository
            .get_snapshot::<RecordedValues>("stream")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.current_sequence, 1);
        assert_eq!(snapshot.current_snapshot, 1);
        assert_eq!(snapshot.aggregate, json!({"values": ["first"]}));

        let (version, payload, status): (i64, Value, String) = sqlx::query_as(
            "SELECT version, payload, status FROM job_queue_projection WHERE view_id = $1",
        )
        .bind(job_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(version, 1);
        assert_eq!(status, "pending");
        assert_eq!(payload["Live"]["Pending"]["kind"], json!("recorded-value"));
        assert_eq!(
            payload["Live"]["Pending"]["payload"],
            json!({"value": "work"})
        );
        let event_payload: Value = sqlx::query_scalar(
            r"
            SELECT payload FROM events
            WHERE aggregate_type = 'job' AND aggregate_id = $1 AND sequence = 1
            ",
        )
        .bind(job_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(event_payload["Enqueued"]["kind"], json!("recorded-value"));
        assert_eq!(
            event_payload["Enqueued"]["payload"],
            json!({"value": "work"})
        );
        assert_eq!(
            event_payload["Enqueued"]["run_at"],
            payload["Live"]["Pending"]["run_at"]
        );
    }

    #[sqlx::test(migrations = false)]
    async fn duplicate_dispatch_rolls_back_domain_event_and_snapshot(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        let job_id = JobId::new();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(repository, 1);
        let framework = CqrsFramework::new(store, vec![], Some(job_id));
        crate::job::with_pending_scope(async {
            framework
                .execute("stream", "first".to_string())
                .await
                .unwrap();
        })
        .await;
        let error = crate::job::with_pending_scope(async {
            framework.execute("stream", "second".to_string()).await
        })
        .await
        .unwrap_err();
        assert!(matches!(error, AggregateError::AggregateConflict));

        let repository = PostgresEventRepository { pool: pool.clone() };
        let snapshot = repository
            .get_snapshot::<RecordedValues>("stream")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.aggregate, json!({"values": ["first"]}));
        assert_eq!(snapshot.current_sequence, 1);
        assert_eq!(snapshot.current_snapshot, 1);
        let events = repository
            .get_events::<RecordedValues>("stream")
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 1);
        let version: i64 =
            sqlx::query_scalar("SELECT version FROM job_queue_projection WHERE view_id = $1")
                .bind(job_id.to_string())
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(version, 1);
    }

    #[sqlx::test(migrations = false)]
    async fn cancelled_command_rolls_back_and_releases_its_pooled_connection(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        let command_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(pool.connect_options().as_ref().clone())
            .await
            .unwrap();
        let mut blocker = pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE job_queue_projection IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *blocker)
            .await
            .unwrap();
        let job_id = JobId::new();
        let repository = PostgresEventRepository {
            pool: command_pool.clone(),
        };
        let store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(repository, 1);
        let framework = CqrsFramework::new(store, vec![], Some(job_id));
        let command = tokio::spawn(crate::job::with_pending_scope(async move {
            framework.execute("cancelled", "before".to_string()).await
        }));
        timeout(Duration::from_secs(10), async {
            loop {
                let waiting: bool = sqlx::query_scalar(
                    r"
                    SELECT EXISTS (
                        SELECT 1 FROM pg_locks
                        WHERE database = (SELECT oid FROM pg_database
                                          WHERE datname = current_database())
                          AND relation = 'job_queue_projection'::REGCLASS
                          AND NOT granted
                    )
                    ",
                )
                .fetch_one(&pool)
                .await
                .unwrap();
                if waiting {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("command must reach the blocked queue write");
        command.abort();
        assert!(command.await.unwrap_err().is_cancelled());
        blocker.rollback().await.unwrap();

        let rows: (i64, i64, i64, i64) = timeout(
            Duration::from_secs(10),
            sqlx::query_as(
                r"
                SELECT (
                    SELECT COUNT(*) FROM events
                    WHERE aggregate_type = 'RecordedValues' AND aggregate_id = 'cancelled'
                ), (
                    SELECT COUNT(*) FROM snapshots
                    WHERE aggregate_type = 'RecordedValues' AND aggregate_id = 'cancelled'
                ), (
                    SELECT COUNT(*) FROM events WHERE aggregate_type = 'job' AND aggregate_id = $1
                ), (
                    SELECT COUNT(*) FROM job_queue_projection WHERE view_id = $1
                )
                ",
            )
            .bind(job_id.to_string())
            .fetch_one(&command_pool),
        )
        .await
        .expect("cancelled transaction must release the only connection")
        .unwrap();
        assert_eq!(rows, (0, 0, 0, 0));

        let repository = PostgresEventRepository {
            pool: command_pool.clone(),
        };
        let store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(repository, 1);
        let framework = CqrsFramework::new(store, vec![], Some(job_id));
        crate::job::with_pending_scope(async {
            framework
                .execute("cancelled", "after".to_string())
                .await
                .unwrap();
        })
        .await;
        let repository = PostgresEventRepository {
            pool: command_pool.clone(),
        };
        let events = repository
            .get_events::<RecordedValues>("cancelled")
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 1);
        assert_eq!(events[0].payload, json!({"Recorded": {"value": "after"}}));
        command_pool.close().await;
    }

    #[sqlx::test(migrations = false)]
    async fn cancelled_enqueue_rolls_back_and_reuses_its_connection(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        let singleton = PgPoolOptions::new()
            .max_connections(1)
            .connect_with((*pool.connect_options()).clone())
            .await
            .unwrap();
        let writer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&singleton)
            .await
            .unwrap();
        let mut blocker = pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE job_queue_projection IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *blocker)
            .await
            .unwrap();

        let job_id = JobId::new();
        let writer_pool = singleton.clone();
        let pending = tokio::spawn(async move {
            let repository = PostgresEventRepository { pool: writer_pool };
            repository
                .enqueue(JobSeed::new(
                    job_id,
                    "cancel-probe",
                    json!({"value": "work"}),
                    1_700_000_000_000,
                ))
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
                     WHERE pid = $1 AND wait_event_type = 'Lock' \
                     AND query LIKE '%INSERT INTO job_queue_projection%')",
                )
                .bind(writer_pid)
                .fetch_one(&pool)
                .await
                .unwrap();
                if waiting {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        blocker.rollback().await.unwrap();
        let mut connection = tokio::time::timeout(Duration::from_secs(5), singleton.acquire())
            .await
            .unwrap()
            .unwrap();
        let ready: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(ready, 1);
        let counts: (i64, i64) = sqlx::query_as(
            "SELECT \
             (SELECT COUNT(*) FROM events WHERE aggregate_type = 'job' AND aggregate_id = $1), \
             (SELECT COUNT(*) FROM job_queue_projection WHERE view_id = $1)",
        )
        .bind(job_id.to_string())
        .fetch_one(&mut *connection)
        .await
        .unwrap();
        assert_eq!(counts, (0, 0));
        drop(connection);
        singleton.close().await;
    }

    #[sqlx::test(migrations = false)]
    async fn queue_write_failure_rolls_back_standalone_event(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        sqlx::query("ALTER TABLE job_queue_projection ADD CONSTRAINT reject_seed CHECK (FALSE)")
            .execute(&pool)
            .await
            .unwrap();
        let job_id = JobId::new();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let error = repository
            .enqueue(JobSeed::new(
                job_id,
                "rollback-probe",
                json!({"value": "work"}),
                1_700_000_000_000,
            ))
            .await
            .unwrap_err();
        let PersistenceError::ConnectionError(source) = error else {
            panic!("expected the injected queue constraint failure");
        };
        let database = source
            .downcast_ref::<sqlx::Error>()
            .unwrap()
            .as_database_error()
            .unwrap();
        assert_eq!(database.code().as_deref(), Some("23514"));
        assert_eq!(database.constraint(), Some("reject_seed"));

        let counts: (i64, i64) = sqlx::query_as(
            "SELECT \
             (SELECT COUNT(*) FROM events WHERE aggregate_type = 'job' AND aggregate_id = $1), \
             (SELECT COUNT(*) FROM job_queue_projection WHERE view_id = $1)",
        )
        .bind(job_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(counts, (0, 0));
    }

    #[sqlx::test(migrations = false)]
    async fn standalone_enqueue_preserves_first_event_and_queue_row_on_duplicate(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        let job_id = JobId::new();
        let repository = PostgresEventRepository { pool: pool.clone() };
        repository
            .enqueue(JobSeed::new(
                job_id,
                "standalone",
                json!({"value": "first"}),
                1_700_000_000_000,
            ))
            .await
            .unwrap();
        let error = repository
            .enqueue(JobSeed::new(
                job_id,
                "standalone",
                json!({"value": "replacement"}),
                1_700_000_030_000,
            ))
            .await
            .unwrap_err();
        assert!(matches!(error, PersistenceError::OptimisticLockError));
        let (version, payload, lease): (i64, Value, Option<i64>) = sqlx::query_as(
            "SELECT version, payload, lease_until FROM job_queue_projection WHERE view_id = $1",
        )
        .bind(job_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(version, 1);
        assert_eq!(lease, None);
        assert_eq!(
            payload,
            json!({"Live": {"Pending": {
                "kind": "standalone", "payload": {"value": "first"},
                "run_at": 1_700_000_000_000_i64, "attempt": 0, "claims": 0,
            }}})
        );
        let events: Vec<(i64, Value, Value)> = sqlx::query_as(
            r"
            SELECT sequence, payload, metadata FROM events
            WHERE aggregate_type = 'job' AND aggregate_id = $1 ORDER BY sequence
            ",
        )
        .bind(job_id.to_string())
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            events,
            [(
                1,
                json!({"Enqueued": {
                    "kind": "standalone", "payload": {"value": "first"},
                    "run_at": 1_700_000_000_000_i64,
                }}),
                json!({})
            )]
        );
    }

    #[sqlx::test(migrations = false)]
    async fn unrelated_unique_constraint_is_not_an_optimistic_conflict(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let retained = JobId::new();
        let rejected = JobId::new();
        repository
            .enqueue(JobSeed::new(
                retained,
                "unique-kind",
                json!({"value": "retained"}),
                1_700_000_000_000,
            ))
            .await
            .unwrap();
        sqlx::query("CREATE UNIQUE INDEX one_job_per_kind ON job_queue_projection (kind)")
            .execute(&pool)
            .await
            .unwrap();

        let error = repository
            .enqueue(JobSeed::new(
                rejected,
                "unique-kind",
                json!({"value": "rejected"}),
                1_700_000_000_000,
            ))
            .await
            .unwrap_err();
        let PersistenceError::ConnectionError(source) = error else {
            panic!("expected the unrelated unique constraint to remain a storage error: {error:?}");
        };
        let source = source.downcast_ref::<sqlx::Error>().unwrap();
        let source = source.as_database_error().unwrap();
        assert_eq!(source.code().as_deref(), Some("23505"));
        assert_eq!(source.table(), Some("job_queue_projection"));
        assert_eq!(source.constraint(), Some("one_job_per_kind"));
        assert!(
            repository
                .get_events::<Lifecycle<JobState>>(&rejected.to_string())
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            repository
                .get_events::<Lifecycle<JobState>>(&retained.to_string())
                .await
                .unwrap()
                .len(),
            1
        );
        let rows: Vec<(String, Value)> =
            sqlx::query_as("SELECT view_id, payload FROM job_queue_projection")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, retained.to_string());
        assert_eq!(
            rows[0].1["Live"]["Pending"]["payload"],
            json!({"value": "retained"})
        );
    }

    #[sqlx::test(migrations = false)]
    async fn invalid_enqueue_instant_leaves_no_event_or_queue_row(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        let job_id = JobId::new();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let error = repository
            .enqueue(JobSeed::new(
                job_id,
                "standalone",
                json!({"value": "work"}),
                i64::MAX,
            ))
            .await
            .unwrap_err();
        let PersistenceError::UnknownError(source) = error else {
            panic!("expected the invalid scheduling instant to fail");
        };
        assert!(matches!(
            source.downcast_ref::<crate::job::JobStoreError>().unwrap(),
            crate::job::JobStoreError::InvalidInstant(i64::MAX)
        ));
        let rows: (i64, i64) = sqlx::query_as(
            r"
            SELECT (
                SELECT COUNT(*) FROM events
                WHERE aggregate_type = 'job' AND aggregate_id = $1
            ), (
                SELECT COUNT(*) FROM job_queue_projection WHERE view_id = $1
            )
            ",
        )
        .bind(job_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(rows, (0, 0));
    }

    #[sqlx::test(migrations = false)]
    async fn bootstrap_preserves_job_rows_and_generated_poll_columns(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();

        let run_at = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let lease_until = DateTime::from_timestamp_millis(1_700_000_030_000).unwrap();
        let pending = JobState::originate(&JobEvent::Enqueued {
            kind: JobKind::new("bootstrap-job"),
            payload: json!({ "opaque": true }),
            run_at,
        })
        .unwrap();
        let claimed = JobState::evolve(
            &pending,
            &JobEvent::Claimed {
                worker: WorkerId::new("bootstrap-worker"),
                claim_id: ClaimId::new(),
                lease_until,
            },
        )
        .unwrap()
        .unwrap();
        let done = JobState::evolve(&claimed, &JobEvent::Succeeded)
            .unwrap()
            .unwrap();
        let dead = JobState::evolve(
            &claimed,
            &JobEvent::Dead {
                reason: DeadReason::Rejected,
                error: "rejected".to_string(),
            },
        )
        .unwrap()
        .unwrap();
        let cases = [
            (pending, "pending", Some(1_700_000_000_000_i64), 1_i64, None),
            (
                claimed,
                "claimed",
                Some(1_700_000_000_000),
                2,
                Some(1_700_000_030_000),
            ),
            (done, "done", None, 3, Some(1_700_000_030_000)),
            (dead, "dead", None, 3, Some(1_700_000_030_000)),
        ];

        for (state, expected_status, expected_run_at, expected_version, expected_lease_until) in
            cases
        {
            let view_id = JobId::new().to_string();
            let payload = serde_json::to_value(Lifecycle::Live(state)).unwrap();
            sqlx::query(
                r"
                INSERT INTO job_queue_projection (view_id, version, payload, lease_until)
                VALUES ($1, $2, $3, $4)
                ",
            )
            .bind(&view_id)
            .bind(expected_version)
            .bind(payload.clone())
            .bind(expected_lease_until)
            .execute(&pool)
            .await
            .unwrap();

            POSTGRES_MIGRATOR.run(&pool).await.unwrap();

            let (stored_kind, status, stored_run_at, stored_version, stored_payload, stored_lease):
                (String, String, Option<i64>, i64, Value, Option<i64>) = sqlx::query_as(
                    r"
                    SELECT kind, status, run_at, version, payload, lease_until
                    FROM job_queue_projection
                    WHERE view_id = $1
                    ",
                )
                .bind(&view_id)
                .fetch_one(&pool)
                .await
                .unwrap();

            assert_eq!(
                (stored_kind.as_str(), status.as_str(), stored_run_at),
                ("bootstrap-job", expected_status, expected_run_at),
            );
            assert_eq!(stored_version, expected_version);
            assert_eq!(stored_payload, payload);
            assert_eq!(stored_lease, expected_lease_until);
        }
    }

    #[sqlx::test(migrations = false)]
    async fn queue_rejects_nonpositive_projection_versions(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();

        for version in [0_i64, -1] {
            let view_id = JobId::new().to_string();
            let payload = serde_json::to_value(Lifecycle::Live(JobState::Pending {
                kind: JobKind::new("bootstrap-job"),
                payload: json!({ "opaque": true }),
                run_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                attempt: 0,
                claims: 0,
            }))
            .unwrap();
            let error = sqlx::query(
                "INSERT INTO job_queue_projection (view_id, version, payload) VALUES ($1, $2, $3)",
            )
            .bind(&view_id)
            .bind(version)
            .bind(payload)
            .execute(&pool)
            .await
            .unwrap_err();

            let sqlx::Error::Database(error) = error else {
                panic!("expected a database constraint failure");
            };
            assert_eq!(error.code().as_deref(), Some("23514"));
        }

        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_queue_projection")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(rows, 0);
    }
}
