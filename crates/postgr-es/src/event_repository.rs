//! PostgreSQL persistence through the shared CQRS repository contracts.

use cqrs_es::Aggregate;
use cqrs_es::persist::{
    PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot,
};
use serde_json::Value;

use event_sorcery::{
    CommitRequest, EngineError, JobSeed, PreparedCommit, SnapshotUpdate, validate_expected_version,
};

/// Pool-backed PostgreSQL repository for the CQRS persistence contract.
///
/// Use it through [`cqrs_es::persist::PersistedEventStore`]. For native entity
/// snapshot-policy enforcement, use [`crate::PostgresBackend`]'s persistence factory.
pub struct PostgresEventRepository {
    /// Application-provisioned PostgreSQL connection pool.
    pub pool: sqlx::PgPool,
}

/// Framework-owned PostgreSQL schema migrations.
///
/// Applications apply these migrations to their PostgreSQL pool before using
/// framework persistence. Existing SQLite migrations remain in `sqlite-es`.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

impl PersistedEventRepository for PostgresEventRepository {
    async fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.official().get_events::<A>(aggregate_id).await
    }

    async fn get_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.official()
            .get_last_events::<A>(aggregate_id, last_sequence)
            .await
    }

    async fn get_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        self.official().get_snapshot::<A>(aggregate_id).await
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
        self.official().stream_events::<A>(aggregate_id).await
    }

    async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
        self.official().stream_all_events::<A>().await
    }
}

impl PostgresEventRepository {
    pub(crate) fn official(&self) -> postgres_es::PostgresEventRepository {
        let repository = postgres_es::PostgresEventRepository::new(self.pool.clone());
        repository.with_tables_and_snapshot_counter("events", "snapshots", "snapshot_version")
    }

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

    async fn commit(&self, request: CommitRequest<'_>) -> Result<(), PersistenceError> {
        let PreparedCommit {
            aggregate_type,
            aggregate_id,
            expected_version,
            events,
            snapshot,
            jobs,
            ..
        } = request.prepare()?;
        let mut transaction = self.pool.begin().await.map_err(EngineError::from)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&mut *transaction)
            .await
            .map_err(EngineError::from)?;

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
            .bind(&aggregate_type)
            .bind(&aggregate_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(write_error)?;
            validate_expected_version(
                expected_version,
                usize::try_from(actual).map_err(EngineError::from)?,
            )?;
        }

        let repository = self.official();
        repository
            .append_serialized_events_in(&mut transaction, &aggregate_type, events)
            .await
            .map_err(PersistenceError::from)
            .map_err(adapter_error)?;
        if let Some(snapshot) = snapshot {
            let previous = repository
                .get_snapshot_in(&mut transaction, &aggregate_type, &aggregate_id)
                .await?;
            if previous.is_some_and(|previous| previous.current_sequence >= snapshot.last_sequence)
            {
                return Err(EngineError::OptimisticLock.into());
            }
            let snapshot = SerializedSnapshot {
                aggregate_id: aggregate_id.clone(),
                aggregate: snapshot.aggregate,
                current_sequence: snapshot.last_sequence,
                current_snapshot: snapshot.snapshot_version,
            };
            let rows = repository
                .persist_snapshot_in(&mut transaction, &aggregate_type, &snapshot)
                .await
                .map_err(PersistenceError::from)
                .map_err(adapter_error)?;
            if rows != 1 {
                return Err(EngineError::OptimisticLock.into());
            }
            sqlx::query(
                "UPDATE snapshots SET timestamp = CURRENT_TIMESTAMP \
                WHERE aggregate_type = $1 AND aggregate_id = $2",
            )
            .bind(&aggregate_type)
            .bind(&aggregate_id)
            .execute(&mut *transaction)
            .await
            .map_err(write_error)?;
        }

        for job in jobs {
            repository
                .append_serialized_events_in(
                    &mut transaction,
                    &job.event.aggregate_type,
                    std::slice::from_ref(&job.event),
                )
                .await
                .map_err(PersistenceError::from)
                .map_err(adapter_error)?;
            sqlx::query(
                r"
                INSERT INTO job_queue_projection (view_id, version, payload, lease_until)
                VALUES ($1, 1, $2::JSONB, NULL)
                ",
            )
            .bind(&job.event.aggregate_id)
            .bind(job.queue_payload)
            .execute(&mut *transaction)
            .await
            .map_err(write_error)?;
        }

        transaction.commit().await.map_err(write_error)?;
        Ok(())
    }
}

pub(crate) fn adapter_error(error: PersistenceError) -> PersistenceError {
    match error {
        PersistenceError::UnknownError(source) => match source.downcast::<sqlx::Error>() {
            Ok(source) => write_error(*source).into(),
            Err(source) => PersistenceError::UnknownError(source),
        },
        other => other,
    }
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
    use async_trait::async_trait;
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

    use event_sorcery::{
        CompactionPolicy, Effect, EventSourced, JobId, JobSeed, Never, Nil, PersistenceBackend,
        StoreBuilder, buffer_pending_job, with_pending_jobs,
    };

    use super::{MIGRATOR, PostgresEventRepository};
    use crate::PostgresBackend;

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
                buffer_pending_job(*job_id, "recorded-value", json!({"value": "work"})).unwrap();
            }
            Ok(())
        }

        fn apply(&mut self, event: ValueEvent) {
            let ValueEvent::Recorded { value } = event;
            self.values.push(value);
        }
    }

    #[async_trait]
    impl EventSourced for RecordedValues {
        type Id = String;
        type Command = String;
        type Event = ValueEvent;
        type Error = Never;
        type Materialized = Nil;
        type Jobs = Nil;

        const AGGREGATE_TYPE: &'static str = "RecordedValues";
        const SCHEMA_VERSION: u64 = 1;
        const SNAPSHOT_SIZE: usize = 1;
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::CompactAfterSnapshot;
        const PROJECTION: Nil = Nil;

        fn originate(event: &ValueEvent) -> Option<Self> {
            let ValueEvent::Recorded { value } = event;
            Some(Self {
                values: vec![value.clone()],
            })
        }

        fn evolve(entity: &Self, event: &ValueEvent) -> Result<Option<Self>, Never> {
            let ValueEvent::Recorded { value } = event;
            let mut next = entity.clone();
            next.values.push(value.clone());
            Ok(Some(next))
        }

        async fn initialize(value: String) -> Result<Effect<Self>, Never> {
            Ok(Effect::Events(vec![ValueEvent::Recorded { value }]))
        }

        async fn transition(&self, value: String) -> Result<Effect<Self>, Never> {
            Self::initialize(value).await
        }
    }

    #[sqlx::test(migrations = false)]
    async fn first_native_registration_preserves_a_preregistry_snapshot(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
        let backend = PostgresBackend { pool: pool.clone() };
        let event_store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(
            backend.persistence_repo(CompactionPolicy::CompactAfterSnapshot),
            1,
        );
        let framework = CqrsFramework::new(event_store, vec![], None);
        framework
            .execute("legacy", "first".to_string())
            .await
            .unwrap();
        let snapshot_sql = "SELECT last_sequence, snapshot_version, payload FROM snapshots \
                            WHERE aggregate_type = 'RecordedValues' AND aggregate_id = 'legacy'";
        let before: (i64, i64, Value) =
            sqlx::query_as(snapshot_sql).fetch_one(&pool).await.unwrap();
        let registry_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'SchemaRegistry'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(registry_count, 0);

        let store = StoreBuilder::<RecordedValues, PostgresBackend>::with_backend(backend)
            .build()
            .await
            .unwrap();

        let after: (i64, i64, Value) = sqlx::query_as(snapshot_sql).fetch_one(&pool).await.unwrap();
        assert_eq!(after, before);
        let fresh = "fresh".to_string();
        store.send(&fresh, "new".to_string()).await.unwrap();
        assert_eq!(store.load(&fresh).await.unwrap().unwrap().values, ["new"]);
    }

    #[sqlx::test(migrations = false)]
    async fn native_factory_guards_compactable_snapshots_without_mutation(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
        let backend = PostgresBackend { pool: pool.clone() };
        let store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(
            backend.persistence_repo(CompactionPolicy::Retain),
            1,
        );
        let framework = CqrsFramework::new(store, vec![], None);
        framework
            .execute("guarded", "first".to_string())
            .await
            .unwrap();
        for policy in [
            CompactionPolicy::CompactAfterSnapshot,
            CompactionPolicy::RetainLast { events: 10 },
        ] {
            let repository = backend.persistence_repo(policy);
            let snapshot = repository
                .get_snapshot::<RecordedValues>("guarded")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(snapshot.aggregate, json!({"values": ["first"]}));
            assert_eq!(snapshot.current_sequence, 1);
            assert_eq!(snapshot.current_snapshot, 1);
            let missing = repository
                .get_snapshot::<RecordedValues>("missing")
                .await
                .unwrap();
            assert_eq!(missing.map(|snapshot| snapshot.current_sequence), None);
        }

        let incompatible = json!({"obsolete_values": ["first"]});
        sqlx::query("UPDATE snapshots SET payload = $1 WHERE aggregate_id = $2")
            .bind(&incompatible)
            .bind("guarded")
            .execute(&pool)
            .await
            .unwrap();

        for policy in [
            CompactionPolicy::CompactAfterSnapshot,
            CompactionPolicy::RetainLast { events: 10 },
        ] {
            let repository = backend.persistence_repo(policy);
            let error = repository
                .get_snapshot::<RecordedValues>("guarded")
                .await
                .unwrap_err();
            let PersistenceError::DeserializationError(source) = error else {
                panic!("expected an incompatible compactable snapshot to fail");
            };
            let source = source.downcast::<serde_json::Error>().unwrap();
            assert_eq!(source.classify(), serde_json::error::Category::Data);
        }

        let repository = backend.persistence_repo(CompactionPolicy::Retain);
        let snapshot = repository
            .get_snapshot::<RecordedValues>("guarded")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.aggregate, incompatible);
        let store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(repository, 1);
        let loaded = store.load_aggregate("guarded").await.unwrap();
        assert_eq!(loaded.aggregate.values, ["first"]);
        assert_eq!(loaded.current_sequence, 1);
        let stored: Value =
            sqlx::query_scalar("SELECT payload FROM snapshots WHERE aggregate_id = $1")
                .bind("guarded")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(stored, incompatible);

        let framework = CqrsFramework::new(store, vec![], None);
        framework
            .execute("guarded", "second".to_string())
            .await
            .unwrap();
        let repository = backend.persistence_repo(CompactionPolicy::Retain);
        let snapshot = repository
            .get_snapshot::<RecordedValues>("guarded")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.current_sequence, 2);
        assert_eq!(snapshot.current_snapshot, 2);
        assert_eq!(snapshot.aggregate, json!({"values": ["first", "second"]}));
        let store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(repository, 1);
        let reloaded = store.load_aggregate("guarded").await.unwrap();
        assert_eq!(reloaded.aggregate.values, ["first", "second"]);
        assert_eq!(reloaded.current_sequence, 2);
    }

    #[sqlx::test(migrations = false)]
    async fn repository_rejects_gapped_appends_without_writing(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
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
        MIGRATOR.run(&pool).await.unwrap();
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
        MIGRATOR.run(&pool).await.unwrap();
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
        MIGRATOR.run(&pool).await.unwrap();
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
        assert_one_winning_snapshot(pool, &["seed"]).await;
    }

    #[sqlx::test(migrations = false)]
    async fn competing_first_commands_preserve_one_winning_snapshot(pool: PgPool) {
        assert_one_winning_snapshot(pool, &[]).await;
    }

    async fn assert_one_winning_snapshot(pool: PgPool, prefix: &[&str]) {
        MIGRATOR.run(&pool).await.unwrap();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(repository, 1);
        let framework = CqrsFramework::new(store, vec![], None);
        for value in prefix {
            framework
                .execute("stream", (*value).to_owned())
                .await
                .unwrap();
        }

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
        let expected: Vec<_> = prefix
            .iter()
            .copied()
            .chain(std::iter::once(winner))
            .collect();
        let sequence = expected.len();
        assert_eq!(restored.aggregate.values, expected);
        assert_eq!(restored.current_sequence, sequence);
        assert_eq!(restored.current_snapshot, Some(sequence));
        let events = PostgresEventRepository { pool }
            .get_events::<RecordedValues>("stream")
            .await
            .unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            (1..=sequence).collect::<Vec<_>>()
        );
    }

    #[sqlx::test(migrations = false)]
    async fn unavailable_pool_surfaces_read_and_replay_errors(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
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
        MIGRATOR.run(&pool).await.unwrap();
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
            source.downcast_ref::<event_sorcery::EngineError>().unwrap(),
            event_sorcery::EngineError::SnapshotBeyondCurrentVersion {
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
        MIGRATOR.run(&pool).await.unwrap();
        let job_id = JobId::new();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(repository, 1);
        let framework = CqrsFramework::new(store, vec![], Some(job_id));
        with_pending_jobs(async {
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
        MIGRATOR.run(&pool).await.unwrap();
        let job_id = JobId::new();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(repository, 1);
        let framework = CqrsFramework::new(store, vec![], Some(job_id));
        with_pending_jobs(async {
            framework
                .execute("stream", "first".to_string())
                .await
                .unwrap();
        })
        .await;
        let error =
            with_pending_jobs(async { framework.execute("stream", "second".to_string()).await })
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
        MIGRATOR.run(&pool).await.unwrap();
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
        let command = tokio::spawn(with_pending_jobs(async move {
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
        with_pending_jobs(async {
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
        MIGRATOR.run(&pool).await.unwrap();
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
        MIGRATOR.run(&pool).await.unwrap();
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
        MIGRATOR.run(&pool).await.unwrap();
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
    async fn append_preserves_application_unique_constraint_identity(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
        sqlx::query(
            "CREATE UNIQUE INDEX one_event_per_type ON events (aggregate_type, event_type)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let store = PersistedEventStore::<_, RecordedValues>::new_event_store(repository);
        let framework = CqrsFramework::new(store, vec![], None);
        framework
            .execute("first", "saved".to_string())
            .await
            .unwrap();
        let error = framework
            .execute("second", "rejected".to_string())
            .await
            .unwrap_err();
        let AggregateError::DatabaseConnectionError(source) = error else {
            panic!("expected the application constraint to remain a storage error: {error:?}");
        };
        let source = source.downcast_ref::<sqlx::Error>().unwrap();
        let source = source.as_database_error().unwrap();
        assert_eq!(source.code().as_deref(), Some("23505"));
        assert_eq!(source.table(), Some("events"));
        assert_eq!(source.constraint(), Some("one_event_per_type"));
        let persisted: Vec<String> = sqlx::query_scalar("SELECT aggregate_id FROM events")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(persisted, ["first"]);
    }

    #[sqlx::test(migrations = false)]
    async fn unrelated_unique_constraint_is_not_an_optimistic_conflict(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
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
        let mut transaction = pool.begin().await.unwrap();
        assert!(
            repository
                .official()
                .get_serialized_events_in(&mut transaction, "job", &rejected.to_string())
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            repository
                .official()
                .get_serialized_events_in(&mut transaction, "job", &retained.to_string())
                .await
                .unwrap()
                .len(),
            1
        );
        transaction.rollback().await.unwrap();
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
        MIGRATOR.run(&pool).await.unwrap();
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
            source
                .downcast_ref::<event_sorcery::JobStoreError>()
                .unwrap(),
            event_sorcery::JobStoreError::InvalidInstant(i64::MAX)
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
        MIGRATOR.run(&pool).await.unwrap();

        let pending = json!({"Live": {"Pending": {
            "kind": "bootstrap-job", "payload": {"opaque": true},
            "run_at": 1_700_000_000_000_i64, "attempt": 0, "claims": 0,
        }}});
        let claimed = json!({"Live": {"Claimed": {
            "kind": "bootstrap-job", "payload": {"opaque": true},
            "worker": "bootstrap-worker", "claim_id": JobId::new().to_string(),
            "run_at": 1_700_000_000_000_i64, "attempt": 0, "claims": 1,
        }}});
        let done = json!({"Live": {"Done": {"kind": "bootstrap-job"}}});
        let dead = json!({"Live": {"Dead": {
            "kind": "bootstrap-job", "reason": "Rejected",
        }}});
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
            let payload = state;
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

            MIGRATOR.run(&pool).await.unwrap();

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
        MIGRATOR.run(&pool).await.unwrap();

        for version in [0_i64, -1] {
            let view_id = JobId::new().to_string();
            let payload = json!({"Live": {"Pending": {
                "kind": "bootstrap-job", "payload": {"opaque": true},
                "run_at": 1_700_000_000_000_i64, "attempt": 0, "claims": 0,
            }}});
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
