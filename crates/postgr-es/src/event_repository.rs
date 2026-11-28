//! PostgreSQL persistence through the shared CQRS repository contracts.

use cqrs_es::Aggregate;
use cqrs_es::persist::{
    PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot,
};
use serde_json::Value;

use event_sorcery::{
    ClaimDecision, ClaimOutcome, ClaimRead, CommitRequest, EngineError, JobSeed, LeaseRenewal,
    SnapshotUpdate, StreamIdentity, validate_expected_version,
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

    pub(crate) async fn claim_job<Decide, Won>(
        &self,
        job_id: &str,
        decide: Decide,
    ) -> Result<ClaimOutcome<Won>, PostgresClaimError>
    where
        Decide: FnOnce(Option<ClaimRead>) -> ClaimDecision<Won> + Send,
        Won: Send,
    {
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query_as::<_, (i64, String, Option<i64>)>(
            "SELECT version, payload::text, lease_until
             FROM job_queue_projection WHERE view_id = $1 FOR UPDATE",
        )
        .bind(job_id)
        .fetch_optional(&mut *transaction)
        .await?
        .map(|(version, payload, lease_until_ms)| ClaimRead {
            version,
            payload,
            lease_until_ms,
        });
        let previous_version = row.as_ref().map(|row| row.version);
        let (event, payload, lease, outcome) = match decide(row) {
            ClaimDecision::Skip => {
                transaction.rollback().await?;
                return Ok(ClaimOutcome::Skip);
            }
            ClaimDecision::Claim {
                event,
                payload,
                lease_until_ms,
                won,
            } => (event, payload, Some(lease_until_ms), ClaimOutcome::Won(won)),
            ClaimDecision::Abandon { event, payload } => {
                (event, payload, None, ClaimOutcome::Abandoned)
            }
        };
        let sequence = i64::try_from(event.sequence)?;
        if event.aggregate_type != "job"
            || event.aggregate_id != job_id
            || previous_version.and_then(|version| version.checked_add(1)) != Some(sequence)
        {
            return Err(PostgresClaimError::InvalidEvent);
        }

        let append = self
            .official()
            .append_serialized_events_in(&mut transaction, "job", std::slice::from_ref(&event))
            .await;
        match append {
            Ok(_) => {}
            Err(error) => match adapter_error(error.into()) {
                PersistenceError::OptimisticLockError => {
                    transaction.rollback().await?;
                    return Ok(ClaimOutcome::Contended);
                }
                other => return Err(other.into()),
            },
        }
        let updated = sqlx::query(
            "UPDATE job_queue_projection SET version = $2, payload = $3::jsonb, lease_until = $4
             WHERE view_id = $1",
        )
        .bind(job_id)
        .bind(sequence)
        .bind(payload)
        .bind(lease)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(PostgresClaimError::ProjectionCount);
        }
        transaction.commit().await?;
        Ok(outcome)
    }

    pub(crate) async fn renew_job(
        &self,
        job_id: &str,
        claim_seq: i64,
        new_lease_until_ms: i64,
    ) -> Result<LeaseRenewal, sqlx::Error> {
        let updated = sqlx::query(
            "UPDATE job_queue_projection SET lease_until = $3
             WHERE view_id = $1 AND version = $2 AND status = 'claimed'",
        )
        .bind(job_id)
        .bind(claim_seq)
        .bind(new_lease_until_ms)
        .execute(&self.pool)
        .await?;
        if updated.rows_affected() == 0 {
            Ok(LeaseRenewal::Lost)
        } else {
            Ok(LeaseRenewal::Held)
        }
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
        let (stream, expected_version, events, snapshot, jobs) = request.check()?.into_parts();
        let StreamIdentity {
            aggregate_type,
            aggregate_id,
        } = stream;
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
            let snapshot = snapshot.prepare(events)?;
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
            let event = job.event().map_err(EngineError::from)?;
            repository
                .append_serialized_events_in(
                    &mut transaction,
                    &event.aggregate_type,
                    std::slice::from_ref(&event),
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
            .bind(&event.aggregate_id)
            .bind(job.queue_payload().map_err(EngineError::from)?)
            .execute(&mut *transaction)
            .await
            .map_err(write_error)?;
        }

        transaction.commit().await.map_err(write_error)?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PostgresClaimError {
    #[error("claim event must target the next sequence of the locked job stream")]
    InvalidEvent,
    #[error("claim must update exactly one projection row")]
    ProjectionCount,
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error(transparent)]
    Sequence(#[from] std::num::TryFromIntError),
    #[error(transparent)]
    Append(#[from] PersistenceError),
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
        StoreBuilder, buffer_pending_job, find_recovered_jobs, plan_test_claim,
        recover_job_queue_row, with_pending_jobs,
    };
    use indexed_query::{Cmp, Predicate, Term, Value as IndexedValue};

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
        assert_eq!(
            events
                .into_iter()
                .map(|event| event.payload)
                .collect::<Vec<_>>(),
            expected
                .into_iter()
                .map(|value| serde_json::json!({"Recorded": {"value": value}}))
                .collect::<Vec<_>>()
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
    async fn stale_version_takes_precedence_over_invalid_snapshot_coverage(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
        let store =
            PersistedEventStore::<_, RecordedValues>::new_event_store(PostgresEventRepository {
                pool: pool.clone(),
            });
        let repository = PostgresEventRepository { pool };
        let framework = CqrsFramework::new(store, vec![], None);
        framework
            .execute("stream", "first".to_owned())
            .await
            .unwrap();
        let mut event = repository
            .get_events::<RecordedValues>("stream")
            .await
            .unwrap()
            .remove(0);
        event.sequence = 2;
        let invalid_snapshot = || cqrs_es::persist::SerializedSnapshot {
            aggregate_id: "stream".to_owned(),
            aggregate: json!({"values": ["first", "second", "third"]}),
            current_sequence: 3,
            current_snapshot: 1,
        };

        let error = repository
            .persist_with_snapshot::<RecordedValues>(
                std::slice::from_ref(&event),
                Some(invalid_snapshot()),
            )
            .await
            .unwrap_err();
        let PersistenceError::UnknownError(source) = error else {
            panic!("expected invalid snapshot coverage, got {error:?}");
        };
        assert!(matches!(
            source.downcast_ref::<event_sorcery::EngineError>(),
            Some(event_sorcery::EngineError::SnapshotBeyondCurrentVersion {
                proposed: 3,
                current: 2
            })
        ));
        let events = repository
            .get_events::<RecordedValues>("stream")
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 1);
        assert!(
            repository
                .get_snapshot::<RecordedValues>("stream")
                .await
                .unwrap()
                .is_none()
        );

        framework
            .execute("stream", "winner".to_owned())
            .await
            .unwrap();
        let before = repository
            .get_events::<RecordedValues>("stream")
            .await
            .unwrap();
        assert_eq!(before[1].payload, json!({"Recorded": {"value": "winner"}}));
        let error = repository
            .persist_with_snapshot::<RecordedValues>(
                std::slice::from_ref(&event),
                Some(invalid_snapshot()),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, PersistenceError::OptimisticLockError),
            "{error:?}"
        );
        assert_eq!(
            repository
                .get_events::<RecordedValues>("stream")
                .await
                .unwrap(),
            before
        );
        assert!(
            repository
                .get_snapshot::<RecordedValues>("stream")
                .await
                .unwrap()
                .is_none()
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
    async fn queue_recovers_a_missing_pending_job_without_workers(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
        let job_id = JobId::new();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let backend = PostgresBackend { pool: pool.clone() };
        repository
            .enqueue(JobSeed::new(
                job_id,
                "standalone",
                json!({"value": "first"}),
                1_700_000_000_000,
            ))
            .await
            .unwrap();

        let deleted = sqlx::query("DELETE FROM job_queue_projection WHERE view_id = $1")
            .bind(job_id.to_string())
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(deleted.rows_affected(), 1);

        let pending = json!({"Pending": {
            "kind": "standalone", "payload": {"value": "first"},
            "run_at": 1_700_000_000_000_i64, "attempt": 0, "claims": 0
        }});
        for _ in 0..2 {
            assert_eq!(
                recover_job_queue_row(&backend, &job_id).await.unwrap(),
                Some(pending.clone())
            );
        }

        let (checkpoint, lease): (i64, Option<i64>) = sqlx::query_as(
            "SELECT version, lease_until FROM job_queue_projection WHERE view_id = $1",
        )
        .bind(job_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!((checkpoint, lease), (1, None));
    }

    #[sqlx::test(migrations = false)]
    async fn competing_claims_commit_one_event_and_lease(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
        let job_id = JobId::new();
        let job_key = job_id.to_string();
        let run_at = 1_700_000_000_000_i64;
        let lease_until_ms = run_at + 10_000;
        PostgresEventRepository { pool: pool.clone() }
            .enqueue(JobSeed::new(job_id, "standalone", json!({}), run_at))
            .await
            .unwrap();

        let options = pool.connect_options();
        let (first_pool, second_pool) = tokio::try_join!(
            PgPoolOptions::new()
                .max_connections(1)
                .connect_with((*options).clone()),
            PgPoolOptions::new()
                .max_connections(1)
                .connect_with((*options).clone()),
        )
        .unwrap();
        let first = PostgresEventRepository { pool: first_pool };
        let second = PostgresEventRepository { pool: second_pool };
        let decide = move |job_key: &str, read: Option<event_sorcery::ClaimRead>| {
            let read = read.unwrap();
            if read.lease_until_ms.is_some() {
                return event_sorcery::ClaimDecision::Skip;
            }
            assert_eq!(read.version, 1);
            let worker = "worker:01HF7YAT000000000000000001";
            let claim_id = "01HF7YAT000000000000000002";
            event_sorcery::ClaimDecision::Claim {
                event: SerializedEvent {
                    aggregate_id: job_key.to_owned(),
                    sequence: 2,
                    aggregate_type: "job".to_owned(),
                    event_type: "JobClaimed".to_owned(),
                    event_version: "1.0".to_owned(),
                    payload: json!({"Claimed": {"worker": worker, "claim_id": claim_id,
                        "lease_until": lease_until_ms}}),
                    metadata: json!({}),
                },
                payload: json!({"Live": {"Claimed": {"kind": "standalone", "payload": {},
                    "worker": worker, "claim_id": claim_id, "run_at": run_at,
                    "attempt": 0, "claims": 1}}})
                .to_string(),
                lease_until_ms,
                won: lease_until_ms,
            }
        };
        sqlx::raw_sql(
            "CREATE FUNCTION reject_claim_projection() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 IF EXISTS (SELECT 1 FROM events WHERE aggregate_type = 'job'
                            AND aggregate_id = NEW.view_id AND sequence = NEW.version
                            AND event_type = 'JobClaimed') THEN
                     RAISE EXCEPTION 'injected projection failure after claim append';
                 END IF;
                 RETURN NEW;
             END;
             $$;
             CREATE TRIGGER reject_claim_projection BEFORE UPDATE ON job_queue_projection
             FOR EACH ROW EXECUTE FUNCTION reject_claim_projection();",
        )
        .execute(&pool)
        .await
        .unwrap();
        let Err(super::PostgresClaimError::Sql(sqlx::Error::Database(error))) = first
            .claim_job(&job_key, |read| decide(&job_key, read))
            .await
        else {
            panic!("expected the injected projection write failure");
        };
        assert_eq!(
            error.message(),
            "injected projection failure after claim append"
        );

        let unchanged: (i64, Option<i64>, i64) = sqlx::query_as(
            "SELECT version, lease_until,
                    (SELECT count(*) FROM events WHERE aggregate_type = 'job' AND aggregate_id = $1)
             FROM job_queue_projection WHERE view_id = $1",
        )
        .bind(&job_key)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(unchanged, (1, None, 1));
        sqlx::query("DROP TRIGGER reject_claim_projection ON job_queue_projection")
            .execute(&pool)
            .await
            .unwrap();

        verify_claim_cancellation(&pool, &first.pool, &job_key, decide).await;
        verify_competing_claims(&pool, &first, &second, &job_key, lease_until_ms, decide).await;
        verify_stale_projection_claim(&first, run_at, decide).await;
    }

    async fn verify_stale_projection_claim(
        repository: &PostgresEventRepository,
        run_at: i64,
        decide: impl Fn(&str, Option<event_sorcery::ClaimRead>) -> event_sorcery::ClaimDecision<i64>
        + Sync,
    ) {
        use event_sorcery::ClaimDecision::Claim;

        let job_id = JobId::new();
        let job_key = job_id.to_string();
        repository
            .enqueue(JobSeed::new(job_id, "standalone", json!({}), run_at))
            .await
            .unwrap();

        let wrong_key = JobId::new().to_string();
        let wrong_stream = repository
            .claim_job(&job_key, |read| decide(&wrong_key, read))
            .await;
        assert!(matches!(
            wrong_stream,
            Err(super::PostgresClaimError::InvalidEvent)
        ));
        let skipped_sequence = repository
            .claim_job(&job_key, |read| {
                let Claim {
                    mut event,
                    payload,
                    lease_until_ms,
                    won,
                } = decide(&job_key, read)
                else {
                    panic!("expected pending job to be claimable")
                };
                event.sequence = 3;
                Claim {
                    event,
                    payload,
                    lease_until_ms,
                    won,
                }
            })
            .await;
        assert!(matches!(
            skipped_sequence,
            Err(super::PostgresClaimError::InvalidEvent)
        ));

        // The authoritative stream can be ahead of the queue until replay catches up.
        let mut transaction = repository.pool.begin().await.unwrap();
        let Claim { event, .. } = decide(
            &job_key,
            Some(event_sorcery::ClaimRead {
                version: 1,
                payload: sqlx::query_scalar(
                    "SELECT payload::text FROM job_queue_projection WHERE view_id = $1",
                )
                .bind(&job_key)
                .fetch_one(&mut *transaction)
                .await
                .unwrap(),
                lease_until_ms: None,
            }),
        ) else {
            panic!("expected a claim fixture")
        };
        repository
            .official()
            .append_serialized_events_in(&mut transaction, "job", &[event])
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let result = repository
            .claim_job(&job_key, |read| decide(&job_key, read))
            .await
            .unwrap();
        assert!(matches!(result, event_sorcery::ClaimOutcome::Contended));
        let row: (i64, Option<i64>) = sqlx::query_as(
            "SELECT version, lease_until FROM job_queue_projection WHERE view_id = $1",
        )
        .bind(&job_key)
        .fetch_one(&repository.pool)
        .await
        .unwrap();
        assert_eq!(row, (1, None));
        let sequences: Vec<i64> = sqlx::query_scalar(
            "SELECT sequence FROM events WHERE aggregate_type = 'job' AND aggregate_id = $1
             ORDER BY sequence",
        )
        .bind(&job_key)
        .fetch_all(&repository.pool)
        .await
        .unwrap();
        assert_eq!(sequences, [1, 2]);
        let stray_events: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM events WHERE aggregate_type = 'job' AND aggregate_id = $1",
        )
        .bind(wrong_key)
        .fetch_one(&repository.pool)
        .await
        .unwrap();
        assert_eq!(stray_events, 0);
    }

    async fn verify_claim_cancellation(
        pool: &PgPool,
        writer_pool: &PgPool,
        job_key: &str,
        decide: impl FnOnce(&str, Option<event_sorcery::ClaimRead>) -> event_sorcery::ClaimDecision<i64>
        + Send
        + 'static,
    ) {
        sqlx::raw_sql(
            "CREATE FUNCTION block_claim_projection() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 IF NOT EXISTS (SELECT 1 FROM events WHERE aggregate_type = 'job'
                                AND aggregate_id = NEW.view_id AND sequence = NEW.version
                                AND event_type = 'JobClaimed') THEN
                     RAISE EXCEPTION 'claim must append before updating its projection';
                 END IF;
                 PERFORM pg_advisory_xact_lock(782341, 10);
                 RETURN NEW;
             END;
             $$;
             CREATE TRIGGER block_claim_projection BEFORE UPDATE ON job_queue_projection
             FOR EACH ROW EXECUTE FUNCTION block_claim_projection();",
        )
        .execute(pool)
        .await
        .unwrap();
        let writer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(writer_pool)
            .await
            .unwrap();
        let mut blocker = pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(782341, 10)")
            .execute(&mut *blocker)
            .await
            .unwrap();
        let writer = PostgresEventRepository {
            pool: writer_pool.clone(),
        };
        let writer_job_key = job_key.to_owned();
        let pending = tokio::spawn(async move {
            writer
                .claim_job(&writer_job_key, |read| decide(&writer_job_key, read))
                .await
        });
        timeout(Duration::from_secs(5), async {
            loop {
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_locks
                     WHERE pid = $1 AND locktype = 'advisory' AND NOT granted)",
                )
                .bind(writer_pid)
                .fetch_one(pool)
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
        assert!(matches!(pending.await, Err(error) if error.is_cancelled()));
        blocker.rollback().await.unwrap();

        let unchanged: (i64, Option<i64>, i64) = timeout(Duration::from_secs(5),
            sqlx::query_as(
                "SELECT version, lease_until,
                        (SELECT count(*) FROM events WHERE aggregate_type = 'job' AND aggregate_id = $1)
                 FROM job_queue_projection WHERE view_id = $1",
            )
            .bind(job_key)
            .fetch_one(writer_pool),
        ).await.unwrap().unwrap();
        assert_eq!(unchanged, (1, None, 1));
        sqlx::query("DROP TRIGGER block_claim_projection ON job_queue_projection")
            .execute(pool)
            .await
            .unwrap();
    }

    async fn verify_competing_claims(
        pool: &PgPool,
        first: &PostgresEventRepository,
        second: &PostgresEventRepository,
        job_key: &str,
        lease_until_ms: i64,
        decide: impl Fn(&str, Option<event_sorcery::ClaimRead>) -> event_sorcery::ClaimDecision<i64>
        + Sync,
    ) {
        let (left, right) = timeout(Duration::from_secs(5), async {
            tokio::try_join!(
                first.claim_job(job_key, |read| decide(job_key, read)),
                second.claim_job(job_key, |read| decide(job_key, read))
            )
        })
        .await
        .unwrap()
        .unwrap();
        use event_sorcery::ClaimOutcome::{Skip, Won};
        let winner = match (left, right) {
            (Won(deadline), Skip) | (Skip, Won(deadline)) => Some(deadline),
            _ => None,
        };
        assert_eq!(winner, Some(lease_until_ms));
        let renewed_deadline = lease_until_ms + 10_000;
        use event_sorcery::LeaseRenewal::{Held, Lost};
        assert!(matches!(
            first.renew_job(job_key, 2, renewed_deadline).await.unwrap(),
            Held
        ));
        assert!(matches!(
            second.renew_job(job_key, 1, lease_until_ms).await.unwrap(),
            Lost
        ));
        let row: (i64, String, i64) = sqlx::query_as(
            "SELECT version, status, lease_until FROM job_queue_projection WHERE view_id = $1",
        )
        .bind(job_key)
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(row, (2, "claimed".to_owned(), renewed_deadline));
        let claims: Vec<(i64, i64)> = sqlx::query_as(
            "SELECT sequence, (payload #>> '{Claimed,lease_until}')::BIGINT FROM events
             WHERE aggregate_type = 'job' AND aggregate_id = $1 AND event_type = 'JobClaimed'",
        )
        .bind(job_key)
        .fetch_all(pool)
        .await
        .unwrap();
        assert_eq!(claims, vec![(2, lease_until_ms)]);
    }

    #[sqlx::test(migrations = false)]
    async fn expired_claims_are_reclaimed_then_abandoned_at_the_claim_budget(pool: PgPool) {
        use event_sorcery::ClaimOutcome::{Abandoned, Skip, Won};
        use event_sorcery::LeaseRenewal::{Held, Lost};

        MIGRATOR.run(&pool).await.unwrap();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let job_id = JobId::new();
        let job_key = job_id.to_string();
        let run_at = 1_700_000_000_000;
        let lease_ms = 1_000;
        let max_claims = 2;
        repository
            .enqueue(JobSeed::new(job_id, "standalone", json!({}), run_at))
            .await
            .unwrap();

        let claim_at = |now_ms| {
            let job_key = job_key.as_str();
            repository.claim_job(job_key, move |read| {
                plan_test_claim(job_key, read, now_ms, lease_ms, max_claims)
            })
        };
        assert!(matches!(claim_at(run_at).await.unwrap(), Won(2)));
        let first_deadline = run_at + 2 * lease_ms;
        assert!(matches!(
            repository
                .renew_job(&job_key, 2, first_deadline)
                .await
                .unwrap(),
            Held
        ));
        // The original event deadline has elapsed, but the renewed lease has not.
        assert!(matches!(claim_at(first_deadline).await.unwrap(), Skip));
        let reclaim_at = first_deadline + 1;
        assert!(matches!(claim_at(reclaim_at).await.unwrap(), Won(3)));
        let second_deadline = reclaim_at + lease_ms;
        assert!(matches!(
            repository
                .renew_job(&job_key, 2, second_deadline + 1)
                .await
                .unwrap(),
            Lost
        ));
        assert!(matches!(claim_at(second_deadline).await.unwrap(), Skip));
        assert!(matches!(
            claim_at(second_deadline + 1).await.unwrap(),
            Abandoned
        ));

        for sequence in [3, 4] {
            assert!(matches!(
                repository
                    .renew_job(&job_key, sequence, second_deadline + 2)
                    .await
                    .unwrap(),
                Lost
            ));
        }
        let terminal: (i64, String, Option<i64>, Value) = sqlx::query_as(
            "SELECT version, status, lease_until, payload FROM job_queue_projection
             WHERE view_id = $1",
        )
        .bind(&job_key)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            terminal,
            (
                4,
                "dead".to_owned(),
                None,
                json!({"Live": {"Dead": {"kind": "standalone", "reason": "Abandoned"}}})
            )
        );
        let events: Vec<(i64, String)> = sqlx::query_as(
            "SELECT sequence, event_type FROM events
             WHERE aggregate_type = 'job' AND aggregate_id = $1 ORDER BY sequence",
        )
        .bind(&job_key)
        .fetch_all(&pool)
        .await
        .unwrap();
        let observed: Vec<_> = events
            .iter()
            .map(|(seq, name)| (*seq, name.as_str()))
            .collect();
        assert_eq!(
            observed,
            [
                (1, "JobEnqueued"),
                (2, "JobClaimed"),
                (3, "JobClaimed"),
                (4, "JobDead")
            ]
        );
    }

    #[sqlx::test(migrations = false)]
    async fn queue_replays_a_claim_without_recreating_its_lease(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
        let job_id = JobId::new();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let backend = PostgresBackend { pool: pool.clone() };
        let run_at = 1_700_000_000_000_i64;
        let audit_deadline = run_at + 10_000;
        let renewed_deadline = run_at + 20_000;
        let payload = json!({"value": "first"});
        repository
            .enqueue(JobSeed::new(job_id, "standalone", payload.clone(), run_at))
            .await
            .unwrap();
        assert_eq!(
            recover_job_queue_row(&backend, &job_id).await.unwrap(),
            Some(json!({"Pending": {
                "kind": "standalone", "payload": payload,
                "run_at": run_at, "attempt": 0, "claims": 0
            }}))
        );

        let claim = SerializedEvent {
            aggregate_id: job_id.to_string(),
            sequence: 2,
            aggregate_type: "job".to_string(),
            event_type: "JobClaimed".to_string(),
            event_version: "1.0".to_string(),
            payload: json!({"Claimed": {
                "worker": "worker:01HF7YAT000000000000000001",
                "claim_id": "01HF7YAT000000000000000002",
                "lease_until": audit_deadline
            }}),
            metadata: json!({}),
        };
        let mut transaction = pool.begin().await.unwrap();
        repository
            .official()
            .append_serialized_events_in(&mut transaction, "job", std::slice::from_ref(&claim))
            .await
            .unwrap();
        let updated = sqlx::query(
            "UPDATE job_queue_projection SET lease_until = $2 WHERE view_id = $1 AND version = 1",
        )
        .bind(job_id.to_string())
        .bind(renewed_deadline)
        .execute(&mut *transaction)
        .await
        .unwrap();
        assert_eq!(updated.rows_affected(), 1);
        transaction.commit().await.unwrap();

        let claimed = json!({"Claimed": {
            "kind": "standalone", "payload": payload, "run_at": run_at,
            "worker": "worker:01HF7YAT000000000000000001",
            "claim_id": "01HF7YAT000000000000000002", "attempt": 0, "claims": 1
        }});
        for lease in [Some(renewed_deadline), Some(renewed_deadline), None] {
            if lease.is_none() {
                let deleted = sqlx::query("DELETE FROM job_queue_projection WHERE view_id = $1")
                    .bind(job_id.to_string())
                    .execute(&pool)
                    .await
                    .unwrap();
                assert_eq!(deleted.rows_affected(), 1);
            }
            assert_eq!(
                recover_job_queue_row(&backend, &job_id).await.unwrap(),
                Some(claimed.clone())
            );
            let row: (i64, Option<i64>) = sqlx::query_as(
                "SELECT version, lease_until FROM job_queue_projection WHERE view_id = $1",
            )
            .bind(job_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(row, (2, lease));
        }

        // Each decoy differs from the match in exactly one queried column.
        for (schedule, is_claimed, lease) in [
            (run_at, false, None),
            (run_at + 1, true, None),
            (run_at, true, Some(renewed_deadline)),
        ] {
            let decoy = JobId::new();
            repository
                .enqueue(JobSeed::new(decoy, "standalone", payload.clone(), schedule))
                .await
                .unwrap();
            if is_claimed {
                let mut transaction = pool.begin().await.unwrap();
                let decoy_claim = SerializedEvent {
                    aggregate_id: decoy.to_string(),
                    ..claim.clone()
                };
                repository
                    .official()
                    .append_serialized_events_in(&mut transaction, "job", &[decoy_claim])
                    .await
                    .unwrap();
                transaction.commit().await.unwrap();
                recover_job_queue_row(&backend, &decoy)
                    .await
                    .unwrap()
                    .unwrap();
            }
            if let Some(deadline) = lease {
                let updated = sqlx::query(
                    "UPDATE job_queue_projection SET lease_until = $2 WHERE view_id = $1",
                )
                .bind(decoy.to_string())
                .bind(deadline)
                .execute(&pool)
                .await
                .unwrap();
                assert_eq!(updated.rows_affected(), 1);
            }
        }

        let predicate = Predicate {
            any_of: vec![vec![
                Term {
                    column: "status".to_string(),
                    cmp: Cmp::Eq,
                    value: Some(IndexedValue::Text("claimed".to_string())),
                },
                Term {
                    column: "run_at".to_string(),
                    cmp: Cmp::Eq,
                    value: Some(IndexedValue::Int(run_at)),
                },
                Term {
                    column: "lease_until".to_string(),
                    cmp: Cmp::IsNull,
                    value: None,
                },
            ]],
        };
        assert_eq!(
            find_recovered_jobs(&backend, &predicate).await.unwrap(),
            [job_id.to_string()]
        );
    }

    #[sqlx::test(migrations = false)]
    async fn queue_reconstructs_retried_deferred_and_terminal_jobs(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let backend = PostgresBackend { pool: pool.clone() };
        let run_at = 1_700_000_000_000_i64;
        let retry_at = run_at + 30_000;
        let defer_at = retry_at + 60_000;
        let retry = json!({"RetryScheduled": {
            "run_at": retry_at, "attempt": 1, "error": "temporary"
        }});
        let mut jobs = Vec::new();
        for (outcomes, expected) in [
            (
                vec![("JobRetryScheduled", retry.clone())],
                json!({"Pending": {
                    "kind": "standalone", "payload": {"value": "original"},
                    "run_at": retry_at, "attempt": 1, "claims": 1
                }}),
            ),
            (
                vec![
                    ("JobRetryScheduled", retry),
                    (
                        "JobRescheduled",
                        json!({"Rescheduled": {"run_at": defer_at}}),
                    ),
                ],
                json!({"Pending": {
                    "kind": "standalone", "payload": {"value": "original"},
                    "run_at": defer_at, "attempt": 1, "claims": 0
                }}),
            ),
            (
                vec![("JobSucceeded", json!("Succeeded"))],
                json!({"Done": {"kind": "standalone"}}),
            ),
            (
                vec![(
                    "JobDead",
                    json!({"Dead": {"reason": "Rejected", "error": "terminal"}}),
                )],
                json!({"Dead": {"kind": "standalone", "reason": "Rejected"}}),
            ),
        ] {
            let job_id = JobId::new();
            repository
                .enqueue(JobSeed::new(
                    job_id,
                    "standalone",
                    json!({"value": "original"}),
                    run_at,
                ))
                .await
                .unwrap();
            let mut events = Vec::new();
            for (index, (event_type, payload)) in outcomes.into_iter().enumerate() {
                let sequence = 2 + 2 * index;
                let claim = json!({"Claimed": {
                    "worker": "worker:01HF7YAT000000000000000001",
                    "claim_id": format!("01HF7YAT00000000000000000{}", index + 2),
                    "lease_until": defer_at + 10_000
                }});
                for (sequence, event_type, payload) in [
                    (sequence, "JobClaimed", claim),
                    (sequence + 1, event_type, payload),
                ] {
                    events.push(SerializedEvent {
                        aggregate_id: job_id.to_string(),
                        sequence,
                        aggregate_type: "job".to_string(),
                        event_type: event_type.to_string(),
                        event_version: "1.0".to_string(),
                        payload,
                        metadata: json!({}),
                    });
                }
            }
            let checkpoint = i64::try_from(events.len()).unwrap() + 1;
            let mut transaction = pool.begin().await.unwrap();
            repository
                .official()
                .append_serialized_events_in(&mut transaction, "job", &events)
                .await
                .unwrap();
            transaction.commit().await.unwrap();
            jobs.push((job_id, checkpoint, expected));
        }

        let deleted = sqlx::query("DELETE FROM job_queue_projection")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(deleted.rows_affected(), 4);
        for (job_id, checkpoint, expected) in jobs {
            assert_eq!(
                recover_job_queue_row(&backend, &job_id).await.unwrap(),
                Some(expected)
            );
            let row: (i64, Option<i64>) = sqlx::query_as(
                "SELECT version, lease_until FROM job_queue_projection WHERE view_id = $1",
            )
            .bind(job_id.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(row, (checkpoint, None));
        }
    }

    #[sqlx::test(migrations = false)]
    async fn queue_recovery_rolls_back_when_schema_registration_fails(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let backend = PostgresBackend { pool: pool.clone() };
        let job_id = JobId::new();
        let run_at = 1_700_000_000_000_i64;
        repository
            .enqueue(JobSeed::new(
                job_id,
                "standalone",
                json!({"value": 1}),
                run_at,
            ))
            .await
            .unwrap();

        sqlx::query("DELETE FROM job_queue_projection WHERE view_id = $1")
            .bind(job_id.to_string())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql(
            "CREATE FUNCTION reject_queue_registration() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 IF EXISTS (SELECT 1 FROM job_queue_projection WHERE version = 1
                            AND status = 'pending' AND kind = 'standalone') THEN
                     RAISE EXCEPTION 'injected registration failure after queue replay';
                 END IF;
                 RAISE EXCEPTION 'registration reached before queue replay';
             END;
             $$;
             CREATE TRIGGER reject_queue_registration BEFORE INSERT ON events
             FOR EACH ROW WHEN (NEW.aggregate_type = 'SchemaRegistry')
             EXECUTE FUNCTION reject_queue_registration();",
        )
        .execute(&pool)
        .await
        .unwrap();

        let error = recover_job_queue_row(&backend, &job_id).await.unwrap_err();
        assert!(matches!(
            error,
            event_sorcery::RecoveredJobQueryError::Reconcile(
                event_sorcery::ReconcileError::Aggregate(_)
            )
        ));
        assert!(
            error
                .to_string()
                .contains("injected registration failure after queue replay")
        );
        let counts: (i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM job_queue_projection),
                    (SELECT COUNT(*) FROM events WHERE aggregate_type = 'SchemaRegistry'),
                    (SELECT COUNT(*) FROM events WHERE aggregate_type = 'job' AND aggregate_id = $1)",
        )
        .bind(job_id.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(counts, (0, 0, 1));

        sqlx::query("DROP TRIGGER reject_queue_registration ON events")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            recover_job_queue_row(&backend, &job_id).await.unwrap(),
            Some(json!({"Pending": {
                "kind": "standalone", "payload": {"value": 1},
                "run_at": run_at, "attempt": 0, "claims": 0
            }}))
        );
    }

    #[sqlx::test(migrations = false)]
    async fn cancelling_queue_recovery_before_registration_rolls_back_replay(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
        let job_id = JobId::new();
        PostgresEventRepository { pool: pool.clone() }
            .enqueue(JobSeed::new(
                job_id,
                "standalone",
                json!({"value": 1}),
                1_700_000_000_000,
            ))
            .await
            .unwrap();
        let deleted = sqlx::query("DELETE FROM job_queue_projection WHERE view_id = $1")
            .bind(job_id.to_string())
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(deleted.rows_affected(), 1);

        // Registration follows replay. Block its insert without locking out replay reads.
        sqlx::raw_sql(
            "CREATE FUNCTION wait_for_queue_registration() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 IF NOT EXISTS (SELECT 1 FROM job_queue_projection WHERE version = 1
                                AND status = 'pending' AND kind = 'standalone') THEN
                     RAISE EXCEPTION 'registration reached before queue replay';
                 END IF;
                 PERFORM pg_advisory_xact_lock(782341, 9);
                 RETURN NEW;
             END;
             $$;
             CREATE TRIGGER wait_for_queue_registration BEFORE INSERT ON events
             FOR EACH ROW WHEN (NEW.aggregate_type = 'SchemaRegistry')
             EXECUTE FUNCTION wait_for_queue_registration();",
        )
        .execute(&pool)
        .await
        .unwrap();
        let singleton = PgPoolOptions::new()
            .max_connections(1)
            .connect_with((*pool.connect_options()).clone())
            .await
            .unwrap();
        let recovery_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&singleton)
            .await
            .unwrap();
        let mut blocker = pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(782341, 9)")
            .execute(&mut *blocker)
            .await
            .unwrap();
        let backend = PostgresBackend { pool: singleton };
        let recovering = backend.pool.clone();
        let recovery = tokio::spawn(async move {
            recover_job_queue_row(&PostgresBackend { pool: recovering }, &job_id).await
        });
        timeout(Duration::from_secs(5), async {
            loop {
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_locks
                     WHERE pid = $1 AND locktype = 'advisory' AND NOT granted
                       AND classid = 782341 AND objid = 9 AND objsubid = 2)",
                )
                .bind(recovery_pid)
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
        .expect("recovery must reach registration after writing the queue row");
        recovery.abort();
        assert!(recovery.await.unwrap_err().is_cancelled());
        blocker.rollback().await.unwrap();

        let counts: (i64, i64, i64) = timeout(
            Duration::from_secs(5),
            sqlx::query_as(
                "SELECT (SELECT COUNT(*) FROM job_queue_projection),
                        (SELECT COUNT(*) FROM events WHERE aggregate_type = 'SchemaRegistry'),
                        (SELECT COUNT(*) FROM events WHERE aggregate_type = 'job' AND aggregate_id = $1)",
            )
            .bind(job_id.to_string())
            .fetch_one(&backend.pool),
        )
        .await
        .expect("cancellation must release the recovery connection")
        .unwrap();
        assert_eq!(counts, (0, 0, 1));
        assert_eq!(
            recover_job_queue_row(&backend, &job_id).await.unwrap(),
            Some(json!({"Pending": {
                "kind": "standalone", "payload": {"value": 1},
                "run_at": 1_700_000_000_000_i64, "attempt": 0, "claims": 0
            }}))
        );
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
    async fn snapshot_constraint_failure_keeps_database_identity_and_rolls_back(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
        sqlx::query("CREATE UNIQUE INDEX unique_snapshot_payload ON snapshots (payload)")
            .execute(&pool)
            .await
            .unwrap();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(repository, 1);
        let framework = CqrsFramework::new(store, vec![], None);
        framework.execute("first", "same".to_owned()).await.unwrap();

        let error = framework
            .execute("second", "same".to_owned())
            .await
            .unwrap_err();
        let AggregateError::DatabaseConnectionError(source) = error else {
            panic!("expected the database constraint error: {error:?}");
        };
        let database = source
            .downcast_ref::<sqlx::Error>()
            .unwrap()
            .as_database_error()
            .unwrap();
        assert_eq!(database.code().as_deref(), Some("23505"));
        assert_eq!(database.constraint(), Some("unique_snapshot_payload"));
        let repository = PostgresEventRepository { pool };
        assert_eq!(
            repository
                .get_events::<RecordedValues>("second")
                .await
                .unwrap(),
            []
        );
        assert_eq!(
            repository
                .get_snapshot::<RecordedValues>("second")
                .await
                .unwrap(),
            None
        );
    }

    #[sqlx::test(migrations = false)]
    async fn zero_snapshot_counter_preserves_reload_and_the_next_snapshot(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let store = PersistedEventStore::<_, RecordedValues>::new_snapshot_store(repository, 2);
        let framework = CqrsFramework::new(store, vec![], None);
        for value in ["first", "second", "third"] {
            framework.execute("stream", value.to_owned()).await.unwrap();
        }
        sqlx::query(
            "UPDATE snapshots SET snapshot_version = DEFAULT WHERE aggregate_id = 'stream'",
        )
        .execute(&pool)
        .await
        .unwrap();
        let repository = PostgresEventRepository { pool };
        let snapshot = repository
            .get_snapshot::<RecordedValues>("stream")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (snapshot.current_sequence, snapshot.current_snapshot),
            (2, 0)
        );
        assert_eq!(snapshot.aggregate, json!({"values": ["first", "second"]}));

        framework
            .execute("stream", "fourth".to_owned())
            .await
            .unwrap();
        let updated = repository
            .get_snapshot::<RecordedValues>("stream")
            .await
            .unwrap()
            .unwrap();
        assert_eq!((updated.current_sequence, updated.current_snapshot), (4, 1));
        assert_eq!(
            updated.aggregate,
            json!({"values": ["first", "second", "third", "fourth"]})
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
