//! PostgreSQL persistence through the shared CQRS repository contracts.

/// Pool-backed PostgreSQL repository for the CQRS persistence contract.
pub struct PostgresEventRepository {
    /// Application-provisioned PostgreSQL connection pool.
    pub pool: sqlx::PgPool,
}

/// Framework-owned PostgreSQL schema migrations.
///
/// Applications apply these migrations to their PostgreSQL pool before using
/// framework persistence. Existing SQLite migrations remain in `sqlite-es`.
pub static POSTGRES_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/postgres");

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use cqrs_es::event_sink::EventSink;
    use cqrs_es::persist::{
        PersistedEventRepository, PersistedEventStore, PersistenceError, ReplayStream,
        SerializedEvent, SerializedSnapshot,
    };
    use cqrs_es::{Aggregate, CqrsFramework, DomainEvent, EventStore};
    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};
    use sqlx::PgPool;
    use std::convert::Infallible;

    use super::{POSTGRES_MIGRATOR, PostgresEventRepository};
    use crate::job::{ClaimId, DeadReason, JobEvent, JobId, JobKind, JobState, WorkerId};
    use crate::{EventSourced, Lifecycle};

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
        type Services = ();

        async fn handle(
            &mut self,
            value: String,
            _services: &(),
            sink: &EventSink<Self>,
        ) -> Result<(), Infallible> {
            let mut scratch = self.clone();
            sink.write(ValueEvent::Recorded { value }, &mut scratch)
                .await;
            Ok(())
        }

        fn apply(&mut self, event: ValueEvent) {
            let ValueEvent::Recorded { value } = event;
            self.values.push(value);
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("PostgreSQL repository contract is awaiting its implementation")]
    struct RepositoryContractPending;

    impl PersistedEventRepository for PostgresEventRepository {
        async fn get_events<A: Aggregate>(
            &self,
            _aggregate_id: &str,
        ) -> Result<Vec<SerializedEvent>, PersistenceError> {
            Err(PersistenceError::UnknownError(Box::new(
                RepositoryContractPending,
            )))
        }

        async fn get_last_events<A: Aggregate>(
            &self,
            _aggregate_id: &str,
            _last_sequence: usize,
        ) -> Result<Vec<SerializedEvent>, PersistenceError> {
            Err(PersistenceError::UnknownError(Box::new(
                RepositoryContractPending,
            )))
        }

        async fn get_snapshot<A: Aggregate>(
            &self,
            _aggregate_id: &str,
        ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
            Err(PersistenceError::UnknownError(Box::new(
                RepositoryContractPending,
            )))
        }

        async fn persist<A: Aggregate>(
            &self,
            _events: &[SerializedEvent],
            _snapshot_update: Option<(String, Value, usize)>,
        ) -> Result<(), PersistenceError> {
            Err(PersistenceError::UnknownError(Box::new(
                RepositoryContractPending,
            )))
        }

        async fn stream_events<A: Aggregate>(
            &self,
            _aggregate_id: &str,
        ) -> Result<ReplayStream, PersistenceError> {
            Err(PersistenceError::UnknownError(Box::new(
                RepositoryContractPending,
            )))
        }

        async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
            Err(PersistenceError::UnknownError(Box::new(
                RepositoryContractPending,
            )))
        }
    }

    #[sqlx::test(migrations = false)]
    async fn repository_reloads_commands_through_the_generic_cqrs_store(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();
        let repository = PostgresEventRepository { pool: pool.clone() };
        let store = PersistedEventStore::<_, RecordedValues>::new_event_store(repository);
        let framework = CqrsFramework::new(store, vec![], ());
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
        let framework = CqrsFramework::new(store, vec![], ());
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
                INSERT INTO job_queue (view_id, version, payload, lease_until)
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
                    FROM job_queue
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
                "INSERT INTO job_queue (view_id, version, payload) VALUES ($1, $2, $3)",
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

        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_queue")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(rows, 0);
    }
}
