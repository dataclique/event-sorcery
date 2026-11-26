//! Atomic SQLite writes for native commands and opaque binding commits.

use cqrs_es::persist::{SerializedEvent, SerializedSnapshot};
use sqlx::SqlitePool;

use crate::{
    SqliteAggregateError, committed_stream_version, insert_serialized_events_batch, load_snapshot,
    write_snapshot,
};

/// Validated event batch with domain inputs that the caller prepares at write time.
pub(crate) struct Commit<'events, Snapshot, Job> {
    /// Aggregate type of the stream being written.
    pub(crate) aggregate_type: &'events str,
    /// ID of the stream being written.
    pub(crate) aggregate_id: &'events str,
    /// Required durable stream version, if the caller supplied one.
    pub(crate) expected_version: Option<usize>,
    /// Validated contiguous events for the stream.
    pub(crate) events: &'events [SerializedEvent],
    /// Optional snapshot input, with coverage resolved after the event append.
    pub(crate) snapshot: Option<Snapshot>,
    /// Job intents whose events and queue rows are part of the same transaction.
    pub(crate) jobs: Vec<Job>,
}

/// Persists an event batch, its snapshot, and its jobs under one SQLite write lock.
///
/// Preparation does not borrow the connection. It runs at the corresponding write
/// step so stale writers and database failures keep their existing precedence over
/// later validation failures. Dropping the transaction rolls back every write.
pub(crate) async fn commit_atomic<Snapshot, Job, Error>(
    pool: &SqlitePool,
    commit: Commit<'_, Snapshot, Job>,
    prepare_snapshot: impl FnOnce(Snapshot) -> Result<SerializedSnapshot, Error> + Send,
    prepare_event: impl Fn(&Job) -> Result<SerializedEvent, Error> + Send,
    prepare_payload: impl Fn(Job) -> Result<String, Error> + Send,
) -> Result<(), Error>
where
    Snapshot: Send,
    Job: Send,
    Error: From<SqliteAggregateError> + From<sqlx::Error>,
{
    let Commit {
        aggregate_type,
        aggregate_id,
        expected_version,
        events,
        snapshot,
        jobs,
    } = commit;
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    if let Some(expected) = expected_version {
        let actual =
            committed_stream_version(&mut *transaction, aggregate_type, aggregate_id).await?;
        if actual != expected {
            return Err(SqliteAggregateError::OptimisticLock.into());
        }
    }

    insert_serialized_events_batch(&mut transaction, "events", events).await?;

    if let Some(snapshot) = snapshot {
        let snapshot = prepare_snapshot(snapshot)?;
        if load_snapshot(&mut *transaction, aggregate_type, aggregate_id)
            .await?
            .is_some_and(|stored| stored.current_sequence >= snapshot.current_sequence)
        {
            return Err(SqliteAggregateError::OptimisticLock.into());
        }
        write_snapshot(
            &mut transaction,
            aggregate_type,
            aggregate_id,
            snapshot.current_sequence,
            snapshot.current_snapshot,
            &snapshot.aggregate,
        )
        .await?;
    }

    for job in jobs {
        let event = prepare_event(&job)?;
        insert_serialized_events_batch(&mut transaction, "events", std::slice::from_ref(&event))
            .await?;

        let payload = prepare_payload(job)?;
        sqlx::query(
            "INSERT INTO job_queue_projection (view_id, version, payload, lease_until)
             VALUES (?1, 1, ?2, NULL)",
        )
        .bind(&event.aggregate_id)
        .bind(payload)
        .execute(&mut *transaction)
        .await?;
    }

    transaction.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    //! Durable invariants at the atomic SQLite write boundary.

    use serde_json::json;

    use super::*;
    use crate::testing::create_test_pool;

    fn event(aggregate_type: &str, aggregate_id: &str, sequence: usize) -> SerializedEvent {
        SerializedEvent {
            aggregate_type: aggregate_type.to_owned(),
            aggregate_id: aggregate_id.to_owned(),
            sequence,
            event_type: "Recorded".to_owned(),
            event_version: "1".to_owned(),
            payload: json!({"sequence": sequence}),
            metadata: json!({}),
        }
    }

    async fn write_with_job(pool: &SqlitePool) -> Result<(), SqliteAggregateError> {
        commit_atomic(
            pool,
            Commit {
                aggregate_type: "account",
                aggregate_id: "one",
                expected_version: Some(0),
                events: &[event("account", "one", 1), event("account", "one", 2)],
                snapshot: Some(SerializedSnapshot {
                    aggregate_id: "one".to_owned(),
                    aggregate: json!({"balance": 42}),
                    current_sequence: 2,
                    current_snapshot: 1,
                }),
                jobs: vec![event("job", "receipt", 1)],
            },
            Ok,
            |job| Ok(job.clone()),
            |_| Ok(r#"{"Live":{"Pending":{"kind":"receipt","run_at":0}}}"#.to_owned()),
        )
        .await
    }

    #[tokio::test]
    async fn a_commit_persists_events_snapshot_and_job_together() {
        let pool = create_test_pool().await.unwrap();
        write_with_job(&pool).await.unwrap();

        let events: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT aggregate_type, aggregate_id, sequence FROM events ORDER BY 1, 2, 3",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            events,
            [
                ("account".into(), "one".into(), 1),
                ("account".into(), "one".into(), 2),
                ("job".into(), "receipt".into(), 1)
            ]
        );
        let snapshot: (i64, i64, String) = sqlx::query_as(
            "SELECT last_sequence, snapshot_version, payload FROM snapshots
             WHERE aggregate_type = 'account' AND aggregate_id = 'one'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(snapshot, (2, 1, r#"{"balance":42}"#.to_owned()));
        let job: (i64, String, Option<i64>) = sqlx::query_as(
            "SELECT version, payload, lease_until FROM job_queue_projection WHERE view_id = 'receipt'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            job,
            (
                1,
                r#"{"Live":{"Pending":{"kind":"receipt","run_at":0}}}"#.to_owned(),
                None
            )
        );
    }

    #[tokio::test]
    async fn queue_insert_failure_rolls_back_the_entire_commit() {
        let pool = create_test_pool().await.unwrap();
        sqlx::query(
            "CREATE TRIGGER refuse_queue_seed BEFORE INSERT ON job_queue_projection
             BEGIN SELECT RAISE(ABORT, 'queue seed rejected'); END",
        )
        .execute(&pool)
        .await
        .unwrap();

        let SqliteAggregateError::Connection(sqlx::Error::Database(source)) =
            write_with_job(&pool).await.unwrap_err()
        else {
            panic!("expected the queue insertion error");
        };
        assert_eq!(source.message(), "queue seed rejected");
        let counts: (i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM events), (SELECT COUNT(*) FROM snapshots),
                    (SELECT COUNT(*) FROM job_queue_projection)",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(counts, (0, 0, 0));
    }

    #[tokio::test]
    async fn stale_writers_fail_before_snapshot_and_job_preparation() {
        let pool = create_test_pool().await.unwrap();
        write_with_job(&pool).await.unwrap();

        let result = commit_atomic::<_, _, SqliteAggregateError>(
            &pool,
            Commit {
                aggregate_type: "account",
                aggregate_id: "one",
                expected_version: Some(1),
                events: &[event("account", "one", 2)],
                snapshot: Some(()),
                jobs: vec![()],
            },
            |()| panic!("stale writer must not prepare a snapshot"),
            |&()| panic!("stale writer must not prepare a job"),
            |()| panic!("stale writer must not prepare a queue payload"),
        )
        .await;
        assert!(matches!(
            result.unwrap_err(),
            SqliteAggregateError::OptimisticLock
        ));
    }
}
