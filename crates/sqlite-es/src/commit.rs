//! Atomic writes of prepared aggregate state and associated job records.

use cqrs_es::persist::{SerializedEvent, SerializedSnapshot};
use sqlx::SqlitePool;

use crate::{SqliteAggregateError, insert_serialized_events_batch};

/// One prepared aggregate write and its associated durable job rows.
pub struct Commit<'events> {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub expected_version: Option<usize>,
    pub events: &'events [SerializedEvent],
    pub snapshot: Option<SerializedSnapshot>,
    pub jobs: Vec<JobWrite>,
}

/// An authoritative job event and its derived queue-row payload.
pub struct JobWrite {
    pub event: SerializedEvent,
    pub queue_payload: String,
}

/// Writes one prepared batch under SQLite's writer lock.
///
/// Event and job preparation belongs to the caller. This function owns the
/// transaction, durable-version fencing and all associated database writes.
pub async fn commit_atomic(
    pool: &SqlitePool,
    commit: Commit<'_>,
) -> Result<(), SqliteAggregateError> {
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
        let actual: i64 = sqlx::query_scalar(
            "SELECT MAX(\
                COALESCE((SELECT MAX(sequence) FROM events \
                    WHERE aggregate_type = ?1 AND aggregate_id = ?2), 0), \
                COALESCE((SELECT last_sequence FROM snapshots \
                    WHERE aggregate_type = ?1 AND aggregate_id = ?2), 0))",
        )
        .bind(&aggregate_type)
        .bind(&aggregate_id)
        .fetch_one(&mut *transaction)
        .await?;
        if usize::try_from(actual)? != expected {
            return Err(SqliteAggregateError::OptimisticLock);
        }
    }

    insert_serialized_events_batch(&mut transaction, "events", events).await?;

    if let Some(snapshot) = snapshot {
        let latest = events
            .last()
            .ok_or(SqliteAggregateError::EmptySnapshotUpdate)?
            .sequence;
        if snapshot.current_sequence > latest {
            return Err(SqliteAggregateError::SnapshotBeyondCurrentVersion {
                proposed: snapshot.current_sequence,
                current: latest,
            });
        }
        let previous: Option<i64> = sqlx::query_scalar(
            "SELECT last_sequence FROM snapshots WHERE aggregate_type = ?1 AND aggregate_id = ?2",
        )
        .bind(&aggregate_type)
        .bind(&aggregate_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(previous) = previous
            && usize::try_from(previous)? >= snapshot.current_sequence
        {
            return Err(SqliteAggregateError::OptimisticLock);
        }
        let last_sequence = i64::try_from(snapshot.current_sequence)?;
        let snapshot_version = i64::try_from(snapshot.current_snapshot)?;
        let payload = serde_json::to_string(&snapshot.aggregate)?;
        sqlx::query(
            "INSERT OR REPLACE INTO snapshots \
                (aggregate_type, aggregate_id, last_sequence, snapshot_version, payload, timestamp) \
             VALUES (?1, ?2, ?3, ?4, ?5, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
        ).bind(&aggregate_type).bind(&aggregate_id).bind(last_sequence)
            .bind(snapshot_version).bind(payload).execute(&mut *transaction).await?;
    }

    for job in jobs {
        insert_serialized_events_batch(
            &mut transaction,
            "events",
            std::slice::from_ref(&job.event),
        )
        .await?;
        sqlx::query(
            "INSERT INTO job_queue_projection (view_id, version, payload, lease_until) \
             VALUES (?1, 1, ?2, NULL)",
        )
        .bind(&job.event.aggregate_id)
        .bind(job.queue_payload)
        .execute(&mut *transaction)
        .await?;
    }

    transaction.commit().await?;
    Ok(())
}
