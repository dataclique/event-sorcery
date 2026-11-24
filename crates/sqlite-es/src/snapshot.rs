//! Snapshots on the caller's SQLite connection.

use cqrs_es::persist::SerializedSnapshot;
use serde_json::Value;
use sqlx::{SqliteConnection, SqlitePool};

use crate::SqliteAggregateError;

/// Writes a snapshot after the caller checks its coverage in the same transaction.
///
/// The snapshot counter and the last covered event sequence are independent.
pub async fn write_snapshot(
    connection: &mut SqliteConnection,
    aggregate_type: &str,
    aggregate_id: &str,
    last_sequence: usize,
    snapshot_version: usize,
    payload: &Value,
) -> Result<(), SqliteAggregateError> {
    let last_sequence = i64::try_from(last_sequence)?;
    let snapshot_version = i64::try_from(snapshot_version)?;
    let payload = serde_json::to_string(payload)?;

    sqlx::query!(
        r#"
                INSERT OR REPLACE INTO snapshots (
                    aggregate_type,
                    aggregate_id,
                    last_sequence,
                    snapshot_version,
                    payload,
                    timestamp
                )
                VALUES (
                    ?1,
                    ?2,
                    ?3,
                    ?4,
                    ?5,
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                )
                "#,
        aggregate_type,
        aggregate_id,
        last_sequence,
        snapshot_version,
        payload,
    )
    .execute(connection)
    .await?;
    Ok(())
}

/// Clears snapshots for an aggregate type after validating each covered history.
///
/// Validation sees the snapshot's covered sequence and the count of retained
/// events in its positive covered range. A refusal rolls back the whole clear.
pub async fn clear_snapshots<Error>(
    pool: &SqlitePool,
    aggregate_type: &str,
    mut validate: impl FnMut(String, i64, i64) -> Result<(), Error> + Send,
) -> Result<(), Error>
where
    Error: From<sqlx::Error>,
{
    let mut transaction = pool.begin().await?;
    let histories: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT s.aggregate_id, s.last_sequence, \
         (SELECT COUNT(*) FROM events e \
          WHERE e.aggregate_type = s.aggregate_type \
            AND e.aggregate_id = s.aggregate_id \
            AND e.sequence BETWEEN 1 AND s.last_sequence) \
         FROM snapshots s WHERE s.aggregate_type = ?1",
    )
    .bind(aggregate_type)
    .fetch_all(&mut *transaction)
    .await?;
    for (aggregate_id, covered_sequence, retained_events) in histories {
        validate(aggregate_id, covered_sequence, retained_events)?;
    }

    sqlx::query("DELETE FROM snapshots WHERE aggregate_type = ?1")
        .bind(aggregate_type)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(())
}

/// Deletes the snapshot for one stream, without changing its events.
pub async fn discard_snapshot(
    pool: &SqlitePool,
    aggregate_type: &str,
    aggregate_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
            DELETE FROM snapshots
            WHERE aggregate_type = ?1 AND aggregate_id = ?2
            "#,
        aggregate_type,
        aggregate_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Loads the current snapshot on the caller's connection, when present.
pub async fn load_snapshot(
    connection: &mut SqliteConnection,
    aggregate_type: &str,
    aggregate_id: &str,
) -> Result<Option<SerializedSnapshot>, SqliteAggregateError> {
    let row = sqlx::query!(
        r#"
            SELECT aggregate_id,
                   last_sequence,
                   snapshot_version,
                   payload AS "payload: String"
            FROM snapshots
            WHERE aggregate_type = ?1 AND aggregate_id = ?2
            "#,
        aggregate_type,
        aggregate_id,
    )
    .fetch_optional(connection)
    .await?;

    row.map(|row| {
        Ok(SerializedSnapshot {
            aggregate_id: row.aggregate_id,
            current_sequence: usize::try_from(row.last_sequence)?,
            current_snapshot: usize::try_from(row.snapshot_version)?,
            aggregate: serde_json::from_str(&row.payload)?,
        })
    })
    .transpose()
}
