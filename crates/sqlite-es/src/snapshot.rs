//! Snapshots on the caller's SQLite connection.

use cqrs_es::persist::SerializedSnapshot;
use serde_json::Value;
use sqlx::SqliteConnection;

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
