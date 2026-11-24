//! Snapshot writes on the caller's SQLite connection.

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
