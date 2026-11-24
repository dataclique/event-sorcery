//! SQLite's durable stream version, including compacted history.

use sqlx::SqliteConnection;

use crate::SqliteAggregateError;

/// Reads one stream's durable version, where zero means nothing was ever
/// committed.
///
/// Compaction deletes every event a snapshot already covers, so
/// `MAX(events.sequence)` alone can fall back below the aggregate's real version
/// and stop reporting stale writers. `snapshots.last_sequence` is the durable
/// record of how far the erased prefix reached, so the greater of the two is the
/// only sound conflict oracle. Reads on the caller's connection, so a commit
/// sees the two records under the write lock it already holds and appends
/// against exactly what it observed.
pub async fn committed_stream_version(
    connection: &mut SqliteConnection,
    aggregate_type: &str,
    aggregate_id: &str,
) -> Result<usize, SqliteAggregateError> {
    let version = sqlx::query_scalar::<_, i64>(
        r"
        SELECT MAX(
            COALESCE(
                (SELECT MAX(sequence) FROM events
                 WHERE aggregate_type = ?1 AND aggregate_id = ?2),
                0
            ),
            COALESCE(
                (SELECT last_sequence FROM snapshots
                 WHERE aggregate_type = ?1 AND aggregate_id = ?2),
                0
            )
        )
        ",
    )
    .bind(aggregate_type)
    .bind(aggregate_id)
    .fetch_one(connection)
    .await?;

    usize::try_from(version).map_err(SqliteAggregateError::from)
}
