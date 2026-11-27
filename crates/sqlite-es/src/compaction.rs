//! Snapshot-bounded SQLite event compaction.

use sqlx::SqlitePool;

use event_sorcery_core::CompactionPolicy;

/// Delete compactable events that are already represented by snapshots.
///
/// This is a no-op for [`CompactionPolicy::Retain`].
/// [`CompactionPolicy::CompactAfterSnapshot`] removes snapshot-covered events;
/// [`CompactionPolicy::RetainLast`] additionally preserves the trailing sequence
/// window for each aggregate type and id. Neither policy deletes unsnapshotted
/// events or events from streams without snapshots.
///
/// # Errors
///
/// Returns database errors from the delete query or an encoding error when the
/// retention window cannot be represented as a SQLite integer.
pub async fn compact_events(
    pool: &SqlitePool,
    aggregate_type: &str,
    policy: CompactionPolicy,
) -> Result<u64, sqlx::Error> {
    let result = match policy {
        CompactionPolicy::Retain => return Ok(0),
        CompactionPolicy::CompactAfterSnapshot => {
            sqlx::query(
                "DELETE FROM events \
                 WHERE aggregate_type = ?1 \
                   AND sequence <= ( \
                       SELECT last_sequence FROM snapshots \
                       WHERE snapshots.aggregate_type = events.aggregate_type \
                         AND snapshots.aggregate_id = events.aggregate_id \
                   )",
            )
            .bind(aggregate_type)
            .execute(pool)
            .await?
        }
        CompactionPolicy::RetainLast { events } => {
            let window =
                i64::try_from(events).map_err(|source| sqlx::Error::Encode(Box::new(source)))?;

            sqlx::query(
                "DELETE FROM events \
                 WHERE aggregate_type = ?1 \
                   AND sequence <= ( \
                       SELECT last_sequence FROM snapshots \
                       WHERE snapshots.aggregate_type = events.aggregate_type \
                         AND snapshots.aggregate_id = events.aggregate_id \
                   ) \
                   AND sequence <= ( \
                       SELECT MAX(newest.sequence) FROM events newest \
                       WHERE newest.aggregate_type = events.aggregate_type \
                         AND newest.aggregate_id = events.aggregate_id \
                   ) - ?2",
            )
            .bind(aggregate_type)
            .bind(window)
            .execute(pool)
            .await?
        }
    };

    Ok(result.rows_affected())
}
