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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use event_sorcery_core::CompactionPolicy;

    use super::*;
    use crate::{compact_events, testing::create_test_pool, write_snapshot};

    #[tokio::test]
    async fn version_is_scoped_to_both_parts_of_the_stream_key() {
        let pool = create_test_pool().await.unwrap();
        let mut connection = pool.acquire().await.unwrap();
        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type,
                                 event_version, payload, metadata)
             VALUES ('invoice', 'one', 3, 'Issued', '1', '{}', '{}'),
                    ('invoice', 'other', 41, 'Issued', '1', '{}', '{}'),
                    ('order', 'one', 72, 'Placed', '1', '{}', '{}')",
        )
        .execute(&mut *connection)
        .await
        .unwrap();
        write_snapshot(&mut connection, "order", "other", 89, 103, &json!({}))
            .await
            .unwrap();

        for (aggregate_type, aggregate_id, expected) in [
            ("invoice", "one", 3),
            ("invoice", "other", 41),
            ("order", "one", 72),
            ("order", "other", 89),
            ("invoice", "missing", 0),
        ] {
            assert_eq!(
                committed_stream_version(&mut connection, aggregate_type, aggregate_id)
                    .await
                    .unwrap(),
                expected,
            );
        }
    }

    #[tokio::test]
    async fn compacted_prefix_and_retained_tail_both_contribute_to_the_version() {
        let pool = create_test_pool().await.unwrap();
        let mut connection = pool.acquire().await.unwrap();
        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type,
                                 event_version, payload, metadata)
             VALUES ('invoice', 'one', 1, 'Issued', '1', '{}', '{}'),
                    ('invoice', 'one', 2, 'Adjusted', '1', '{}', '{}')",
        )
        .execute(&mut *connection)
        .await
        .unwrap();
        write_snapshot(&mut connection, "invoice", "one", 2, 11, &json!({}))
            .await
            .unwrap();
        drop(connection);

        assert_eq!(
            compact_events(&pool, "invoice", CompactionPolicy::CompactAfterSnapshot)
                .await
                .unwrap(),
            2,
        );
        let mut transaction = pool.begin().await.unwrap();
        assert_eq!(
            committed_stream_version(&mut transaction, "invoice", "one")
                .await
                .unwrap(),
            2,
        );
        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type,
                                 event_version, payload, metadata)
             VALUES ('invoice', 'one', 3, 'Adjusted', '1', '{}', '{}')",
        )
        .execute(&mut *transaction)
        .await
        .unwrap();
        assert_eq!(
            committed_stream_version(&mut transaction, "invoice", "one")
                .await
                .unwrap(),
            3,
        );
        transaction.rollback().await.unwrap();

        let mut connection = pool.acquire().await.unwrap();
        assert_eq!(
            committed_stream_version(&mut connection, "invoice", "one")
                .await
                .unwrap(),
            2,
        );
    }
}
