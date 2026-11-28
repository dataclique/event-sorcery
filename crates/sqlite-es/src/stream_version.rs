//! SQLite's durable stream version, including compacted history.

use sqlx::SqliteExecutor;

use crate::SqliteAggregateError;

/// Reads one stream's durable version, where zero means nothing was ever
/// committed.
///
/// Compaction deletes every event a snapshot already covers, so
/// `MAX(events.sequence)` alone can fall back below the aggregate's real version
/// and stop reporting stale writers. `snapshots.last_sequence` is the durable
/// record of how far the erased prefix reached, so the greater of the two is the
/// only sound conflict oracle. Pass the transaction's connection when fencing
/// a write so it observes the same state as the append.
pub async fn committed_stream_version<'connection>(
    executor: impl SqliteExecutor<'connection>,
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
    .fetch_one(executor)
    .await?;

    usize::try_from(version).map_err(SqliteAggregateError::from)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use event_sorcery_core::CompactionPolicy;

    use super::*;
    use crate::testing::{create_test_pool, snapshot_fixture::record};
    use crate::{compact_events, store_snapshot, write_snapshot};

    #[tokio::test]
    async fn version_is_scoped_to_both_parts_of_the_stream_key() {
        let pool = create_test_pool().await.unwrap();
        record::<false>(&pool, "one", 3).await;
        record::<false>(&pool, "other", 41).await;
        record::<true>(&pool, "one", 72).await;
        let mut connection = pool.acquire().await.unwrap();
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
                committed_stream_version(&mut *connection, aggregate_type, aggregate_id)
                    .await
                    .unwrap(),
                expected,
            );
        }
    }

    #[tokio::test]
    async fn compacted_prefix_and_retained_tail_both_contribute_to_the_version() {
        let pool = create_test_pool().await.unwrap();
        record::<false>(&pool, "one", 2).await;
        store_snapshot(&pool, "invoice", "one", 2, &json!({"Live": null}))
            .await
            .unwrap();

        assert_eq!(
            compact_events(&pool, "invoice", CompactionPolicy::CompactAfterSnapshot)
                .await
                .unwrap(),
            2,
        );
        let mut transaction = pool.begin().await.unwrap();
        assert_eq!(
            committed_stream_version(&mut *transaction, "invoice", "one")
                .await
                .unwrap(),
            2,
        );
        transaction.rollback().await.unwrap();
        record::<false>(&pool, "one", 1).await;
        let mut transaction = pool.begin().await.unwrap();
        assert_eq!(
            committed_stream_version(&mut *transaction, "invoice", "one")
                .await
                .unwrap(),
            3,
        );
        transaction.rollback().await.unwrap();

        let mut connection = pool.acquire().await.unwrap();
        assert_eq!(
            committed_stream_version(&mut *connection, "invoice", "one")
                .await
                .unwrap(),
            3,
        );
    }
}
