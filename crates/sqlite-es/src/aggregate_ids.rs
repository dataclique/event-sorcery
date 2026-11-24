//! Enumerating aggregates from SQLite events and snapshots.

use sqlx::SqlitePool;

/// Lists distinct aggregate IDs in ascending lexicographic order.
pub async fn load_all_ids(
    pool: &SqlitePool,
    aggregate_type: &str,
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT aggregate_id FROM ( \
             SELECT aggregate_id FROM events WHERE aggregate_type = ?1 \
             UNION \
             SELECT aggregate_id FROM snapshots WHERE aggregate_type = ?1 \
         ) \
         ORDER BY aggregate_id ASC",
    )
    .bind(aggregate_type)
    .fetch_all(pool)
    .await
}

/// Lists distinct IDs, ordered by descending maximum event or snapshot rowid.
///
/// Appending to an existing aggregate can change its position; ties have no
/// guaranteed order. `limit` and `offset` use SQLite's `LIMIT` and `OFFSET` semantics.
pub async fn load_ids_paginated(
    pool: &SqlitePool,
    aggregate_type: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT aggregate_id FROM ( \
             SELECT aggregate_id, MAX(rowid) AS latest_rowid \
             FROM events \
             WHERE aggregate_type = ?1 \
             GROUP BY aggregate_id \
             UNION ALL \
             SELECT aggregate_id, MAX(rowid) AS latest_rowid \
             FROM snapshots \
             WHERE aggregate_type = ?1 \
             GROUP BY aggregate_id \
         ) \
         GROUP BY aggregate_id \
         ORDER BY MAX(latest_rowid) DESC \
         LIMIT ?2 OFFSET ?3",
    )
    .bind(aggregate_type)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await
}

/// Counts distinct IDs with at least one event or a snapshot.
pub async fn count_aggregates(pool: &SqlitePool, aggregate_type: &str) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM ( \
             SELECT aggregate_id FROM events WHERE aggregate_type = ?1 \
             UNION \
             SELECT aggregate_id FROM snapshots WHERE aggregate_type = ?1 \
         )",
    )
    .bind(aggregate_type)
    .fetch_one(pool)
    .await
}
