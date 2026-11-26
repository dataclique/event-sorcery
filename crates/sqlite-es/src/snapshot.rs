//! Snapshots on the caller's SQLite connection.

use cqrs_es::persist::SerializedSnapshot;
use serde_json::Value;
use sqlx::{SqliteConnection, SqliteExecutor, SqlitePool};

use event_sorcery_core::SnapshotHistory;

use crate::{SqliteAggregateError, committed_stream_version};

/// Stores a snapshot independently of an event append, under the SQLite write lock.
///
/// The covered event sequence must not exceed the durable stream version, or
/// precede the current snapshot's coverage. Returns the next snapshot counter.
pub async fn store_snapshot(
    pool: &SqlitePool,
    aggregate_type: &str,
    aggregate_id: &str,
    last_sequence: usize,
    aggregate: &Value,
) -> Result<usize, StoreSnapshotError> {
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    let current = committed_stream_version(&mut *transaction, aggregate_type, aggregate_id).await?;
    if current == 0 {
        return Err(StoreSnapshotError::EmptyStream);
    }
    if last_sequence > current {
        return Err(StoreSnapshotError::BeyondCurrentVersion {
            proposed: last_sequence,
            current,
        });
    }

    let stored = load_snapshot(&mut *transaction, aggregate_type, aggregate_id).await?;
    if stored
        .as_ref()
        .is_some_and(|snapshot| snapshot.current_sequence > last_sequence)
    {
        return Err(SqliteAggregateError::OptimisticLock.into());
    }
    let snapshot_version = stored.map_or(Ok(1), |snapshot| {
        snapshot
            .current_snapshot
            .checked_add(1)
            .ok_or(StoreSnapshotError::VersionExhausted)
    })?;
    write_snapshot(
        &mut transaction,
        aggregate_type,
        aggregate_id,
        last_sequence,
        snapshot_version,
        aggregate,
    )
    .await?;
    transaction.commit().await?;
    Ok(snapshot_version)
}

/// Failure to write a snapshot independently of an event append.
#[derive(Debug, thiserror::Error)]
pub enum StoreSnapshotError {
    /// The stream has never committed any events.
    #[error("snapshot update was requested without persisted events")]
    EmptyStream,
    /// The proposed coverage exceeds the durable stream version.
    #[error("snapshot sequence {proposed} is beyond the current stream version {current}")]
    BeyondCurrentVersion {
        /// Proposed covered event sequence.
        proposed: usize,
        /// Current durable version of the stream.
        current: usize,
    },
    /// The snapshot counter cannot be incremented.
    #[error("snapshot version is exhausted")]
    VersionExhausted,
    /// SQLite could not start or commit the write transaction.
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    /// Loading or writing the snapshot failed.
    #[error(transparent)]
    Repository(#[from] SqliteAggregateError),
}

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
    for history in snapshot_histories(&mut transaction, aggregate_type, None).await? {
        validate(
            history.aggregate_id,
            history.covered_sequence,
            history.retained_events,
        )?;
    }

    sqlx::query("DELETE FROM snapshots WHERE aggregate_type = ?1")
        .bind(aggregate_type)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(())
}

pub(crate) async fn snapshot_histories(
    connection: &mut SqliteConnection,
    aggregate_type: &str,
    aggregate_id: Option<&str>,
) -> Result<Vec<SnapshotHistory>, sqlx::Error> {
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT s.aggregate_id, s.last_sequence,
                (SELECT COUNT(*) FROM events e
                 WHERE e.aggregate_type = s.aggregate_type
                   AND e.aggregate_id = s.aggregate_id
                   AND e.sequence BETWEEN 1 AND s.last_sequence)
         FROM snapshots s
         WHERE s.aggregate_type = ?1 AND (?2 IS NULL OR s.aggregate_id = ?2)",
    )
    .bind(aggregate_type)
    .bind(aggregate_id)
    .fetch_all(connection)
    .await?;
    Ok(rows.into_iter().map(SnapshotHistory::from).collect())
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

/// Loads the current snapshot from the pool or the caller's transaction.
pub async fn load_snapshot<'connection>(
    executor: impl SqliteExecutor<'connection>,
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
    .fetch_optional(executor)
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::testing::create_test_pool;

    async fn seed_events(pool: &SqlitePool) {
        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type,
                                 event_version, payload, metadata)
             VALUES ('invoice', 'one', 1, 'Issued', '1', '{}', '{}'),
                    ('invoice', 'one', 2, 'Adjusted', '1', '{}', '{}'),
                    ('invoice', 'one', 3, 'Adjusted', '1', '{}', '{}'),
                    ('invoice', 'two', 1, 'Issued', '1', '{}', '{}')",
        )
        .execute(pool)
        .await
        .unwrap();
    }

    async fn stored(
        pool: &SqlitePool,
        aggregate_type: &str,
        id: &str,
    ) -> Option<SerializedSnapshot> {
        let mut connection = pool.acquire().await.unwrap();
        load_snapshot(&mut *connection, aggregate_type, id)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn stores_snapshot_coverage_and_counter_independently() {
        let pool = create_test_pool().await.unwrap();
        seed_events(&pool).await;
        assert_eq!(stored(&pool, "invoice", "one").await, None);

        for (coverage, counter, balance) in [(0, 1, 0), (2, 2, 15), (2, 3, 18)] {
            let state = json!({"balance": balance});
            assert_eq!(
                store_snapshot(&pool, "invoice", "one", coverage, &state)
                    .await
                    .unwrap(),
                counter
            );
            assert_eq!(
                stored(&pool, "invoice", "one").await,
                Some(SerializedSnapshot {
                    aggregate_id: "one".to_owned(),
                    aggregate: state,
                    current_sequence: coverage,
                    current_snapshot: counter,
                })
            );
        }
        assert_eq!(stored(&pool, "invoice", "two").await, None);
    }

    #[tokio::test]
    async fn refuses_empty_future_and_stale_snapshot_writes_without_mutation() {
        let pool = create_test_pool().await.unwrap();
        seed_events(&pool).await;
        store_snapshot(&pool, "invoice", "one", 2, &json!({"balance": 15}))
            .await
            .unwrap();
        let before = stored(&pool, "invoice", "one").await.unwrap();
        let rejected = json!({"balance": 999});

        assert!(matches!(
            store_snapshot(&pool, "invoice", "missing", 0, &rejected)
                .await
                .unwrap_err(),
            StoreSnapshotError::EmptyStream
        ));
        assert!(matches!(
            store_snapshot(&pool, "invoice", "one", 4, &rejected)
                .await
                .unwrap_err(),
            StoreSnapshotError::BeyondCurrentVersion {
                proposed: 4,
                current: 3
            }
        ));
        assert!(matches!(
            store_snapshot(&pool, "invoice", "one", 1, &rejected)
                .await
                .unwrap_err(),
            StoreSnapshotError::Repository(SqliteAggregateError::OptimisticLock)
        ));
        assert_eq!(stored(&pool, "invoice", "one").await, Some(before));
        assert_eq!(stored(&pool, "invoice", "missing").await, None);
    }

    #[tokio::test]
    async fn corrupt_snapshot_returns_a_decode_error_without_replacing_its_bytes() {
        let pool = create_test_pool().await.unwrap();
        seed_events(&pool).await;
        sqlx::query(
            "INSERT INTO snapshots (aggregate_type, aggregate_id, last_sequence,
                                    snapshot_version, payload, timestamp)
             VALUES ('invoice', 'one', 1, 7, '{broken', 'fixture')",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut connection = pool.acquire().await.unwrap();
        let error = load_snapshot(&mut *connection, "invoice", "one")
            .await
            .unwrap_err();
        assert!(matches!(error, SqliteAggregateError::Deserialization(_)));
        drop(connection);

        let error = store_snapshot(&pool, "invoice", "one", 2, &json!({}))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            StoreSnapshotError::Repository(SqliteAggregateError::Deserialization(_))
        ));
        let persisted: (i64, i64, String) =
            sqlx::query_as("SELECT last_sequence, snapshot_version, payload FROM snapshots")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(persisted, (1, 7, "{broken".to_owned()));
    }

    #[derive(Debug, thiserror::Error)]
    enum ClearError {
        #[error(transparent)]
        Sql(#[from] sqlx::Error),
        #[error("history unavailable for {0}")]
        MissingHistory(String),
    }

    #[tokio::test]
    async fn clearing_snapshots_validates_all_covered_ranges_before_removing_rows() {
        let pool = create_test_pool().await.unwrap();
        seed_events(&pool).await;
        let mut connection = pool.acquire().await.unwrap();
        for (aggregate_type, id, coverage) in [
            ("invoice", "one", 2),
            ("invoice", "two", 2),
            ("order", "one", 1),
        ] {
            write_snapshot(
                &mut connection,
                aggregate_type,
                id,
                coverage,
                9,
                &json!({"id": id}),
            )
            .await
            .unwrap();
        }
        drop(connection);

        let mut inspected = Vec::new();
        let error = clear_snapshots(&pool, "invoice", |id, covered, retained| {
            inspected.push((id.clone(), covered, retained));
            if covered == retained {
                Ok(())
            } else {
                Err(ClearError::MissingHistory(id))
            }
        })
        .await
        .unwrap_err();
        assert!(matches!(error, ClearError::MissingHistory(id) if id == "two"));
        assert!(inspected.contains(&("two".to_owned(), 2, 1)));
        assert_eq!(
            stored(&pool, "invoice", "one")
                .await
                .unwrap()
                .current_snapshot,
            9
        );
        assert_eq!(
            stored(&pool, "invoice", "two")
                .await
                .unwrap()
                .current_snapshot,
            9
        );

        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type,
                                 event_version, payload, metadata)
             VALUES ('invoice', 'two', 2, 'Adjusted', '1', '{}', '{}')",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut inspected = Vec::new();
        clear_snapshots::<ClearError>(&pool, "invoice", |id, covered, retained| {
            inspected.push((id, covered, retained));
            Ok(())
        })
        .await
        .unwrap();
        inspected.sort_unstable();
        assert_eq!(
            inspected,
            [("one".to_owned(), 2, 2), ("two".to_owned(), 2, 2)]
        );
        assert_eq!(stored(&pool, "invoice", "one").await, None);
        assert_eq!(stored(&pool, "invoice", "two").await, None);
        assert_eq!(
            stored(&pool, "order", "one")
                .await
                .unwrap()
                .current_snapshot,
            9
        );
        let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(events, 5);
    }

    #[tokio::test]
    async fn discard_is_idempotent_and_preserves_events_and_other_snapshots() {
        let pool = create_test_pool().await.unwrap();
        seed_events(&pool).await;
        store_snapshot(&pool, "invoice", "one", 2, &json!({"balance": 15}))
            .await
            .unwrap();
        store_snapshot(&pool, "invoice", "two", 1, &json!({"balance": 7}))
            .await
            .unwrap();
        let other = stored(&pool, "invoice", "two").await.unwrap();

        discard_snapshot(&pool, "invoice", "one").await.unwrap();
        discard_snapshot(&pool, "invoice", "one").await.unwrap();

        assert_eq!(stored(&pool, "invoice", "one").await, None);
        assert_eq!(stored(&pool, "invoice", "two").await, Some(other));
        let sequences: Vec<i64> = sqlx::query_scalar(
            "SELECT sequence FROM events
             WHERE aggregate_type = 'invoice' AND aggregate_id = 'one' ORDER BY sequence",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(sequences, [1, 2, 3]);
    }

    #[tokio::test]
    async fn snapshot_replacement_failure_keeps_the_previous_snapshot() {
        let pool = create_test_pool().await.unwrap();
        seed_events(&pool).await;
        store_snapshot(&pool, "invoice", "one", 2, &json!({"balance": 15}))
            .await
            .unwrap();
        let before = stored(&pool, "invoice", "one").await.unwrap();
        sqlx::query(
            "CREATE TRIGGER reject_snapshot BEFORE INSERT ON snapshots
             WHEN NEW.snapshot_version > 1
             BEGIN SELECT RAISE(ABORT, 'snapshot replacement rejected'); END",
        )
        .execute(&pool)
        .await
        .unwrap();

        let error = store_snapshot(&pool, "invoice", "one", 3, &json!({"balance": 22}))
            .await
            .unwrap_err();
        let StoreSnapshotError::Repository(SqliteAggregateError::Connection(
            sqlx::Error::Database(source),
        )) = error
        else {
            panic!("expected the snapshot write error, got {error:?}");
        };
        assert_eq!(source.message(), "snapshot replacement rejected");
        assert_eq!(stored(&pool, "invoice", "one").await, Some(before));
    }
}
