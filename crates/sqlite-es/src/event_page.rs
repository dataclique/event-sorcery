//! Reading item- and storage-byte-bounded event pages from SQLite.

use sqlx::sqlite::SqliteRow;
use sqlx::{FromRow, SqlitePool};
use std::num::NonZeroUsize;

/// Loads at most `limit` events from one stream after an optional exclusive
/// sequence checkpoint.
pub async fn load_events_page<EventRow>(
    pool: &SqlitePool,
    aggregate_type: &str,
    aggregate_id: &str,
    after_sequence: Option<usize>,
    limit: NonZeroUsize,
) -> Result<Vec<EventRow>, EventPageReadError>
where
    EventRow: for<'row> FromRow<'row, SqliteRow> + Send + Unpin,
{
    let limit = i64::try_from(limit.get())?;
    let rows = match after_sequence {
        None => {
            sqlx::query_as::<_, EventRow>(
                r"
                SELECT aggregate_type,
                       aggregate_id,
                       sequence,
                       event_type,
                       event_version,
                       payload,
                       metadata
                FROM events
                WHERE aggregate_type = ?1 AND aggregate_id = ?2
                ORDER BY sequence
                LIMIT ?3
                ",
            )
            .bind(aggregate_type)
            .bind(aggregate_id)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
        Some(after_sequence) => {
            let after_sequence = i64::try_from(after_sequence)?;
            sqlx::query_as::<_, EventRow>(
                r"
                SELECT aggregate_type,
                       aggregate_id,
                       sequence,
                       event_type,
                       event_version,
                       payload,
                       metadata
                FROM events
                WHERE aggregate_type = ?1
                  AND aggregate_id = ?2
                  AND sequence > ?3
                ORDER BY sequence
                LIMIT ?4
                ",
            )
            .bind(aggregate_type)
            .bind(aggregate_id)
            .bind(after_sequence)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
    };

    Ok(rows)
}

/// Loads an item- and storage-byte-bounded event page.
pub async fn load_events_page_bounded<EventRow>(
    pool: &SqlitePool,
    aggregate_type: &str,
    aggregate_id: &str,
    after_sequence: Option<usize>,
    item_limit: NonZeroUsize,
    byte_limit: NonZeroUsize,
) -> Result<Vec<EventRow>, EventPageReadError>
where
    EventRow: for<'row> FromRow<'row, SqliteRow> + Send + Unpin,
{
    let item_limit = i64::try_from(item_limit.get())?;
    let mut transaction = pool.begin().await?;
    let storage_bytes = match after_sequence {
        None => {
            sqlx::query_scalar::<_, i64>(
                r"
                SELECT COALESCE(SUM(
                    length(CAST(aggregate_type AS BLOB))
                    + length(CAST(aggregate_id AS BLOB))
                    + 8
                    + length(CAST(event_type AS BLOB))
                    + length(CAST(event_version AS BLOB))
                    + length(CAST(payload AS BLOB))
                    + length(CAST(metadata AS BLOB))
                ), 0)
                FROM (
                    SELECT aggregate_type,
                           aggregate_id,
                           sequence,
                           event_type,
                           event_version,
                           payload,
                           metadata
                    FROM events
                    WHERE aggregate_type = ?1 AND aggregate_id = ?2
                    ORDER BY sequence
                    LIMIT ?3
                )
                ",
            )
            .bind(aggregate_type)
            .bind(aggregate_id)
            .bind(item_limit)
            .fetch_one(&mut *transaction)
            .await?
        }
        Some(after_sequence) => {
            let after_sequence = i64::try_from(after_sequence)?;
            sqlx::query_scalar::<_, i64>(
                r"
                SELECT COALESCE(SUM(
                    length(CAST(aggregate_type AS BLOB))
                    + length(CAST(aggregate_id AS BLOB))
                    + 8
                    + length(CAST(event_type AS BLOB))
                    + length(CAST(event_version AS BLOB))
                    + length(CAST(payload AS BLOB))
                    + length(CAST(metadata AS BLOB))
                ), 0)
                FROM (
                    SELECT aggregate_type,
                           aggregate_id,
                           sequence,
                           event_type,
                           event_version,
                           payload,
                           metadata
                    FROM events
                    WHERE aggregate_type = ?1
                      AND aggregate_id = ?2
                      AND sequence > ?3
                    ORDER BY sequence
                    LIMIT ?4
                )
                ",
            )
            .bind(aggregate_type)
            .bind(aggregate_id)
            .bind(after_sequence)
            .bind(item_limit)
            .fetch_one(&mut *transaction)
            .await?
        }
    };
    let observed = usize::try_from(storage_bytes)?;
    if observed > byte_limit.get() {
        return Err(EventPageReadError::TooLarge {
            observed,
            limit: byte_limit.get(),
        });
    }

    let rows = match after_sequence {
        None => {
            sqlx::query_as::<_, EventRow>(
                r"
                SELECT aggregate_type,
                       aggregate_id,
                       sequence,
                       event_type,
                       event_version,
                       payload,
                       metadata
                FROM events
                WHERE aggregate_type = ?1 AND aggregate_id = ?2
                ORDER BY sequence
                LIMIT ?3
                ",
            )
            .bind(aggregate_type)
            .bind(aggregate_id)
            .bind(item_limit)
            .fetch_all(&mut *transaction)
            .await?
        }
        Some(after_sequence) => {
            let after_sequence = i64::try_from(after_sequence)?;
            sqlx::query_as::<_, EventRow>(
                r"
                SELECT aggregate_type,
                       aggregate_id,
                       sequence,
                       event_type,
                       event_version,
                       payload,
                       metadata
                FROM events
                WHERE aggregate_type = ?1
                  AND aggregate_id = ?2
                  AND sequence > ?3
                ORDER BY sequence
                LIMIT ?4
                ",
            )
            .bind(aggregate_type)
            .bind(aggregate_id)
            .bind(after_sequence)
            .bind(item_limit)
            .fetch_all(&mut *transaction)
            .await?
        }
    };
    transaction.commit().await?;

    Ok(rows)
}

/// An event page failed to load or exceeded the caller's storage-byte limit.
#[derive(Debug, thiserror::Error)]
pub enum EventPageReadError {
    /// SQLite could not read the event page.
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    /// A page limit, sequence, or byte count did not fit its storage representation.
    #[error(transparent)]
    Integer(#[from] std::num::TryFromIntError),
    /// The selected rows exceed the budget and were not loaded.
    #[error("event page storage bytes {observed} exceed limit {limit}")]
    TooLarge {
        /// Total bytes measured for the selected rows.
        observed: usize,
        /// Maximum number of storage bytes requested.
        limit: usize,
    },
}
