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
                       CAST(payload AS TEXT) AS payload,
                       CAST(metadata AS TEXT) AS metadata
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
                       CAST(payload AS TEXT) AS payload,
                       CAST(metadata AS TEXT) AS metadata
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
                       CAST(payload AS TEXT) AS payload,
                       CAST(metadata AS TEXT) AS metadata
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
                       CAST(payload AS TEXT) AS payload,
                       CAST(metadata AS TEXT) AS metadata
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

#[cfg(test)]
mod tests {
    //! Byte budgets track the exact rows returned from the selected stream.

    use cqrs_es::event_sink::EventSink;
    use cqrs_es::{Aggregate, DomainEvent};
    use serde::{Deserialize, Serialize};
    use sqlx::SqlitePool;
    use std::convert::Infallible;
    use std::num::NonZeroUsize;

    use super::{EventPageReadError, load_events_page_bounded};
    use crate::sqlite_cqrs;
    use crate::testing::create_test_pool;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(transparent)]
    struct RecordedText(String);

    impl DomainEvent for RecordedText {
        fn event_type(&self) -> String {
            "E".to_owned()
        }

        fn event_version(&self) -> String {
            "1".to_owned()
        }
    }

    #[derive(Default, Serialize, Deserialize)]
    struct PageStream<const OTHER_TYPE: bool>;

    impl<const OTHER_TYPE: bool> Aggregate for PageStream<OTHER_TYPE> {
        const TYPE: &'static str = if OTHER_TYPE { "B" } else { "A" };
        type Command = RecordedText;
        type Event = RecordedText;
        type Error = Infallible;
        type Services = ();

        async fn handle(
            &mut self,
            command: Self::Command,
            _services: &Self::Services,
            sink: &EventSink<Self>,
        ) -> Result<(), Self::Error> {
            sink.write(command, self).await;
            Ok(())
        }

        fn apply(&mut self, _event: Self::Event) {}
    }

    #[derive(Debug, PartialEq, sqlx::FromRow)]
    struct EventRow {
        aggregate_type: String,
        aggregate_id: String,
        sequence: i64,
        payload: String,
    }

    async fn seed_page() -> SqlitePool {
        let pool = create_test_pool().await.unwrap();
        let stream = sqlite_cqrs::<PageStream<false>>(pool.clone(), vec![], ());
        let other_stream = sqlite_cqrs::<PageStream<true>>(pool.clone(), vec![], ());
        let large = "payload ".repeat(20);

        for text in ["ab".to_owned(), "\u{e9}".to_owned(), large.clone()] {
            stream.execute("x", RecordedText(text)).await.unwrap();
        }
        for text in ["ab".to_owned(), large] {
            stream
                .execute("y", RecordedText(text.clone()))
                .await
                .unwrap();
            other_stream.execute("x", RecordedText(text)).await.unwrap();
        }

        pool
    }

    #[tokio::test]
    async fn bounded_page_respects_the_stream_checkpoint_item_limit_and_exact_byte_budget() {
        let pool = seed_page().await;
        let result = load_events_page_bounded::<EventRow>(
            &pool,
            "A",
            "x",
            Some(1),
            NonZeroUsize::MIN,
            NonZeroUsize::new(18).unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(
            result,
            [EventRow {
                aggregate_type: "A".into(),
                aggregate_id: "x".into(),
                sequence: 2,
                payload: "\"\u{e9}\"".into(),
            }]
        );
    }

    #[tokio::test]
    async fn bounded_page_rejects_when_the_budget_is_one_byte_short() {
        let pool = seed_page().await;
        let error = load_events_page_bounded::<EventRow>(
            &pool,
            "A",
            "x",
            Some(1),
            NonZeroUsize::MIN,
            NonZeroUsize::new(17).unwrap(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            EventPageReadError::TooLarge {
                observed: 18,
                limit: 17
            }
        ));
    }

    #[tokio::test]
    async fn bounded_page_is_empty_after_the_last_event() {
        let pool = seed_page().await;
        let result = load_events_page_bounded::<EventRow>(
            &pool,
            "A",
            "x",
            Some(3),
            NonZeroUsize::MIN,
            NonZeroUsize::MIN,
        )
        .await
        .unwrap();
        assert_eq!(result, []);
    }
}
