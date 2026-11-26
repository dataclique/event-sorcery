//! Enumerating aggregates from SQLite events and snapshots.

use sqlx::SqlitePool;
use std::{fmt::Debug, str::FromStr};

/// Lists distinct aggregate IDs in ascending lexicographic order before parsing them.
pub async fn load_all_ids<Id: FromStr>(
    pool: &SqlitePool,
    aggregate_type: &'static str,
) -> Result<Vec<Id>, LoadAllIdsError>
where
    Id::Err: Debug,
{
    let rows = sqlx::query_scalar(
        "SELECT aggregate_id
         FROM (
             SELECT aggregate_id FROM events WHERE aggregate_type = ?1
             UNION
             SELECT aggregate_id FROM snapshots WHERE aggregate_type = ?1
         )
         ORDER BY aggregate_id ASC",
    )
    .bind(aggregate_type)
    .fetch_all(pool)
    .await?;

    parse_ids(rows, aggregate_type)
}

/// Lists distinct IDs, ordered by descending maximum event or snapshot rowid.
///
/// Appending to an existing aggregate can change its position; ties have no
/// guaranteed order. An offset skips distinct IDs, not individual rows.
pub async fn load_ids_paginated<Id: FromStr>(
    pool: &SqlitePool,
    aggregate_type: &'static str,
    limit: usize,
    offset: usize,
) -> Result<Vec<Id>, LoadAllIdsError>
where
    Id::Err: Debug,
{
    let rows = sqlx::query_scalar(
        "SELECT aggregate_id
         FROM (
             SELECT aggregate_id, MAX(rowid) AS latest_rowid
             FROM events
             WHERE aggregate_type = ?1
             GROUP BY aggregate_id
             UNION ALL
             SELECT aggregate_id, MAX(rowid) AS latest_rowid
             FROM snapshots
             WHERE aggregate_type = ?1
             GROUP BY aggregate_id
         )
         GROUP BY aggregate_id
         ORDER BY MAX(latest_rowid) DESC
         LIMIT ?2 OFFSET ?3",
    )
    .bind(aggregate_type)
    .bind(i64::try_from(limit)?)
    .bind(i64::try_from(offset)?)
    .fetch_all(pool)
    .await?;

    parse_ids(rows, aggregate_type)
}

/// Counts distinct IDs with at least one event or a snapshot.
pub async fn count_aggregates(
    pool: &SqlitePool,
    aggregate_type: &str,
) -> Result<usize, LoadAllIdsError> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM (
             SELECT aggregate_id FROM events WHERE aggregate_type = ?1
             UNION
             SELECT aggregate_id FROM snapshots WHERE aggregate_type = ?1
         )",
    )
    .bind(aggregate_type)
    .fetch_one(pool)
    .await?;

    Ok(usize::try_from(count)?)
}

/// An aggregate ID query could not return its results.
#[derive(Debug, thiserror::Error)]
pub enum LoadAllIdsError {
    /// The database could not complete the query.
    #[error("Database error: {0}")]
    Sql(#[from] sqlx::Error),
    /// Stored identifiers do not parse as the caller's requested ID type.
    #[error("Found unparseable aggregate IDs for {aggregate_type}: {ids:?}")]
    InvalidIds {
        /// Aggregate type namespace used for the query.
        aggregate_type: &'static str,
        /// The stored identifiers that could not be parsed.
        ids: Vec<String>,
    },
    /// A count or page bound did not fit the destination integer type.
    #[error("Numeric conversion error: {0}")]
    NumericConversion(#[from] std::num::TryFromIntError),
}

fn parse_ids<Id: FromStr>(
    rows: Vec<String>,
    aggregate_type: &'static str,
) -> Result<Vec<Id>, LoadAllIdsError>
where
    Id::Err: Debug,
{
    let mut ids = Vec::with_capacity(rows.len());
    let mut invalid = Vec::new();
    for row in rows {
        match row.parse::<Id>() {
            Ok(id) => ids.push(id),
            Err(parse_error) => {
                tracing::warn!(
                    aggregate_id = row,
                    aggregate_type,
                    ?parse_error,
                    "Failed to parse aggregate ID"
                );
                invalid.push(row);
            }
        }
    }

    if invalid.is_empty() {
        Ok(ids)
    } else {
        Err(LoadAllIdsError::InvalidIds {
            aggregate_type,
            ids: invalid,
        })
    }
}
