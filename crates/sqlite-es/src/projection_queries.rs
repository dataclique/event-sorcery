//! SQLite reads and column diagnostics for materialized projections.

use sqlx::{AssertSqlSafe, Sqlite, SqlitePool};
use tracing::warn;

/// Reads projection IDs and serialized payloads in ascending ID order.
///
/// The table must be a trusted SQL identifier from the projection declaration.
pub async fn projection_rows(
    pool: &SqlitePool,
    table: &'static str,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    sqlx::query_as(AssertSqlSafe(format!(
        "SELECT view_id, payload
         FROM {table}
         ORDER BY view_id ASC"
    )))
    .fetch_all(pool)
    .await
}

/// Validates a projection column and reads matching rows in ID order.
///
/// The table must be a trusted SQL identifier from application code. The
/// column is the literal name as stored in the table schema.
/// The filter value is bound as a query parameter.
pub async fn filter_projection<Value>(
    pool: &SqlitePool,
    table: &'static str,
    column: &'static str,
    value: &Value,
) -> Result<Vec<(String, String)>, SqliteProjectionFilterError>
where
    for<'query> &'query Value: sqlx::Encode<'query, Sqlite>,
    Value: sqlx::Type<Sqlite> + Send + Sync,
{
    let columns: Vec<String> = sqlx::query_scalar("SELECT name FROM pragma_table_xinfo(?1)")
        .bind(table)
        .fetch_all(pool)
        .await?;
    if !columns.iter().any(|name| name == column) {
        warn!(%column, %table, "Column does not exist in table schema");
        return Err(SqliteProjectionFilterError::ColumnNotFound);
    }

    let quoted_column = indexed_query::quote_identifier(column);
    let (row_count, non_null_count): (i64, i64) = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT COUNT(*), COUNT({quoted_column}) FROM {table}"
    )))
    .fetch_one(pool)
    .await?;
    if row_count > 0 && non_null_count == 0 {
        warn!(%column, %table, row_count, "Generated column has all NULL values, likely stale JSON path");
        return Err(SqliteProjectionFilterError::StaleColumn { row_count });
    }

    Ok(sqlx::query_as(AssertSqlSafe(format!(
        "SELECT view_id, payload FROM {table} WHERE {quoted_column} = ?1 ORDER BY view_id"
    )))
    .bind(value)
    .fetch_all(pool)
    .await?)
}

/// A SQLite projection filter could not validate or query its column.
#[derive(Debug, thiserror::Error)]
pub enum SqliteProjectionFilterError {
    /// The column does not exist in the declared table.
    #[error("projection filter column does not exist")]
    ColumnNotFound,
    /// Every row has a NULL value in the requested column.
    #[error("projection filter column is NULL in all {row_count} rows")]
    StaleColumn {
        /// Number of rows in the table at validation time.
        row_count: i64,
    },
    /// The query failed in SQLite.
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
}

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;

    use super::{SqliteProjectionFilterError, filter_projection, projection_rows};

    #[tokio::test]
    async fn filter_accepts_a_column_whose_name_is_a_sql_keyword() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "CREATE TABLE note_projection (
                view_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                \"select\" TEXT
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO note_projection VALUES ('note', '{}', 'wanted')")
            .execute(&pool)
            .await
            .unwrap();

        let rows = filter_projection(&pool, "note_projection", "select", &"wanted")
            .await
            .unwrap();
        assert_eq!(rows, [("note".to_owned(), "{}".to_owned())]);
    }

    #[tokio::test]
    async fn filter_distinguishes_missing_stale_and_real_matches() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "CREATE TABLE note_projection (view_id TEXT PRIMARY KEY, payload TEXT NOT NULL, label TEXT)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let missing = filter_projection(&pool, "note_projection", "missing", &"wanted")
            .await
            .unwrap_err();
        assert!(matches!(
            missing,
            SqliteProjectionFilterError::ColumnNotFound
        ));
        assert_eq!(
            filter_projection(&pool, "note_projection", "label", &"wanted")
                .await
                .unwrap(),
            [],
        );
        sqlx::query("INSERT INTO note_projection VALUES ('null', '{}', NULL)")
            .execute(&pool)
            .await
            .unwrap();
        let stale = filter_projection(&pool, "note_projection", "label", &"wanted")
            .await
            .unwrap_err();
        assert!(matches!(
            stale,
            SqliteProjectionFilterError::StaleColumn { row_count: 1 }
        ));

        sqlx::query(
            "INSERT INTO note_projection VALUES ('b', '{}', 'wanted'), ('a', '{}', 'wanted')",
        )
        .execute(&pool)
        .await
        .unwrap();
        let matches = filter_projection(&pool, "note_projection", "label", &"wanted")
            .await
            .unwrap();
        assert_eq!(
            matches,
            [
                ("a".to_string(), "{}".to_string()),
                ("b".to_string(), "{}".to_string())
            ]
        );
        assert_eq!(
            projection_rows(&pool, "note_projection")
                .await
                .unwrap()
                .len(),
            3
        );
    }
}
