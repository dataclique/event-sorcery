use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Pool, Sqlite};

use crate::MIGRATOR;

/// Creates an in-memory SQLite database with migrations applied
///
/// # Errors
///
/// Returns an error if the database connection fails or migrations cannot be applied
pub async fn create_test_pool() -> Result<Pool<Sqlite>, sqlx::Error> {
    let pool = SqlitePoolOptions::new()
        .min_connections(1)
        .max_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect(":memory:")
        .await?;

    MIGRATOR.run(&pool).await?;

    Ok(pool)
}
#[cfg(test)]
mod tests {
    //! Tests for SQLite fixture connection persistence.

    use super::*;

    #[tokio::test]
    async fn test_pool_preserves_schema_and_rows_between_acquisitions() {
        let pool = create_test_pool().await.unwrap();

        let mut first_connection = pool.acquire().await.unwrap();
        sqlx::query("CREATE TABLE fixture_probe (value INTEGER NOT NULL)")
            .execute(&mut *first_connection)
            .await
            .unwrap();
        sqlx::query("INSERT INTO fixture_probe (value) VALUES (42)")
            .execute(&mut *first_connection)
            .await
            .unwrap();
        drop(first_connection);

        let mut second_connection = pool.acquire().await.unwrap();
        let schema_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM sqlite_schema
             WHERE type = 'table'
               AND name = 'fixture_probe'",
        )
        .fetch_one(&mut *second_connection)
        .await
        .unwrap();
        let value = sqlx::query_scalar::<_, i64>("SELECT value FROM fixture_probe")
            .fetch_one(&mut *second_connection)
            .await
            .unwrap();

        assert_eq!(schema_count, 1);
        assert_eq!(value, 42);

        assert_eq!(pool.options().get_max_connections(), 1);
        assert_eq!(pool.options().get_min_connections(), 1);
        assert_eq!(pool.options().get_idle_timeout(), None);
        assert_eq!(pool.options().get_max_lifetime(), None);
    }
}
