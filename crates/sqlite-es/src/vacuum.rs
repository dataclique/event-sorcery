//! Reclaiming free space in SQLite databases.

use sqlx::{AssertSqlSafe, SqlitePool};

/// Reclaim SQLite file space after event compaction.
///
/// Full `VACUUM` can take an exclusive database lock and should be reserved for
/// explicit maintenance windows, not hot runtime loops.
///
/// # Errors
///
/// Returns database errors from SQLite `VACUUM`.
pub async fn vacuum(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query("VACUUM").execute(pool).await?;
    Ok(())
}

/// Reclaim a bounded number of SQLite freelist pages.
///
/// This is intended for periodic background cleanup on databases configured
/// with `auto_vacuum = INCREMENTAL`. A budget of zero does no work.
///
/// # Errors
///
/// Returns database errors from SQLite `PRAGMA incremental_vacuum`.
pub async fn incremental_vacuum(pool: &SqlitePool, pages: u32) -> Result<(), sqlx::Error> {
    if pages == 0 {
        return Ok(());
    }

    sqlx::query(AssertSqlSafe(format!("PRAGMA incremental_vacuum({pages})")))
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    //! SQLite maintenance respects the caller's page budget.

    use sqlx::sqlite::SqlitePoolOptions;

    use super::incremental_vacuum;

    #[tokio::test]
    async fn incremental_vacuum_respects_zero_and_positive_budgets() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::raw_sql(
            "PRAGMA auto_vacuum = INCREMENTAL;
             CREATE TABLE payloads (payload BLOB);
             INSERT INTO payloads VALUES (zeroblob(65536));
             DROP TABLE payloads;",
        )
        .execute(&pool)
        .await
        .unwrap();

        let freelist = || sqlx::query_scalar::<_, i64>("PRAGMA freelist_count");
        let before = freelist().fetch_one(&pool).await.unwrap();
        assert!(
            before > 1,
            "fixture needs multiple free pages; got {before}"
        );

        incremental_vacuum(&pool, 0).await.unwrap();
        assert_eq!(freelist().fetch_one(&pool).await.unwrap(), before);

        incremental_vacuum(&pool, 1).await.unwrap();
        assert_eq!(freelist().fetch_one(&pool).await.unwrap(), before - 1);
    }
}
