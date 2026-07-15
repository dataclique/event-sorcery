use sqlx::SqlitePool;

#[tokio::test]
async fn sqlite_ddl_creates_the_complete_engine_schema() -> Result<(), Box<dyn std::error::Error>> {
    let pool = SqlitePool::connect(":memory:").await?;
    sqlx::migrate!("../../ddl/sqlite").run(&pool).await?;

    for table in [
        "events",
        "jobs",
        "outbox",
        "delivery_receipts",
        "reactors",
        "projections",
        "snapshots",
        "schemas",
    ] {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(&pool)
        .await?;
        assert_eq!(count, 1, "missing engine table {table}");
    }

    Ok(())
}
