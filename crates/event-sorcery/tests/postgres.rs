//! Real PostgreSQL coverage for the shared persistence contract.

#![cfg(feature = "postgres")]

use sqlx::PgPool;

use event_sorcery::POSTGRES_MIGRATOR;

#[sqlx::test(migrations = false)]
async fn bootstrap_initializes_framework_storage(pool: PgPool) {
    POSTGRES_MIGRATOR.run(&pool).await.unwrap();

    let tables: Vec<String> = sqlx::query_scalar(
        r"
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename IN ('events', 'snapshots', 'job_queue')
        ORDER BY tablename
        ",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(tables, ["events", "job_queue", "snapshots"]);
}
