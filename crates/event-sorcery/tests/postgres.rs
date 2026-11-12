//! Real PostgreSQL coverage for the shared persistence contract.

#![cfg(feature = "postgres")]

use sqlx::PgPool;
use sqlx::migrate::MigrateError;

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

#[sqlx::test(migrations = false)]
async fn bootstrap_failure_rolls_back_and_can_be_retried(pool: PgPool) {
    sqlx::query("CREATE TABLE snapshots (marker TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO snapshots (marker) VALUES ('keep')")
        .execute(&pool)
        .await
        .unwrap();

    let error = POSTGRES_MIGRATOR.run(&pool).await.unwrap_err();
    let MigrateError::ExecuteMigration(source, version) = error else {
        panic!("expected the bootstrap migration to fail");
    };
    assert_eq!(version, POSTGRES_MIGRATOR.iter().next().unwrap().version);
    let sqlx::Error::Database(source) = source else {
        panic!("expected a database error for the conflicting table");
    };
    assert_eq!(source.code().as_deref(), Some("42P07"));

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
    assert_eq!(tables, ["snapshots"]);

    let marker: String = sqlx::query_scalar("SELECT marker FROM snapshots")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(marker, "keep");

    sqlx::query("DROP TABLE snapshots")
        .execute(&pool)
        .await
        .unwrap();
    POSTGRES_MIGRATOR.run(&pool).await.unwrap();

    let tables: i64 = sqlx::query_scalar(
        r"
        SELECT COUNT(*)
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename IN ('events', 'snapshots', 'job_queue')
        ",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(tables, 3);
}
