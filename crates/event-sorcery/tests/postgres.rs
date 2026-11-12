//! Real PostgreSQL coverage for the shared persistence contract.

#![cfg(feature = "postgres")]

use serde_json::json;
use sqlx::PgPool;
use sqlx::migrate::{MigrateError, Migrator};

use event_sorcery::POSTGRES_MIGRATOR;

#[sqlx::test(migrations = false)]
async fn bootstrap_initializes_framework_storage(pool: PgPool) {
    POSTGRES_MIGRATOR.run(&pool).await.unwrap();

    let tables: Vec<String> = sqlx::query_scalar(
        r"
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename IN ('events', 'snapshots', 'job_queue_projection')
        ORDER BY tablename
        ",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(tables, ["events", "job_queue_projection", "snapshots"]);
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
          AND tablename IN ('events', 'snapshots', 'job_queue_projection')
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
          AND tablename IN ('events', 'snapshots', 'job_queue_projection')
        ",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(tables, 3);
}

#[sqlx::test(migrations = false)]
async fn queue_projection_migration_preserves_claimed_job(pool: PgPool) {
    let previous = Migrator::with_migrations(
        POSTGRES_MIGRATOR
            .iter()
            .filter(|migration| migration.version < 20_261_112_224_646)
            .cloned()
            .collect(),
    );
    previous.run(&pool).await.unwrap();

    let payload = json!({"Live": {"Claimed": {
        "kind": "migration-probe", "payload": {"value": "retained"},
        "worker": "migration-worker", "claim_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "run_at": 1_700_000_000_000_i64, "attempt": 0, "claims": 1,
    }}});
    sqlx::query(
        "INSERT INTO job_queue (view_id, version, payload, lease_until) VALUES ($1, 2, $2, $3)",
    )
    .bind("01ARZ3NDEKTSV4RRFFQ69G5FAW")
    .bind(&payload)
    .bind(1_700_000_030_000_i64)
    .execute(&pool)
    .await
    .unwrap();

    POSTGRES_MIGRATOR.run(&pool).await.unwrap();
    let (version, stored, lease, kind, status, run_at): (
        i64,
        serde_json::Value,
        Option<i64>,
        String,
        String,
        i64,
    ) = sqlx::query_as(
        "SELECT version, payload, lease_until, kind, status, run_at \
         FROM job_queue_projection WHERE view_id = $1",
    )
    .bind("01ARZ3NDEKTSV4RRFFQ69G5FAW")
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(version, 2);
    assert_eq!(stored, payload);
    assert_eq!(lease, Some(1_700_000_030_000_i64));
    assert_eq!(kind, "migration-probe");
    assert_eq!(status, "claimed");
    assert_eq!(run_at, 1_700_000_000_000_i64);
}
