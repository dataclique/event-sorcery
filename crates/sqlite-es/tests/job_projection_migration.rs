//! Migration coverage for durable job projections.

use serde_json::json;
use sqlx::migrate::Migrator;
use sqlx::sqlite::SqlitePoolOptions;

use sqlite_es::MIGRATOR;

#[tokio::test]
async fn queue_projection_migration_preserves_claimed_job() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(":memory:")
        .await
        .unwrap();
    let previous = Migrator::with_migrations(
        MIGRATOR
            .iter()
            .filter(|migration| migration.version < 20_261_112_224_645)
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
        "INSERT INTO job_queue (view_id, version, payload, lease_until) VALUES (?1, 2, ?2, ?3)",
    )
    .bind("01ARZ3NDEKTSV4RRFFQ69G5FAW")
    .bind(payload.to_string())
    .bind(1_700_000_030_000_i64)
    .execute(&pool)
    .await
    .unwrap();

    MIGRATOR.run(&pool).await.unwrap();
    let (version, stored, lease, kind, status, run_at): (
        i64,
        String,
        Option<i64>,
        String,
        String,
        i64,
    ) = sqlx::query_as(
        "SELECT version, payload, lease_until, kind, status, run_at \
         FROM job_queue_projection WHERE view_id = ?1",
    )
    .bind("01ARZ3NDEKTSV4RRFFQ69G5FAW")
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(version, 2);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&stored).unwrap(),
        payload
    );
    assert_eq!(lease, Some(1_700_000_030_000_i64));
    assert_eq!(kind, "migration-probe");
    assert_eq!(status, "claimed");
    assert_eq!(run_at, 1_700_000_000_000_i64);
}
