//! Durable terminal-job projection metadata survives schema migration.

use serde_json::json;
use sqlx::migrate::Migrator;
use sqlx::sqlite::SqlitePoolOptions;

use sqlite_es::MIGRATOR;

#[tokio::test]
async fn terminal_job_migration_preserves_rows_and_recomputes_generated_fields() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(":memory:")
        .await
        .unwrap();
    let previous = Migrator::with_migrations(
        MIGRATOR
            .iter()
            .filter(|migration| migration.version < 20_261_115_095_705)
            .cloned()
            .collect(),
    );
    previous.run(&pool).await.unwrap();
    sqlx::query(
        "ALTER TABLE job_queue_projection ADD COLUMN operator_note TEXT DEFAULT 'retained'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let jobs = [
        (
            "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            json!({"Live": {"Done": {
                "kind": "migration-probe"
            }}}),
            "done",
        ),
        (
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            json!({"Live": {"Dead": {
                "kind": "migration-probe", "reason": "Abandoned"
            }}}),
            "dead",
        ),
    ];
    for (view_id, payload, _) in &jobs {
        sqlx::query(
            "INSERT INTO job_queue_projection (view_id, version, payload, lease_until) \
             VALUES (?1, 7, ?2, ?3)",
        )
        .bind(view_id)
        .bind(payload.to_string())
        .bind(1_700_000_030_000_i64)
        .execute(&pool)
        .await
        .unwrap();
    }

    MIGRATOR.run(&pool).await.unwrap();
    let notes: Vec<String> =
        sqlx::query_scalar("SELECT operator_note FROM job_queue_projection ORDER BY view_id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(notes, ["retained", "retained"]);

    for (view_id, payload, expected_status) in &jobs {
        let (version, stored, lease, kind, status, run_at): (
            i64,
            String,
            Option<i64>,
            Option<String>,
            Option<String>,
            Option<i64>,
        ) = sqlx::query_as(
            "SELECT version, payload, lease_until, kind, status, run_at \
             FROM job_queue_projection WHERE view_id = ?1",
        )
        .bind(view_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(version, 7);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&stored).unwrap(),
            *payload
        );
        assert_eq!(lease, Some(1_700_000_030_000));
        assert_eq!(kind.as_deref(), Some("migration-probe"));
        assert_eq!(status.as_deref(), Some(*expected_status));
        assert_eq!(run_at, None);
    }
}
