//! PostgreSQL persistence through the shared CQRS repository contracts.

/// Framework-owned PostgreSQL schema migrations.
///
/// Applications apply these migrations to their PostgreSQL pool before using
/// framework persistence. Existing SQLite migrations remain in `sqlite-es`.
pub static POSTGRES_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/postgres");

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use serde_json::{Value, json};
    use sqlx::PgPool;

    use super::POSTGRES_MIGRATOR;
    use crate::job::{ClaimId, DeadReason, JobEvent, JobId, JobKind, JobState, WorkerId};
    use crate::{EventSourced, Lifecycle};

    #[sqlx::test(migrations = false)]
    async fn bootstrap_preserves_job_rows_and_generated_poll_columns(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();

        let run_at = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let lease_until = DateTime::from_timestamp_millis(1_700_000_030_000).unwrap();
        let pending = JobState::originate(&JobEvent::Enqueued {
            kind: JobKind::new("bootstrap-job"),
            payload: json!({ "opaque": true }),
            run_at,
        })
        .unwrap();
        let claimed = JobState::evolve(
            &pending,
            &JobEvent::Claimed {
                worker: WorkerId::new("bootstrap-worker"),
                claim_id: ClaimId::new(),
                lease_until,
            },
        )
        .unwrap()
        .unwrap();
        let done = JobState::evolve(&claimed, &JobEvent::Succeeded)
            .unwrap()
            .unwrap();
        let dead = JobState::evolve(
            &claimed,
            &JobEvent::Dead {
                reason: DeadReason::Rejected,
                error: "rejected".to_string(),
            },
        )
        .unwrap()
        .unwrap();
        let cases = [
            (pending, "pending", Some(1_700_000_000_000_i64), 1_i64, None),
            (
                claimed,
                "claimed",
                Some(1_700_000_000_000),
                2,
                Some(1_700_000_030_000),
            ),
            (done, "done", None, 3, Some(1_700_000_030_000)),
            (dead, "dead", None, 3, Some(1_700_000_030_000)),
        ];

        for (state, expected_status, expected_run_at, expected_version, expected_lease_until) in
            cases
        {
            let view_id = JobId::new().to_string();
            let payload = serde_json::to_value(Lifecycle::Live(state)).unwrap();
            sqlx::query(
                r"
                INSERT INTO job_queue (view_id, version, payload, lease_until)
                VALUES ($1, $2, $3, $4)
                ",
            )
            .bind(&view_id)
            .bind(expected_version)
            .bind(payload.clone())
            .bind(expected_lease_until)
            .execute(&pool)
            .await
            .unwrap();

            POSTGRES_MIGRATOR.run(&pool).await.unwrap();

            let (stored_kind, status, stored_run_at, stored_version, stored_payload, stored_lease):
                (String, String, Option<i64>, i64, Value, Option<i64>) = sqlx::query_as(
                    r"
                    SELECT kind, status, run_at, version, payload, lease_until
                    FROM job_queue
                    WHERE view_id = $1
                    ",
                )
                .bind(&view_id)
                .fetch_one(&pool)
                .await
                .unwrap();

            assert_eq!(
                (stored_kind.as_str(), status.as_str(), stored_run_at),
                ("bootstrap-job", expected_status, expected_run_at),
            );
            assert_eq!(stored_version, expected_version);
            assert_eq!(stored_payload, payload);
            assert_eq!(stored_lease, expected_lease_until);
        }
    }

    #[sqlx::test(migrations = false)]
    async fn queue_rejects_nonpositive_projection_versions(pool: PgPool) {
        POSTGRES_MIGRATOR.run(&pool).await.unwrap();

        for version in [0_i64, -1] {
            let view_id = JobId::new().to_string();
            let payload = serde_json::to_value(Lifecycle::Live(JobState::Pending {
                kind: JobKind::new("bootstrap-job"),
                payload: json!({ "opaque": true }),
                run_at: DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                attempt: 0,
                claims: 0,
            }))
            .unwrap();
            let error = sqlx::query(
                "INSERT INTO job_queue (view_id, version, payload) VALUES ($1, $2, $3)",
            )
            .bind(&view_id)
            .bind(version)
            .bind(payload)
            .execute(&pool)
            .await
            .unwrap_err();

            let sqlx::Error::Database(error) = error else {
                panic!("expected a database constraint failure");
            };
            assert_eq!(error.code().as_deref(), Some("23514"));
        }

        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_queue")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(rows, 0);
    }
}
