//! SQLite transactions for the durable job queue.

use cqrs_es::persist::SerializedEvent;
use sqlx::{SqliteConnection, SqlitePool};

use event_sorcery_core::{ClaimDecision, ClaimOutcome, ClaimRead, LeaseRenewal};

use crate::{SqliteAggregateError, insert_serialized_events_batch};

/// Reads the queue row under a write lock and applies the caller's claim decision.
pub async fn claim_job<Decide, Won>(
    pool: &SqlitePool,
    job_id: &str,
    decide: Decide,
) -> Result<ClaimOutcome<Won>, SqliteJobError>
where
    Decide: FnOnce(Option<ClaimRead>) -> ClaimDecision<Won> + Send,
    Won: Send,
{
    immediate_transaction(pool, job_id, async move |connection| {
        let outcome = claim_job_in_transaction(connection, job_id, decide).await?;
        Ok(match outcome {
            ClaimOutcome::Won(won) => TransactionOutcome::Commit(ClaimOutcome::Won(won)),
            ClaimOutcome::Abandoned => TransactionOutcome::Commit(ClaimOutcome::Abandoned),
            ClaimOutcome::Contended => TransactionOutcome::Rollback(ClaimOutcome::Contended),
            ClaimOutcome::Skip => TransactionOutcome::Rollback(ClaimOutcome::Skip),
        })
    })
    .await
}

/// Extends the lease in epoch milliseconds only if the claimed version still matches.
pub async fn renew_job(
    pool: &SqlitePool,
    job_id: &str,
    claim_seq: i64,
    new_lease_until_ms: i64,
) -> Result<LeaseRenewal, SqliteJobError> {
    let done = sqlx::query!(
        r#"
            UPDATE job_queue_projection
            SET lease_until = ?1
            WHERE view_id = ?2
              AND version = ?3
              AND status = 'claimed'
            "#,
        new_lease_until_ms,
        job_id,
        claim_seq,
    )
    .execute(pool)
    .await?;

    if done.rows_affected() == 0 {
        Ok(LeaseRenewal::Lost)
    } else {
        Ok(LeaseRenewal::Held)
    }
}

/// Appends a standalone enqueue event and seeds its queue row atomically.
pub async fn enqueue_job(
    pool: &SqlitePool,
    event: SerializedEvent,
    payload: String,
) -> Result<(), SqliteJobError> {
    let job_id = event.aggregate_id.clone();
    let mut transaction = pool.begin().await?;

    let seeded = match append_job_event(&mut transaction, &event).await? {
        Some(version) => {
            write_job_projection(&mut transaction, &job_id, version, &payload, None).await
        }
        None => Err(SqliteJobError::DuplicateEnqueue { job_id }),
    };

    match seeded {
        Ok(()) => {
            transaction.commit().await?;
            Ok(())
        }
        Err(error) => {
            transaction.rollback().await?;
            Err(error)
        }
    }
}

/// Backend-native error for the SQLite job store.
#[derive(Debug, thiserror::Error)]
pub enum SqliteJobError {
    /// A direct SQL statement failed.
    #[error("job store SQL error")]
    Sql(#[from] sqlx::Error),
    /// A compare-and-swap event append failed (other than a conflict, which is
    /// reported as [`ClaimOutcome::Contended`]).
    #[error("job event append failed")]
    Append(#[from] SqliteAggregateError),
    /// A sequence value exceeded the storable range.
    #[error("job sequence out of range")]
    Sequence(#[from] std::num::TryFromIntError),
    /// A standalone enqueue hit an existing stream for
    /// its `view_id` -- the caller reused a job id (a freshly minted id never
    /// collides).
    #[error("duplicate enqueue: job id {job_id} already has an event stream")]
    DuplicateEnqueue {
        /// The reused job id.
        job_id: String,
    },
}

enum TransactionOutcome<Output> {
    Commit(Output),
    Rollback(Output),
}

async fn immediate_transaction<Output, Operation>(
    pool: &SqlitePool,
    job_id: &str,
    operation: Operation,
) -> Result<Output, SqliteJobError>
where
    Operation:
        AsyncFnOnce(&mut SqliteConnection) -> Result<TransactionOutcome<Output>, SqliteJobError>,
{
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    let outcome = operation(&mut transaction).await;

    match outcome {
        Ok(TransactionOutcome::Commit(output)) => {
            transaction.commit().await?;
            Ok(output)
        }
        Ok(TransactionOutcome::Rollback(output)) => {
            transaction.rollback().await?;
            Ok(output)
        }
        Err(claim_error) => {
            if let Err(error) = transaction.rollback().await {
                tracing::warn!(?error, job_id, "job claim rollback failed");
            }
            Err(claim_error)
        }
    }
}

async fn claim_job_in_transaction<Decide, Won>(
    connection: &mut SqliteConnection,
    job_id: &str,
    decide: Decide,
) -> Result<ClaimOutcome<Won>, SqliteJobError>
where
    Decide: FnOnce(Option<ClaimRead>) -> ClaimDecision<Won>,
{
    let read = read_job_projection_for_claim(connection, job_id).await?;
    match decide(read) {
        ClaimDecision::Skip => Ok(ClaimOutcome::Skip),
        ClaimDecision::Claim {
            event,
            payload,
            lease_until_ms,
            won,
        } => match append_job_event(connection, &event).await? {
            Some(version) => {
                write_job_projection(connection, job_id, version, &payload, Some(lease_until_ms))
                    .await?;
                Ok(ClaimOutcome::Won(won))
            }
            None => Ok(ClaimOutcome::Contended),
        },
        ClaimDecision::Abandon { event, payload } => {
            match append_job_event(connection, &event).await? {
                Some(version) => {
                    write_job_projection(connection, job_id, version, &payload, None).await?;
                    Ok(ClaimOutcome::Abandoned)
                }
                None => Ok(ClaimOutcome::Contended),
            }
        }
    }
}

async fn append_job_event(
    connection: &mut SqliteConnection,
    event: &SerializedEvent,
) -> Result<Option<i64>, SqliteJobError> {
    match insert_serialized_events_batch(connection, "events", std::slice::from_ref(event)).await {
        Ok(()) => Ok(Some(i64::try_from(event.sequence)?)),
        Err(SqliteAggregateError::OptimisticLock) => Ok(None),
        Err(other) => Err(SqliteJobError::Append(other)),
    }
}

async fn read_job_projection_for_claim(
    connection: &mut SqliteConnection,
    job_id: &str,
) -> Result<Option<ClaimRead>, SqliteJobError> {
    Ok(sqlx::query_as!(
        ClaimRead,
        r#"
        SELECT version,
               payload,
               lease_until AS "lease_until_ms"
        FROM job_queue_projection
        WHERE view_id = ?1
        "#,
        job_id,
    )
    .fetch_optional(connection)
    .await?)
}

async fn write_job_projection(
    connection: &mut SqliteConnection,
    job_id: &str,
    version: i64,
    payload: &str,
    lease_until_ms: Option<i64>,
) -> Result<(), SqliteJobError> {
    sqlx::query!(
        r#"
        INSERT INTO job_queue_projection (
            view_id,
            version,
            payload,
            lease_until
        )
        VALUES (?1, ?2, ?3, ?4)
        ON CONFLICT(view_id) DO UPDATE SET
            version = excluded.version,
            payload = excluded.payload,
            lease_until = excluded.lease_until
        "#,
        job_id,
        version,
        payload,
        lease_until_ms,
    )
    .execute(connection)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Notify;

    use super::{SqliteJobError, TransactionOutcome, immediate_transaction};
    use crate::testing::create_test_pool;

    #[tokio::test]
    async fn cancelled_immediate_transaction_does_not_poison_the_pooled_connection() {
        let pool = create_test_pool().await.unwrap();
        let operation_started = Arc::new(Notify::new());
        let task_pool = pool.clone();
        let task_started = Arc::clone(&operation_started);

        let claim = tokio::spawn(async move {
            immediate_transaction(&task_pool, "cancelled-job", async move |_connection| {
                task_started.notify_one();
                pending::<Result<TransactionOutcome<()>, SqliteJobError>>().await
            })
            .await
        });

        tokio::time::timeout(Duration::from_secs(1), operation_started.notified())
            .await
            .expect("claim entered its transaction");
        claim.abort();
        assert!(claim.await.expect_err("claim was cancelled").is_cancelled());

        let mut connection = pool.acquire().await.expect("reacquire connection");
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await
            .expect("cancelled transaction was rolled back before pool reuse");
        sqlx::query("ROLLBACK")
            .execute(&mut *connection)
            .await
            .expect("close verification transaction");
    }

    #[tokio::test]
    async fn rollback_failure_is_returned_to_the_caller() {
        let pool = create_test_pool().await.unwrap();

        let result = immediate_transaction(&pool, "rollback-failure", async |connection| {
            sqlx::query("ROLLBACK").execute(connection).await?;

            Ok(TransactionOutcome::Rollback(()))
        })
        .await;

        assert!(matches!(result, Err(SqliteJobError::Sql(_))));
    }
}
