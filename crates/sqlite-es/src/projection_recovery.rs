//! SQLite storage operations for projection recovery.

use futures_util::future::BoxFuture;
use sqlx::{AssertSqlSafe, SqliteConnection, SqlitePool};
use std::num::NonZeroU64;

use event_sorcery_core::{
    ProjectionRecoveryMode, ProjectionRecoveryStorage, ProjectionReplayRow, SnapshotHistory, Table,
};

/// SQLite projection recovery over a borrowed connection.
pub struct SqliteProjectionRecovery<'connection> {
    /// Connection borrowed from the caller-owned recovery transaction.
    pub connection: &'connection mut SqliteConnection,
}

/// Runs a replay and publishes its writes in one SQLite transaction.
pub async fn recover_projection<Error>(
    pool: &SqlitePool,
    mode: ProjectionRecoveryMode,
    replay: impl for<'connection> FnOnce(
        SqliteProjectionRecovery<'connection>,
        ProjectionRecoveryMode,
    ) -> BoxFuture<'connection, Result<(), Error>>
    + Send,
) -> Result<(), Error>
where
    Error: From<sqlx::Error> + Send,
{
    // Reserve the writer before taking a snapshot that recovery will write from.
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    let storage = SqliteProjectionRecovery {
        connection: &mut transaction,
    };
    replay(storage, mode).await?;
    transaction.commit().await?;
    Ok(())
}

impl ProjectionRecoveryStorage for SqliteProjectionRecovery<'_> {
    async fn snapshot_histories(
        &mut self,
        aggregate_type: &str,
        aggregate_id: Option<&str>,
    ) -> Result<Vec<SnapshotHistory>, sqlx::Error> {
        crate::snapshot::snapshot_histories(self.connection, aggregate_type, aggregate_id).await
    }

    async fn reset(
        &mut self,
        Table(table): Table,
        mode: &ProjectionRecoveryMode,
    ) -> Result<(), sqlx::Error> {
        use ProjectionRecoveryMode::{CatchUp, CatchUpOne, RebuildAll, RebuildOne};

        match mode {
            CatchUp | CatchUpOne { .. } => {}
            RebuildAll => {
                sqlx::query(AssertSqlSafe(format!("DELETE FROM {table}")))
                    .execute(&mut *self.connection)
                    .await?;
            }
            RebuildOne { aggregate_id } => {
                sqlx::query(AssertSqlSafe(format!(
                    "DELETE FROM {table} WHERE view_id = ?1"
                )))
                .bind(aggregate_id)
                .execute(&mut *self.connection)
                .await?;
            }
        }
        Ok(())
    }

    async fn streams(
        &mut self,
        Table(table): Table,
        aggregate_type: &str,
        mode: &ProjectionRecoveryMode,
    ) -> Result<Vec<ProjectionReplayRow>, sqlx::Error> {
        use ProjectionRecoveryMode::{CatchUp, CatchUpOne, RebuildAll, RebuildOne};

        let selection = match mode {
            CatchUp => "v.version IS NULL OR e.max_seq > v.version",
            CatchUpOne { .. } => {
                "e.aggregate_id = ?2 AND (v.version IS NULL OR e.max_seq > v.version)"
            }
            RebuildAll => "1 = 1",
            RebuildOne { .. } => "e.aggregate_id = ?2",
        };
        let sql = format!(
            "SELECT e.aggregate_id, v.version, v.payload, e.max_seq, e.snapshot_seq
             FROM (
                SELECT aggregate_id, MAX(sequence) AS max_seq, MAX(snapshot_seq) AS snapshot_seq
                FROM (
                    SELECT aggregate_id, sequence, NULL AS snapshot_seq FROM events
                    WHERE aggregate_type = ?1
                    UNION ALL
                    SELECT aggregate_id, last_sequence, last_sequence FROM snapshots
                    WHERE aggregate_type = ?1 AND last_sequence > 0
                ) history GROUP BY aggregate_id
             ) e LEFT JOIN {table} v ON v.view_id = e.aggregate_id
             WHERE {selection} ORDER BY e.aggregate_id"
        );
        let mut query =
            sqlx::query_as::<_, (String, Option<i64>, Option<String>, i64, Option<i64>)>(
                AssertSqlSafe(sql),
            )
            .bind(aggregate_type);
        if let CatchUpOne { aggregate_id } | RebuildOne { aggregate_id } = mode {
            query = query.bind(aggregate_id);
        }
        query
            .fetch_all(&mut *self.connection)
            .await?
            .into_iter()
            .map(ProjectionReplayRow::try_from)
            .collect()
    }

    async fn events(
        &mut self,
        aggregate_type: &str,
        aggregate_id: &str,
        after: u64,
        through: NonZeroU64,
    ) -> Result<Vec<(i64, String)>, sqlx::Error> {
        let after = i64::try_from(after).map_err(|source| sqlx::Error::Encode(Box::new(source)))?;
        let through =
            i64::try_from(through.get()).map_err(|source| sqlx::Error::Encode(Box::new(source)))?;
        sqlx::query_as(
            "SELECT sequence, payload FROM events WHERE aggregate_type = ?1 AND aggregate_id = ?2
             AND sequence > ?3 AND sequence <= ?4 ORDER BY sequence",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(after)
        .bind(through)
        .fetch_all(&mut *self.connection)
        .await
    }

    async fn save(
        &mut self,
        Table(table): Table,
        aggregate_id: &str,
        sequence: NonZeroU64,
        payload: &str,
    ) -> Result<(), sqlx::Error> {
        let sequence = i64::try_from(sequence.get())
            .map_err(|source| sqlx::Error::Encode(Box::new(source)))?;
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {table} (view_id, version, payload) VALUES (?1, ?2, ?3)
             ON CONFLICT(view_id) DO UPDATE SET version = ?2, payload = ?3"
        )))
        .bind(aggregate_id)
        .bind(sequence)
        .bind(payload)
        .execute(&mut *self.connection)
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use sqlx::Connection;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn recovery_reserves_the_writer_before_reading() {
        let options = "sqlite::memory:"
            .parse::<SqliteConnectOptions>()
            .unwrap()
            .busy_timeout(Duration::ZERO);
        let pool = SqlitePoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await
            .unwrap();
        let mut competitor = pool.acquire().await.unwrap();
        let (started, wait_for_start) = tokio::sync::oneshot::channel();
        let (finished, mut wait_for_finish) = tokio::sync::oneshot::channel();
        let writer = tokio::spawn(async move {
            wait_for_start.await.unwrap();
            let transaction = competitor.begin_with("BEGIN IMMEDIATE").await.unwrap();
            let _ = finished.send(());
            transaction.rollback().await.unwrap();
        });

        recover_projection::<sqlx::Error>(&pool, ProjectionRecoveryMode::CatchUp, |_, _| {
            Box::pin(async move {
                started.send(()).unwrap();
                let pending_writer =
                    tokio::time::timeout(Duration::from_millis(200), &mut wait_for_finish).await;
                pending_writer.expect_err("competing writer must wait for recovery to finish");
                Ok(())
            })
        })
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(5), writer)
            .await
            .unwrap()
            .unwrap();
    }
}
