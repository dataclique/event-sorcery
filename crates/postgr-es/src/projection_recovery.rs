//! PostgreSQL I/O for the shared materialized-view replay algorithm.

use sqlx::{AssertSqlSafe, PgConnection};
use std::num::NonZeroU64;

use event_sorcery::{
    ProjectionRecoveryMode, ProjectionRecoveryStorage, ProjectionReplayRow, SnapshotHistory, Table,
};
use indexed_query::quote_identifier;

pub(crate) struct PostgresProjectionRecovery<'connection> {
    /// Connection borrowed from the transaction that also owns schema registration.
    pub(crate) connection: &'connection mut PgConnection,
}

pub(crate) async fn snapshot_histories(
    connection: &mut PgConnection,
    aggregate_type: &str,
    aggregate_id: Option<&str>,
) -> Result<Vec<SnapshotHistory>, sqlx::Error> {
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(
        "SELECT s.aggregate_id, s.last_sequence,
                (SELECT COUNT(*) FROM events e
                 WHERE e.aggregate_type = s.aggregate_type
                   AND e.aggregate_id = s.aggregate_id
                   AND e.sequence BETWEEN 1 AND s.last_sequence)
         FROM snapshots s
         WHERE s.aggregate_type = $1 AND ($2::text IS NULL OR s.aggregate_id = $2)",
    )
    .bind(aggregate_type)
    .bind(aggregate_id)
    .fetch_all(connection)
    .await?;
    Ok(rows.into_iter().map(SnapshotHistory::from).collect())
}

impl ProjectionRecoveryStorage for PostgresProjectionRecovery<'_> {
    async fn snapshot_histories(
        &mut self,
        aggregate_type: &str,
        aggregate_id: Option<&str>,
    ) -> Result<Vec<SnapshotHistory>, sqlx::Error> {
        snapshot_histories(self.connection, aggregate_type, aggregate_id).await
    }

    async fn reset(
        &mut self,
        Table(table): Table,
        mode: &ProjectionRecoveryMode,
    ) -> Result<(), sqlx::Error> {
        use ProjectionRecoveryMode::{CatchUp, CatchUpOne, RebuildAll, RebuildOne};

        let table = quote_identifier(table);
        sqlx::query(AssertSqlSafe(format!(
            "LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE"
        )))
        .execute(&mut *self.connection)
        .await?;
        match mode {
            CatchUp | CatchUpOne { .. } => {}
            RebuildAll => {
                sqlx::query(AssertSqlSafe(format!("DELETE FROM {table}")))
                    .execute(&mut *self.connection)
                    .await?;
            }
            RebuildOne { aggregate_id } => {
                sqlx::query(AssertSqlSafe(format!(
                    "DELETE FROM {table} WHERE view_id = $1"
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

        let table = quote_identifier(table);
        let selection = match mode {
            CatchUp => "v.version IS NULL OR e.max_seq > v.version",
            CatchUpOne { .. } => {
                "e.aggregate_id = $2 AND (v.version IS NULL OR e.max_seq > v.version)"
            }
            RebuildAll => "TRUE",
            RebuildOne { .. } => "e.aggregate_id = $2",
        };
        let sql = format!(
            "SELECT e.aggregate_id, v.version, v.payload::text, e.max_seq, e.snapshot_seq
             FROM (
                SELECT aggregate_id, MAX(sequence) AS max_seq, MAX(snapshot_seq) AS snapshot_seq
                FROM (
                    SELECT aggregate_id, sequence, NULL AS snapshot_seq FROM events
                    WHERE aggregate_type = $1
                    UNION ALL
                    SELECT aggregate_id, last_sequence, last_sequence FROM snapshots
                    WHERE aggregate_type = $1 AND last_sequence > 0
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
            "SELECT sequence, payload::text FROM events WHERE aggregate_type = $1 AND aggregate_id = $2
             AND sequence > $3 AND sequence <= $4 ORDER BY sequence",
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
        let table = quote_identifier(table);
        let sequence = i64::try_from(sequence.get())
            .map_err(|source| sqlx::Error::Encode(Box::new(source)))?;
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO {table} (view_id, version, payload) VALUES ($1, $2, $3::jsonb)
             ON CONFLICT(view_id) DO UPDATE SET version = $2, payload = $3::jsonb"
        )))
        .bind(aggregate_id)
        .bind(sequence)
        .bind(payload)
        .execute(&mut *self.connection)
        .await?;
        Ok(())
    }
}
