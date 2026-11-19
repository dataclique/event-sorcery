//! SQLite materialized-view construction, queries and transaction-bound recovery.

use cqrs_es::Aggregate;
use sqlx::sqlite::Sqlite;
use sqlx::{AssertSqlSafe, SqliteConnection, SqlitePool};
use std::num::NonZeroU64;
use tracing::warn;

use sqlite_es::SqliteViewRepository;

use crate::{
    Column, EventSourced, Projection, ProjectionError, ProjectionRecoveryMode,
    ProjectionRecoveryStorage, ProjectionReplayRow, Table, ViewBackend,
};

/// SQLite repository family for aggregate-state materialized views.
#[derive(Debug, Clone, Copy)]
pub struct SqliteViewBackend;

impl ViewBackend for SqliteViewBackend {
    type Repo<View, Agg>
        = SqliteViewRepository<View, Agg>
    where
        View: cqrs_es::View<Agg> + Clone + 'static,
        Agg: Aggregate + 'static;

    fn is_retryable_error(error: &(dyn std::error::Error + 'static)) -> bool {
        crate::is_retryable_sqlite_busy(error)
    }
}

impl<Entity: EventSourced<Materialized = Table>> Projection<Entity, SqliteViewBackend> {
    /// Constructs the SQLite materialized view using the caller-owned pool.
    pub fn sqlite(pool: SqlitePool) -> Self {
        Self::with_backend(pool)
    }
}

pub(crate) struct SqliteProjectionRecovery<'connection> {
    /// Connection borrowed from the caller-owned recovery transaction.
    pub(crate) connection: &'connection mut SqliteConnection,
}

impl ProjectionRecoveryStorage for SqliteProjectionRecovery<'_> {
    async fn reset(
        &mut self,
        Table(table): Table,
        mode: &ProjectionRecoveryMode,
    ) -> Result<(), sqlx::Error> {
        use ProjectionRecoveryMode::{CatchUp, RebuildAll, RebuildOne};

        match mode {
            CatchUp => {}
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
        use ProjectionRecoveryMode::{CatchUp, RebuildAll, RebuildOne};

        let selection = match mode {
            CatchUp => "v.version IS NULL OR e.max_seq > v.version",
            RebuildAll => "1 = 1",
            RebuildOne { .. } => "e.aggregate_id = ?2",
        };
        let sql = format!(
            "SELECT e.aggregate_id, v.version, v.payload, e.max_seq FROM (
                SELECT aggregate_id, MAX(sequence) AS max_seq FROM events
                WHERE aggregate_type = ?1 GROUP BY aggregate_id
             ) e LEFT JOIN {table} v ON v.view_id = e.aggregate_id
             WHERE {selection} ORDER BY e.aggregate_id"
        );
        let mut query =
            sqlx::query_as::<_, (String, Option<i64>, Option<String>, i64)>(AssertSqlSafe(sql))
                .bind(aggregate_type);
        if let RebuildOne { aggregate_id } = mode {
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
    ) -> Result<Vec<String>, sqlx::Error> {
        let after = i64::try_from(after).map_err(|source| sqlx::Error::Encode(Box::new(source)))?;
        let through =
            i64::try_from(through.get()).map_err(|source| sqlx::Error::Encode(Box::new(source)))?;
        sqlx::query_scalar(
            "SELECT payload FROM events WHERE aggregate_type = ?1 AND aggregate_id = ?2
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

impl crate::NativeProjectionBackend for SqliteViewBackend {
    type Source = SqlitePool;
    type Database = Sqlite;

    fn repository<ViewState, AggregateType>(
        source: SqlitePool,
        Table(table): Table,
    ) -> Self::Repo<ViewState, AggregateType>
    where
        ViewState: cqrs_es::View<AggregateType> + Clone + 'static,
        AggregateType: Aggregate + 'static,
    {
        SqliteViewRepository::new(source, table.to_string())
    }

    fn source<ViewState, AggregateType>(
        repository: &Self::Repo<ViewState, AggregateType>,
    ) -> &SqlitePool
    where
        ViewState: cqrs_es::View<AggregateType> + Clone + 'static,
        AggregateType: Aggregate + 'static,
    {
        repository.as_ref()
    }

    async fn rows(
        source: &SqlitePool,
        Table(table): Table,
    ) -> Result<Vec<(String, String)>, sqlx::Error> {
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT view_id, payload FROM {table}"
        )))
        .fetch_all(source)
        .await
    }

    async fn filter<Entity, Value>(
        source: &SqlitePool,
        Table(table): Table,
        column: Column,
        value: &Value,
    ) -> Result<Vec<(String, String)>, ProjectionError<Entity>>
    where
        Entity: EventSourced,
        for<'query> &'query Value: sqlx::Encode<'query, Sqlite>,
        Value: sqlx::Type<Sqlite> + Send + Sync,
    {
        validate_column::<Entity>(source, table, &column).await?;
        let Column(column) = column;
        Ok(sqlx::query_as(AssertSqlSafe(format!(
            "SELECT view_id, payload FROM {table} WHERE {column} = ?1 ORDER BY view_id"
        )))
        .bind(value)
        .fetch_all(source)
        .await?)
    }

    async fn recover<Entity>(
        source: &SqlitePool,
        mode: &crate::ProjectionRecoveryMode,
    ) -> Result<(), ProjectionError<Entity>>
    where
        Entity: EventSourced<Materialized = Table>,
    {
        let mut transaction = source.begin().await?;
        {
            let mut storage = crate::sqlite_projection::SqliteProjectionRecovery {
                connection: &mut transaction,
            };
            crate::recover_projection::<Entity>(&mut storage, mode).await?;
        }
        transaction.commit().await?;
        Ok(())
    }
}

/// Validates that a column exists in the table schema and returns
/// an error if all values are NULL (indicates a stale generated
/// column).
async fn validate_column<Entity: EventSourced>(
    pool: &SqlitePool,
    table: &str,
    column: &Column,
) -> Result<(), ProjectionError<Entity>> {
    let column_name = column.0;

    let columns: Vec<(String,)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT name FROM pragma_table_xinfo('{table}')"
    )))
    .fetch_all(pool)
    .await?;

    if !columns.iter().any(|(name,)| name == column_name) {
        warn!(
            target: "cqrs",
            %column_name, %table,
            "Column does not exist in table schema"
        );

        return Err(ProjectionError::ColumnNotFound {
            column: column.clone(),
            table: table.to_string(),
        });
    }

    let row_count: (i64,) = sqlx::query_as(AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
        .fetch_one(pool)
        .await?;

    if row_count.0 > 0 {
        let non_null_count: (i64,) = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM {table}
             WHERE {column_name} IS NOT NULL"
        )))
        .fetch_one(pool)
        .await?;

        if non_null_count.0 == 0 {
            warn!(
                target: "cqrs",
                %column_name, %table, row_count = %row_count.0,
                "Generated column has all NULL values, likely stale JSON path"
            );

            return Err(ProjectionError::StaleColumn {
                column: column.clone(),
                table: table.to_string(),
                row_count: row_count.0,
            });
        }
    }

    Ok(())
}
