//! SQLite materialized-view construction, queries and transaction-bound recovery.

use cqrs_es::Aggregate;
use sqlx::sqlite::Sqlite;
use sqlx::{AssertSqlSafe, SqlitePool};
use tracing::warn;

use sqlite_es::{SqliteProjectionRecovery, SqliteViewRepository};

use crate::{Column, EventSourced, Projection, ProjectionError, Table, ViewBackend};

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
            let mut storage = SqliteProjectionRecovery {
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
