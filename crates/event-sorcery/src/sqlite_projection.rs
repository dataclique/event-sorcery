//! SQLite materialized-view construction, queries and transaction-bound recovery.

use cqrs_es::Aggregate;
use sqlx::SqlitePool;
use sqlx::sqlite::Sqlite;

use sqlite_es::{SqliteProjectionFilterError, SqliteProjectionRecovery, SqliteViewRepository};

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
        sqlite_es::projection_rows(source, table).await
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
        let Column(column_name) = column;
        sqlite_es::filter_projection(source, table, column_name, value)
            .await
            .map_err(|error| {
                use SqliteProjectionFilterError::{ColumnNotFound, Sql, StaleColumn};

                match error {
                    ColumnNotFound => ProjectionError::ColumnNotFound {
                        column,
                        table: table.to_string(),
                    },
                    StaleColumn { row_count } => ProjectionError::StaleColumn {
                        column,
                        table: table.to_string(),
                        row_count,
                    },
                    Sql(source) => ProjectionError::Sqlx(source),
                }
            })
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
