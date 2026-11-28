//! SQLite native view integration.
use cqrs_es::Aggregate;
use sqlx::{Sqlite, SqlitePool};

use event_sorcery::{
    Column, EventSourced, NativeProjectionBackend, ProjectionError, ProjectionRecoveryMode, Table,
};

use crate::{SqliteProjectionFilterError, SqliteViewBackend, SqliteViewRepository};

impl NativeProjectionBackend for SqliteViewBackend {
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
        crate::projection_rows(source, table).await
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
        crate::filter_projection(source, table, column_name, value)
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
        mode: &ProjectionRecoveryMode,
    ) -> Result<(), ProjectionError<Entity>>
    where
        Entity: EventSourced<Materialized = Table>,
    {
        crate::recover_projection(source, mode.clone(), |mut storage, mode| {
            Box::pin(async move {
                event_sorcery::recover_projection::<Entity>(&mut storage, &mode).await
            })
        })
        .await
    }
}
