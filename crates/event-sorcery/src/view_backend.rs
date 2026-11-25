//! Native construction and recovery of materialized views.

use cqrs_es::Aggregate;
#[cfg(any(test, feature = "sqlite"))]
use sqlx::{Sqlite, SqlitePool};

pub use event_sorcery_core::{ViewBackend, ViewUpdateError};
#[cfg(any(test, feature = "sqlite"))]
use sqlite_es::{SqliteProjectionFilterError, SqliteViewBackend, SqliteViewRepository};

#[cfg(any(test, feature = "sqlite"))]
use crate::Projection;
use crate::{Column, EventSourced, ProjectionError, ProjectionRecoveryMode, Table};

/// Optional native construction, queries and retained-history recovery.
///
/// Ordinary custom [`ViewBackend`] implementations remain unchanged; only
/// backends supporting native startup opt into this capability.
///
/// Native repositories use `ViewContext::version` as the last included aggregate-local
/// event sequence. Payload and checkpoint must be written atomically. `update_view`
/// advances the expected version by exactly one and reports competing writes as
/// `PersistenceError::OptimisticLockError`, including competing initial inserts.
/// Recovery may instead install the captured event maximum atomically with its payload.
/// These rules do not apply to plain `ViewBackend` implementations with opaque revisions.
/// Mixing direct unsequenced `Reactor::react` writes with native framework delivery is
/// unsupported: direct writes cannot establish event coverage.
pub trait NativeProjectionBackend: ViewBackend {
    /// Caller-owned connection source used to construct the view repository.
    type Source: Send + Sync + 'static;
    /// Native SQLx encoding contract accepted by typed column filters.
    type Database: sqlx::Database;

    /// Constructs the existing repository family without exposing lifecycle types.
    fn repository<ViewState, AggregateType>(
        source: Self::Source,
        table: Table,
    ) -> Self::Repo<ViewState, AggregateType>
    where
        ViewState: cqrs_es::View<AggregateType> + Clone + 'static,
        AggregateType: Aggregate + 'static;

    /// Borrows the repository's original source without opening another pool.
    fn source<ViewState, AggregateType>(
        repository: &Self::Repo<ViewState, AggregateType>,
    ) -> &Self::Source
    where
        ViewState: cqrs_es::View<AggregateType> + Clone + 'static,
        AggregateType: Aggregate + 'static;

    /// Reads serialized view rows for the existing typed projection decoder.
    fn rows(
        source: &Self::Source,
        table: Table,
    ) -> impl Future<Output = Result<Vec<(String, String)>, sqlx::Error>> + Send;

    /// Validates a column and executes its native typed equality filter.
    fn filter<Entity, Value>(
        source: &Self::Source,
        table: Table,
        column: Column,
        value: &Value,
    ) -> impl Future<Output = Result<Vec<(String, String)>, ProjectionError<Entity>>> + Send
    where
        Entity: EventSourced,
        for<'query> &'query Value: sqlx::Encode<'query, Self::Database>,
        Value: sqlx::Type<Self::Database> + Send + Sync;

    /// Owns a standalone recovery transaction and reuses borrowed-I/O replay.
    fn recover<Entity>(
        source: &Self::Source,
        mode: &ProjectionRecoveryMode,
    ) -> impl Future<Output = Result<(), ProjectionError<Entity>>> + Send
    where
        Entity: EventSourced<Materialized = Table>;
}

#[cfg(any(test, feature = "sqlite"))]
impl<Entity: EventSourced<Materialized = Table>> Projection<Entity, SqliteViewBackend> {
    /// Constructs the SQLite materialized view using the caller-owned pool.
    pub fn sqlite(pool: SqlitePool) -> Self {
        Self::with_backend(pool)
    }
}

#[cfg(any(test, feature = "sqlite"))]
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
        sqlite_es::recover_projection(source, mode.clone(), |mut storage, mode| {
            Box::pin(async move { crate::recover_projection::<Entity>(&mut storage, &mode).await })
        })
        .await
    }
}
