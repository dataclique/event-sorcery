//! PostgreSQL materialized-view storage through the existing view contracts.

use cqrs_es::persist::{PersistenceError, ViewContext, ViewRepository};
use cqrs_es::{Aggregate, View};
use sqlx::{AssertSqlSafe, PgPool};
use std::fmt::{self, Display};
use std::marker::PhantomData;

use sqlite_es::{IndexedView, Order, Predicate};

use crate::ViewBackend;
use crate::engine::EngineError;

/// PostgreSQL repositories for the existing materialized-view capability.
#[derive(Debug, Clone, Copy)]
pub struct PostgresViewBackend;

impl ViewBackend for PostgresViewBackend {
    type Repo<ViewState, AggregateType>
        = PostgresViewRepository<ViewState, AggregateType>
    where
        ViewState: View<AggregateType> + Clone + 'static,
        AggregateType: Aggregate + 'static;
}

/// PostgreSQL storage for a materialized view and its optimistic version.
///
/// Application migrations provide `view_id TEXT PRIMARY KEY`, a positive `BIGINT`
/// `version`, and a non-null `JSONB` payload. Updates preserve other columns.
pub struct PostgresViewRepository<ViewState, AggregateType> {
    pool: PgPool,
    table: String,
    _types: PhantomData<fn() -> (ViewState, AggregateType)>,
}

impl<ViewState, AggregateType> PostgresViewRepository<ViewState, AggregateType> {
    /// Constructs storage for an application-owned projection table.
    pub fn new(pool: PgPool, table: &str) -> Self {
        Self {
            pool,
            table: format!("{}", QuotedIdentifier(table)),
            _types: PhantomData,
        }
    }
}

/// Invalid optimistic versions rejected before accessing storage.
#[derive(Debug, thiserror::Error)]
pub enum ProjectionVersionError {
    /// A caller supplied a negative expected version.
    #[error("optimistic projection version cannot be negative: {0}")]
    Negative(i64),
    /// Advancing the expected version exceeds PostgreSQL storage range.
    #[error("optimistic projection version cannot advance beyond storage range: {0}")]
    Overflow(i64),
    /// A persisted row has an invalid optimistic checkpoint.
    #[error("stored optimistic projection version must be positive: {0}")]
    StoredNonPositive(i64),
}

#[derive(Debug, thiserror::Error)]
#[error("PostgreSQL indexed projection queries are not implemented")]
struct PendingViewPersistence;

#[derive(sqlx::FromRow)]
struct ProjectionRow {
    view_id: String,
    version: i64,
    payload: String,
}

struct QuotedIdentifier<'name>(&'name str);

impl Display for QuotedIdentifier<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self(name) = self;
        write!(formatter, "\"{}\"", name.replace('"', "\"\""))
    }
}

impl<ViewState, AggregateType> ViewRepository<ViewState, AggregateType>
    for PostgresViewRepository<ViewState, AggregateType>
where
    ViewState: View<AggregateType>,
    AggregateType: Aggregate,
{
    async fn load(&self, view_id: &str) -> Result<Option<ViewState>, PersistenceError> {
        Ok(self.load_with_context(view_id).await?.map(|(view, _)| view))
    }

    async fn load_with_context(
        &self,
        view_id: &str,
    ) -> Result<Option<(ViewState, ViewContext)>, PersistenceError> {
        let query = format!(
            "SELECT view_id, version, payload::TEXT AS payload FROM {} WHERE view_id = $1",
            self.table,
        );
        let row = sqlx::query_as::<_, ProjectionRow>(AssertSqlSafe(query))
            .bind(view_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(EngineError::from)?;
        let Some(row) = row else {
            return Ok(None);
        };
        if row.version <= 0 {
            return Err(PersistenceError::UnknownError(Box::new(
                ProjectionVersionError::StoredNonPositive(row.version),
            )));
        }
        let view = serde_json::from_str(&row.payload)?;
        Ok(Some((view, ViewContext::new(row.view_id, row.version))))
    }

    async fn update_view(
        &self,
        view: ViewState,
        context: ViewContext,
    ) -> Result<(), PersistenceError> {
        if context.version < 0 {
            return Err(PersistenceError::UnknownError(Box::new(
                ProjectionVersionError::Negative(context.version),
            )));
        }
        let next_version = context.version.checked_add(1).ok_or_else(|| {
            PersistenceError::UnknownError(Box::new(ProjectionVersionError::Overflow(
                context.version,
            )))
        })?;
        let payload = serde_json::to_value(&view)
            .map_err(|source| PersistenceError::UnknownError(Box::new(source)))?;
        drop(view);

        let result = if context.version == 0 {
            let query = format!(
                "INSERT INTO {} (view_id, version, payload) VALUES ($1, $2, $3) \
                 ON CONFLICT (view_id) DO NOTHING",
                self.table,
            );
            sqlx::query(AssertSqlSafe(query))
                .bind(&context.view_instance_id)
                .bind(next_version)
                .bind(payload)
                .execute(&self.pool)
                .await
        } else {
            let query = format!(
                "UPDATE {} SET version = $1, payload = $2 WHERE view_id = $3 AND version = $4",
                self.table,
            );
            sqlx::query(AssertSqlSafe(query))
                .bind(next_version)
                .bind(payload)
                .bind(&context.view_instance_id)
                .bind(context.version)
                .execute(&self.pool)
                .await
        }
        .map_err(EngineError::from)?;

        if result.rows_affected() != 1 {
            return Err(PersistenceError::OptimisticLockError);
        }
        Ok(())
    }
}

impl<ViewState, AggregateType> IndexedView<ViewState, AggregateType>
    for PostgresViewRepository<ViewState, AggregateType>
where
    ViewState: View<AggregateType>,
    AggregateType: Aggregate,
{
    async fn find(
        &self,
        _predicate: &Predicate,
        _order: Option<&Order>,
        _limit: i64,
    ) -> Result<Vec<String>, PersistenceError> {
        Err(PersistenceError::UnknownError(Box::new(
            PendingViewPersistence,
        )))
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::EventEnvelope;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::*;
    use crate::schema_registry::SchemaRegistryEvent;
    use crate::{Lifecycle, SchemaRegistry};

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    struct RegisteredSchemaView {
        name: String,
        schema_version: u64,
    }

    impl View<Lifecycle<SchemaRegistry>> for RegisteredSchemaView {
        fn update(&mut self, event: &EventEnvelope<Lifecycle<SchemaRegistry>>) {
            let SchemaRegistryEvent::VersionUpdated { name, version } = &event.payload;
            self.name.clone_from(name);
            self.schema_version = *version;
        }
    }

    #[tokio::test]
    async fn view_version_validation_precedes_storage_access() {
        let pool = PgPool::connect_lazy("postgresql://localhost/unused").unwrap();
        pool.close().await;
        let repository =
            PostgresViewRepository::<RegisteredSchemaView, Lifecycle<SchemaRegistry>>::new(
                pool,
                "schema_projection",
            );
        for version in [-1, i64::MAX] {
            let error = repository
                .update_view(
                    RegisteredSchemaView::default(),
                    ViewContext::new("schema".to_string(), version),
                )
                .await
                .unwrap_err();
            let PersistenceError::UnknownError(source) = error else {
                panic!("expected a typed optimistic-version validation failure");
            };
            let source = source.downcast::<ProjectionVersionError>().unwrap();
            match (version, *source) {
                (-1, ProjectionVersionError::Negative(-1))
                | (i64::MAX, ProjectionVersionError::Overflow(i64::MAX)) => {}
                other => panic!("unexpected version failure: {other:?}"),
            }
        }
    }

    #[sqlx::test(migrations = false)]
    async fn view_storage_round_trips_and_fences_stale_writers(pool: PgPool) {
        sqlx::query(
            "CREATE TABLE schema_projection (view_id TEXT PRIMARY KEY, \
             version BIGINT NOT NULL CHECK (version > 0), payload JSONB NOT NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let repository =
            PostgresViewRepository::<RegisteredSchemaView, Lifecycle<SchemaRegistry>>::new(
                pool,
                "schema_projection",
            );
        assert_eq!(repository.load("schema").await.unwrap(), None);
        repository
            .update_view(
                RegisteredSchemaView {
                    name: "Account".to_string(),
                    schema_version: 10,
                },
                ViewContext::new("schema".to_string(), 0),
            )
            .await
            .unwrap();
        let (view, context) = repository
            .load_with_context("schema")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(&view).unwrap(),
            json!({"name": "Account", "schema_version": 10})
        );
        assert_eq!(context.version, 1);
        assert_eq!(context.view_instance_id, "schema");
        repository
            .update_view(
                RegisteredSchemaView {
                    name: "Account".to_string(),
                    schema_version: 20,
                },
                context,
            )
            .await
            .unwrap();
        let error = repository
            .update_view(
                RegisteredSchemaView {
                    name: "stale".to_string(),
                    schema_version: 99,
                },
                ViewContext::new("schema".to_string(), 1),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, PersistenceError::OptimisticLockError));
        let (view, context) = repository
            .load_with_context("schema")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(view.schema_version, 20);
        assert_eq!(context.version, 2);
    }

    #[sqlx::test(migrations = false)]
    async fn competing_initial_views_have_one_optimistic_winner(pool: PgPool) {
        sqlx::query(
            "CREATE TABLE schema_projection (view_id TEXT PRIMARY KEY, \
             version BIGINT NOT NULL, payload JSONB NOT NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let repository =
            PostgresViewRepository::<RegisteredSchemaView, Lifecycle<SchemaRegistry>>::new(
                pool,
                "schema_projection",
            );
        let results = tokio::join!(
            repository.update_view(
                RegisteredSchemaView {
                    name: "first".to_string(),
                    schema_version: 10
                },
                ViewContext::new("schema".to_string(), 0),
            ),
            repository.update_view(
                RegisteredSchemaView {
                    name: "second".to_string(),
                    schema_version: 20
                },
                ViewContext::new("schema".to_string(), 0),
            ),
        );
        let winner = match results {
            (Ok(()), Err(PersistenceError::OptimisticLockError)) => "first",
            (Err(PersistenceError::OptimisticLockError), Ok(())) => "second",
            other => panic!("expected one optimistic winner: {other:?}"),
        };
        let (view, context) = repository
            .load_with_context("schema")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(view.name, winner);
        assert_eq!(context.version, 1);
    }

    #[sqlx::test(migrations = false)]
    async fn table_identifiers_are_quoted_as_literals(pool: PgPool) {
        sqlx::query(
            r#"CREATE TABLE "quoted""_projection" (
                view_id TEXT PRIMARY KEY, version BIGINT NOT NULL, payload JSONB NOT NULL
            )"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        let repository =
            PostgresViewRepository::<RegisteredSchemaView, Lifecycle<SchemaRegistry>>::new(
                pool,
                "quoted\"_projection",
            );
        repository
            .update_view(
                RegisteredSchemaView {
                    name: "Account".to_string(),
                    schema_version: 10,
                },
                ViewContext::new("schema".to_string(), 0),
            )
            .await
            .unwrap();
        assert_eq!(
            repository.load("schema").await.unwrap().unwrap().name,
            "Account"
        );
    }

    #[sqlx::test(migrations = false)]
    async fn unrelated_unique_failures_remain_storage_errors(pool: PgPool) {
        sqlx::query(
            "CREATE TABLE schema_projection (view_id TEXT PRIMARY KEY, \
             version BIGINT NOT NULL, payload JSONB NOT NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE UNIQUE INDEX unique_schema_name ON schema_projection ((payload->>'name'))",
        )
        .execute(&pool)
        .await
        .unwrap();
        let repository =
            PostgresViewRepository::<RegisteredSchemaView, Lifecycle<SchemaRegistry>>::new(
                pool,
                "schema_projection",
            );
        let view = RegisteredSchemaView {
            name: "Account".to_string(),
            schema_version: 10,
        };
        repository
            .update_view(view.clone(), ViewContext::new("first".to_string(), 0))
            .await
            .unwrap();
        let error = repository
            .update_view(view, ViewContext::new("second".to_string(), 0))
            .await
            .unwrap_err();
        let PersistenceError::ConnectionError(source) = error else {
            panic!("expected an unrelated uniqueness failure to remain a storage error");
        };
        let source = source.downcast::<sqlx::Error>().unwrap();
        let sqlx::Error::Database(source) = *source else {
            panic!("expected a PostgreSQL constraint error");
        };
        assert_eq!(source.code().as_deref(), Some("23505"));
    }

    #[sqlx::test(migrations = false)]
    async fn malformed_projection_rows_return_typed_errors(pool: PgPool) {
        sqlx::query(
            "CREATE TABLE schema_projection (view_id TEXT PRIMARY KEY, \
             version BIGINT NOT NULL, payload JSONB NOT NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO schema_projection VALUES ($1, $2, $3)")
            .bind("invalid-version")
            .bind(0_i64)
            .bind(json!({"name": "Account", "schema_version": 10}))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO schema_projection VALUES ($1, $2, $3)")
            .bind("invalid-payload")
            .bind(1_i64)
            .bind(json!({"obsolete": true}))
            .execute(&pool)
            .await
            .unwrap();
        let repository =
            PostgresViewRepository::<RegisteredSchemaView, Lifecycle<SchemaRegistry>>::new(
                pool,
                "schema_projection",
            );
        let error = repository.load("invalid-version").await.unwrap_err();
        let PersistenceError::UnknownError(source) = error else {
            panic!("expected an invalid stored-version error");
        };
        assert!(matches!(
            *source.downcast::<ProjectionVersionError>().unwrap(),
            ProjectionVersionError::StoredNonPositive(0),
        ));
        let error = repository.load("invalid-payload").await.unwrap_err();
        assert!(matches!(error, PersistenceError::DeserializationError(_)));
    }
}
