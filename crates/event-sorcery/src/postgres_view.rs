//! PostgreSQL materialized-view storage through the existing view contracts.

use cqrs_es::persist::{PersistenceError, ViewContext, ViewRepository};
use cqrs_es::{Aggregate, View};
use sqlx::PgPool;
use std::marker::PhantomData;

use sqlite_es::{IndexedView, Order, Predicate};

use crate::ViewBackend;

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
pub struct PostgresViewRepository<ViewState, AggregateType> {
    _types: PhantomData<fn() -> (ViewState, AggregateType)>,
}

impl<ViewState, AggregateType> PostgresViewRepository<ViewState, AggregateType> {
    /// Constructs storage for an application-owned projection table.
    pub fn new(pool: PgPool, table: String) -> Self {
        drop((pool, table));
        Self {
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
}

#[derive(Debug, thiserror::Error)]
#[error("PostgreSQL view persistence is not implemented")]
struct PendingViewPersistence;

impl<ViewState, AggregateType> ViewRepository<ViewState, AggregateType>
    for PostgresViewRepository<ViewState, AggregateType>
where
    ViewState: View<AggregateType>,
    AggregateType: Aggregate,
{
    async fn load(&self, _view_id: &str) -> Result<Option<ViewState>, PersistenceError> {
        Err(PersistenceError::UnknownError(Box::new(
            PendingViewPersistence,
        )))
    }

    async fn load_with_context(
        &self,
        _view_id: &str,
    ) -> Result<Option<(ViewState, ViewContext)>, PersistenceError> {
        Err(PersistenceError::UnknownError(Box::new(
            PendingViewPersistence,
        )))
    }

    async fn update_view(
        &self,
        _view: ViewState,
        _context: ViewContext,
    ) -> Result<(), PersistenceError> {
        Err(PersistenceError::UnknownError(Box::new(
            PendingViewPersistence,
        )))
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
                "schema_projection".to_string(),
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
                "schema_projection".to_string(),
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
            json!({"name":"Account", "schema_version":10})
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
}
