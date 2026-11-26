//! PostgreSQL materialized-view storage through the existing view contracts.

use cqrs_es::persist::{PersistenceError, ViewContext, ViewRepository};
use cqrs_es::{Aggregate, View};
use sqlx::postgres::PgArguments;
use sqlx::{Arguments, AssertSqlSafe, PgPool, Postgres, QueryBuilder};
use std::ops::ControlFlow;

use event_sorcery::{
    Column, EngineError, EventSourced, NativeProjectionBackend, ProjectionError,
    ProjectionRecoveryMode, Table, ViewBackend, ViewUpdateError, recover_projection,
};
use indexed_query::{IndexedView, Order, Predicate, ProjectionQueryError, Value};

use crate::event_repository::adapter_error;

/// PostgreSQL repositories for the existing materialized-view capability.
#[derive(Debug, Clone, Copy)]
pub struct PostgresViewBackend;

impl ViewBackend for PostgresViewBackend {
    type Repo<ViewState, AggregateType>
        = PostgresViewRepository<ViewState, AggregateType>
    where
        ViewState: View<AggregateType> + Clone + 'static,
        AggregateType: Aggregate + 'static;

    async fn try_update<ViewState, AggregateType, Skip>(
        repository: &Self::Repo<ViewState, AggregateType>,
        view_id: &str,
        apply: impl FnOnce(Option<ViewState>, &ViewContext) -> ControlFlow<Skip, ViewState> + Send,
    ) -> Result<Option<Skip>, ViewUpdateError>
    where
        ViewState: View<AggregateType> + Clone + 'static,
        AggregateType: Aggregate + 'static,
        Skip: Send,
    {
        let table = indexed_query::quote_identifier(&repository.table);
        let stored: Option<(i64, String)> = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT version, payload::text FROM {table} WHERE view_id = $1"
        )))
        .bind(view_id)
        .fetch_optional(&repository.pool)
        .await
        .map_err(|source| {
            ViewUpdateError::Load(PersistenceError::ConnectionError(Box::new(source)))
        })?;

        let (view, context, stored_payload) = match stored {
            Some((version, payload)) => {
                validate_stored_version(version).map_err(ViewUpdateError::Load)?;
                let view = serde_json::from_str::<serde_json::Value>(&payload)
                    .and_then(serde_json::from_value)
                    .map_err(|source| ViewUpdateError::Load(source.into()))?;
                (
                    Some(view),
                    ViewContext::new(view_id.to_owned(), version),
                    Some(payload),
                )
            }
            None => (None, ViewContext::new(view_id.to_owned(), 0), None),
        };
        let updated = match apply(view, &context) {
            ControlFlow::Break(skipped) => return Ok(Some(skipped)),
            ControlFlow::Continue(updated) => updated,
        };
        match stored_payload {
            Some(payload) => {
                repository
                    .compare_update(&table, updated, context, payload)
                    .await
            }
            None => repository.update_view(updated, context).await,
        }
        .map_err(ViewUpdateError::Write)?;
        Ok(None)
    }

    fn is_retryable_error(error: &(dyn std::error::Error + 'static)) -> bool {
        use PersistenceError::{ConnectionError, UnknownError};

        // CQRS does not expose these boxed errors through Error::source.
        let sql_error = match error.downcast_ref::<PersistenceError>() {
            Some(ConnectionError(source) | UnknownError(source)) => {
                source.downcast_ref::<sqlx::Error>()
            }
            Some(_) => None,
            None => error.downcast_ref::<sqlx::Error>(),
        };
        sql_error
            .and_then(sqlx::Error::as_database_error)
            .is_some_and(|source| match source.code().as_deref() {
                Some("55P03" | "40001" | "40P01") => true,
                Some(_) | None => false,
            })
    }
}

/// PostgreSQL storage for a materialized view and its optimistic version.
///
/// Application migrations provide `view_id TEXT PRIMARY KEY`, a positive `BIGINT`
/// `version`, and a non-null `JSONB` payload. Updates preserve other columns.
pub struct PostgresViewRepository<ViewState, AggregateType> {
    pool: PgPool,
    table: String,
    repository: postgres_es::PostgresViewRepository<ViewState, AggregateType>,
}

impl<ViewState, AggregateType> AsRef<PgPool> for PostgresViewRepository<ViewState, AggregateType> {
    fn as_ref(&self) -> &PgPool {
        &self.pool
    }
}

impl<ViewState, AggregateType> PostgresViewRepository<ViewState, AggregateType>
where
    ViewState: View<AggregateType>,
    AggregateType: Aggregate,
{
    /// Constructs storage for an application-owned projection table.
    pub fn new(pool: PgPool, table: &str) -> Self {
        let repository = postgres_es::PostgresViewRepository::new(
            AssertSqlSafe(indexed_query::quote_identifier(table)),
            pool.clone(),
        );
        Self {
            pool,
            table: table.to_owned(),
            repository,
        }
    }

    async fn compare_update(
        &self,
        quoted_table: &str,
        view: ViewState,
        context: ViewContext,
        observed_payload: String,
    ) -> Result<(), PersistenceError> {
        let version = next_version(context.version)?;
        let payload = serde_json::to_value(view)?;

        // Recovery can change the payload without advancing the event checkpoint.
        // Compare PostgreSQL's original JSONB text without reserializing the view
        // or letting a collation equate distinct payloads.
        let result = sqlx::query(AssertSqlSafe(format!(
            r#"UPDATE {quoted_table}
               SET payload = $1, version = $2
               WHERE view_id = $3 AND version = $4 AND payload::text COLLATE "C" = $5"#
        )))
        .bind(payload)
        .bind(version)
        .bind(context.view_instance_id)
        .bind(context.version)
        .bind(observed_payload)
        .execute(&self.pool)
        .await
        .map_err(|source| PersistenceError::ConnectionError(Box::new(source)))?;
        if result.rows_affected() == 0 {
            return Err(PersistenceError::OptimisticLockError);
        }
        Ok(())
    }
}

impl NativeProjectionBackend for PostgresViewBackend {
    type Source = PgPool;
    type Database = Postgres;

    fn repository<ViewState, AggregateType>(
        source: PgPool,
        Table(table): Table,
    ) -> Self::Repo<ViewState, AggregateType>
    where
        ViewState: View<AggregateType> + Clone + 'static,
        AggregateType: Aggregate + 'static,
    {
        PostgresViewRepository::new(source, table)
    }

    fn source<ViewState, AggregateType>(
        repository: &Self::Repo<ViewState, AggregateType>,
    ) -> &PgPool
    where
        ViewState: View<AggregateType> + Clone + 'static,
        AggregateType: Aggregate + 'static,
    {
        repository.as_ref()
    }

    async fn rows(
        source: &PgPool,
        Table(table): Table,
    ) -> Result<Vec<(String, String)>, sqlx::Error> {
        let table = indexed_query::quote_identifier(table);
        sqlx::query_as(AssertSqlSafe(format!(
            "SELECT view_id, payload::text
             FROM {table}
             ORDER BY view_id ASC"
        )))
        .fetch_all(source)
        .await
    }

    async fn filter<Entity, Value>(
        source: &PgPool,
        Table(table): Table,
        column: Column,
        value: &Value,
    ) -> Result<Vec<(String, String)>, ProjectionError<Entity>>
    where
        Entity: EventSourced,
        for<'query> &'query Value: sqlx::Encode<'query, Postgres>,
        Value: sqlx::Type<Postgres> + Send + Sync,
    {
        let quoted_table = indexed_query::quote_identifier(table);
        let Column(column_name) = &column;
        let mut transaction = source
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await?;

        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_attribute
             WHERE attrelid = to_regclass($1) AND attname = $2 AND NOT attisdropped)",
        )
        .bind(&quoted_table)
        .bind(column_name)
        .fetch_one(&mut *transaction)
        .await?;
        if !exists {
            return Err(ProjectionError::ColumnNotFound {
                column,
                table: table.to_string(),
            });
        }
        let quoted_column = indexed_query::quote_identifier(column_name);
        let (rows, non_null): (i64, i64) = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT COUNT(*), COUNT({quoted_column}) FROM {quoted_table}"
        )))
        .fetch_one(&mut *transaction)
        .await?;
        if rows > 0 && non_null == 0 {
            return Err(ProjectionError::StaleColumn {
                column,
                table: table.to_string(),
                row_count: rows,
            });
        }
        let matches = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT view_id, payload::text FROM {quoted_table}
             WHERE {quoted_column} = $1 ORDER BY view_id"
        )))
        .bind(value)
        .fetch_all(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(matches)
    }

    async fn recover<Entity>(
        source: &PgPool,
        mode: &ProjectionRecoveryMode,
    ) -> Result<(), ProjectionError<Entity>>
    where
        Entity: EventSourced<Materialized = Table>,
    {
        let mut transaction = source.begin().await?;
        {
            let mut storage = crate::projection_recovery::PostgresProjectionRecovery {
                connection: &mut transaction,
            };
            recover_projection::<Entity>(&mut storage, mode).await?;
        }
        transaction.commit().await?;
        Ok(())
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
        let stored = self
            .repository
            .load_with_context(view_id)
            .await
            .map_err(adapter_error)?;
        if let Some((_, context)) = &stored {
            validate_stored_version(context.version)?;
        }
        Ok(stored)
    }

    async fn update_view(
        &self,
        view: ViewState,
        context: ViewContext,
    ) -> Result<(), PersistenceError> {
        next_version(context.version)?;
        self.repository
            .update_view(view, context)
            .await
            .map_err(adapter_error)
    }
}

fn validate_stored_version(version: i64) -> Result<(), PersistenceError> {
    if version <= 0 {
        return Err(PersistenceError::UnknownError(Box::new(
            ProjectionVersionError::StoredNonPositive(version),
        )));
    }
    Ok(())
}

fn next_version(version: i64) -> Result<i64, PersistenceError> {
    if version < 0 {
        return Err(PersistenceError::UnknownError(Box::new(
            ProjectionVersionError::Negative(version),
        )));
    }
    version.checked_add(1).ok_or_else(|| {
        PersistenceError::UnknownError(Box::new(ProjectionVersionError::Overflow(version)))
    })
}

impl<ViewState, AggregateType> IndexedView<ViewState, AggregateType>
    for PostgresViewRepository<ViewState, AggregateType>
where
    ViewState: View<AggregateType>,
    AggregateType: Aggregate,
{
    async fn find(
        &self,
        predicate: &Predicate,
        order: Option<&Order>,
        limit: i64,
    ) -> Result<Vec<String>, PersistenceError> {
        let mut arguments = PgArguments::default();
        let sql = indexed_query::compile_query(
            &self.table,
            indexed_query::IdentifierMode::Literal,
            predicate,
            order,
            limit,
            |value| encode_argument(&mut arguments, value),
        )
        .map_err(|source| PersistenceError::UnknownError(Box::new(source)))?;
        let Some(sql) = sql else {
            return Ok(vec![]);
        };
        let mut query = QueryBuilder::<Postgres>::with_arguments(sql, arguments);
        Ok(query
            .build_query_scalar::<String>()
            .fetch_all(&self.pool)
            .await
            .map_err(EngineError::from)?)
    }
}

fn encode_argument(
    arguments: &mut PgArguments,
    value: &Value,
) -> Result<String, ProjectionQueryError> {
    if arguments.len() >= usize::from(u16::MAX) {
        return Err(ProjectionQueryError::ParameterLimit);
    }
    match value {
        Value::Int(integer) => arguments.add(*integer),
        Value::Text(text) => arguments.add(text.as_str()),
    }
    .map_err(ProjectionQueryError::Encoding)?;
    let mut placeholder = String::new();
    arguments.format_placeholder(&mut placeholder)?;
    Ok(placeholder)
}

#[cfg(test)]
pub(crate) mod tests {
    use cqrs_es::event_sink::EventSink;
    use cqrs_es::{DomainEvent, EventEnvelope};
    use proptest::prelude::{any, prop_assert_eq, proptest};
    use serde::{Deserialize, Serialize};

    use serde_json::json;
    use std::convert::Infallible;

    use indexed_query::{Cmp, Term, Value};

    use super::*;

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    pub(crate) struct RegisteredSchemaView {
        name: String,
        schema_version: u64,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub(crate) enum RegisteredSchemaEvent {
        VersionUpdated { name: String, version: u64 },
    }

    impl DomainEvent for RegisteredSchemaEvent {
        fn event_type(&self) -> String {
            "VersionUpdated".to_string()
        }
        fn event_version(&self) -> String {
            "1".to_string()
        }
    }

    impl Aggregate for RegisteredSchemaView {
        const TYPE: &'static str = "RegisteredSchema";
        type Command = RegisteredSchemaEvent;
        type Event = RegisteredSchemaEvent;
        type Error = Infallible;
        type Services = ();

        async fn handle(
            &mut self,
            event: Self::Event,
            _services: &(),
            sink: &EventSink<Self>,
        ) -> Result<(), Self::Error> {
            let mut scratch = self.clone();
            sink.write(event, &mut scratch).await;
            Ok(())
        }

        fn apply(&mut self, event: Self::Event) {
            let RegisteredSchemaEvent::VersionUpdated { name, version } = event;
            self.name = name;
            self.schema_version = version;
        }
    }

    impl View<Self> for RegisteredSchemaView {
        fn update(&mut self, event: &EventEnvelope<Self>) {
            let RegisteredSchemaEvent::VersionUpdated { name, version } = &event.payload;
            self.name.clone_from(name);
            self.schema_version = *version;
        }
    }

    #[tokio::test]
    async fn view_version_validation_precedes_storage_access() {
        let pool = PgPool::connect_lazy("postgresql://localhost/unused").unwrap();
        pool.close().await;
        let repository = PostgresViewRepository::<RegisteredSchemaView, RegisteredSchemaView>::new(
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

    fn query_error(result: Result<Vec<String>, PersistenceError>) -> ProjectionQueryError {
        let Err(PersistenceError::UnknownError(source)) = result else {
            panic!("expected a typed projection-query error");
        };
        *source.downcast::<ProjectionQueryError>().unwrap()
    }

    #[tokio::test]
    async fn malformed_indexed_queries_fail_before_storage_access() {
        let pool = PgPool::connect_lazy("postgresql://localhost/unused").unwrap();
        pool.close().await;
        let repository = PostgresViewRepository::<RegisteredSchemaView, RegisteredSchemaView>::new(
            pool,
            "schema_projection",
        );
        assert!(matches!(
            query_error(
                repository
                    .find(&Predicate { any_of: vec![] }, None, -1)
                    .await
            ),
            ProjectionQueryError::NegativeLimit(-1)
        ));
        assert!(matches!(
            query_error(
                repository
                    .find(
                        &Predicate {
                            any_of: vec![vec![]]
                        },
                        None,
                        1
                    )
                    .await
            ),
            ProjectionQueryError::EmptyConjunction
        ));
        let missing = Predicate {
            any_of: vec![vec![Term {
                column: "name".to_string(),
                cmp: Cmp::Eq,
                value: None,
            }]],
        };
        assert!(matches!(
            query_error(repository.find(&missing, None, 1).await),
            ProjectionQueryError::MissingValue
        ));
        let late_missing = Predicate {
            any_of: vec![vec![
                Term {
                    column: "schema_version".to_string(),
                    cmp: Cmp::Eq,
                    value: Some(Value::Int(1)),
                },
                Term {
                    column: "name".to_string(),
                    cmp: Cmp::Eq,
                    value: None,
                },
            ]],
        };
        assert!(matches!(
            query_error(repository.find(&late_missing, None, 1).await),
            ProjectionQueryError::MissingValue
        ));
        let unexpected = Predicate {
            any_of: vec![vec![Term {
                column: "name".to_string(),
                cmp: Cmp::IsNull,
                value: Some(Value::Int(1)),
            }]],
        };
        assert!(matches!(
            query_error(repository.find(&unexpected, None, 1).await),
            ProjectionQueryError::UnexpectedValue
        ));
        assert_eq!(
            repository
                .find(&Predicate { any_of: vec![] }, None, 1)
                .await
                .unwrap(),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn indexed_parameter_limit_fails_before_storage_access() {
        let pool = PgPool::connect_lazy("postgresql://localhost/unused").unwrap();
        pool.close().await;
        let repository = PostgresViewRepository::<RegisteredSchemaView, RegisteredSchemaView>::new(
            pool,
            "schema_projection",
        );
        let terms = (0..usize::from(u16::MAX))
            .map(|_| Term {
                column: "schema_version".to_string(),
                cmp: Cmp::Eq,
                value: Some(Value::Int(1)),
            })
            .collect();
        let predicate = Predicate {
            any_of: vec![terms],
        };
        assert!(matches!(
            query_error(repository.find(&predicate, None, 1).await),
            ProjectionQueryError::ParameterLimit
        ));
    }

    #[test]
    fn native_arguments_accept_the_exact_parameter_boundary_including_limit() {
        let predicate = Predicate {
            any_of: vec![
                (0..65_534)
                    .map(|_| Term {
                        column: "schema_version".to_string(),
                        cmp: Cmp::Eq,
                        value: Some(Value::Int(1)),
                    })
                    .collect(),
            ],
        };
        let mut arguments = PgArguments::default();
        let sql = indexed_query::compile_query(
            "schema_projection",
            indexed_query::IdentifierMode::Literal,
            &predicate,
            None,
            1,
            |value| encode_argument(&mut arguments, value),
        )
        .unwrap()
        .unwrap();
        assert_eq!(arguments.len(), usize::from(u16::MAX));
        assert_eq!(sql.rsplit_once(" LIMIT ").unwrap().1, "$65535");
    }

    proptest! {
        #[test]
        fn query_text_is_independent_of_string_values(value in any::<String>()) {
            let predicate = Predicate {
                any_of: vec![vec![Term {
                    column: "name".to_string(),
                    cmp: Cmp::Eq,
                    value: Some(Value::Text(value.clone())),
                }]],
            };
            let mut arguments = PgArguments::default();
            let sql = indexed_query::compile_query("test_projection", indexed_query::IdentifierMode::Literal, &predicate, None, 1, |value| {
                encode_argument(&mut arguments, value)
            }).unwrap().unwrap();
            prop_assert_eq!(sql, r#"SELECT view_id FROM "test_projection" WHERE ("name" = $1) LIMIT $2"#);
            prop_assert_eq!(arguments.len(), 2);
            let mut expected = PgArguments::default();
            expected.add(value.as_str()).unwrap();
            expected.add(1_i64).unwrap();
            prop_assert_eq!(format!("{arguments:?}"), format!("{expected:?}"));
        }
    }

    async fn indexed_repository(
        pool: PgPool,
    ) -> PostgresViewRepository<RegisteredSchemaView, RegisteredSchemaView> {
        sqlx::query(
            "CREATE TABLE schema_projection (view_id TEXT PRIMARY KEY, version BIGINT NOT NULL, \
             payload JSONB NOT NULL, name TEXT GENERATED ALWAYS AS (payload->>'name') STORED, \
             schema_version BIGINT GENERATED ALWAYS AS \
             ((payload->>'schema_version')::BIGINT) STORED, marker TEXT)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let repository = PostgresViewRepository::new(pool, "schema_projection");
        for (id, name, schema_version) in [
            ("account", "Account", 10),
            ("invoice", "Invoice", 20),
            ("other", "Draft", 30),
        ] {
            repository
                .update_view(
                    RegisteredSchemaView {
                        name: name.to_string(),
                        schema_version,
                    },
                    ViewContext::new(id.to_string(), 0),
                )
                .await
                .unwrap();
        }
        repository
    }

    #[sqlx::test(migrations = false)]
    async fn literal_dotted_quoted_columns_are_bound_filtered_and_ordered(pool: PgPool) {
        let repository = indexed_repository(pool.clone()).await;
        sqlx::query(
            r#"ALTER TABLE schema_projection ADD COLUMN "rank.""value" BIGINT
               GENERATED ALWAYS AS ((payload->>'schema_version')::BIGINT) STORED"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        let predicate = Predicate {
            any_of: vec![vec![Term {
                column: "rank.\"value".to_string(),
                cmp: Cmp::Gt,
                value: Some(Value::Int(15)),
            }]],
        };
        let order = Order {
            column: "rank.\"value".to_string(),
            ascending: false,
        };
        assert_eq!(
            repository.find(&predicate, Some(&order), 1).await.unwrap(),
            vec!["other".to_string()],
        );
    }

    #[sqlx::test(migrations = false)]
    async fn indexed_queries_preserve_dnf_order_and_limit(pool: PgPool) {
        let repository = indexed_repository(pool).await;
        let predicate = Predicate {
            any_of: vec![
                vec![
                    Term {
                        column: "schema_version".to_string(),
                        cmp: Cmp::Le,
                        value: Some(Value::Int(20)),
                    },
                    Term {
                        column: "name".to_string(),
                        cmp: Cmp::Eq,
                        value: Some(Value::Text("Account".to_string())),
                    },
                ],
                vec![Term {
                    column: "name".to_string(),
                    cmp: Cmp::Eq,
                    value: Some(Value::Text("Draft".to_string())),
                }],
            ],
        };
        let order = Order {
            column: "name".to_string(),
            ascending: true,
        };
        assert_eq!(
            repository.find(&predicate, Some(&order), 10).await.unwrap(),
            ["account", "other"]
        );
        assert_eq!(
            repository.find(&predicate, Some(&order), 1).await.unwrap(),
            ["account"]
        );
        assert_eq!(
            repository.find(&predicate, Some(&order), 0).await.unwrap(),
            Vec::<String>::new()
        );
        let quoted = Predicate {
            any_of: vec![vec![Term {
                column: "name".to_string(),
                cmp: Cmp::Eq,
                value: Some(Value::Text("Account' OR 1=1 --".to_string())),
            }]],
        };
        assert_eq!(
            repository.find(&quoted, None, 10).await.unwrap(),
            Vec::<String>::new()
        );
    }

    #[sqlx::test(migrations = false)]
    async fn indexed_numeric_comparisons_match_the_requested_operator(pool: PgPool) {
        let repository = indexed_repository(pool).await;
        let order = Order {
            column: "schema_version".to_string(),
            ascending: true,
        };
        for (cmp, expected) in [
            (Cmp::Eq, vec!["invoice"]),
            (Cmp::Lt, vec!["account"]),
            (Cmp::Le, vec!["account", "invoice"]),
            (Cmp::Gt, vec!["other"]),
            (Cmp::Ge, vec!["invoice", "other"]),
        ] {
            let predicate = Predicate {
                any_of: vec![vec![Term {
                    column: "schema_version".to_string(),
                    cmp,
                    value: Some(Value::Int(20)),
                }]],
            };
            assert_eq!(
                repository.find(&predicate, Some(&order), 10).await.unwrap(),
                expected
            );
        }
    }

    #[sqlx::test(migrations = false)]
    async fn indexed_null_order_and_sidecar_columns_are_preserved(pool: PgPool) {
        let repository = indexed_repository(pool).await;
        sqlx::query("UPDATE schema_projection SET marker = 'pinned' WHERE view_id = 'account'")
            .execute(&repository.pool)
            .await
            .unwrap();
        repository
            .update_view(
                RegisteredSchemaView {
                    name: "Account".to_string(),
                    schema_version: 11,
                },
                ViewContext::new("account".to_string(), 1),
            )
            .await
            .unwrap();
        let predicate = Predicate {
            any_of: vec![vec![Term {
                column: "schema_version".to_string(),
                cmp: Cmp::Ge,
                value: Some(Value::Int(0)),
            }]],
        };
        let order = Order {
            column: "marker".to_string(),
            ascending: true,
        };
        let rows = repository.find(&predicate, Some(&order), 3).await.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows.last().map(String::as_str), Some("account"));
        let mut nulls = rows[..2].to_vec();
        nulls.sort();
        assert_eq!(nulls, ["invoice", "other"]);
        let descending = Order {
            column: "marker".to_string(),
            ascending: false,
        };
        let rows = repository
            .find(&predicate, Some(&descending), 3)
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows.first().map(String::as_str), Some("account"));
        let pinned = Predicate {
            any_of: vec![vec![Term {
                column: "marker".to_string(),
                cmp: Cmp::IsNotNull,
                value: None,
            }]],
        };
        assert_eq!(
            repository.find(&pinned, None, 3).await.unwrap(),
            ["account"]
        );
        let unpinned = Predicate {
            any_of: vec![vec![Term {
                column: "marker".to_string(),
                cmp: Cmp::IsNull,
                value: None,
            }]],
        };
        let by_name = Order {
            column: "name".to_string(),
            ascending: true,
        };
        assert_eq!(
            repository.find(&unpinned, Some(&by_name), 3).await.unwrap(),
            ["other", "invoice"]
        );
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
        let repository = PostgresViewRepository::<RegisteredSchemaView, RegisteredSchemaView>::new(
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
        let repository = PostgresViewRepository::<RegisteredSchemaView, RegisteredSchemaView>::new(
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
        let repository = PostgresViewRepository::<RegisteredSchemaView, RegisteredSchemaView>::new(
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
    async fn projection_lock_timeout_is_retryable_without_advancing_the_checkpoint(pool: PgPool) {
        sqlx::query(
            "CREATE TABLE schema_projection (view_id TEXT PRIMARY KEY, \
             version BIGINT NOT NULL, payload JSONB NOT NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let writer = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(pool.connect_options().as_ref().clone())
            .await
            .unwrap();
        sqlx::query("SET lock_timeout = '100ms'")
            .execute(&writer)
            .await
            .unwrap();
        let repository = PostgresViewRepository::<RegisteredSchemaView, RegisteredSchemaView>::new(
            writer,
            "schema_projection",
        );
        let view = RegisteredSchemaView {
            name: "Account".to_string(),
            schema_version: 10,
        };
        repository
            .update_view(view.clone(), ViewContext::new("schema".to_string(), 0))
            .await
            .unwrap();

        let mut blocker = pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE schema_projection IN SHARE MODE")
            .execute(&mut *blocker)
            .await
            .unwrap();
        let (_, context) = repository
            .load_with_context("schema")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(context.version, 1);
        let error = repository
            .update_view(view.clone(), context)
            .await
            .unwrap_err();

        let PersistenceError::ConnectionError(source) = &error else {
            panic!("expected a SQLx lock timeout: {error:?}");
        };
        let source = source.downcast_ref::<sqlx::Error>().unwrap();
        let database = source.as_database_error().unwrap();
        assert_eq!(database.code().as_deref(), Some("55P03"));
        assert!(PostgresViewBackend::is_retryable_error(&error));

        let error = PostgresViewBackend::try_update(&repository, "schema", |stored, context| {
            assert_eq!(context.version, 1);
            ControlFlow::<(), _>::Continue(stored.unwrap())
        })
        .await
        .unwrap_err();
        let ViewUpdateError::Write(error) = error else {
            panic!("expected the live compare-and-write to fail during the write: {error:?}");
        };
        let PersistenceError::ConnectionError(source) = &error else {
            panic!("expected a typed SQLx lock timeout from the live write: {error:?}");
        };
        let source = source.downcast_ref::<sqlx::Error>().unwrap();
        assert_eq!(
            source.as_database_error().unwrap().code().as_deref(),
            Some("55P03")
        );
        assert!(PostgresViewBackend::is_retryable_error(&error));

        blocker.rollback().await.unwrap();
        let (_, context) = repository
            .load_with_context("schema")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(context.version, 1);
        repository.update_view(view, context).await.unwrap();
        let (_, context) = repository
            .load_with_context("schema")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(context.version, 2);
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
        let repository = PostgresViewRepository::<RegisteredSchemaView, RegisteredSchemaView>::new(
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
        assert!(!PostgresViewBackend::is_retryable_error(&error));
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
    async fn incompatible_view_storage_returns_a_typed_error_without_panicking(pool: PgPool) {
        sqlx::query(
            "CREATE TABLE schema_projection \
            (view_id TEXT PRIMARY KEY, version TEXT NOT NULL, payload JSONB NOT NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO schema_projection VALUES ($1, $2, $3)")
            .bind("schema")
            .bind("not-a-version")
            .bind(json!({"name": "Account", "schema_version": 10}))
            .execute(&pool)
            .await
            .unwrap();
        let repository = PostgresViewRepository::<RegisteredSchemaView, RegisteredSchemaView>::new(
            pool,
            "schema_projection",
        );
        let Err(error) = repository.load_with_context("schema").await else {
            panic!("incompatible storage must not decode successfully");
        };
        let PersistenceError::ConnectionError(source) = error else {
            panic!("incompatible storage must return a typed SQL error: {error:?}");
        };
        assert!(matches!(
            *source.downcast::<sqlx::Error>().unwrap(),
            sqlx::Error::ColumnDecode { .. }
        ));
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
        let repository = PostgresViewRepository::<RegisteredSchemaView, RegisteredSchemaView>::new(
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
