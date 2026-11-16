//! PostgreSQL materialized-view storage through the existing view contracts.

use cqrs_es::persist::{PersistenceError, ViewContext, ViewRepository};
use cqrs_es::{Aggregate, View};
use sqlx::postgres::PgArguments;
use sqlx::{Arguments, AssertSqlSafe, PgPool, Postgres, QueryBuilder};
use std::fmt::{self, Display};

use sqlite_es::{Cmp, IndexedView, Order, Predicate, Value};

use event_sorcery::{EngineError, ViewBackend};

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

impl<ViewState, AggregateType> PostgresViewRepository<ViewState, AggregateType>
where
    ViewState: View<AggregateType>,
    AggregateType: Aggregate,
{
    /// Constructs storage for an application-owned projection table.
    pub fn new(pool: PgPool, table: &str) -> Self {
        let table = format!("{}", QuotedIdentifier(table));
        let repository =
            postgres_es::PostgresViewRepository::new(AssertSqlSafe(table.clone()), pool.clone());
        Self {
            pool,
            table,
            repository,
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

/// Invalid indexed-query inputs rejected before executing SQL.
#[derive(Debug, thiserror::Error)]
pub enum ProjectionQueryError {
    /// The PostgreSQL driver could not encode a query argument.
    #[error("indexed projection argument encoding failed")]
    Encoding(#[source] sqlx::error::BoxDynError),
    /// The driver could not format its own placeholder.
    #[error("indexed projection placeholder formatting failed")]
    Placeholder(#[from] fmt::Error),
    /// A row limit must not be negative.
    #[error("indexed projection query limit cannot be negative: {0}")]
    NegativeLimit(i64),
    /// Every disjunction arm must contain at least one comparison.
    #[error("indexed projection query contains an empty conjunction")]
    EmptyConjunction,
    /// Binary comparisons require a bound value.
    #[error("indexed projection comparison requires a bound value")]
    MissingValue,
    /// Null comparisons do not accept a bound value.
    #[error("indexed projection null comparison cannot contain a bound value")]
    UnexpectedValue,
    /// No further placeholder fits the PostgreSQL wire representation.
    #[error("indexed projection query exceeds the PostgreSQL parameter limit")]
    ParameterLimit,
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
        let stored = self
            .repository
            .load_with_context(view_id)
            .await
            .map_err(adapter_error)?;
        if let Some((_, context)) = &stored
            && context.version <= 0
        {
            return Err(PersistenceError::UnknownError(Box::new(
                ProjectionVersionError::StoredNonPositive(context.version),
            )));
        }
        Ok(stored)
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
        context.version.checked_add(1).ok_or_else(|| {
            PersistenceError::UnknownError(Box::new(ProjectionVersionError::Overflow(
                context.version,
            )))
        })?;
        self.repository
            .update_view(view, context)
            .await
            .map_err(adapter_error)
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
        predicate: &Predicate,
        order: Option<&Order>,
        limit: i64,
    ) -> Result<Vec<String>, PersistenceError> {
        if limit < 0 {
            return Err(PersistenceError::UnknownError(Box::new(
                ProjectionQueryError::NegativeLimit(limit),
            )));
        }
        if predicate.any_of.is_empty() {
            return Ok(vec![]);
        }

        let mut query =
            QueryBuilder::<Postgres>::new(format!("SELECT view_id FROM {} WHERE ", self.table));
        let mut arguments = PgArguments::default();
        push_predicate(&mut query, &mut arguments, predicate)
            .map_err(|source| PersistenceError::UnknownError(Box::new(source)))?;
        if let Some(order) = order {
            let direction = if order.ascending {
                " ASC NULLS FIRST"
            } else {
                " DESC NULLS LAST"
            };
            query
                .push(" ORDER BY ")
                .push(QuotedIdentifier(&order.column))
                .push(direction);
        }
        query.push(" LIMIT ");
        push_argument(&mut query, &mut arguments, &Value::Int(limit))
            .map_err(|source| PersistenceError::UnknownError(Box::new(source)))?;
        let mut query =
            QueryBuilder::<Postgres>::with_arguments(query.sql().as_str().to_owned(), arguments);
        Ok(query
            .build_query_scalar::<String>()
            .fetch_all(&self.pool)
            .await
            .map_err(EngineError::from)?)
    }
}

fn push_argument(
    query: &mut QueryBuilder<Postgres>,
    arguments: &mut PgArguments,
    value: &Value,
) -> Result<(), ProjectionQueryError> {
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
    query.push(placeholder);
    Ok(())
}

fn push_predicate(
    query: &mut QueryBuilder<Postgres>,
    arguments: &mut PgArguments,
    predicate: &Predicate,
) -> Result<(), ProjectionQueryError> {
    for (group_index, conjunction) in predicate.any_of.iter().enumerate() {
        if conjunction.is_empty() {
            return Err(ProjectionQueryError::EmptyConjunction);
        }
        if group_index != 0 {
            query.push(" OR ");
        }
        query.push("(");
        for (term_index, term) in conjunction.iter().enumerate() {
            if term_index != 0 {
                query.push(" AND ");
            }
            let operator = match &term.cmp {
                Cmp::Eq => " = ",
                Cmp::Lt => " < ",
                Cmp::Le => " <= ",
                Cmp::Gt => " > ",
                Cmp::Ge => " >= ",
                Cmp::IsNull => " IS NULL",
                Cmp::IsNotNull => " IS NOT NULL",
            };
            query.push(QuotedIdentifier(&term.column)).push(operator);
            match (&term.cmp, term.value.as_ref()) {
                (Cmp::IsNull | Cmp::IsNotNull, None) => {}
                (Cmp::Eq | Cmp::Lt | Cmp::Le | Cmp::Gt | Cmp::Ge, Some(value)) => {
                    push_argument(query, arguments, value)?;
                }
                (Cmp::Eq | Cmp::Lt | Cmp::Le | Cmp::Gt | Cmp::Ge, None) => {
                    return Err(ProjectionQueryError::MissingValue);
                }
                (Cmp::IsNull | Cmp::IsNotNull, Some(_)) => {
                    return Err(ProjectionQueryError::UnexpectedValue);
                }
            }
        }
        query.push(")");
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use cqrs_es::event_sink::EventSink;
    use cqrs_es::{DomainEvent, EventEnvelope};
    use proptest::prelude::{any, prop_assert_eq, proptest};
    use serde::{Deserialize, Serialize};

    use serde_json::json;
    use std::convert::Infallible;

    use sqlite_es::{Cmp, Term, Value};

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
            let mut query = QueryBuilder::<Postgres>::default();
            let mut arguments = PgArguments::default();
            push_predicate(&mut query, &mut arguments, &predicate).unwrap();
            let sql = query.sql();
            prop_assert_eq!(sql.as_str(), r#"("name" = $1)"#);
            prop_assert_eq!(arguments.len(), 1);
            let mut expected = PgArguments::default();
            expected.add(value.as_str()).unwrap();
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
