//! PostgreSQL materialized-view storage through the existing view contracts.

use cqrs_es::persist::{PersistenceError, ViewContext, ViewRepository};
use cqrs_es::{Aggregate, View};
use sqlx::{AssertSqlSafe, PgPool};
use std::fmt::{self, Display};
use std::marker::PhantomData;

use sqlite_es::{Cmp, IndexedView, Order, Predicate, Value};

use event_sorcery::{EngineError, ViewBackend};

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

/// Invalid indexed-query inputs rejected before executing SQL.
#[derive(Debug, thiserror::Error)]
pub enum ProjectionQueryError {
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

        let (where_clause, bindings) = predicate_sql(predicate)
            .map_err(|source| PersistenceError::UnknownError(Box::new(source)))?;
        let limit_index = next_parameter(bindings.len())
            .map_err(|source| PersistenceError::UnknownError(Box::new(source)))?;
        let order_clause = order.map_or_else(String::new, |order| {
            let direction = if order.ascending {
                "ASC NULLS FIRST"
            } else {
                "DESC NULLS LAST"
            };
            format!(" ORDER BY {} {direction}", QuotedIdentifier(&order.column))
        });
        let sql = format!(
            "SELECT view_id FROM {} WHERE {where_clause}{order_clause} LIMIT ${limit_index}",
            self.table,
        );
        let mut query = sqlx::query_scalar::<_, String>(AssertSqlSafe(sql));
        for value in bindings {
            query = match value {
                Value::Int(integer) => query.bind(integer),
                Value::Text(text) => query.bind(text),
            };
        }
        Ok(query
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(EngineError::from)?)
    }
}

fn next_parameter(bound: usize) -> Result<u16, ProjectionQueryError> {
    let bound = u16::try_from(bound).map_err(|_| ProjectionQueryError::ParameterLimit)?;
    bound
        .checked_add(1)
        .ok_or(ProjectionQueryError::ParameterLimit)
}

fn predicate_sql(predicate: &Predicate) -> Result<(String, Vec<&Value>), ProjectionQueryError> {
    let mut bindings = vec![];
    let mut groups = vec![];

    for conjunction in &predicate.any_of {
        if conjunction.is_empty() {
            return Err(ProjectionQueryError::EmptyConjunction);
        }

        let mut terms = vec![];
        for term in conjunction {
            let operator = match &term.cmp {
                Cmp::Eq => "=",
                Cmp::Lt => "<",
                Cmp::Le => "<=",
                Cmp::Gt => ">",
                Cmp::Ge => ">=",
                Cmp::IsNull => "IS NULL",
                Cmp::IsNotNull => "IS NOT NULL",
            };

            let column = QuotedIdentifier(&term.column);
            let expression = match (&term.cmp, term.value.as_ref()) {
                (Cmp::IsNull | Cmp::IsNotNull, None) => format!("{column} {operator}"),
                (Cmp::Eq | Cmp::Lt | Cmp::Le | Cmp::Gt | Cmp::Ge, Some(value)) => {
                    let index = next_parameter(bindings.len())?;
                    bindings.push(value);
                    format!("{column} {operator} ${index}")
                }
                (Cmp::Eq | Cmp::Lt | Cmp::Le | Cmp::Gt | Cmp::Ge, None) => {
                    return Err(ProjectionQueryError::MissingValue);
                }
                (Cmp::IsNull | Cmp::IsNotNull, Some(_)) => {
                    return Err(ProjectionQueryError::UnexpectedValue);
                }
            };
            terms.push(expression);
        }

        groups.push(format!("({})", terms.join(" AND ")));
    }

    Ok((groups.join(" OR "), bindings))
}

#[cfg(test)]
mod tests {
    use cqrs_es::EventEnvelope;
    use proptest::prelude::{any, prop_assert_eq, proptest};
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use sqlite_es::{Cmp, Term, Value};

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
        let repository =
            PostgresViewRepository::<RegisteredSchemaView, Lifecycle<SchemaRegistry>>::new(
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
        let repository =
            PostgresViewRepository::<RegisteredSchemaView, Lifecycle<SchemaRegistry>>::new(
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
            let (sql, bindings) = predicate_sql(&predicate).unwrap();
            prop_assert_eq!(sql, r#"("name" = $1)"#);
            prop_assert_eq!(bindings.len(), 1);
            let Value::Text(bound) = bindings[0] else {
                panic!("expected the original text binding");
            };
            prop_assert_eq!(bound, &value);
        }
    }

    async fn indexed_repository(
        pool: PgPool,
    ) -> PostgresViewRepository<RegisteredSchemaView, Lifecycle<SchemaRegistry>> {
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
