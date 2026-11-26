//! SQLite materialized-view storage and indexed predicate execution.

use cqrs_es::persist::{PersistenceError, ViewContext, ViewRepository};
use cqrs_es::{Aggregate, View};
use sqlx::{Arguments, AssertSqlSafe, Pool, Row, Sqlite};
use std::marker::PhantomData;
use std::ops::ControlFlow;

pub use indexed_query::{Cmp, IndexedView, Order, Predicate, Term, Value};

use event_sorcery_core::{ViewBackend, ViewUpdateError};

use crate::sql_query::SqlQueryFactory;

/// SQLite repository family for aggregate-state materialized views.
#[derive(Debug, Clone, Copy)]
pub struct SqliteViewBackend;

impl ViewBackend for SqliteViewBackend {
    type Repo<View, Agg>
        = SqliteViewRepository<View, Agg>
    where
        View: cqrs_es::View<Agg> + Clone + 'static,
        Agg: Aggregate + 'static;

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
        let stored = repository
            .read_version_and_payload(view_id)
            .await
            .map_err(|error| ViewUpdateError::Load(error.into()))?;
        let (view, version, payload) = match stored {
            Some((version, payload)) => {
                let view = serde_json::from_str(&payload).map_err(|error| {
                    ViewUpdateError::Load(SqliteViewError::Deserialization(error).into())
                })?;
                (Some(view), version, Some(payload))
            }
            None => (None, 0, None),
        };
        let context = ViewContext::new(view_id.to_owned(), version);
        let view = match apply(view, &context) {
            ControlFlow::Break(skipped) => return Ok(Some(skipped)),
            ControlFlow::Continue(view) => view,
        };

        repository
            .write_view(view, context, payload.as_deref())
            .await
            .map_err(|error| ViewUpdateError::Write(error.into()))?;
        Ok(None)
    }

    fn is_retryable_error(error: &(dyn std::error::Error + 'static)) -> bool {
        crate::is_retryable_sqlite_busy(error)
    }
}

/// Errors that can occur during SQLite view repository operations
#[derive(Debug, thiserror::Error)]
pub enum SqliteViewError {
    /// Optimistic locking conflict - the view was modified concurrently
    #[error("Optimistic lock error: view has been modified concurrently")]
    OptimisticLock,

    /// Database connection or query error
    #[error("Database connection error: {0}")]
    Connection(#[from] sqlx::Error),

    /// View serialization error
    #[error("View serialization error: {0}")]
    Serialization(#[source] serde_json::Error),

    /// View deserialization error
    #[error("View deserialization error: {0}")]
    Deserialization(#[source] serde_json::Error),

    /// The stored version cannot be incremented.
    #[error("view version is exhausted")]
    VersionExhausted,

    /// Integer conversion error (e.g., version number overflow)
    #[error("Integer conversion error: {0}")]
    TryFromInt(#[from] std::num::TryFromIntError),
}

impl From<SqliteViewError> for PersistenceError {
    fn from(err: SqliteViewError) -> Self {
        match err {
            SqliteViewError::OptimisticLock => Self::OptimisticLockError,
            SqliteViewError::Connection(e) => Self::ConnectionError(Box::new(e)),
            SqliteViewError::Serialization(e) => Self::UnknownError(Box::new(e)),
            SqliteViewError::Deserialization(e) => Self::DeserializationError(Box::new(e)),
            SqliteViewError::TryFromInt(e) => Self::UnknownError(Box::new(e)),
            SqliteViewError::VersionExhausted => Self::UnknownError(Box::new(err)),
        }
    }
}

/// SQLite implementation of the `ViewRepository` trait
///
/// Provides view persistence backed by SQLite for use with the `GenericQuery`
/// processor in the cqrs-es framework.
///
/// # Type Parameters
///
/// * `V` - The view type that implements `View<A>`
/// * `A` - The aggregate type that the view projects from
///
/// # Example
///
/// ```ignore
/// let pool = Pool::<Sqlite>::connect("sqlite::memory:").await?;
/// let view_repo = SqliteViewRepository::<MintView, Mint>::new(
///     pool,
///     "mint_view".to_string()
/// );
/// ```
pub struct SqliteViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    pool: Pool<Sqlite>,
    view_table: String,
    _phantom: PhantomData<(V, A)>,
}

impl<ViewState, AggregateType> AsRef<Pool<Sqlite>>
    for SqliteViewRepository<ViewState, AggregateType>
where
    ViewState: View<AggregateType>,
    AggregateType: Aggregate,
{
    fn as_ref(&self) -> &Pool<Sqlite> {
        &self.pool
    }
}

impl<V, A> SqliteViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    /// Creates a new `SqliteViewRepository` for a specific view table
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    /// * `view_table` - Name of the table storing this view (e.g., "mint_view")
    #[must_use]
    pub const fn new(pool: Pool<Sqlite>, view_table: String) -> Self {
        Self {
            pool,
            view_table,
            _phantom: PhantomData,
        }
    }

    async fn read_version_and_payload(
        &self,
        view_id: &str,
    ) -> Result<Option<(i64, String)>, SqliteViewError> {
        let query = SqlQueryFactory::select_view(&self.view_table);
        let row = sqlx::query(AssertSqlSafe(query))
            .bind(view_id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|row| Ok((row.try_get("version")?, row.try_get("payload")?)))
            .transpose()
    }

    async fn write_view(
        &self,
        view: V,
        context: ViewContext,
        observed_payload: Option<&str>,
    ) -> Result<(), SqliteViewError> {
        let payload = serde_json::to_string(&view).map_err(SqliteViewError::Serialization)?;
        let new_version = context
            .version
            .checked_add(1)
            .ok_or(SqliteViewError::VersionExhausted)?;
        let affected = if context.version == 0 && observed_payload.is_none() {
            let insert = SqlQueryFactory::insert_view(&self.view_table);
            sqlx::query(AssertSqlSafe(insert))
                .bind(&context.view_instance_id)
                .bind(new_version)
                .bind(&payload)
                .execute(&self.pool)
                .await?
                .rows_affected()
        } else {
            let mut update = SqlQueryFactory::update_view(&self.view_table);
            if observed_payload.is_some() {
                update.push_str(" AND CAST(payload AS BLOB) = CAST(? AS BLOB)");
            }
            let query = sqlx::query(AssertSqlSafe(update))
                .bind(new_version)
                .bind(&payload)
                .bind(&context.view_instance_id)
                .bind(context.version);
            let query = match observed_payload {
                Some(observed) => query.bind(observed),
                None => query,
            };
            query.execute(&self.pool).await?.rows_affected()
        };
        if affected == 0 {
            return Err(SqliteViewError::OptimisticLock);
        }

        Ok(())
    }
}

impl<V, A> ViewRepository<V, A> for SqliteViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    async fn load(&self, view_id: &str) -> Result<Option<V>, PersistenceError> {
        let query = SqlQueryFactory::select_view(&self.view_table);

        let row = sqlx::query(AssertSqlSafe(query))
            .bind(view_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(SqliteViewError::Connection)?;

        let Some(row) = row else {
            return Ok(None);
        };

        let payload_str: String = row
            .try_get("payload")
            .map_err(SqliteViewError::Connection)?;

        let view: V =
            serde_json::from_str(&payload_str).map_err(SqliteViewError::Deserialization)?;

        Ok(Some(view))
    }

    async fn load_with_context(
        &self,
        view_id: &str,
    ) -> Result<Option<(V, ViewContext)>, PersistenceError> {
        let query = SqlQueryFactory::select_view(&self.view_table);

        let row = sqlx::query(AssertSqlSafe(query))
            .bind(view_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(SqliteViewError::Connection)?;

        let Some(row) = row else {
            return Ok(None);
        };

        let view_id_str: String = row
            .try_get("view_id")
            .map_err(SqliteViewError::Connection)?;

        let version_i64: i64 = row
            .try_get("version")
            .map_err(SqliteViewError::Connection)?;

        let payload_str: String = row
            .try_get("payload")
            .map_err(SqliteViewError::Connection)?;

        let view: V =
            serde_json::from_str(&payload_str).map_err(SqliteViewError::Deserialization)?;

        let context = ViewContext::new(view_id_str, version_i64);

        Ok(Some((view, context)))
    }

    /// Optimistic locking strategy: version 0 means initial insert, subsequent
    /// updates check the current version matches the expected version. If the
    /// UPDATE affects 0 rows, it means the view was modified concurrently and
    /// an OptimisticLockError is returned.
    async fn update_view(&self, view: V, context: ViewContext) -> Result<(), PersistenceError> {
        self.write_view(view, context, None).await?;
        Ok(())
    }
}

impl<V, A> IndexedView<V, A> for SqliteViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    async fn find(
        &self,
        predicate: &Predicate,
        order: Option<&Order>,
        limit: i64,
    ) -> Result<Vec<String>, PersistenceError> {
        let mut arguments = sqlx::sqlite::SqliteArguments::default();
        let sql = indexed_query::compile_query(
            &self.view_table,
            indexed_query::IdentifierMode::TrustedSql,
            predicate,
            order,
            limit,
            |value| {
                match value {
                    Value::Int(integer) => arguments.add(*integer),
                    Value::Text(text) => arguments.add(text.as_str()),
                }
                .map_err(indexed_query::ProjectionQueryError::Encoding)?;
                let mut placeholder = String::new();
                arguments.format_placeholder(&mut placeholder)?;
                Ok(placeholder)
            },
        )
        .map_err(|source| PersistenceError::UnknownError(Box::new(source)))?;
        let Some(sql) = sql else {
            return Ok(vec![]);
        };
        let mut query = sqlx::QueryBuilder::<Sqlite>::with_arguments(sql, arguments);
        query
            .build_query_scalar::<String>()
            .fetch_all(&self.pool)
            .await
            .map_err(SqliteViewError::Connection)
            .map_err(PersistenceError::from)
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::event_sink::EventSink;
    use cqrs_es::{DomainEvent, EventEnvelope};
    use serde::{Deserialize, Serialize};
    use std::fmt::{self, Display};
    use std::time::Duration;

    use indexed_query::ProjectionQueryError;

    use super::*;
    use crate::testing::create_test_pool;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
    struct TestAggregate;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    enum TestEvent {
        Created,
        Updated { value: String },
    }

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> String {
            match self {
                Self::Created => "Created".to_string(),
                Self::Updated { .. } => "Updated".to_string(),
            }
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug)]
    struct TestError;

    impl Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "test error")
        }
    }

    impl std::error::Error for TestError {}

    impl Aggregate for TestAggregate {
        const TYPE: &'static str = "TestAggregate";
        type Command = ();
        type Event = TestEvent;
        type Error = TestError;
        type Services = ();

        async fn handle(
            &mut self,
            _command: Self::Command,
            _services: &Self::Services,
            _sink: &EventSink<Self>,
        ) -> Result<(), Self::Error> {
            Ok(())
        }

        fn apply(&mut self, _event: Self::Event) {}
    }

    #[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
    struct TestView {
        count: i64,
        values: Vec<String>,
    }

    impl View<TestAggregate> for TestView {
        fn update(&mut self, event: &EventEnvelope<TestAggregate>) {
            match &event.payload {
                TestEvent::Created => self.count += 1,
                TestEvent::Updated { value } => self.values.push(value.clone()),
            }
        }
    }

    #[tokio::test]
    async fn test_load_nonexistent_view() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let repo =
            SqliteViewRepository::<TestView, TestAggregate>::new(pool, "test_view".to_string());

        let result = repo.load("nonexistent").await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_load_view() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let test_view = TestView {
            count: 42,
            values: vec!["foo".to_string(), "bar".to_string()],
        };
        let payload = serde_json::to_string(&test_view).unwrap();

        sqlx::query("INSERT INTO test_view (view_id, version, payload) VALUES (?, ?, ?)")
            .bind("test-123")
            .bind(5_i64)
            .bind(&payload)
            .execute(&pool)
            .await
            .unwrap();

        let repo =
            SqliteViewRepository::<TestView, TestAggregate>::new(pool, "test_view".to_string());

        let result = repo.load("test-123").await.unwrap();

        assert!(result.is_some());
        let loaded_view = result.unwrap();
        assert_eq!(loaded_view.count, 42);
        assert_eq!(loaded_view.values, vec!["foo", "bar"]);
    }

    #[tokio::test]
    async fn explicitly_quoted_dotted_names_keep_literal_table_and_column_identity() {
        let pool = create_test_pool().await.unwrap();
        sqlx::query(
            r#"CREATE TABLE "literal.""name_projection" (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL,
                "metric.""key" BIGINT NOT NULL DEFAULT 7
            )"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        let repository = SqliteViewRepository::<TestView, TestAggregate>::new(
            pool,
            r#""literal.""name_projection""#.to_string(),
        );
        repository
            .update_view(
                TestView {
                    count: 7,
                    values: vec![],
                },
                ViewContext::new("literal".to_string(), 0),
            )
            .await
            .unwrap();
        assert_eq!(repository.load("literal").await.unwrap().unwrap().count, 7);
        let predicate = Predicate {
            any_of: vec![vec![Term {
                column: r#""metric.""key""#.to_string(),
                cmp: Cmp::Eq,
                value: Some(Value::Int(7)),
            }]],
        };
        let order = Order {
            column: r#""metric.""key""#.to_string(),
            ascending: true,
        };
        assert_eq!(
            repository.find(&predicate, Some(&order), 1).await.unwrap(),
            vec!["literal".to_string()],
        );
    }

    #[tokio::test]
    async fn qualified_indexed_names_target_the_same_table_as_load_and_update() {
        let pool = create_test_pool().await.unwrap();
        sqlx::query(
            "CREATE TABLE qualified_projection (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        let repository = SqliteViewRepository::<TestView, TestAggregate>::new(
            pool,
            "main.qualified_projection".to_string(),
        );
        repository
            .update_view(
                TestView {
                    count: 7,
                    values: vec![],
                },
                ViewContext::new("qualified".to_string(), 0),
            )
            .await
            .unwrap();
        assert_eq!(
            repository.load("qualified").await.unwrap().unwrap().count,
            7
        );
        let predicate = Predicate {
            any_of: vec![vec![Term {
                column: "qualified_projection.payload".to_string(),
                cmp: Cmp::IsNotNull,
                value: None,
            }]],
        };
        let order = Order {
            column: "qualified_projection.view_id".to_string(),
            ascending: true,
        };
        assert_eq!(
            repository.find(&predicate, Some(&order), 1).await.unwrap(),
            vec!["qualified".to_string()],
        );
    }

    #[tokio::test]
    async fn negative_indexed_limit_fails_before_empty_query_or_storage_access() {
        let pool = create_test_pool().await.unwrap();
        pool.close().await;
        let repository =
            SqliteViewRepository::<TestView, TestAggregate>::new(pool, "test_view".to_string());
        let error = repository
            .find(&Predicate { any_of: vec![] }, None, -1)
            .await
            .unwrap_err();
        let PersistenceError::UnknownError(source) = error else {
            panic!("expected typed indexed-query validation failure");
        };
        assert!(matches!(
            source.downcast_ref::<ProjectionQueryError>(),
            Some(ProjectionQueryError::NegativeLimit(-1))
        ));
    }

    #[tokio::test]
    async fn find_returns_view_ids_matching_a_dnf_predicate_ordered_and_limited() {
        let pool = create_test_pool().await.unwrap();
        sqlx::query(
            "CREATE TABLE q (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL,
                status TEXT    GENERATED ALWAYS AS (json_extract(payload, '$.status')) STORED,
                run_at INTEGER GENERATED ALWAYS AS (json_extract(payload, '$.run_at')) STORED
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        for (id, status, run_at) in [
            ("a", "pending", 10),
            ("b", "pending", 30),
            ("c", "claimed", 20),
            ("d", "done", 5),
        ] {
            let payload = serde_json::json!({ "status": status, "run_at": run_at }).to_string();
            sqlx::query("INSERT INTO q (view_id, version, payload) VALUES (?, 1, ?)")
                .bind(id)
                .bind(&payload)
                .execute(&pool)
                .await
                .unwrap();
        }

        let repo = SqliteViewRepository::<TestView, TestAggregate>::new(pool, "q".to_string());

        // (status='pending' AND run_at <= 20) OR (status='claimed'), oldest run_at first.
        let predicate = Predicate {
            any_of: vec![
                vec![
                    Term {
                        column: "status".to_string(),
                        cmp: Cmp::Eq,
                        value: Some(Value::Text("pending".to_string())),
                    },
                    Term {
                        column: "run_at".to_string(),
                        cmp: Cmp::Le,
                        value: Some(Value::Int(20)),
                    },
                ],
                vec![Term {
                    column: "status".to_string(),
                    cmp: Cmp::Eq,
                    value: Some(Value::Text("claimed".to_string())),
                }],
            ],
        };
        let order = Order {
            column: "run_at".to_string(),
            ascending: true,
        };

        let ids = repo.find(&predicate, Some(&order), 10).await.unwrap();
        // a (pending, 10) then c (claimed, 20); b excluded (run_at 30 > 20), d excluded (done).
        assert_eq!(ids, vec!["a".to_string(), "c".to_string()]);

        let limited = repo.find(&predicate, Some(&order), 1).await.unwrap();
        assert_eq!(limited, vec!["a".to_string()]);
    }

    #[tokio::test]
    async fn test_load_with_context() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let test_view = TestView {
            count: 100,
            values: vec!["alpha".to_string()],
        };
        let payload = serde_json::to_string(&test_view).unwrap();

        sqlx::query("INSERT INTO test_view (view_id, version, payload) VALUES (?, ?, ?)")
            .bind("test-456")
            .bind(7_i64)
            .bind(&payload)
            .execute(&pool)
            .await
            .unwrap();

        let repo =
            SqliteViewRepository::<TestView, TestAggregate>::new(pool, "test_view".to_string());

        let result = repo.load_with_context("test-456").await.unwrap();

        assert!(result.is_some());
        let (loaded_view, context) = result.unwrap();
        assert_eq!(loaded_view.count, 100);
        assert_eq!(loaded_view.values, vec!["alpha"]);
        assert_eq!(context.view_instance_id, "test-456");
        assert_eq!(context.version, 7);
    }

    #[test]
    fn test_optimistic_lock_error_conversion() {
        let err = SqliteViewError::OptimisticLock;
        let persistence_err: PersistenceError = err.into();

        assert!(matches!(
            persistence_err,
            PersistenceError::OptimisticLockError
        ));
    }

    #[test]
    fn test_serialization_error_conversion() {
        let json_err = serde_json::from_str::<i32>("invalid").unwrap_err();
        let err = SqliteViewError::Serialization(json_err);
        let persistence_err: PersistenceError = err.into();

        assert!(matches!(persistence_err, PersistenceError::UnknownError(_)));
    }

    #[test]
    fn test_deserialization_error_conversion() {
        let json_err = serde_json::from_str::<i32>("invalid").unwrap_err();
        let err = SqliteViewError::Deserialization(json_err);
        let persistence_err: PersistenceError = err.into();

        assert!(matches!(
            persistence_err,
            PersistenceError::DeserializationError(_)
        ));
    }

    #[test]
    fn test_try_from_int_error_conversion() {
        let int_err = i64::try_from(u64::MAX).unwrap_err();
        let err = SqliteViewError::TryFromInt(int_err);
        let persistence_err: PersistenceError = err.into();

        assert!(matches!(persistence_err, PersistenceError::UnknownError(_)));
    }

    #[test]
    fn test_error_display() {
        let err = SqliteViewError::OptimisticLock;
        assert_eq!(
            err.to_string(),
            "Optimistic lock error: view has been modified concurrently"
        );

        let json_err = serde_json::from_str::<i32>("invalid").unwrap_err();
        let err = SqliteViewError::Serialization(json_err);
        assert!(err.to_string().contains("View serialization error"));

        let json_err = serde_json::from_str::<i32>("invalid").unwrap_err();
        let err = SqliteViewError::Deserialization(json_err);
        assert!(err.to_string().contains("View deserialization error"));
    }

    #[tokio::test]
    async fn test_update_view_insert() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let repo = SqliteViewRepository::<TestView, TestAggregate>::new(
            pool.clone(),
            "test_view".to_string(),
        );

        let test_view = TestView {
            count: 10,
            values: vec!["test".to_string()],
        };

        let context = ViewContext::new("test-insert".to_string(), 0);

        repo.update_view(test_view.clone(), context).await.unwrap();

        let loaded = repo.load("test-insert").await.unwrap();
        assert!(loaded.is_some());
        let loaded_view = loaded.unwrap();
        assert_eq!(loaded_view.count, 10);
        assert_eq!(loaded_view.values, vec!["test"]);

        let row = sqlx::query("SELECT version FROM test_view WHERE view_id = ?")
            .bind("test-insert")
            .fetch_one(&pool)
            .await
            .unwrap();
        let version: i64 = row.try_get("version").unwrap();
        assert_eq!(version, 1);
    }

    #[tokio::test]
    async fn test_update_view_increment_version() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let repo = SqliteViewRepository::<TestView, TestAggregate>::new(
            pool.clone(),
            "test_view".to_string(),
        );

        let initial_view = TestView {
            count: 1,
            values: vec!["first".to_string()],
        };

        let context_v0 = ViewContext::new("test-increment".to_string(), 0);
        repo.update_view(initial_view, context_v0).await.unwrap();

        let updated_view = TestView {
            count: 2,
            values: vec!["first".to_string(), "second".to_string()],
        };

        let context_v1 = ViewContext::new("test-increment".to_string(), 1);
        repo.update_view(updated_view, context_v1).await.unwrap();

        let (loaded_view, loaded_context) = repo
            .load_with_context("test-increment")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(loaded_view.count, 2);
        assert_eq!(loaded_view.values, vec!["first", "second"]);
        assert_eq!(loaded_context.version, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_update_rejects_a_rebuild_at_the_same_checkpoint() {
        let pool = create_test_pool().await.unwrap();
        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        let repository = SqliteViewRepository::<TestView, TestAggregate>::new(
            pool.clone(),
            "test_view".to_owned(),
        );
        repository
            .update_view(
                TestView {
                    count: 0,
                    values: vec!["stale".to_owned()],
                },
                ViewContext::new("one".to_owned(), 0),
            )
            .await
            .unwrap();

        let budget = Duration::from_secs(5);
        let (loaded, wait_for_load) = tokio::sync::oneshot::channel();
        let (replaced, wait_for_replacement) = std::sync::mpsc::sync_channel(1);
        let update = tokio::spawn(async move {
            SqliteViewBackend::try_update(&repository, "one", move |view, context| {
                assert_eq!(context.version, 1);
                let mut view = view.unwrap();
                assert_eq!(view.values, ["stale"]);
                loaded.send(()).unwrap();
                tokio::task::block_in_place(|| wait_for_replacement.recv_timeout(budget)).unwrap();
                view.values.push("later".to_owned());
                ControlFlow::<(), _>::Continue(view)
            })
            .await
        });
        tokio::time::timeout(budget, wait_for_load)
            .await
            .unwrap()
            .unwrap();
        sqlx::query("UPDATE test_view SET payload = ? WHERE view_id = 'one' AND version = 1")
            .bind(r#"{"count":1,"values":["rebuilt"]}"#)
            .execute(&pool)
            .await
            .unwrap();
        replaced.send(()).unwrap();

        let error = update.await.unwrap().unwrap_err();
        assert!(matches!(
            error,
            event_sorcery_core::ViewUpdateError::Write(PersistenceError::OptimisticLockError)
        ));
        let repository =
            SqliteViewRepository::<TestView, TestAggregate>::new(pool, "test_view".to_owned());
        let (view, context) = repository.load_with_context("one").await.unwrap().unwrap();
        assert_eq!(context.version, 1);
        assert_eq!(view.values, ["rebuilt"]);
    }

    #[tokio::test]
    async fn exhausted_view_version_does_not_overwrite_the_row() {
        let pool = create_test_pool().await.unwrap();
        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO test_view (view_id, version, payload) VALUES ('one', ?, ?)")
            .bind(i64::MAX)
            .bind(r#"{"count":1,"values":["original"]}"#)
            .execute(&pool)
            .await
            .unwrap();
        let repository =
            SqliteViewRepository::<TestView, TestAggregate>::new(pool, "test_view".to_owned());

        let error = repository
            .update_view(
                TestView::default(),
                ViewContext::new("one".to_owned(), i64::MAX),
            )
            .await
            .unwrap_err();

        let PersistenceError::UnknownError(source) = error else {
            panic!("expected version exhaustion, got {error:?}")
        };
        assert!(matches!(
            source.downcast_ref::<SqliteViewError>(),
            Some(SqliteViewError::VersionExhausted)
        ));
        let (view, context) = repository.load_with_context("one").await.unwrap().unwrap();
        assert_eq!(context.version, i64::MAX);
        assert_eq!(view.values, ["original"]);
    }

    #[tokio::test]
    async fn test_view_serialization_roundtrip() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let repo =
            SqliteViewRepository::<TestView, TestAggregate>::new(pool, "test_view".to_string());

        let original_view = TestView {
            count: 42,
            values: vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()],
        };

        let context = ViewContext::new("test-roundtrip".to_string(), 0);
        repo.update_view(original_view.clone(), context)
            .await
            .unwrap();

        let loaded_view = repo.load("test-roundtrip").await.unwrap().unwrap();

        assert_eq!(loaded_view, original_view);
    }

    #[tokio::test]
    async fn test_multiple_views() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let repo =
            SqliteViewRepository::<TestView, TestAggregate>::new(pool, "test_view".to_string());

        let view1 = TestView {
            count: 1,
            values: vec!["one".to_string()],
        };
        let view2 = TestView {
            count: 2,
            values: vec!["two".to_string()],
        };
        let view3 = TestView {
            count: 3,
            values: vec!["three".to_string()],
        };

        let context1 = ViewContext::new("view-1".to_string(), 0);
        let context2 = ViewContext::new("view-2".to_string(), 0);
        let context3 = ViewContext::new("view-3".to_string(), 0);

        repo.update_view(view1.clone(), context1).await.unwrap();
        repo.update_view(view2.clone(), context2).await.unwrap();
        repo.update_view(view3.clone(), context3).await.unwrap();

        let loaded1 = repo.load("view-1").await.unwrap().unwrap();
        let loaded2 = repo.load("view-2").await.unwrap().unwrap();
        let loaded3 = repo.load("view-3").await.unwrap().unwrap();

        assert_eq!(loaded1, view1);
        assert_eq!(loaded2, view2);
        assert_eq!(loaded3, view3);
    }

    #[tokio::test]
    async fn initial_identity_conflicts_are_retryable_but_other_unique_failures_are_not() {
        let pool = create_test_pool().await.unwrap();
        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL,
                unique_count INTEGER GENERATED ALWAYS AS (json_extract(payload, '$.count')) STORED UNIQUE
            )",
        ).execute(&pool).await.unwrap();
        let repo =
            SqliteViewRepository::<TestView, TestAggregate>::new(pool, "test_view".to_string());
        let initial = TestView {
            count: 1,
            values: vec!["winner".to_string()],
        };
        repo.update_view(initial.clone(), ViewContext::new("same".to_string(), 0))
            .await
            .unwrap();
        let rejected = TestView {
            count: 2,
            values: vec!["loser".to_string()],
        };
        let error = repo
            .update_view(rejected, ViewContext::new("same".to_string(), 0))
            .await
            .unwrap_err();
        assert!(matches!(error, PersistenceError::OptimisticLockError));
        let error = repo
            .update_view(initial.clone(), ViewContext::new("same".to_string(), 0))
            .await
            .unwrap_err();
        assert!(matches!(error, PersistenceError::OptimisticLockError));
        let error = repo
            .update_view(initial.clone(), ViewContext::new("other".to_string(), 0))
            .await
            .unwrap_err();
        let PersistenceError::ConnectionError(source) = error else {
            panic!("unrelated unique constraints must retain the database error");
        };
        let sqlx::Error::Database(source) = source.downcast_ref::<sqlx::Error>().unwrap() else {
            panic!("connection error must retain its SQLx database source");
        };
        assert_eq!(source.kind(), sqlx::error::ErrorKind::UniqueViolation);
        let (view, context) = repo.load_with_context("same").await.unwrap().unwrap();
        assert_eq!(view, initial);
        assert_eq!(context.version, 1);
        assert_eq!(repo.load("other").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_optimistic_locking() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let repo =
            SqliteViewRepository::<TestView, TestAggregate>::new(pool, "test_view".to_string());

        let initial_view = TestView {
            count: 1,
            values: vec!["initial".to_string()],
        };

        let context_v0 = ViewContext::new("test-lock".to_string(), 0);
        repo.update_view(initial_view, context_v0).await.unwrap();

        let updated_view = TestView {
            count: 2,
            values: vec!["updated".to_string()],
        };

        let context_v1 = ViewContext::new("test-lock".to_string(), 1);
        repo.update_view(updated_view, context_v1).await.unwrap();

        let stale_view = TestView {
            count: 99,
            values: vec!["stale".to_string()],
        };

        let stale_context_v1 = ViewContext::new("test-lock".to_string(), 1);
        let result = repo.update_view(stale_view, stale_context_v1).await;

        assert!(matches!(
            result.unwrap_err(),
            PersistenceError::OptimisticLockError
        ));

        let (loaded_view, loaded_context) =
            repo.load_with_context("test-lock").await.unwrap().unwrap();

        assert_eq!(loaded_view.count, 2);
        assert_eq!(loaded_view.values, vec!["updated"]);
        assert_eq!(loaded_context.version, 2);
    }
}
