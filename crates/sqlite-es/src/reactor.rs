//! Retrying an idempotent reactor on SQLite lock contention.

use async_trait::async_trait;
use tracing::warn;

use event_sorcery::{
    Dependent, EntityList, RETRY_BASE_DELAY_MS, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY_MS, Reactor,
    ReactorContext, retry_with_backoff,
};

use crate::is_retryable_sqlite_busy;

/// Marks a [`Reactor`] whose `react_with_context()` is safe to retry in
/// full after a transient SQLite busy error.
///
/// # Safety contract
///
/// Implement this only if the whole delivery, including any `react_with_context()`
/// override, performs exclusively SQLite writes, with no side effect preceding the write that would double-
/// fire on retry: no HTTP/RPC calls, no `Store::send()` to another aggregate,
/// no message-queue publish. `Projection` does not need this -- it has its
/// own internal retry already, covering both optimistic-lock conflicts and
/// transient SQLite busy errors (see `Projection::react`). Reactors that
/// orchestrate across aggregates (e.g. the `RebalancingTrigger` example in
/// `docs/cqrs.md`) must NOT implement this unless the downstream command
/// handler is independently confirmed idempotent under re-invocation.
///
/// If the first of several non-transactional statements commits and a later
/// one hits `SQLITE_BUSY`, retrying repeats the already-committed statement.
/// The whole delivery must therefore be atomic (one statement or transaction)
/// or safe to replay. This applies to `react()` when using the default
/// `react_with_context()` implementation.
///
/// This is a marker trait: implementing it is a declaration, not a
/// capability check the compiler can verify -- the burden of proof is on the
/// implementor, the same as an `unsafe impl Send`. Unlike `Send`, though,
/// this trait is not `unsafe`: implementors write a plain
/// `impl IdempotentReactor for MyReactor {}` (see `docs/cqrs.md`), so treat
/// the analogy as a discipline reminder, not a compiler guarantee.
///
/// # Latency tradeoff of wrapping this in `RetryOnBusy`
///
/// `CqrsFramework::execute_with_metadata` awaits every registered reactor's
/// `dispatch()` synchronously before returning to the command caller, once per
/// event. With [`RetryOnBusy`], the caller waits for each attempt, including
/// SQLite's own busy wait, plus up to 4,270 ms of retry sleeps per event.
/// The sleep budget is not a command timeout. Even writes to other streams
/// in the same SQLite file can cause contention.
pub trait IdempotentReactor: Reactor {}

/// Wraps an [`IdempotentReactor`] to retry on transient SQLite busy errors.
///
/// Retries `react_with_context()` with exponential backoff when it fails with
/// `SQLITE_BUSY` / `SQLITE_BUSY_SNAPSHOT`. See [`IdempotentReactor`] for the
/// safety contract that gates this, including the caller-latency tradeoff.
pub struct RetryOnBusy<R> {
    /// Reactor whose complete, idempotent delivery is retried on SQLite busy errors.
    pub inner: R,
}

impl<R: Dependent> Dependent for RetryOnBusy<R> {
    type Dependencies = R::Dependencies;
}

#[async_trait]
impl<R> Reactor for RetryOnBusy<R>
where
    R: IdempotentReactor,
    R::Error: 'static,
    <R::Dependencies as EntityList>::Event: Clone,
{
    type Error = R::Error;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        self.react_with_context(event, ReactorContext { sequence: None })
            .await
    }

    async fn react_with_context(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
        context: ReactorContext,
    ) -> Result<(), Self::Error> {
        retry_with_backoff(
            RETRY_MAX_ATTEMPTS,
            RETRY_BASE_DELAY_MS,
            RETRY_MAX_DELAY_MS,
            move || {
                let event = event.clone();
                async move { self.inner.react_with_context(event, context).await }
            },
            |error: &Self::Error| is_retryable_sqlite_busy(error),
        )
        .await
        .inspect_err(|error| {
            warn!(
                ?error,
                "RetryOnBusy giving up: reactor error was not a retryable SQLite busy error, \
                 or the busy-retry budget was exhausted"
            );
        })
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use serde::{Deserialize, Serialize};
    use sqlx::Connection;
    use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
    use std::borrow::Cow;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    use super::*;
    use std::{sync::Arc, time::Duration};

    use event_sorcery::{
        CompactionPolicy, Effect, EventSourced, Never, Nil, OneOf, ProjectionError, deps,
        uneventful,
    };

    /// Minimal [`DatabaseError`] impl so tests don't need real SQLite lock
    /// contention -- just a stand-in with a controllable extended result code.
    #[derive(Debug)]
    struct TestDatabaseError {
        code: String,
    }

    impl std::fmt::Display for TestDatabaseError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "test database error (code {})", self.code)
        }
    }

    impl std::error::Error for TestDatabaseError {}

    impl sqlx::error::DatabaseError for TestDatabaseError {
        fn message(&self) -> &'static str {
            "test database error"
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(&self.code))
        }

        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    fn sqlx_error_with_code(code: &str) -> sqlx::Error {
        sqlx::Error::Database(Box::new(TestDatabaseError {
            code: code.to_string(),
        }))
    }

    #[test]
    fn is_retryable_sqlite_busy_true_for_busy_code() {
        let error = sqlx_error_with_code("5");
        assert!(is_retryable_sqlite_busy(&error));
    }

    #[test]
    fn is_retryable_sqlite_busy_true_for_busy_snapshot_code() {
        let error = sqlx_error_with_code("517");
        assert!(is_retryable_sqlite_busy(&error));
    }

    #[test]
    fn is_retryable_sqlite_busy_true_for_busy_recovery_code() {
        let error = sqlx_error_with_code("261");
        assert!(is_retryable_sqlite_busy(&error));
    }

    #[test]
    fn is_retryable_sqlite_busy_true_for_busy_timeout_code() {
        let error = sqlx_error_with_code("773");
        assert!(is_retryable_sqlite_busy(&error));
    }

    #[test]
    fn is_retryable_sqlite_busy_false_for_other_code() {
        let error = sqlx_error_with_code("19");
        assert!(!is_retryable_sqlite_busy(&error));
    }

    /// Reproduces the idiomatic downstream-reactor error shape from
    /// `docs/sqlx.md`'s transparent-wrapper pitfall:
    /// `#[error(transparent)] Sqlx(#[from] sqlx::Error)`. Thiserror's
    /// transparent delegation skips the `sqlx::Error` node itself, landing
    /// `source()` on the `Box<dyn DatabaseError>` that `sqlx::Error::Database`
    /// exposes one hop in -- the node `is_retryable_sqlite_busy` must also
    /// classify.
    #[derive(Debug, thiserror::Error)]
    enum DownstreamTransparentError {
        #[error(transparent)]
        Sqlx(#[from] sqlx::Error),
    }

    /// Counter for unique real-SQLite-file paths across concurrently running
    /// tests in this process (nextest runs tests from one binary as threads,
    /// not separate processes, so a shared fixed path would race).
    static REAL_BUSY_TEST_DB_COUNTER: AtomicU32 = AtomicU32::new(0);

    /// Provokes a genuine `SQLITE_BUSY` (extended code `"5"`) from real sqlx
    /// against a real SQLite file, then wraps it in the transparent shape
    /// above and asserts `is_retryable_sqlite_busy` still classifies it.
    ///
    /// `holder_connection` takes the RESERVED write lock via
    /// `BEGIN IMMEDIATE`; `contender_connection`, with `busy_timeout`
    /// disabled so it fails instead of waiting, then tries to write and is
    /// rejected. This also pins sqlx's `.code()` contract -- the extended
    /// result code as a decimal string -- against a real response, not just
    /// the hand-built `TestDatabaseError` used by the other tests here.
    #[tokio::test]
    async fn is_retryable_sqlite_busy_true_for_real_busy_through_transparent_wrapper() {
        let db_path = std::env::temp_dir().join(format!(
            "event_sorcery_reactor_real_busy_test_{}.sqlite3",
            REAL_BUSY_TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_file(&db_path);

        let connect_options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true)
            .busy_timeout(Duration::from_secs(0));

        let mut holder_connection = SqliteConnection::connect_with(&connect_options)
            .await
            .unwrap();
        let mut contender_connection = SqliteConnection::connect_with(&connect_options)
            .await
            .unwrap();

        sqlx::query("CREATE TABLE busy_probe (value INTEGER NOT NULL)")
            .execute(&mut holder_connection)
            .await
            .unwrap();

        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut holder_connection)
            .await
            .unwrap();
        sqlx::query("INSERT INTO busy_probe (value) VALUES (1)")
            .execute(&mut holder_connection)
            .await
            .unwrap();

        let real_busy_error = sqlx::query("INSERT INTO busy_probe (value) VALUES (2)")
            .execute(&mut contender_connection)
            .await
            .unwrap_err();

        let downstream_error = DownstreamTransparentError::from(real_busy_error);

        assert!(is_retryable_sqlite_busy(&downstream_error));

        let _ = std::fs::remove_file(&db_path);
    }

    #[derive(Debug, thiserror::Error)]
    enum InnerError {
        #[error("sqlx failure: {0}")]
        Sqlx(#[from] sqlx::Error),
    }

    #[derive(Debug, thiserror::Error)]
    enum OuterError {
        #[error("wrapped: {0}")]
        Wrapped(#[source] InnerError),
    }

    #[test]
    fn is_retryable_sqlite_busy_true_two_levels_deep_via_source_chain() {
        let error = OuterError::Wrapped(InnerError::Sqlx(sqlx_error_with_code("5")));
        assert!(is_retryable_sqlite_busy(&error));
    }

    #[derive(Debug, thiserror::Error)]
    enum ReactorLikeError {
        #[error("persistence: {0}")]
        Persistence(#[from] cqrs_es::persist::PersistenceError),
    }

    #[test]
    fn is_retryable_sqlite_busy_true_behind_persistence_error_connection_error() {
        let boxed_sqlx_error: Box<dyn std::error::Error + Send + Sync + 'static> =
            Box::new(sqlx_error_with_code("517"));
        let error = ReactorLikeError::Persistence(
            cqrs_es::persist::PersistenceError::ConnectionError(boxed_sqlx_error),
        );

        assert!(is_retryable_sqlite_busy(&error));
    }

    /// Regression test for the `#[error(transparent)]` blind spot: thiserror
    /// makes a transparent variant's `source()` delegate to the wrapped
    /// field's *own* `source()` rather than returning the field itself, so a
    /// naive walk skips straight past it. `ProjectionError::Sqlx` and
    /// `ProjectionError::Persistence` used to be declared `transparent` --
    /// this exercises the real (non-generic-instantiation-specific) type
    /// after switching those variants to an explicit `#[error("...: {0}")]`
    /// message, which makes thiserror return the wrapped field itself from
    /// `source()` so the existing walk can reach it.
    #[test]
    fn is_retryable_sqlite_busy_true_for_projection_error_sqlx_variant() {
        let error = ProjectionError::<TestEntity>::Sqlx(sqlx_error_with_code("5"));
        assert!(is_retryable_sqlite_busy(&error));
    }

    #[test]
    fn is_retryable_sqlite_busy_true_for_projection_error_persistence_variant() {
        let boxed_sqlx_error: Box<dyn std::error::Error + Send + Sync + 'static> =
            Box::new(sqlx_error_with_code("517"));
        let error = ProjectionError::<TestEntity>::Persistence(
            cqrs_es::persist::PersistenceError::ConnectionError(boxed_sqlx_error),
        );
        assert!(is_retryable_sqlite_busy(&error));
    }

    /// Mirrors the shape of `cqrs_es::AggregateError::DatabaseConnectionError`
    /// -- a boxed `dyn Error` field with no `#[source]`/`#[from]`, so the
    /// walk cannot see through it. `AggregateError<T>` itself can't be named
    /// here (it's generic over `Entity`, unknowable at a generic
    /// classification site), but this reproduces the exact blind spot: a
    /// busy error sealed behind an opaque boxed field is un-classifiable and
    /// must fail closed.
    #[derive(Debug, thiserror::Error)]
    #[error("aggregate-shaped: {0}")]
    struct AggregateShapedError(Box<dyn std::error::Error + Send + Sync + 'static>);

    #[test]
    fn is_retryable_sqlite_busy_fails_closed_behind_unsourced_aggregate_error() {
        let error = AggregateShapedError(Box::new(sqlx_error_with_code("5")));
        assert!(!is_retryable_sqlite_busy(&error));
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEntity {
        name: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEvent;

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> String {
            "TestEvent".to_string()
        }
        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[async_trait]
    impl EventSourced for TestEntity {
        type Id = String;
        type Error = Never;
        type Command = ();
        type Event = TestEvent;
        type Materialized = Nil;
        type Jobs = Nil;

        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;
        const SNAPSHOT_SIZE: usize = 10;
        const AGGREGATE_TYPE: &'static str = "TestEntity";
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;

        fn originate(_event: &TestEvent) -> Option<Self> {
            Some(Self {
                name: "test".to_string(),
            })
        }

        fn evolve(entity: &Self, _event: &TestEvent) -> Result<Option<Self>, Never> {
            Ok(Some(entity.clone()))
        }

        async fn initialize((): ()) -> Result<Effect<Self>, Never> {
            Ok(Effect::Events(vec![TestEvent]))
        }

        async fn transition(&self, _command: ()) -> Result<Effect<Self>, Never> {
            uneventful()
        }
    }

    #[derive(Debug, thiserror::Error)]
    enum FlakyReactorError {
        #[error("busy: {0}")]
        Busy(#[source] sqlx::Error),
        #[error("permanent")]
        Permanent,
    }

    /// Test reactor whose `react()` outcome is fully controlled: it can be
    /// configured to fail with a busy-classified error a fixed number of
    /// times before succeeding, or to always fail with a non-busy error.
    /// Mirrors `ConflictingRepo`'s pattern in `projection.rs`.
    struct FlakyReactor {
        remaining_busy_failures: AtomicU32,
        permanent_failure: bool,
        calls: AtomicU32,
        applied: AtomicBool,
    }

    deps!(FlakyReactor, [TestEntity]);

    #[async_trait]
    impl Reactor for FlakyReactor {
        type Error = FlakyReactorError;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);

            if self.permanent_failure {
                return Err(FlakyReactorError::Permanent);
            }

            let remaining = self.remaining_busy_failures.load(Ordering::SeqCst);
            if remaining > 0 {
                self.remaining_busy_failures
                    .store(remaining - 1, Ordering::SeqCst);
                return Err(FlakyReactorError::Busy(sqlx_error_with_code("5")));
            }

            let _ = event.into_inner();
            self.applied.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    impl IdempotentReactor for FlakyReactor {}

    #[tokio::test]
    async fn retry_on_busy_retries_busy_classified_error_then_succeeds() {
        let wrapped = RetryOnBusy {
            inner: FlakyReactor {
                remaining_busy_failures: AtomicU32::new(2),
                permanent_failure: false,
                calls: AtomicU32::new(0),
                applied: AtomicBool::new(false),
            },
        };

        let event: OneOf<(String, TestEvent), Never> = OneOf::Here(("id-1".to_string(), TestEvent));

        wrapped.react(event).await.unwrap();

        assert!(wrapped.inner.applied.load(Ordering::SeqCst));
        assert_eq!(wrapped.inner.calls.load(Ordering::SeqCst), 3);
    }

    struct SequencedReactor {
        sequence: AtomicU32,
        inner: FlakyReactor,
    }

    deps!(SequencedReactor, [TestEntity]);

    #[async_trait]
    impl Reactor for SequencedReactor {
        type Error = FlakyReactorError;

        async fn react(
            &self,
            _event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Self::Error> {
            Err(FlakyReactorError::Permanent)
        }

        async fn react_with_context(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
            context: ReactorContext,
        ) -> Result<(), Self::Error> {
            self.sequence.store(
                u32::try_from(context.sequence.unwrap().get()).unwrap(),
                Ordering::SeqCst,
            );
            self.inner.react(event).await
        }
    }

    impl IdempotentReactor for SequencedReactor {}

    #[tokio::test(start_paused = true)]
    async fn arc_and_busy_retries_preserve_persisted_delivery_context() {
        let wrapped = Arc::new(RetryOnBusy {
            inner: SequencedReactor {
                sequence: AtomicU32::new(0),
                inner: FlakyReactor {
                    remaining_busy_failures: AtomicU32::new(2),
                    permanent_failure: false,
                    calls: AtomicU32::new(0),
                    applied: AtomicBool::new(false),
                },
            },
        });
        let event: OneOf<(String, TestEvent), Never> = OneOf::Here(("id-1".to_string(), TestEvent));
        wrapped
            .react_with_context(
                event,
                ReactorContext {
                    sequence: Some(7_usize.try_into().unwrap()),
                },
            )
            .await
            .unwrap();
        assert_eq!(wrapped.inner.sequence.load(Ordering::SeqCst), 7);
        assert_eq!(wrapped.inner.inner.calls.load(Ordering::SeqCst), 3);
        assert!(wrapped.inner.inner.applied.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn retry_on_busy_does_not_retry_non_busy_error() {
        let wrapped = RetryOnBusy {
            inner: FlakyReactor {
                remaining_busy_failures: AtomicU32::new(0),
                permanent_failure: true,
                calls: AtomicU32::new(0),
                applied: AtomicBool::new(false),
            },
        };

        let event: OneOf<(String, TestEvent), Never> = OneOf::Here(("id-1".to_string(), TestEvent));

        let error = wrapped.react(event).await.unwrap_err();

        assert!(matches!(error, FlakyReactorError::Permanent));
        assert!(!wrapped.inner.applied.load(Ordering::SeqCst));
        assert_eq!(wrapped.inner.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dispatch_retries_busy_error_via_retry_on_busy_and_succeeds() {
        let reactor = Arc::new(RetryOnBusy {
            inner: FlakyReactor {
                remaining_busy_failures: AtomicU32::new(2),
                permanent_failure: false,
                calls: AtomicU32::new(0),
                applied: AtomicBool::new(false),
            },
        });
        let store = event_sorcery::TestStore::<TestEntity>::with_reactor(Arc::clone(&reactor));
        let id = "counter-1".to_owned();

        store.send(&id, ()).await.unwrap();

        assert!(reactor.inner.applied.load(Ordering::SeqCst));
        assert_eq!(reactor.inner.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn dispatch_swallows_retry_on_busy_exhaustion_after_full_schedule() {
        // Never reaches zero within RETRY_MAX_ATTEMPTS + 1 calls -- exercises
        // the real production schedule end to end (~4.3s), proving dispatch
        // still swallows the error once RetryOnBusy exhausts its budget.
        let reactor = Arc::new(RetryOnBusy {
            inner: FlakyReactor {
                remaining_busy_failures: AtomicU32::new(u32::MAX),
                permanent_failure: false,
                calls: AtomicU32::new(0),
                applied: AtomicBool::new(false),
            },
        });
        let store = event_sorcery::TestStore::<TestEntity>::with_reactor(Arc::clone(&reactor));
        let id = "counter-1".to_owned();
        let full_schedule = std::time::Duration::from_millis(4_270);
        let started = tokio::time::Instant::now();

        store.send(&id, ()).await.unwrap();

        // Paused Tokio time advances only through the retry sleeps, so the
        // elapsed virtual time IS the backoff schedule -- immediate retries
        // or removed delays fail this exactly.
        assert_eq!(started.elapsed(), full_schedule);
        assert!(!reactor.inner.applied.load(Ordering::SeqCst));
        assert_eq!(
            reactor.inner.calls.load(Ordering::SeqCst),
            RETRY_MAX_ATTEMPTS + 1
        );
    }
}
