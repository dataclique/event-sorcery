//! SQLite contention classification and opt-in idempotent reactor retries.

use async_trait::async_trait;
use cqrs_es::persist::PersistenceError;
use sqlx::error::DatabaseError;
use tracing::warn;

use crate::dependency::{Dependent, EntityList};
use crate::reactor::{
    RETRY_BASE_DELAY_MS, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY_MS, Reactor, ReactorContext,
    retry_with_backoff,
};

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
/// event. Wrapping an `IdempotentReactor` in [`RetryOnBusy`] means a
/// busy/busy-snapshot conflict -- including one caused by an unrelated writer
/// on the same database file -- can block that caller for up to the full
/// retry budget (~4.3s) *per reacted event*: a command that emits multiple
/// events dispatches the reactor once per event, so the worst-case block is
/// that budget multiplied by the event count. See `docs/cqrs.md`'s "Retrying
/// on transient SQLite busy errors" section for the full writeup.
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
                target: "cqrs",
                ?error,
                "RetryOnBusy giving up: reactor error was not a retryable SQLite busy error, \
                 or the busy-retry budget was exhausted"
            );
        })
    }
}

/// Classifies whether an error chain contains a transient SQLite busy error.
///
/// The `SQLITE_BUSY` extended-code family (`5` plain, `261` recovery, `517`
/// snapshot, `773` timeout) is safe to retry from an event-log perspective.
///
/// Walks the `source()` chain, downcasting each node to `sqlx::Error`. A
/// downstream reactor error using the idiomatic
/// `#[error(transparent)] Sqlx(#[from] sqlx::Error)` shape makes thiserror
/// delegate `source()` past the `sqlx::Error` node to the `Box<dyn
/// DatabaseError>` that `sqlx::Error::Database` exposes one hop further in, so
/// each node is also downcast to that boxed database error.
/// `cqrs_es::persist::PersistenceError`'s boxed inner error isn't wired as
/// `#[source]` by cqrs-es, so the walk special-cases it and continues
/// manually into the box. `cqrs_es::AggregateError<T>` has the same shape but
/// is generic over the aggregate's `Entity`, so it can't be downcast from
/// fully generic code -- a busy error sealed behind it is unreachable here
/// and this function fails closed (`false`) for it. See `docs/sqlx.md` for the
/// full writeup of these shapes.
pub fn is_retryable_sqlite_busy(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);

    while let Some(this_error) = current {
        if let Some(sqlx::Error::Database(database_error)) =
            this_error.downcast_ref::<sqlx::Error>()
            && is_busy_extended_code(database_error.code().as_deref())
        {
            return true;
        }

        // A `#[error(transparent)]` sqlx wrapper delegates `source()` past the
        // `sqlx::Error` node straight to this boxed database error, so classify
        // it here too rather than miss the whole idiomatic downstream shape.
        if let Some(database_error) = this_error.downcast_ref::<Box<dyn DatabaseError>>()
            && is_busy_extended_code(database_error.code().as_deref())
        {
            return true;
        }

        let persistence_boxed_source =
            this_error
                .downcast_ref::<PersistenceError>()
                .and_then(|persistence_error| match persistence_error {
                    PersistenceError::ConnectionError(inner)
                    | PersistenceError::UnknownError(inner) => {
                        Some(inner.as_ref() as &(dyn std::error::Error + 'static))
                    }
                    PersistenceError::DeserializationError(_)
                    | PersistenceError::OptimisticLockError => None,
                });

        current = persistence_boxed_source.or_else(|| this_error.source());
    }

    false
}

/// Whether a `DatabaseError::code()` value is in SQLite's `SQLITE_BUSY`
/// extended-code family.
///
/// sqlx reports the extended result code as a decimal string; the primary code
/// is the low byte (`extended & 0xFF`). `5` is `SQLITE_BUSY`, and every extended
/// code built on it (recovery `261`, snapshot `517`, timeout `773`) is the same
/// underlying lock conflict, so match the whole family rather than enumerating
/// each extended code by hand.
fn is_busy_extended_code(code: Option<&str>) -> bool {
    match code.map(str::parse::<i32>) {
        Some(Ok(extended_code)) => extended_code & 0xFF == 5,
        Some(Err(_)) | None => false,
    }
}
