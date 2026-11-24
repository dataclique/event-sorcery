//! Opt-in idempotent reactor retries after SQLite contention.

use async_trait::async_trait;
use tracing::warn;

pub use sqlite_es::is_retryable_sqlite_busy;

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
