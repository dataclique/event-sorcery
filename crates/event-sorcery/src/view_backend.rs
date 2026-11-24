//! Pluggable storage backend for materialized views.
//!
//! [`ViewBackend`] is a higher-kinded-type emulation: it supplies,
//! for each `(View, Aggregate)` pair, a concrete [`ViewRepository`]
//! implementation. Database integrations supply their repository family;
//! custom backends can also provide in-memory storage for tests.

use std::ops::ControlFlow;

use cqrs_es::Aggregate;
use cqrs_es::persist::{PersistenceError, ViewContext, ViewRepository};

use indexed_query::IndexedView;

use crate::{Column, EventSourced, ProjectionError, ProjectionRecoveryMode, Table};

/// Pluggable storage backend for materialized views, supplying a
/// concrete [`ViewRepository`] for each `(View, Aggregate)` pair.
///
/// Implemented as a higher-kinded type emulation: the GAT
/// [`Repo`](Self::Repo) is a "type-level function"
/// `(View, Aggregate) -> SomeRepo` that [`crate::Projection`]
/// applies internally to obtain a repository for
/// `(Lifecycle<Entity>, Lifecycle<Entity>)`. This lets
/// `Projection<Entity, Backend>` express its repository requirement
/// without naming the `pub(crate)` `Lifecycle` type in any public
/// bound — the `Lifecycle` saturation happens inside the struct,
/// not in the impl `where` clauses.
///
/// Why this exists: Rust lacks native HKT, so we cannot write
/// `Projection<Entity, Repo<_, _>>` and have the compiler apply
/// `Repo` to `Lifecycle<Entity>`. The GAT-on-trait pattern below
/// is the standard workaround. The `Send + Sync + 'static` bound
/// on the GAT itself means downstream impls don't have to repeat
/// these bounds (and so don't reintroduce `Lifecycle` into their
/// `where` clauses).
pub trait ViewBackend: Send + Sync + 'static {
    /// View repository for views of `View` over `Aggregate`. Carries
    /// [`IndexedView`] so [`crate::Projection::find`] can run a predicate scan
    /// without naming the `pub(crate)` `Lifecycle` type in a public bound.
    type Repo<View, Agg>: ViewRepository<View, Agg> + IndexedView<View, Agg> + Send + Sync + 'static
    where
        View: cqrs_es::View<Agg> + Clone + 'static,
        Agg: Aggregate + 'static;

    /// Runs one live delivery against the observed state and its optimistic context.
    ///
    /// Backends can override this to compare more than the optimistic version when
    /// saving, for example when recovery can replace a payload at the same checkpoint.
    /// A failed comparison must report `PersistenceError::OptimisticLockError` as
    /// a write error. Returning `Break` from `apply` skips the write entirely.
    fn try_update<ViewState, AggregateType, Skip>(
        repository: &Self::Repo<ViewState, AggregateType>,
        view_id: &str,
        apply: impl FnOnce(Option<ViewState>, &ViewContext) -> ControlFlow<Skip, ViewState> + Send,
    ) -> impl Future<Output = Result<Option<Skip>, ViewUpdateError>> + Send
    where
        ViewState: cqrs_es::View<AggregateType> + Clone + 'static,
        AggregateType: Aggregate + 'static,
        Skip: Send,
    {
        async move {
            let (stored, context) = match repository
                .load_with_context(view_id)
                .await
                .map_err(ViewUpdateError::Load)?
            {
                Some((stored, context)) => (Some(stored), context),
                None => (None, ViewContext::new(view_id.to_owned(), 0)),
            };
            match apply(stored, &context) {
                ControlFlow::Break(skipped) => Ok(Some(skipped)),
                ControlFlow::Continue(view) => {
                    repository
                        .update_view(view, context)
                        .await
                        .map_err(ViewUpdateError::Write)?;
                    Ok(None)
                }
            }
        }
    }

    /// Classifies storage contention that can safely retry a complete projection update.
    /// Unknown errors are returned without retrying unless the backend opts in.
    fn is_retryable_error(_error: &(dyn std::error::Error + 'static)) -> bool {
        false
    }
}

/// Storage phase that failed during one live projection delivery.
#[derive(Debug, thiserror::Error)]
pub enum ViewUpdateError {
    /// The previous view state could not be loaded.
    #[error("failed to load the view for update")]
    Load(#[source] PersistenceError),
    /// The new view state could not be saved.
    #[error("failed to save the view update")]
    Write(#[source] PersistenceError),
}

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
