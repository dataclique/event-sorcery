//! Native construction and recovery of materialized views.

use cqrs_es::Aggregate;

pub use event_sorcery_core::{ViewBackend, ViewUpdateError};

use crate::{Column, EventSourced, ProjectionError, ProjectionRecoveryMode, Table};

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
/// A live write must not replace a payload that recovery changed after the live
/// read, even at the same checkpoint. Native backends must fence this race in
/// [`ViewBackend::try_update`], for example by comparing the observed payload
/// as well as the checkpoint. The default version-only implementation is valid
/// only if the backend prevents recovery from replacing state between that read
/// and write. These rules do not apply to plain `ViewBackend` implementations
/// with opaque revisions.
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
