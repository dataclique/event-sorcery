//! A safer, more ergonomic interface for event-sourced entities
//! on top of cqrs-es.
//!
//! # Why this exists
//!
//! cqrs-es provides the `Aggregate` trait, but it has several
//! sharp edges that have caused production bugs:
//!
//! - **Infallible `apply`**: `Aggregate::apply(&mut self, event)`
//!   returns nothing. Financial applications cannot panic on
//!   arithmetic overflow, so every aggregate needs a wrapper to
//!   capture errors without panicking. Every aggregate in the
//!   codebase had identical boilerplate for this.
//!
//! - **Stringly-typed aggregate IDs**: `cqrs.execute("some-id",
//!   cmd)` takes `&str`, making it trivial to pass the wrong ID.
//!   This has caused production bugs.
//!
//! - **Manual schema versioning**: When aggregate or view schemas
//!   change, a stale `SCHEMA_VERSION` leaves snapshots and views
//!   in the old shape. Bumping it is the operator's responsibility;
//!   [`EventSourced::SCHEMA_VERSION`] plus startup reconciliation
//!   clears version-mismatched snapshots and rebuilds views. On load,
//!   an incompatible snapshot for a [`CompactionPolicy::Retain`]
//!   aggregate is ignored and the entity rebuilt from its
//!   always-present event history; for a
//!   [`CompactionPolicy::CompactAfterSnapshot`] aggregate it instead
//!   surfaces an error, since its events may be gone and the snapshot
//!   is the only durable state -- so state is never silently lost.
//!
//! - **Flat command handling**: A single `handle` method receives
//!   all commands regardless of lifecycle state. Implementors
//!   must manually match on (lifecycle_state, command) tuples,
//!   making it easy to accidentally reference state during
//!   initialization or forget to handle a case.
//!
//! # Design
//!
//! [`EventSourced`] replaces direct `Aggregate` usage. Domain
//! types implement `EventSourced`, and `Lifecycle` provides a
//! blanket `Aggregate` impl that bridges to cqrs-es. Consumers
//! interact through [`Store`], which enforces typed IDs and hides
//! cqrs-es internals.
//!
//! ```text
//! Domain type          Adapter             cqrs-es
//! +--------------+     +----------------+  +------------+
//! | impl         | --> | Lifecycle      |  | Aggregate  |
//! | EventSourced |     | (blanket impl) |--| trait      |
//! +--------------+     +----------------+  +------------+
//!                             |
//!                      +------+------+
//!                      | Store       |
//!                      | (typed IDs, |
//!                      |  send())    |
//!                      +-------------+
//! ```
//!
//! # Naming
//!
//! Method names follow two themes to distinguish their purpose:
//!
//! **Event-side** (replaying events to reconstruct state) uses
//! evolution-themed names:
//! - [`originate`](EventSourced::originate) -- create initial
//!   state from the first event
//! - [`evolve`](EventSourced::evolve) -- derive new state from
//!   subsequent events
//!
//! **Command-side** (processing commands to produce events) uses
//! state-machine names:
//! - [`initialize`](EventSourced::initialize) -- handle a
//!   command when no state exists yet
//! - [`transition`](EventSourced::transition) -- handle a
//!   command against existing state
//!
//! The asymmetry is intentional: commands express intent,
//! events express facts. Different verbs for different
//! semantics.
//!
//! cqrs-es names (`Aggregate`, `Query`, `View`, `DomainEvent`)
//! are deliberately avoided in our public API to make it
//! immediately obvious whether code belongs to this crate or
//! to cqrs-es.

#[cfg(doctest)]
mod configuration_contracts;

pub(crate) mod dependency;
mod dispatch;
mod engine;
mod job;
mod job_backend;
#[cfg(any(test, feature = "sqlite"))]
mod job_sqlite;
mod job_store;
mod lifecycle;
mod persistence;
mod projection;
mod projection_recovery;
mod reactor;
mod schema_registry;
#[cfg(any(test, feature = "sqlite"))]
mod sqlite_event_repository;
#[cfg(any(test, feature = "sqlite"))]
mod sqlite_projection;
#[cfg(any(test, feature = "sqlite"))]
mod sqlite_reactor;
#[cfg(any(test, feature = "sqlite"))]
mod sqlite_store;
#[cfg(any(test, feature = "test-support"))]
mod testing;
mod view_backend;
mod wire;

use async_trait::async_trait;
pub use cqrs_es::AggregateError;
use cqrs_es::CqrsFramework;
pub use cqrs_es::DomainEvent;
use cqrs_es::EventStore;
use cqrs_es::persist::PersistedEventStore;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt::{Debug, Display};
use std::str::FromStr;

pub use event_sorcery_core::{
    CompactionPolicy, ProjectionCheckpoint, ProjectionRecoveryMode, ProjectionRecoveryStorage,
    ProjectionReplayRow, Table,
};

#[doc(hidden)]
pub use dependency::Cons;
pub use dependency::Nil;
pub use dependency::{Dependent, EntityList, Fold, HasEntity, OneOf};
#[doc(hidden)]
pub use dispatch::fx_marker;
pub use dispatch::{
    Contains, DeliveryPolicy, DispatchEvent, DispatchFailure, DispatchOutcome, DispatchRefused,
    DispatchReplay, DispatchedJob, Effect, Fx, Here, Job, JobDispatch, JobInput, JobList,
    OriginDeliveryError, OriginPort, Reconciliation, Settled, SettledFailure, There, fx,
    uneventful,
};
pub use engine::{
    CommitRequest, EngineError, JobSeed, OpaqueProposedEvent, OpaqueStoredEvent, PreparedCommit,
    PreparedJob, PreparedSnapshot, SnapshotUpdate, StoredEventRow, StoredSnapshotRow,
    StreamIdentity, decode_opaque_payload, encode_opaque_payload, validate_expected_version,
};
#[cfg(any(test, feature = "sqlite"))]
pub use engine::{Engine, OpaqueCommitRequest, SnapshotWrite};
pub use job::{
    DeadReason, JobContext, JobFailure, JobId, JobOutcome, JobStoreError, Label,
    PreparedPendingJobs, StandaloneJob,
};
pub use job_backend::{
    Backoff, ClaimedJob, Clock, EventStoreBackend, JobClaimHandle, JobClaimResult, JobEnqueueError,
    JobLeaseResult, JobRuntime, JobRuntimeError, JobScheduleError, JobSettlementResult,
    JobWorkerConfig, run_job,
};
#[cfg(any(test, feature = "sqlite"))]
pub use job_sqlite::{SqliteBackend, SqliteJobError};
pub use job_store::{ClaimDecision, ClaimOutcome, ClaimRead, EventBackend, LeaseRenewal};
use lifecycle::Lifecycle;
pub use lifecycle::{LifecycleError, Never};
pub use persistence::{
    PersistenceBackend, ProjectedStoreBackend, SCHEMA_REGISTRY_ID, UnmaterializedStoreBackend,
    guard_snapshot, reconcile_schema, register_schema,
};
pub use projection::{Column, Projection, ProjectionError};
pub use projection_recovery::recover_projection;
#[cfg(any(test, feature = "sqlite"))]
pub use reactor::{IdempotentReactor, RetryOnBusy, is_retryable_sqlite_busy};
pub use reactor::{
    RETRY_BASE_DELAY_MS, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY_MS, Reactor, ReactorContext,
    retry_with_backoff,
};
#[cfg(any(test, feature = "sqlite"))]
pub use schema_registry::Reconciler;
pub use schema_registry::{
    ReconcileError, SchemaReconciliation, SchemaRegistry, SchemaTarget, validate_snapshot_history,
};
#[cfg(any(test, feature = "sqlite"))]
pub use sqlite_es::{incremental_vacuum, vacuum};
#[cfg(any(test, feature = "sqlite"))]
pub use sqlite_event_repository::SqliteEventRepository;
#[cfg(any(test, feature = "sqlite"))]
pub use sqlite_projection::SqliteViewBackend;
#[cfg(all(any(test, feature = "test-support"), any(test, feature = "sqlite")))]
pub use sqlite_store::test_store;
#[cfg(any(test, feature = "sqlite"))]
pub use sqlite_store::{
    compact_events, count_aggregates, load_all_ids, load_entity, load_ids_paginated, send_command,
};
#[cfg(any(test, feature = "test-support"))]
pub use testing::{
    ReactorHarness, SpyReactor, TestHarness, TestResult, TestStore, buffer_pending_job, replay,
    with_pending_jobs,
};
pub use view_backend::{NativeProjectionBackend, ViewBackend};
pub use wire::StoreBuilder;

pub(crate) type EsCqrs<Entity, Backend> = CqrsFramework<
    Lifecycle<Entity>,
    PersistedEventStore<<Backend as PersistenceBackend>::PersistenceRepo, Lifecycle<Entity>>,
>;

/// apalis items that [`build_supervised_worker!`] expands to, so consumers need
/// not depend on `apalis-core` directly. Not part of the stable API.
#[doc(hidden)]
pub mod __worker {
    pub use apalis_core::monitor::Monitor;
    pub use apalis_core::task::data::Data;
    pub use apalis_core::worker::builder::WorkerBuilder;

    pub use crate::job_backend::JobInput;
}

/// The core abstraction for event-sourced domain entities.
///
/// Implement this trait on your domain type (e.g., `Position`,
/// `OffchainOrder`) to get a complete event-sourcing setup:
/// `Lifecycle` provides a blanket `Aggregate` impl, and
/// [`Store`] provides type-safe command dispatch.
///
/// # Associated types
///
/// - `Id`: The strongly-typed aggregate identifier. Prevents
///   mixing up IDs between different entity types at compile
///   time. Converted to string at the cqrs-es boundary only.
/// - `Error`: Domain-specific errors from command handling or
///   event application (e.g., arithmetic overflow). For
///   entities with infallible operations, use [`Never`].
/// - `Command`: Instructions that produce events. A single
///   command type is used for both initialization and
///   transitions -- the lifecycle routes based on state.
/// - `Event`: Domain events that drive state changes. Must be
///   `Eq` so lifecycle error states can carry typed events.
/// - `Jobs`: Type-level list of the [`Job`] types this entity may
///   dispatch (`jobs![ChargeCard, SendReceipt]`, or [`Nil`] for
///   none). Handlers kick a job off by returning
///   [`Effect::Dispatch`]; the framework enqueues it in the same
///   transaction that commits the `Dispatched` event.
///
/// # Constants
///
/// - `SCHEMA_VERSION`: Bump when the entity's state, event, or
///   view schema changes. On startup, the wiring infrastructure
///   detects version mismatches and automatically clears stale
///   snapshots and replays views.
/// - `AGGREGATE_TYPE`: Stable identifier for the event store.
///   Must not change after events are persisted.
///
/// # Event-side methods
///
/// These reconstruct state from the event log during replay.
/// They are called by the blanket `Aggregate::apply` impl on
/// `Lifecycle`, never by application code directly.
///
/// - `originate`: Attempt to create initial state from an
///   event. Returns `Some(state)` for genesis events, `None`
///   for events that require existing state.
/// - `evolve`: Attempt to derive new state from an event
///   applied to existing state. Returns `Ok(Some(new_state))`
///   on success, `Ok(None)` if the event doesn't apply to the
///   current state (mismatch), or `Err` for domain failures
///   like arithmetic overflow.
///
/// # Command-side methods
///
/// These process commands to produce events. They are called by
/// the blanket `Aggregate::handle` impl on `Lifecycle`, which
/// routes commands based on lifecycle state.
///
/// - `initialize`: Handle a command when the entity doesn't
///   exist yet. Has no `&self` parameter, preventing accidental
///   reference to existing state during creation.
/// - `transition`: Handle a command against existing state.
///   Receives `&self` (the domain type, not `Lifecycle`), so
///   the handler only deals with live state.
#[async_trait]
pub trait EventSourced:
    Clone + Debug + Send + Sync + Sized + Serialize + DeserializeOwned + 'static
{
    /// Aggregate identity type, used as the key in the event store.
    type Id: Debug + Display + FromStr + Clone + Send + Sync;
    /// Domain error type returned by command handlers and event
    /// application.
    type Error: DomainError;
    /// Command type that drives state transitions.
    type Command: Send + Sync;
    /// Domain event type emitted by commands and applied during replay.
    type Event: DomainEvent;
    /// Whether this entity has a materialized view.
    ///
    /// Set to `Table` with `PROJECTION = Table("view_name")` for
    /// entities with materialized views. Set to `Nil` with
    /// `PROJECTION = Nil` for entities without views.
    ///
    /// [`StoreBuilder::build()`] uses this to auto-wire projections:
    /// `Table` entities return `(Store, Projection)`, `Nil` entities
    /// return just `Store`.
    type Materialized;
    /// Type-level list of the [`Job`] types this entity may dispatch.
    ///
    /// Write it with the [`jobs!`] macro (`jobs![ChargeCard, SendReceipt]`),
    /// or [`Nil`] for an entity that dispatches no jobs. A handler kicks a
    /// job off by returning [`Effect::Dispatch`], and
    /// [`DispatchedJob::dispatch`] compile-checks that the job is declared
    /// here -- dispatching an undeclared job is a compile error. The
    /// framework enqueues the job in the same transaction that commits the
    /// `Dispatched` event.
    type Jobs: JobList;

    /// Projection table name (for `Table` entities) or `Nil`.
    const PROJECTION: Self::Materialized;
    /// Schema version for migration reconciliation. Bump when the
    /// event schema changes.
    const SCHEMA_VERSION: u64;
    /// How many events between automatic snapshot boundaries.
    ///
    /// A value of `1` triggers a snapshot write for every command that emits
    /// events -- ideal for compactable aggregates where the snapshot
    /// is the durable source of pre-compaction state. Retained
    /// aggregates with low event counts per instance benefit from a
    /// larger value (e.g., 10-50) to reduce write amplification.
    ///
    /// Each implementation must select this interval explicitly.
    const SNAPSHOT_SIZE: usize;
    /// Unique string identifying this aggregate type in the event
    /// store. Must be stable across deployments.
    const AGGREGATE_TYPE: &'static str;
    /// Event retention policy for this entity.
    ///
    /// Financial audit aggregates must explicitly select
    /// [`CompactionPolicy::Retain`]. Only observational aggregates
    /// whose old events have no audit value should opt into compaction.
    const COMPACTION_POLICY: CompactionPolicy;

    /// Create initial state from a genesis event.
    ///
    /// Returns `Some(state)` if this event creates the entity,
    /// `None` if it requires existing state. Returning `None`
    /// causes `Lifecycle` to enter a `Failed` state with a
    /// [`LifecycleError::EventCantOriginate`].
    fn originate(event: &Self::Event) -> Option<Self>;

    /// Derive new entity from an event applied to the current one.
    ///
    /// - `Ok(Some(new_entity))` -- event applied successfully
    /// - `Ok(None)` -- event doesn't apply to current entity
    ///   (becomes [`LifecycleError::UnexpectedEvent`])
    /// - `Err(error)` -- domain error during application
    ///   (becomes [`LifecycleError::Apply`])
    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error>;

    /// Handle a command when the entity doesn't exist yet.
    ///
    /// No `&self` -- impossible to accidentally reference existing state
    /// during creation. Returns an [`Effect`]: pure domain events, or
    /// exactly one job dispatch (via [`DispatchedJob::dispatch`]) whose
    /// `Dispatched` event and enqueue the framework commits together.
    async fn initialize(command: Self::Command) -> Result<Effect<Self>, Self::Error>;

    /// Handle a command against existing state.
    ///
    /// `&self` is the domain type directly, not `Lifecycle`. The handler
    /// only deals with live state; lifecycle routing is handled by the
    /// blanket `Aggregate` impl. Returns an [`Effect`]: pure domain events,
    /// or exactly one job dispatch.
    async fn transition(&self, command: Self::Command) -> Result<Effect<Self>, Self::Error>;
}

/// Type-safe command dispatch for an event-sourced entity.
///
/// Wraps `SqliteCqrs<Lifecycle<Entity>>` and enforces that
/// commands are addressed to the correct entity type via
/// strongly-typed IDs. This prevents a class of bugs where
/// string aggregate IDs are mixed up between different entity
/// types.
///
/// # Usage
///
/// ```ignore
/// let positions: Store<Position, SqliteBackend> = /* built by StoreBuilder */;
///
/// // Typed ID -- can't accidentally pass an OffchainOrderId
/// let symbol = Symbol::new("AAPL").unwrap();
/// positions.send(&symbol, PositionCommand::AcknowledgeFill { .. }).await?;
/// ```
///
/// Produced by [`StoreBuilder::build()`] during conductor
/// startup. The builder handles CQRS framework construction,
/// query wiring, and schema reconciliation, returning a
/// ready-to-use `Store`.
pub struct Store<Entity: EventSourced, Backend: PersistenceBackend> {
    cqrs: EsCqrs<Entity, Backend>,
    event_store: PersistedEventStore<Backend::PersistenceRepo, Lifecycle<Entity>>,
}

impl<Entity: EventSourced, Backend: PersistenceBackend> Store<Entity, Backend> {
    /// Wrap an existing CQRS framework built over `backend`.
    ///
    /// Prefer using `StoreBuilder::build()` which handles wiring
    /// and reconciliation. This constructor exists for cases
    /// where direct construction is needed (e.g., tests).
    pub(crate) fn new(cqrs: EsCqrs<Entity, Backend>, backend: &Backend) -> Self {
        let event_store = PersistedEventStore::new_snapshot_store(
            backend.persistence_repo(Entity::COMPACTION_POLICY),
            Entity::SNAPSHOT_SIZE,
        );
        Self { cqrs, event_store }
    }

    /// Send a command to the entity identified by `id`.
    ///
    /// The command is routed based on the entity's lifecycle
    /// state:
    /// - Uninitialized -> `Entity::initialize`
    /// - Live -> `Entity::transition`
    /// - Failed -> returns the stored error
    pub async fn send(
        &self,
        id: &Entity::Id,
        command: Entity::Command,
    ) -> Result<(), SendError<Entity>> {
        // Open a pending-job scope so the job an `Effect::Dispatch` carries is
        // drained by the repository's flush in the same commit transaction.
        job::with_pending_scope(self.cqrs.execute(&id.to_string(), command)).await
    }

    /// Load an entity's current state directly from the event store.
    ///
    /// Replays events to reconstruct aggregate state. No query
    /// processors are dispatched.
    ///
    /// Returns:
    /// - `Ok(Some(entity))` if the entity is live
    /// - `Ok(None)` if the entity has not been initialized
    /// - `Err` if the entity is in a failed lifecycle state or on infrastructure error
    pub async fn load(&self, id: &Entity::Id) -> Result<Option<Entity>, SendError<Entity>> {
        let context = self.event_store.load_aggregate(&id.to_string()).await?;

        Ok(context.aggregate.into_result()?)
    }
}

/// Error returned by [`Store::send`] and [`Store::load`].
///
/// Wraps the cqrs-es `AggregateError` containing a
/// `LifecycleError` so that consumers don't import from cqrs-es
/// or lifecycle directly.
pub type SendError<Entity> = AggregateError<LifecycleError<Entity>>;

impl<Entity: EventSourced> From<LifecycleError<Entity>> for SendError<Entity> {
    fn from(error: LifecycleError<Entity>) -> Self {
        Self::UserError(error)
    }
}

/// Bounds required for domain error types used with
/// [`EventSourced`].
///
/// [`LifecycleError`] stores the entity's error in its `Apply`
/// variant and derives `Clone`, `Serialize`, `Deserialize`,
/// `PartialEq`, and `Eq`. This trait captures those bounds in
/// one place so implementors see a single meaningful name
/// instead of a long bound list.
pub trait DomainError:
    std::error::Error + Clone + Serialize + DeserializeOwned + Send + Sync
{
}

impl<T> DomainError for T where
    T: std::error::Error + Clone + Serialize + DeserializeOwned + Send + Sync
{
}

/// Errors that can occur when loading all aggregate IDs.
#[derive(Debug, thiserror::Error)]
pub enum LoadAllIdsError {
    #[error("Database error: {0}")]
    Sql(#[from] sqlx::Error),
    #[error(
        "Found unparseable aggregate IDs for {aggregate_type}: \
         {ids:?}"
    )]
    InvalidIds {
        aggregate_type: &'static str,
        ids: Vec<String>,
    },
    #[error("Numeric conversion error: {0}")]
    NumericConversion(#[from] std::num::TryFromIntError),
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use serde::{Deserialize, Serialize};
    use sqlx::SqlitePool;

    use super::*;

    /// Numeric-only ID that rejects non-numeric strings.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct NumericId(u64);

    impl Display for NumericId {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "{}", self.0)
        }
    }

    impl FromStr for NumericId {
        type Err = std::num::ParseIntError;

        fn from_str(value: &str) -> Result<Self, Self::Err> {
            value.parse::<u64>().map(NumericId)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Widget {
        name: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum WidgetEvent {
        Created { name: String },
        Renamed { name: String },
    }

    impl DomainEvent for WidgetEvent {
        fn event_type(&self) -> String {
            match self {
                Self::Created { .. } => "WidgetEvent::Created".to_string(),
                Self::Renamed { .. } => "WidgetEvent::Renamed".to_string(),
            }
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
    #[error("widget error")]
    struct WidgetError;

    enum WidgetCommand {
        Create { name: String },
        Rename { name: String },
    }

    #[async_trait]
    impl EventSourced for Widget {
        type Id = NumericId;
        type Error = WidgetError;
        type Command = WidgetCommand;
        type Event = WidgetEvent;
        type Materialized = Nil;
        type Jobs = Nil;

        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;
        const SNAPSHOT_SIZE: usize = 1;
        const AGGREGATE_TYPE: &'static str = "Widget";
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::CompactAfterSnapshot;

        fn originate(event: &WidgetEvent) -> Option<Self> {
            match event {
                WidgetEvent::Created { name } => Some(Self { name: name.clone() }),
                WidgetEvent::Renamed { .. } => None,
            }
        }

        fn evolve(_entity: &Self, event: &WidgetEvent) -> Result<Option<Self>, WidgetError> {
            match event {
                WidgetEvent::Created { .. } => Ok(None),
                WidgetEvent::Renamed { name } => Ok(Some(Self { name: name.clone() })),
            }
        }

        async fn initialize(command: WidgetCommand) -> Result<Effect<Self>, WidgetError> {
            match command {
                WidgetCommand::Create { name } => {
                    Ok(Effect::Events(vec![WidgetEvent::Created { name }]))
                }
                WidgetCommand::Rename { name } => {
                    Ok(Effect::Events(vec![WidgetEvent::Renamed { name }]))
                }
            }
        }

        async fn transition(&self, command: WidgetCommand) -> Result<Effect<Self>, WidgetError> {
            match command {
                WidgetCommand::Create { .. } => uneventful(),
                WidgetCommand::Rename { name } => {
                    Ok(Effect::Events(vec![WidgetEvent::Renamed { name }]))
                }
            }
        }
    }

    /// The same widget under `RetainLast`: compaction keeps the newest
    /// two events per aggregate instead of deleting to the snapshot.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct WindowedWidget<const EVENTS: usize = 2, const SNAPSHOT_SIZE: usize = 1> {
        name: String,
    }

    #[async_trait]
    impl<const EVENTS: usize, const SNAPSHOT_SIZE: usize> EventSourced
        for WindowedWidget<EVENTS, SNAPSHOT_SIZE>
    {
        type Id = NumericId;
        type Error = WidgetError;
        type Command = WidgetCommand;
        type Event = WidgetEvent;
        type Materialized = Nil;
        type Jobs = Nil;

        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;
        const SNAPSHOT_SIZE: usize = SNAPSHOT_SIZE;
        const AGGREGATE_TYPE: &'static str = "WindowedWidget";
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::RetainLast { events: EVENTS };

        fn originate(event: &WidgetEvent) -> Option<Self> {
            match event {
                WidgetEvent::Created { name } => Some(Self { name: name.clone() }),
                WidgetEvent::Renamed { .. } => None,
            }
        }

        fn evolve(_entity: &Self, event: &WidgetEvent) -> Result<Option<Self>, WidgetError> {
            match event {
                WidgetEvent::Created { .. } => Ok(None),
                WidgetEvent::Renamed { name } => Ok(Some(Self { name: name.clone() })),
            }
        }

        async fn initialize(command: WidgetCommand) -> Result<Effect<Self>, WidgetError> {
            match command {
                WidgetCommand::Create { name } => {
                    Ok(Effect::Events(vec![WidgetEvent::Created { name }]))
                }
                WidgetCommand::Rename { name } => {
                    Ok(Effect::Events(vec![WidgetEvent::Renamed { name }]))
                }
            }
        }

        async fn transition(&self, command: WidgetCommand) -> Result<Effect<Self>, WidgetError> {
            match command {
                WidgetCommand::Create { .. } => uneventful(),
                WidgetCommand::Rename { name } => {
                    Ok(Effect::Events(vec![WidgetEvent::Renamed { name }]))
                }
            }
        }
    }

    async fn test_pool() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlite_es::MIGRATOR.run(&pool).await.unwrap();
        pool
    }

    async fn insert_event(pool: &SqlitePool, aggregate_type: &str, aggregate_id: &str) {
        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, \
             event_type, event_version, payload, metadata) \
             VALUES (?1, ?2, 1, 'WidgetEvent::Created', '1.0', ?3, '{}')",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(r#"{"Created":{"name":"test-widget"}}"#)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn load_entity_replays_events_into_entity() {
        let pool = test_pool().await;
        let store = test_store::<Widget>(pool.clone());

        store
            .send(
                &NumericId(42),
                WidgetCommand::Create {
                    name: "test-widget".to_string(),
                },
            )
            .await
            .unwrap();

        let entity = load_entity::<Widget>(&pool, &NumericId(42)).await.unwrap();

        let widget = entity.expect("entity should exist after event replay");
        assert_eq!(widget.name, "test-widget");
    }

    #[tokio::test]
    async fn load_entity_uses_snapshot_after_events_are_compacted() {
        let pool = test_pool().await;
        let store = test_store::<Widget>(pool.clone());

        store
            .send(
                &NumericId(42),
                WidgetCommand::Create {
                    name: "first".to_string(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &NumericId(42),
                WidgetCommand::Rename {
                    name: "second".to_string(),
                },
            )
            .await
            .unwrap();

        let deleted = compact_events::<Widget>(&pool).await.unwrap();

        assert_eq!(deleted, 2);
        let event_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events \
             WHERE aggregate_type = 'Widget' AND aggregate_id = '42'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(event_count, 0);

        let entity = load_entity::<Widget>(&pool, &NumericId(42)).await.unwrap();

        let widget = entity.expect("entity should load from snapshot after compaction");
        assert_eq!(widget.name, "second");
    }

    #[tokio::test]
    async fn commands_still_commit_after_events_are_compacted() {
        let pool = test_pool().await;
        let store = test_store::<Widget>(pool.clone());

        store
            .send(
                &NumericId(42),
                WidgetCommand::Create {
                    name: "first".to_string(),
                },
            )
            .await
            .unwrap();

        // Compaction leaves the events table reporting a lower maximum sequence
        // than the aggregate's real version; a write must not read that as a
        // concurrency conflict.
        let deleted = compact_events::<Widget>(&pool).await.unwrap();
        assert_eq!(deleted, 1);

        store
            .send(
                &NumericId(42),
                WidgetCommand::Rename {
                    name: "second".to_string(),
                },
            )
            .await
            .unwrap();

        let entity = load_entity::<Widget>(&pool, &NumericId(42)).await.unwrap();
        let widget = entity.expect("entity should exist after a post-compaction command");
        assert_eq!(widget.name, "second");
    }

    #[tokio::test]
    async fn oversized_retention_window_is_rejected() {
        let pool = test_pool().await;
        let error = compact_events::<WindowedWidget<{ usize::MAX }>>(&pool)
            .await
            .unwrap_err();
        let sqlx::Error::Encode(source) = error else {
            panic!("retention window conversion must report an encoding error");
        };
        source.downcast::<std::num::TryFromIntError>().unwrap();
    }

    #[tokio::test]
    async fn windowed_compaction_preserves_the_unsnapshotted_tail() {
        let pool = test_pool().await;
        let store = test_store::<WindowedWidget<1, 3>>(pool.clone());
        store
            .send(
                &NumericId(7),
                WidgetCommand::Create {
                    name: "first".to_string(),
                },
            )
            .await
            .unwrap();
        for name in ["second", "third", "fourth", "fifth"] {
            store
                .send(
                    &NumericId(7),
                    WidgetCommand::Rename {
                        name: name.to_string(),
                    },
                )
                .await
                .unwrap();
        }

        assert_eq!(
            compact_events::<WindowedWidget<1, 3>>(&pool).await.unwrap(),
            3
        );
        let remaining: Vec<i64> = sqlx::query_scalar(
            "SELECT sequence FROM events WHERE aggregate_type = 'WindowedWidget' ORDER BY sequence",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(remaining, vec![4, 5]);
        let entity = load_entity::<WindowedWidget<1, 3>>(&pool, &NumericId(7))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entity.name, "fifth");
    }

    #[tokio::test]
    async fn retain_last_keeps_the_newest_window_and_the_entity_loads() {
        let pool = test_pool().await;
        let store = test_store::<WindowedWidget>(pool.clone());

        store
            .send(
                &NumericId(7),
                WidgetCommand::Create {
                    name: "first".to_string(),
                },
            )
            .await
            .unwrap();
        for renamed in ["second", "third", "fourth", "fifth"] {
            store
                .send(
                    &NumericId(7),
                    WidgetCommand::Rename {
                        name: renamed.to_string(),
                    },
                )
                .await
                .unwrap();
        }

        let deleted = compact_events::<WindowedWidget>(&pool).await.unwrap();

        assert_eq!(deleted, 3, "five events minus the two-event window");
        let remaining: Vec<i64> = sqlx::query_scalar(
            "SELECT sequence FROM events \
             WHERE aggregate_type = 'WindowedWidget' AND aggregate_id = '7' \
             ORDER BY sequence",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(remaining, vec![4, 5], "the newest window survives");

        let entity = load_entity::<WindowedWidget>(&pool, &NumericId(7))
            .await
            .unwrap()
            .expect("the entity loads from snapshot plus the retained window");
        assert_eq!(entity.name, "fifth");
    }

    #[tokio::test]
    async fn windowed_compaction_isolates_streams_and_preserves_unsnapshotted_events() {
        let pool = test_pool().await;
        let windowed = test_store::<WindowedWidget<2, 5>>(pool.clone());
        let unrelated = test_store::<Widget>(pool.clone());

        for (aggregate_id, event_count) in [(7, 6), (8, 11), (9, 4)] {
            windowed
                .send(
                    &NumericId(aggregate_id),
                    WidgetCommand::Create {
                        name: "initial".to_string(),
                    },
                )
                .await
                .unwrap();

            for sequence in 2..=event_count {
                windowed
                    .send(
                        &NumericId(aggregate_id),
                        WidgetCommand::Rename {
                            name: format!("version-{sequence}"),
                        },
                    )
                    .await
                    .unwrap();
            }
        }

        unrelated
            .send(
                &NumericId(7),
                WidgetCommand::Create {
                    name: "unrelated".to_string(),
                },
            )
            .await
            .unwrap();

        for sequence in 2..=12 {
            unrelated
                .send(
                    &NumericId(7),
                    WidgetCommand::Rename {
                        name: format!("unrelated-{sequence}"),
                    },
                )
                .await
                .unwrap();
        }

        let snapshots: Vec<(String, i64)> = sqlx::query_as(
            "SELECT aggregate_id, last_sequence FROM snapshots \
             WHERE aggregate_type = 'WindowedWidget' ORDER BY aggregate_id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(snapshots, vec![("7".to_string(), 5), ("8".to_string(), 10)]);

        assert_eq!(
            compact_events::<WindowedWidget<2, 5>>(&pool).await.unwrap(),
            13,
        );

        for (aggregate_type, aggregate_id, expected) in [
            ("WindowedWidget", "7", vec![5, 6]),
            ("WindowedWidget", "8", vec![10, 11]),
            ("WindowedWidget", "9", vec![1, 2, 3, 4]),
            ("Widget", "7", (1..=12).collect()),
        ] {
            let remaining: Vec<i64> = sqlx::query_scalar(
                "SELECT sequence FROM events \
                 WHERE aggregate_type = ?1 AND aggregate_id = ?2 \
                 ORDER BY sequence",
            )
            .bind(aggregate_type)
            .bind(aggregate_id)
            .fetch_all(&pool)
            .await
            .unwrap();

            assert_eq!(remaining, expected, "{aggregate_type}/{aggregate_id}");
        }
    }

    #[tokio::test]
    async fn snapshot_version_advances_across_loads() {
        let pool = test_pool().await;
        let store = test_store::<Widget>(pool.clone());

        store
            .send(
                &NumericId(42),
                WidgetCommand::Create {
                    name: "first".to_string(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &NumericId(42),
                WidgetCommand::Rename {
                    name: "second".to_string(),
                },
            )
            .await
            .unwrap();

        let snapshot_version: i64 = sqlx::query_scalar(
            "SELECT snapshot_version FROM snapshots \
             WHERE aggregate_type = 'Widget' AND aggregate_id = '42'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(snapshot_version, 2);
    }

    #[tokio::test]
    async fn snapshot_rebuild_applies_events_exactly_once() {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        struct Tally {
            count: u64,
        }

        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        enum TallyEvent {
            Started,
            Incremented,
        }

        impl DomainEvent for TallyEvent {
            fn event_type(&self) -> String {
                format!("{self:?}")
            }

            fn event_version(&self) -> String {
                "1.0".to_string()
            }
        }

        enum TallyCommand {
            Start,
            Increment,
        }

        #[async_trait]
        impl EventSourced for Tally {
            type Id = NumericId;
            type Error = WidgetError;
            type Command = TallyCommand;
            type Event = TallyEvent;
            type Materialized = Nil;
            type Jobs = Nil;

            const PROJECTION: Nil = Nil;
            const SCHEMA_VERSION: u64 = 1;
            const SNAPSHOT_SIZE: usize = 1;
            const AGGREGATE_TYPE: &'static str = "Tally";
            const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;

            fn originate(event: &TallyEvent) -> Option<Self> {
                match event {
                    TallyEvent::Started => Some(Self { count: 0 }),
                    TallyEvent::Incremented => None,
                }
            }

            fn evolve(entity: &Self, event: &TallyEvent) -> Result<Option<Self>, WidgetError> {
                match event {
                    TallyEvent::Started => Ok(None),
                    TallyEvent::Incremented => Ok(Some(Self {
                        count: entity.count + 1,
                    })),
                }
            }

            async fn initialize(command: TallyCommand) -> Result<Effect<Self>, WidgetError> {
                match command {
                    TallyCommand::Start => Ok(Effect::Events(vec![TallyEvent::Started])),
                    TallyCommand::Increment => Ok(Effect::Events(vec![TallyEvent::Incremented])),
                }
            }

            async fn transition(&self, command: TallyCommand) -> Result<Effect<Self>, WidgetError> {
                match command {
                    TallyCommand::Start => uneventful(),
                    TallyCommand::Increment => Ok(Effect::Events(vec![TallyEvent::Incremented])),
                }
            }
        }

        let pool = test_pool().await;
        let store = test_store::<Tally>(pool.clone());

        // SNAPSHOT_SIZE = 1 forces commit's snapshot rebuild
        // (`update_snapshot_with_events`) after every command, exercising the
        // re-apply path that requires `Lifecycle::handle` to leave `self` at
        // its pre-command state. A counting entity makes double-application
        // visible as a wrong count rather than a coincidentally-equal value.
        store
            .send(&NumericId(1), TallyCommand::Start)
            .await
            .unwrap();
        store
            .send(&NumericId(1), TallyCommand::Increment)
            .await
            .unwrap();
        store
            .send(&NumericId(1), TallyCommand::Increment)
            .await
            .unwrap();

        let entity = load_entity::<Tally>(&pool, &NumericId(1)).await.unwrap();
        let tally = entity.expect("tally should exist after three commands");
        assert_eq!(tally.count, 2);

        let payload: String = sqlx::query_scalar(
            "SELECT payload FROM snapshots \
             WHERE aggregate_type = 'Tally' AND aggregate_id = '1'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let snapshot: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(snapshot, serde_json::json!({"Live": {"count": 2}}));
    }

    #[tokio::test]
    async fn retain_policy_does_not_compact_events() {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        struct RetainedWidget {
            name: String,
        }

        #[async_trait]
        impl EventSourced for RetainedWidget {
            type Id = NumericId;
            type Error = WidgetError;
            type Command = WidgetCommand;
            type Event = WidgetEvent;
            type Materialized = Nil;
            type Jobs = Nil;

            const PROJECTION: Nil = Nil;
            const SCHEMA_VERSION: u64 = 1;
            const SNAPSHOT_SIZE: usize = 10;
            const AGGREGATE_TYPE: &'static str = "RetainedWidget";
            const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;

            fn originate(event: &WidgetEvent) -> Option<Self> {
                match event {
                    WidgetEvent::Created { name } => Some(Self { name: name.clone() }),
                    WidgetEvent::Renamed { .. } => None,
                }
            }

            fn evolve(_entity: &Self, event: &WidgetEvent) -> Result<Option<Self>, WidgetError> {
                match event {
                    WidgetEvent::Created { .. } => Ok(None),
                    WidgetEvent::Renamed { name } => Ok(Some(Self { name: name.clone() })),
                }
            }

            async fn initialize(command: WidgetCommand) -> Result<Effect<Self>, WidgetError> {
                match command {
                    WidgetCommand::Create { name } => {
                        Ok(Effect::Events(vec![WidgetEvent::Created { name }]))
                    }
                    WidgetCommand::Rename { name } => {
                        Ok(Effect::Events(vec![WidgetEvent::Renamed { name }]))
                    }
                }
            }

            async fn transition(
                &self,
                command: WidgetCommand,
            ) -> Result<Effect<Self>, WidgetError> {
                match command {
                    WidgetCommand::Create { .. } => uneventful(),
                    WidgetCommand::Rename { name } => {
                        Ok(Effect::Events(vec![WidgetEvent::Renamed { name }]))
                    }
                }
            }
        }

        let pool = test_pool().await;
        let store = test_store::<RetainedWidget>(pool.clone());
        store
            .send(
                &NumericId(7),
                WidgetCommand::Create {
                    name: "kept".to_string(),
                },
            )
            .await
            .unwrap();

        let deleted = compact_events::<RetainedWidget>(&pool).await.unwrap();

        assert_eq!(deleted, 0);
        let event_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events \
             WHERE aggregate_type = 'RetainedWidget' AND aggregate_id = '7'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(event_count, 1);
    }

    #[tokio::test]
    async fn load_entity_returns_none_when_no_events() {
        let pool = test_pool().await;

        let entity = load_entity::<Widget>(&pool, &NumericId(999)).await.unwrap();

        assert!(entity.is_none(), "expected None for nonexistent aggregate");
    }

    #[tokio::test]
    async fn load_all_ids_returns_parsed_ids() {
        let pool = test_pool().await;
        insert_event(&pool, "Widget", "10").await;
        insert_event(&pool, "Widget", "20").await;

        let ids = load_all_ids::<Widget>(&pool).await.unwrap();

        assert_eq!(ids, vec![NumericId(10), NumericId(20)]);
    }

    #[tokio::test]
    async fn load_ids_paginated_orders_by_maximum_rowid_after_an_append() {
        let pool = test_pool().await;
        let store = test_store::<Widget>(pool.clone());
        for id in [1, 2] {
            store
                .send(
                    &NumericId(id),
                    WidgetCommand::Create {
                        name: id.to_string(),
                    },
                )
                .await
                .unwrap();
        }
        store
            .send(
                &NumericId(1),
                WidgetCommand::Rename {
                    name: "renamed".to_string(),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            load_ids_paginated::<Widget>(&pool, 1, 0).await.unwrap(),
            [NumericId(1)]
        );
        assert_eq!(
            load_ids_paginated::<Widget>(&pool, 1, 1).await.unwrap(),
            [NumericId(2)]
        );
        assert_eq!(load_ids_paginated::<Widget>(&pool, 1, 2).await.unwrap(), []);
    }

    #[tokio::test]
    async fn load_all_ids_includes_snapshot_only_aggregates() {
        let pool = test_pool().await;
        let store = test_store::<Widget>(pool.clone());
        store
            .send(
                &NumericId(30),
                WidgetCommand::Create {
                    name: "snapshot-only".to_string(),
                },
            )
            .await
            .unwrap();
        compact_events::<Widget>(&pool).await.unwrap();

        let ids = load_all_ids::<Widget>(&pool).await.unwrap();

        assert_eq!(ids, vec![NumericId(30)]);
    }

    #[tokio::test]
    async fn load_all_ids_returns_empty_when_no_events() {
        let pool = test_pool().await;

        let ids = load_all_ids::<Widget>(&pool).await.unwrap();

        assert!(ids.is_empty());
    }

    #[tokio::test]
    async fn load_all_ids_errors_on_unparseable_id() {
        let pool = test_pool().await;
        insert_event(&pool, "Widget", "42").await;
        insert_event(&pool, "Widget", "not-a-number").await;

        let error = load_all_ids::<Widget>(&pool)
            .await
            .expect_err("should fail when an ID cannot parse");

        match error {
            LoadAllIdsError::InvalidIds {
                aggregate_type,
                ids,
            } => {
                assert_eq!(aggregate_type, "Widget");
                assert_eq!(ids, vec!["not-a-number"]);
            }
            LoadAllIdsError::Sql(sql_error) => {
                panic!("expected InvalidIds, got Sql: {sql_error}")
            }
            LoadAllIdsError::NumericConversion(conv_error) => {
                panic!("expected InvalidIds, got NumericConversion: {conv_error}")
            }
        }
    }

    #[tokio::test]
    async fn load_all_ids_ignores_other_aggregate_types() {
        let pool = test_pool().await;
        insert_event(&pool, "Widget", "1").await;
        insert_event(&pool, "OtherAggregate", "should-be-excluded").await;

        let ids = load_all_ids::<Widget>(&pool).await.unwrap();

        assert_eq!(ids, vec![NumericId(1)]);
    }

    #[tokio::test]
    async fn count_aggregates_includes_snapshot_only_aggregates() {
        let pool = test_pool().await;
        let store = test_store::<Widget>(pool.clone());
        store
            .send(
                &NumericId(30),
                WidgetCommand::Create {
                    name: "snapshot-only".to_string(),
                },
            )
            .await
            .unwrap();
        compact_events::<Widget>(&pool).await.unwrap();

        let count = count_aggregates::<Widget>(&pool).await.unwrap();

        assert_eq!(count, 1);
    }
}
