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
mod job_store;
mod lifecycle;
mod persistence;
mod projection;
mod projection_recovery;
mod reactor;
mod schema_registry;
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
    ProjectionReplayRow, SnapshotHistory, Table,
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
    CheckedCommit, CommitRequest, EngineError, JobSeed, OpaqueCommitRequest, OpaqueProposedEvent,
    OpaqueStoredEvent, PreparedCommit, PreparedJob, PreparedSnapshot, SnapshotUpdate,
    StreamIdentity, decode_opaque_payload, encode_opaque_payload, validate_expected_version,
};
pub use job::{
    DeadReason, JobContext, JobFailure, JobId, JobOutcome, JobStoreError, Label,
    PreparedPendingJobs, StandaloneJob,
};
pub use job_backend::{
    Backoff, ClaimedJob, Clock, EventStoreBackend, JobClaimHandle, JobClaimResult, JobEnqueueError,
    JobLeaseResult, JobRuntime, JobRuntimeError, JobScheduleError, JobSettlementResult,
    JobWorkerConfig, run_job,
};
pub use job_store::{ClaimDecision, ClaimOutcome, ClaimRead, EventBackend, LeaseRenewal};
use lifecycle::Lifecycle;
pub use lifecycle::{LifecycleError, Never};
pub use persistence::{
    PersistenceBackend, ProjectedStoreBackend, SCHEMA_REGISTRY_ID, UnmaterializedStoreBackend,
    guard_snapshot, reconcile_schema, register_schema,
};
pub use projection::{Column, Projection, ProjectionError};
pub use projection_recovery::recover_projection;
pub use reactor::{
    RETRY_BASE_DELAY_MS, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY_MS, Reactor, ReactorContext,
    retry_with_backoff,
};
pub use schema_registry::{
    ReconcileError, SchemaReconciliation, SchemaRegistry, SchemaRegistryWriter, SchemaTarget,
    validate_snapshot_history,
};
#[cfg(any(test, feature = "test-support"))]
pub use testing::{
    ReactorHarness, RecoveredJobQueryError, SpyReactor, TestHarness, TestResult, TestStore,
    buffer_pending_job, find_recovered_jobs, plan_test_claim, recover_job_queue_row, replay,
    with_pending_jobs,
};
pub use view_backend::{NativeProjectionBackend, ViewBackend, ViewUpdateError};
#[cfg(any(test, feature = "test-support"))]
pub use wire::test_store;
pub use wire::{StoreBuilder, load_entity, send_command};

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
/// Uses the selected backend's repository and accepts only the entity's
/// typed IDs, so a command cannot address another entity type's stream.
///
/// # Usage
///
/// ```ignore
/// let positions: Store<Position, Backend> = /* built by StoreBuilder */;
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
