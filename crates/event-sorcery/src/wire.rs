//! CQRS framework construction via [`StoreBuilder`].
//!
//! All CQRS framework construction must go through
//! [`StoreBuilder`], which reconciles schema versions and
//! registers reactors. Direct CQRS framework construction is
//! blocked via clippy's `disallowed-methods`; `StoreBuilder`
//! contains the narrow escape hatch.
//!
//! # Registering reactors
//!
//! Use [`.with()`](StoreBuilder::with) to register a reactor
//! with a builder. For single-entity reactors, wrap in
//! `Arc::new()`. For multi-entity reactors, clone the same
//! `Arc` into each builder.
//!
//! # Auto-wired projections
//!
//! `build()` dispatches on `Entity::Materialized` via a type
//! parameter that defaults to `Entity::Materialized`:
//!
//! - `Table` entities: auto-creates and wires a [`Projection`],
//!   returning `(Arc<Store>, Arc<Projection>)`.
//! - `Nil` entities: returns `Arc<Store>`.
//!
//! This eliminates the footgun of forgetting to wire a
//! projection -- if the entity declares a table, the projection
//! is always present.
//!
//! Exhaustive entity handling is enforced by the reactor's
//! [`.on()`](crate::OneOf::on) /
//! [`.exhaustive()`](crate::Fold::exhaustive) chain at compile
//! time, not by the wiring infrastructure.

use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use cqrs_es::persist::{PersistedEventRepository, PersistedEventStore};
use cqrs_es::{CqrsFramework, EventStore, Query};

use crate::Nil;
use crate::dependency::HasEntity;
use crate::lifecycle::{Lifecycle, ReactorBridge};
use crate::projection::{Projection, ProjectionQuery};
use crate::reactor::Reactor;
use crate::schema_registry::ReconcileError;
use crate::{
    CompactionPolicy, EventSourced, PersistenceBackend, ProjectedStoreBackend, SendError, Store,
    Table, UnmaterializedStoreBackend,
};

/// Builder for a single CQRS framework.
///
/// Parameterized on an [`EventSourced`] entity type. The
/// `Materialized` type parameter defaults to
/// `Entity::Materialized` and determines the `build()` return
/// type: `Table` returns `(Arc<Store>, Arc<Projection>)`, `Nil`
/// returns `Arc<Store>`.
///
/// Register reactors via [`.with()`](Self::with), then call
/// [`.build()`](Self::build) to construct the framework.
pub struct StoreBuilder<
    Entity: EventSourced,
    Backend: PersistenceBackend,
    Materialized = <Entity as EventSourced>::Materialized,
> {
    backend: Backend,
    queries: Vec<Box<dyn Query<Lifecycle<Entity>>>>,
    _materialized: std::marker::PhantomData<Materialized>,
}

impl<Entity: EventSourced, Backend: PersistenceBackend> StoreBuilder<Entity, Backend> {
    /// Creates a new builder over an arbitrary [`PersistenceBackend`].
    pub fn with_backend(backend: Backend) -> Self {
        Self {
            backend,
            queries: vec![],
            _materialized: std::marker::PhantomData,
        }
    }

    /// Registers a reactor with this CQRS framework.
    ///
    /// The reactor must declare `Entity` in its dependency list
    /// (via [`deps!`](crate::deps)). For multi-entity reactors,
    /// clone the same `Arc` into each relevant builder.
    #[must_use]
    pub fn with<R>(mut self, reactor: Arc<R>) -> Self
    where
        R: Reactor + 'static,
        R::Dependencies: HasEntity<Entity>,
        Entity::Id: Clone,
        Entity::Event: Clone,
        <Entity::Id as FromStr>::Err: Debug,
    {
        self.queries.push(Box::new(ReactorBridge { reactor }));
        self
    }
}

pub(crate) fn es_cqrs<Entity: EventSourced, Repository: PersistedEventRepository>(
    store: PersistedEventStore<Repository, Lifecycle<Entity>>,
    queries: Vec<Box<dyn Query<Lifecycle<Entity>>>>,
) -> CqrsFramework<Lifecycle<Entity>, PersistedEventStore<Repository, Lifecycle<Entity>>> {
    // `Lifecycle`'s cqrs-es services are unit -- handlers return an `Effect`
    // instead of calling out through injected services.
    #[allow(clippy::disallowed_methods)]
    CqrsFramework::new(store, queries, ())
}

/// Projected entities: auto-creates and wires a [`Projection`],
/// returning `(Arc<Store>, Arc<Projection>)`.
impl<Entity, Backend> StoreBuilder<Entity, Backend, Table>
where
    Entity: EventSourced<Materialized = Table> + 'static,
    Backend: ProjectedStoreBackend,
    Entity::Id: Clone,
    Entity::Event: Clone,
    <Entity::Id as FromStr>::Err: Debug,
{
    pub async fn build(
        mut self,
    ) -> Result<
        (
            Arc<Store<Entity, Backend>>,
            Arc<Projection<Entity, Backend::ViewBackend>>,
        ),
        ReconcileError,
    > {
        // Projected entities must retain all events so that
        // `catch_up`/`rebuild_all` can replay the full history.
        // Compacted aggregates lose events after snapshot, making
        // projection rebuilds silently incomplete.
        const {
            assert!(
                matches!(Entity::COMPACTION_POLICY, CompactionPolicy::Retain),
                "CompactAfterSnapshot entities must not have table projections -- \
                 rebuild_all only reads the events table and would miss \
                 compacted snapshot-only aggregates"
            );
        }

        let projection = Arc::new(self.backend.reconcile_projected::<Entity>().await?);

        self.queries.push(Box::new(ProjectionQuery {
            projection: projection.clone(),
        }));

        let Self {
            backend, queries, ..
        } = self;
        let event_store = PersistedEventStore::new_snapshot_store(
            backend.persistence_repo(Entity::COMPACTION_POLICY),
            Entity::SNAPSHOT_SIZE,
        );
        let cqrs = es_cqrs(event_store, queries);
        Ok((Arc::new(Store::new(cqrs, &backend)), projection))
    }
}

/// Non-projected entities: returns just `Store`.
impl<Entity: EventSourced<Materialized = Nil>, Backend: UnmaterializedStoreBackend>
    StoreBuilder<Entity, Backend, Nil>
{
    pub async fn build(self) -> Result<Arc<Store<Entity, Backend>>, ReconcileError> {
        self.backend.reconcile_unmaterialized::<Entity>().await?;

        let Self {
            backend, queries, ..
        } = self;
        let event_store = PersistedEventStore::new_snapshot_store(
            backend.persistence_repo(Entity::COMPACTION_POLICY),
            Entity::SNAPSHOT_SIZE,
        );
        let cqrs = es_cqrs(event_store, queries);
        Ok(Arc::new(Store::new(cqrs, &backend)))
    }
}

/// Reconstructs an entity from its snapshot and events without a [`Store`].
///
/// Returns an error if reading storage or reconstructing the entity fails.
pub async fn load_entity<Entity: EventSourced>(
    backend: &impl PersistenceBackend,
    id: &Entity::Id,
) -> Result<Option<Entity>, SendError<Entity>> {
    let repository = backend.persistence_repo(Entity::COMPACTION_POLICY);
    let store = PersistedEventStore::<_, Lifecycle<Entity>>::new_snapshot_store(
        repository,
        Entity::SNAPSHOT_SIZE,
    );
    let context = store.load_aggregate(&id.to_string()).await?;

    Ok(context.aggregate.into_result()?)
}

/// Execute a single command against an aggregate without a pre-built
/// [`Store`].
///
/// Creates a temporary CQRS framework with no query processors,
/// executes the command (inside a pending-job scope, so any jobs the
/// handler enqueues flush with the events), and discards the framework.
/// Useful in CLI contexts where you need to send a command but don't have
/// (or need) a full server-lifetime Store.
pub async fn send_command<Entity: EventSourced>(
    backend: &impl PersistenceBackend,
    id: &Entity::Id,
    command: Entity::Command,
) -> Result<(), SendError<Entity>> {
    let repository = backend.persistence_repo(Entity::COMPACTION_POLICY);
    let store = PersistedEventStore::new_snapshot_store(repository, Entity::SNAPSHOT_SIZE);
    let cqrs = es_cqrs::<Entity, _>(store, vec![]);

    crate::job::with_pending_scope(cqrs.execute(&id.to_string(), command)).await
}

#[cfg(any(test, feature = "test-support"))]
/// Opens a store without startup recovery or reactors for isolated behavior tests.
pub fn test_store<Entity: EventSourced, Backend: PersistenceBackend>(
    backend: &Backend,
) -> Store<Entity, Backend> {
    let event_store = PersistedEventStore::new_snapshot_store(
        backend.persistence_repo(Entity::COMPACTION_POLICY),
        Entity::SNAPSHOT_SIZE,
    );
    let cqrs = es_cqrs(event_store, vec![]);
    Store::new(cqrs, backend)
}
