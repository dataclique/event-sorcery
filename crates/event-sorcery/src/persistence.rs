//! Persistence capabilities for native stores, independent of worker operation.

use std::future::Future;

use cqrs_es::Aggregate;
use cqrs_es::persist::{PersistedEventRepository, PersistenceError, SerializedSnapshot};
use tracing::warn;

use crate::engine::EngineError;
use crate::schema_registry::{REGISTRY_ID, SchemaRegistryCommand};
use crate::{
    CompactionPolicy, EventBackend, EventSourced, Nil, ReconcileError, SchemaReconciliation,
    SchemaRegistry, SchemaTarget, SqliteBackend,
};

/// Storage required to execute and reload native aggregates.
///
/// Repositories must atomically persist command events, snapshots and dispatched
/// jobs. Bootstrap applies library-owned migrations; it does not reconcile an
/// entity's schema or start workers. Callers invoke bootstrap explicitly.
///
/// Existing [`crate::EventBackend`] implementations receive this capability
/// automatically. Backend authors implement either this narrower contract or
/// `EventBackend`, not both. Existing backend member names remain unambiguous:
///
/// ```
/// use event_sorcery::*;
///
/// fn repository<Backend: EventBackend>(backend: &Backend) -> Backend::EventRepo {
///     backend.event_repo(CompactionPolicy::Retain)
/// }
///
/// async fn migrate<Backend: EventBackend>(backend: &Backend) -> Result<(), Backend::Error> {
///     backend.migrate().await
/// }
/// ```
pub trait PersistenceBackend: Clone + Send + Sync + 'static {
    /// The repository used by the existing cqrs-es execution path.
    type PersistenceRepo: PersistedEventRepository + Send + Sync + 'static;

    /// Failure while applying library-owned storage migrations.
    type BootstrapError: std::error::Error + Send + Sync + 'static;

    /// Constructs a repository honoring the entity's retention policy.
    fn persistence_repo(&self, policy: CompactionPolicy) -> Self::PersistenceRepo;

    /// Applies library-owned migrations without changing existing data.
    fn bootstrap_persistence(
        &self,
    ) -> impl Future<Output = Result<(), Self::BootstrapError>> + Send;
}

/// Native startup capability for entities without materialized state.
pub trait UnmaterializedStoreBackend: PersistenceBackend {
    fn reconcile_unmaterialized<Entity: EventSourced<Materialized = Nil>>(
        &self,
    ) -> impl Future<Output = Result<(), ReconcileError>> + Send;
}

impl UnmaterializedStoreBackend for SqliteBackend {
    async fn reconcile_unmaterialized<Entity: EventSourced<Materialized = Nil>>(
        &self,
    ) -> Result<(), ReconcileError> {
        let reconciler = crate::Reconciler::new(self.pool().clone());
        let _ = reconciler.reconcile::<Entity>().await?;
        reconciler.record_version::<Entity>().await
    }
}

/// Stable identity of the singleton schema authority's event stream.
pub const SCHEMA_REGISTRY_ID: &str = REGISTRY_ID;

/// Replays schema authority and applies the shared entity compatibility policy.
pub fn reconcile_schema<Entity: EventSourced>(
    payloads: Vec<String>,
) -> Result<SchemaReconciliation, ReconcileError> {
    let registry = SchemaRegistry::from_payloads(payloads)?;
    let target = SchemaTarget::new(
        Entity::AGGREGATE_TYPE,
        Entity::SCHEMA_VERSION,
        Entity::COMPACTION_POLICY,
    );
    target.reconciliation(registry.as_ref())
}

/// Records reconciliation through the existing registry command on a caller-owned repository.
pub async fn register_schema<Entity: EventSourced, Repository: PersistedEventRepository>(
    repository: Repository,
) -> Result<(), ReconcileError> {
    let store = cqrs_es::persist::PersistedEventStore::new_event_store(repository);
    let framework = crate::wire::es_cqrs::<SchemaRegistry, _>(store, vec![]);
    framework
        .execute(
            REGISTRY_ID,
            SchemaRegistryCommand::Register {
                name: Entity::AGGREGATE_TYPE.to_string(),
                version: Entity::SCHEMA_VERSION,
            },
        )
        .await?;
    Ok(())
}

impl<Backend: EventBackend> PersistenceBackend for Backend {
    type PersistenceRepo = <Backend as EventBackend>::EventRepo;
    type BootstrapError = <Backend as EventBackend>::Error;

    fn persistence_repo(&self, policy: CompactionPolicy) -> Self::PersistenceRepo {
        <Backend as EventBackend>::event_repo(self, policy)
    }

    async fn bootstrap_persistence(&self) -> Result<(), Self::BootstrapError> {
        <Backend as EventBackend>::migrate(self).await
    }
}

/// Applies the shared retention policy to a repository's decoded snapshot.
pub fn guard_snapshot<AggregateType: Aggregate>(
    snapshot: SerializedSnapshot,
    policy: CompactionPolicy,
) -> Result<SerializedSnapshot, PersistenceError> {
    // Retained history can rebuild; compacted history may exist only in this snapshot.
    match policy {
        CompactionPolicy::Retain => Ok(snapshot),
        CompactionPolicy::CompactAfterSnapshot | CompactionPolicy::RetainLast { .. } => {
            match AggregateType::deserialize(&snapshot.aggregate) {
                Ok(_) => Ok(snapshot),
                Err(source) => {
                    warn!(
                        target: "cqrs",
                        aggregate_type = AggregateType::TYPE,
                        aggregate_id = snapshot.aggregate_id,
                        %source,
                        "Incompatible snapshot for a compactable aggregate cannot be \
                         safely rebuilt from events; surfacing error"
                    );
                    Err(EngineError::Json(source).into())
                }
            }
        }
    }
}
