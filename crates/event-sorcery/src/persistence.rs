//! Persistence capabilities for native stores, independent of worker operation.

use cqrs_es::Aggregate;
use cqrs_es::persist::{PersistedEventRepository, PersistenceError, SerializedSnapshot};
use std::future::Future;
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
/// fn repository<Backend: EventBackend>(
///     backend: &Backend,
///     _store: Option<&Store<SchemaRegistry, Backend>>,
/// ) -> Backend::EventRepo {
///     backend.event_repo(CompactionPolicy::Retain)
/// }
///
/// fn persistence_only<Backend: PersistenceBackend>(backend: Backend) {
///     let _builder = StoreBuilder::<SchemaRegistry, Backend>::with_backend(backend);
///
///     let _store: Option<Store<SchemaRegistry, Backend>> = None;
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

#[cfg(test)]
mod tests {
    //! External compatibility contracts for persistence and legacy worker backends.

    use cqrs_es::persist::SerializedEvent;
    use sqlx::SqlitePool;
    use sqlx::migrate::MigrateError;
    use sqlx::sqlite::SqlitePoolOptions;

    use crate::{
        ClaimDecision, ClaimOutcome, ClaimRead, CompactionPolicy, EventBackend, LeaseRenewal,
        PersistenceBackend, SchemaRegistry, SqliteBackend, SqliteJobError, Store, StoreBuilder,
    };

    #[derive(Clone)]
    struct LegacyBackend(SqliteBackend);

    impl EventBackend for LegacyBackend {
        type EventRepo = <SqliteBackend as EventBackend>::EventRepo;
        type Error = SqliteJobError;

        fn event_repo(&self, policy: CompactionPolicy) -> Self::EventRepo {
            let Self(backend) = self;
            backend.event_repo(policy)
        }

        async fn migrate(&self) -> Result<(), Self::Error> {
            let Self(backend) = self;
            backend.migrate().await
        }

        async fn claim<Decide, Won>(
            &self,
            job_id: &str,
            decide: Decide,
        ) -> Result<ClaimOutcome<Won>, Self::Error>
        where
            Decide: FnOnce(Option<ClaimRead>) -> ClaimDecision<Won> + Send,
            Won: Send,
        {
            let Self(backend) = self;
            backend.claim(job_id, decide).await
        }

        async fn renew(
            &self,
            job_id: &str,
            claim_seq: i64,
            lease_until: i64,
        ) -> Result<LeaseRenewal, Self::Error> {
            let Self(backend) = self;
            backend.renew(job_id, claim_seq, lease_until).await
        }

        async fn enqueue(
            &self,
            event: SerializedEvent,
            payload: String,
        ) -> Result<(), Self::Error> {
            let Self(backend) = self;
            backend.enqueue(event, payload).await
        }
    }

    #[derive(Clone)]
    struct NarrowBackend(SqliteBackend);

    impl PersistenceBackend for NarrowBackend {
        type PersistenceRepo = <SqliteBackend as EventBackend>::EventRepo;
        type BootstrapError = SqliteJobError;

        fn persistence_repo(&self, policy: CompactionPolicy) -> Self::PersistenceRepo {
            let Self(backend) = self;
            backend.event_repo(policy)
        }

        async fn bootstrap_persistence(&self) -> Result<(), Self::BootstrapError> {
            let Self(backend) = self;
            backend.migrate().await
        }
    }

    async fn legacy_contract<Backend: EventBackend>(
        backend: &Backend,
        _store: Option<&Store<SchemaRegistry, Backend>>,
    ) -> Result<Backend::EventRepo, Backend::Error> {
        backend.migrate().await?;
        Ok(backend.event_repo(CompactionPolicy::Retain))
    }

    #[tokio::test]
    async fn legacy_backends_gain_persistence_without_new_required_members() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        let backend = LegacyBackend(SqliteBackend::new(pool.clone()));

        backend.bootstrap_persistence().await.unwrap();
        let _: <SqliteBackend as EventBackend>::EventRepo =
            legacy_contract(&backend, None).await.unwrap();
        let _: <SqliteBackend as EventBackend>::EventRepo =
            backend.persistence_repo(CompactionPolicy::Retain);

        let _store = StoreBuilder::<SchemaRegistry>::new(pool.clone())
            .build()
            .await
            .unwrap();
        let before: Vec<(String, i64, String)> =
            sqlx::query_as("SELECT aggregate_id, sequence, payload FROM events ORDER BY sequence")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(before.len(), 1);

        backend.bootstrap_persistence().await.unwrap();

        let after: Vec<(String, i64, String)> =
            sqlx::query_as("SELECT aggregate_id, sequence, payload FROM events ORDER BY sequence")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(after, before);
    }

    #[tokio::test]
    async fn narrow_backend_types_do_not_require_worker_capabilities() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        let backend = NarrowBackend(SqliteBackend::new(pool.clone()));

        backend.bootstrap_persistence().await.unwrap();
        let _: <SqliteBackend as EventBackend>::EventRepo =
            backend.persistence_repo(CompactionPolicy::Retain);
        let _: Option<Store<SchemaRegistry, NarrowBackend>> = None;
        let _builder = StoreBuilder::<SchemaRegistry, NarrowBackend>::with_backend(backend);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn bootstrap_preserves_the_legacy_backend_error() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        let backend = LegacyBackend(SqliteBackend::new(pool.clone()));
        pool.close().await;

        let error = backend.bootstrap_persistence().await.unwrap_err();

        let SqliteJobError::Sql(sqlx::Error::Migrate(source)) = error else {
            panic!("expected the migration error from the legacy backend");
        };
        assert!(matches!(
            *source,
            MigrateError::Execute(sqlx::Error::PoolClosed)
        ));
    }
}
