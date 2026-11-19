//! Persistence capabilities for native stores, independent of worker operation.

use cqrs_es::Aggregate;
use cqrs_es::persist::{PersistedEventRepository, PersistenceError, SerializedSnapshot};
use std::future::Future;
use tracing::warn;

use crate::engine::EngineError;
use crate::schema_registry::{REGISTRY_ID, SchemaRegistryCommand};
use crate::{
    CompactionPolicy, EventBackend, EventSourced, NativeProjectionBackend, Nil, Projection,
    ReconcileError, SchemaReconciliation, SchemaRegistry, SchemaTarget, SqliteBackend, Table,
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

/// Native startup for entities with a materialized view, independent of workers.
pub trait ProjectedStoreBackend: PersistenceBackend {
    /// View repository family used by the returned, fully recovered projection.
    type ViewBackend: NativeProjectionBackend;

    /// Recovers view state and records the schema before publishing a projection.
    fn reconcile_projected<Entity>(
        &self,
    ) -> impl Future<Output = Result<Projection<Entity, Self::ViewBackend>, ReconcileError>> + Send
    where
        Entity: EventSourced<Materialized = Table> + 'static;
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

    use async_trait::async_trait;
    use cqrs_es::AggregateError;
    use cqrs_es::persist::{PersistenceError, SerializedEvent, SerializedSnapshot};
    use proptest::prelude::{Just, any, prop_assert_eq, prop_oneof, proptest};
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use sqlx::SqlitePool;
    use sqlx::migrate::MigrateError;
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::{Arc, Mutex};

    use super::{guard_snapshot, reconcile_schema, register_schema};
    use crate::lifecycle::Lifecycle;
    use crate::{
        ClaimDecision, ClaimOutcome, ClaimRead, CompactionPolicy, DomainEvent, Effect,
        EventBackend, EventSourced, LeaseRenewal, Never, Nil, PersistenceBackend, ReconcileError,
        SchemaReconciliation, SchemaRegistry, SqliteBackend, SqliteJobError, Store, StoreBuilder,
    };

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct SchemaProbe<const VERSION: u64, const COMPACT: bool> {
        value: u64,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    enum ProbeEvent {
        ValueWritten { value: u64 },
    }

    impl DomainEvent for ProbeEvent {
        fn event_type(&self) -> String {
            "ValueWritten".to_string()
        }

        fn event_version(&self) -> String {
            "1".to_string()
        }
    }

    #[async_trait]
    impl<const VERSION: u64, const COMPACT: bool> EventSourced for SchemaProbe<VERSION, COMPACT> {
        type Id = String;
        type Command = u64;
        type Event = ProbeEvent;
        type Error = Never;
        type Materialized = Nil;
        type Jobs = Nil;

        const AGGREGATE_TYPE: &'static str = "PersistenceProbe";
        const SCHEMA_VERSION: u64 = VERSION;
        const SNAPSHOT_SIZE: usize = 2;
        const COMPACTION_POLICY: CompactionPolicy = if COMPACT {
            CompactionPolicy::CompactAfterSnapshot
        } else {
            CompactionPolicy::Retain
        };
        const PROJECTION: Nil = Nil;

        fn originate(event: &Self::Event) -> Option<Self> {
            let ProbeEvent::ValueWritten { value } = event;
            Some(Self { value: *value })
        }

        fn evolve(_entity: &Self, event: &Self::Event) -> Result<Option<Self>, Never> {
            Ok(Self::originate(event))
        }

        async fn initialize(value: u64) -> Result<Effect<Self>, Never> {
            Ok(Effect::Events(vec![ProbeEvent::ValueWritten { value }]))
        }

        async fn transition(&self, value: u64) -> Result<Effect<Self>, Never> {
            Self::initialize(value).await
        }
    }

    #[derive(Clone)]
    struct LegacyBackend {
        backend: SqliteBackend,
        policies: Arc<Mutex<Vec<CompactionPolicy>>>,
    }

    impl EventBackend for LegacyBackend {
        type EventRepo = <SqliteBackend as EventBackend>::EventRepo;
        type Error = SqliteJobError;

        fn event_repo(&self, policy: CompactionPolicy) -> Self::EventRepo {
            self.policies.lock().unwrap().push(policy);
            self.backend.event_repo(policy)
        }

        async fn migrate(&self) -> Result<(), Self::Error> {
            self.backend.migrate().await
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
            self.backend.claim(job_id, decide).await
        }

        async fn renew(
            &self,
            job_id: &str,
            claim_seq: i64,
            lease_until: i64,
        ) -> Result<LeaseRenewal, Self::Error> {
            self.backend.renew(job_id, claim_seq, lease_until).await
        }

        async fn enqueue(
            &self,
            event: SerializedEvent,
            payload: String,
        ) -> Result<(), Self::Error> {
            self.backend.enqueue(event, payload).await
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
        let backend = LegacyBackend {
            backend: SqliteBackend::new(pool.clone()),
            policies: Arc::new(Mutex::new(vec![])),
        };

        backend.bootstrap_persistence().await.unwrap();
        let _: <SqliteBackend as EventBackend>::EventRepo =
            legacy_contract(&backend, None).await.unwrap();
        let _: <SqliteBackend as EventBackend>::EventRepo =
            backend.persistence_repo(CompactionPolicy::Retain);

        let _store = StoreBuilder::<SchemaRegistry, _>::new(pool.clone())
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
        let backend = LegacyBackend {
            backend: SqliteBackend::new(pool.clone()),
            policies: Arc::new(Mutex::new(vec![])),
        };
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

    #[test]
    fn reconciliation_distinguishes_first_registration_restart_and_retained_upgrade() {
        let cases = [
            (vec![], SchemaReconciliation::Changed),
            (
                vec![
                    json!({"VersionUpdated": {"name": "PersistenceProbe", "version": 2}})
                        .to_string(),
                ],
                SchemaReconciliation::Unchanged,
            ),
            (
                vec![
                    json!({"VersionUpdated": {"name": "PersistenceProbe", "version": 1}})
                        .to_string(),
                ],
                SchemaReconciliation::Changed,
            ),
        ];
        for (payloads, expected) in cases {
            assert_eq!(
                reconcile_schema::<SchemaProbe<2, false>>(payloads).unwrap(),
                expected
            );
        }
        assert_eq!(
            reconcile_schema::<SchemaProbe<2, true>>(vec![]).unwrap(),
            SchemaReconciliation::Changed,
        );
    }

    #[test]
    fn reconciliation_refuses_a_registered_compactable_upgrade() {
        let error = reconcile_schema::<SchemaProbe<2, true>>(vec![
            json!({"VersionUpdated": {"name": "PersistenceProbe", "version": 1}}).to_string(),
        ])
        .unwrap_err();
        let ReconcileError::CompactedSnapshotClear {
            aggregate,
            old_version,
            new_version,
        } = error
        else {
            panic!("expected a compacted schema mismatch, got {error:?}");
        };
        assert_eq!(aggregate, "PersistenceProbe");
        assert_eq!(old_version, Some(1));
        assert_eq!(new_version, 2);
    }

    #[test]
    fn reconciliation_preserves_malformed_registry_payload_errors() {
        let error =
            reconcile_schema::<SchemaProbe<2, false>>(vec![json!({"obsolete": true}).to_string()])
                .unwrap_err();
        let ReconcileError::Json(source) = error else {
            panic!("expected a registry payload error, got {error:?}");
        };
        assert_eq!(source.classify(), serde_json::error::Category::Data);
    }

    #[tokio::test]
    async fn legacy_repository_factory_receives_each_policy_unchanged() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        let backend = LegacyBackend {
            backend: SqliteBackend::new(pool),
            policies: Arc::new(Mutex::new(vec![])),
        };
        let policies = [
            CompactionPolicy::Retain,
            CompactionPolicy::CompactAfterSnapshot,
            CompactionPolicy::RetainLast { events: 17 },
        ];
        for policy in policies {
            let _repository = backend.persistence_repo(policy);
        }
        assert_eq!(*backend.policies.lock().unwrap(), policies);
    }

    #[tokio::test]
    async fn schema_registration_is_idempotent_and_records_each_upgrade_once() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        let backend = SqliteBackend::new(pool.clone());
        backend.bootstrap_persistence().await.unwrap();

        register_schema::<SchemaProbe<1, false>, _>(
            backend.persistence_repo(CompactionPolicy::Retain),
        )
        .await
        .unwrap();
        register_schema::<SchemaProbe<1, false>, _>(
            backend.persistence_repo(CompactionPolicy::Retain),
        )
        .await
        .unwrap();
        register_schema::<SchemaProbe<2, false>, _>(
            backend.persistence_repo(CompactionPolicy::Retain),
        )
        .await
        .unwrap();

        let recorded: Vec<(String, i64, String)> = sqlx::query_as(
            "SELECT aggregate_id, sequence, payload FROM events \
             WHERE aggregate_type = 'SchemaRegistry' ORDER BY sequence",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        let recorded: Vec<_> = recorded
            .into_iter()
            .map(|(identity, sequence, payload)| {
                (
                    identity,
                    sequence,
                    serde_json::from_str::<serde_json::Value>(&payload).unwrap(),
                )
            })
            .collect();
        assert_eq!(
            recorded,
            [
                (
                    "schema".to_string(),
                    1,
                    json!({"VersionUpdated": {"name": "PersistenceProbe", "version": 1}}),
                ),
                (
                    "schema".to_string(),
                    2,
                    json!({"VersionUpdated": {"name": "PersistenceProbe", "version": 2}}),
                ),
            ],
        );
    }

    #[tokio::test]
    async fn schema_registration_preserves_the_storage_error_chain() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        let repository =
            SqliteBackend::new(pool.clone()).persistence_repo(CompactionPolicy::Retain);
        pool.close().await;

        let error = register_schema::<SchemaProbe<1, false>, _>(repository)
            .await
            .unwrap_err();
        let ReconcileError::Aggregate(AggregateError::DatabaseConnectionError(source)) = error
        else {
            panic!("expected the registry storage error, got {error:?}");
        };
        let source = source.downcast::<sqlx::Error>().unwrap();
        assert!(matches!(*source, sqlx::Error::PoolClosed));
    }

    proptest! {
        #[test]
        fn snapshot_guard_preserves_valid_payload_and_independent_counters_for_each_policy(
            value in any::<u64>(),
            policy in prop_oneof![
                Just(CompactionPolicy::Retain),
                Just(CompactionPolicy::CompactAfterSnapshot),
                Just(CompactionPolicy::RetainLast { events: 2 }),
            ],
        ) {
            let snapshot = guard_snapshot::<Lifecycle<SchemaProbe<1, false>>>(
                SerializedSnapshot {
                    aggregate_id: "probe".to_string(),
                    aggregate: json!({"Live": {"value": value}}),
                    current_sequence: 6,
                    current_snapshot: 3,
                },
                policy,
            )
            .unwrap();
            prop_assert_eq!(
                (
                    snapshot.aggregate_id,
                    snapshot.aggregate,
                    snapshot.current_sequence,
                    snapshot.current_snapshot,
                ),
                ("probe".to_string(), json!({"Live": {"value": value}}), 6, 3),
            );
        }
    }

    #[test]
    fn snapshot_guard_leaves_incompatible_retained_payload_available_for_rebuild() {
        let snapshot = guard_snapshot::<Lifecycle<SchemaProbe<1, false>>>(
            SerializedSnapshot {
                aggregate_id: "probe".to_string(),
                aggregate: json!({"obsolete": true}),
                current_sequence: 6,
                current_snapshot: 3,
            },
            CompactionPolicy::Retain,
        )
        .unwrap();
        assert_eq!(snapshot.aggregate_id, "probe");
        assert_eq!(snapshot.aggregate, json!({"obsolete": true}));
        assert_eq!(snapshot.current_sequence, 6);
        assert_eq!(snapshot.current_snapshot, 3);
    }

    #[test]
    fn snapshot_guard_preserves_decode_errors_when_compacted_history_cannot_rebuild() {
        for policy in [
            CompactionPolicy::CompactAfterSnapshot,
            CompactionPolicy::RetainLast { events: 2 },
        ] {
            let error = guard_snapshot::<Lifecycle<SchemaProbe<1, false>>>(
                SerializedSnapshot {
                    aggregate_id: "probe".to_string(),
                    aggregate: json!({"obsolete": true}),
                    current_sequence: 6,
                    current_snapshot: 3,
                },
                policy,
            )
            .unwrap_err();
            let PersistenceError::DeserializationError(source) = error else {
                panic!("expected the snapshot decode failure, got {error:?}");
            };
            let source = source.downcast::<serde_json::Error>().unwrap();
            assert_eq!(source.classify(), serde_json::error::Category::Data);
        }
    }
}
