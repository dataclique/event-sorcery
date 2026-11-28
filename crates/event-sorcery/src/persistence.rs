//! Persistence capabilities for native stores, independent of worker operation.

use cqrs_es::Aggregate;
use cqrs_es::persist::{PersistedEventRepository, PersistenceError, SerializedSnapshot};
use std::future::Future;
use tracing::warn;

use crate::engine::EngineError;
use crate::schema_registry::{REGISTRY_ID, SchemaRegistryWriter};
use crate::{
    CompactionPolicy, EventBackend, EventSourced, NativeProjectionBackend, Nil, Projection,
    ReconcileError, SchemaReconciliation, SchemaRegistry, SchemaTarget, Table,
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
    let target = SchemaTarget::new(
        Entity::AGGREGATE_TYPE,
        Entity::SCHEMA_VERSION,
        Entity::COMPACTION_POLICY,
    );
    SchemaRegistryWriter::new(repository).record(&target).await
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
    use async_trait::async_trait;
    use cqrs_es::persist::{PersistenceError, SerializedSnapshot};
    use proptest::prelude::{Just, any, prop_assert_eq, prop_oneof, proptest};
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::{guard_snapshot, reconcile_schema};
    use crate::lifecycle::Lifecycle;
    use crate::{
        CompactionPolicy, DomainEvent, Effect, EventSourced, Never, Nil, ReconcileError,
        SchemaReconciliation,
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
