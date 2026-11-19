//! Schema version registry for detecting stale snapshots.
//!
//! Tracks the last-known [`SCHEMA_VERSION`] for each aggregate type
//! in the event store. Startup reconciliation is a two-step contract:
//! [`Reconciler::reconcile`] compares the stored version against the
//! current code version and clears snapshots when they diverge, then
//! the caller runs any further version-change recovery it owns (for
//! projected entities, a view rebuild) and finally calls
//! [`Reconciler::record_version`] to mark the version handled.
//! Recording is deferred to the end so a crash mid-recovery re-runs the
//! whole idempotent sequence on restart instead of stranding
//! half-reconciled state behind an already-advanced version. The
//! `StoreBuilder` wires this sequence; callers driving [`Reconciler`]
//! directly must follow the same order.
//!
//! This is itself an event-sourced aggregate whose state is rebuilt
//! from the full event log on every startup -- no views, no
//! snapshots. This avoids a circular dependency: views depend on
//! the schema registry for reprojection, so the registry must be
//! self-sufficient.
//!
//! [`SCHEMA_VERSION`]: crate::EventSourced::SCHEMA_VERSION

use async_trait::async_trait;
use cqrs_es::AggregateError;
use cqrs_es::persist::PersistenceError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::CompactionPolicy;
use crate::lifecycle::{LifecycleError, Never};
use crate::{DomainEvent, Effect, EventSourced, Nil, StreamIdentity, uneventful};

#[cfg(any(test, feature = "sqlite"))]
#[path = "sqlite_schema_registry.rs"]
mod sqlite;

#[cfg(any(test, feature = "sqlite"))]
pub use sqlite::Reconciler;

/// Singleton aggregate ID for the schema registry.
pub(crate) const REGISTRY_ID: &str = "schema";

/// Language-neutral aggregate schema metadata used during startup recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaTarget {
    aggregate_type: String,
    version: u64,
    compaction: CompactionPolicy,
}

impl SchemaTarget {
    #[must_use]
    pub fn new(
        aggregate_type: impl Into<String>,
        version: u64,
        compaction: CompactionPolicy,
    ) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            version,
            compaction,
        }
    }

    pub(crate) fn reconciliation(
        &self,
        registry: Option<&SchemaRegistry>,
    ) -> Result<SchemaReconciliation, ReconcileError> {
        let stored_version = registry.and_then(|state| state.version_of(&self.aggregate_type));
        if stored_version == Some(self.version) {
            return Ok(SchemaReconciliation::Unchanged);
        }

        if stored_version.is_some() {
            match self.compaction {
                CompactionPolicy::Retain => {}
                CompactionPolicy::CompactAfterSnapshot | CompactionPolicy::RetainLast { .. } => {
                    return Err(ReconcileError::CompactedSnapshotClear {
                        aggregate: self.aggregate_type.clone(),
                        old_version: stored_version,
                        new_version: self.version,
                    });
                }
            }
        }

        Ok(SchemaReconciliation::Changed)
    }
}

/// Tracks schema versions for all aggregates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaRegistry {
    versions: BTreeMap<String, u64>,
}

impl SchemaRegistry {
    fn version_of(&self, name: &str) -> Option<u64> {
        self.versions.get(name).copied()
    }

    pub(crate) fn from_payloads(payloads: Vec<String>) -> Result<Option<Self>, ReconcileError> {
        payloads.into_iter().try_fold(None, |state, payload| {
            let event: SchemaRegistryEvent = serde_json::from_str(&payload)?;
            let next = match state {
                None => Self::originate(&event),
                Some(current) => Some(Self::evolve(&current, &event)?.unwrap_or(current)),
            };
            Ok(next)
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SchemaRegistryEvent {
    VersionUpdated { name: String, version: u64 },
}

impl DomainEvent for SchemaRegistryEvent {
    fn event_type(&self) -> String {
        "SchemaRegistryEvent::VersionUpdated".to_string()
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaRegistryCommand {
    Register { name: String, version: u64 },
}

#[async_trait]
impl EventSourced for SchemaRegistry {
    type Id = String;
    type Error = Never;
    type Command = SchemaRegistryCommand;
    type Event = SchemaRegistryEvent;
    type Materialized = Nil;
    type Jobs = Nil;

    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 1;
    const SNAPSHOT_SIZE: usize = 10;
    const AGGREGATE_TYPE: &'static str = "SchemaRegistry";
    const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;

    fn originate(event: &Self::Event) -> Option<Self> {
        let SchemaRegistryEvent::VersionUpdated { name, version } = event;
        let mut versions = BTreeMap::new();
        versions.insert(name.clone(), *version);
        Some(Self { versions })
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        let SchemaRegistryEvent::VersionUpdated { name, version } = event;
        let mut new_state = entity.clone();
        new_state.versions.insert(name.clone(), *version);
        Ok(Some(new_state))
    }

    async fn initialize(command: Self::Command) -> Result<Effect<Self>, Self::Error> {
        let SchemaRegistryCommand::Register { name, version } = command;
        Ok(Effect::Events(vec![SchemaRegistryEvent::VersionUpdated {
            name,
            version,
        }]))
    }

    async fn transition(&self, command: Self::Command) -> Result<Effect<Self>, Self::Error> {
        let SchemaRegistryCommand::Register { name, version } = command;
        if self.version_of(&name) == Some(version) {
            uneventful()
        } else {
            Ok(Effect::Events(vec![SchemaRegistryEvent::VersionUpdated {
                name,
                version,
            }]))
        }
    }
}

/// Outcome of schema reconciliation: whether the stored schema version
/// diverged from the code's [`SCHEMA_VERSION`] for an entity.
///
/// Callers branch on this to choose recovery: a `Changed` projected entity
/// rebuilds its views, an `Unchanged` one only catches up on missed events.
///
/// [`SCHEMA_VERSION`]: crate::EventSourced::SCHEMA_VERSION
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "reconciliation must be completed by recording the schema version after recovery; \
              dropping the outcome leaves the schema version unadvanced and recovery re-running \
              on every startup"]
pub enum SchemaReconciliation {
    /// The stored version did not match the current code version. Retained-history
    /// snapshots are cleared; compactable snapshots survive first registration.
    Changed,
    /// The stored version matched the code version; nothing was cleared.
    Unchanged,
}

impl SchemaReconciliation {
    /// Only retained history can rebuild snapshots invalidated during reconciliation.
    #[must_use]
    pub fn clears_snapshots(self, policy: CompactionPolicy) -> bool {
        use CompactionPolicy::{CompactAfterSnapshot, Retain, RetainLast};
        use SchemaReconciliation::{Changed, Unchanged};

        match (self, policy) {
            (Changed, Retain) => true,
            (Changed, CompactAfterSnapshot | RetainLast { .. })
            | (Unchanged, Retain | CompactAfterSnapshot | RetainLast { .. }) => false,
        }
    }
}

/// Proves that every positive sequence covered by a snapshot remains available.
///
/// Counts must include only this stream and sequences in `1..=covered_sequence`.
/// Sparse or compacted histories without this proof require an explicit migration.
pub fn validate_snapshot_history(
    stream: StreamIdentity,
    covered_sequence: i64,
    retained_events: i64,
) -> Result<(), ReconcileError> {
    if covered_sequence <= 0 || retained_events != covered_sequence {
        return Err(ReconcileError::UnprovenSnapshotHistory {
            stream,
            covered_sequence,
            retained_events,
        });
    }
    Ok(())
}

/// Errors from schema reconciliation during startup.
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    /// Snapshot clearing requires proof that its entire covered history is retained.
    #[error(
        "Cannot clear snapshot for {stream:?}: covered sequence {covered_sequence}, \
         retained covered events {retained_events}; complete history is unproven. \
         An explicit snapshot-preserving migration is required."
    )]
    UnprovenSnapshotHistory {
        stream: StreamIdentity,
        covered_sequence: i64,
        retained_events: i64,
    },
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Aggregate(#[from] AggregateError<LifecycleError<SchemaRegistry>>),
    #[error(transparent)]
    Persistence(#[from] PersistenceError),

    /// A compactable aggregate's schema version changed but its
    /// snapshots cannot be safely deleted because compacted events
    /// are already gone. Requires an explicit migration.
    #[error(
        "Cannot clear snapshots for compactable aggregate '{aggregate}' \
         (version {old_version:?} -> {new_version}). \
         Compacted events have been deleted; clearing snapshots would \
         permanently lose state. Write a migration that handles the \
         schema change without deleting snapshots."
    )]
    CompactedSnapshotClear {
        aggregate: String,
        old_version: Option<u64>,
        new_version: u64,
    },
}

impl From<Never> for ReconcileError {
    fn from(never: Never) -> Self {
        match never {}
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::Aggregate;
    use cqrs_es::event_sink::EventSink;
    use proptest::prelude::{any, prop_assert_eq, proptest};
    use serde_json::json;
    use sqlx::SqlitePool;

    use super::*;
    use crate::lifecycle::Lifecycle;

    #[test]
    fn registry_replay_rejects_incompatible_event_payloads() {
        let error =
            SchemaRegistry::from_payloads(vec![json!({"obsolete": true}).to_string()]).unwrap_err();
        let ReconcileError::Json(source) = error else {
            panic!("expected a typed registry payload error");
        };
        assert_eq!(source.classify(), serde_json::error::Category::Data);
    }

    proptest! {
        #[test]
        fn registry_replay_keeps_the_latest_version_for_each_name(
            registrations in proptest::collection::vec(("[a-z]{1,3}", any::<u64>()), 0..32)
        ) {
            let payloads = registrations.iter().map(|(name, version)| {
                json!({"VersionUpdated": {"name": name, "version": version}}).to_string()
            }).collect();

            let registry = SchemaRegistry::from_payloads(payloads).unwrap();

            let expected: BTreeMap<String, u64> = registrations.iter().cloned().collect();
            let expected = if registrations.is_empty() { None } else { Some(expected) };

            prop_assert_eq!(registry.map(|state| state.versions), expected);
        }
    }

    #[tokio::test]
    async fn register_new_aggregate_emits_event() {
        let mut aggregate = Lifecycle::<SchemaRegistry>::default();
        let sink = EventSink::default();

        aggregate
            .handle(
                SchemaRegistryCommand::Register {
                    name: "Position".to_string(),
                    version: 1,
                },
                &(),
                &sink,
            )
            .await
            .unwrap();

        let events = sink.collect().await;

        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0],
            SchemaRegistryEvent::VersionUpdated {
                name: "Position".to_string(),
                version: 1,
            }
        );
    }

    #[tokio::test]
    async fn register_same_version_is_noop() {
        let mut aggregate = Lifecycle::<SchemaRegistry>::default();

        aggregate.apply(SchemaRegistryEvent::VersionUpdated {
            name: "Position".to_string(),
            version: 1,
        });

        let sink = EventSink::default();

        aggregate
            .handle(
                SchemaRegistryCommand::Register {
                    name: "Position".to_string(),
                    version: 1,
                },
                &(),
                &sink,
            )
            .await
            .unwrap();

        let events = sink.collect().await;

        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn register_new_version_emits_event() {
        let mut aggregate = Lifecycle::<SchemaRegistry>::default();

        aggregate.apply(SchemaRegistryEvent::VersionUpdated {
            name: "Position".to_string(),
            version: 1,
        });

        let sink = EventSink::default();

        aggregate
            .handle(
                SchemaRegistryCommand::Register {
                    name: "Position".to_string(),
                    version: 2,
                },
                &(),
                &sink,
            )
            .await
            .unwrap();

        let events = sink.collect().await;

        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0],
            SchemaRegistryEvent::VersionUpdated {
                name: "Position".to_string(),
                version: 2,
            }
        );
    }

    #[test]
    fn tracks_multiple_aggregates() {
        let mut aggregate = Lifecycle::<SchemaRegistry>::default();

        aggregate.apply(SchemaRegistryEvent::VersionUpdated {
            name: "Position".to_string(),
            version: 1,
        });
        aggregate.apply(SchemaRegistryEvent::VersionUpdated {
            name: "OffchainOrder".to_string(),
            version: 3,
        });

        let Lifecycle::Live(registry) = &aggregate else {
            panic!("Expected Live state");
        };

        assert_eq!(registry.version_of("Position"), Some(1));
        assert_eq!(registry.version_of("OffchainOrder"), Some(3));
        assert_eq!(registry.version_of("Unknown"), None);
    }

    #[tokio::test]
    async fn reconciler_detects_version_change() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlite_es::MIGRATOR.run(&pool).await.unwrap();

        let reconciler = Reconciler::new(pool);

        // First run: no stored version -> changed.
        let outcome = reconciler.reconcile::<SchemaRegistry>().await.unwrap();
        assert_eq!(outcome, SchemaReconciliation::Changed);

        // Until the version is recorded, every reconcile still reports a change
        // -- a crash before record_version re-runs the whole recovery.
        let outcome = reconciler.reconcile::<SchemaRegistry>().await.unwrap();
        assert_eq!(outcome, SchemaReconciliation::Changed);

        reconciler.record_version::<SchemaRegistry>().await.unwrap();

        // After recording, the version matches -> unchanged.
        let outcome = reconciler.reconcile::<SchemaRegistry>().await.unwrap();
        assert_eq!(outcome, SchemaReconciliation::Unchanged);
    }

    #[tokio::test]
    async fn load_registry_replays_from_events() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlite_es::MIGRATOR.run(&pool).await.unwrap();

        let reconciler = Reconciler::new(pool);

        // Initially empty
        let registry = reconciler.load_registry().await.unwrap();
        assert!(registry.is_none());

        // Reconcile then record the version into the registry.
        let _ = reconciler.reconcile::<SchemaRegistry>().await.unwrap();
        reconciler.record_version::<SchemaRegistry>().await.unwrap();

        // Should have SchemaRegistry at version 1
        let registry = reconciler.load_registry().await.unwrap().unwrap();
        assert_eq!(registry.version_of("SchemaRegistry"), Some(1));
    }

    /// Minimal compactable entity for testing the reconciliation guard.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct CompactableWidget;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    enum CompactableEvent {
        Created,
    }

    impl cqrs_es::DomainEvent for CompactableEvent {
        fn event_type(&self) -> String {
            "Created".to_string()
        }
        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[async_trait]
    impl EventSourced for CompactableWidget {
        type Id = String;
        type Error = Never;
        type Command = ();
        type Event = CompactableEvent;
        type Materialized = Nil;
        type Jobs = Nil;

        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 2;
        const SNAPSHOT_SIZE: usize = 1;
        const AGGREGATE_TYPE: &'static str = "CompactableWidget";
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::CompactAfterSnapshot;

        fn originate(_event: &Self::Event) -> Option<Self> {
            Some(Self)
        }

        fn evolve(_entity: &Self, _event: &Self::Event) -> Result<Option<Self>, Never> {
            Ok(Some(Self))
        }

        async fn initialize(_command: Self::Command) -> Result<Effect<Self>, Never> {
            Ok(Effect::Events(vec![CompactableEvent::Created]))
        }

        async fn transition(&self, _command: Self::Command) -> Result<Effect<Self>, Never> {
            uneventful()
        }
    }

    #[tokio::test]
    async fn reconcile_allows_first_registration_for_compactable_aggregate() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlite_es::MIGRATOR.run(&pool).await.unwrap();

        let reconciler = Reconciler::new(pool);

        // First reconcile: stored=None, current=2. No prior snapshots
        // exist, so this should succeed even for compactable entities.
        let outcome = reconciler.reconcile::<CompactableWidget>().await.unwrap();

        assert_eq!(outcome, SchemaReconciliation::Changed);
    }

    #[tokio::test]
    async fn first_registration_preserves_existing_compactable_snapshots() {
        for policy in [
            CompactionPolicy::CompactAfterSnapshot,
            CompactionPolicy::RetainLast { events: 2 },
        ] {
            let pool = sqlite_es::testing::create_test_pool().await.unwrap();
            let store = crate::test_store::<CompactableWidget>(pool.clone());
            let id = "legacy-widget".to_string();
            store.send(&id, ()).await.unwrap();
            let snapshot_sql = "SELECT last_sequence, snapshot_version, payload FROM snapshots \
                                WHERE aggregate_type = 'CompactableWidget'";
            let before: (i64, i64, String) =
                sqlx::query_as(snapshot_sql).fetch_one(&pool).await.unwrap();
            assert_eq!((before.0, before.1), (1, 1));
            let reconciler = Reconciler::new(pool.clone());
            assert_eq!(reconciler.load_registry().await.unwrap(), None);
            let target = SchemaTarget::new("CompactableWidget", 2, policy);

            assert_eq!(
                reconciler.reconcile_target(&target).await.unwrap(),
                SchemaReconciliation::Changed
            );
            reconciler.record_target(&target).await.unwrap();

            let after: (i64, i64, String) =
                sqlx::query_as(snapshot_sql).fetch_one(&pool).await.unwrap();
            assert_eq!(after, before);
            let registry = reconciler.load_registry().await.unwrap().unwrap();
            assert_eq!(registry.version_of("CompactableWidget"), Some(2));
            store.load(&id).await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn reconcile_refuses_schema_changes_for_windowed_compaction() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlite_es::MIGRATOR.run(&pool).await.unwrap();
        let reconciler = Reconciler::new(pool);
        let original = SchemaTarget::new(
            "WindowedWidget",
            1,
            CompactionPolicy::RetainLast { events: 2 },
        );
        assert_eq!(
            reconciler.reconcile_target(&original).await.unwrap(),
            SchemaReconciliation::Changed
        );
        reconciler.record_target(&original).await.unwrap();
        let changed = SchemaTarget::new(
            "WindowedWidget",
            2,
            CompactionPolicy::RetainLast { events: 2 },
        );

        let error = reconciler.reconcile_target(&changed).await.unwrap_err();
        assert!(matches!(
            error,
            ReconcileError::CompactedSnapshotClear {
                aggregate,
                old_version: Some(1),
                new_version: 2,
            } if aggregate == "WindowedWidget"
        ));
        let registry = reconciler.load_registry().await.unwrap().unwrap();
        assert_eq!(registry.version_of("WindowedWidget"), Some(1));
    }

    #[tokio::test]
    async fn reconcile_errors_on_version_mismatch_for_compactable_aggregate() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlite_es::MIGRATOR.run(&pool).await.unwrap();

        let reconciler = Reconciler::new(pool.clone());

        // First registration succeeds (stored=None) and records version 2.
        let _ = reconciler.reconcile::<CompactableWidget>().await.unwrap();
        reconciler
            .record_version::<CompactableWidget>()
            .await
            .unwrap();

        // Manually update the stored version to simulate a version
        // bump (we can't change the const at runtime).
        sqlx::query(
            "UPDATE events SET payload = \
             '{\"VersionUpdated\":{\"name\":\"CompactableWidget\",\"version\":999}}' \
             WHERE aggregate_type = 'SchemaRegistry' \
             AND payload LIKE '%CompactableWidget%'",
        )
        .execute(&pool)
        .await
        .unwrap();

        // Second reconcile: stored=999, current=2. The entity is
        // compactable, so reconcile should refuse.
        let result = reconciler.reconcile::<CompactableWidget>().await;

        assert!(
            matches!(result, Err(ReconcileError::CompactedSnapshotClear { .. })),
            "Expected CompactedSnapshotClear error, got {result:?}"
        );
    }
}
