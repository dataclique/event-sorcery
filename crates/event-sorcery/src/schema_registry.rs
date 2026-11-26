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
use cqrs_es::persist::{PersistedEventRepository, PersistedEventStore, PersistenceError};
use cqrs_es::{AggregateError, CqrsFramework};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::CompactionPolicy;
use crate::lifecycle::{Lifecycle, LifecycleError, Never};
use crate::{DomainEvent, Effect, EventSourced, Nil, StreamIdentity, uneventful};

/// Singleton aggregate ID for the schema registry.
pub(crate) const REGISTRY_ID: &str = "schema";

/// Language-neutral aggregate schema metadata used during startup recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaTarget {
    /// Aggregate namespace used to read the registry and identify stale snapshots.
    pub aggregate_type: String,
    /// Current schema version supplied by the entity or binding.
    pub version: u64,
    /// Retention policy controlling whether stale snapshots can be removed.
    pub compaction: CompactionPolicy,
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

    /// Compares the declared schema with the replayed registry state.
    pub fn reconciliation(
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
    /// Reads the last version durably registered for an aggregate type.
    pub fn version_of(&self, name: &str) -> Option<u64> {
        self.versions.get(name).copied()
    }

    /// Replays registry event payloads in stream order, without snapshots.
    pub fn from_payloads(payloads: Vec<String>) -> Result<Option<Self>, ReconcileError> {
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
/// Zero coverage requires zero retained events in that range.
/// Sparse or compacted histories without this proof require an explicit migration.
pub fn validate_snapshot_history(
    stream: StreamIdentity,
    covered_sequence: i64,
    retained_events: i64,
) -> Result<(), ReconcileError> {
    if covered_sequence < 0 || retained_events != covered_sequence {
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
    /// A materialized view cannot recover from a compactable event stream.
    #[error("projection startup requires retained history for aggregate '{aggregate_type}'")]
    ProjectionRequiresRetainedHistory {
        /// Entity type whose projection needs complete history.
        aggregate_type: &'static str,
    },
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

/// Records reconciled schema versions through the registry's existing aggregate.
pub struct SchemaRegistryWriter<Repository: PersistedEventRepository> {
    cqrs: CqrsFramework<
        Lifecycle<SchemaRegistry>,
        PersistedEventStore<Repository, Lifecycle<SchemaRegistry>>,
    >,
}

impl<Repository: PersistedEventRepository> SchemaRegistryWriter<Repository> {
    /// Opens the registry's command handler over caller-owned storage.
    pub fn new(repository: Repository) -> Self {
        let store = PersistedEventStore::new_event_store(repository);
        let cqrs = crate::wire::es_cqrs::<SchemaRegistry, _>(store, vec![]);

        Self { cqrs }
    }

    /// Marks a schema version after its recovery has durably completed.
    pub async fn record(&self, target: &SchemaTarget) -> Result<(), ReconcileError> {
        crate::job::with_pending_scope(self.cqrs.execute(
            REGISTRY_ID,
            SchemaRegistryCommand::Register {
                name: target.aggregate_type.clone(),
                version: target.version,
            },
        ))
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::Aggregate;
    use cqrs_es::event_sink::EventSink;
    use proptest::prelude::{any, prop_assert_eq, proptest};
    use serde_json::json;

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
}
