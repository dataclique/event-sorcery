//! SQLite startup reconciliation over the shared schema registry.

use sqlx::SqlitePool;
use tracing::{debug, info};

use event_sorcery::{
    EventSourced, ReconcileError, SchemaReconciliation, SchemaRegistry, SchemaRegistryWriter,
    SchemaTarget, StreamIdentity, validate_snapshot_history,
};

use crate::SqliteEventRepository;

/// Handles schema version reconciliation at startup.
///
/// Reads state by replaying all SchemaRegistry events from the event
/// store (no views, no snapshots). Writes go through the CQRS
/// framework to maintain event sourcing invariants.
pub struct Reconciler {
    writer: SchemaRegistryWriter<SqliteEventRepository>,
    pool: SqlitePool,
}

impl Reconciler {
    pub fn new(pool: SqlitePool) -> Self {
        let writer = SchemaRegistryWriter::new(SqliteEventRepository::new(pool.clone()));
        Self { writer, pool }
    }

    /// Reconciles language-neutral schema metadata through the same registry
    /// and snapshot recovery used by the native typed API.
    pub async fn reconcile_target(
        &self,
        target: &SchemaTarget,
    ) -> Result<SchemaReconciliation, ReconcileError> {
        let registry = self.load_registry().await?;
        let stored_version = registry
            .as_ref()
            .and_then(|state| state.version_of(&target.aggregate_type));

        debug!(
            aggregate = target.aggregate_type,
            ?stored_version,
            "Loaded stored schema version"
        );

        let reconciliation = target.reconciliation(registry.as_ref())?;
        if !reconciliation.clears_snapshots(target.compaction) {
            debug!(
                aggregate = target.aggregate_type,
                version = target.version,
                ?stored_version,
                "Schema reconciliation preserves snapshots"
            );
            return Ok(reconciliation);
        }

        crate::clear_snapshots(
            &self.pool,
            &target.aggregate_type,
            |aggregate_id, covered_sequence, retained_events| {
                validate_snapshot_history(
                    StreamIdentity::new(&target.aggregate_type, aggregate_id),
                    covered_sequence,
                    retained_events,
                )
            },
        )
        .await?;

        info!(
            aggregate = target.aggregate_type,
            old_version = ?stored_version,
            new_version = target.version,
            "Cleared stale snapshots for schema version change"
        );

        Ok(SchemaReconciliation::Changed)
    }

    /// Records a schema version only after its recovery has durably completed.
    pub async fn record_target(&self, target: &SchemaTarget) -> Result<(), ReconcileError> {
        self.writer.record(target).await
    }

    /// Rebuilds SchemaRegistry state from the full event log.
    async fn load_registry(&self) -> Result<Option<SchemaRegistry>, ReconcileError> {
        let payloads = crate::load_schema_registry_payloads(&self.pool).await?;

        SchemaRegistry::from_payloads(payloads)
    }

    /// Reconciles `Entity`'s schema and clears mismatched retained-history snapshots.
    /// First registration preserves compactable snapshots; recorded compactable
    /// version changes are refused.
    ///
    /// Does NOT record the new version. The caller must call
    /// [`Reconciler::record_version`] only after every version-change recovery
    /// it owns (the snapshot clear here, plus any view rebuild the caller
    /// performs) has durably completed -- recording the version is the "done"
    /// marker, and a crash before it leaves the version unadvanced so the whole
    /// recovery re-runs on the next startup. The snapshot clear is idempotent,
    /// so re-running is safe.
    pub async fn reconcile<Entity: EventSourced>(
        &self,
    ) -> Result<SchemaReconciliation, ReconcileError> {
        self.reconcile_target(&SchemaTarget::new(
            Entity::AGGREGATE_TYPE,
            Entity::SCHEMA_VERSION,
            Entity::COMPACTION_POLICY,
        ))
        .await
    }

    /// Records that `Entity`'s current [`SCHEMA_VERSION`] has been fully
    /// reconciled. Call only after all version-change recovery (snapshot clear
    /// and, for projected entities, view rebuild) has durably completed, so a
    /// recorded version always implies its recovery finished.
    ///
    /// [`SCHEMA_VERSION`]: event_sorcery::EventSourced::SCHEMA_VERSION
    pub async fn record_version<Entity: EventSourced>(&self) -> Result<(), ReconcileError> {
        self.record_target(&SchemaTarget::new(
            Entity::AGGREGATE_TYPE,
            Entity::SCHEMA_VERSION,
            Entity::COMPACTION_POLICY,
        ))
        .await
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use sqlx::SqlitePool;

    use event_sorcery::{
        CompactionPolicy, Effect, Never, Nil, SCHEMA_REGISTRY_ID, test_store, uneventful,
    };

    use super::*;
    use crate::SqliteBackend;

    #[tokio::test]
    async fn reconciler_detects_version_change() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        crate::MIGRATOR.run(&pool).await.unwrap();

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
        crate::MIGRATOR.run(&pool).await.unwrap();

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

    #[tokio::test]
    async fn registry_reads_only_the_singleton_stream_in_sequence_order() {
        let pool = crate::testing::create_test_pool().await.unwrap();
        let reconciler = Reconciler::new(pool.clone());

        for version in [2, 5] {
            reconciler
                .record_target(&SchemaTarget::new(
                    "Widget",
                    version,
                    CompactionPolicy::Retain,
                ))
                .await
                .unwrap();
        }
        let registry = test_store::<SchemaRegistry, _>(&SqliteBackend { pool: pool.clone() });
        registry
            .send(
                &"not-the-registry".to_owned(),
                serde_json::from_value(json!({"Register": {"name": "Widget", "version": 9}}))
                    .unwrap(),
            )
            .await
            .unwrap();
        let other = test_store::<CompactableWidget, _>(&SqliteBackend { pool: pool.clone() });
        other
            .send(&SCHEMA_REGISTRY_ID.to_owned(), ())
            .await
            .unwrap();

        let payloads = crate::load_schema_registry_payloads(&pool)
            .await
            .unwrap()
            .into_iter()
            .map(|payload| serde_json::from_str::<serde_json::Value>(&payload).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            payloads,
            [
                json!({"VersionUpdated": {"name": "Widget", "version": 2}}),
                json!({"VersionUpdated": {"name": "Widget", "version": 5}}),
            ]
        );
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
        crate::MIGRATOR.run(&pool).await.unwrap();

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
            let pool = crate::testing::create_test_pool().await.unwrap();
            let store = test_store::<CompactableWidget, _>(&SqliteBackend { pool: pool.clone() });
            let id = "legacy-widget".to_string();
            store.send(&id, ()).await.unwrap();
            assert_eq!(
                crate::compact_events(
                    &pool,
                    CompactableWidget::AGGREGATE_TYPE,
                    CompactableWidget::COMPACTION_POLICY
                )
                .await
                .unwrap(),
                1
            );
            let retained_events: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM events WHERE aggregate_type = ?1 AND aggregate_id = ?2",
            )
            .bind("CompactableWidget")
            .bind(&id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(retained_events, 0);

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
        crate::MIGRATOR.run(&pool).await.unwrap();
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
        crate::MIGRATOR.run(&pool).await.unwrap();

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
