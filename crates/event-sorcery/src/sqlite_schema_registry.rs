//! SQLite schema reconciliation over the canonical event-sourced registry.

use sqlx::SqlitePool;
use tracing::{debug, info};

use sqlite_es::SqliteCqrs;

use crate::lifecycle::Lifecycle;

use super::{
    REGISTRY_ID, ReconcileError, SchemaReconciliation, SchemaRegistry, SchemaRegistryCommand,
    SchemaTarget, validate_snapshot_history,
};
use crate::{EventSourced, StreamIdentity};

/// Handles schema version reconciliation at startup.
///
/// Reads state by replaying all SchemaRegistry events from the event
/// store (no views, no snapshots). Writes go through the CQRS
/// framework to maintain event sourcing invariants.
pub struct Reconciler {
    cqrs: SqliteCqrs<Lifecycle<SchemaRegistry>>,
    pool: SqlitePool,
}

impl Reconciler {
    pub fn new(pool: SqlitePool) -> Self {
        #[allow(clippy::disallowed_methods)]
        let cqrs = sqlite_es::sqlite_cqrs(pool.clone(), vec![], ());
        Self { cqrs, pool }
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
            target: "cqrs",
            aggregate = target.aggregate_type,
            ?stored_version,
            "Loaded stored schema version"
        );

        let reconciliation = target.reconciliation(registry.as_ref())?;
        if !reconciliation.clears_snapshots(target.compaction) {
            debug!(
                target: "cqrs",
                aggregate = target.aggregate_type,
                version = target.version,
                ?stored_version,
                "Schema reconciliation preserves snapshots"
            );
            return Ok(reconciliation);
        }

        let mut transaction = self.pool.begin().await?;
        let histories: Vec<(String, i64, i64)> = sqlx::query_as(
            "SELECT s.aggregate_id, s.last_sequence, \
             (SELECT COUNT(*) FROM events e \
              WHERE e.aggregate_type = s.aggregate_type \
                AND e.aggregate_id = s.aggregate_id \
                AND e.sequence BETWEEN 1 AND s.last_sequence) \
             FROM snapshots s WHERE s.aggregate_type = ?1",
        )
        .bind(&target.aggregate_type)
        .fetch_all(&mut *transaction)
        .await?;
        for (aggregate_id, covered_sequence, retained_events) in histories {
            validate_snapshot_history(
                StreamIdentity::new(&target.aggregate_type, aggregate_id),
                covered_sequence,
                retained_events,
            )?;
        }
        sqlx::query("DELETE FROM snapshots WHERE aggregate_type = ?1")
            .bind(&target.aggregate_type)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;

        info!(
            target: "cqrs",
            aggregate = target.aggregate_type,
            old_version = ?stored_version,
            new_version = target.version,
            "Cleared stale snapshots for schema version change"
        );

        Ok(SchemaReconciliation::Changed)
    }

    /// Records a schema version only after its recovery has durably completed.
    pub async fn record_target(&self, target: &SchemaTarget) -> Result<(), ReconcileError> {
        self.cqrs
            .execute(
                REGISTRY_ID,
                SchemaRegistryCommand::Register {
                    name: target.aggregate_type.clone(),
                    version: target.version,
                },
            )
            .await?;

        Ok(())
    }

    /// Rebuilds SchemaRegistry state from the full event log.
    pub(super) async fn load_registry(&self) -> Result<Option<SchemaRegistry>, ReconcileError> {
        let payloads = sqlx::query_scalar!(
            r#"
            SELECT payload AS "payload: String"
            FROM events
            WHERE aggregate_type = 'SchemaRegistry'
              AND aggregate_id = 'schema'
            ORDER BY sequence
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

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
    #[allow(clippy::cognitive_complexity)]
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
    /// [`SCHEMA_VERSION`]: crate::EventSourced::SCHEMA_VERSION
    pub async fn record_version<Entity: EventSourced>(&self) -> Result<(), ReconcileError> {
        self.record_target(&SchemaTarget::new(
            Entity::AGGREGATE_TYPE,
            Entity::SCHEMA_VERSION,
            Entity::COMPACTION_POLICY,
        ))
        .await
    }
}
