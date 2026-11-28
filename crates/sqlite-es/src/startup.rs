//! Startup wiring for native SQLite stores.
use cqrs_es::persist::PersistenceError;

use event_sorcery::{
    EventSourced, Nil, ProjectedStoreBackend, Projection, ProjectionError, ReconcileError,
    SchemaReconciliation, Table, UnmaterializedStoreBackend,
};

use crate::{Reconciler, SqliteBackend, SqliteViewBackend};

impl ProjectedStoreBackend for SqliteBackend {
    type ViewBackend = SqliteViewBackend;

    async fn reconcile_projected<Entity>(
        &self,
    ) -> Result<Projection<Entity, SqliteViewBackend>, ReconcileError>
    where
        Entity: EventSourced<Materialized = Table> + 'static,
    {
        let pool = self.pool.clone();
        let reconciler = Reconciler::new(pool.clone());
        let reconciliation = reconciler.reconcile::<Entity>().await?;

        let projection = Projection::with_backend(pool);

        // A schema version change can leave stored view payloads in an
        // incompatible format. `catch_up` only revisits views that are behind
        // on event sequence, so a view that is current but incompatible (the
        // normal state after a view-schema change with no new events) would
        // never be healed. On a detected schema change, rebuild every view
        // from the event log; otherwise just replay any events the view missed
        // due to a crash between event persistence and view update. Projected
        // entities are guaranteed `CompactionPolicy::Retain` (asserted above),
        // so the full history is always available to rebuild from. Either path
        // runs before registering the projection as a reactor so no concurrent
        // writes can interfere.
        let recovery = match reconciliation {
            SchemaReconciliation::Changed => projection.rebuild_all().await,
            SchemaReconciliation::Unchanged => projection.catch_up().await,
        };
        recovery.map_err(|error| match error {
            ProjectionError::Sqlx(sqlx_error) => ReconcileError::from(sqlx_error),
            ProjectionError::Persistence(persistence_error) => {
                ReconcileError::from(persistence_error)
            }
            other => ReconcileError::Persistence(PersistenceError::UnknownError(Box::new(other))),
        })?;

        // Mark the schema version reconciled only after the snapshot clear and
        // view recovery above have durably completed. A crash before this point
        // leaves the version unadvanced, so the next startup re-runs the whole
        // reconcile -- including rebuild_all -- instead of recording a version
        // whose view rebuild never finished.
        reconciler.record_version::<Entity>().await?;

        Ok(projection)
    }
}

impl UnmaterializedStoreBackend for SqliteBackend {
    async fn reconcile_unmaterialized<Entity: EventSourced<Materialized = Nil>>(
        &self,
    ) -> Result<(), ReconcileError> {
        let reconciler = Reconciler::new(self.pool.clone());
        let _ = reconciler.reconcile::<Entity>().await?;
        reconciler.record_version::<Entity>().await
    }
}
