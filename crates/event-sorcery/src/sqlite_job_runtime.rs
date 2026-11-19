//! SQLite construction for the backend-independent durable job runtime.

use sqlx::SqlitePool;

use super::JobRuntime;
use crate::job::JobState;
use crate::{ReconcileError, SqliteBackend, StoreBuilder};

impl JobRuntime<SqliteBackend> {
    /// Wires the SQLite job runtime over `pool`: the job `Store` + `job_queue_projection`
    /// projection. The canonical schema (events + snapshots + `job_queue_projection`) must
    /// already be applied -- like [`StoreBuilder`], the consumer owns migrations,
    /// so this composes with the consumer's own view migrations instead of
    /// re-running event-sorcery's and conflicting. `pool` MUST satisfy the pool
    /// contract (WAL, `busy_timeout>=5000ms`, `synchronous=FULL`); see
    /// [`SqliteBackend`].
    pub async fn build(pool: SqlitePool) -> Result<Self, ReconcileError> {
        let backend = SqliteBackend::new(pool);
        let (jobs, queue) = StoreBuilder::<JobState, _>::with_backend(backend.clone())
            .build()
            .await?;
        Ok(Self {
            backend,
            jobs,
            queue,
        })
    }
}
