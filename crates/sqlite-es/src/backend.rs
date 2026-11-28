//! SQLite implementation of the shared durable storage contract.

use cqrs_es::persist::SerializedEvent;
use sqlx::SqlitePool;

use event_sorcery_core::{
    ClaimDecision, ClaimOutcome, ClaimRead, CompactionPolicy, EventBackend, LeaseRenewal,
};

use crate::{MIGRATOR, NativeRepository, SqliteJobError, claim_job, enqueue_job, renew_job};

/// SQLite backend for atomic native commands and durable job operations.
///
/// The application pool must use WAL, a busy timeout of at least 5 seconds, and full synchronous writes.
#[derive(Clone)]
pub struct SqliteBackend {
    /// Pool shared by the repository, registry, views and job queue.
    pub pool: SqlitePool,
}

impl EventBackend for SqliteBackend {
    type EventRepo = NativeRepository;
    type Error = SqliteJobError;

    fn event_repo(&self, compaction_policy: CompactionPolicy) -> NativeRepository {
        NativeRepository::new(self.pool.clone(), compaction_policy)
    }

    async fn migrate(&self) -> Result<(), SqliteJobError> {
        MIGRATOR.run(&self.pool).await.map_err(sqlx::Error::from)?;
        Ok(())
    }

    async fn claim<Decide, Won>(
        &self,
        job_id: &str,
        decide: Decide,
    ) -> Result<ClaimOutcome<Won>, SqliteJobError>
    where
        Decide: FnOnce(Option<ClaimRead>) -> ClaimDecision<Won> + Send,
        Won: Send,
    {
        claim_job(&self.pool, job_id, decide).await
    }

    async fn renew(
        &self,
        job_id: &str,
        claim_seq: i64,
        new_lease_until_ms: i64,
    ) -> Result<LeaseRenewal, SqliteJobError> {
        renew_job(&self.pool, job_id, claim_seq, new_lease_until_ms).await
    }

    async fn enqueue(&self, event: SerializedEvent, payload: String) -> Result<(), SqliteJobError> {
        enqueue_job(&self.pool, event, payload).await
    }
}
