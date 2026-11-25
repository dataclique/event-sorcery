//! SQLite implementation of the shared durable storage contract.

use cqrs_es::persist::{PersistedEventRepository, SerializedEvent};
use sqlx::SqlitePool;
use std::marker::PhantomData;

use event_sorcery_core::{
    ClaimDecision, ClaimOutcome, ClaimRead, CompactionPolicy, EventBackend, LeaseRenewal,
};

use crate::{MIGRATOR, SqliteJobError, claim_job, enqueue_job, renew_job};

/// Constructs a command repository over the backend's application-owned pool.
pub trait CommandRepository: PersistedEventRepository + Send + Sync + 'static {
    /// Selects the retention policy for the aggregate handled by the repository.
    fn for_pool(pool: SqlitePool, compaction: CompactionPolicy) -> Self;
}

/// SQLite backend for atomic native commands and durable job operations.
///
/// The application's pool must use WAL, a busy timeout of at least 5 seconds,
/// and full synchronous writes.
pub struct SqliteBackend<Repository> {
    /// Pool shared by the repository, registry, views and job queue.
    pub pool: SqlitePool,
    repository: PhantomData<fn() -> Repository>,
}

impl<Repository> SqliteBackend<Repository> {
    /// Uses a caller-owned pool without applying migrations.
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            repository: PhantomData,
        }
    }
}

impl<Repository> Clone for SqliteBackend<Repository> {
    fn clone(&self) -> Self {
        Self::new(self.pool.clone())
    }
}

impl<Repository: CommandRepository> EventBackend for SqliteBackend<Repository> {
    type EventRepo = Repository;
    type Error = SqliteJobError;

    fn event_repo(&self, compaction_policy: CompactionPolicy) -> Repository {
        Repository::for_pool(self.pool.clone(), compaction_policy)
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
