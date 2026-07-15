//! Native Rust [`EventBackend`] adapter over the shared SQLite engine.
//!
//! Two job-shaped primitives: the [`claim`](EventBackend::claim) transaction
//! (`BEGIN IMMEDIATE`, re-read the projection, enact the crate's decision via the
//! event stream's UNIQUE compare-and-swap, update the projection) and the
//! projection-only [`renew`](EventBackend::renew). Everything else is cqrs-es. See
//! [ADR-0006](../../adrs/0006-cqrs-native-durable-jobs.md).

use cqrs_es::persist::SerializedEvent;
use sqlx::SqlitePool;

use sqlite_es::SqliteAggregateError;

use crate::CompactionPolicy;
use crate::engine::Engine;
use crate::job_store::{ClaimDecision, ClaimOutcome, ClaimRead, EventBackend, LeaseRenewal};
use crate::sqlite_event_repository::SqliteEventRepository;

/// The default [`EventBackend`]: durable jobs over SQLite.
///
/// The pool MUST be configured with `journal_mode=WAL`, `busy_timeout>=5000ms`,
/// and `synchronous=FULL` so the compare-and-swap claim holds (a zero
/// busy-timeout makes the CAS loser see `SQLITE_BUSY` before the uniqueness
/// check, defeating the conflict contract).
#[derive(Clone)]
pub struct SqliteBackend {
    engine: Engine,
}

impl SqliteBackend {
    /// Builds a SQLite job backend over `pool`.
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            engine: Engine::new(pool),
        }
    }

    /// The underlying pool, for the SQLite-bound view reconciliation paths.
    pub(crate) fn pool(&self) -> &SqlitePool {
        self.engine.pool()
    }
}

/// Backend-native error for the SQLite job store.
#[derive(Debug, thiserror::Error)]
pub enum SqliteJobError {
    /// A direct SQL statement failed.
    #[error("job store SQL error")]
    Sql(#[from] sqlx::Error),
    /// A compare-and-swap event append failed (other than a conflict, which is
    /// reported as [`ClaimOutcome::Contended`]).
    #[error("job event append failed")]
    Append(#[from] SqliteAggregateError),
    /// A sequence value exceeded the storable range.
    #[error("job sequence out of range")]
    Sequence(#[from] std::num::TryFromIntError),
    /// A standalone [`enqueue`](EventBackend::enqueue) hit an existing stream for
    /// its `view_id` -- the caller reused a job id (a freshly minted id never
    /// collides).
    #[error("duplicate enqueue: job id {job_id} already has an event stream")]
    DuplicateEnqueue {
        /// The reused job id.
        job_id: String,
    },
}

impl EventBackend for SqliteBackend {
    type EventRepo = SqliteEventRepository;
    type Error = SqliteJobError;

    fn event_repo(&self, compaction_policy: CompactionPolicy) -> SqliteEventRepository {
        SqliteEventRepository::new(self.engine.pool().clone(), compaction_policy)
    }

    async fn migrate(&self) -> Result<(), SqliteJobError> {
        sqlite_es::MIGRATOR
            .run(self.engine.pool())
            .await
            .map_err(|error| SqliteJobError::Sql(error.into()))?;
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
        self.engine.claim_job(job_id, decide).await
    }

    async fn renew(
        &self,
        job_id: &str,
        claim_seq: i64,
        new_lease_until_ms: i64,
    ) -> Result<LeaseRenewal, SqliteJobError> {
        self.engine
            .renew_job(job_id, claim_seq, new_lease_until_ms)
            .await
    }

    async fn enqueue(&self, event: SerializedEvent, payload: String) -> Result<(), SqliteJobError> {
        self.engine.enqueue_job(event, payload).await
    }
}
