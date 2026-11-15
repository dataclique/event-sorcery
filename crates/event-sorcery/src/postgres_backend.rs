//! Native PostgreSQL persistence capabilities over the shared repository.

use cqrs_es::Aggregate;
use cqrs_es::persist::{
    PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot,
};
use serde_json::Value;
use sqlx::PgPool;

use crate::{CompactionPolicy, PersistenceBackend, PostgresEventRepository};

/// Native persistence capability over an application-provisioned pool.
#[derive(Clone)]
pub struct PostgresBackend {
    /// Application-provisioned PostgreSQL connection pool.
    pub pool: PgPool,
}

/// Policy-aware repository constructed by [`PostgresBackend`].
pub struct PostgresStoreRepository {
    repository: PostgresEventRepository,
    compaction_policy: CompactionPolicy,
}

#[derive(Debug, thiserror::Error)]
#[error("PostgreSQL native persistence capability is not implemented")]
struct PostgresPersistencePending;

impl PersistenceBackend for PostgresBackend {
    type PersistenceRepo = PostgresStoreRepository;
    type BootstrapError = sqlx::Error;

    fn persistence_repo(&self, policy: CompactionPolicy) -> Self::PersistenceRepo {
        PostgresStoreRepository {
            repository: PostgresEventRepository {
                pool: self.pool.clone(),
            },
            compaction_policy: policy,
        }
    }

    async fn bootstrap_persistence(&self) -> Result<(), Self::BootstrapError> {
        Err(sqlx::Error::Configuration(Box::new(
            PostgresPersistencePending,
        )))
    }
}

impl PersistedEventRepository for PostgresStoreRepository {
    async fn get_events<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.repository
            .get_events::<AggregateType>(aggregate_id)
            .await
    }

    async fn get_last_events<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.repository
            .get_last_events::<AggregateType>(aggregate_id, last_sequence)
            .await
    }

    async fn get_snapshot<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        match self.compaction_policy {
            CompactionPolicy::Retain => {
                self.repository
                    .get_snapshot::<AggregateType>(aggregate_id)
                    .await
            }
            CompactionPolicy::CompactAfterSnapshot | CompactionPolicy::RetainLast { .. } => Err(
                PersistenceError::UnknownError(Box::new(PostgresPersistencePending)),
            ),
        }
    }

    async fn persist<AggregateType: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        self.repository
            .persist::<AggregateType>(events, snapshot_update)
            .await
    }

    async fn persist_with_snapshot<AggregateType: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<SerializedSnapshot>,
    ) -> Result<(), PersistenceError> {
        self.repository
            .persist_with_snapshot::<AggregateType>(events, snapshot_update)
            .await
    }

    async fn stream_events<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<ReplayStream, PersistenceError> {
        self.repository
            .stream_events::<AggregateType>(aggregate_id)
            .await
    }

    async fn stream_all_events<AggregateType: Aggregate>(
        &self,
    ) -> Result<ReplayStream, PersistenceError> {
        self.repository.stream_all_events::<AggregateType>().await
    }
}
