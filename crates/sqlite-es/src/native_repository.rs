//! CQRS repository integration with caller-owned commit preparation and snapshot policy.

use std::future::{Future, poll_fn};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::Poll;

use cqrs_es::Aggregate;
use cqrs_es::persist::{
    EventUpcaster, PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent,
    SerializedSnapshot,
};
use futures_util::TryStreamExt;
use serde_json::Value;
use sqlx::SqlitePool;

use crate::event_repository::row_to_serialized_event;
use crate::{Commit, SqliteEventRepository, commit_atomic};

/// Snapshot coverage supplied by the two cqrs-es persistence entry points.
pub enum SnapshotInput {
    Counter {
        aggregate_id: String,
        aggregate: Value,
        snapshot_version: usize,
    },
    Covered(SerializedSnapshot),
}

/// Domain preparation and completion without database-specific logic.
pub trait RepositoryPolicy: Clone + Send + Sync + 'static {
    type Pending: Send;

    fn prepare<'events>(
        &self,
        aggregate_type: &str,
        events: &'events [SerializedEvent],
        snapshot: Option<SnapshotInput>,
    ) -> Result<(Commit<'events>, Self::Pending), PersistenceError>;

    fn committed(pending: Self::Pending);

    fn snapshot<AggregateType: Aggregate>(
        &self,
        snapshot: SerializedSnapshot,
    ) -> Result<SerializedSnapshot, PersistenceError>;
}

/// SQLite CQRS repository with native command preparation and ordered upcasting.
pub struct NativeRepository<Policy: RepositoryPolicy> {
    pool: SqlitePool,
    repository: SqliteEventRepository,
    policy: Policy,
    upcasters: Arc<Vec<Arc<dyn EventUpcaster>>>,
    stream_channel_size: usize,
}

impl<Policy: RepositoryPolicy> NativeRepository<Policy> {
    /// Uses caller-owned policy while keeping all reads and writes in this backend.
    pub fn new(pool: SqlitePool, policy: impl Into<Policy>) -> Self {
        Self {
            repository: SqliteEventRepository::new(pool.clone()),
            pool,
            policy: policy.into(),
            upcasters: Arc::new(Vec::new()),
            stream_channel_size: 1000,
        }
    }

    /// Sets a nonzero bounded replay buffer.
    #[must_use]
    pub fn with_stream_channel_size(mut self, size: NonZeroUsize) -> Self {
        self.stream_channel_size = size.get();
        self
    }

    /// Applies upcasters in registration order on every read and replay.
    #[must_use]
    pub fn with_upcasters(mut self, upcasters: Vec<Arc<dyn EventUpcaster>>) -> Self {
        self.upcasters = Arc::new(upcasters);
        self
    }

    async fn persist_updates<AggregateType: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot: Option<SnapshotInput>,
    ) -> Result<(), PersistenceError> {
        let (commit, pending) = self.policy.prepare(AggregateType::TYPE, events, snapshot)?;
        commit_atomic(&self.pool, commit).await?;
        Policy::committed(pending);
        Ok(())
    }

    fn stream<AggregateType: Aggregate>(&self, aggregate_id: Option<String>) -> ReplayStream {
        let (mut feed, stream) = ReplayStream::new(self.stream_channel_size);
        let pool = self.pool.clone();
        let aggregate_type = AggregateType::TYPE;
        let upcasters = Arc::clone(&self.upcasters);
        tokio::spawn(async move {
            let mut rows = sqlx::query(
                "SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, \
                 payload, metadata FROM events WHERE aggregate_type = ?1 \
                 AND (?2 IS NULL OR aggregate_id = ?2) ORDER BY sequence",
            )
            .bind(aggregate_type)
            .bind(aggregate_id)
            .fetch(&pool);
            loop {
                let event = match rows.try_next().await {
                    Ok(Some(row)) => row_to_serialized_event(&row)
                        .map(|event| upcast_event(event, &upcasters))
                        .map_err(PersistenceError::from),
                    Ok(None) => return,
                    Err(source) => Err(PersistenceError::ConnectionError(Box::new(source))),
                };
                match event {
                    Ok(event) => {
                        if feed.push(Ok(event)).await.is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        let _ = feed.push(Err(error)).await;
                        return;
                    }
                }
            }
        });
        stream
    }
}

impl<Policy: RepositoryPolicy> PersistedEventRepository for NativeRepository<Policy> {
    async fn get_events<AggregateType: Aggregate>(
        &self,
        id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        Ok(self
            .repository
            .get_events::<AggregateType>(id)
            .await?
            .into_iter()
            .map(|event| upcast_event(event, &self.upcasters))
            .collect())
    }

    async fn get_last_events<AggregateType: Aggregate>(
        &self,
        id: &str,
        sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        Ok(self
            .repository
            .get_last_events::<AggregateType>(id, sequence)
            .await?
            .into_iter()
            .map(|event| upcast_event(event, &self.upcasters))
            .collect())
    }

    async fn get_snapshot<AggregateType: Aggregate>(
        &self,
        id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        self.repository
            .get_snapshot::<AggregateType>(id)
            .await?
            .map(|snapshot| self.policy.snapshot::<AggregateType>(snapshot))
            .transpose()
    }

    async fn persist<AggregateType: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        let snapshot =
            snapshot.map(
                |(aggregate_id, aggregate, snapshot_version)| SnapshotInput::Counter {
                    aggregate_id,
                    aggregate,
                    snapshot_version,
                },
            );
        self.persist_updates::<AggregateType>(events, snapshot)
            .await
    }

    async fn persist_with_snapshot<AggregateType: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot: Option<SerializedSnapshot>,
    ) -> Result<(), PersistenceError> {
        self.persist_updates::<AggregateType>(events, snapshot.map(SnapshotInput::Covered))
            .await
    }

    fn stream_events<AggregateType: Aggregate>(
        &self,
        id: &str,
    ) -> impl Future<Output = Result<ReplayStream, PersistenceError>> + Send {
        poll_fn(move |_| Poll::Ready(Ok(self.stream::<AggregateType>(Some(id.to_string())))))
    }

    fn stream_all_events<AggregateType: Aggregate>(
        &self,
    ) -> impl Future<Output = Result<ReplayStream, PersistenceError>> + Send {
        poll_fn(move |_| Poll::Ready(Ok(self.stream::<AggregateType>(None))))
    }
}

fn upcast_event(
    mut event: SerializedEvent,
    upcasters: &[Arc<dyn EventUpcaster>],
) -> SerializedEvent {
    for upcaster in upcasters {
        if upcaster.can_upcast(&event.event_type, &event.event_version) {
            event = upcaster.upcast(event);
        }
    }
    event
}
