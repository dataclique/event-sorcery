//! SQLite storage adapter for native command repositories.

use std::future::{Future, poll_fn};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::Poll;

use cqrs_es::Aggregate;
use cqrs_es::persist::{
    EventUpcaster, PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent,
    SerializedSnapshot,
};
use serde_json::Value;
use sqlx::SqlitePool;

use event_sorcery::{CommitRequest, SnapshotUpdate, guard_snapshot};
use event_sorcery_core::CompactionPolicy;

use crate::engine::commit_serialized;
use crate::{StoredEventRow, load_event_rows, load_snapshot, replay_events};

/// Snapshot inputs supplied by the two CQRS persistence entry points.
enum SnapshotInput {
    /// Snapshot at the end of the event batch.
    BatchEnd {
        /// Aggregate ID associated with the snapshot.
        aggregate_id: String,
        /// Serialized aggregate state.
        aggregate: Value,
        /// Snapshot write counter, independent of event sequencing.
        snapshot_version: usize,
    },
    /// Snapshot that specifies exactly which events it covers.
    Covered(SerializedSnapshot),
}

/// SQLite CQRS repository with atomic writes and ordered read-time upcasting.
pub struct NativeRepository {
    pool: SqlitePool,
    compaction: CompactionPolicy,
    upcasters: Arc<Vec<Arc<dyn EventUpcaster>>>,
    channel_size: usize,
}

impl NativeRepository {
    /// Uses a caller-owned pool without applying migrations.
    #[must_use]
    pub fn new(pool: SqlitePool, compaction: CompactionPolicy) -> Self {
        Self {
            pool,
            compaction,
            upcasters: Arc::new(Vec::new()),
            channel_size: 1000,
        }
    }

    /// Sets a bounded, nonzero event replay buffer.
    #[must_use]
    pub fn with_stream_channel_size(mut self, size: NonZeroUsize) -> Self {
        self.channel_size = size.get();
        self
    }

    /// Applies upcasters in order so each step sees the previous step's result.
    #[must_use]
    pub fn with_upcasters(mut self, upcasters: Vec<Arc<dyn EventUpcaster>>) -> Self {
        self.upcasters = Arc::new(upcasters);
        self
    }

    async fn load_events<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
        after: Option<usize>,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        load_event_rows::<StoredEventRow>(&self.pool, AggregateType::TYPE, aggregate_id, after)
            .await?
            .into_iter()
            .map(|row| {
                row.try_into()
                    .map(|event| upcast(event, &self.upcasters))
                    .map_err(Into::into)
            })
            .collect()
    }

    fn replay<AggregateType: Aggregate>(&self, aggregate_id: Option<String>) -> ReplayStream {
        let upcasters = Arc::clone(&self.upcasters);
        replay_events::<StoredEventRow>(
            &self.pool,
            AggregateType::TYPE,
            aggregate_id,
            self.channel_size,
            move |event| upcast(event, &upcasters),
        )
    }
}

impl PersistedEventRepository for NativeRepository {
    async fn get_events<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.load_events::<AggregateType>(aggregate_id, None).await
    }

    async fn get_last_events<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
        sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.load_events::<AggregateType>(aggregate_id, Some(sequence))
            .await
    }

    async fn get_snapshot<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        load_snapshot(&self.pool, AggregateType::TYPE, aggregate_id)
            .await?
            .map(|snapshot| guard_snapshot::<AggregateType>(snapshot, self.compaction))
            .transpose()
    }

    async fn persist<AggregateType: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        let snapshot =
            snapshot.map(
                |(aggregate_id, aggregate, snapshot_version)| SnapshotInput::BatchEnd {
                    aggregate_id,
                    aggregate,
                    snapshot_version,
                },
            );
        persist(&self.pool, AggregateType::TYPE, events, snapshot).await
    }

    async fn persist_with_snapshot<AggregateType: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot: Option<SerializedSnapshot>,
    ) -> Result<(), PersistenceError> {
        persist(
            &self.pool,
            AggregateType::TYPE,
            events,
            snapshot.map(SnapshotInput::Covered),
        )
        .await
    }

    fn stream_events<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> impl Future<Output = Result<ReplayStream, PersistenceError>> + Send {
        poll_fn(move |_| {
            Poll::Ready(Ok(
                self.replay::<AggregateType>(Some(aggregate_id.to_owned()))
            ))
        })
    }

    fn stream_all_events<AggregateType: Aggregate>(
        &self,
    ) -> impl Future<Output = Result<ReplayStream, PersistenceError>> + Send {
        poll_fn(move |_| Poll::Ready(Ok(self.replay::<AggregateType>(None))))
    }
}

async fn persist(
    pool: &SqlitePool,
    aggregate_type: &str,
    events: &[SerializedEvent],
    snapshot: Option<SnapshotInput>,
) -> Result<(), PersistenceError> {
    let update = snapshot.map(|snapshot| match snapshot {
        SnapshotInput::BatchEnd {
            aggregate_id,
            aggregate,
            snapshot_version,
        } => (
            aggregate_id,
            SnapshotUpdate::new(aggregate, snapshot_version),
        ),
        SnapshotInput::Covered(snapshot) => SnapshotUpdate::from_serialized(snapshot),
    });
    let (request, pending_jobs) =
        CommitRequest::prepare_repository(aggregate_type, events, update)?;
    commit_serialized(pool, request.check()?).await?;
    pending_jobs.mark_committed();
    Ok(())
}

fn upcast(mut event: SerializedEvent, upcasters: &[Arc<dyn EventUpcaster>]) -> SerializedEvent {
    for upcaster in upcasters {
        if upcaster.can_upcast(&event.event_type, &event.event_version) {
            event = upcaster.upcast(event);
        }
    }
    event
}
