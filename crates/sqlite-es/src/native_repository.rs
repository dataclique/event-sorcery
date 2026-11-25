//! SQLite storage adapter for native command repositories.

use std::num::NonZeroUsize;
use std::sync::Arc;

use cqrs_es::Aggregate;
use cqrs_es::persist::{
    EventUpcaster, PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent,
    SerializedSnapshot,
};
use serde_json::Value;
use sqlx::sqlite::SqliteRow;
use sqlx::{FromRow, SqlitePool};

use event_sorcery_core::CompactionPolicy;

use crate::{
    CommandRepository, SqliteAggregateError, load_event_rows, load_snapshot, replay_events,
};

/// Snapshot inputs supplied by the two CQRS persistence entry points.
pub enum SnapshotInput {
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

/// Domain policy supplied by the framework, without exposing its aggregate adapter.
pub trait RepositoryPolicy: Send + Sync + 'static {
    /// Row decoder retaining the framework's public error contract.
    type EventRow: for<'row> FromRow<'row, SqliteRow>
        + TryInto<SerializedEvent, Error: Into<PersistenceError>>
        + Send
        + Unpin
        + 'static;

    /// Prepares a native commit, delegates to the writer and acknowledges pending jobs.
    fn persist(
        pool: &SqlitePool,
        aggregate_type: &str,
        events: &[SerializedEvent],
        snapshot: Option<SnapshotInput>,
    ) -> impl Future<Output = Result<(), PersistenceError>> + Send;

    /// Checks whether the repository can safely return the stored snapshot.
    fn guard_snapshot<AggregateType: Aggregate>(
        snapshot: SerializedSnapshot,
        policy: CompactionPolicy,
    ) -> Result<SerializedSnapshot, PersistenceError>;
}

/// SQLite CQRS repository with atomic writes and ordered read-time upcasting.
pub struct NativeRepository<Policy: RepositoryPolicy> {
    pool: SqlitePool,
    compaction: CompactionPolicy,
    upcasters: Arc<Vec<Arc<dyn EventUpcaster>>>,
    channel_size: usize,
    policy: std::marker::PhantomData<Policy>,
}

impl<Policy: RepositoryPolicy> NativeRepository<Policy> {
    /// Uses a caller-owned pool without applying migrations.
    #[must_use]
    pub fn new(pool: SqlitePool, compaction: CompactionPolicy) -> Self {
        Self {
            pool,
            compaction,
            upcasters: Arc::new(Vec::new()),
            channel_size: 1000,
            policy: std::marker::PhantomData,
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
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(SqliteAggregateError::from)?;
        load_event_rows::<Policy::EventRow>(
            &mut connection,
            AggregateType::TYPE,
            aggregate_id,
            after,
        )
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
        replay_events::<Policy::EventRow>(
            &self.pool,
            AggregateType::TYPE,
            aggregate_id,
            self.channel_size,
            move |event| upcast(event, &upcasters),
        )
    }
}

impl<Policy: RepositoryPolicy> CommandRepository for NativeRepository<Policy> {
    fn for_pool(pool: SqlitePool, compaction: CompactionPolicy) -> Self {
        Self::new(pool, compaction)
    }
}

impl<Policy: RepositoryPolicy> PersistedEventRepository for NativeRepository<Policy> {
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
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(SqliteAggregateError::from)?;
        load_snapshot(&mut connection, AggregateType::TYPE, aggregate_id)
            .await?
            .map(|snapshot| Policy::guard_snapshot::<AggregateType>(snapshot, self.compaction))
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
        Policy::persist(&self.pool, AggregateType::TYPE, events, snapshot).await
    }

    async fn persist_with_snapshot<AggregateType: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot: Option<SerializedSnapshot>,
    ) -> Result<(), PersistenceError> {
        Policy::persist(
            &self.pool,
            AggregateType::TYPE,
            events,
            snapshot.map(SnapshotInput::Covered),
        )
        .await
    }

    async fn stream_events<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<ReplayStream, PersistenceError> {
        Ok(self.replay::<AggregateType>(Some(aggregate_id.to_owned())))
    }

    async fn stream_all_events<AggregateType: Aggregate>(
        &self,
    ) -> Result<ReplayStream, PersistenceError> {
        Ok(self.replay::<AggregateType>(None))
    }
}

fn upcast(mut event: SerializedEvent, upcasters: &[Arc<dyn EventUpcaster>]) -> SerializedEvent {
    for upcaster in upcasters {
        if upcaster.can_upcast(&event.event_type, &event.event_version) {
            event = upcaster.upcast(event);
        }
    }
    event
}
