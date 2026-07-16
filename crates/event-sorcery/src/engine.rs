//! Serialized SQLite storage facade shared by native and foreign-language callers.
//!
//! The engine owns storage-boundary reads and replay plus the transactions that
//! atomically commit aggregate events, snapshots, and durable job intent.

use std::num::NonZeroUsize;

use cqrs_es::persist::{PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot};
use futures_util::TryStreamExt;
use serde_json::Value;
use sqlx::{SqliteConnection, SqlitePool};

use crate::job::EnqueueRequest;
use crate::job_sqlite::SqliteJobError;
use crate::job_store::{ClaimDecision, ClaimOutcome, ClaimRead, LeaseRenewal};

/// Identifies one serialized aggregate event stream at the storage boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamIdentity {
    aggregate_type: String,
    aggregate_id: String,
}

/// Constructs and compares storage-boundary stream identities.
impl StreamIdentity {
    /// Creates a storage-boundary identity from serialized aggregate values.
    pub fn new(aggregate_type: impl Into<String>, aggregate_id: impl Into<String>) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
        }
    }

    fn matches(&self, event: &SerializedEvent) -> bool {
        self.aggregate_type == event.aggregate_type && self.aggregate_id == event.aggregate_id
    }

    fn from_event(event: &SerializedEvent) -> Self {
        Self::new(&event.aggregate_type, &event.aggregate_id)
    }
}

/// Snapshot payload written atomically with the events it covers.
pub struct SnapshotUpdate {
    aggregate: Value,
    snapshot_version: usize,
}

impl SnapshotUpdate {
    /// Creates a serialized snapshot update at its aggregate schema version.
    pub fn new(aggregate: Value, snapshot_version: usize) -> Self {
        Self {
            aggregate,
            snapshot_version,
        }
    }
}

/// One atomic event-store commit, including optional snapshot and job intent.
pub struct CommitRequest<'events> {
    stream: StreamIdentity,
    events: &'events [SerializedEvent],
    snapshot: Option<SnapshotUpdate>,
    jobs: Vec<EnqueueRequest>,
}

/// Shared serialized SQLite persistence engine.
#[derive(Clone)]
pub struct Engine {
    pool: SqlitePool,
}

/// Failures produced by serialized engine operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EngineError {
    #[error("optimistic lock error: aggregate has been modified concurrently")]
    OptimisticLock,
    #[error("snapshot update was requested without persisted events")]
    EmptySnapshotUpdate,
    #[error("commit event stream {actual:?} does not match requested stream {expected:?}")]
    StreamIdentityMismatch {
        expected: StreamIdentity,
        actual: StreamIdentity,
    },
    #[error("event page storage bytes {observed} exceed limit {limit}")]
    EventPageTooLarge { observed: usize, limit: usize },
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Integer(#[from] std::num::TryFromIntError),
    #[error(transparent)]
    JobFlush(#[from] crate::job::JobStoreError),
}

impl<'events> CommitRequest<'events> {
    /// Starts a commit for one stream and its proposed events.
    pub fn new(stream: StreamIdentity, events: &'events [SerializedEvent]) -> Self {
        Self {
            stream,
            events,
            snapshot: None,
            jobs: vec![],
        }
    }

    /// Attaches the snapshot covered by this commit's events.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::EmptySnapshotUpdate`] when this request has no
    /// events to persist.
    pub fn with_snapshot(mut self, snapshot: SnapshotUpdate) -> Result<Self, EngineError> {
        if self.events.is_empty() {
            return Err(EngineError::EmptySnapshotUpdate);
        }
        self.snapshot = Some(snapshot);
        Ok(self)
    }

    #[must_use]
    /// Attaches durable jobs that must commit atomically with the events.
    pub(crate) fn with_jobs(mut self, jobs: Vec<EnqueueRequest>) -> Self {
        self.jobs = jobs;
        self
    }
}

impl Engine {
    /// Creates an engine over a SQLite pool.
    ///
    /// This constructor does not run migrations. Call [`Self::migrate`] before
    /// using storage operations when the pool's schema is not initialized.
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Migrates the engine's existing SQLite schema.
    pub async fn migrate(&self) -> Result<(), SqliteJobError> {
        sqlite_es::MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|error| SqliteJobError::Sql(error.into()))?;
        Ok(())
    }

    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn claim_job<Decide, Won>(
        &self,
        job_id: &str,
        decide: Decide,
    ) -> Result<ClaimOutcome<Won>, SqliteJobError>
    where
        Decide: FnOnce(Option<ClaimRead>) -> ClaimDecision<Won> + Send,
        Won: Send,
    {
        immediate_transaction(&self.pool, job_id, async move |connection| {
            let outcome = claim_job_in_transaction(connection, job_id, decide).await?;
            Ok(match outcome {
                ClaimOutcome::Won(won) => TransactionOutcome::Commit(ClaimOutcome::Won(won)),
                ClaimOutcome::Abandoned => TransactionOutcome::Commit(ClaimOutcome::Abandoned),
                ClaimOutcome::Contended => TransactionOutcome::Rollback(ClaimOutcome::Contended),
                ClaimOutcome::Skip => TransactionOutcome::Rollback(ClaimOutcome::Skip),
            })
        })
        .await
    }

    pub async fn renew_job(
        &self,
        job_id: &str,
        claim_seq: i64,
        new_lease_until_ms: i64,
    ) -> Result<LeaseRenewal, SqliteJobError> {
        let done = sqlx::query!(
            r#"
            UPDATE job_queue
            SET lease_until = ?1
            WHERE view_id = ?2
              AND version = ?3
              AND status = 'claimed'
            "#,
            new_lease_until_ms,
            job_id,
            claim_seq,
        )
        .execute(&self.pool)
        .await?;

        if done.rows_affected() == 0 {
            Ok(LeaseRenewal::Lost)
        } else {
            Ok(LeaseRenewal::Held)
        }
    }

    pub async fn enqueue_job(
        &self,
        event: SerializedEvent,
        payload: String,
    ) -> Result<(), SqliteJobError> {
        let job_id = event.aggregate_id.clone();
        let mut transaction = self.pool.begin().await?;

        let seeded = match append_job_event(&mut transaction, &event).await? {
            Some(version) => {
                write_job_projection(&mut transaction, &job_id, version, &payload, None).await
            }
            None => Err(SqliteJobError::DuplicateEnqueue { job_id }),
        };

        match seeded {
            Ok(()) => {
                transaction.commit().await?;
                Ok(())
            }
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }

    /// Loads one stream, optionally after an exclusive sequence checkpoint.
    pub async fn load_events(
        &self,
        stream: &StreamIdentity,
        after_sequence: Option<usize>,
    ) -> Result<Vec<SerializedEvent>, EngineError> {
        let rows = match after_sequence {
            None => {
                sqlx::query_as!(
                    StoredEventRow,
                    r#"
                    SELECT aggregate_type,
                           aggregate_id,
                           sequence,
                           event_type,
                           event_version,
                           payload AS "payload: String",
                           metadata AS "metadata: String"
                    FROM events
                    WHERE aggregate_type = ?1 AND aggregate_id = ?2
                    ORDER BY sequence
                    "#,
                    stream.aggregate_type,
                    stream.aggregate_id,
                )
                .fetch_all(&self.pool)
                .await?
            }
            Some(after_sequence) => {
                let after_sequence = i64::try_from(after_sequence)?;
                sqlx::query_as!(
                    StoredEventRow,
                    r#"
                    SELECT aggregate_type,
                           aggregate_id,
                           sequence,
                           event_type,
                           event_version,
                           payload AS "payload: String",
                           metadata AS "metadata: String"
                    FROM events
                    WHERE aggregate_type = ?1
                      AND aggregate_id = ?2
                      AND sequence > ?3
                    ORDER BY sequence
                    "#,
                    stream.aggregate_type,
                    stream.aggregate_id,
                    after_sequence,
                )
                .fetch_all(&self.pool)
                .await?
            }
        };

        rows.into_iter().map(SerializedEvent::try_from).collect()
    }

    /// Loads at most `limit` events from one stream after an optional exclusive
    /// sequence checkpoint.
    pub async fn load_events_page(
        &self,
        stream: &StreamIdentity,
        after_sequence: Option<usize>,
        limit: NonZeroUsize,
    ) -> Result<Vec<SerializedEvent>, EngineError> {
        let limit = i64::try_from(limit.get())?;
        let rows = match after_sequence {
            None => {
                sqlx::query_as::<_, StoredEventRow>(
                    r"
                    SELECT aggregate_type,
                           aggregate_id,
                           sequence,
                           event_type,
                           event_version,
                           payload,
                           metadata
                    FROM events
                    WHERE aggregate_type = ?1 AND aggregate_id = ?2
                    ORDER BY sequence
                    LIMIT ?3
                    ",
                )
                .bind(&stream.aggregate_type)
                .bind(&stream.aggregate_id)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
            Some(after_sequence) => {
                let after_sequence = i64::try_from(after_sequence)?;
                sqlx::query_as::<_, StoredEventRow>(
                    r"
                    SELECT aggregate_type,
                           aggregate_id,
                           sequence,
                           event_type,
                           event_version,
                           payload,
                           metadata
                    FROM events
                    WHERE aggregate_type = ?1
                      AND aggregate_id = ?2
                      AND sequence > ?3
                    ORDER BY sequence
                    LIMIT ?4
                    ",
                )
                .bind(&stream.aggregate_type)
                .bind(&stream.aggregate_id)
                .bind(after_sequence)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?
            }
        };

        rows.into_iter().map(SerializedEvent::try_from).collect()
    }

    /// Loads an item- and storage-byte-bounded event page.
    pub async fn load_events_page_bounded(
        &self,
        stream: &StreamIdentity,
        after_sequence: Option<usize>,
        item_limit: NonZeroUsize,
        byte_limit: NonZeroUsize,
    ) -> Result<Vec<SerializedEvent>, EngineError> {
        let item_limit = i64::try_from(item_limit.get())?;
        let mut transaction = self.pool.begin().await?;
        let storage_bytes = match after_sequence {
            None => {
                sqlx::query_scalar::<_, i64>(
                    r"
                    SELECT COALESCE(SUM(
                        length(CAST(aggregate_type AS BLOB))
                        + length(CAST(aggregate_id AS BLOB))
                        + 8
                        + length(CAST(event_type AS BLOB))
                        + length(CAST(event_version AS BLOB))
                        + length(CAST(payload AS BLOB))
                        + length(CAST(metadata AS BLOB))
                    ), 0)
                    FROM (
                        SELECT aggregate_type,
                               aggregate_id,
                               sequence,
                               event_type,
                               event_version,
                               payload,
                               metadata
                        FROM events
                        WHERE aggregate_type = ?1 AND aggregate_id = ?2
                        ORDER BY sequence
                        LIMIT ?3
                    )
                    ",
                )
                .bind(&stream.aggregate_type)
                .bind(&stream.aggregate_id)
                .bind(item_limit)
                .fetch_one(&mut *transaction)
                .await?
            }
            Some(after_sequence) => {
                let after_sequence = i64::try_from(after_sequence)?;
                sqlx::query_scalar::<_, i64>(
                    r"
                    SELECT COALESCE(SUM(
                        length(CAST(aggregate_type AS BLOB))
                        + length(CAST(aggregate_id AS BLOB))
                        + 8
                        + length(CAST(event_type AS BLOB))
                        + length(CAST(event_version AS BLOB))
                        + length(CAST(payload AS BLOB))
                        + length(CAST(metadata AS BLOB))
                    ), 0)
                    FROM (
                        SELECT aggregate_type,
                               aggregate_id,
                               sequence,
                               event_type,
                               event_version,
                               payload,
                               metadata
                        FROM events
                        WHERE aggregate_type = ?1
                          AND aggregate_id = ?2
                          AND sequence > ?3
                        ORDER BY sequence
                        LIMIT ?4
                    )
                    ",
                )
                .bind(&stream.aggregate_type)
                .bind(&stream.aggregate_id)
                .bind(after_sequence)
                .bind(item_limit)
                .fetch_one(&mut *transaction)
                .await?
            }
        };
        let observed = usize::try_from(storage_bytes)?;
        if observed > byte_limit.get() {
            return Err(EngineError::EventPageTooLarge {
                observed,
                limit: byte_limit.get(),
            });
        }

        let rows = match after_sequence {
            None => {
                sqlx::query_as::<_, StoredEventRow>(
                    r"
                    SELECT aggregate_type,
                           aggregate_id,
                           sequence,
                           event_type,
                           event_version,
                           payload,
                           metadata
                    FROM events
                    WHERE aggregate_type = ?1 AND aggregate_id = ?2
                    ORDER BY sequence
                    LIMIT ?3
                    ",
                )
                .bind(&stream.aggregate_type)
                .bind(&stream.aggregate_id)
                .bind(item_limit)
                .fetch_all(&mut *transaction)
                .await?
            }
            Some(after_sequence) => {
                let after_sequence = i64::try_from(after_sequence)?;
                sqlx::query_as::<_, StoredEventRow>(
                    r"
                    SELECT aggregate_type,
                           aggregate_id,
                           sequence,
                           event_type,
                           event_version,
                           payload,
                           metadata
                    FROM events
                    WHERE aggregate_type = ?1
                      AND aggregate_id = ?2
                      AND sequence > ?3
                    ORDER BY sequence
                    LIMIT ?4
                    ",
                )
                .bind(&stream.aggregate_type)
                .bind(&stream.aggregate_id)
                .bind(after_sequence)
                .bind(item_limit)
                .fetch_all(&mut *transaction)
                .await?
            }
        };
        transaction.commit().await?;

        rows.into_iter().map(SerializedEvent::try_from).collect()
    }

    /// Reads the current stream version, where zero means no events.
    pub async fn current_version(&self, stream: &StreamIdentity) -> Result<usize, EngineError> {
        let version = sqlx::query_scalar::<_, Option<i64>>(
            r"
            SELECT MAX(sequence)
            FROM events
            WHERE aggregate_type = ?1 AND aggregate_id = ?2
            ",
        )
        .bind(&stream.aggregate_type)
        .bind(&stream.aggregate_id)
        .fetch_one(&self.pool)
        .await?;
        version.map_or(Ok(0), |version| {
            usize::try_from(version).map_err(Into::into)
        })
    }

    /// Loads the current snapshot for one stream when present.
    pub async fn load_snapshot(
        &self,
        stream: &StreamIdentity,
    ) -> Result<Option<SerializedSnapshot>, EngineError> {
        let row = sqlx::query_as!(
            StoredSnapshotRow,
            r#"
            SELECT aggregate_id,
                   last_sequence,
                   snapshot_version,
                   payload AS "payload: String"
            FROM snapshots
            WHERE aggregate_type = ?1 AND aggregate_id = ?2
            "#,
            stream.aggregate_type,
            stream.aggregate_id,
        )
        .fetch_optional(&self.pool)
        .await?;

        row.map(SerializedSnapshot::try_from).transpose()
    }

    /// Replays matching events through a bounded feed without materializing the full result.
    pub(crate) fn stream_events(
        &self,
        aggregate_type: &'static str,
        aggregate_id: Option<String>,
        channel_size: usize,
    ) -> ReplayStream {
        let (mut feed, stream) = ReplayStream::new(channel_size);
        let pool = self.pool.clone();

        tokio::spawn(async move {
            let mut rows = aggregate_id.as_ref().map_or_else(
                || {
                    sqlx::query_as!(
                        StoredEventRow,
                        r#"
                        SELECT aggregate_type,
                               aggregate_id,
                               sequence,
                               event_type,
                               event_version,
                               payload AS "payload: String",
                               metadata AS "metadata: String"
                        FROM events
                        WHERE aggregate_type = ?1
                        ORDER BY sequence
                        "#,
                        aggregate_type,
                    )
                    .fetch(&pool)
                },
                |aggregate_id| {
                    sqlx::query_as!(
                        StoredEventRow,
                        r#"
                        SELECT aggregate_type,
                               aggregate_id,
                               sequence,
                               event_type,
                               event_version,
                               payload AS "payload: String",
                               metadata AS "metadata: String"
                        FROM events
                        WHERE aggregate_type = ?1 AND aggregate_id = ?2
                        ORDER BY sequence
                        "#,
                        aggregate_type,
                        aggregate_id,
                    )
                    .fetch(&pool)
                },
            );

            loop {
                let row = match rows.try_next().await {
                    Ok(Some(row)) => row,
                    Ok(None) => return,
                    Err(error) => {
                        let error = EngineError::Sql(error);
                        let _ = feed.push(Err(PersistenceError::from(error))).await;
                        return;
                    }
                };
                let event = match SerializedEvent::try_from(row) {
                    Ok(event) => event,
                    Err(error) => {
                        let _ = feed.push(Err(PersistenceError::from(error))).await;
                        return;
                    }
                };

                if feed.push(Ok(event)).await.is_err() {
                    return;
                }
            }
        });

        stream
    }

    /// Atomically persists a validated event, snapshot, and durable-job batch.
    pub async fn commit(&self, request: CommitRequest<'_>) -> Result<(), EngineError> {
        let CommitRequest {
            stream,
            events,
            snapshot,
            jobs,
        } = request;
        validate_event_stream(&stream, events)?;
        let mut tx = self.pool.begin().await?;

        sqlite_es::insert_serialized_events_batch(&mut tx, "events", events).await?;

        if let Some(snapshot) = snapshot {
            let last_sequence = events
                .last()
                .map(|event| event.sequence)
                .ok_or(EngineError::EmptySnapshotUpdate)?;
            let last_sequence = i64::try_from(last_sequence)?;
            let snapshot_version = i64::try_from(snapshot.snapshot_version)?;
            let aggregate = serde_json::to_string(&snapshot.aggregate)?;

            sqlx::query!(
                r#"
                INSERT OR REPLACE INTO snapshots (
                    aggregate_type,
                    aggregate_id,
                    last_sequence,
                    snapshot_version,
                    payload,
                    timestamp
                )
                VALUES (
                    ?1,
                    ?2,
                    ?3,
                    ?4,
                    ?5,
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                )
                "#,
                stream.aggregate_type,
                stream.aggregate_id,
                last_sequence,
                snapshot_version,
                aggregate,
            )
            .execute(&mut *tx)
            .await?;
        }

        for request in jobs {
            let event = crate::job::enqueued_event(&request)?;
            sqlite_es::insert_serialized_events_batch(
                &mut tx,
                "events",
                std::slice::from_ref(&event),
            )
            .await?;

            let payload = crate::job::pending_seed_payload(&request)?;
            let job_id = request.job_id.to_string();
            sqlx::query!(
                r#"
                INSERT INTO job_queue (view_id, version, payload, lease_until)
                VALUES (?1, 1, ?2, NULL)
                "#,
                job_id,
                payload,
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}

enum TransactionOutcome<Output> {
    Commit(Output),
    Rollback(Output),
}

async fn immediate_transaction<Output, Operation>(
    pool: &SqlitePool,
    job_id: &str,
    operation: Operation,
) -> Result<Output, SqliteJobError>
where
    Operation:
        AsyncFnOnce(&mut SqliteConnection) -> Result<TransactionOutcome<Output>, SqliteJobError>,
{
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await?;
    let outcome = operation(&mut transaction).await;

    match outcome {
        Ok(TransactionOutcome::Commit(output)) => {
            transaction.commit().await?;
            Ok(output)
        }
        Ok(TransactionOutcome::Rollback(output)) => {
            transaction.rollback().await?;
            Ok(output)
        }
        Err(claim_error) => {
            if let Err(error) = transaction.rollback().await {
                tracing::warn!(target: "cqrs", ?error, job_id, "job claim rollback failed");
            }
            Err(claim_error)
        }
    }
}

async fn claim_job_in_transaction<Decide, Won>(
    connection: &mut SqliteConnection,
    job_id: &str,
    decide: Decide,
) -> Result<ClaimOutcome<Won>, SqliteJobError>
where
    Decide: FnOnce(Option<ClaimRead>) -> ClaimDecision<Won>,
{
    let read = read_job_projection_for_claim(connection, job_id).await?;
    match decide(read) {
        ClaimDecision::Skip => Ok(ClaimOutcome::Skip),
        ClaimDecision::Claim {
            event,
            payload,
            lease_until_ms,
            won,
        } => match append_job_event(connection, &event).await? {
            Some(version) => {
                write_job_projection(connection, job_id, version, &payload, Some(lease_until_ms))
                    .await?;
                Ok(ClaimOutcome::Won(won))
            }
            None => Ok(ClaimOutcome::Contended),
        },
        ClaimDecision::Abandon { event, payload } => {
            match append_job_event(connection, &event).await? {
                Some(version) => {
                    write_job_projection(connection, job_id, version, &payload, None).await?;
                    Ok(ClaimOutcome::Abandoned)
                }
                None => Ok(ClaimOutcome::Contended),
            }
        }
    }
}

async fn append_job_event(
    connection: &mut SqliteConnection,
    event: &SerializedEvent,
) -> Result<Option<i64>, SqliteJobError> {
    match sqlite_es::insert_serialized_events_batch(
        connection,
        "events",
        std::slice::from_ref(event),
    )
    .await
    {
        Ok(()) => Ok(Some(i64::try_from(event.sequence)?)),
        Err(sqlite_es::SqliteAggregateError::OptimisticLock) => Ok(None),
        Err(other) => Err(SqliteJobError::Append(other)),
    }
}

async fn read_job_projection_for_claim(
    connection: &mut SqliteConnection,
    job_id: &str,
) -> Result<Option<ClaimRead>, SqliteJobError> {
    Ok(sqlx::query_as!(
        ClaimRead,
        r#"
        SELECT version,
               payload,
               lease_until AS "lease_until_ms"
        FROM job_queue
        WHERE view_id = ?1
        "#,
        job_id,
    )
    .fetch_optional(connection)
    .await?)
}

async fn write_job_projection(
    connection: &mut SqliteConnection,
    job_id: &str,
    version: i64,
    payload: &str,
    lease_until_ms: Option<i64>,
) -> Result<(), SqliteJobError> {
    sqlx::query!(
        r#"
        INSERT INTO job_queue (
            view_id,
            version,
            payload,
            lease_until
        )
        VALUES (?1, ?2, ?3, ?4)
        ON CONFLICT(view_id) DO UPDATE SET
            version = excluded.version,
            payload = excluded.payload,
            lease_until = excluded.lease_until
        "#,
        job_id,
        version,
        payload,
        lease_until_ms,
    )
    .execute(connection)
    .await?;
    Ok(())
}

impl From<EngineError> for PersistenceError {
    fn from(error: EngineError) -> Self {
        use EngineError::*;
        match error {
            OptimisticLock => Self::OptimisticLockError,
            EmptySnapshotUpdate | StreamIdentityMismatch { .. } | EventPageTooLarge { .. } => {
                Self::UnknownError(Box::new(error))
            }
            Sql(source) => Self::ConnectionError(Box::new(source)),
            Json(source) => Self::DeserializationError(Box::new(source)),
            Integer(source) => Self::UnknownError(Box::new(source)),
            JobFlush(source) => Self::UnknownError(Box::new(source)),
        }
    }
}

impl From<sqlite_es::SqliteAggregateError> for EngineError {
    fn from(error: sqlite_es::SqliteAggregateError) -> Self {
        match error {
            sqlite_es::SqliteAggregateError::OptimisticLock => Self::OptimisticLock,
            sqlite_es::SqliteAggregateError::Connection(source) => Self::Sql(source),
            sqlite_es::SqliteAggregateError::Deserialization(source) => Self::Json(source),
            sqlite_es::SqliteAggregateError::TryFromInt(source) => Self::Integer(source),
            sqlite_es::SqliteAggregateError::EmptySnapshotUpdate => Self::EmptySnapshotUpdate,
        }
    }
}

fn validate_event_stream(
    stream: &StreamIdentity,
    events: &[SerializedEvent],
) -> Result<(), EngineError> {
    events
        .iter()
        .find(|event| !stream.matches(event))
        .map_or_else(
            || Ok(()),
            |event| {
                Err(EngineError::StreamIdentityMismatch {
                    expected: stream.clone(),
                    actual: StreamIdentity::from_event(event),
                })
            },
        )
}

#[derive(sqlx::FromRow)]
struct StoredEventRow {
    aggregate_type: String,
    aggregate_id: String,
    sequence: i64,
    event_type: String,
    event_version: String,
    payload: String,
    metadata: String,
}

impl TryFrom<StoredEventRow> for SerializedEvent {
    type Error = EngineError;

    fn try_from(row: StoredEventRow) -> Result<Self, Self::Error> {
        Ok(Self {
            aggregate_type: row.aggregate_type,
            aggregate_id: row.aggregate_id,
            sequence: usize::try_from(row.sequence)?,
            event_type: row.event_type,
            event_version: row.event_version,
            payload: serde_json::from_str(&row.payload)?,
            metadata: serde_json::from_str(&row.metadata)?,
        })
    }
}

struct StoredSnapshotRow {
    aggregate_id: String,
    last_sequence: i64,
    snapshot_version: i64,
    payload: String,
}

impl TryFrom<StoredSnapshotRow> for SerializedSnapshot {
    type Error = EngineError;

    fn try_from(row: StoredSnapshotRow) -> Result<Self, Self::Error> {
        Ok(Self {
            aggregate_id: row.aggregate_id,
            aggregate: serde_json::from_str(&row.payload)?,
            current_sequence: usize::try_from(row.last_sequence)?,
            current_snapshot: usize::try_from(row.snapshot_version)?,
        })
    }
}

#[cfg(test)]
mod tests {
    //! Integration coverage for the serialized engine facade.

    use std::future::pending;
    use std::sync::Arc;
    use std::time::Duration;

    use sqlite_es::testing::create_test_pool;
    use sqlx::sqlite::SqlitePoolOptions;
    use tokio::sync::Notify;

    use super::*;
    use crate::job::{JobId, JobKind, WorkerId, enqueued_event, pending_seed_payload, plan_claim};

    #[tokio::test]
    async fn migrate_initializes_the_existing_sqlite_schema() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        let engine = Engine::new(pool);

        engine.migrate().await.unwrap();

        let stream = StreamIdentity::new("engine-migration-test", "one");
        let event = SerializedEvent {
            aggregate_type: "engine-migration-test".to_string(),
            aggregate_id: "one".to_string(),
            sequence: 1,
            event_type: "Created".to_string(),
            event_version: "1.0".to_string(),
            payload: serde_json::json!({}),
            metadata: serde_json::json!({}),
        };
        engine
            .commit(CommitRequest::new(stream, std::slice::from_ref(&event)))
            .await
            .unwrap();
    }

    fn serialized_event(stream: &StreamIdentity, sequence: usize) -> SerializedEvent {
        SerializedEvent {
            aggregate_type: stream.aggregate_type.clone(),
            aggregate_id: stream.aggregate_id.clone(),
            sequence,
            event_type: "Created".to_string(),
            event_version: "1.0".to_string(),
            payload: serde_json::json!({ "sequence": sequence }),
            metadata: serde_json::json!({}),
        }
    }

    #[tokio::test]
    async fn bounded_stream_reads_stop_at_the_requested_page_size() {
        let engine = Engine::new(create_test_pool().await.unwrap());
        let stream = StreamIdentity::new("engine-page-test", "one");
        let events = (1..=3)
            .map(|sequence| serialized_event(&stream, sequence))
            .collect::<Vec<_>>();
        engine
            .commit(CommitRequest::new(stream.clone(), &events))
            .await
            .unwrap();

        let page = engine
            .load_events_page(&stream, None, NonZeroUsize::new(2).unwrap())
            .await
            .unwrap();

        assert_eq!(
            page.iter().map(|event| event.sequence).collect::<Vec<_>>(),
            vec![1, 2]
        );
        let page = engine
            .load_events_page(&stream, Some(1), NonZeroUsize::MIN)
            .await
            .unwrap();
        assert_eq!(
            page.iter().map(|event| event.sequence).collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(engine.current_version(&stream).await.unwrap(), 3);
        assert_eq!(
            engine
                .current_version(&StreamIdentity::new("engine-page-test", "missing"))
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn bounded_stream_reads_reject_storage_bytes_before_loading_the_page() {
        let engine = Engine::new(create_test_pool().await.unwrap());
        let stream = StreamIdentity::new("engine-byte-page-test", "one");
        let event = SerializedEvent {
            payload: serde_json::Value::String("x".repeat(256)),
            ..serialized_event(&stream, 1)
        };
        engine
            .commit(CommitRequest::new(
                stream.clone(),
                std::slice::from_ref(&event),
            ))
            .await
            .unwrap();

        let result = engine
            .load_events_page_bounded(
                &stream,
                None,
                NonZeroUsize::new(2).unwrap(),
                NonZeroUsize::new(64).unwrap(),
            )
            .await;

        assert!(matches!(
            result,
            Err(EngineError::EventPageTooLarge {
                observed,
                limit: 64,
            }) if observed > 64
        ));
    }

    #[tokio::test]
    async fn commit_and_load_use_the_existing_serialized_event_contract() {
        let engine = Engine::new(create_test_pool().await.unwrap());
        let stream = StreamIdentity::new("engine-test", "one");
        let event = serialized_event(&stream, 1);

        engine
            .commit(CommitRequest::new(
                stream.clone(),
                std::slice::from_ref(&event),
            ))
            .await
            .unwrap();

        let loaded = engine.load_events(&stream, None).await.unwrap();
        assert_eq!(loaded, vec![event]);
    }
    #[tokio::test]
    async fn load_events_returns_only_sequences_after_the_checkpoint() {
        let engine = Engine::new(create_test_pool().await.unwrap());
        let stream = StreamIdentity::new("engine-test", "checkpointed");
        let events = (1..=3)
            .map(|sequence| serialized_event(&stream, sequence))
            .collect::<Vec<_>>();

        engine
            .commit(CommitRequest::new(stream.clone(), &events))
            .await
            .unwrap();

        let loaded = engine.load_events(&stream, Some(1)).await.unwrap();
        assert_eq!(loaded, events[1..]);
    }

    #[tokio::test]
    async fn commit_rejects_events_from_multiple_streams_without_writing() {
        let pool = create_test_pool().await.unwrap();
        let engine = Engine::new(pool.clone());
        let requested_stream = StreamIdentity::new("engine-test", "requested");
        let other_stream = StreamIdentity::new("engine-test", "other");
        let events = [
            serialized_event(&requested_stream, 1),
            serialized_event(&other_stream, 1),
        ];

        let result = engine
            .commit(CommitRequest::new(requested_stream, &events))
            .await;

        assert!(matches!(
            result,
            Err(EngineError::StreamIdentityMismatch { .. })
        ));
        let event_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(event_count, 0);
    }

    #[test]
    fn snapshot_attachment_requires_persisted_events() {
        let stream = StreamIdentity::new("engine-test", "empty-snapshot");
        let request = CommitRequest::new(stream, &[])
            .with_snapshot(SnapshotUpdate::new(serde_json::json!({ "sequence": 0 }), 1));

        assert!(matches!(request, Err(EngineError::EmptySnapshotUpdate)));
    }

    #[tokio::test]
    async fn commit_rejects_snapshot_identity_mismatch_without_writing() {
        let pool = create_test_pool().await.unwrap();
        let engine = Engine::new(pool.clone());
        let snapshot_stream = StreamIdentity::new("engine-test", "snapshot");
        let event_stream = StreamIdentity::new("engine-test", "event");
        let event = serialized_event(&event_stream, 1);
        let request = CommitRequest::new(snapshot_stream, std::slice::from_ref(&event))
            .with_snapshot(SnapshotUpdate {
                aggregate: serde_json::json!({ "sequence": 1 }),
                snapshot_version: 1,
            })
            .unwrap();

        let result = engine.commit(request).await;

        assert!(matches!(
            result,
            Err(EngineError::StreamIdentityMismatch { .. })
        ));
        let event_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        let snapshot_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM snapshots")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!((event_count, snapshot_count), (0, 0));
    }

    #[tokio::test]
    async fn job_operations_preserve_the_existing_claim_protocol() {
        let engine = Engine::new(create_test_pool().await.unwrap());
        let request = EnqueueRequest {
            job_id: JobId::new(),
            kind: JobKind::new("engine-test"),
            payload: serde_json::json!({ "value": 42 }),
            run_at_ms: 1_000,
        };
        let job_id = request.job_id.to_string();
        let event = enqueued_event(&request).unwrap();
        let payload = pending_seed_payload(&request).unwrap();

        engine.enqueue_job(event, payload).await.unwrap();
        let worker = WorkerId::new("engine-test-worker");
        let outcome = engine
            .claim_job(&job_id, |read| {
                plan_claim(&job_id, read, &worker, 1_000, 30_000, 50)
            })
            .await
            .unwrap();
        let ClaimOutcome::Won(claim) = outcome else {
            panic!("expected the existing claim decision to win");
        };

        assert_eq!(claim.claim_seq, 2);
        let job_stream = StreamIdentity::new("job", &job_id);
        let events = engine.load_events(&job_stream, None).await.unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.event_type.as_str())
                .collect::<Vec<_>>(),
            ["JobEnqueued", "JobClaimed"]
        );
        assert!(matches!(
            engine.renew_job(&job_id, claim.claim_seq, 60_000).await,
            Ok(LeaseRenewal::Held)
        ));
        assert!(matches!(
            engine.renew_job(&job_id, claim.claim_seq + 1, 90_000).await,
            Ok(LeaseRenewal::Lost)
        ));
        assert_eq!(engine.load_events(&job_stream, None).await.unwrap(), events);
    }

    #[tokio::test]
    async fn cancelled_immediate_transaction_does_not_poison_the_pooled_connection() {
        let pool = create_test_pool().await.unwrap();
        let operation_started = Arc::new(Notify::new());
        let task_pool = pool.clone();
        let task_started = Arc::clone(&operation_started);

        let claim = tokio::spawn(async move {
            immediate_transaction(&task_pool, "cancelled-job", async move |_connection| {
                task_started.notify_one();
                pending::<Result<TransactionOutcome<()>, SqliteJobError>>().await
            })
            .await
        });

        tokio::time::timeout(Duration::from_secs(1), operation_started.notified())
            .await
            .expect("claim entered its transaction");
        claim.abort();
        assert!(claim.await.expect_err("claim was cancelled").is_cancelled());

        let mut connection = pool.acquire().await.expect("reacquire connection");
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await
            .expect("cancelled transaction was rolled back before pool reuse");
        sqlx::query("ROLLBACK")
            .execute(&mut *connection)
            .await
            .expect("close verification transaction");
    }

    #[tokio::test]
    async fn rollback_failure_is_returned_to_the_caller() {
        let pool = create_test_pool().await.unwrap();

        let result = immediate_transaction(&pool, "rollback-failure", async |connection| {
            sqlx::query("ROLLBACK").execute(connection).await?;

            Ok(TransactionOutcome::Rollback(()))
        })
        .await;

        assert!(matches!(result, Err(SqliteJobError::Sql(_))));
    }
}
