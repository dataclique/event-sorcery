//! Serialized SQLite storage facade shared by native and foreign-language callers.
//!
//! The engine owns storage-boundary reads and replay plus the transactions that
//! atomically commit aggregate events, snapshots, and durable job intent.

use std::num::NonZeroUsize;

use cqrs_es::persist::{PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot};
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{SqliteConnection, SqlitePool};

use crate::job::EnqueueRequest;
use crate::job_sqlite::SqliteJobError;
use crate::job_store::{ClaimDecision, ClaimOutcome, ClaimRead, LeaseRenewal};

const ENGINE_PAYLOAD_KEY: &str = "$event-sorcery-engine";
const ENGINE_PAYLOAD_VERSION: u8 = 1;

/// Derives the two bounded-page queries from one column list and one window.
///
/// `load_events_page_bounded` only bounds what it returns if the bytes it
/// measures describe exactly the rows it fetches, so neither the columns nor
/// the window may be restated per query.
macro_rules! event_page_queries {
    (window: $window:literal, blob_columns: [$($column:literal),+ $(,)?]) => {
        /// Rows of one bounded page window, ordered by sequence.
        const EVENT_PAGE_ROWS_SQL: &str =
            concat!("SELECT sequence", $(", ", $column,)+ $window);

        /// Storage bytes of every row in that same window, ordered by sequence.
        ///
        /// The leading `8` accounts for the stored sequence, which SQLite keeps
        /// as an integer rather than a measurable blob.
        const EVENT_PAGE_STORAGE_BYTES_SQL: &str =
            concat!("SELECT 8", $(" + length(CAST(", $column, " AS BLOB))",)+ $window);
    };
}

event_page_queries!(
    window: r"
    FROM events
    WHERE aggregate_type = ?1
      AND aggregate_id = ?2
      AND (?3 IS NULL OR sequence > ?3)
    ORDER BY sequence
    LIMIT ?4
    ",
    blob_columns: [
        "aggregate_type",
        "aggregate_id",
        "event_type",
        "event_version",
        "payload",
        "metadata",
    ]
);

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

/// One event loaded through the erased engine boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedEvent {
    /// One-based sequence within the aggregate stream.
    pub sequence: usize,
    /// Stable domain event type.
    pub event_type: String,
    /// Stable domain event schema version.
    pub event_version: String,
    /// Payload representation recorded by the engine.
    pub payload: LoadedPayload,
}

/// Payload representation recovered from engine-owned persistence metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadedPayload {
    /// A native JSON domain payload.
    Json(Value),
    /// Opaque bytes supplied through a foreign-language binding.
    OpaqueBytes(Vec<u8>),
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
    payload_encoding: CommitPayloadEncoding,
}

#[derive(Clone, Copy)]
enum CommitPayloadEncoding {
    NativeJson,
    OpaqueBytes,
}

#[derive(Deserialize, Serialize)]
struct EnginePayloadEnvelope {
    version: u8,
    payload: EnginePayload,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum EnginePayload {
    OpaqueBytes(Vec<u8>),
    EscapedJson(Value),
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
    #[error(
        "optimistic lock error: stream is at version {actual_version}, \
         not the expected {expected_version}"
    )]
    OptimisticLock {
        expected_version: usize,
        actual_version: usize,
    },
    #[error("commit event sequence {offending} does not follow stream version {expected_version}")]
    NonContiguousSequences {
        expected_version: usize,
        offending: usize,
    },
    #[error("stored engine payload envelope version {version} is not supported")]
    UnsupportedPayloadVersion { version: u8 },
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

/// An erased durable-job intent committed atomically with domain events.
pub struct JobSeed(EnqueueRequest);

impl JobSeed {
    /// Builds a language-neutral seed for the existing event-sourced job aggregate.
    #[must_use]
    pub fn new(
        job_id: crate::JobId,
        kind: impl Into<String>,
        payload: Value,
        run_at_ms: i64,
    ) -> Self {
        Self(EnqueueRequest {
            job_id,
            kind: crate::job::JobKind::new(kind),
            payload,
            run_at_ms,
        })
    }
}

impl<'events> CommitRequest<'events> {
    /// Starts a commit for one stream and its proposed events.
    pub fn new(stream: StreamIdentity, events: &'events [SerializedEvent]) -> Self {
        Self {
            stream,
            events,
            snapshot: None,
            jobs: vec![],
            payload_encoding: CommitPayloadEncoding::NativeJson,
        }
    }

    /// Marks every event payload as opaque bytes owned by a language binding.
    ///
    /// Each event payload must be a JSON byte array. The engine records its
    /// provenance in a reserved envelope while preserving the shared writer
    /// transaction.
    #[must_use]
    pub fn with_opaque_payloads(mut self) -> Self {
        self.payload_encoding = CommitPayloadEncoding::OpaqueBytes;
        self
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

    /// Includes one durable job intent in the same event-store transaction.
    #[must_use]
    pub fn with_job(mut self, job: JobSeed) -> Self {
        let JobSeed(request) = job;
        self.jobs.push(request);
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

    /// Loads an event page bounded by item count and stored payload bytes.
    ///
    /// The byte budget shrinks the page instead of rejecting it: the returned
    /// prefix is the longest run of events whose stored bytes fit `byte_limit`,
    /// so a caller advances through an oversized stream by paging on the last
    /// returned sequence. A single event larger than the whole budget cannot be
    /// paged around and is reported as [`EngineError::EventPageTooLarge`].
    pub async fn load_events_page_bounded(
        &self,
        stream: &StreamIdentity,
        after_sequence: Option<usize>,
        item_limit: NonZeroUsize,
        byte_limit: NonZeroUsize,
    ) -> Result<Vec<LoadedEvent>, EngineError> {
        let item_limit = i64::try_from(item_limit.get())?;
        let after_sequence = after_sequence.map(i64::try_from).transpose()?;
        let mut transaction = self.pool.begin().await?;
        let storage_bytes = sqlx::query_scalar::<_, i64>(EVENT_PAGE_STORAGE_BYTES_SQL)
            .bind(&stream.aggregate_type)
            .bind(&stream.aggregate_id)
            .bind(after_sequence)
            .bind(item_limit)
            .fetch_all(&mut *transaction)
            .await?;
        let page_limit = fitting_page_limit(storage_bytes, byte_limit)?;

        let rows = sqlx::query_as::<_, StoredEventRow>(EVENT_PAGE_ROWS_SQL)
            .bind(&stream.aggregate_type)
            .bind(&stream.aggregate_id)
            .bind(after_sequence)
            .bind(page_limit)
            .fetch_all(&mut *transaction)
            .await?;
        transaction.commit().await?;

        rows.into_iter().map(LoadedEvent::try_from).collect()
    }

    /// Reads the current stream version, where zero means the stream holds no
    /// events and no snapshot covers any.
    ///
    /// This is the same oracle [`Engine::commit`] validates against, so a
    /// compacted stream reports the version its snapshot still covers rather
    /// than the lower maximum sequence its surviving events show.
    pub async fn current_version(&self, stream: &StreamIdentity) -> Result<usize, EngineError> {
        let mut connection = self.pool.acquire().await?;
        committed_stream_version(&mut connection, stream).await
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
            payload_encoding,
        } = request;
        validate_event_stream(&stream, events)?;
        let prepared_events = prepare_event_payloads(events, payload_encoding)?;
        let events = prepared_events.as_deref().unwrap_or(events);
        let expected_version = validate_event_sequences(events)?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        // An empty batch has no sequence to conflict with and nothing to append;
        // such a commit carries only durable job intent.
        if let Some(expected_version) = expected_version {
            let actual_version = committed_stream_version(&mut tx, &stream).await?;
            // Only a durable version *ahead* of the batch is a conflict. A
            // compacted stream legally reports a lower maximum event sequence
            // than the aggregate's real version (see ADR-0003), so the snapshot's
            // covered sequence has to bound the check too: without it a stale
            // writer would silently reuse a sequence compaction erased.
            if actual_version > expected_version {
                return Err(EngineError::OptimisticLock {
                    expected_version,
                    actual_version,
                });
            }

            append_events(&mut tx, &stream, expected_version, events).await?;
        }

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
            let job_stream = StreamIdentity::from_event(&event);
            append_events(&mut tx, &job_stream, 0, std::slice::from_ref(&event)).await?;

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

/// Reads one stream's durable version, where zero means nothing was ever
/// committed.
///
/// Compaction deletes every event a snapshot already covers, so
/// `MAX(events.sequence)` alone can fall back below the aggregate's real version
/// and stop reporting stale writers. `snapshots.last_sequence` is the durable
/// record of how far the erased prefix reached, so the greater of the two is the
/// only sound conflict oracle. Reads on the caller's connection, so a commit
/// sees the two records under the write lock it already holds and appends
/// against exactly what it observed.
async fn committed_stream_version(
    connection: &mut SqliteConnection,
    stream: &StreamIdentity,
) -> Result<usize, EngineError> {
    let version = sqlx::query_scalar::<_, i64>(
        r"
        SELECT MAX(
            COALESCE(
                (SELECT MAX(sequence) FROM events
                 WHERE aggregate_type = ?1 AND aggregate_id = ?2),
                0
            ),
            COALESCE(
                (SELECT last_sequence FROM snapshots
                 WHERE aggregate_type = ?1 AND aggregate_id = ?2),
                0
            )
        )
        ",
    )
    .bind(&stream.aggregate_type)
    .bind(&stream.aggregate_id)
    .fetch_one(connection)
    .await?;
    usize::try_from(version).map_err(EngineError::from)
}

/// Appends one validated batch, reporting a sequence collision as the conflict
/// it is.
///
/// The durable version is re-read on the caller's connection, which still holds
/// the commit's write lock, so the reported versions are the ones that produced
/// the collision rather than whatever a later unlocked read would observe.
async fn append_events(
    connection: &mut SqliteConnection,
    stream: &StreamIdentity,
    expected_version: usize,
    events: &[SerializedEvent],
) -> Result<(), EngineError> {
    let inserted = sqlite_es::insert_serialized_events_batch(connection, "events", events).await;

    match inserted {
        Ok(()) => Ok(()),
        Err(sqlite_es::SqliteAggregateError::OptimisticLock) => {
            let actual_version = committed_stream_version(connection, stream).await?;
            Err(EngineError::OptimisticLock {
                expected_version,
                actual_version,
            })
        }
        Err(sqlite_es::SqliteAggregateError::Connection(source)) => Err(EngineError::Sql(source)),
        Err(sqlite_es::SqliteAggregateError::Deserialization(source)) => {
            Err(EngineError::Json(source))
        }
        Err(sqlite_es::SqliteAggregateError::TryFromInt(source)) => {
            Err(EngineError::Integer(source))
        }
        Err(sqlite_es::SqliteAggregateError::EmptySnapshotUpdate) => {
            Err(EngineError::EmptySnapshotUpdate)
        }
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
            OptimisticLock { .. } => Self::OptimisticLockError,
            EmptySnapshotUpdate
            | StreamIdentityMismatch { .. }
            | EventPageTooLarge { .. }
            | NonContiguousSequences { .. }
            | UnsupportedPayloadVersion { .. } => Self::UnknownError(Box::new(error)),
            Sql(source) => Self::ConnectionError(Box::new(source)),
            Json(source) => Self::DeserializationError(Box::new(source)),
            Integer(source) => Self::UnknownError(Box::new(source)),
            JobFlush(source) => Self::UnknownError(Box::new(source)),
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

fn validate_event_sequences(events: &[SerializedEvent]) -> Result<Option<usize>, EngineError> {
    let Some(first) = events.first() else {
        return Ok(None);
    };
    let Some(expected_version) = first.sequence.checked_sub(1) else {
        return Err(EngineError::NonContiguousSequences {
            expected_version: 0,
            offending: first.sequence,
        });
    };
    events.iter().enumerate().try_for_each(|(index, event)| {
        let expected_sequence = expected_version
            .checked_add(index)
            .and_then(|sequence| sequence.checked_add(1));
        match expected_sequence {
            Some(expected_sequence) if event.sequence == expected_sequence => Ok(()),
            Some(_) | None => Err(EngineError::NonContiguousSequences {
                expected_version,
                offending: event.sequence,
            }),
        }
    })?;
    Ok(Some(expected_version))
}

/// How many leading events of a measured page window fit the storage-byte budget.
///
/// Returning a shorter page keeps an oversized stream readable: the caller
/// continues from the last returned sequence. Only a first event that alone
/// exceeds the whole budget leaves no prefix to return.
fn fitting_page_limit(
    storage_bytes: Vec<i64>,
    byte_limit: NonZeroUsize,
) -> Result<i64, EngineError> {
    let storage_bytes = storage_bytes
        .into_iter()
        .map(usize::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let fitting = storage_bytes
        .iter()
        .scan(0_usize, |total, bytes| {
            *total = total.checked_add(*bytes)?;
            Some(*total)
        })
        .take_while(|total| *total <= byte_limit.get())
        .count();

    if let (0, Some(observed)) = (fitting, storage_bytes.first()) {
        return Err(EngineError::EventPageTooLarge {
            observed: *observed,
            limit: byte_limit.get(),
        });
    }

    i64::try_from(fitting).map_err(EngineError::from)
}

fn prepare_event_payloads(
    events: &[SerializedEvent],
    encoding: CommitPayloadEncoding,
) -> Result<Option<Vec<SerializedEvent>>, EngineError> {
    match encoding {
        CommitPayloadEncoding::NativeJson
            if !events
                .iter()
                .any(|event| reserves_engine_payload(&event.payload)) =>
        {
            Ok(None)
        }
        CommitPayloadEncoding::NativeJson => events
            .iter()
            .map(|event| {
                let payload = if reserves_engine_payload(&event.payload) {
                    encode_engine_payload(EnginePayload::EscapedJson(event.payload.clone()))?
                } else {
                    event.payload.clone()
                };
                Ok(clone_event_with_payload(event, payload))
            })
            .collect::<Result<Vec<_>, EngineError>>()
            .map(Some),
        CommitPayloadEncoding::OpaqueBytes => events
            .iter()
            .map(|event| {
                let bytes = serde_json::from_value::<Vec<u8>>(event.payload.clone())?;
                let payload = encode_engine_payload(EnginePayload::OpaqueBytes(bytes))?;
                Ok(clone_event_with_payload(event, payload))
            })
            .collect::<Result<Vec<_>, EngineError>>()
            .map(Some),
    }
}

fn reserves_engine_payload(payload: &Value) -> bool {
    matches!(payload, Value::Object(object) if object.contains_key(ENGINE_PAYLOAD_KEY))
}

fn encode_engine_payload(payload: EnginePayload) -> Result<Value, EngineError> {
    let mut object = serde_json::Map::new();
    object.insert(
        ENGINE_PAYLOAD_KEY.to_string(),
        serde_json::to_value(EnginePayloadEnvelope {
            version: ENGINE_PAYLOAD_VERSION,
            payload,
        })?,
    );
    Ok(Value::Object(object))
}

/// Reads the engine envelope out of a stored payload, if it carries one.
///
/// A stored object whose single key is the engine-reserved one can only have
/// been written by the engine, because [`prepare_event_payloads`] escapes any
/// domain payload that would collide with it. Such an object failing to parse
/// as an envelope, or carrying an unknown envelope version, is therefore
/// corruption or a newer writer -- never domain data -- and must not be handed
/// back to the caller as an ordinary JSON payload.
fn engine_envelope(payload: &Value) -> Result<Option<EnginePayload>, EngineError> {
    let Value::Object(object) = payload else {
        return Ok(None);
    };
    if object.len() != 1 {
        return Ok(None);
    }
    let Some(envelope) = object.get(ENGINE_PAYLOAD_KEY) else {
        return Ok(None);
    };
    let envelope = serde_json::from_value::<EnginePayloadEnvelope>(envelope.clone())?;
    if envelope.version != ENGINE_PAYLOAD_VERSION {
        return Err(EngineError::UnsupportedPayloadVersion {
            version: envelope.version,
        });
    }
    Ok(Some(envelope.payload))
}

/// Recovers a stored payload's representation.
fn decode_engine_payload(payload: Value) -> Result<LoadedPayload, EngineError> {
    Ok(match engine_envelope(&payload)? {
        None => LoadedPayload::Json(payload),
        Some(EnginePayload::OpaqueBytes(bytes)) => LoadedPayload::OpaqueBytes(bytes),
        Some(EnginePayload::EscapedJson(escaped)) => LoadedPayload::Json(escaped),
    })
}

fn clone_event_with_payload(event: &SerializedEvent, payload: Value) -> SerializedEvent {
    SerializedEvent {
        aggregate_type: event.aggregate_type.clone(),
        aggregate_id: event.aggregate_id.clone(),
        sequence: event.sequence,
        event_type: event.event_type.clone(),
        event_version: event.event_version.clone(),
        payload,
        metadata: event.metadata.clone(),
    }
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
        let stored: Value = serde_json::from_str(&row.payload)?;
        // Opaque bytes have no native representation, so they stay in the
        // envelope they were stored in: decoding and re-encoding would only
        // rebuild the value already in hand.
        let payload = match engine_envelope(&stored)? {
            None | Some(EnginePayload::OpaqueBytes(_)) => stored,
            Some(EnginePayload::EscapedJson(escaped)) => escaped,
        };
        Ok(Self {
            aggregate_type: row.aggregate_type,
            aggregate_id: row.aggregate_id,
            sequence: usize::try_from(row.sequence)?,
            event_type: row.event_type,
            event_version: row.event_version,
            payload,
            metadata: serde_json::from_str(&row.metadata)?,
        })
    }
}

impl TryFrom<StoredEventRow> for LoadedEvent {
    type Error = EngineError;

    fn try_from(row: StoredEventRow) -> Result<Self, Self::Error> {
        let StoredEventRow {
            aggregate_type: _,
            aggregate_id: _,
            sequence,
            event_type,
            event_version,
            payload,
            metadata,
        } = row;
        let _: Value = serde_json::from_str(&metadata)?;
        Ok(Self {
            sequence: usize::try_from(sequence)?,
            event_type,
            event_version,
            payload: decode_engine_payload(serde_json::from_str(&payload)?)?,
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
    use crate::job::{
        JobId, JobKind, JobStoreError, WorkerId, enqueued_event, pending_seed_payload, plan_claim,
    };

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

    async fn queue_row_count(engine: &Engine, job_id: &JobId) -> i64 {
        sqlx::query_scalar::<_, i64>(
            r"
            SELECT COUNT(*)
            FROM job_queue
            WHERE view_id = ?1
            ",
        )
        .bind(job_id.to_string())
        .fetch_one(engine.pool())
        .await
        .unwrap()
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
    async fn bounded_stream_reads_reject_a_lone_event_larger_than_the_whole_budget() {
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
    async fn bounded_stream_reads_truncate_at_the_storage_byte_boundary() {
        let engine = Engine::new(create_test_pool().await.unwrap());
        let stream = StreamIdentity::new("engine-truncate-test", "one");
        let events = [
            serialized_event(&stream, 1),
            serialized_event(&stream, 2),
            SerializedEvent {
                payload: serde_json::Value::String("x".repeat(4096)),
                ..serialized_event(&stream, 3)
            },
        ];
        engine
            .commit(CommitRequest::new(stream.clone(), &events))
            .await
            .unwrap();

        // The two small events fit the budget; the fat third one does not, so
        // the page stops short instead of failing the whole read.
        let page = engine
            .load_events_page_bounded(
                &stream,
                None,
                NonZeroUsize::new(10).unwrap(),
                NonZeroUsize::new(1_024).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            page.iter().map(|event| event.sequence).collect::<Vec<_>>(),
            vec![1, 2]
        );

        // Paging on the last returned sequence reaches the fat event under a
        // budget that can hold it, so the stream stays fully readable.
        let page = engine
            .load_events_page_bounded(
                &stream,
                Some(2),
                NonZeroUsize::new(10).unwrap(),
                NonZeroUsize::new(8_192).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            page.iter().map(|event| event.sequence).collect::<Vec<_>>(),
            vec![3]
        );
    }

    #[tokio::test]
    async fn bounded_stream_reads_return_an_empty_page_for_an_unknown_stream() {
        let engine = Engine::new(create_test_pool().await.unwrap());
        let stream = StreamIdentity::new("engine-truncate-test", "missing");

        let page = engine
            .load_events_page_bounded(
                &stream,
                None,
                NonZeroUsize::new(10).unwrap(),
                NonZeroUsize::new(1_024).unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(page, Vec::<LoadedEvent>::new());
    }

    #[test]
    fn stored_payloads_under_an_unknown_envelope_version_are_rejected() {
        let mut object = serde_json::Map::new();
        object.insert(
            ENGINE_PAYLOAD_KEY.to_string(),
            serde_json::json!({
                "version": 2,
                "payload": { "opaque_bytes": [1, 2, 3] },
            }),
        );

        let error = decode_engine_payload(Value::Object(object)).unwrap_err();

        assert!(matches!(
            error,
            EngineError::UnsupportedPayloadVersion { version: 2 }
        ));
    }

    #[test]
    fn stored_payloads_under_a_corrupt_envelope_are_rejected() {
        let mut object = serde_json::Map::new();
        object.insert(
            ENGINE_PAYLOAD_KEY.to_string(),
            serde_json::json!({ "version": 1 }),
        );

        let error = decode_engine_payload(Value::Object(object)).unwrap_err();

        assert!(matches!(error, EngineError::Json(_)));
    }

    #[test]
    fn stored_payloads_without_the_reserved_key_stay_domain_json() {
        let payload = serde_json::json!({ "sequence": 1 });

        let decoded = decode_engine_payload(payload.clone()).unwrap();

        assert_eq!(decoded, LoadedPayload::Json(payload));
    }

    fn payload_reserving_the_engine_key(extra: Option<(&str, Value)>) -> Value {
        let mut object = serde_json::Map::new();
        object.insert(
            ENGINE_PAYLOAD_KEY.to_string(),
            serde_json::json!({ "domain": true }),
        );
        if let Some((key, value)) = extra {
            object.insert(key.to_string(), value);
        }
        Value::Object(object)
    }

    #[tokio::test]
    async fn committed_payloads_that_collide_with_the_reserved_key_load_back_unchanged() {
        let engine = Engine::new(create_test_pool().await.unwrap());
        let stream = StreamIdentity::new("engine-reserved-key-test", "one");
        // Both the lone reserved key and the reserved key beside a sibling must
        // be escaped on write; otherwise the reader would reject them as a
        // corrupt engine envelope.
        let lone = payload_reserving_the_engine_key(None);
        let beside_sibling =
            payload_reserving_the_engine_key(Some(("sequence", serde_json::json!(2))));
        let events = [
            SerializedEvent {
                payload: lone.clone(),
                ..serialized_event(&stream, 1)
            },
            SerializedEvent {
                payload: beside_sibling.clone(),
                ..serialized_event(&stream, 2)
            },
        ];

        engine
            .commit(CommitRequest::new(stream.clone(), &events))
            .await
            .unwrap();

        assert_eq!(engine.load_events(&stream, None).await.unwrap(), events);
        let page = engine
            .load_events_page_bounded(
                &stream,
                None,
                NonZeroUsize::new(10).unwrap(),
                NonZeroUsize::new(8_192).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            page.into_iter()
                .map(|event| event.payload)
                .collect::<Vec<_>>(),
            vec![
                LoadedPayload::Json(lone),
                LoadedPayload::Json(beside_sibling)
            ]
        );
    }

    #[tokio::test]
    async fn opaque_payload_commits_load_back_as_the_bytes_they_stored() {
        let engine = Engine::new(create_test_pool().await.unwrap());
        let stream = StreamIdentity::new("engine-opaque-test", "one");
        // Not valid UTF-8, so only the byte-preserving envelope can carry it.
        let bytes = vec![0_u8, 1, 0xff, 0xfe, 0x80];
        let event = SerializedEvent {
            payload: serde_json::json!(bytes.clone()),
            ..serialized_event(&stream, 1)
        };

        engine
            .commit(
                CommitRequest::new(stream.clone(), std::slice::from_ref(&event))
                    .with_opaque_payloads(),
            )
            .await
            .unwrap();

        let page = engine
            .load_events_page_bounded(
                &stream,
                None,
                NonZeroUsize::new(10).unwrap(),
                NonZeroUsize::new(8_192).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            page.into_iter()
                .map(|event| event.payload)
                .collect::<Vec<_>>(),
            vec![LoadedPayload::OpaqueBytes(bytes.clone())]
        );

        // The native contract has no opaque representation, so a native read
        // hands back the stored envelope, which still decodes to the same bytes.
        let stored_events = engine.load_events(&stream, None).await.unwrap();
        let [stored] = stored_events.as_slice() else {
            panic!("expected exactly one stored event");
        };
        assert_eq!(
            decode_engine_payload(stored.payload.clone()).unwrap(),
            LoadedPayload::OpaqueBytes(bytes)
        );
    }

    #[tokio::test]
    async fn commit_accepts_a_stream_whose_compacted_prefix_lowered_its_max_sequence() {
        let pool = create_test_pool().await.unwrap();
        let engine = Engine::new(pool.clone());
        let stream = StreamIdentity::new("engine-compaction-test", "one");
        let persisted = (1..=2)
            .map(|sequence| serialized_event(&stream, sequence))
            .collect::<Vec<_>>();
        engine
            .commit(CommitRequest::new(stream.clone(), &persisted))
            .await
            .unwrap();

        // Compaction of a snapshot-backed aggregate deletes the covered prefix,
        // so the events table no longer holds the stream's real version.
        sqlx::query("DELETE FROM events WHERE aggregate_type = ?1 AND aggregate_id = ?2")
            .bind("engine-compaction-test")
            .bind("one")
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(engine.current_version(&stream).await.unwrap(), 0);

        let next = [serialized_event(&stream, 3)];
        engine
            .commit(CommitRequest::new(stream.clone(), &next))
            .await
            .unwrap();

        assert_eq!(engine.current_version(&stream).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn current_version_reports_the_sequence_a_snapshot_covers_after_compaction() {
        let pool = create_test_pool().await.unwrap();
        let engine = Engine::new(pool.clone());
        let stream = StreamIdentity::new("engine-compacted-version-test", "one");
        let persisted = (1..=2)
            .map(|sequence| serialized_event(&stream, sequence))
            .collect::<Vec<_>>();
        engine
            .commit(
                CommitRequest::new(stream.clone(), &persisted)
                    .with_snapshot(SnapshotUpdate::new(serde_json::json!({ "sequence": 2 }), 1))
                    .unwrap(),
            )
            .await
            .unwrap();

        sqlx::query(
            "DELETE FROM events \
             WHERE aggregate_type = ?1 \
               AND aggregate_id = ?2 \
               AND sequence <= (SELECT last_sequence FROM snapshots \
                                WHERE aggregate_type = ?1 AND aggregate_id = ?2)",
        )
        .bind("engine-compacted-version-test")
        .bind("one")
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(engine.load_events(&stream, None).await.unwrap(), vec![]);

        // The events table forgot every sequence, so only the snapshot still
        // records how far the stream reached. A reader that trusted the events
        // alone would restart a live aggregate from version zero.
        assert_eq!(engine.current_version(&stream).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn commit_rejects_a_stale_writer_whose_sequence_a_snapshot_already_covers() {
        let pool = create_test_pool().await.unwrap();
        let engine = Engine::new(pool.clone());
        let stream = StreamIdentity::new("engine-compaction-conflict-test", "one");
        let persisted = [serialized_event(&stream, 1)];
        engine
            .commit(
                CommitRequest::new(stream.clone(), &persisted)
                    .with_snapshot(SnapshotUpdate::new(serde_json::json!({ "sequence": 1 }), 1))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Compaction deletes exactly the prefix the snapshot covers, so the
        // events table no longer remembers that sequence 1 was ever written.
        sqlx::query(
            "DELETE FROM events \
             WHERE aggregate_type = ?1 \
               AND aggregate_id = ?2 \
               AND sequence <= (SELECT last_sequence FROM snapshots \
                                WHERE aggregate_type = ?1 AND aggregate_id = ?2)",
        )
        .bind("engine-compaction-conflict-test")
        .bind("one")
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(engine.current_version(&stream).await.unwrap(), 1);

        // A writer that loaded before the first commit proposes sequence 1
        // again. Replay would never reach it behind the snapshot, so accepting
        // it would lose the update silently.
        let stale = [serialized_event(&stream, 1)];
        let result = engine
            .commit(CommitRequest::new(stream.clone(), &stale))
            .await;

        assert!(matches!(
            result,
            Err(EngineError::OptimisticLock {
                expected_version: 0,
                actual_version: 1,
            })
        ));
        assert_eq!(engine.current_version(&stream).await.unwrap(), 1);

        // A writer that loaded the snapshot proposes the sequence after it and
        // still commits.
        let next = [serialized_event(&stream, 2)];
        engine
            .commit(CommitRequest::new(stream.clone(), &next))
            .await
            .unwrap();

        assert_eq!(engine.current_version(&stream).await.unwrap(), 2);
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
    async fn commit_rejects_noncontiguous_sequences_without_writing() {
        let engine = Engine::new(create_test_pool().await.unwrap());
        let stream = StreamIdentity::new("engine-test", "sequence-gap");
        let events = [serialized_event(&stream, 1), serialized_event(&stream, 3)];

        let result = engine
            .commit(CommitRequest::new(stream.clone(), &events))
            .await;

        assert!(matches!(
            result,
            Err(EngineError::NonContiguousSequences {
                expected_version: 0,
                offending: 3,
            })
        ));
        assert_eq!(engine.current_version(&stream).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn commit_rejects_a_batch_starting_below_the_first_sequence() {
        let engine = Engine::new(create_test_pool().await.unwrap());
        let stream = StreamIdentity::new("engine-test", "zero-sequence");
        let events = [serialized_event(&stream, 0)];

        let result = engine
            .commit(CommitRequest::new(stream.clone(), &events))
            .await;

        assert!(matches!(
            result,
            Err(EngineError::NonContiguousSequences {
                expected_version: 0,
                offending: 0,
            })
        ));
        assert_eq!(engine.current_version(&stream).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn commit_rejects_a_writer_whose_expected_version_trails_the_stream() {
        let engine = Engine::new(create_test_pool().await.unwrap());
        let stream = StreamIdentity::new("engine-test", "stale-writer");
        let persisted = (1..=3)
            .map(|sequence| serialized_event(&stream, sequence))
            .collect::<Vec<_>>();
        engine
            .commit(CommitRequest::new(stream.clone(), &persisted))
            .await
            .unwrap();

        // A writer that loaded the stream at version 1 proposes sequence 2,
        // which two committed events already passed.
        let stale = [serialized_event(&stream, 2)];
        let result = engine
            .commit(CommitRequest::new(stream.clone(), &stale))
            .await;

        assert!(matches!(
            result,
            Err(EngineError::OptimisticLock {
                expected_version: 1,
                actual_version: 3,
            })
        ));
        assert_eq!(engine.current_version(&stream).await.unwrap(), 3);
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

    #[tokio::test]
    async fn language_neutral_job_seed_commits_with_domain_events() {
        let pool = create_test_pool().await.unwrap();
        let engine = Engine::new(pool);
        let stream = StreamIdentity::new("engine-test", "one");
        let event = SerializedEvent {
            aggregate_type: "engine-test".to_string(),
            aggregate_id: "one".to_string(),
            sequence: 1,
            event_type: "Created".to_string(),
            event_version: "1.0".to_string(),
            payload: serde_json::json!({}),
            metadata: serde_json::json!({}),
        };
        let job_id = JobId::new();
        let request = CommitRequest::new(stream.clone(), std::slice::from_ref(&event)).with_job(
            JobSeed::new(
                job_id,
                "haskell-test",
                serde_json::json!([0, 1, 255]),
                1_000,
            ),
        );

        engine.commit(request).await.unwrap();

        assert_eq!(engine.load_events(&stream, None).await.unwrap(), [event]);
        assert_eq!(queue_row_count(&engine, &job_id).await, 1);
        let job_stream = StreamIdentity::new("job", job_id.to_string());
        assert_eq!(
            engine
                .load_events(&job_stream, None)
                .await
                .unwrap()
                .into_iter()
                .map(|event| event.event_type)
                .collect::<Vec<_>>(),
            ["JobEnqueued"]
        );
    }

    #[tokio::test]
    async fn invalid_job_seed_instant_rolls_back_the_domain_event() {
        let pool = create_test_pool().await.unwrap();
        let engine = Engine::new(pool);
        let stream = StreamIdentity::new("engine-test", "invalid-job-instant");
        let event = SerializedEvent {
            aggregate_type: "engine-test".to_string(),
            aggregate_id: "invalid-job-instant".to_string(),
            sequence: 1,
            event_type: "Created".to_string(),
            event_version: "1.0".to_string(),
            payload: serde_json::json!({}),
            metadata: serde_json::json!({}),
        };
        let job_id = JobId::new();
        let request = CommitRequest::new(stream.clone(), std::slice::from_ref(&event)).with_job(
            JobSeed::new(job_id, "haskell-test", serde_json::json!([]), i64::MAX),
        );

        let error = engine.commit(request).await.unwrap_err();

        assert!(matches!(
            error,
            EngineError::JobFlush(JobStoreError::InvalidInstant(i64::MAX))
        ));
        assert!(engine.load_events(&stream, None).await.unwrap().is_empty());
        assert_eq!(queue_row_count(&engine, &job_id).await, 0);
        let job_stream = StreamIdentity::new("job", job_id.to_string());
        assert!(
            engine
                .load_events(&job_stream, None)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
