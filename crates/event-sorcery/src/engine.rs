use cqrs_es::persist::{PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot};
use serde_json::Value;
use sqlx::SqlitePool;

use crate::job::EnqueueRequest;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamIdentity {
    pub(crate) aggregate_type: String,
    pub(crate) aggregate_id: String,
}

impl StreamIdentity {
    pub(crate) fn new(aggregate_type: impl Into<String>, aggregate_id: impl Into<String>) -> Self {
        Self {
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
        }
    }
}

pub(crate) struct SnapshotUpdate {
    pub(crate) aggregate: Value,
    pub(crate) snapshot_version: usize,
}

pub(crate) struct CommitRequest<'events> {
    stream: StreamIdentity,
    events: &'events [SerializedEvent],
    snapshot: Option<SnapshotUpdate>,
    jobs: Vec<EnqueueRequest>,
}

impl<'events> CommitRequest<'events> {
    pub(crate) fn new(stream: StreamIdentity, events: &'events [SerializedEvent]) -> Self {
        Self {
            stream,
            events,
            snapshot: None,
            jobs: vec![],
        }
    }

    #[must_use]
    pub(crate) fn with_snapshot(mut self, snapshot: SnapshotUpdate) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    #[must_use]
    pub(crate) fn with_jobs(mut self, jobs: Vec<EnqueueRequest>) -> Self {
        self.jobs = jobs;
        self
    }
}

#[derive(Clone)]
pub(crate) struct Engine {
    pool: SqlitePool,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum EngineError {
    #[error("optimistic lock error: aggregate has been modified concurrently")]
    OptimisticLock,
    #[error("snapshot update was requested without persisted events")]
    EmptySnapshotUpdate,
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Integer(#[from] std::num::TryFromIntError),
    #[error(transparent)]
    JobFlush(#[from] crate::job::JobStoreError),
}

impl Engine {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub(crate) async fn load_events(
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

    pub(crate) async fn load_snapshot(
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

    pub(crate) fn stream_events(
        &self,
        aggregate_type: &'static str,
        aggregate_id: Option<String>,
        channel_size: usize,
    ) -> ReplayStream {
        let (mut feed, stream) = ReplayStream::new(channel_size);
        let pool = self.pool.clone();

        tokio::spawn(async move {
            let rows = match &aggregate_id {
                Some(aggregate_id) => {
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
                    .fetch_all(&pool)
                    .await
                }
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
                        WHERE aggregate_type = ?1
                        ORDER BY sequence
                        "#,
                        aggregate_type,
                    )
                    .fetch_all(&pool)
                    .await
                }
            };

            let rows = match rows {
                Ok(rows) => rows,
                Err(error) => {
                    let error = EngineError::Sql(error);
                    let _ = feed.push(Err(PersistenceError::from(error))).await;
                    return;
                }
            };

            for row in rows {
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

    pub(crate) async fn commit(&self, request: CommitRequest<'_>) -> Result<(), EngineError> {
        let CommitRequest {
            stream,
            events,
            snapshot,
            jobs,
        } = request;
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

impl From<EngineError> for PersistenceError {
    fn from(error: EngineError) -> Self {
        use EngineError::*;
        match error {
            OptimisticLock => Self::OptimisticLockError,
            EmptySnapshotUpdate => Self::UnknownError(Box::new(error)),
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
    use super::*;

    #[tokio::test]
    async fn commit_and_load_use_the_existing_serialized_event_contract() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlite_es::MIGRATOR.run(&pool).await.unwrap();
        let engine = Engine::new(pool);
        let stream = StreamIdentity::new("engine-test", "one");
        let event = SerializedEvent {
            aggregate_type: stream.aggregate_type.clone(),
            aggregate_id: stream.aggregate_id.clone(),
            sequence: 1,
            event_type: "Created".to_string(),
            event_version: "1.0".to_string(),
            payload: serde_json::json!({ "value": 42 }),
            metadata: serde_json::json!({}),
        };

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
}
