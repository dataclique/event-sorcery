//! Serialized persistence contracts, payload codecs and validated commit data.

use cqrs_es::persist::{PersistenceError, SerializedEvent, SerializedSnapshot};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::job::{EnqueueRequest, PreparedPendingJobs};
use crate::schema_registry::ReconcileError;

#[cfg(any(test, feature = "sqlite"))]
mod binding;

#[cfg(any(test, feature = "sqlite"))]
pub(crate) use binding::commit_serialized;
#[cfg(any(test, feature = "sqlite"))]
pub use binding::{Engine, OpaqueCommitRequest, SnapshotWrite};

const ENGINE_PAYLOAD_PROVENANCE_KEY: &str = "$event-sorcery-engine-owned";

/// Identifies one serialized aggregate event stream at the storage boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamIdentity {
    /// Aggregate type namespace stored with each event.
    pub(crate) aggregate_type: String,
    /// Aggregate identifier within that namespace.
    pub(crate) aggregate_id: String,
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

/// One binding-owned event payload proposed at the erased engine boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpaqueProposedEvent {
    event_type: String,
    event_version: String,
    payload: Vec<u8>,
}

impl OpaqueProposedEvent {
    /// Creates an event whose payload remains opaque to the engine.
    pub fn new(
        event_type: impl Into<String>,
        event_version: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            event_version: event_version.into(),
            payload,
        }
    }
}

/// One stored event returned through the erased opaque-payload facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpaqueStoredEvent {
    sequence: usize,
    event_type: String,
    event_version: String,
    payload: Vec<u8>,
}

impl OpaqueStoredEvent {
    /// Creates a stored-event value for adapters and conformance fixtures.
    pub fn new(
        sequence: usize,
        event_type: impl Into<String>,
        event_version: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            sequence,
            event_type: event_type.into(),
            event_version: event_version.into(),
            payload,
        }
    }

    /// Consumes the event into its stable storage-boundary fields.
    pub fn into_parts(self) -> (usize, String, String, Vec<u8>) {
        (
            self.sequence,
            self.event_type,
            self.event_version,
            self.payload,
        )
    }
}

enum SnapshotCoverage {
    BatchEnd,
    Through(usize),
}

/// Snapshot payload written atomically with the events it covers.
pub struct SnapshotUpdate {
    /// Serialized aggregate state proposed for the snapshot.
    pub(crate) aggregate: Value,
    /// Snapshot write counter, independent of covered event sequence.
    pub(crate) snapshot_version: usize,
    coverage: SnapshotCoverage,
}

impl SnapshotUpdate {
    /// Creates a snapshot update covering the batch at its snapshot-version counter.
    pub fn new(aggregate: Value, snapshot_version: usize) -> Self {
        Self {
            aggregate,
            snapshot_version,
            coverage: SnapshotCoverage::BatchEnd,
        }
    }

    /// Preserves the snapshot's covered sequence at a repository boundary.
    pub fn from_serialized(snapshot: SerializedSnapshot) -> (String, Self) {
        let SerializedSnapshot {
            aggregate_id,
            aggregate,
            current_sequence,
            current_snapshot,
        } = snapshot;
        (
            aggregate_id,
            Self {
                aggregate,
                snapshot_version: current_snapshot,
                coverage: SnapshotCoverage::Through(current_sequence),
            },
        )
    }

    pub(crate) fn sequence_for(&self, events: &[SerializedEvent]) -> Result<usize, EngineError> {
        let current = events
            .last()
            .ok_or(EngineError::EmptySnapshotUpdate)?
            .sequence;
        match self.coverage {
            SnapshotCoverage::BatchEnd => Ok(current),
            SnapshotCoverage::Through(sequence) if sequence <= current => Ok(sequence),
            SnapshotCoverage::Through(sequence) => Err(EngineError::SnapshotBeyondCurrentVersion {
                proposed: sequence,
                current,
            }),
        }
    }
}

/// One atomic event-store commit, including optional snapshot and job intent.
pub struct CommitRequest<'events> {
    /// Identity shared by all events in this atomic write.
    pub(crate) stream: StreamIdentity,
    /// Optional caller-supplied stream version guard.
    pub(crate) expected_version: Option<usize>,
    /// Borrowed batch of proposed serialized events.
    pub(crate) events: &'events [SerializedEvent],
    /// Aggregate snapshot to commit alongside the events, when supplied.
    pub(crate) snapshot: Option<SnapshotUpdate>,
    /// Durable job intents committed in the same transaction.
    pub(crate) jobs: Vec<EnqueueRequest>,
}

/// Validated batch whose snapshot and jobs are prepared at their write steps.
#[non_exhaustive]
pub struct CheckedCommit<'events> {
    /// Identity shared by all events in this batch.
    pub stream: StreamIdentity,
    /// Durable stream version the writer must check before appending.
    pub expected_version: Option<usize>,
    /// Contiguous events belonging to the identified stream.
    pub events: &'events [SerializedEvent],
    /// Snapshot to prepare after appending the batch, when supplied.
    pub snapshot: Option<SnapshotUpdate>,
    /// Durable job intents to prepare and write in order after the snapshot.
    pub jobs: Vec<JobSeed>,
}

/// Validated storage data consumed by a concrete database writer.
#[non_exhaustive]
pub struct PreparedCommit<'events> {
    /// Aggregate type namespace validated against the proposed events.
    pub aggregate_type: String,
    /// Aggregate identifier validated against the proposed events.
    pub aggregate_id: String,
    /// Optional caller-supplied stream version guard.
    pub expected_version: Option<usize>,
    /// Contiguous serialized event batch validated for this stream.
    pub events: &'events [SerializedEvent],
    /// Snapshot with validated event coverage and an independent write counter.
    pub snapshot: Option<PreparedSnapshot>,
    /// Canonical job events and queue payloads committed atomically with this batch.
    pub jobs: Vec<PreparedJob>,
}

/// Snapshot data with its independently validated coverage and counter.
#[non_exhaustive]
pub struct PreparedSnapshot {
    /// Serialized aggregate state at the covered event sequence.
    pub aggregate: Value,
    /// Snapshot write counter, not an event sequence.
    pub snapshot_version: usize,
    /// Highest event sequence represented by the aggregate state.
    pub last_sequence: usize,
}

/// Canonical job event and derived queue payload for one atomic write.
#[non_exhaustive]
pub struct PreparedJob {
    /// Canonical enqueue event appended to the job's event stream.
    pub event: SerializedEvent,
    /// Serialized initial job state for the derived queue row.
    pub queue_payload: String,
}

/// Failures produced by serialized engine operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EngineError {
    #[error("optimistic lock error: aggregate has been modified concurrently")]
    OptimisticLock,
    #[error("snapshot update was requested without persisted events")]
    EmptySnapshotUpdate,
    #[error("snapshot sequence {proposed} is beyond the current stream version {current}")]
    SnapshotBeyondCurrentVersion { proposed: usize, current: usize },
    #[error("snapshot version is exhausted")]
    SnapshotVersionExhausted,
    #[error("commit event stream {actual:?} does not match requested stream {expected:?}")]
    StreamIdentityMismatch {
        expected: StreamIdentity,
        actual: StreamIdentity,
    },
    #[error("event page storage bytes {observed} exceed limit {limit}")]
    EventPageTooLarge { observed: usize, limit: usize },
    #[error("event metadata uses reserved engine key {key}")]
    ReservedMetadata { key: &'static str },
    #[error("unsupported opaque payload envelope version {actual}")]
    OpaquePayloadVersion { actual: u8 },
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Integer(#[from] std::num::TryFromIntError),
    #[error(transparent)]
    JobFlush(#[from] crate::job::JobStoreError),
    #[error(transparent)]
    Schema(#[from] ReconcileError),
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
            expected_version: None,
            events,
            snapshot: None,
            jobs: vec![],
        }
    }

    /// Requires this version to still be current when the commit is written.
    #[must_use]
    pub fn at_expected_version(mut self, expected_version: usize) -> Self {
        self.expected_version = Some(expected_version);
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

    /// Prepares a standalone job for the same atomic storage path as commands.
    pub fn for_job(job: JobSeed) -> Self {
        let JobSeed(request) = job;
        let stream = StreamIdentity::new(
            <crate::job::JobState as crate::EventSourced>::AGGREGATE_TYPE,
            request.job_id.to_string(),
        );
        Self::new(stream, &[]).with_jobs(vec![request])
    }

    /// Captures command-scoped jobs without clearing them before database commit.
    pub fn prepare_repository(
        aggregate_type: &str,
        events: &'events [SerializedEvent],
        snapshot_update: Option<(String, SnapshotUpdate)>,
    ) -> Result<(Self, PreparedPendingJobs), EngineError> {
        let aggregate_id = snapshot_update
            .as_ref()
            .map_or_else(
                || events.first().map(|event| event.aggregate_id.as_str()),
                |update| Some(update.0.as_str()),
            )
            .unwrap_or_default();
        let stream = StreamIdentity::new(aggregate_type, aggregate_id);
        let mut pending_jobs = crate::job::prepare_pending()?;
        let mut request = Self::new(stream, events).with_jobs(pending_jobs.take_requests());
        if let Some(first) = events.first() {
            let expected = first
                .sequence
                .checked_sub(1)
                .ok_or(EngineError::OptimisticLock)?;
            request = request.at_expected_version(expected);
        }
        if let Some((_, snapshot)) = snapshot_update {
            request = request.with_snapshot(snapshot)?;
        }

        Ok((request, pending_jobs))
    }

    /// Checks typed metadata and stream identity without preparing snapshot or job writes.
    pub fn check(self) -> Result<CheckedCommit<'events>, EngineError> {
        self.validate_metadata()?;
        self.check_stream()
    }

    fn check_stream(self) -> Result<CheckedCommit<'events>, EngineError> {
        self.validate_stream()?;

        Ok(CheckedCommit {
            stream: self.stream,
            expected_version: self.expected_version,
            events: self.events,
            snapshot: self.snapshot,
            jobs: self.jobs.into_iter().map(JobSeed).collect(),
        })
    }

    /// Validates and serializes shared domain decisions before database-specific SQL.
    pub fn prepare(self) -> Result<PreparedCommit<'events>, EngineError> {
        let CheckedCommit {
            stream,
            expected_version,
            events,
            snapshot,
            jobs,
        } = self.check()?;
        let snapshot = snapshot
            .map(|snapshot| {
                let last_sequence = snapshot.sequence_for(events)?;
                Ok::<_, EngineError>(PreparedSnapshot {
                    aggregate: snapshot.aggregate,
                    snapshot_version: snapshot.snapshot_version,
                    last_sequence,
                })
            })
            .transpose()?;
        let jobs = jobs
            .iter()
            .map(|JobSeed(request)| {
                Ok::<_, EngineError>(PreparedJob {
                    event: crate::job::enqueued_event(request)?,
                    queue_payload: crate::job::pending_seed_payload(request)?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PreparedCommit {
            aggregate_type: stream.aggregate_type,
            aggregate_id: stream.aggregate_id,
            expected_version,
            events,
            snapshot,
            jobs,
        })
    }

    pub(crate) fn validate_metadata(&self) -> Result<(), EngineError> {
        if self
            .events
            .iter()
            .any(|event| reserves_engine_payload(&event.metadata))
        {
            return Err(EngineError::ReservedMetadata {
                key: ENGINE_PAYLOAD_PROVENANCE_KEY,
            });
        }

        Ok(())
    }

    pub(crate) fn validate_stream(&self) -> Result<(), EngineError> {
        validate_event_stream(&self.stream, self.events)?;
        if self
            .events
            .windows(2)
            .any(|events| events[0].sequence.checked_add(1) != Some(events[1].sequence))
        {
            return Err(EngineError::OptimisticLock);
        }

        if self
            .expected_version
            .is_some_and(|expected| !events_follow_version(self.events, expected))
        {
            return Err(EngineError::OptimisticLock);
        }

        Ok(())
    }
}

impl From<EngineError> for PersistenceError {
    fn from(error: EngineError) -> Self {
        use EngineError::*;
        match error {
            OptimisticLock => Self::OptimisticLockError,
            EmptySnapshotUpdate
            | StreamIdentityMismatch { .. }
            | EventPageTooLarge { .. }
            | ReservedMetadata { .. }
            | OpaquePayloadVersion { .. }
            | SnapshotBeyondCurrentVersion { .. }
            | SnapshotVersionExhausted => Self::UnknownError(Box::new(error)),
            Sql(source) => Self::ConnectionError(Box::new(source)),
            Json(source) => Self::DeserializationError(Box::new(source)),
            Integer(source) => Self::UnknownError(Box::new(source)),
            JobFlush(source) => Self::UnknownError(Box::new(source)),
            Schema(source) => Self::UnknownError(Box::new(source)),
        }
    }
}

pub fn validate_expected_version(expected: usize, actual: usize) -> Result<(), EngineError> {
    if actual != expected {
        return Err(EngineError::OptimisticLock);
    }

    Ok(())
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

fn events_follow_version(events: &[SerializedEvent], expected: usize) -> bool {
    events.iter().enumerate().all(|(index, event)| {
        expected
            .checked_add(index)
            .and_then(|sequence| sequence.checked_add(1))
            == Some(event.sequence)
    })
}

#[derive(Deserialize, Serialize)]
struct EnginePayloadEnvelope {
    #[serde(rename = "$event-sorcery-engine-payload")]
    opaque: EngineOpaquePayload,
}

#[derive(Deserialize, Serialize)]
struct EngineOpaquePayload {
    version: u8,
    bytes: Vec<u8>,
}

/// Wraps binding-owned bytes in the envelope the engine stores them under.
///
/// Every erased payload the engine owns -- event or snapshot -- is stored
/// through this envelope, so one reader understands all of them.
pub fn encode_opaque_payload(payload: Vec<u8>) -> Result<Value, EngineError> {
    Ok(serde_json::to_value(EnginePayloadEnvelope {
        opaque: EngineOpaquePayload {
            version: 1,
            bytes: payload,
        },
    })?)
}

/// Reads binding-owned bytes back out of the envelope they were stored under.
///
/// # Errors
///
/// Returns [`EngineError::Json`] when the stored value is not an engine-owned
/// envelope -- the shape a native consumer's own JSON has -- and
/// [`EngineError::OpaquePayloadVersion`] when the envelope is one this engine
/// cannot read.
pub fn decode_opaque_payload(payload: Value) -> Result<Vec<u8>, EngineError> {
    let envelope: EnginePayloadEnvelope = serde_json::from_value(payload)?;
    if envelope.opaque.version != 1 {
        return Err(EngineError::OpaquePayloadVersion {
            actual: envelope.opaque.version,
        });
    }

    Ok(envelope.opaque.bytes)
}

fn reserves_engine_payload(metadata: &Value) -> bool {
    match metadata {
        Value::Object(object) => object.contains_key(ENGINE_PAYLOAD_PROVENANCE_KEY),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    //! Commit preparation validates batches independently of database version guards.

    use cqrs_es::persist::SerializedEvent;
    use serde_json::json;

    use super::{CommitRequest, EngineError, StreamIdentity};

    #[test]
    fn unguarded_commit_rejects_noncontiguous_event_batches() {
        for sequences in [[1, 3], [2, 1], [1, 1], [usize::MAX, 0]] {
            let events: Vec<_> = sequences
                .into_iter()
                .map(|sequence| SerializedEvent {
                    aggregate_type: "CommitProbe".to_string(),
                    aggregate_id: "probe".to_string(),
                    sequence,
                    event_type: "Updated".to_string(),
                    event_version: "1".to_string(),
                    payload: json!({"value": sequence}),
                    metadata: json!({}),
                })
                .collect();
            let result =
                CommitRequest::new(StreamIdentity::new("CommitProbe", "probe"), &events).prepare();
            assert!(matches!(result, Err(EngineError::OptimisticLock)));
        }
    }

    #[test]
    fn unguarded_commit_preserves_contiguous_and_empty_batches() {
        for sequences in [vec![], vec![7], vec![7, 8, 9]] {
            let events: Vec<_> = sequences
                .into_iter()
                .map(|sequence| SerializedEvent {
                    aggregate_type: "CommitProbe".to_string(),
                    aggregate_id: "probe".to_string(),
                    sequence,
                    event_type: "Updated".to_string(),
                    event_version: "1".to_string(),
                    payload: json!({"value": sequence}),
                    metadata: json!({}),
                })
                .collect();
            let prepared = CommitRequest::new(StreamIdentity::new("CommitProbe", "probe"), &events)
                .prepare()
                .unwrap();
            assert_eq!(prepared.events.len(), events.len());
            assert_eq!(prepared.expected_version, None);
        }
    }
}
