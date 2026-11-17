//! Materialized-view replay through borrowed database I/O.

use cqrs_es::Aggregate;
use std::num::NonZeroU64;
use tracing::warn;

use crate::lifecycle::Lifecycle;
use crate::{EventSourced, ProjectionError, Table};

/// Retained history selected for a materialized-view recovery pass.
#[derive(Clone, Debug)]
pub enum ProjectionRecoveryMode {
    /// Replay only streams whose materialized row is missing or behind.
    CatchUp,
    /// Rebuild every materialized row from retained history.
    RebuildAll,
    /// Rebuild one materialized row from its retained stream.
    RebuildOne {
        /// Aggregate identity encoded by the existing CQRS framework.
        aggregate_id: String,
    },
}

/// Stored state selected consistently with an event-stream maximum.
pub struct ProjectionCheckpoint {
    /// Event sequence covered by the stored materialized state, not schema version.
    pub sequence: u64,
    /// Serialized lifecycle state read with the checkpoint.
    pub payload: String,
}

/// One stream selected for recovery by an events-to-views left join.
pub struct ProjectionReplayRow {
    /// Persisted aggregate identity of the authoritative event stream.
    pub aggregate_id: String,
    /// Existing materialized state; absent when no view row exists.
    pub checkpoint: Option<ProjectionCheckpoint>,
    /// Positive event maximum captured in the same query as the checkpoint.
    pub maximum_sequence: NonZeroU64,
}

#[derive(Debug, thiserror::Error)]
#[error("materialized view checkpoint and payload must both be present or absent")]
struct IncompleteProjectionCheckpoint;

impl TryFrom<(String, Option<i64>, Option<String>, i64)> for ProjectionReplayRow {
    type Error = sqlx::Error;

    fn try_from(
        (aggregate_id, sequence, payload, maximum): (String, Option<i64>, Option<String>, i64),
    ) -> Result<Self, Self::Error> {
        let maximum =
            u64::try_from(maximum).map_err(|source| sqlx::Error::Decode(Box::new(source)))?;
        let maximum_sequence = NonZeroU64::try_from(maximum)
            .map_err(|source| sqlx::Error::Decode(Box::new(source)))?;
        let checkpoint = match (sequence, payload) {
            (None, None) => None,
            (Some(sequence), Some(payload)) => Some(ProjectionCheckpoint {
                sequence: u64::try_from(sequence)
                    .map_err(|source| sqlx::Error::Decode(Box::new(source)))?,
                payload,
            }),
            (None, Some(_)) | (Some(_), None) => {
                return Err(sqlx::Error::Decode(Box::new(
                    IncompleteProjectionCheckpoint,
                )));
            }
        };
        Ok(Self {
            aggregate_id,
            checkpoint,
            maximum_sequence,
        })
    }
}

/// Database operations performed inside the caller's recovery transaction.
///
/// Implementations never commit. Startup records the reconciled schema version
/// after replay succeeds, then commits the complete recovery operation.
pub trait ProjectionRecoveryStorage: Send {
    /// Deletes only rows selected by an explicit rebuild; catch-up keeps rows.
    fn reset(
        &mut self,
        table: Table,
        mode: &ProjectionRecoveryMode,
    ) -> impl Future<Output = Result<(), sqlx::Error>> + Send;

    /// Captures stream maxima and materialized state in one consistent query.
    fn streams(
        &mut self,
        table: Table,
        aggregate_type: &str,
        mode: &ProjectionRecoveryMode,
    ) -> impl Future<Output = Result<Vec<ProjectionReplayRow>, sqlx::Error>> + Send;

    /// Reads ordered payloads after `after` and through the captured maximum.
    fn events(
        &mut self,
        aggregate_type: &str,
        aggregate_id: &str,
        after: u64,
        through: NonZeroU64,
    ) -> impl Future<Output = Result<Vec<String>, sqlx::Error>> + Send;

    /// Writes replayed state at the captured event checkpoint, not version + 1.
    fn save(
        &mut self,
        table: Table,
        aggregate_id: &str,
        sequence: NonZeroU64,
        payload: &str,
    ) -> impl Future<Output = Result<(), sqlx::Error>> + Send;
}

/// Replays materialized state without acquiring or committing a connection.
///
/// # Errors
///
/// Preserves database, payload-decoding and lifecycle failures. The caller must
/// roll back on failure and register the schema before committing on success.
pub async fn recover_projection<Entity: EventSourced<Materialized = Table>>(
    storage: &mut impl ProjectionRecoveryStorage,
    mode: &ProjectionRecoveryMode,
) -> Result<(), ProjectionError<Entity>> {
    let table = Entity::PROJECTION;
    let aggregate_type = <Lifecycle<Entity> as Aggregate>::TYPE;
    storage.reset(table, mode).await?;
    let streams = storage.streams(table, aggregate_type, mode).await?;
    for stream in streams {
        let ProjectionReplayRow {
            aggregate_id,
            checkpoint,
            maximum_sequence,
        } = stream;
        let (mut lifecycle, after) = match checkpoint {
            None => (Lifecycle::<Entity>::default(), 0),
            Some(ProjectionCheckpoint { sequence, payload }) => {
                match serde_json::from_str(&payload) {
                    Ok(state) => (state, sequence),
                    Err(error) => {
                        warn!(aggregate_type, %aggregate_id, %error, "Replaying incompatible view payload");
                        (Lifecycle::<Entity>::default(), 0)
                    }
                }
            }
        };
        let payloads = storage
            .events(aggregate_type, &aggregate_id, after, maximum_sequence)
            .await?;
        for payload in payloads {
            let event =
                serde_json::from_str(&payload).map_err(|source| ProjectionError::Serde {
                    aggregate_id: aggregate_id.clone(),
                    source,
                })?;
            lifecycle.apply(event);
        }
        lifecycle.clone().into_result()?;
        let payload =
            serde_json::to_string(&lifecycle).map_err(|source| ProjectionError::Serde {
                aggregate_id: aggregate_id.clone(),
                source,
            })?;
        storage
            .save(table, &aggregate_id, maximum_sequence, &payload)
            .await?;
    }
    Ok(())
}
