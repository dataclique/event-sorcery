//! Storage contract for projection recovery on an existing connection.

use std::num::NonZeroU64;

use crate::Table;

/// Retained history selected for a materialized-view recovery pass.
#[derive(Clone, Debug)]
pub enum ProjectionRecoveryMode {
    /// Replay only streams whose materialized row is missing or behind.
    CatchUp,
    /// Replay missing events for one stream without resetting its checkpoint.
    CatchUpOne {
        /// Persisted aggregate identity selected for catch-up.
        aggregate_id: String,
    },
    /// Rebuild every materialized row from retained history.
    RebuildAll,
    /// Rebuild one materialized row from its retained stream.
    RebuildOne {
        /// Aggregate identity encoded by the existing CQRS framework.
        aggregate_id: String,
    },
}

/// Stored state selected consistently with an event-stream maximum.
#[derive(Debug)]
pub struct ProjectionCheckpoint {
    /// Event sequence covered by the stored materialized state, not schema version.
    pub sequence: u64,
    /// Serialized lifecycle state read with the checkpoint.
    pub payload: String,
}

/// One stream selected for recovery by an events-to-views left join.
#[derive(Debug)]
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

/// Evidence of retained events through a persisted snapshot boundary.
#[derive(Debug)]
pub struct SnapshotHistory {
    /// Persisted ID of the snapshotted aggregate.
    pub aggregate_id: String,
    /// Last event sequence included in the snapshot.
    pub covered_sequence: i64,
    /// Number of retained events in sequences 1 through `covered_sequence`.
    pub retained_events: i64,
}

impl From<(String, i64, i64)> for SnapshotHistory {
    fn from((aggregate_id, covered_sequence, retained_events): (String, i64, i64)) -> Self {
        Self {
            aggregate_id,
            covered_sequence,
            retained_events,
        }
    }
}

/// Database operations performed inside the caller's recovery transaction.
///
/// Implementations never commit through this interface. The caller owns commit
/// and schema-registration ordering; schema versions must not advance over
/// failed replay. A backend may include registration in the same transaction.
pub trait ProjectionRecoveryStorage: Send {
    /// Reads history evidence for snapshots within the requested rebuild scope.
    /// A missing ID selects all snapshots for the aggregate type.
    fn snapshot_histories(
        &mut self,
        aggregate_type: &str,
        aggregate_id: Option<&str>,
    ) -> impl Future<Output = Result<Vec<SnapshotHistory>, sqlx::Error>> + Send;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_row_decodes_missing_and_present_checkpoints() {
        let missing =
            ProjectionReplayRow::try_from(("missing".to_string(), None, None, 7)).unwrap();
        assert_eq!(missing.aggregate_id, "missing");
        assert_eq!(missing.maximum_sequence.get(), 7);
        let None = missing.checkpoint else {
            panic!("a missing view must retain its absent checkpoint");
        };
        let present = ProjectionReplayRow::try_from((
            "present".to_string(),
            Some(3),
            Some("{}".to_string()),
            7,
        ))
        .unwrap();
        let Some(checkpoint) = present.checkpoint else {
            panic!("stored state and sequence must be retained together");
        };
        assert_eq!(present.maximum_sequence.get(), 7);
        assert_eq!(checkpoint.sequence, 3);
        assert_eq!(checkpoint.payload, "{}");
    }

    #[test]
    fn replay_row_rejects_partial_checkpoints_and_invalid_counters() {
        for (sequence, payload, maximum) in [
            (Some(1), None, 2),
            (None, Some("{}".to_string()), 2),
            (Some(-1), Some("{}".to_string()), 2),
            (None, None, 0),
            (None, None, -1),
        ] {
            let error =
                ProjectionReplayRow::try_from(("invalid".to_string(), sequence, payload, maximum))
                    .unwrap_err();
            assert!(matches!(error, sqlx::Error::Decode(_)));
        }
    }
}
