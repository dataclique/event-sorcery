//! Materialized-view replay through borrowed database I/O.

use cqrs_es::Aggregate;
use event_sorcery_core::{
    ProjectionCheckpoint, ProjectionRecoveryMode, ProjectionRecoveryStorage, ProjectionReplayRow,
};
use tracing::warn;

use crate::lifecycle::Lifecycle;
use crate::{
    CompactionPolicy, EventSourced, ProjectionError, StreamIdentity, Table,
    validate_snapshot_history,
};

/// Replays materialized state without acquiring or committing a connection.
///
/// # Errors
///
/// Rejects compactable entities and rebuilds with unproven snapshot history
/// before modifying the projection. Preserves
/// database, payload-decoding and lifecycle failures. The caller must roll back
/// on failure and register the schema before committing on successful startup.
pub async fn recover_projection<Entity: EventSourced<Materialized = Table>>(
    storage: &mut impl ProjectionRecoveryStorage,
    mode: &ProjectionRecoveryMode,
) -> Result<(), ProjectionError<Entity>> {
    use CompactionPolicy::{CompactAfterSnapshot, Retain, RetainLast};

    let aggregate_type = <Lifecycle<Entity> as Aggregate>::TYPE;
    match Entity::COMPACTION_POLICY {
        Retain => {}
        CompactAfterSnapshot | RetainLast { .. } => {
            return Err(ProjectionError::RequiresRetainedHistory { aggregate_type });
        }
    }

    use ProjectionRecoveryMode::{CatchUp, CatchUpOne, RebuildAll, RebuildOne};

    let histories = match mode {
        RebuildAll => storage.snapshot_histories(aggregate_type, None).await?,
        RebuildOne { aggregate_id } => {
            storage
                .snapshot_histories(aggregate_type, Some(aggregate_id))
                .await?
        }
        CatchUp | CatchUpOne { .. } => Vec::new(),
    };
    for history in histories {
        validate_snapshot_history(
            StreamIdentity::new(aggregate_type, history.aggregate_id),
            history.covered_sequence,
            history.retained_events,
        )
        .map_err(ProjectionError::UnprovenSnapshotHistory)?;
    }

    let table = Entity::PROJECTION;
    storage.reset(table, mode).await?;
    let streams = storage.streams(table, aggregate_type, mode).await?;
    for stream in streams {
        let ProjectionReplayRow {
            aggregate_id,
            checkpoint,
            maximum_sequence,
            snapshot_sequence,
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
        let events = storage
            .events(aggregate_type, &aggregate_id, after, maximum_sequence)
            .await?;
        if let Some(covered_through) = snapshot_sequence.filter(|&sequence| sequence > after) {
            let boundary = i64::try_from(covered_through)
                .map_err(|source| sqlx::Error::Decode(Box::new(source)))?;
            let retained_events = u64::try_from(
                events
                    .iter()
                    .take_while(|(sequence, _)| *sequence <= boundary)
                    .count(),
            )
            .map_err(|source| sqlx::Error::Decode(Box::new(source)))?;
            if retained_events != covered_through - after {
                return Err(ProjectionError::UnprovenReplayRange {
                    stream: StreamIdentity::new(aggregate_type, aggregate_id),
                    after,
                    covered_through,
                    retained_events,
                });
            }
        }
        for (_, payload) in events {
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
