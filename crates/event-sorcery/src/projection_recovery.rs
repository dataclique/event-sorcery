//! Materialized-view replay through borrowed database I/O.

use cqrs_es::Aggregate;
use event_sorcery_core::{
    ProjectionCheckpoint, ProjectionRecoveryMode, ProjectionRecoveryStorage, ProjectionReplayRow,
};
use tracing::warn;

use crate::lifecycle::Lifecycle;
use crate::{EventSourced, ProjectionError, Table};

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
