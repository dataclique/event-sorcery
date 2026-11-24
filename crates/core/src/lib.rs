//! Shared persistence contracts for event-sorcery and its storage adapters.

mod projection_recovery;

pub use projection_recovery::{
    ProjectionCheckpoint, ProjectionRecoveryMode, ProjectionRecoveryStorage, ProjectionReplayRow,
};

/// A materialized view table name.
#[derive(Debug, Clone, Copy)]
pub struct Table(
    /// The table declared by the entity for its materialized view.
    pub &'static str,
);

impl std::fmt::Display for Table {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(name) = self;
        formatter.write_str(name)
    }
}

/// Whether old events may be deleted after they are captured in a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionPolicy {
    /// Keep all events indefinitely.
    Retain,
    /// Delete events at or before the current snapshot sequence.
    CompactAfterSnapshot,
    /// Preserve a trailing sequence window independently for each aggregate type and id.
    ///
    /// Delete only events both covered by the snapshot and at or below the
    /// stream's newest sequence minus `events`. Unsnapshotted events survive
    /// even when they exceed the window. Sparse histories may retain fewer
    /// rows than the numeric window. Without a snapshot, nothing is deleted.
    RetainLast {
        /// Width of the retained sequence interval; zero keeps only unsnapshotted events.
        events: usize,
    },
}
