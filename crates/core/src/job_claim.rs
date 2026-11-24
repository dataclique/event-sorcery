//! Storage contracts for claiming jobs and renewing their leases.

use cqrs_es::persist::SerializedEvent;

/// The `job_queue_projection` row as the claim transaction re-reads it, raw. The crate
/// deserializes `payload` into `Lifecycle<JobState>` itself.
pub struct ClaimRead {
    /// The last applied event sequence (the view version).
    pub version: i64,
    /// The serialized `Lifecycle<JobState>` projection payload.
    pub payload: String,
    /// The projection-only lease column -- the value the runnable check reads.
    pub lease_until_ms: Option<i64>,
}

/// What the crate's `plan_claim` decided after re-reading the row
/// in-transaction. `Won` is the crate's strongly-typed success payload, opaque
/// to the backend.
pub enum ClaimDecision<Won> {
    /// Win the claim: append `event` (a `Claimed`) at `event.sequence` (a
    /// uniqueness violation -> [`ClaimOutcome::Contended`]); then write the row
    /// `(version = event.sequence, payload, lease_until_ms)` ->
    /// [`ClaimOutcome::Won`].
    Claim {
        /// The `Claimed` event to compare-and-swap-append.
        event: SerializedEvent,
        /// The new `Lifecycle<JobState>` projection payload.
        payload: String,
        /// The lease to write on the projection row.
        lease_until_ms: i64,
        /// The strongly-typed result handed back on success.
        won: Won,
    },
    /// Dead-letter as abandoned (claim budget exhausted): append `event` (a
    /// `Dead`) then write the terminal row -> [`ClaimOutcome::Abandoned`].
    Abandon {
        /// The `Dead` event to append.
        event: SerializedEvent,
        /// The new `Lifecycle<JobState>` (Dead) projection payload.
        payload: String,
    },
    /// Gone, not runnable, or undecodable: roll back -> [`ClaimOutcome::Skip`].
    Skip,
}

/// Outcome of the claim transaction, carrying the crate's strongly-typed `Won`.
pub enum ClaimOutcome<Won> {
    /// Claimed; run it.
    Won(Won),
    /// Dead-lettered as abandoned; the slot is freed.
    Abandoned,
    /// A concurrent worker won the compare-and-swap.
    Contended,
    /// No longer runnable (terminated, re-claimed with a live lease, or gone).
    Skip,
}

/// Outcome of the lease renewal write.
pub enum LeaseRenewal {
    /// The update matched the claimed row: the lease is still held.
    Held,
    /// Zero rows matched: the ack advanced the row, or a re-claimer took it.
    Lost,
}
