//! The backend abstraction for durable jobs: [`EventBackend`].
//!
//! A consumer supplies a single `EventBackend` -- a cqrs-es event repository plus
//! two engine-shaped primitives ([`claim`](EventBackend::claim) and
//! [`renew`](EventBackend::renew)) -- and gets the whole durable-jobs capability,
//! built by event-sorcery's own cqrs/es machinery (the job is an
//! [`crate::EventSourced`] aggregate, the ack is a command, the `job_queue_projection` is a
//! projection). The backend never names a job type: the claim *decision* (which
//! event to append, the new projection payload, the strongly-typed claim result)
//! is a crate-side closure, and the result rides through the backend as an opaque
//! `Won` payload. The backend owns only the *transaction*. See
//! [ADR-0006](../../adrs/0006-cqrs-native-durable-jobs.md).

use std::future::Future;

use cqrs_es::persist::{PersistedEventRepository, SerializedEvent};

pub use event_sorcery_core::{ClaimDecision, ClaimOutcome, ClaimRead, LeaseRenewal};

use crate::CompactionPolicy;

/// A complete event-sorcery backend.
///
/// Beyond the cqrs-es event repository, it exposes exactly two job-shaped
/// primitives that cqrs-es cannot: a write-locked compare-and-swap
/// [`claim`](Self::claim) (whose runnable check must read the projection-only
/// lease column, not a folded event lease), and a projection-only
/// [`renew`](Self::renew). Everything else -- the claim/ack CAS, the fence, the
/// retry, the dead-letter, the poll -- is cqrs-es `execute` + the generic
/// projection. Used only as a generic bound, never `dyn`.
pub trait EventBackend: Clone + Send + Sync + 'static {
    /// The cqrs-es event repository. Its `persist` flushes buffered `Enqueued`
    /// events AND seeds their `job_queue_projection` rows in the same transaction that
    /// commits the triggering events (the atomic-enqueue guarantee).
    type EventRepo: PersistedEventRepository + Send + Sync + 'static;

    /// Backend-native error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Builds the flush-aware event repository for `compaction_policy`.
    fn event_repo(&self, compaction_policy: CompactionPolicy) -> Self::EventRepo;

    /// Applies the canonical events + snapshots + `job_queue_projection` schema.
    fn migrate(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Claims one job in a single write-locked transaction: re-read the row, hand
    /// the RAW row to `decide`, and enact the decision (compare-and-swap-append
    /// the event via the events UNIQUE; write the row). `decide` is crate-side
    /// and owns all `JobState`/`JobEvent` knowledge; its strongly-typed success
    /// payload `Won` rides back through the backend untouched -- so the backend
    /// stays generic and names no job type.
    fn claim<Decide, Won>(
        &self,
        job_id: &str,
        decide: Decide,
    ) -> impl Future<Output = Result<ClaimOutcome<Won>, Self::Error>> + Send
    where
        Decide: FnOnce(Option<ClaimRead>) -> ClaimDecision<Won> + Send,
        Won: Send;

    /// Projection-only lease renewal: bump `lease_until` on the claimed row,
    /// keyed on its `version` (== the claim sequence), without touching
    /// `version`. Zero rows matched (the ack already advanced/changed the row, or
    /// a re-claimer took it) -> [`LeaseRenewal::Lost`].
    fn renew(
        &self,
        job_id: &str,
        claim_seq: i64,
        new_lease_until_ms: i64,
    ) -> impl Future<Output = Result<LeaseRenewal, Self::Error>> + Send;

    /// Standalone durable enqueue (ADR-0007): append the `Enqueued` `event` and
    /// seed its `job_queue_projection` row (`view_id = event.aggregate_id`, version 1,
    /// `payload`, no lease) in the backend's OWN transaction -- for enqueues from
    /// reactors, pollers, job-chains, and startup, which have no command commit to
    /// ride. A uniqueness violation on the event (the `view_id` already has a
    /// stream) surfaces as the backend error; callers mint a fresh id per enqueue.
    fn enqueue(
        &self,
        event: SerializedEvent,
        payload: String,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
