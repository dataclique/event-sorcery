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

pub use event_sorcery_core::{ClaimDecision, ClaimOutcome, ClaimRead, EventBackend, LeaseRenewal};
