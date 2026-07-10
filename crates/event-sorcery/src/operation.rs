//! Entity-scoped durable operations (ADR-0008).
//!
//! The lifecycle of a fallible external operation *on an entity* -- request it,
//! run it durably, feed the outcome back into entity state -- as a library
//! primitive instead of per-consumer plumbing. An entity embeds an
//! [`Operation<J>`] in its state and nests [`OperationEvent<J>`] /
//! [`OperationCommand<J>`] in its own event/command enums; the machine owns the
//! state guard ("ignore an outcome that already landed, refuse a second request
//! while one is in flight") and requesting enqueues the driving job in the same
//! transaction that commits the `Requested` event.
//!
//! The driving job implements [`OperationJob`], not [`Job`](crate::Job)'s
//! `perform`: the framework routes the first execution to
//! [`submit`](OperationJob::submit) and every later one to
//! [`reconcile`](OperationJob::reconcile), so a follow-up can never blindly
//! resubmit a call whose fate is unknown.

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, warn};

use crate::job::{
    Contains, DeadReason, Job, JobContext, JobFailure, JobId, JobList, JobOutcome, JobQueue, Label,
};
use crate::job_backend::error_chain;
use crate::job_store::EventBackend;
use crate::{EventSourced, Store};

/// A durable job that drives one entity-scoped operation (ADR-0008).
///
/// Unlike a plain [`Job`](crate::Job), an `OperationJob` does not implement
/// `perform`: the framework routes the FIRST execution to [`submit`](Self::submit)
/// and every later execution (retry, lease-expired reclaim, crash mid-call) to
/// [`reconcile`](Self::reconcile). The routing is driven by the durable claim
/// history on the job aggregate and over-approximates submissions in the safe
/// direction -- an execution can never be routed to `submit` while a prior
/// submission might exist.
///
/// Its settled outcome is delivered back to the [`Origin`](Self::Origin) entity
/// as an [`OperationCommand`] before the job acks, so the entity's
/// [`Operation<J>`] field always settles.
pub trait OperationJob:
    Clone + Debug + PartialEq + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// Dependency bundle injected into [`submit`](Self::submit) and
    /// [`reconcile`](Self::reconcile).
    type Input: Send + Sync + 'static;

    /// Value produced when the operation confirms. Lives inside the origin
    /// entity's events and state, so it must be event-grade data.
    type Output: Clone + Debug + PartialEq + Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Domain error produced when the operation is definitively rejected.
    /// Lives inside the origin entity's events and state.
    type Error: std::error::Error
        + Clone
        + Debug
        + PartialEq
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static;

    /// The entity whose [`Operation<Self>`] field this job settles.
    type Origin: EventSourced;

    /// Worker name prefix; the registered worker name is
    /// `format!("{WORKER_NAME}-{index}")`.
    const WORKER_NAME: &'static str;

    /// Stable identifier for this job kind, recorded on the `Enqueued` event and
    /// used to route a job to the worker that runs it.
    const KIND: &'static str;

    /// Human-readable label for structured logging.
    fn label(&self) -> Label;

    /// Identity of the origin entity whose operation this job settles. Carried
    /// by the job payload itself, so outcome delivery needs no side lookup.
    fn origin_id(&self) -> <Self::Origin as EventSourced>::Id;

    /// Make the external call for the FIRST time. Only ever invoked when no
    /// prior execution of this job can have reached the external system.
    ///
    /// Return [`JobOutcome::Done`] when the operation's outcome is known
    /// immediately, or [`JobOutcome::Defer`] when the call was submitted and
    /// the outcome must be polled ([`reconcile`](Self::reconcile) runs next).
    /// Derive the external-boundary idempotency key from
    /// [`JobContext::job_id`], never the attempt number.
    fn submit(
        &self,
        ctx: &JobContext,
        input: &Self::Input,
    ) -> impl Future<Output = Result<JobOutcome<Self::Output>, JobFailure<Self::Error>>> + Send;

    /// Determine the fate of an earlier execution -- typically by querying the
    /// external system with the operation's idempotency key. Invoked instead of
    /// [`submit`](Self::submit) whenever a prior execution might have reached
    /// the external system.
    fn reconcile(
        &self,
        ctx: &JobContext,
        input: &Self::Input,
    ) -> impl Future<Output = Result<Reconciliation<Self::Output>, JobFailure<Self::Error>>> + Send;
}

/// The verdict of [`OperationJob::reconcile`]: what happened to the earlier
/// execution whose fate was unknown.
#[derive(Debug)]
pub enum Reconciliation<Output> {
    /// The earlier submission landed; this is its outcome.
    Settled(Output),
    /// The earlier execution provably never reached the external system; the
    /// framework authorizes a fresh [`submit`](OperationJob::submit).
    NotSubmitted,
    /// Cannot tell yet -- re-run reconciliation after this delay, without
    /// counting an attempt (maps onto [`JobOutcome::Defer`]).
    Indeterminate(Duration),
}

/// How settled outcomes reach the origin entity.
///
/// Object-safe so the worker input stays backend-agnostic; [`Store`]
/// implements it whenever the origin's command enum absorbs
/// [`OperationCommand<J>`] via `From`.
#[async_trait]
pub trait OriginPort<J: OperationJob>: Send + Sync {
    /// Deliver a settled outcome as a command on the origin entity.
    async fn deliver(
        &self,
        id: &<J::Origin as EventSourced>::Id,
        command: OperationCommand<J>,
    ) -> Result<(), OriginDeliveryError>;
}

/// Delivering a settled outcome to the origin entity failed. The worker defers
/// and retries -- delivery failures never count as operation attempts, because
/// the operation itself already settled.
#[derive(Debug, thiserror::Error)]
#[error("outcome delivery to the origin entity failed")]
pub struct OriginDeliveryError(#[source] Box<dyn std::error::Error + Send + Sync>);

#[async_trait]
impl<J, Backend> OriginPort<J> for Store<J::Origin, Backend>
where
    J: OperationJob,
    Backend: EventBackend,
    <J::Origin as EventSourced>::Command: From<OperationCommand<J>>,
    <J::Origin as EventSourced>::Event: Clone + Debug + Serialize + DeserializeOwned + Send + Sync,
{
    async fn deliver(
        &self,
        id: &<J::Origin as EventSourced>::Id,
        command: OperationCommand<J>,
    ) -> Result<(), OriginDeliveryError> {
        self.send(id, command.into())
            .await
            .map_err(|send_error| OriginDeliveryError(Box::new(send_error)))
    }
}

/// Worker-side dependency bundle for an [`OperationJob`]: the consumer's input
/// plus the port outcomes are delivered through.
///
/// Register it as the job's input in `build_supervised_worker!`:
/// `PlaceHedge => OperationInput::new(hedge_deps, order_store.clone())`.
pub struct OperationInput<J: OperationJob> {
    input: J::Input,
    origin: Arc<dyn OriginPort<J>>,
}

impl<J: OperationJob> OperationInput<J> {
    /// Bundles the job's dependencies with the origin entity's delivery port
    /// (usually the origin's `Arc<Store>`).
    pub fn new<Port: OriginPort<J> + 'static>(input: J::Input, origin: Arc<Port>) -> Self {
        Self {
            input,
            origin: origin as Arc<dyn OriginPort<J>>,
        }
    }
}

/// How long the worker snoozes before retrying a failed outcome delivery.
/// Delivery retries ride [`JobOutcome::Defer`], so they never count attempts.
const DELIVERY_RETRY_DELAY: Duration = Duration::from_secs(5);

/// Every [`OperationJob`] is a [`Job`] whose execution is the submit/reconcile
/// routing plus outcome feed-back (ADR-0008).
///
/// The first execution runs [`submit`](OperationJob::submit); every later one
/// runs [`reconcile`](OperationJob::reconcile) first and only re-submits on an
/// explicit [`Reconciliation::NotSubmitted`]. A settled outcome is delivered to
/// the origin entity BEFORE the job acks: failed delivery defers the job (no
/// attempt counted -- the operation itself settled), so the verdict is
/// re-derived and re-delivered until the origin accepts it, and duplicates are
/// absorbed by the [`Operation`] state guard. A transient failure that exhausts
/// the retry budget best-effort delivers `Failed(DeadLettered)` before the
/// worker dead-letters, so the origin does not dangle in flight.
impl<J: OperationJob> Job for J {
    type Input = OperationInput<J>;
    type Output = ();
    type Error = J::Error;

    const WORKER_NAME: &'static str = <J as OperationJob>::WORKER_NAME;
    const KIND: &'static str = <J as OperationJob>::KIND;

    fn label(&self) -> Label {
        OperationJob::label(self)
    }

    async fn perform(
        &self,
        ctx: &JobContext,
        wrapped: &OperationInput<J>,
    ) -> Result<JobOutcome<()>, JobFailure<J::Error>> {
        let outcome = if ctx.is_first_execution() {
            self.submit(ctx, &wrapped.input).await
        } else {
            match self.reconcile(ctx, &wrapped.input).await {
                Ok(Reconciliation::Settled(output)) => Ok(JobOutcome::Done(output)),
                Ok(Reconciliation::Indeterminate(delay)) => return Ok(JobOutcome::Defer(delay)),
                Ok(Reconciliation::NotSubmitted) => self.submit(ctx, &wrapped.input).await,
                Err(failure) => Err(failure),
            }
        };

        let attempts = ctx.attempt() + 1;
        match outcome {
            Ok(JobOutcome::Done(output)) => {
                let confirm = OperationCommand::Confirm {
                    job_id: ctx.job_id(),
                    output,
                    attempts,
                };
                match wrapped.origin.deliver(&self.origin_id(), confirm).await {
                    Ok(()) => Ok(JobOutcome::Done(())),
                    Err(delivery_error) => {
                        warn!(
                            target: "cqrs", ?delivery_error, job_id = %ctx.job_id(),
                            "confirmed operation outcome could not be delivered; deferring"
                        );
                        Ok(JobOutcome::Defer(DELIVERY_RETRY_DELAY))
                    }
                }
            }
            Ok(JobOutcome::Defer(delay)) => Ok(JobOutcome::Defer(delay)),
            Err(JobFailure::Terminal(domain_error)) => {
                let fail = OperationCommand::Fail {
                    job_id: ctx.job_id(),
                    failure: OperationFailure::Rejected(domain_error.clone()),
                    attempts,
                };
                match wrapped.origin.deliver(&self.origin_id(), fail).await {
                    Ok(()) => Err(JobFailure::Terminal(domain_error)),
                    Err(delivery_error) => {
                        warn!(
                            target: "cqrs", ?delivery_error, job_id = %ctx.job_id(),
                            "terminal operation outcome could not be delivered; deferring"
                        );
                        Ok(JobOutcome::Defer(DELIVERY_RETRY_DELAY))
                    }
                }
            }
            Err(JobFailure::Transient(domain_error)) => {
                if ctx.is_final_attempt() {
                    // The worker will dead-letter this job; tell the origin so
                    // its operation does not dangle in flight. Best effort: a
                    // failed delivery here is logged loudly for the operator.
                    let fail = OperationCommand::Fail {
                        job_id: ctx.job_id(),
                        failure: OperationFailure::DeadLettered {
                            reason: DeadReason::RetriesExhausted,
                            detail: error_chain(&domain_error),
                        },
                        attempts,
                    };
                    if let Err(delivery_error) =
                        wrapped.origin.deliver(&self.origin_id(), fail).await
                    {
                        error!(
                            target: "cqrs", ?delivery_error, job_id = %ctx.job_id(),
                            "OPERATION DANGLING: job dead-letters but the origin \
                             entity could not be told; operator intervention needed"
                        );
                    }
                }
                Err(JobFailure::Transient(domain_error))
            }
        }
    }
}

/// The durable state of one entity-scoped operation, embedded in entity state.
///
/// Folded from [`OperationEvent`]s by [`evolve`](Self::evolve); commands are
/// guarded by [`transition`](Self::transition). The default state is
/// [`Idle`](Self::Idle).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum Operation<J: OperationJob> {
    /// No operation requested (or the previous one was superseded).
    #[default]
    Idle,
    /// A driving job is in flight; its outcome has not landed yet.
    Requested {
        /// The driving job, for outcome correlation and audit.
        job_id: JobId,
    },
    /// The operation confirmed with this output after `attempts` tries.
    Confirmed {
        job_id: JobId,
        output: J::Output,
        attempts: u32,
    },
    /// The operation definitively failed after `attempts` tries.
    Failed {
        job_id: JobId,
        failure: OperationFailure<J::Error>,
        attempts: u32,
    },
}

/// An operation-lifecycle fact, nested by the consumer in their entity's event
/// enum (one variant per operation field).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum OperationEvent<J: OperationJob> {
    /// The operation was requested; job `job_id` drives it and was enqueued in
    /// the same transaction that commits this event.
    Requested { job_id: JobId },
    /// The driving job settled the operation successfully.
    Confirmed {
        job_id: JobId,
        output: J::Output,
        attempts: u32,
    },
    /// The driving job settled the operation as definitively failed.
    Failed {
        job_id: JobId,
        failure: OperationFailure<J::Error>,
        attempts: u32,
    },
}

/// An operation-lifecycle instruction, nested in the entity's command enum.
///
/// `Request` comes from domain code; `Confirm`/`Fail` are delivered by the
/// framework when the driving job settles.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum OperationCommand<J: OperationJob> {
    /// Start the operation: enqueue `J` transactionally and record `Requested`.
    Request(J),
    /// The driving job confirmed the operation.
    Confirm {
        job_id: JobId,
        output: J::Output,
        attempts: u32,
    },
    /// The driving job definitively failed the operation.
    Fail {
        job_id: JobId,
        failure: OperationFailure<J::Error>,
        attempts: u32,
    },
}

/// Why an operation settled as [`Operation::Failed`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationFailure<DomainError> {
    /// The external system definitively rejected the operation
    /// ([`JobFailure::Terminal`]).
    Rejected(DomainError),
    /// The driving job dead-lettered without a definitive answer (retries
    /// exhausted, claim budget, undecodable payload); `detail` is the
    /// dead-letter error chain. The operation needs operator attention.
    DeadLettered { reason: DeadReason, detail: String },
}

/// A command the operation's state guard refused. Map into the entity's domain
/// error with `#[from]`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OperationRefused {
    /// A `Request` arrived while a driving job is already in flight.
    #[error("operation already in flight")]
    InFlight,
    /// A `Request` arrived after the operation confirmed.
    #[error("operation already confirmed")]
    AlreadyConfirmed,
    /// A `Confirm`/`Fail` does not correspond to this operation's state (wrong
    /// job id, no operation in flight, or contradicting an already-settled
    /// outcome).
    #[error("outcome does not correspond to the operation's state")]
    OutcomeMismatch,
}

/// Replayed an [`OperationEvent`] that cannot follow from the current state --
/// stream corruption or a schema bug, never a normal outcome.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("operation event cannot be applied to the current operation state")]
pub struct OperationReplay;

impl<J: OperationJob> Operation<J> {
    /// Guarded command handling: produces the events a command justifies, or
    /// refuses it. Call from the entity's `transition` handler, forwarding the
    /// handler's [`JobQueue`]; a `Request` enqueues the driving job onto it so
    /// the job flushes in the same transaction as the `Requested` event.
    ///
    /// Duplicate outcome delivery (the at-least-once ack path re-delivering a
    /// settled verdict) is absorbed: it produces no events and no error.
    pub fn transition<Jobs, Index>(
        &self,
        command: OperationCommand<J>,
        jobs: &JobQueue<Jobs>,
    ) -> Result<Vec<OperationEvent<J>>, OperationRefused>
    where
        Jobs: JobList + Contains<J, Index>,
    {
        match (self, command) {
            (Self::Idle | Self::Failed { .. }, OperationCommand::Request(job)) => {
                let job_id = jobs.push(job);
                Ok(vec![OperationEvent::Requested { job_id }])
            }
            (Self::Requested { .. }, OperationCommand::Request(_)) => {
                Err(OperationRefused::InFlight)
            }
            (Self::Confirmed { .. }, OperationCommand::Request(_)) => {
                Err(OperationRefused::AlreadyConfirmed)
            }
            (
                Self::Requested { job_id },
                OperationCommand::Confirm {
                    job_id: incoming,
                    output,
                    attempts,
                },
            ) => {
                if *job_id == incoming {
                    Ok(vec![OperationEvent::Confirmed {
                        job_id: incoming,
                        output,
                        attempts,
                    }])
                } else {
                    Err(OperationRefused::OutcomeMismatch)
                }
            }
            (
                Self::Requested { job_id },
                OperationCommand::Fail {
                    job_id: incoming,
                    failure,
                    attempts,
                },
            ) => {
                if *job_id == incoming {
                    Ok(vec![OperationEvent::Failed {
                        job_id: incoming,
                        failure,
                        attempts,
                    }])
                } else {
                    Err(OperationRefused::OutcomeMismatch)
                }
            }
            (
                Self::Confirmed { job_id, .. },
                OperationCommand::Confirm {
                    job_id: incoming, ..
                },
            )
            | (
                Self::Failed { job_id, .. },
                OperationCommand::Fail {
                    job_id: incoming, ..
                },
            ) => {
                if *job_id == incoming {
                    Ok(vec![])
                } else {
                    Err(OperationRefused::OutcomeMismatch)
                }
            }
            (Self::Idle | Self::Failed { .. }, OperationCommand::Confirm { .. })
            | (Self::Idle | Self::Confirmed { .. }, OperationCommand::Fail { .. }) => {
                Err(OperationRefused::OutcomeMismatch)
            }
        }
    }

    /// Folds an [`OperationEvent`] into the next state. Call from the entity's
    /// `evolve`/`originate`.
    pub fn evolve(&self, event: &OperationEvent<J>) -> Result<Self, OperationReplay> {
        match (self, event) {
            (Self::Idle | Self::Failed { .. }, OperationEvent::Requested { job_id }) => {
                Ok(Self::Requested { job_id: *job_id })
            }
            (
                Self::Requested { job_id },
                OperationEvent::Confirmed {
                    job_id: incoming,
                    output,
                    attempts,
                },
            ) if job_id == incoming => Ok(Self::Confirmed {
                job_id: *incoming,
                output: output.clone(),
                attempts: *attempts,
            }),
            (
                Self::Requested { job_id },
                OperationEvent::Failed {
                    job_id: incoming,
                    failure,
                    attempts,
                },
            ) if job_id == incoming => Ok(Self::Failed {
                job_id: *incoming,
                failure: failure.clone(),
                attempts: *attempts,
            }),
            (
                Self::Idle | Self::Requested { .. } | Self::Confirmed { .. } | Self::Failed { .. },
                OperationEvent::Requested { .. }
                | OperationEvent::Confirmed { .. }
                | OperationEvent::Failed { .. },
            ) => Err(OperationReplay),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use crate::job::{JobFailure, JobOutcome, take_pending, with_pending_scope};
    use crate::{Never, Nil, jobs};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct PlaceOrder {
        origin: u64,
        amount: u32,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
    #[error("order rejected: {reason}")]
    struct OrderRejected {
        reason: String,
    }

    #[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
    struct Desk;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum DeskEvent {
        Order(OperationEvent<PlaceOrder>),
    }

    impl cqrs_es::DomainEvent for DeskEvent {
        fn event_type(&self) -> String {
            "DeskEvent".to_string()
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[async_trait::async_trait]
    impl EventSourced for Desk {
        type Id = u64;
        type Event = DeskEvent;
        type Command = ();
        type Error = Never;
        type Jobs = jobs![PlaceOrder];
        type Materialized = Nil;

        const AGGREGATE_TYPE: &'static str = "Desk";
        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;

        fn originate(_event: &DeskEvent) -> Option<Self> {
            Some(Self)
        }

        fn evolve(_entity: &Self, _event: &DeskEvent) -> Result<Option<Self>, Never> {
            Ok(Some(Self))
        }

        async fn initialize(
            _command: (),
            _jobs: &JobQueue<Self::Jobs>,
        ) -> Result<Vec<DeskEvent>, Never> {
            Ok(vec![])
        }

        async fn transition(
            &self,
            _command: (),
            _jobs: &JobQueue<Self::Jobs>,
        ) -> Result<Vec<DeskEvent>, Never> {
            Ok(vec![])
        }
    }

    impl OperationJob for PlaceOrder {
        type Input = ();
        type Output = String;
        type Error = OrderRejected;
        type Origin = Desk;

        const WORKER_NAME: &'static str = "place-order";
        const KIND: &'static str = "place-order";

        fn label(&self) -> Label {
            Label::new(format!("place-order:{}", self.amount))
        }

        fn origin_id(&self) -> u64 {
            self.origin
        }

        async fn submit(
            &self,
            _ctx: &JobContext,
            _input: &(),
        ) -> Result<JobOutcome<String>, JobFailure<OrderRejected>> {
            Ok(JobOutcome::Done("filled".to_string()))
        }

        async fn reconcile(
            &self,
            _ctx: &JobContext,
            _input: &(),
        ) -> Result<Reconciliation<String>, JobFailure<OrderRejected>> {
            Ok(Reconciliation::Settled("filled".to_string()))
        }
    }

    fn place() -> PlaceOrder {
        PlaceOrder {
            origin: 7,
            amount: 100,
        }
    }

    fn queue() -> JobQueue<jobs![PlaceOrder]> {
        JobQueue::default()
    }

    #[tokio::test]
    async fn request_from_idle_enqueues_the_job_and_records_its_id() {
        let (events, pending) = with_pending_scope(async {
            let events = Operation::<PlaceOrder>::Idle
                .transition(OperationCommand::Request(place()), &queue())
                .unwrap();
            let pending = take_pending().unwrap();
            (events, pending)
        })
        .await;

        let [OperationEvent::Requested { job_id }] = events.as_slice() else {
            panic!("expected exactly one Requested event, got {events:?}");
        };
        let [request] = pending.as_slice() else {
            panic!("expected exactly one enqueued job");
        };
        assert_eq!(request.job_id, *job_id);
    }

    #[tokio::test]
    async fn request_while_in_flight_is_refused() {
        let state = Operation::<PlaceOrder>::Requested {
            job_id: JobId::new(),
        };
        let refused = with_pending_scope(async {
            state
                .transition(OperationCommand::Request(place()), &queue())
                .unwrap_err()
        })
        .await;
        assert_eq!(refused, OperationRefused::InFlight);
    }

    #[tokio::test]
    async fn request_after_confirmation_is_refused() {
        let state = Operation::<PlaceOrder>::Confirmed {
            job_id: JobId::new(),
            output: "filled".to_string(),
            attempts: 1,
        };
        let refused = with_pending_scope(async {
            state
                .transition(OperationCommand::Request(place()), &queue())
                .unwrap_err()
        })
        .await;
        assert_eq!(refused, OperationRefused::AlreadyConfirmed);
    }

    #[tokio::test]
    async fn request_after_failure_starts_a_fresh_operation() {
        let state = Operation::<PlaceOrder>::Failed {
            job_id: JobId::new(),
            failure: OperationFailure::Rejected(OrderRejected {
                reason: "no funds".to_string(),
            }),
            attempts: 1,
        };
        let events = with_pending_scope(async {
            state
                .transition(OperationCommand::Request(place()), &queue())
                .unwrap()
        })
        .await;
        assert!(matches!(
            events.as_slice(),
            [OperationEvent::Requested { .. }]
        ));
    }

    #[test]
    fn matching_confirm_settles_and_evolves_to_confirmed() {
        let job_id = JobId::new();
        let state = Operation::<PlaceOrder>::Requested { job_id };

        let events = state
            .transition(
                OperationCommand::Confirm {
                    job_id,
                    output: "filled".to_string(),
                    attempts: 2,
                },
                &queue(),
            )
            .unwrap();
        assert_eq!(
            events,
            vec![OperationEvent::Confirmed {
                job_id,
                output: "filled".to_string(),
                attempts: 2,
            }]
        );

        let settled = state.evolve(&events[0]).unwrap();
        assert_eq!(
            settled,
            Operation::Confirmed {
                job_id,
                output: "filled".to_string(),
                attempts: 2,
            }
        );
    }

    #[test]
    fn matching_fail_settles_and_evolves_to_failed() {
        let job_id = JobId::new();
        let state = Operation::<PlaceOrder>::Requested { job_id };
        let failure = OperationFailure::Rejected(OrderRejected {
            reason: "no funds".to_string(),
        });

        let events = state
            .transition(
                OperationCommand::Fail {
                    job_id,
                    failure: failure.clone(),
                    attempts: 1,
                },
                &queue(),
            )
            .unwrap();
        let settled = state.evolve(&events[0]).unwrap();
        assert_eq!(
            settled,
            Operation::Failed {
                job_id,
                failure,
                attempts: 1,
            }
        );
    }

    #[test]
    fn duplicate_outcome_delivery_is_absorbed_without_events() {
        let job_id = JobId::new();
        let state = Operation::<PlaceOrder>::Confirmed {
            job_id,
            output: "filled".to_string(),
            attempts: 1,
        };

        let events = state
            .transition(
                OperationCommand::Confirm {
                    job_id,
                    output: "filled".to_string(),
                    attempts: 1,
                },
                &queue(),
            )
            .unwrap();
        assert_eq!(events, vec![]);
    }

    #[test]
    fn outcome_for_a_different_job_is_refused() {
        let state = Operation::<PlaceOrder>::Requested {
            job_id: JobId::new(),
        };
        let refused = state
            .transition(
                OperationCommand::Confirm {
                    job_id: JobId::new(),
                    output: "filled".to_string(),
                    attempts: 1,
                },
                &queue(),
            )
            .unwrap_err();
        assert_eq!(refused, OperationRefused::OutcomeMismatch);
    }

    #[test]
    fn outcome_while_idle_is_refused() {
        let refused = Operation::<PlaceOrder>::Idle
            .transition(
                OperationCommand::Fail {
                    job_id: JobId::new(),
                    failure: OperationFailure::DeadLettered {
                        reason: DeadReason::RetriesExhausted,
                        detail: "gave up".to_string(),
                    },
                    attempts: 5,
                },
                &queue(),
            )
            .unwrap_err();
        assert_eq!(refused, OperationRefused::OutcomeMismatch);
    }

    #[test]
    fn replaying_an_outcome_onto_idle_is_a_replay_error() {
        let event = OperationEvent::<PlaceOrder>::Confirmed {
            job_id: JobId::new(),
            output: "filled".to_string(),
            attempts: 1,
        };
        assert_eq!(
            Operation::<PlaceOrder>::Idle.evolve(&event),
            Err(OperationReplay)
        );
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum ProbeVerdict {
        Settled,
        NotSubmitted,
        Indeterminate,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum ProbeSubmit {
        Succeed,
        RejectTerminal,
        FailTransient,
    }

    /// Records which methods ran (via its `Input`), submits and reconciles with
    /// configured behavior, so routing and delivery are observable.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct ProbeJob {
        submit: ProbeSubmit,
        verdict: ProbeVerdict,
    }

    impl OperationJob for ProbeJob {
        type Input = Arc<std::sync::Mutex<Vec<&'static str>>>;
        type Output = String;
        type Error = OrderRejected;
        type Origin = Desk;

        const WORKER_NAME: &'static str = "probe";
        const KIND: &'static str = "probe";

        fn label(&self) -> Label {
            Label::new("probe")
        }

        fn origin_id(&self) -> u64 {
            1
        }

        async fn submit(
            &self,
            _ctx: &JobContext,
            input: &Self::Input,
        ) -> Result<JobOutcome<String>, JobFailure<OrderRejected>> {
            input.lock().unwrap().push("submit");
            match self.submit {
                ProbeSubmit::Succeed => Ok(JobOutcome::Done("submitted".to_string())),
                ProbeSubmit::RejectTerminal => Err(JobFailure::Terminal(OrderRejected {
                    reason: "no funds".to_string(),
                })),
                ProbeSubmit::FailTransient => Err(JobFailure::Transient(OrderRejected {
                    reason: "timeout".to_string(),
                })),
            }
        }

        async fn reconcile(
            &self,
            _ctx: &JobContext,
            input: &Self::Input,
        ) -> Result<Reconciliation<String>, JobFailure<OrderRejected>> {
            input.lock().unwrap().push("reconcile");
            match self.verdict {
                ProbeVerdict::Settled => Ok(Reconciliation::Settled("reconciled".to_string())),
                ProbeVerdict::NotSubmitted => Ok(Reconciliation::NotSubmitted),
                ProbeVerdict::Indeterminate => {
                    Ok(Reconciliation::Indeterminate(Duration::from_secs(60)))
                }
            }
        }
    }

    /// Test [`OriginPort`]: records deliveries, optionally failing them.
    #[derive(Default)]
    struct RecordingPort {
        delivered: std::sync::Mutex<Vec<(u64, OperationCommand<ProbeJob>)>>,
        unavailable: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl OriginPort<ProbeJob> for RecordingPort {
        async fn deliver(
            &self,
            id: &u64,
            command: OperationCommand<ProbeJob>,
        ) -> Result<(), OriginDeliveryError> {
            if self.unavailable.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(OriginDeliveryError("origin unavailable".into()));
            }
            self.delivered.lock().unwrap().push((*id, command));
            Ok(())
        }
    }

    struct ProbeRun {
        calls: Arc<std::sync::Mutex<Vec<&'static str>>>,
        port: Arc<RecordingPort>,
        input: OperationInput<ProbeJob>,
    }

    fn probe_run() -> ProbeRun {
        let calls = Arc::new(std::sync::Mutex::new(vec![]));
        let port = Arc::new(RecordingPort::default());
        let input = OperationInput::new(calls.clone(), port.clone());
        ProbeRun { calls, port, input }
    }

    fn probe(submit: ProbeSubmit, verdict: ProbeVerdict) -> ProbeJob {
        ProbeJob { submit, verdict }
    }

    fn ctx_with_claim_seq(claim_seq: i64) -> JobContext {
        JobContext {
            claim_seq,
            ..JobContext::default()
        }
    }

    #[tokio::test]
    async fn first_execution_routes_to_submit_and_delivers_confirm() {
        let run = probe_run();
        let job = probe(ProbeSubmit::Succeed, ProbeVerdict::Settled);
        let ctx = ctx_with_claim_seq(2);

        let outcome = Job::perform(&job, &ctx, &run.input).await.unwrap();

        assert_eq!(*run.calls.lock().unwrap(), ["submit"]);
        assert!(matches!(outcome, JobOutcome::Done(())));
        assert_eq!(
            *run.port.delivered.lock().unwrap(),
            vec![(
                1,
                OperationCommand::Confirm {
                    job_id: ctx.job_id(),
                    output: "submitted".to_string(),
                    attempts: 1,
                }
            )]
        );
    }

    #[tokio::test]
    async fn later_execution_routes_to_reconcile_and_delivers_its_verdict() {
        let run = probe_run();
        let job = probe(ProbeSubmit::Succeed, ProbeVerdict::Settled);

        let outcome = Job::perform(&job, &ctx_with_claim_seq(3), &run.input)
            .await
            .unwrap();

        assert_eq!(*run.calls.lock().unwrap(), ["reconcile"]);
        assert!(matches!(outcome, JobOutcome::Done(())));
        assert!(matches!(
            run.port.delivered.lock().unwrap().as_slice(),
            [(1, OperationCommand::Confirm { output, .. })] if output == "reconciled"
        ));
    }

    #[tokio::test]
    async fn reconcile_not_submitted_authorizes_a_resubmit() {
        let run = probe_run();
        let job = probe(ProbeSubmit::Succeed, ProbeVerdict::NotSubmitted);

        let outcome = Job::perform(&job, &ctx_with_claim_seq(4), &run.input)
            .await
            .unwrap();

        assert_eq!(*run.calls.lock().unwrap(), ["reconcile", "submit"]);
        assert!(matches!(outcome, JobOutcome::Done(())));
    }

    #[tokio::test]
    async fn reconcile_indeterminate_defers_without_submitting_or_delivering() {
        let run = probe_run();
        let job = probe(ProbeSubmit::Succeed, ProbeVerdict::Indeterminate);

        let outcome = Job::perform(&job, &ctx_with_claim_seq(3), &run.input)
            .await
            .unwrap();

        assert_eq!(*run.calls.lock().unwrap(), ["reconcile"]);
        assert!(matches!(outcome, JobOutcome::Defer(_)));
        assert!(run.port.delivered.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn terminal_failure_delivers_fail_before_dead_lettering() {
        let run = probe_run();
        let job = probe(ProbeSubmit::RejectTerminal, ProbeVerdict::Settled);
        let ctx = ctx_with_claim_seq(2);

        let failure = Job::perform(&job, &ctx, &run.input).await.unwrap_err();

        assert!(matches!(failure, JobFailure::Terminal(_)));
        assert_eq!(
            *run.port.delivered.lock().unwrap(),
            vec![(
                1,
                OperationCommand::Fail {
                    job_id: ctx.job_id(),
                    failure: OperationFailure::Rejected(OrderRejected {
                        reason: "no funds".to_string(),
                    }),
                    attempts: 1,
                }
            )]
        );
    }

    #[tokio::test]
    async fn failed_delivery_defers_instead_of_counting_an_attempt() {
        let run = probe_run();
        run.port
            .unavailable
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let job = probe(ProbeSubmit::Succeed, ProbeVerdict::Settled);

        let outcome = Job::perform(&job, &ctx_with_claim_seq(2), &run.input)
            .await
            .unwrap();

        assert!(matches!(outcome, JobOutcome::Defer(_)));
    }

    #[tokio::test]
    async fn final_transient_attempt_delivers_the_dead_letter_verdict() {
        let run = probe_run();
        let job = probe(ProbeSubmit::FailTransient, ProbeVerdict::NotSubmitted);
        let ctx = JobContext {
            claim_seq: 9,
            attempt: 4,
            max_attempts: 5,
            ..JobContext::default()
        };

        let failure = Job::perform(&job, &ctx, &run.input).await.unwrap_err();

        assert!(matches!(failure, JobFailure::Transient(_)));
        assert!(matches!(
            run.port.delivered.lock().unwrap().as_slice(),
            [(
                1,
                OperationCommand::Fail {
                    failure: OperationFailure::DeadLettered {
                        reason: DeadReason::RetriesExhausted,
                        ..
                    },
                    attempts: 5,
                    ..
                }
            )]
        ));
    }

    #[tokio::test]
    async fn nonfinal_transient_attempt_delivers_nothing() {
        let run = probe_run();
        let job = probe(ProbeSubmit::FailTransient, ProbeVerdict::NotSubmitted);
        let ctx = JobContext {
            claim_seq: 9,
            attempt: 1,
            max_attempts: 5,
            ..JobContext::default()
        };

        let failure = Job::perform(&job, &ctx, &run.input).await.unwrap_err();

        assert!(matches!(failure, JobFailure::Transient(_)));
        assert!(run.port.delivered.lock().unwrap().is_empty());
    }

    #[test]
    fn operation_state_round_trips_through_serde() {
        let state = Operation::<PlaceOrder>::Failed {
            job_id: JobId::new(),
            failure: OperationFailure::DeadLettered {
                reason: DeadReason::Abandoned,
                detail: "claim budget exhausted".to_string(),
            },
            attempts: 3,
        };
        let encoded = serde_json::to_string(&state).unwrap();
        let decoded: Operation<PlaceOrder> = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, state);
    }
}
