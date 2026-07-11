//! Entity-dispatched durable jobs (ADR-0008, ADR-0009).
//!
//! A command is the operation being performed; if it needs the outside world,
//! it kicks off a **job** for a worker. The coupling is enforced by the
//! handler signature: `initialize`/`transition` return an [`Effect`] -- either
//! domain events, or exactly one [`JobDispatch`]. The framework (never the
//! handler) emits the `Dispatched` event and enqueues the job in the same
//! transaction, and only the framework's delivery path can construct the
//! `Confirmed`/`Failed` verdicts -- so [`DispatchedJob::Confirmed`] in entity
//! state is a proof the job settled, not a claim.
//!
//! The driving job implements [`Job`], not [`StandaloneJob`]'s `perform`: the
//! framework routes the first execution to [`submit`](Job::submit) and every
//! later one to [`reconcile`](Job::reconcile), so a follow-up can never
//! blindly resubmit a call whose fate is unknown.

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, warn};

use crate::job::{
    DeadReason, JobContext, JobFailure, JobId, JobOutcome, Label, PendingPush, StandaloneJob,
};
use crate::job_backend::error_chain;
use crate::job_store::EventBackend;
use crate::{EventSourced, Store};

/// A durable job an entity kicks off for a worker (ADR-0008/0009).
///
/// Unlike a [`StandaloneJob`], a `Job` has an [`Origin`](Self::Origin) entity:
/// it is dispatched from a command handler (via [`DispatchedJob::dispatch`]),
/// and its settled verdict is delivered back to the origin as a
/// [`DispatchOutcome`] before the job acks.
///
/// It does not implement `perform`: the framework routes the FIRST execution
/// to [`submit`](Self::submit) and every later execution (retry, lease-expired
/// reclaim, crash mid-call) to [`reconcile`](Self::reconcile). The routing is
/// driven by the durable claim history on the job aggregate and
/// over-approximates submissions in the safe direction -- an execution can
/// never be routed to `submit` while a prior submission might exist.
pub trait Job:
    Clone + Debug + PartialEq + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// Dependency bundle injected into [`submit`](Self::submit) and
    /// [`reconcile`](Self::reconcile).
    type Input: Send + Sync + 'static;

    /// Value produced when the job settles successfully. Lives inside the
    /// origin entity's events and state, so it must be event-grade data.
    type Output: Clone + Debug + PartialEq + Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Domain error produced when the job is definitively rejected. Lives
    /// inside the origin entity's events and state.
    type Error: std::error::Error
        + Clone
        + Debug
        + PartialEq
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static;

    /// The entity whose [`DispatchedJob<Self>`] field this job settles.
    type Origin: EventSourced;

    /// Worker name prefix; the registered worker name is
    /// `format!("{WORKER_NAME}-{index}")`.
    const WORKER_NAME: &'static str;

    /// Stable identifier for this job kind, recorded on the `Enqueued` event
    /// and used to route a job to the worker that runs it.
    const KIND: &'static str;

    /// Human-readable label for structured logging.
    fn label(&self) -> Label;

    /// Identity of the origin entity whose dispatch this job settles. Carried
    /// by the job payload itself, so verdict delivery needs no side lookup.
    fn origin_id(&self) -> <Self::Origin as EventSourced>::Id;

    /// Make the external call for the FIRST time. Only ever invoked when no
    /// prior execution of this job can have reached the external system.
    ///
    /// Return [`JobOutcome::Done`] when the outcome is known immediately, or
    /// [`JobOutcome::Defer`] when the call was submitted and the outcome must
    /// be polled ([`reconcile`](Self::reconcile) runs next). Derive the
    /// external-boundary idempotency key from [`JobContext::job_id`], never
    /// the attempt number.
    fn submit(
        &self,
        ctx: &JobContext,
        input: &Self::Input,
    ) -> impl Future<Output = Result<JobOutcome<Self::Output>, JobFailure<Self::Error>>> + Send;

    /// Determine the fate of an earlier execution -- typically by querying the
    /// external system with the idempotency key. Invoked instead of
    /// [`submit`](Self::submit) whenever a prior execution might have reached
    /// the external system.
    fn reconcile(
        &self,
        ctx: &JobContext,
        input: &Self::Input,
    ) -> impl Future<Output = Result<Reconciliation<Self::Output>, JobFailure<Self::Error>>> + Send;
}

/// The verdict of [`Job::reconcile`]: what happened to the earlier execution
/// whose fate was unknown.
#[derive(Debug)]
pub enum Reconciliation<Output> {
    /// The earlier submission landed; this is its outcome.
    Settled(Output),
    /// The earlier execution provably never reached the external system; the
    /// framework authorizes a fresh [`submit`](Job::submit).
    NotSubmitted,
    /// Cannot tell yet -- re-run reconciliation after this delay, without
    /// counting an attempt (maps onto [`JobOutcome::Defer`]).
    Indeterminate(Duration),
}

/// What a command handler decided (ADR-0009): domain events, or exactly one
/// job kicked off for a worker.
///
/// This is the whole durability contract. There is no other way to enqueue
/// from a handler, and a dispatch carries no free-form events -- the framework
/// emits the `Dispatched` intent record itself, so an accomplished-fact event
/// cannot ride along with a merely-enqueued effect.
pub enum Effect<Entity: EventSourced> {
    /// Pure domain facts; nothing external was requested.
    Events(Vec<Entity::Event>),
    /// One durable job dispatch, obtained from [`DispatchedJob::dispatch`]
    /// (or [`Effect::kickoff`] when the entity has no state yet). The
    /// framework emits the wrapped `Dispatched` event and enqueues the job
    /// in the same transaction.
    Dispatch(JobDispatch<Entity>),
}

impl<Entity: EventSourced> Effect<Entity> {
    /// Kick off `job` from a handler with no prior dispatch to guard --
    /// an `initialize` handler, where the entity does not exist yet. The
    /// [`Idle`](DispatchedJob::Idle) machine cannot refuse, so this form is
    /// infallible; from `transition`, dispatch through the entity's state
    /// (`self.field.dispatch(job)?`) so an in-flight or confirmed job
    /// refuses the overlap.
    ///
    /// Carries the same wiring proof as [`DispatchedJob::dispatch`]: the
    /// job's origin is this entity, the entity's event enum absorbs
    /// [`DispatchEvent<J>`], and the job is declared in the entity's
    /// [`jobs!`](crate::jobs) list.
    pub fn kickoff<J, Index>(job: J) -> Self
    where
        J: Job<Origin = Entity>,
        Entity::Event: From<DispatchEvent<J>>,
        Entity::Jobs: Contains<J, Index>,
    {
        Self::Dispatch(kick(job))
    }
}

/// Wraps a handler-outcome value into the full handler result, so every match
/// arm reads uniformly regardless of what it returns:
///
/// ```ignore
/// match command {
///     Initialize { .. } => fx(InventoryError::AlreadyInitialized),
///     Restock { added } => fx(Restocked { added }),
///     Place { .. } => fx(SendOrderConfirmation { .. }),          // kick-off
///     Settle(outcome) => fx(self.confirmation.settle(outcome)?), // events
/// }
/// ```
///
/// Accepts anything implementing [`Fx`]: a single event, a `Vec`/array of
/// events, a [`Job`] (the infallible [`Effect::kickoff`] -- initialize-side),
/// a guarded [`JobDispatch`], or the entity's domain error (which becomes the
/// `Err` arm).
pub fn fx<Entity, Marker, Value>(value: Value) -> Result<Effect<Entity>, Entity::Error>
where
    Entity: EventSourced,
    Value: Fx<Entity, Marker>,
{
    value.produce()
}

/// The no-op handler outcome: accept the command, record nothing.
///
/// The empty counterpart of [`fx`] -- a bare `fx(vec![])` cannot infer its
/// element type (an empty event list and an empty settlement list are
/// indistinguishable), so the empty outcome gets its own name.
pub fn uneventful<Entity: EventSourced>() -> Result<Effect<Entity>, Entity::Error> {
    Ok(Effect::Events(vec![]))
}

/// A value a command handler can return as its entire outcome.
///
/// Domain events, a job kick-off, a guarded [`JobDispatch`], or the entity's
/// domain error -- wrapped by [`fx`] into the handler's
/// `Result<Effect, Error>`. `Marker` exists only to keep the blanket impls
/// coherent; the compiler infers it at every [`fx`] call and consumers never
/// name it.
pub trait Fx<Entity: EventSourced, Marker> {
    /// The handler result this value stands for.
    fn produce(self) -> Result<Effect<Entity>, Entity::Error>;
}

/// Inference markers for the [`Fx`] blanket impls; never named by consumers.
#[doc(hidden)]
pub mod fx_marker {
    use std::marker::PhantomData;

    pub struct Event;
    pub struct Events;
    pub struct Kickoff<Index>(PhantomData<Index>);
    pub struct Settlement<Index>(PhantomData<Index>);
    pub struct Guarded;
    pub struct Failure;
}

impl<Entity, Ev> Fx<Entity, fx_marker::Event> for Ev
where
    Entity: EventSourced<Event = Ev>,
{
    fn produce(self) -> Result<Effect<Entity>, Entity::Error> {
        Ok(Effect::Events(vec![self]))
    }
}

impl<Entity: EventSourced> Fx<Entity, fx_marker::Events> for Vec<Entity::Event> {
    fn produce(self) -> Result<Effect<Entity>, Entity::Error> {
        Ok(Effect::Events(self))
    }
}

impl<Entity: EventSourced, const N: usize> Fx<Entity, fx_marker::Events> for [Entity::Event; N] {
    fn produce(self) -> Result<Effect<Entity>, Entity::Error> {
        Ok(Effect::Events(self.into()))
    }
}

impl<J, Index> Fx<J::Origin, fx_marker::Settlement<Index>> for Vec<DispatchEvent<J>>
where
    J: Job,
    <J::Origin as EventSourced>::Event: From<DispatchEvent<J>>,
    <J::Origin as EventSourced>::Jobs: Contains<J, Index>,
{
    fn produce(self) -> Result<Effect<J::Origin>, <J::Origin as EventSourced>::Error> {
        Ok(Effect::Events(self.into_iter().map(Into::into).collect()))
    }
}

impl<J, Index> Fx<J::Origin, fx_marker::Kickoff<Index>> for J
where
    J: Job,
    <J::Origin as EventSourced>::Event: From<DispatchEvent<J>>,
    <J::Origin as EventSourced>::Jobs: Contains<J, Index>,
{
    fn produce(self) -> Result<Effect<J::Origin>, <J::Origin as EventSourced>::Error> {
        Ok(Effect::kickoff(self))
    }
}

impl<Entity: EventSourced> Fx<Entity, fx_marker::Guarded> for JobDispatch<Entity> {
    fn produce(self) -> Result<Effect<Entity>, Entity::Error> {
        Ok(Effect::Dispatch(self))
    }
}

impl<Entity, Error> Fx<Entity, fx_marker::Failure> for Error
where
    Entity: EventSourced<Error = Error>,
{
    fn produce(self) -> Result<Effect<Entity>, Error> {
        Err(self)
    }
}

/// The unguarded kick-off construction shared by [`Effect::kickoff`] and the
/// state-guarded [`DispatchedJob::dispatch`].
fn kick<J: Job>(job: J) -> JobDispatch<J::Origin>
where
    <J::Origin as EventSourced>::Event: From<DispatchEvent<J>>,
{
    let job_id = JobId::new();
    let event = DispatchEvent::Dispatched {
        job_id,
        job: job.clone(),
    };
    JobDispatch {
        event: event.into(),
        pending: PendingPush {
            job_id,
            job: Box::new(job),
            delay: None,
        },
    }
}

/// A wiring-proven job kick-off.
///
/// Holds the wrapped `Dispatched` event plus the pending enqueue, produced
/// only by [`DispatchedJob::dispatch`]. Opaque -- handlers return it inside
/// [`Effect::Dispatch`]; the framework takes it apart.
pub struct JobDispatch<Entity: EventSourced> {
    pub(crate) event: Entity::Event,
    pub(crate) pending: PendingPush,
}

/// The entity-embedded state of one dispatched job: the durable lifecycle of
/// a fallible external action from the origin's perspective.
///
/// Folded from [`DispatchEvent`]s by [`evolve`](Self::evolve). New dispatches
/// are guarded by [`dispatch`](Self::dispatch); delivered verdicts are folded
/// in by [`settle`](Self::settle). The default state is [`Idle`](Self::Idle).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum DispatchedJob<J: Job> {
    /// Nothing dispatched (or the previous dispatch was superseded).
    #[default]
    Idle,
    /// A job is in flight; its verdict has not landed yet.
    InFlight {
        /// The driving job, for verdict correlation and audit.
        job_id: JobId,
    },
    /// The job settled successfully. Constructible only through the
    /// framework's delivery path -- this state is a proof, not a claim.
    Confirmed(Settled<J::Output>),
    /// The job settled as definitively failed.
    Failed(SettledFailure<J::Error>),
}

/// An event on the origin's stream recording one hop of a dispatched job's
/// lifecycle. Nested by the consumer in their entity's event enum (one
/// variant per dispatched-job field).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum DispatchEvent<J: Job> {
    /// The job was kicked off; this event carries the full intent, and the
    /// enqueue committed in the same transaction.
    Dispatched { job_id: JobId, job: J },
    /// The job settled successfully (sealed: framework-constructed only).
    Confirmed(Settled<J::Output>),
    /// The job settled as definitively failed (sealed).
    Failed(SettledFailure<J::Error>),
}

/// A settled success: the job's output plus its durable identity.
///
/// Fields are private -- only the framework's delivery path constructs one,
/// so its presence in an event or state proves the job ran and settled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settled<Output> {
    pub(crate) job_id: JobId,
    pub(crate) output: Output,
    pub(crate) attempts: u32,
}

impl<Output> Settled<Output> {
    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn output(&self) -> &Output {
        &self.output
    }

    /// How many executions the job took to settle.
    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Test-only: fabricate a settled success for given-history tests.
    /// Production code cannot construct one -- that is the seal.
    #[cfg(any(test, feature = "test-support"))]
    pub fn simulated(job_id: JobId, output: Output, attempts: u32) -> Self {
        Self {
            job_id,
            output,
            attempts,
        }
    }
}

/// A settled failure: why the job definitively failed, plus its durable
/// identity. Fields are private -- framework-constructed only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettledFailure<DomainError> {
    pub(crate) job_id: JobId,
    pub(crate) failure: DispatchFailure<DomainError>,
    pub(crate) attempts: u32,
}

impl<DomainError> SettledFailure<DomainError> {
    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    pub fn failure(&self) -> &DispatchFailure<DomainError> {
        &self.failure
    }

    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Test-only: fabricate a settled failure for given-history tests.
    #[cfg(any(test, feature = "test-support"))]
    pub fn simulated(job_id: JobId, failure: DispatchFailure<DomainError>, attempts: u32) -> Self {
        Self {
            job_id,
            failure,
            attempts,
        }
    }
}

/// Why a dispatched job settled as [`DispatchedJob::Failed`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DispatchFailure<DomainError> {
    /// The external system definitively rejected the job
    /// ([`JobFailure::Terminal`]).
    Rejected(DomainError),
    /// The job dead-lettered without a definitive answer (retries exhausted,
    /// claim budget, undecodable payload); `detail` is the dead-letter error
    /// chain. The dispatch needs operator attention.
    DeadLettered { reason: DeadReason, detail: String },
}

/// A settled verdict delivered by the framework onto the origin entity.
///
/// Arrives as a command: opaque and framework-constructed only. The
/// consumer's command enum absorbs it via `From`, and the handler folds it
/// with [`DispatchedJob::settle`].
#[derive(Debug, Clone)]
pub struct DispatchOutcome<J: Job>(pub(crate) Verdict<J>);

#[derive(Debug, Clone)]
pub(crate) enum Verdict<J: Job> {
    Confirmed(Settled<J::Output>),
    Failed(SettledFailure<J::Error>),
}

impl<J: Job> DispatchOutcome<J> {
    pub(crate) fn confirmed(job_id: JobId, output: J::Output, attempts: u32) -> Self {
        Self(Verdict::Confirmed(Settled {
            job_id,
            output,
            attempts,
        }))
    }

    pub(crate) fn failed(job_id: JobId, failure: DispatchFailure<J::Error>, attempts: u32) -> Self {
        Self(Verdict::Failed(SettledFailure {
            job_id,
            failure,
            attempts,
        }))
    }

    /// Test-only: fabricate the confirmed verdict the framework would deliver.
    /// Production code cannot construct one -- that is the seal.
    #[cfg(any(test, feature = "test-support"))]
    pub fn simulated_confirmed(job_id: JobId, output: J::Output, attempts: u32) -> Self {
        Self::confirmed(job_id, output, attempts)
    }

    /// Test-only: fabricate the failed verdict the framework would deliver.
    #[cfg(any(test, feature = "test-support"))]
    pub fn simulated_failed(
        job_id: JobId,
        failure: DispatchFailure<J::Error>,
        attempts: u32,
    ) -> Self {
        Self::failed(job_id, failure, attempts)
    }
}

/// A command the dispatch guard refused. Map into the entity's domain error
/// with `#[from]`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum DispatchRefused {
    /// A dispatch was attempted while a job is already in flight.
    #[error("a job is already in flight for this dispatch")]
    InFlight,
    /// A dispatch was attempted after the previous job confirmed.
    #[error("the dispatch already confirmed")]
    AlreadyConfirmed,
    /// A delivered verdict does not correspond to this dispatch's state
    /// (wrong job id, nothing in flight, or contradicting an already-settled
    /// verdict).
    #[error("verdict does not correspond to the dispatch's state")]
    OutcomeMismatch,
}

/// Replayed a [`DispatchEvent`] that cannot follow from the current state --
/// stream corruption or a schema bug, never a normal outcome.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("dispatch event cannot be applied to the current dispatch state")]
pub struct DispatchReplay;

impl<J: Job> DispatchedJob<J> {
    /// Kick off `job`: the guarded, wiring-proven construction of a
    /// [`JobDispatch`] to return inside [`Effect::Dispatch`].
    ///
    /// Refuses while a job is in flight or after a confirmation; a failed
    /// dispatch may be retried with a fresh job. The bounds prove at the
    /// dispatch site everything delivery needs: the job's origin is this
    /// entity, the entity's event enum absorbs [`DispatchEvent<J>`], and the
    /// job kind is declared in the entity's [`jobs!`](crate::jobs) list.
    pub fn dispatch<Index>(&self, job: J) -> Result<JobDispatch<J::Origin>, DispatchRefused>
    where
        <J::Origin as EventSourced>::Event: From<DispatchEvent<J>>,
        <J::Origin as EventSourced>::Jobs: Contains<J, Index>,
    {
        match self {
            Self::Idle | Self::Failed(_) => Ok(kick(job)),
            Self::InFlight { .. } => Err(DispatchRefused::InFlight),
            Self::Confirmed(_) => Err(DispatchRefused::AlreadyConfirmed),
        }
    }

    /// Folds a delivered [`DispatchOutcome`] into the events it justifies.
    /// Call from the entity's `transition` handler and return them as
    /// [`Effect::Events`].
    ///
    /// Duplicate delivery (the at-least-once ack path re-delivering a settled
    /// verdict) is absorbed: it produces no events and no error.
    pub fn settle(
        &self,
        outcome: DispatchOutcome<J>,
    ) -> Result<Vec<DispatchEvent<J>>, DispatchRefused> {
        let DispatchOutcome(verdict) = outcome;
        match (self, verdict) {
            (Self::InFlight { job_id }, Verdict::Confirmed(settled)) => {
                if *job_id == settled.job_id {
                    Ok(vec![DispatchEvent::Confirmed(settled)])
                } else {
                    Err(DispatchRefused::OutcomeMismatch)
                }
            }
            (Self::InFlight { job_id }, Verdict::Failed(settled)) => {
                if *job_id == settled.job_id {
                    Ok(vec![DispatchEvent::Failed(settled)])
                } else {
                    Err(DispatchRefused::OutcomeMismatch)
                }
            }
            (Self::Confirmed(existing), Verdict::Confirmed(settled)) => {
                if existing.job_id == settled.job_id {
                    Ok(vec![])
                } else {
                    Err(DispatchRefused::OutcomeMismatch)
                }
            }
            (Self::Failed(existing), Verdict::Failed(settled)) => {
                if existing.job_id == settled.job_id {
                    Ok(vec![])
                } else {
                    Err(DispatchRefused::OutcomeMismatch)
                }
            }
            (Self::Idle, Verdict::Confirmed(_) | Verdict::Failed(_))
            | (Self::Confirmed(_), Verdict::Failed(_))
            | (Self::Failed(_), Verdict::Confirmed(_)) => Err(DispatchRefused::OutcomeMismatch),
        }
    }

    /// Folds the FIRST [`DispatchEvent`] of a fresh entity into a machine.
    /// Call from the entity's `originate` -- it mirrors
    /// [`EventSourced::originate`] the way [`evolve`](Self::evolve) mirrors
    /// [`EventSourced::evolve`].
    pub fn originate(event: &DispatchEvent<J>) -> Result<Self, DispatchReplay> {
        Self::Idle.evolve(event)
    }

    /// Folds a [`DispatchEvent`] into the next state. Call from the entity's
    /// `evolve`.
    pub fn evolve(&self, event: &DispatchEvent<J>) -> Result<Self, DispatchReplay> {
        match (self, event) {
            (Self::Idle | Self::Failed(_), DispatchEvent::Dispatched { job_id, .. }) => {
                Ok(Self::InFlight { job_id: *job_id })
            }
            (Self::InFlight { job_id }, DispatchEvent::Confirmed(settled))
                if *job_id == settled.job_id =>
            {
                Ok(Self::Confirmed(settled.clone()))
            }
            (Self::InFlight { job_id }, DispatchEvent::Failed(settled))
                if *job_id == settled.job_id =>
            {
                Ok(Self::Failed(settled.clone()))
            }
            (
                Self::Idle | Self::InFlight { .. } | Self::Confirmed(_) | Self::Failed(_),
                DispatchEvent::Dispatched { .. }
                | DispatchEvent::Confirmed(_)
                | DispatchEvent::Failed(_),
            ) => Err(DispatchReplay),
        }
    }
}

/// How settled verdicts reach the origin entity.
///
/// Object-safe so the worker input stays backend-agnostic; [`Store`]
/// implements it whenever the origin's command enum absorbs
/// [`DispatchOutcome<J>`] via `From`.
#[async_trait]
pub trait OriginPort<J: Job>: Send + Sync {
    /// Deliver a settled verdict as a command on the origin entity.
    async fn deliver(
        &self,
        id: &<J::Origin as EventSourced>::Id,
        outcome: DispatchOutcome<J>,
    ) -> Result<(), OriginDeliveryError>;
}

/// Delivering a settled verdict to the origin entity failed. The worker
/// defers and retries -- delivery failures never count as job attempts,
/// because the job itself already settled.
#[derive(Debug, thiserror::Error)]
pub enum OriginDeliveryError {
    /// The origin could not be reached or written (database contention,
    /// connection loss). Retrying the delivery is expected to succeed.
    #[error("verdict delivery to the origin entity failed")]
    Transport(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The origin's domain logic refused the verdict (a [`DispatchRefused`]
    /// mapped into its error type) -- a wiring or correlation bug that no
    /// retry can fix. The worker backs off on a long delay and logs for the
    /// operator instead of hot-looping.
    #[error("the origin entity refused the delivered verdict")]
    Refused(#[source] Box<dyn std::error::Error + Send + Sync>),
}

#[async_trait]
impl<J, Backend> OriginPort<J> for Store<J::Origin, Backend>
where
    J: Job,
    Backend: EventBackend,
    <J::Origin as EventSourced>::Command: From<DispatchOutcome<J>>,
    <J::Origin as EventSourced>::Event: Clone + Debug + Serialize + DeserializeOwned + Send + Sync,
{
    async fn deliver(
        &self,
        id: &<J::Origin as EventSourced>::Id,
        outcome: DispatchOutcome<J>,
    ) -> Result<(), OriginDeliveryError> {
        self.send(id, outcome.into())
            .await
            .map_err(|send_error| match send_error {
                crate::AggregateError::UserError(_) => {
                    OriginDeliveryError::Refused(Box::new(send_error))
                }
                crate::AggregateError::AggregateConflict
                | crate::AggregateError::DatabaseConnectionError(_)
                | crate::AggregateError::DeserializationError(_)
                | crate::AggregateError::UnexpectedError(_) => {
                    OriginDeliveryError::Transport(Box::new(send_error))
                }
            })
    }
}

/// Worker-side dependency bundle for a [`Job`]: the consumer's input plus the
/// port verdicts are delivered through.
///
/// Register it as the job's input in `build_supervised_worker!`:
/// `ChargeCard => JobInput::<ChargeCard>::new(gateway, store.clone())`.
pub struct JobInput<J: Job> {
    input: J::Input,
    origin: Arc<dyn OriginPort<J>>,
}

impl<J: Job> JobInput<J> {
    /// Bundles the job's dependencies with the origin entity's delivery port
    /// (usually the origin's `Arc<Store>`).
    pub fn new<Port: OriginPort<J> + 'static>(input: J::Input, origin: Arc<Port>) -> Self {
        Self {
            input,
            origin: origin as Arc<dyn OriginPort<J>>,
        }
    }
}

/// How long a worker defers after a failed verdict delivery, per failure
/// class. Set on [`JobWorkerConfig`](crate::JobWorkerConfig); deferrals ride
/// [`JobOutcome::Defer`], so they never count attempts.
#[derive(Clone, Copy, Debug)]
pub struct DeliveryPolicy {
    /// Snooze before retrying a transport-failed delivery (database
    /// contention, connection loss) -- retrying is expected to succeed.
    pub retry_delay: Duration,
    /// Backoff after the origin REFUSED a verdict -- a wiring/correlation bug
    /// retries cannot fix, kept slow and loud rather than hot-looping while
    /// the operator investigates.
    pub refused_backoff: Duration,
}

impl Default for DeliveryPolicy {
    fn default() -> Self {
        Self {
            retry_delay: Duration::from_secs(5),
            refused_backoff: Duration::from_secs(300),
        }
    }
}

fn delivery_deferral(ctx: &JobContext, delivery_error: &OriginDeliveryError) -> Duration {
    let job_id = ctx.job_id();
    match delivery_error {
        OriginDeliveryError::Transport(_) => {
            warn!(
                target: "cqrs", ?delivery_error, %job_id,
                "settled verdict could not be delivered; deferring"
            );
            ctx.delivery.retry_delay
        }
        OriginDeliveryError::Refused(_) => {
            error!(
                target: "cqrs", ?delivery_error, %job_id,
                "VERDICT REFUSED: the origin entity rejected a settled verdict; \
                 backing off -- operator intervention needed"
            );
            ctx.delivery.refused_backoff
        }
    }
}

/// Every [`Job`] is a [`StandaloneJob`] whose execution is the
/// submit/reconcile routing plus verdict delivery (ADR-0008/0009).
///
/// The first execution runs [`submit`](Job::submit); every later one runs
/// [`reconcile`](Job::reconcile) first and only re-submits on an explicit
/// [`Reconciliation::NotSubmitted`]. A settled verdict is delivered to the
/// origin entity BEFORE the job acks: failed delivery defers the job (no
/// attempt counted -- the job itself settled), so the verdict is re-derived
/// and re-delivered until the origin accepts it, and duplicates are absorbed
/// by the [`DispatchedJob`] guard. A transient failure that exhausts the
/// retry budget best-effort delivers a dead-letter verdict before the worker
/// dead-letters; if even that delivery fails, the worker logs loudly and
/// dead-letters anyway, leaving the origin in flight.
impl<J: Job> StandaloneJob for J {
    type Input = JobInput<J>;
    type Output = ();
    type Error = J::Error;

    const WORKER_NAME: &'static str = <J as Job>::WORKER_NAME;
    const KIND: &'static str = <J as Job>::KIND;

    fn label(&self) -> Label {
        Job::label(self)
    }

    async fn perform(
        &self,
        ctx: &JobContext,
        wrapped: &JobInput<J>,
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
                let confirm = DispatchOutcome::confirmed(ctx.job_id(), output, attempts);
                match wrapped.origin.deliver(&self.origin_id(), confirm).await {
                    Ok(()) => Ok(JobOutcome::Done(())),
                    Err(delivery_error) => {
                        Ok(JobOutcome::Defer(delivery_deferral(ctx, &delivery_error)))
                    }
                }
            }
            Ok(JobOutcome::Defer(delay)) => Ok(JobOutcome::Defer(delay)),
            Err(JobFailure::Terminal(domain_error)) => {
                let fail = DispatchOutcome::failed(
                    ctx.job_id(),
                    DispatchFailure::Rejected(domain_error.clone()),
                    attempts,
                );
                match wrapped.origin.deliver(&self.origin_id(), fail).await {
                    Ok(()) => Err(JobFailure::Terminal(domain_error)),
                    Err(delivery_error) => {
                        Ok(JobOutcome::Defer(delivery_deferral(ctx, &delivery_error)))
                    }
                }
            }
            Err(JobFailure::Transient(domain_error)) => {
                if ctx.is_final_attempt() {
                    // The worker will dead-letter this job; tell the origin so
                    // its dispatch does not dangle in flight. Best effort: a
                    // failed delivery here is logged loudly for the operator.
                    let fail = DispatchOutcome::failed(
                        ctx.job_id(),
                        DispatchFailure::DeadLettered {
                            reason: DeadReason::RetriesExhausted,
                            detail: error_chain(&domain_error),
                        },
                        attempts,
                    );
                    if let Err(delivery_error) =
                        wrapped.origin.deliver(&self.origin_id(), fail).await
                    {
                        error!(
                            target: "cqrs", ?delivery_error, job_id = %ctx.job_id(),
                            "DISPATCH DANGLING: job dead-letters but the origin \
                             entity could not be told; operator intervention needed"
                        );
                    }
                }
                Err(JobFailure::Transient(domain_error))
            }
        }
    }
}

/// Type-level list of the [`Job`] types an entity may dispatch.
///
/// Built from [`Cons`](crate::Cons)/[`Nil`](crate::Nil) -- write it with the
/// [`jobs!`](crate::jobs) macro (`jobs![ChargeCard, SendReceipt]`).
/// [`DispatchedJob::dispatch`] compile-checks that the dispatched job is a
/// declared member.
pub trait JobList {}

impl JobList for crate::Nil {}

impl<Head: Job, Tail: JobList> JobList for crate::Cons<Head, Tail> {}

/// Compile-time proof that job `J` is a member of a [`JobList`].
///
/// `Index` is an inferred [`Here`]/[`There`] witness that disambiguates the
/// two recursive impls so they don't overlap; call sites never name it.
pub trait Contains<J: Job, Index> {}

/// Membership witness: `J` is the head of the list.
pub struct Here;

/// Membership witness: `J` is `Index` positions into the tail.
pub struct There<Index>(PhantomData<Index>);

impl<J: Job, Tail> Contains<J, Here> for crate::Cons<J, Tail> {}

impl<J: Job, Head, Tail, Index> Contains<J, There<Index>> for crate::Cons<Head, Tail> where
    Tail: Contains<J, Index>
{
}

/// Build a type-level [`JobList`] from job types.
///
/// `jobs![ChargeCard, SendReceipt]` expands to
/// `Cons<ChargeCard, Cons<SendReceipt, Nil>>`; empty `jobs![]` is `Nil`. Use
/// it for [`EventSourced::Jobs`](crate::EventSourced::Jobs).
#[macro_export]
macro_rules! jobs {
    () => { $crate::Nil };
    ($head:ty $(, $tail:ty)* $(,)?) => {
        $crate::Cons<$head, $crate::jobs![$($tail),*]>
    };
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use crate::job::take_pending;
    use crate::{Effect, Nil};

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct PlaceOrder {
        origin: u64,
        amount: u32,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
    #[error("order rejected: {reason}")]
    struct OrderRejected {
        reason: String,
    }

    /// Reference embedded-dispatch entity: one `DispatchedJob<PlaceOrder>`
    /// field, events/commands/errors delegating to the machine.
    #[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
    struct Desk {
        order: DispatchedJob<PlaceOrder>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum DeskEvent {
        Dispatch(DispatchEvent<PlaceOrder>),
    }

    impl From<DispatchEvent<PlaceOrder>> for DeskEvent {
        fn from(event: DispatchEvent<PlaceOrder>) -> Self {
            Self::Dispatch(event)
        }
    }

    #[derive(Debug, Clone)]
    enum DeskCommand {
        Place(PlaceOrder),
        Settle(DispatchOutcome<PlaceOrder>),
    }

    impl From<DispatchOutcome<PlaceOrder>> for DeskCommand {
        fn from(outcome: DispatchOutcome<PlaceOrder>) -> Self {
            Self::Settle(outcome)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
    enum DeskError {
        #[error(transparent)]
        Refused(#[from] DispatchRefused),
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
        type Error = DeskError;
        type Command = DeskCommand;
        type Event = DeskEvent;
        type Materialized = Nil;
        type Jobs = jobs![PlaceOrder];

        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;
        const AGGREGATE_TYPE: &'static str = "Desk";

        fn originate(event: &DeskEvent) -> Option<Self> {
            let DeskEvent::Dispatch(dispatch_event) = event;
            let order = DispatchedJob::originate(dispatch_event).ok()?;
            Some(Self { order })
        }

        fn evolve(entity: &Self, event: &DeskEvent) -> Result<Option<Self>, DeskError> {
            let DeskEvent::Dispatch(dispatch_event) = event;
            Ok(entity
                .order
                .evolve(dispatch_event)
                .ok()
                .map(|order| Self { order }))
        }

        async fn initialize(command: DeskCommand) -> Result<Effect<Self>, DeskError> {
            match command {
                DeskCommand::Place(job) => fx(job),
                DeskCommand::Settle(_) => fx(DeskError::Refused(DispatchRefused::OutcomeMismatch)),
            }
        }

        async fn transition(&self, command: DeskCommand) -> Result<Effect<Self>, DeskError> {
            match command {
                DeskCommand::Place(job) => fx(self.order.dispatch(job)?),
                DeskCommand::Settle(outcome) => fx(self.order.settle(outcome)?),
            }
        }
    }

    impl Job for PlaceOrder {
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

    fn confirmed(job_id: JobId) -> DispatchOutcome<PlaceOrder> {
        DispatchOutcome::confirmed(job_id, "filled".to_string(), 1)
    }

    #[test]
    fn dispatch_from_idle_pairs_the_event_with_the_enqueue() {
        let dispatch = DispatchedJob::<PlaceOrder>::Idle.dispatch(place()).unwrap();

        let DeskEvent::Dispatch(DispatchEvent::Dispatched { job_id, job }) = &dispatch.event else {
            panic!("expected a Dispatched event");
        };
        assert_eq!(dispatch.pending.job_id, *job_id);
        assert_eq!(*job, place());
    }

    #[test]
    fn dispatch_while_in_flight_is_refused() {
        let state = DispatchedJob::<PlaceOrder>::InFlight {
            job_id: JobId::new(),
        };
        let Err(refused) = state.dispatch(place()) else {
            panic!("expected the in-flight guard to refuse");
        };
        assert_eq!(refused, DispatchRefused::InFlight);
    }

    #[test]
    fn dispatch_after_confirmation_is_refused() {
        let state = DispatchedJob::<PlaceOrder>::Confirmed(Settled {
            job_id: JobId::new(),
            output: "filled".to_string(),
            attempts: 1,
        });
        let Err(refused) = state.dispatch(place()) else {
            panic!("expected the confirmed guard to refuse");
        };
        assert_eq!(refused, DispatchRefused::AlreadyConfirmed);
    }

    #[test]
    fn dispatch_after_failure_starts_a_fresh_job() {
        let state = DispatchedJob::<PlaceOrder>::Failed(SettledFailure {
            job_id: JobId::new(),
            failure: DispatchFailure::Rejected(OrderRejected {
                reason: "no funds".to_string(),
            }),
            attempts: 1,
        });
        let dispatch = state.dispatch(place()).unwrap();
        assert!(matches!(
            dispatch.event,
            DeskEvent::Dispatch(DispatchEvent::Dispatched { .. })
        ));
    }

    #[test]
    fn matching_confirm_settles_and_evolves_to_confirmed() {
        let job_id = JobId::new();
        let state = DispatchedJob::<PlaceOrder>::InFlight { job_id };

        let events = state.settle(confirmed(job_id)).unwrap();
        let [event] = events.as_slice() else {
            panic!("expected exactly one settle event, got {events:?}");
        };

        let settled = state.evolve(event).unwrap();
        let DispatchedJob::Confirmed(settled) = settled else {
            panic!("expected Confirmed, got {settled:?}");
        };
        assert_eq!(settled.job_id(), job_id);
        assert_eq!(settled.output(), "filled");
        assert_eq!(settled.attempts(), 1);
    }

    #[test]
    fn duplicate_verdict_delivery_is_absorbed_without_events() {
        let job_id = JobId::new();
        let state = DispatchedJob::<PlaceOrder>::Confirmed(Settled {
            job_id,
            output: "filled".to_string(),
            attempts: 1,
        });

        assert_eq!(state.settle(confirmed(job_id)).unwrap(), vec![]);
    }

    #[test]
    fn verdict_for_a_different_job_is_refused() {
        let state = DispatchedJob::<PlaceOrder>::InFlight {
            job_id: JobId::new(),
        };
        assert_eq!(
            state.settle(confirmed(JobId::new())).unwrap_err(),
            DispatchRefused::OutcomeMismatch
        );
    }

    #[test]
    fn verdict_while_idle_is_refused() {
        assert_eq!(
            DispatchedJob::<PlaceOrder>::Idle
                .settle(confirmed(JobId::new()))
                .unwrap_err(),
            DispatchRefused::OutcomeMismatch
        );
    }

    #[test]
    fn replaying_a_verdict_onto_idle_is_a_replay_error() {
        let event = DispatchEvent::<PlaceOrder>::Confirmed(Settled {
            job_id: JobId::new(),
            output: "filled".to_string(),
            attempts: 1,
        });
        assert_eq!(
            DispatchedJob::<PlaceOrder>::Idle.evolve(&event),
            Err(DispatchReplay)
        );
    }

    #[test]
    fn dispatch_state_round_trips_through_serde() {
        let state = DispatchedJob::<PlaceOrder>::Failed(SettledFailure {
            job_id: JobId::new(),
            failure: DispatchFailure::DeadLettered {
                reason: DeadReason::Abandoned,
                detail: "claim budget exhausted".to_string(),
            },
            attempts: 3,
        });
        let encoded = serde_json::to_string(&state).unwrap();
        let decoded: DispatchedJob<PlaceOrder> = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, state);
    }

    /// The kicked-off job never enters the pending buffer unless the
    /// framework puts it there: `dispatch` alone buffers nothing.
    #[tokio::test]
    async fn dispatch_alone_does_not_enqueue() {
        let inspected = crate::job::with_pending_scope(async {
            let _dispatch = DispatchedJob::<PlaceOrder>::Idle.dispatch(place()).unwrap();
            take_pending().unwrap()
        })
        .await;
        assert!(inspected.is_empty());
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

    /// Records which methods ran (via its `Input`), submits and reconciles
    /// with configured behavior, so routing and delivery are observable.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct ProbeJob {
        submit: ProbeSubmit,
        verdict: ProbeVerdict,
    }

    impl Job for ProbeJob {
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
        delivered: std::sync::Mutex<Vec<(u64, DispatchOutcome<ProbeJob>)>>,
        unavailable: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl OriginPort<ProbeJob> for RecordingPort {
        async fn deliver(
            &self,
            id: &u64,
            outcome: DispatchOutcome<ProbeJob>,
        ) -> Result<(), OriginDeliveryError> {
            if self.unavailable.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(OriginDeliveryError::Transport("origin unavailable".into()));
            }
            self.delivered.lock().unwrap().push((*id, outcome));
            Ok(())
        }
    }

    struct ProbeRun {
        calls: Arc<std::sync::Mutex<Vec<&'static str>>>,
        port: Arc<RecordingPort>,
        input: JobInput<ProbeJob>,
    }

    fn probe_run() -> ProbeRun {
        let calls = Arc::new(std::sync::Mutex::new(vec![]));
        let port = Arc::new(RecordingPort::default());
        let input = JobInput::new(calls.clone(), port.clone());
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
    async fn first_execution_routes_to_submit_and_delivers_the_verdict() {
        let run = probe_run();
        let job = probe(ProbeSubmit::Succeed, ProbeVerdict::Settled);
        let ctx = ctx_with_claim_seq(2);

        let outcome = StandaloneJob::perform(&job, &ctx, &run.input)
            .await
            .unwrap();

        assert_eq!(*run.calls.lock().unwrap(), ["submit"]);
        assert!(matches!(outcome, JobOutcome::Done(())));
        assert!(matches!(
            run.port.delivered.lock().unwrap().as_slice(),
            [(1, DispatchOutcome(Verdict::Confirmed(settled)))]
                if settled.output == "submitted"
                    && settled.attempts == 1
                    && settled.job_id == ctx.job_id()
        ));
    }

    #[tokio::test]
    async fn later_execution_routes_to_reconcile() {
        let run = probe_run();
        let job = probe(ProbeSubmit::Succeed, ProbeVerdict::Settled);

        let outcome = StandaloneJob::perform(&job, &ctx_with_claim_seq(3), &run.input)
            .await
            .unwrap();

        assert_eq!(*run.calls.lock().unwrap(), ["reconcile"]);
        assert!(matches!(outcome, JobOutcome::Done(())));
        assert!(matches!(
            run.port.delivered.lock().unwrap().as_slice(),
            [(1, DispatchOutcome(Verdict::Confirmed(settled)))]
                if settled.output == "reconciled"
        ));
    }

    #[tokio::test]
    async fn reconcile_not_submitted_authorizes_a_resubmit() {
        let run = probe_run();
        let job = probe(ProbeSubmit::Succeed, ProbeVerdict::NotSubmitted);

        let outcome = StandaloneJob::perform(&job, &ctx_with_claim_seq(4), &run.input)
            .await
            .unwrap();

        assert_eq!(*run.calls.lock().unwrap(), ["reconcile", "submit"]);
        assert!(matches!(outcome, JobOutcome::Done(())));
    }

    #[tokio::test]
    async fn reconcile_indeterminate_defers_without_submitting_or_delivering() {
        let run = probe_run();
        let job = probe(ProbeSubmit::Succeed, ProbeVerdict::Indeterminate);

        let outcome = StandaloneJob::perform(&job, &ctx_with_claim_seq(3), &run.input)
            .await
            .unwrap();

        assert_eq!(*run.calls.lock().unwrap(), ["reconcile"]);
        assert!(matches!(outcome, JobOutcome::Defer(_)));
        assert!(run.port.delivered.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn terminal_failure_delivers_the_rejection_before_dead_lettering() {
        let run = probe_run();
        let job = probe(ProbeSubmit::RejectTerminal, ProbeVerdict::Settled);
        let ctx = ctx_with_claim_seq(2);

        let failure = StandaloneJob::perform(&job, &ctx, &run.input)
            .await
            .unwrap_err();

        assert!(matches!(failure, JobFailure::Terminal(_)));
        assert!(matches!(
            run.port.delivered.lock().unwrap().as_slice(),
            [(1, DispatchOutcome(Verdict::Failed(settled)))]
                if matches!(&settled.failure, DispatchFailure::Rejected(error)
                    if error.reason == "no funds")
        ));
    }

    #[tokio::test]
    async fn failed_delivery_defers_instead_of_counting_an_attempt() {
        let run = probe_run();
        run.port
            .unavailable
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let job = probe(ProbeSubmit::Succeed, ProbeVerdict::Settled);
        let retry_delay = Duration::from_millis(750);
        let ctx = JobContext {
            claim_seq: 2,
            delivery: DeliveryPolicy {
                retry_delay,
                ..DeliveryPolicy::default()
            },
            ..JobContext::default()
        };

        let outcome = StandaloneJob::perform(&job, &ctx, &run.input)
            .await
            .unwrap();

        assert!(matches!(outcome, JobOutcome::Defer(delay) if delay == retry_delay));
    }

    #[test]
    fn delivery_deferral_uses_the_configured_delays() {
        let retry_delay = Duration::from_millis(250);
        let refused_backoff = Duration::from_secs(60);
        let ctx = JobContext {
            delivery: DeliveryPolicy {
                retry_delay,
                refused_backoff,
            },
            ..JobContext::default()
        };

        assert_eq!(
            delivery_deferral(&ctx, &OriginDeliveryError::Transport("down".into())),
            retry_delay
        );
        assert_eq!(
            delivery_deferral(&ctx, &OriginDeliveryError::Refused("mismatch".into())),
            refused_backoff
        );
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

        let failure = StandaloneJob::perform(&job, &ctx, &run.input)
            .await
            .unwrap_err();

        assert!(matches!(failure, JobFailure::Transient(_)));
        assert!(matches!(
            run.port.delivered.lock().unwrap().as_slice(),
            [(1, DispatchOutcome(Verdict::Failed(settled)))]
                if matches!(&settled.failure, DispatchFailure::DeadLettered {
                    reason: DeadReason::RetriesExhausted, ..
                }) && settled.attempts == 5
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

        let failure = StandaloneJob::perform(&job, &ctx, &run.input)
            .await
            .unwrap_err();

        assert!(matches!(failure, JobFailure::Transient(_)));
        assert!(run.port.delivered.lock().unwrap().is_empty());
    }

    /// End to end over a real store: dispatch through `Store::send` (the
    /// `Dispatched` event + job enqueued in one commit), claim like the
    /// worker does, run the routed execution with the store itself as the
    /// origin port, and watch the entity settle -- with the guard refusing
    /// overlap and absorbing duplicate delivery along the way.
    #[tokio::test]
    async fn dispatched_job_settles_through_a_real_store() {
        use crate::job::{WorkerId, plan_claim};
        use crate::job_store::ClaimOutcome;
        use crate::{AggregateError, LifecycleError, SqliteBackend, StoreBuilder};

        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        let desk_store = StoreBuilder::<Desk>::new(pool.clone())
            .build()
            .await
            .unwrap();

        desk_store
            .send(&7, DeskCommand::Place(place()))
            .await
            .unwrap();
        let desk = desk_store.load(&7).await.unwrap().unwrap();
        let DispatchedJob::InFlight { job_id } = desk.order else {
            panic!("expected the job in flight, got {:?}", desk.order);
        };

        // The guard refuses an overlapping dispatch through the full stack.
        let refused = desk_store
            .send(&7, DeskCommand::Place(place()))
            .await
            .unwrap_err();
        assert!(matches!(
            refused,
            AggregateError::UserError(LifecycleError::Apply(DeskError::Refused(
                DispatchRefused::InFlight
            )))
        ));

        // Claim exactly as the worker poll does and run the routed execution
        // with the real store as the origin port.
        let backend = SqliteBackend::new(pool.clone());
        let view_id = job_id.to_string();
        let worker = WorkerId::new("test-worker");
        let now_ms = chrono::Utc::now().timestamp_millis() + 1_000;
        let outcome = backend
            .claim(&view_id, |read| {
                plan_claim(&view_id, read, &worker, now_ms, 30_000, 50)
            })
            .await
            .unwrap();
        let ClaimOutcome::Won(won) = outcome else {
            panic!("expected to win the claim");
        };
        let job: PlaceOrder = serde_json::from_value(won.args.clone()).unwrap();
        let ctx = JobContext {
            job_id,
            claim_seq: won.claim_seq,
            claim_id: won.claim_id,
            attempt: won.attempt,
            max_attempts: 5,
            ..JobContext::default()
        };
        let input = JobInput::new((), desk_store.clone());

        let done = StandaloneJob::perform(&job, &ctx, &input).await.unwrap();
        assert!(matches!(done, JobOutcome::Done(())));

        let settled = desk_store.load(&7).await.unwrap().unwrap();
        let DispatchedJob::Confirmed(settled_order) = &settled.order else {
            panic!("expected Confirmed, got {:?}", settled.order);
        };
        assert_eq!(settled_order.job_id(), job_id);
        assert_eq!(settled_order.output(), "filled");
        assert_eq!(settled_order.attempts(), 1);

        // An at-least-once redelivery of the same verdict is absorbed.
        desk_store
            .send(&7, DeskCommand::Settle(confirmed(job_id)))
            .await
            .unwrap();
        let after = desk_store.load(&7).await.unwrap().unwrap();
        assert_eq!(after.order, settled.order);
    }
}
