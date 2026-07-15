//! Durable, retryable jobs for command side effects.
//!
//! Command handlers stay pure `(state, command) -> Vec<Event>` and enqueue side
//! effects as [`Job`]s. The framework flushes pending jobs inside the same
//! transaction that commits the triggering events, so a job is enqueued iff its
//! events commit -- closing the crash window between a side effect and the event
//! meant to record it.
//!
//! A job is its own event stream (`aggregate_type = "job"`): [`JobState`] is an
//! [`EventSourced`] aggregate, so the claim/ack lifecycle rides cqrs-es. The
//! claim itself is a backend transaction ([`crate::EventBackend::claim`]) because
//! the live lease is projection-only (see [ADR-0006]); the ack (succeed / retry /
//! dead) is a [`JobCommand`] fenced on the [`ClaimId`] of the claim that ran it.
//!
//! [ADR-0006]: ../../adrs/0006-cqrs-native-durable-jobs.md

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use cqrs_es::persist::SerializedEvent;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::future::Future;
use std::time::Duration;
use tracing::error;
use ulid::Ulid;

use crate::dependency::Nil;
use crate::dispatch::DeliveryPolicy;
use crate::job_store::{ClaimDecision, ClaimRead};
use crate::lifecycle::Lifecycle;
use crate::{CompactionPolicy, EventSourced, Table};

const JOB_AGGREGATE_TYPE: &str = "job";
const JOB_EVENT_VERSION: &str = "1.0";

/// A durable, retryable worker job with no origin entity.
///
/// This is the ADR-0007 standalone path: reactors, pollers, job chains, and
/// startup recovery enqueue these directly on [`crate::JobRuntime`] -- there
/// is no command commit to ride and no entity waiting on the outcome. A job
/// an ENTITY kicks off implements [`crate::Job`] instead (submit/reconcile +
/// verdict delivery) and is dispatched from a command handler.
pub trait StandaloneJob: Serialize + DeserializeOwned + Send + 'static {
    /// Dependency bundle injected into [`perform`](StandaloneJob::perform).
    type Input: Send + Sync + 'static;

    /// Value produced on successful completion.
    type Output: Send + 'static;

    /// Error returned when [`perform`](StandaloneJob::perform) fails.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Worker name prefix; the registered worker name is
    /// `format!("{WORKER_NAME}-{index}")`.
    const WORKER_NAME: &'static str;

    /// Stable identifier for this job kind, recorded on the `Enqueued` event and
    /// used to route a job to the worker that runs it.
    const KIND: &'static str;

    /// Logged when retries are exhausted.
    const TERMINAL_FAILURE_MSG: &'static str = "Job failed after retries";

    /// Human-readable label for structured logging.
    fn label(&self) -> Label;

    /// Execute this job against the injected input.
    ///
    /// Return [`JobOutcome::Done`] when finished, or [`JobOutcome::Defer`] to
    /// re-run later without counting an attempt (a poll-while-pending or
    /// external-wait snooze). Failures state their retry class explicitly at
    /// the return site: [`JobFailure::Transient`] counts an attempt and is
    /// retried with backoff; [`JobFailure::Terminal`] dead-letters immediately.
    ///
    /// `ctx` carries the job's durable identity: derive external-boundary
    /// idempotency keys from [`JobContext::job_id`] (never from the attempt
    /// number, which changes across retries) so every retry presents the same
    /// key and the external system deduplicates.
    fn perform(
        &self,
        ctx: &JobContext,
        input: &Self::Input,
    ) -> impl Future<Output = Result<JobOutcome<Self::Output>, JobFailure<Self::Error>>> + Send;
}

/// Identity of a durable job -- a ULID minted at enqueue time.
///
/// Stable across every retry and re-claim of the job, so it is the correct
/// root for external-boundary idempotency keys. Converted to a string only at
/// the storage boundary (the job's aggregate id and `job_queue` view id).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(Ulid);

impl JobId {
    /// Mints a fresh job id. The framework mints one per dispatch/enqueue;
    /// public so tests and interop code can fabricate identities.
    #[must_use]
    pub fn new() -> Self {
        Self(Ulid::new())
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(ulid) = self;
        std::fmt::Display::fmt(ulid, formatter)
    }
}

impl std::str::FromStr for JobId {
    type Err = ulid::DecodeError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        raw.parse().map(Self)
    }
}

/// Durable execution context a worker hands to [`Job::perform`], captured at
/// claim time; the ack reads it from the task's `parts.ctx`.
#[derive(Clone, Default)]
pub struct JobContext {
    pub(crate) job_id: JobId,
    /// Sequence of the `Claimed` event this worker appended; the renew keys on it.
    pub(crate) claim_seq: i64,
    /// Identity of the claim that produced this run; the ack fences on it.
    pub(crate) claim_id: ClaimId,
    /// Recorded failures so far (0 on first run), from the fold.
    pub(crate) attempt: u32,
    /// The worker's retry budget ([`crate::JobWorkerConfig::max_attempts`]),
    /// captured at claim time; 0 when unknown (a context built outside a
    /// worker).
    pub(crate) max_attempts: u32,
    /// The worker's verdict-delivery deferrals
    /// ([`crate::JobWorkerConfig::delivery`]), captured at claim time.
    pub(crate) delivery: DeliveryPolicy,
}

impl JobContext {
    /// Stable identifier of the job being executed -- the same across every
    /// retry and re-claim of this job, so it is the correct root for
    /// external-boundary idempotency keys.
    pub fn job_id(&self) -> JobId {
        self.job_id
    }

    /// Durably recorded failures before this run: `0` on the first execution,
    /// `n` after `n` failed attempts. Deferred runs do not advance it.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Whether this run is the job's FIRST execution ever -- no earlier run can
    /// have reached an external system.
    ///
    /// Derived from the durable event stream: the first claim is always the
    /// event after `Enqueued`, and every later run (retry, defer, lease-expired
    /// reclaim) claims at a higher sequence because the first `Claimed` event is
    /// permanent. This over-approximates submissions in the safe direction: a
    /// crash after the claim but before the external call still reports `false`
    /// on the next run.
    pub fn is_first_execution(&self) -> bool {
        const FIRST_CLAIM_SEQ: i64 = 2;
        self.claim_seq == FIRST_CLAIM_SEQ
    }

    /// Whether a failure recorded for this run would exhaust the retry budget,
    /// so the worker dead-letters instead of rescheduling. `false` when the
    /// budget is unknown (a context built outside a worker).
    pub fn is_final_attempt(&self) -> bool {
        self.max_attempts > 0 && self.attempt + 1 >= self.max_attempts
    }
}

/// What a [`Job::perform`] reports back to the worker (ADR-0007).
#[derive(Debug)]
pub enum JobOutcome<Output> {
    /// The job finished; the worker records success and acks it (terminal).
    Done(Output),
    /// The job is not done but did not fail -- re-run it after this delay WITHOUT
    /// counting an attempt or recording a failure (distinct from a retry). The
    /// claim budget resets on a defer, so a poller may defer indefinitely; the
    /// job keeps its id, attempt, and payload across the snooze.
    Defer(Duration),
}

/// How a failed [`Job::perform`] must be treated -- chosen explicitly at every
/// failure return (ADR-0008).
///
/// There is deliberately NO `From<Error>` impl: `?` cannot silently classify a
/// failure, because silent conversion ergonomics are exactly how unexpected
/// retries (or missing ones) happen. Wrap the error at the return site --
/// `Err(JobFailure::Transient(error))` / `Err(JobFailure::Terminal(error))` --
/// so every failure site states its retry class.
#[derive(Debug, thiserror::Error)]
pub enum JobFailure<PerformError> {
    /// Worth retrying (timeout, rate limit, connection loss): the worker
    /// reschedules with backoff, counting an attempt, and dead-letters as
    /// `RetriesExhausted` once attempts run out.
    #[error("transient job failure")]
    Transient(#[source] PerformError),
    /// A definitive rejection that retrying can never fix (validation failure,
    /// insufficient funds, permanently rejected order): the worker dead-letters
    /// immediately -- no retries.
    #[error("terminal job failure")]
    Terminal(#[source] PerformError),
}

/// Human-readable identifier for a job instance, used in logs.
#[derive(Debug, Clone)]
pub struct Label(String);

impl Label {
    /// Wraps a string-like value as a label.
    pub fn new(label: impl Into<String>) -> Self {
        Self(label.into())
    }
}

impl std::fmt::Display for Label {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Stable identifier for a job kind -- the [`Job::KIND`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JobKind(String);

impl JobKind {
    pub(crate) fn new(kind: impl Into<String>) -> Self {
        Self(kind.into())
    }
}

/// Identifier of the worker holding a claim, recorded on `Claimed` for diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkerId(String);

impl WorkerId {
    /// A process-run-unique worker id: the name plus a fresh ULID.
    pub(crate) fn new(name: &str) -> Self {
        Self(format!("{name}:{}", Ulid::new()))
    }
}

/// Identity of a single claim attempt -- a fresh ULID minted per claim (never per
/// worker), so an ack fences against the exact claim that produced it. A
/// re-claim mints a new `ClaimId`, so the prior runner's ack is rejected.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClaimId(String);

impl ClaimId {
    pub(crate) fn new() -> Self {
        Self(Ulid::new().to_string())
    }
}

/// Why a job was dead-lettered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeadReason {
    /// Every retry attempt failed.
    RetriesExhausted,
    /// The job failed with [`JobFailure::Terminal`] -- a definitive rejection
    /// retrying can never fix, dead-lettered on first occurrence.
    Rejected,
    /// The stored payload no longer deserializes into the job type.
    Undecodable,
    /// The job was claimed too many times without recording an outcome
    /// (a crash/hang loop), exceeding the claim budget.
    Abandoned,
}

/// A transition in a durable job's lifecycle, appended to the job's event stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum JobEvent {
    /// Job created and runnable at `run_at`.
    Enqueued {
        kind: JobKind,
        payload: serde_json::Value,
        #[serde(with = "chrono::serde::ts_milliseconds")]
        run_at: DateTime<Utc>,
    },
    /// A worker took the job under claim `claim_id`, leased until `lease_until`.
    /// `lease_until` is retained for audit; the live lease is the projection
    /// column, so the fold drops it from [`JobState`].
    Claimed {
        worker: WorkerId,
        claim_id: ClaimId,
        #[serde(with = "chrono::serde::ts_milliseconds")]
        lease_until: DateTime<Utc>,
    },
    /// The job ran to success (terminal).
    Succeeded,
    /// An attempt failed with `error`; the job runs again as `attempt` at `run_at`.
    RetryScheduled {
        #[serde(with = "chrono::serde::ts_milliseconds")]
        run_at: DateTime<Utc>,
        attempt: u32,
        error: String,
    },
    /// A successful defer: the job runs again at `run_at` without counting an
    /// attempt or recording a failure (ADR-0007). The claim budget resets.
    Rescheduled {
        #[serde(with = "chrono::serde::ts_milliseconds")]
        run_at: DateTime<Utc>,
    },
    /// An attempt failed terminally (terminal).
    Dead { reason: DeadReason, error: String },
}

impl JobEvent {
    fn type_name(&self) -> &'static str {
        match self {
            Self::Enqueued { .. } => "JobEnqueued",
            Self::Claimed { .. } => "JobClaimed",
            Self::Succeeded => "JobSucceeded",
            Self::RetryScheduled { .. } => "JobRetryScheduled",
            Self::Rescheduled { .. } => "JobRescheduled",
            Self::Dead { .. } => "JobDead",
        }
    }

    /// Serializes this event for the given job stream at `sequence`.
    pub(crate) fn serialized(
        &self,
        job_id: &str,
        sequence: usize,
    ) -> Result<SerializedEvent, serde_json::Error> {
        Ok(SerializedEvent {
            aggregate_id: job_id.to_string(),
            sequence,
            aggregate_type: JOB_AGGREGATE_TYPE.to_string(),
            event_type: self.type_name().to_string(),
            event_version: JOB_EVENT_VERSION.to_string(),
            payload: serde_json::to_value(self)?,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        })
    }
}

impl DomainEvent for JobEvent {
    fn event_type(&self) -> String {
        self.type_name().to_string()
    }

    fn event_version(&self) -> String {
        JOB_EVENT_VERSION.to_string()
    }
}

/// The folded state of a durable job -- the [`EventSourced`] entity.
///
/// `Claimed` carries no `lease_until`: the live lease lives only in the
/// `job_queue` projection column (D1), so it cannot be folded from the event
/// stream. `claims` counts claim attempts (the D5 budget), folded because
/// `transition` sees only `&self`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum JobState {
    /// Runnable at `run_at`; `attempt` counts attempts that have already failed.
    Pending {
        kind: JobKind,
        payload: serde_json::Value,
        #[serde(with = "chrono::serde::ts_milliseconds")]
        run_at: DateTime<Utc>,
        attempt: u32,
        claims: u32,
    },
    /// Held under claim `claim_id` by `worker`.
    Claimed {
        kind: JobKind,
        payload: serde_json::Value,
        worker: WorkerId,
        claim_id: ClaimId,
        #[serde(with = "chrono::serde::ts_milliseconds")]
        run_at: DateTime<Utc>,
        attempt: u32,
        claims: u32,
    },
    /// Completed successfully (terminal).
    Done,
    /// Dead-lettered (terminal).
    Dead { reason: DeadReason },
}

/// A command against a claimed job: the ack of the claim `claim_id` ran. The
/// claim itself is not a command -- it is [`crate::EventBackend::claim`].
#[derive(Debug, Clone)]
pub(crate) enum JobCommand {
    /// The job ran to success.
    Succeed { claim_id: ClaimId },
    /// The attempt failed; reschedule as `attempt` at `run_at`.
    RetrySchedule {
        claim_id: ClaimId,
        run_at: DateTime<Utc>,
        attempt: u32,
        error: String,
    },
    /// The attempt deferred (success): re-run at `run_at` without counting an
    /// attempt or recording a failure.
    Reschedule {
        claim_id: ClaimId,
        run_at: DateTime<Utc>,
    },
    /// The attempt failed terminally; dead-letter it.
    Kill {
        claim_id: ClaimId,
        reason: DeadReason,
        error: String,
    },
}

impl JobCommand {
    fn claim_id(&self) -> &ClaimId {
        match self {
            Self::Succeed { claim_id }
            | Self::RetrySchedule { claim_id, .. }
            | Self::Reschedule { claim_id, .. }
            | Self::Kill { claim_id, .. } => claim_id,
        }
    }
}

/// Domain error from a [`JobCommand`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub(crate) enum JobError {
    /// The ack's claim does not own the job -- a re-claimer took it. The ack is
    /// rejected before any event is written.
    #[error("job ack fenced: the claim no longer owns the job")]
    Fenced,
}

#[async_trait]
impl EventSourced for JobState {
    type Id = JobId;
    type Error = JobError;
    type Command = JobCommand;
    type Event = JobEvent;
    type Materialized = Table;
    type Jobs = Nil;

    const PROJECTION: Table = Table("job_queue");
    const SCHEMA_VERSION: u64 = 1;
    const AGGREGATE_TYPE: &'static str = JOB_AGGREGATE_TYPE;
    const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;

    fn originate(event: &JobEvent) -> Option<Self> {
        match event {
            JobEvent::Enqueued {
                kind,
                payload,
                run_at,
            } => Some(Self::Pending {
                kind: kind.clone(),
                payload: payload.clone(),
                run_at: *run_at,
                attempt: 0,
                claims: 0,
            }),
            _ => None,
        }
    }

    fn evolve(entity: &Self, event: &JobEvent) -> Result<Option<Self>, JobError> {
        let next = match (entity, event) {
            // Claim or lease-expiry re-claim: adopt the new claim, count it, keep
            // the attempt (a crash is not a failed attempt). The event's
            // lease_until is dropped -- the live lease is projection-only.
            (
                Self::Pending {
                    kind,
                    payload,
                    run_at,
                    attempt,
                    claims,
                }
                | Self::Claimed {
                    kind,
                    payload,
                    run_at,
                    attempt,
                    claims,
                    ..
                },
                JobEvent::Claimed {
                    worker, claim_id, ..
                },
            ) => Self::Claimed {
                kind: kind.clone(),
                payload: payload.clone(),
                worker: worker.clone(),
                claim_id: claim_id.clone(),
                run_at: *run_at,
                attempt: *attempt,
                claims: claims + 1,
            },
            (Self::Claimed { .. }, JobEvent::Succeeded) => Self::Done,
            (
                Self::Claimed {
                    kind,
                    payload,
                    claims,
                    ..
                },
                JobEvent::RetryScheduled {
                    run_at, attempt, ..
                },
            ) => Self::Pending {
                kind: kind.clone(),
                payload: payload.clone(),
                run_at: *run_at,
                attempt: *attempt,
                claims: *claims,
            },
            // A defer is a productive checkpoint, not a failure: keep the attempt,
            // re-arm run_at, and reset the claim budget so a poller may defer
            // indefinitely (the budget only catches claims with no outcome).
            (
                Self::Claimed {
                    kind,
                    payload,
                    attempt,
                    ..
                },
                JobEvent::Rescheduled { run_at },
            ) => Self::Pending {
                kind: kind.clone(),
                payload: payload.clone(),
                run_at: *run_at,
                attempt: *attempt,
                claims: 0,
            },
            (Self::Pending { .. } | Self::Claimed { .. }, JobEvent::Dead { reason, .. }) => {
                Self::Dead {
                    reason: reason.clone(),
                }
            }
            // Any other event for this state is a corrupt stream; surface it
            // loudly rather than silently no-op.
            _ => return Ok(None),
        };
        Ok(Some(next))
    }

    async fn initialize(_command: JobCommand) -> Result<crate::Effect<Self>, JobError> {
        // Jobs are born from the enqueue flush (a raw `Enqueued` append), never a
        // command. An ack of a vanished job is a harmless no-op.
        crate::dispatch::uneventful()
    }

    async fn transition(&self, command: JobCommand) -> Result<crate::Effect<Self>, JobError> {
        let Self::Claimed { claim_id: held, .. } = self else {
            return Err(JobError::Fenced);
        };
        if held != command.claim_id() {
            return Err(JobError::Fenced);
        }
        let event = match command {
            JobCommand::Succeed { .. } => JobEvent::Succeeded,
            JobCommand::RetrySchedule {
                run_at,
                attempt,
                error,
                ..
            } => JobEvent::RetryScheduled {
                run_at,
                attempt,
                error,
            },
            JobCommand::Reschedule { run_at, .. } => JobEvent::Rescheduled { run_at },
            JobCommand::Kill { reason, error, .. } => JobEvent::Dead { reason, error },
        };
        Ok(crate::Effect::Events(vec![event]))
    }
}

/// The strongly-typed success payload of a claim, handed back to the worker (and
/// carried opaquely through [`crate::EventBackend::claim`] as its `Won`).
pub(crate) struct WonClaim {
    /// Sequence of the appended `Claimed` event; the renew/ack key.
    pub(crate) claim_seq: i64,
    /// This claim attempt's id; the ack fence key.
    pub(crate) claim_id: ClaimId,
    /// Recorded failures so far.
    pub(crate) attempt: u32,
    /// The job arguments, folded from state.
    pub(crate) args: serde_json::Value,
}

/// A fully-resolved enqueue (id + run_at computed), drained from the per-command
/// buffer and flushed by the event repository in the commit transaction.
pub(crate) struct EnqueueRequest {
    /// The new job's id (a fresh ULID).
    pub(crate) job_id: JobId,
    /// The job kind.
    pub(crate) kind: JobKind,
    /// The job arguments.
    pub(crate) payload: serde_json::Value,
    /// When the job becomes runnable (Unix epoch milliseconds).
    pub(crate) run_at_ms: i64,
}

/// Pure es-side errors building enqueue/claim payloads (no backend specifics).
#[derive(Debug, thiserror::Error)]
pub enum JobStoreError {
    /// A job event/state could not be JSON-encoded.
    #[error("failed to encode a job payload")]
    Encode(#[from] serde_json::Error),
    /// A requested delay exceeds the representable range.
    #[error("job delay exceeds the representable range")]
    DelayOverflow,
}

/// Decides what a claim transaction should do, given the row it re-read in-txn.
///
/// The runnable check reads `read.lease_until_ms` -- the renewed projection
/// column, NOT a folded event lease -- which is the entire reason the claim is a
/// backend transaction rather than an `execute(Claim)` command.
pub(crate) fn plan_claim(
    job_id: &str,
    read: Option<ClaimRead>,
    worker: &WorkerId,
    now_ms: i64,
    lease_ms: i64,
    max_claims: u32,
) -> ClaimDecision<WonClaim> {
    let Some(read) = read else {
        return ClaimDecision::Skip;
    };
    let lifecycle: Lifecycle<JobState> = match serde_json::from_str(&read.payload) {
        Ok(lifecycle) => lifecycle,
        Err(error) => {
            error!(target: "cqrs", job_id, ?error, "job_queue payload undecodable; needs rebuild");
            return ClaimDecision::Skip;
        }
    };
    let Lifecycle::Live(state) = lifecycle else {
        return ClaimDecision::Skip;
    };

    // Runnable keys on the STATE, not on whether the lease column is set: the
    // reactor never clears lease_until, so a pending row can carry a stale lease
    // from a prior claim. A Pending consults run_at; a Claimed consults the lease
    // column (NULL means a rebuilt-but-unclaimed row, which is reclaimable).
    let runnable = match &state {
        JobState::Pending { run_at, .. } => run_at.timestamp_millis() <= now_ms,
        JobState::Claimed { .. } => read.lease_until_ms.is_none_or(|lease| lease < now_ms),
        JobState::Done | JobState::Dead { .. } => false,
    };
    if !runnable {
        return ClaimDecision::Skip;
    }

    let (kind, payload, run_at, attempt, claims) = match state {
        JobState::Pending {
            kind,
            payload,
            run_at,
            attempt,
            claims,
        }
        | JobState::Claimed {
            kind,
            payload,
            run_at,
            attempt,
            claims,
            ..
        } => (kind, payload, run_at, attempt, claims),
        JobState::Done | JobState::Dead { .. } => return ClaimDecision::Skip,
    };

    let claim_seq = read.version + 1;
    let Ok(claim_seq_usize) = usize::try_from(claim_seq) else {
        error!(target: "cqrs", job_id, claim_seq, "job claim sequence out of range");
        return ClaimDecision::Skip;
    };

    if claims >= max_claims {
        return plan_abandon(job_id, claim_seq_usize);
    }

    let claim_id = ClaimId::new();
    let lease_until_ms = now_ms + lease_ms;
    let claimed = JobEvent::Claimed {
        worker: worker.clone(),
        claim_id: claim_id.clone(),
        lease_until: from_millis(lease_until_ms),
    };
    let new_state = JobState::Claimed {
        kind,
        payload: payload.clone(),
        worker: worker.clone(),
        claim_id: claim_id.clone(),
        run_at,
        attempt,
        claims: claims + 1,
    };
    let (Ok(event), Ok(payload_str)) = (
        claimed.serialized(job_id, claim_seq_usize),
        serde_json::to_string(&Lifecycle::Live(new_state)),
    ) else {
        error!(target: "cqrs", job_id, "failed to encode Claimed");
        return ClaimDecision::Skip;
    };

    ClaimDecision::Claim {
        event,
        payload: payload_str,
        lease_until_ms,
        won: WonClaim {
            claim_seq,
            claim_id,
            attempt,
            args: payload,
        },
    }
}

fn plan_abandon(job_id: &str, claim_seq_usize: usize) -> ClaimDecision<WonClaim> {
    let dead = JobEvent::Dead {
        reason: DeadReason::Abandoned,
        error: "claim budget exhausted".to_string(),
    };
    let (Ok(event), Ok(payload_str)) = (
        dead.serialized(job_id, claim_seq_usize),
        serde_json::to_string(&Lifecycle::<JobState>::Live(JobState::Dead {
            reason: DeadReason::Abandoned,
        })),
    ) else {
        error!(target: "cqrs", job_id, "failed to encode Dead{{Abandoned}}");
        return ClaimDecision::Skip;
    };
    ClaimDecision::Abandon {
        event,
        payload: payload_str,
    }
}

fn from_millis(millis: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(millis).unwrap_or_else(Utc::now)
}

/// Builds the `Enqueued` event (sequence 1) for an [`EnqueueRequest`].
pub(crate) fn enqueued_event(request: &EnqueueRequest) -> Result<SerializedEvent, JobStoreError> {
    Ok(JobEvent::Enqueued {
        kind: request.kind.clone(),
        payload: request.payload.clone(),
        run_at: from_millis(request.run_at_ms),
    }
    .serialized(&request.job_id.to_string(), 1)?)
}

/// Builds the seed `job_queue` projection payload (version 1) for an enqueue:
/// `Lifecycle::Live(Pending)`.
pub(crate) fn pending_seed_payload(request: &EnqueueRequest) -> Result<String, JobStoreError> {
    Ok(serde_json::to_string(&Lifecycle::Live(
        JobState::Pending {
            kind: request.kind.clone(),
            payload: request.payload.clone(),
            run_at: from_millis(request.run_at_ms),
            attempt: 0,
            claims: 0,
        },
    ))?)
}

/// A dispatched job awaiting flush at the next commit, produced only by
/// [`crate::DispatchedJob::dispatch`] and buffered only by the framework
/// (`Lifecycle::handle`).
pub(crate) struct PendingPush {
    pub(crate) job_id: JobId,
    pub(crate) job: Box<dyn ErasedJob>,
    pub(crate) delay: Option<Duration>,
}

/// Encoded pending jobs whose source buffer remains intact until commit.
pub(crate) struct PreparedPendingJobs {
    requests: Vec<EnqueueRequest>,
    prepared_count: Option<usize>,
}

/// Type erasure over a dispatched [`crate::Job`] so the pending buffer can
/// hold heterogeneous job types; the flush needs only the routing kind and
/// the encoded payload.
pub(crate) trait ErasedJob: Send {
    fn kind(&self) -> JobKind;
    fn encode(&self) -> Result<serde_json::Value, serde_json::Error>;
}

impl<J: crate::Job> ErasedJob for J {
    fn kind(&self) -> JobKind {
        JobKind::new(<J as crate::Job>::KIND)
    }

    fn encode(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}

impl PreparedPendingJobs {
    /// Moves the encoded requests into an engine commit while retaining their source buffer.
    pub(crate) fn take_requests(&mut self) -> Vec<EnqueueRequest> {
        std::mem::take(&mut self.requests)
    }

    /// Removes the retained source pushes included in a successful database commit.
    pub(crate) fn mark_committed(self) {
        if let Some(prepared_count) = self.prepared_count {
            PENDING_JOBS.with(|pending| {
                drop(pending.borrow_mut().drain(..prepared_count));
            });
        }
    }
}

tokio::task_local! {
    /// Per-command buffer of jobs awaiting flush, cleared by the event
    /// repository only after its commit transaction succeeds.
    static PENDING_JOBS: RefCell<Vec<PendingPush>>;
}

/// Runs `command_execution` with a fresh pending-job scope active, so the
/// framework's [`buffer`] of a [`crate::Effect::Dispatch`] lands where the
/// repository's [`prepare_pending`] can prepare it for the event transaction.
/// [`PreparedPendingJobs::mark_committed`] clears the prepared entries only
/// after that transaction commits successfully.
pub(crate) async fn with_pending_scope<Output>(
    command_execution: impl Future<Output = Output>,
) -> Output {
    PENDING_JOBS
        .scope(RefCell::new(Vec::new()), command_execution)
        .await
}

/// Buffers a dispatched job onto the active pending scope. Called only by
/// `Lifecycle::handle` while `Store::send` holds the scope open; an inactive
/// scope is a framework bug, and the error MUST fail the command so the
/// `Dispatched` event is never committed without its enqueue.
pub(crate) fn buffer(push: PendingPush) -> Result<(), DispatchNotBuffered> {
    PENDING_JOBS
        .try_with(|pending| pending.borrow_mut().push(push))
        .map_err(|_| DispatchNotBuffered)
}

/// A job dispatch could not be buffered because no command scope was active.
/// Surfacing this fails the command, preserving the ADR-0009 invariant that a
/// `Dispatched` event and its enqueue commit together or not at all.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("job dispatch buffered outside a command scope")]
pub struct DispatchNotBuffered;

/// Prepares the pending jobs without clearing their task-local buffer. The
/// caller marks the batch committed only after its database transaction
/// succeeds, leaving the original pushes available after any failure.
pub(crate) fn prepare_pending() -> Result<PreparedPendingJobs, JobStoreError> {
    let prepared = PENDING_JOBS.try_with(|pending| {
        let pending = pending.borrow();
        let requests = pending
            .iter()
            .map(|push| {
                Ok(EnqueueRequest {
                    job_id: push.job_id,
                    kind: push.job.kind(),
                    payload: push.job.encode()?,
                    run_at_ms: resolve_run_at_ms(push.delay)?,
                })
            })
            .collect::<Result<Vec<_>, JobStoreError>>()?;
        Ok::<_, JobStoreError>((requests, pending.len()))
    });
    let (requests, prepared_count) = match prepared {
        Ok(prepared) => {
            let (requests, prepared_count) = prepared?;
            (requests, Some(prepared_count))
        }
        Err(_) => (Vec::new(), None),
    };
    Ok(PreparedPendingJobs {
        requests,
        prepared_count,
    })
}

/// Drains the per-command pending-job buffer into [`EnqueueRequest`]s for tests
/// and non-transactional inspection. Production commits use [`prepare_pending`]
/// so a failed transaction cannot lose buffered jobs.
#[cfg(test)]
pub(crate) fn take_pending() -> Result<Vec<EnqueueRequest>, JobStoreError> {
    let mut pending = prepare_pending()?;
    let requests = pending.take_requests();
    pending.mark_committed();
    Ok(requests)
}

/// Builds a standalone [`EnqueueRequest`] (fresh ULID, resolved `run_at`) for one
/// `job` -- the reactor / poller / job-chain enqueue path, without the
/// command-scope buffer [`prepare_pending`] flushes.
pub(crate) fn enqueue_request<J: StandaloneJob>(
    job: &J,
    delay: Option<Duration>,
) -> Result<EnqueueRequest, JobStoreError> {
    Ok(EnqueueRequest {
        job_id: JobId::new(),
        kind: JobKind::new(J::KIND),
        payload: serde_json::to_value(job)?,
        run_at_ms: resolve_run_at_ms(delay)?,
    })
}

/// Resolves a job's `run_at` (epoch millis) from an optional delay-from-now.
fn resolve_run_at_ms(delay: Option<Duration>) -> Result<i64, JobStoreError> {
    let run_at = match delay {
        None => Utc::now(),
        Some(delay) => {
            Utc::now()
                + chrono::Duration::from_std(delay).map_err(|_| JobStoreError::DelayOverflow)?
        }
    };
    Ok(run_at.timestamp_millis())
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    enum SendEmail {
        Welcome { address: String },
        Reminder { address: String },
    }

    struct TestPendingJob;

    impl ErasedJob for TestPendingJob {
        fn kind(&self) -> JobKind {
            JobKind::new("test-pending")
        }

        fn encode(&self) -> Result<serde_json::Value, serde_json::Error> {
            Ok(serde_json::json!({ "payload": "test" }))
        }
    }

    impl StandaloneJob for SendEmail {
        type Input = ();
        type Output = ();
        type Error = Infallible;

        const WORKER_NAME: &'static str = "send-email";
        const KIND: &'static str = "send-email";

        fn label(&self) -> Label {
            match self {
                Self::Welcome { address } => Label::new(format!("welcome:{address}")),
                Self::Reminder { address } => Label::new(format!("reminder:{address}")),
            }
        }

        async fn perform(
            &self,
            _ctx: &JobContext,
            _input: &(),
        ) -> Result<JobOutcome<()>, JobFailure<Infallible>> {
            Ok(JobOutcome::Done(()))
        }
    }

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).expect("valid timestamp")
    }

    fn claimed(claim_id: &str, claims: u32) -> JobState {
        JobState::Claimed {
            kind: JobKind::new("send-email"),
            payload: serde_json::json!({}),
            worker: WorkerId::new("w"),
            claim_id: ClaimId(claim_id.to_string()),
            run_at: at(0),
            attempt: 0,
            claims,
        }
    }

    #[tokio::test]
    async fn committed_batch_preserves_jobs_buffered_after_prepare() {
        with_pending_scope(async {
            let committed_job_id = JobId::new();
            buffer(PendingPush {
                job_id: committed_job_id,
                job: Box::new(TestPendingJob),
                delay: None,
            })
            .unwrap();

            let mut prepared = prepare_pending().unwrap();
            let committed = prepared.take_requests();
            assert_eq!(committed.len(), 1);
            assert_eq!(committed[0].job_id, committed_job_id);

            let later_job_id = JobId::new();
            tokio::task::yield_now().await;
            buffer(PendingPush {
                job_id: later_job_id,
                job: Box::new(TestPendingJob),
                delay: None,
            })
            .unwrap();

            prepared.mark_committed();

            let remaining = take_pending().unwrap();
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].job_id, later_job_id);
        })
        .await;
    }

    #[test]
    fn label_reflects_variant_and_renders_via_display() {
        let welcome = SendEmail::Welcome {
            address: "a@example.com".to_string(),
        };
        let reminder = SendEmail::Reminder {
            address: "b@example.com".to_string(),
        };

        assert_eq!(welcome.label().to_string(), "welcome:a@example.com");
        assert_eq!(reminder.label().to_string(), "reminder:b@example.com");
    }

    #[tokio::test]
    async fn transition_fences_a_foreign_claim_and_succeeds_for_the_owner() {
        let state = claimed("claim-a", 1);

        let fenced = state
            .transition(JobCommand::Succeed {
                claim_id: ClaimId("claim-b".to_string()),
            })
            .await;
        assert!(matches!(fenced, Err(JobError::Fenced)));

        let owned = state
            .transition(JobCommand::Succeed {
                claim_id: ClaimId("claim-a".to_string()),
            })
            .await
            .unwrap();
        let crate::Effect::Events(events) = owned else {
            panic!("expected events");
        };
        assert_eq!(events, vec![JobEvent::Succeeded]);
    }

    #[test]
    fn evolve_claim_increments_claims_and_drops_the_lease() {
        let pending = JobState::Pending {
            kind: JobKind::new("send-email"),
            payload: serde_json::json!({}),
            run_at: at(0),
            attempt: 0,
            claims: 0,
        };
        let event = JobEvent::Claimed {
            worker: WorkerId::new("w"),
            claim_id: ClaimId("c".to_string()),
            lease_until: at(100),
        };

        let next = JobState::evolve(&pending, &event).unwrap().unwrap();
        let JobState::Claimed {
            claims, claim_id, ..
        } = next
        else {
            panic!("expected Claimed");
        };
        // The fold counts the claim and adopts its id; the event's lease_until is
        // intentionally not folded -- the live lease is the projection column.
        assert_eq!(claims, 1);
        assert_eq!(claim_id, ClaimId("c".to_string()));
    }

    #[test]
    fn evolve_retry_preserves_claims_and_returns_to_pending() {
        let state = claimed("c", 3);
        let event = JobEvent::RetryScheduled {
            run_at: at(50),
            attempt: 1,
            error: "boom".to_string(),
        };

        let next = JobState::evolve(&state, &event).unwrap().unwrap();
        let JobState::Pending {
            claims, attempt, ..
        } = next
        else {
            panic!("expected Pending");
        };
        assert_eq!(claims, 3);
        assert_eq!(attempt, 1);
    }
}
