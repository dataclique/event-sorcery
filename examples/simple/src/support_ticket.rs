//! `SupportTicket` aggregate: a small ticket state machine with a materialized
//! view (filtered queries via a SQLite generated column) and an
//! entity-dispatched durable job (ADR-0009).
//!
//! Closing a ticket does not emit a "closed" fact -- it KICKS OFF a
//! [`NotifyClosed`] job. The framework commits the `Dispatched` intent and the
//! enqueue in one transaction (the ticket shows `Closing`), a supervised worker
//! notifies the customer, and only the delivered verdict settles the ticket to
//! `Closed`. The handler cannot claim the notification happened; the type
//! system makes the eager-fact bug unrepresentable.

use std::fmt::{self, Display};
use std::str::FromStr;

use async_trait::async_trait;
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use event_sorcery::{
    Column, Decision, DispatchEvent, DispatchOutcome, DispatchRefused, DispatchedJob,
    EventSourced, Job, JobContext, JobFailure, JobOutcome, Label, Never, Reconciliation,
    StandaloneJob, Table,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TicketId(pub u64);

impl Display for TicketId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "T-{:04}", self.0)
    }
}

impl FromStr for TicketId {
    type Err = ParseTicketIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let suffix = value
            .strip_prefix("T-")
            .ok_or(ParseTicketIdError::MissingPrefix)?;
        let raw = suffix.parse::<u64>()?;
        Ok(Self(raw))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseTicketIdError {
    #[error("ticket id must start with 'T-'")]
    MissingPrefix,
    #[error("ticket id suffix is not numeric: {0}")]
    NotNumeric(#[from] std::num::ParseIntError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "TEXT")]
pub enum Status {
    Open,
    Pending,
    /// The close was requested; the notify job is in flight. Only the
    /// delivered verdict moves the ticket to `Closed`.
    Closing,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SupportTicket {
    pub ticket: TicketId,
    pub subject: String,
    pub status: Status,
    pub last_updated_at: chrono::DateTime<chrono::Utc>,
    pub notify: DispatchedJob<NotifyClosed>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SupportTicketEvent {
    Opened {
        ticket: TicketId,
        subject: String,
        at: chrono::DateTime<chrono::Utc>,
    },
    AwaitingCustomer {
        at: chrono::DateTime<chrono::Utc>,
    },
    Notify(DispatchEvent<NotifyClosed>),
}

impl From<DispatchEvent<NotifyClosed>> for SupportTicketEvent {
    fn from(event: DispatchEvent<NotifyClosed>) -> Self {
        Self::Notify(event)
    }
}

impl DomainEvent for SupportTicketEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Opened { .. } => "SupportTicketEvent::Opened".to_string(),
            Self::AwaitingCustomer { .. } => "SupportTicketEvent::AwaitingCustomer".to_string(),
            Self::Notify(DispatchEvent::Dispatched { .. }) => {
                "SupportTicketEvent::CloseRequested".to_string()
            }
            Self::Notify(DispatchEvent::Confirmed(_)) => {
                "SupportTicketEvent::Closed".to_string()
            }
            Self::Notify(DispatchEvent::Failed(_)) => {
                "SupportTicketEvent::CloseFailed".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

/// Commands carry the timestamp the caller wants stamped on the resulting
/// event (handlers are deterministic; there is no injected clock).
#[derive(Debug, Clone)]
pub enum SupportTicketCommand {
    Open {
        ticket: TicketId,
        subject: String,
        at: chrono::DateTime<chrono::Utc>,
    },
    AwaitCustomer {
        at: chrono::DateTime<chrono::Utc>,
    },
    Close {
        at: chrono::DateTime<chrono::Utc>,
    },
    /// Delivered by the framework when the notify job settles.
    Notify(DispatchOutcome<NotifyClosed>),
}

impl From<DispatchOutcome<NotifyClosed>> for SupportTicketCommand {
    fn from(outcome: DispatchOutcome<NotifyClosed>) -> Self {
        Self::Notify(outcome)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum SupportTicketError {
    #[error("ticket already exists")]
    AlreadyOpen,
    #[error("ticket has not been opened")]
    NotOpen,
    #[error("ticket is already closed or closing")]
    AlreadyClosed,
    #[error(transparent)]
    Notify(#[from] DispatchRefused),
}

/// The job a `Close` command kicks off: notify the customer. The job carries
/// the full intent (ticket, subject, close timestamp) -- the `Dispatched`
/// event records it durably, and the fold derives ticket state from it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotifyClosed {
    pub ticket: TicketId,
    pub subject: String,
    pub closed_at: chrono::DateTime<chrono::Utc>,
}

/// Dependency bundle the worker injects (alongside the origin store, via
/// `JobInput`). A real app would hold an email client; here it prints.
#[derive(Clone, Default)]
pub struct Notifier;

impl Job for NotifyClosed {
    type Input = Notifier;
    type Output = ();
    type Error = Never;
    type Origin = SupportTicket;

    const WORKER_NAME: &'static str = "notify-closed";
    const KIND: &'static str = "notify-closed";

    fn label(&self) -> Label {
        Label::new(format!("notify-closed:{}", self.subject))
    }

    fn origin_id(&self) -> TicketId {
        self.ticket
    }

    async fn submit(
        &self,
        _ctx: &JobContext,
        _input: &Notifier,
    ) -> Result<JobOutcome<()>, JobFailure<Never>> {
        println!(
            "  [worker] notified customer that '{}' was closed",
            self.subject
        );
        Ok(JobOutcome::Done(()))
    }

    async fn reconcile(
        &self,
        _ctx: &JobContext,
        _input: &Notifier,
    ) -> Result<Reconciliation<()>, JobFailure<Never>> {
        // A duplicate notification is tolerable; treat an unknown fate as
        // settled rather than double-checking an email provider.
        Ok(Reconciliation::Settled(()))
    }
}

/// A standalone job (ADR-0007): origin-less background work enqueued directly
/// on the `JobRuntime` by reactors, pollers, or startup code -- never from a
/// command handler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepStaleTickets;

impl StandaloneJob for SweepStaleTickets {
    type Input = ();
    type Output = ();
    type Error = Never;

    const WORKER_NAME: &'static str = "sweep-stale-tickets";
    const KIND: &'static str = "sweep-stale-tickets";

    fn label(&self) -> Label {
        Label::new("sweep-stale-tickets")
    }

    async fn perform(
        &self,
        _ctx: &JobContext,
        _input: &(),
    ) -> Result<JobOutcome<()>, JobFailure<Never>> {
        println!("  [worker] swept stale tickets");
        Ok(JobOutcome::Done(()))
    }
}

#[async_trait]
impl EventSourced for SupportTicket {
    type Id = TicketId;
    type Event = SupportTicketEvent;
    type Command = SupportTicketCommand;
    type Error = SupportTicketError;
    type Jobs = event_sorcery::jobs![NotifyClosed];
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "SupportTicket";
    const PROJECTION: Table = Table("support_ticket_view");
    const SCHEMA_VERSION: u64 = 2;

    fn originate(event: &SupportTicketEvent) -> Option<Self> {
        match event {
            SupportTicketEvent::Opened {
                ticket,
                subject,
                at,
            } => Some(Self {
                ticket: *ticket,
                subject: subject.clone(),
                status: Status::Open,
                last_updated_at: *at,
                notify: DispatchedJob::Idle,
            }),
            SupportTicketEvent::AwaitingCustomer { .. } | SupportTicketEvent::Notify(_) => None,
        }
    }

    fn evolve(
        entity: &Self,
        event: &SupportTicketEvent,
    ) -> Result<Option<Self>, SupportTicketError> {
        match event {
            SupportTicketEvent::Opened { .. } => Ok(None),
            SupportTicketEvent::AwaitingCustomer { at } => Ok(Some(Self {
                status: Status::Pending,
                last_updated_at: *at,
                ..entity.clone()
            })),
            SupportTicketEvent::Notify(dispatch_event) => {
                let Ok(notify) = entity.notify.evolve(dispatch_event) else {
                    return Ok(None);
                };
                let (status, last_updated_at) = match dispatch_event {
                    DispatchEvent::Dispatched { job, .. } => (Status::Closing, job.closed_at),
                    DispatchEvent::Confirmed(_) => (Status::Closed, entity.last_updated_at),
                    // `Never` fails: the arm exists for exhaustiveness.
                    DispatchEvent::Failed(_) => (Status::Closing, entity.last_updated_at),
                };
                Ok(Some(Self {
                    status,
                    last_updated_at,
                    notify,
                    ..entity.clone()
                }))
            }
        }
    }

    async fn initialize(
        command: SupportTicketCommand,
    ) -> Result<Decision<Self>, SupportTicketError> {
        match command {
            SupportTicketCommand::Open {
                ticket,
                subject,
                at,
            } => Ok(Decision::Events(vec![SupportTicketEvent::Opened {
                ticket,
                subject,
                at,
            }])),
            SupportTicketCommand::AwaitCustomer { .. }
            | SupportTicketCommand::Close { .. }
            | SupportTicketCommand::Notify(_) => Err(SupportTicketError::NotOpen),
        }
    }

    async fn transition(
        &self,
        command: SupportTicketCommand,
    ) -> Result<Decision<Self>, SupportTicketError> {
        match command {
            SupportTicketCommand::Open { .. } => Err(SupportTicketError::AlreadyOpen),
            SupportTicketCommand::AwaitCustomer { at } => match self.status {
                Status::Closing | Status::Closed => Err(SupportTicketError::AlreadyClosed),
                Status::Open | Status::Pending => Ok(Decision::Events(vec![
                    SupportTicketEvent::AwaitingCustomer { at },
                ])),
            },
            SupportTicketCommand::Close { at } => match self.status {
                Status::Closing | Status::Closed => Err(SupportTicketError::AlreadyClosed),
                // Closing IS kicking off the notify job. The framework emits
                // the `Dispatched` event and enqueues in one transaction; the
                // ticket only reaches `Closed` when the verdict lands.
                Status::Open | Status::Pending => {
                    Ok(Decision::Dispatch(self.notify.dispatch(NotifyClosed {
                        ticket: self.ticket,
                        subject: self.subject.clone(),
                        closed_at: at,
                    })?))
                }
            },
            SupportTicketCommand::Notify(outcome) => {
                let events = self.notify.settle(outcome)?;
                Ok(Decision::Events(
                    events.into_iter().map(SupportTicketEvent::Notify).collect(),
                ))
            }
        }
    }
}

pub const STATUS: Column = Column("status");

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;

    use event_sorcery::{JobId, Settled, StoreBuilder, TestHarness, replay};

    use super::*;

    fn fixed_instant() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .to_utc()
    }

    fn opened(ticket: TicketId) -> SupportTicketEvent {
        SupportTicketEvent::Opened {
            ticket,
            subject: "printer on fire".to_string(),
            at: fixed_instant(),
        }
    }

    fn dispatched(ticket: TicketId, job_id: JobId) -> SupportTicketEvent {
        SupportTicketEvent::Notify(DispatchEvent::Dispatched {
            job_id,
            job: NotifyClosed {
                ticket,
                subject: "printer on fire".to_string(),
                closed_at: fixed_instant(),
            },
        })
    }

    #[tokio::test]
    async fn close_kicks_off_the_notify_job_instead_of_claiming_closed() {
        let events = TestHarness::<SupportTicket>::new()
            .given(vec![opened(TicketId(1))])
            .when(SupportTicketCommand::Close {
                at: fixed_instant(),
            })
            .await
            .events();

        // The only event a close can produce is the framework-built intent.
        assert!(matches!(
            events.as_slice(),
            [SupportTicketEvent::Notify(DispatchEvent::Dispatched { job, .. })]
                if job.subject == "printer on fire" && job.ticket == TicketId(1)
        ));
    }

    #[tokio::test]
    async fn closing_twice_is_refused_while_the_job_is_in_flight() {
        let error = TestHarness::<SupportTicket>::new()
            .given(vec![opened(TicketId(1)), dispatched(TicketId(1), JobId::new())])
            .when(SupportTicketCommand::Close {
                at: fixed_instant(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            event_sorcery::LifecycleError::Apply(SupportTicketError::AlreadyClosed)
        ));
    }

    #[test]
    fn replay_settles_to_closed_only_after_the_verdict() {
        let job_id = JobId::new();
        let in_flight: SupportTicket = replay([opened(TicketId(1)), dispatched(TicketId(1), job_id)])
            .unwrap()
            .unwrap();
        assert_eq!(in_flight.status, Status::Closing);

        let settled: SupportTicket = replay([
            opened(TicketId(1)),
            dispatched(TicketId(1), job_id),
            SupportTicketEvent::Notify(DispatchEvent::Confirmed(Settled::simulated(
                job_id,
                (),
                1,
            ))),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(settled.status, Status::Closed);
    }

    #[test]
    fn replay_rejects_history_without_genesis_event() {
        let result: Result<Option<SupportTicket>, _> = replay([SupportTicketEvent::AwaitingCustomer {
            at: fixed_instant(),
        }]);
        assert!(matches!(
            result,
            Err(event_sorcery::LifecycleError::EventCantOriginate { .. })
        ));
    }

    #[tokio::test]
    async fn delivered_verdict_settles_the_ticket_through_the_store() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let (store, projection) = StoreBuilder::<SupportTicket>::new(pool.clone())
            .build()
            .await
            .unwrap();

        let id = TicketId(9);
        store
            .send(
                &id,
                SupportTicketCommand::Open {
                    ticket: id,
                    subject: "printer on fire".to_string(),
                    at: fixed_instant(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                SupportTicketCommand::Close {
                    at: fixed_instant(),
                },
            )
            .await
            .unwrap();

        let closing = store.load(&id).await.unwrap().unwrap();
        assert_eq!(closing.status, Status::Closing);
        let DispatchedJob::InFlight { job_id } = closing.notify else {
            panic!("expected the notify job in flight, got {:?}", closing.notify);
        };

        // Simulate the framework delivering the settled verdict.
        store
            .send(
                &id,
                SupportTicketCommand::Notify(DispatchOutcome::simulated_confirmed(job_id, (), 1)),
            )
            .await
            .unwrap();

        let closed = store.load(&id).await.unwrap().unwrap();
        assert_eq!(closed.status, Status::Closed);

        let closed_rows = projection.filter(STATUS, &Status::Closed).await.unwrap();
        assert_eq!(closed_rows.len(), 1);
    }

    #[tokio::test]
    async fn projection_filter_returns_only_matching_status() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        let (store, projection) = StoreBuilder::<SupportTicket>::new(pool.clone())
            .build()
            .await
            .unwrap();

        for (id, subject) in [(TicketId(1), "a"), (TicketId(2), "b"), (TicketId(3), "c")] {
            store
                .send(
                    &id,
                    SupportTicketCommand::Open {
                        ticket: id,
                        subject: subject.to_string(),
                        at: fixed_instant(),
                    },
                )
                .await
                .unwrap();
        }
        store
            .send(
                &TicketId(2),
                SupportTicketCommand::AwaitCustomer {
                    at: fixed_instant(),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &TicketId(3),
                SupportTicketCommand::Close {
                    at: fixed_instant(),
                },
            )
            .await
            .unwrap();

        let open = projection.filter(STATUS, &Status::Open).await.unwrap();
        let pending = projection.filter(STATUS, &Status::Pending).await.unwrap();
        let closing = projection.filter(STATUS, &Status::Closing).await.unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(pending.len(), 1);
        assert_eq!(closing.len(), 1);
    }
}
