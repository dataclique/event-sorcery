//! `Order` aggregate: placing an order KICKS OFF a durable
//! [`SendOrderConfirmation`] job (ADR-0009). The `Dispatched` intent commits
//! atomically with the enqueue, the order shows `PendingConfirmation`, and
//! only the worker's delivered verdict moves it to `Placed` -- the handler
//! cannot claim the confirmation happened. `Fill`/`Cancel` are pure domain
//! events on a placed order.
//!
//! Orders hit a short-lived terminal state (filled or cancelled), so the
//! aggregate opts into `CompactionPolicy::CompactAfterSnapshot`. Once a
//! snapshot covers an event, `compact_events` can reclaim it; the snapshot
//! becomes the durable record of pre-compaction state, hence
//! `SNAPSHOT_SIZE = 1`. Compactable entities must be `Materialized = Nil`
//! -- the projection rebuild path reads from the events table and would
//! miss compacted aggregates.

use std::fmt::{self, Display};
use std::str::FromStr;

use async_trait::async_trait;
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use event_sorcery::{
    CompactionPolicy, Decision, DispatchEvent, DispatchOutcome, DispatchRefused, DispatchedJob,
    EventSourced, Job, JobContext, JobFailure, JobOutcome, Label, Never, Nil, Reconciliation,
};

use crate::inventory::Sku;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OrderId(pub u64);

impl Display for OrderId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "O-{:04}", self.0)
    }
}

impl FromStr for OrderId {
    type Err = ParseOrderIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let suffix = value
            .strip_prefix("O-")
            .ok_or(ParseOrderIdError::MissingPrefix)?;
        let raw = suffix.parse::<u64>()?;
        Ok(Self(raw))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseOrderIdError {
    #[error("order id must start with 'O-'")]
    MissingPrefix,
    #[error("order id suffix is not numeric: {0}")]
    NotNumeric(#[from] std::num::ParseIntError),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub item: Sku,
    pub quantity: u32,
    pub status: OrderStatus,
    pub confirmation: DispatchedJob<SendOrderConfirmation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    /// The confirmation job is in flight; the order is not yet placed.
    PendingConfirmation,
    Placed,
    Filled,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderEvent {
    Confirmation(DispatchEvent<SendOrderConfirmation>),
    Filled,
    Cancelled,
}

impl From<DispatchEvent<SendOrderConfirmation>> for OrderEvent {
    fn from(event: DispatchEvent<SendOrderConfirmation>) -> Self {
        Self::Confirmation(event)
    }
}

impl DomainEvent for OrderEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Confirmation(DispatchEvent::Dispatched { .. }) => {
                "OrderEvent::Placed".to_string()
            }
            Self::Confirmation(DispatchEvent::Confirmed(_)) => {
                "OrderEvent::ConfirmationSent".to_string()
            }
            Self::Confirmation(DispatchEvent::Failed(_)) => {
                "OrderEvent::ConfirmationFailed".to_string()
            }
            Self::Filled => "OrderEvent::Filled".to_string(),
            Self::Cancelled => "OrderEvent::Cancelled".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone)]
pub enum OrderCommand {
    Place { order: OrderId, item: Sku, quantity: u32 },
    Fill,
    Cancel,
    /// Delivered by the framework when the confirmation job settles.
    Confirmation(DispatchOutcome<SendOrderConfirmation>),
}

impl From<DispatchOutcome<SendOrderConfirmation>> for OrderCommand {
    fn from(outcome: DispatchOutcome<SendOrderConfirmation>) -> Self {
        Self::Confirmation(outcome)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum OrderError {
    #[error("order already placed")]
    AlreadyPlaced,
    #[error("order has not been placed")]
    NotPlaced,
    #[error("order is not confirmed yet")]
    NotConfirmed,
    #[error("order is no longer open")]
    NotOpen,
    #[error(transparent)]
    Confirmation(#[from] DispatchRefused),
}

/// The job a `Place` command kicks off: confirm the order to the customer.
/// The job carries the full intent, so the `Dispatched` event durably records
/// the order and `originate` derives the entity state from it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendOrderConfirmation {
    pub order: OrderId,
    pub item: Sku,
    pub quantity: u32,
}

/// Dependency bundle the worker injects (alongside the origin store, via
/// `JobInput`). A real app would hold an email/SMS client; here it prints.
#[derive(Clone, Default)]
pub struct Confirmer;

impl Job for SendOrderConfirmation {
    type Input = Confirmer;
    type Output = ();
    type Error = Never;
    type Origin = Order;

    const WORKER_NAME: &'static str = "send-order-confirmation";
    const KIND: &'static str = "send-order-confirmation";

    fn label(&self) -> Label {
        Label::new(format!("send-order-confirmation:{}", self.item))
    }

    fn origin_id(&self) -> OrderId {
        self.order
    }

    async fn submit(
        &self,
        _ctx: &JobContext,
        _input: &Confirmer,
    ) -> Result<JobOutcome<()>, JobFailure<Never>> {
        println!(
            "  [worker] sent confirmation for {} x{}",
            self.item, self.quantity
        );
        Ok(JobOutcome::Done(()))
    }

    async fn reconcile(
        &self,
        _ctx: &JobContext,
        _input: &Confirmer,
    ) -> Result<Reconciliation<()>, JobFailure<Never>> {
        // A duplicate confirmation email is tolerable; treat an unknown fate
        // as settled.
        Ok(Reconciliation::Settled(()))
    }
}

#[async_trait]
impl EventSourced for Order {
    type Id = OrderId;
    type Event = OrderEvent;
    type Command = OrderCommand;
    type Error = OrderError;
    type Jobs = event_sorcery::jobs![SendOrderConfirmation];
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "Order";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 2;
    const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::CompactAfterSnapshot;
    const SNAPSHOT_SIZE: usize = 1;

    fn originate(event: &OrderEvent) -> Option<Self> {
        match event {
            // The intent IS the job: the order is born from the dispatched
            // confirmation, carrying item and quantity.
            OrderEvent::Confirmation(dispatch_event @ DispatchEvent::Dispatched { job, .. }) => {
                let confirmation = DispatchedJob::Idle.evolve(dispatch_event).ok()?;
                Some(Self {
                    item: job.item.clone(),
                    quantity: job.quantity,
                    status: OrderStatus::PendingConfirmation,
                    confirmation,
                })
            }
            OrderEvent::Confirmation(DispatchEvent::Confirmed(_) | DispatchEvent::Failed(_))
            | OrderEvent::Filled
            | OrderEvent::Cancelled => None,
        }
    }

    fn evolve(entity: &Self, event: &OrderEvent) -> Result<Option<Self>, OrderError> {
        match event {
            OrderEvent::Confirmation(dispatch_event) => {
                let Ok(confirmation) = entity.confirmation.evolve(dispatch_event) else {
                    return Ok(None);
                };
                let status = match dispatch_event {
                    DispatchEvent::Dispatched { .. } => OrderStatus::PendingConfirmation,
                    DispatchEvent::Confirmed(_) => OrderStatus::Placed,
                    // Unreachable while `SendOrderConfirmation::Error = Never`.
                    // If that error type ever becomes real, this arm leaves the
                    // order stuck in PendingConfirmation -- add a failed status
                    // (or allow cancellation from here) at the same time.
                    DispatchEvent::Failed(_) => OrderStatus::PendingConfirmation,
                };
                Ok(Some(Self {
                    status,
                    confirmation,
                    ..entity.clone()
                }))
            }
            OrderEvent::Filled => Ok(Some(Self {
                status: OrderStatus::Filled,
                ..entity.clone()
            })),
            OrderEvent::Cancelled => Ok(Some(Self {
                status: OrderStatus::Cancelled,
                ..entity.clone()
            })),
        }
    }

    async fn initialize(command: OrderCommand) -> Result<Decision<Self>, OrderError> {
        match command {
            // Placing IS kicking off the confirmation job; the framework
            // commits the `Dispatched` intent and the enqueue together.
            OrderCommand::Place {
                order,
                item,
                quantity,
            } => Ok(Decision::Dispatch(DispatchedJob::Idle.dispatch(
                SendOrderConfirmation {
                    order,
                    item,
                    quantity,
                },
            )?)),
            OrderCommand::Fill | OrderCommand::Cancel | OrderCommand::Confirmation(_) => {
                Err(OrderError::NotPlaced)
            }
        }
    }

    async fn transition(&self, command: OrderCommand) -> Result<Decision<Self>, OrderError> {
        match command {
            OrderCommand::Place { .. } => Err(OrderError::AlreadyPlaced),
            OrderCommand::Fill => match self.status {
                OrderStatus::PendingConfirmation => Err(OrderError::NotConfirmed),
                OrderStatus::Placed => Ok(Decision::Events(vec![OrderEvent::Filled])),
                OrderStatus::Filled | OrderStatus::Cancelled => Err(OrderError::NotOpen),
            },
            OrderCommand::Cancel => match self.status {
                OrderStatus::PendingConfirmation => Err(OrderError::NotConfirmed),
                OrderStatus::Placed => Ok(Decision::Events(vec![OrderEvent::Cancelled])),
                OrderStatus::Filled | OrderStatus::Cancelled => Err(OrderError::NotOpen),
            },
            OrderCommand::Confirmation(outcome) => {
                let events = self.confirmation.settle(outcome)?;
                Ok(Decision::Events(
                    events.into_iter().map(OrderEvent::Confirmation).collect(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use event_sorcery::{JobId, Settled, TestHarness};

    use super::*;

    fn widgets() -> Sku {
        Sku("widgets".to_string())
    }

    fn placed(order: OrderId, job_id: JobId, quantity: u32) -> Vec<OrderEvent> {
        vec![
            OrderEvent::Confirmation(DispatchEvent::Dispatched {
                job_id,
                job: SendOrderConfirmation {
                    order,
                    item: widgets(),
                    quantity,
                },
            }),
            OrderEvent::Confirmation(DispatchEvent::Confirmed(Settled::simulated(
                job_id,
                (),
                1,
            ))),
        ]
    }

    #[tokio::test]
    async fn fill_after_confirmed_place_emits_filled_event() {
        TestHarness::<Order>::new()
            .given(placed(OrderId(1), JobId::new(), 3))
            .when(OrderCommand::Fill)
            .await
            .then_expect_events(&[OrderEvent::Filled]);
    }

    #[tokio::test]
    async fn confirmation_verdict_transitions_pending_to_placed() {
        let job_id = JobId::new();
        TestHarness::<Order>::new()
            .given(vec![OrderEvent::Confirmation(DispatchEvent::Dispatched {
                job_id,
                job: SendOrderConfirmation {
                    order: OrderId(1),
                    item: widgets(),
                    quantity: 3,
                },
            })])
            .when(OrderCommand::Confirmation(
                event_sorcery::DispatchOutcome::simulated_confirmed(job_id, (), 1),
            ))
            .await
            .then_expect_events(&[OrderEvent::Confirmation(DispatchEvent::Confirmed(
                Settled::simulated(job_id, (), 1),
            ))]);
    }

    #[tokio::test]
    async fn fill_before_the_confirmation_settles_is_refused() {
        let job_id = JobId::new();
        let error = TestHarness::<Order>::new()
            .given(vec![OrderEvent::Confirmation(DispatchEvent::Dispatched {
                job_id,
                job: SendOrderConfirmation {
                    order: OrderId(1),
                    item: widgets(),
                    quantity: 3,
                },
            })])
            .when(OrderCommand::Fill)
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            event_sorcery::LifecycleError::Apply(OrderError::NotConfirmed)
        ));
    }

    #[tokio::test]
    async fn cancel_filled_order_returns_not_open() {
        let mut history = placed(OrderId(1), JobId::new(), 1);
        history.push(OrderEvent::Filled);
        let error = TestHarness::<Order>::new()
            .given(history)
            .when(OrderCommand::Cancel)
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            event_sorcery::LifecycleError::Apply(OrderError::NotOpen)
        ));
    }
}
