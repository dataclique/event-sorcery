//! Multi-entity `Reactor` wired across two stores, plus a single-entity
//! reactor running alongside an auto-wired `Materialized = Table`
//! projection.
//!
//! Domain:
//! - `Order` (`Materialized = Nil`): a tiny placed/filled/cancelled state
//!   machine.
//! - `Inventory` (`Materialized = Table`): stock-on-hand per item, with an
//!   auto-wired projection.
//! - `StockAlert` reactor: depends on both entities. Increments an
//!   `AtomicUsize` whenever stock crosses a low-water threshold *or* an
//!   order fills. Demonstrates `deps!` + `.on(...).on(...).exhaustive()`.
//! - `AuditLog` reactor: depends on `Order` only. Pushes a string per
//!   event into an in-memory log. Demonstrates the single-entity
//!   `OneOf::into_inner` pattern and the "custom reactor wired alongside
//!   the auto-projection" use case.
//!
//! `main()` walks through:
//! - sharing one `Arc<StockAlert>` across two `StoreBuilder` calls;
//! - wiring `AuditLog` on the `Order` builder via `.with()`;
//! - sending commands to both stores and observing reactor side-effects.
//!
//! Run with: `cargo run -p event-sorcery --example reactor`
//!
//! See `README.md` next to this file for design notes.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::sync::Mutex;

use event_sorcery::{EntityList, EventSourced, Never, Nil, Reactor, StoreBuilder, Table, deps};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Order {
    item: String,
    quantity: u32,
    status: OrderStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum OrderStatus {
    Placed,
    Filled,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum OrderEvent {
    Placed { item: String, quantity: u32 },
    Filled,
    Cancelled,
}

impl DomainEvent for OrderEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Placed { .. } => "OrderEvent::Placed".to_string(),
            Self::Filled => "OrderEvent::Filled".to_string(),
            Self::Cancelled => "OrderEvent::Cancelled".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone)]
enum OrderCommand {
    Place { item: String, quantity: u32 },
    Fill,
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
enum OrderError {
    #[error("order already placed")]
    AlreadyPlaced,
    #[error("order has not been placed")]
    NotPlaced,
    #[error("order is no longer open")]
    NotOpen,
}

#[async_trait]
impl EventSourced for Order {
    type Id = String;
    type Event = OrderEvent;
    type Command = OrderCommand;
    type Error = OrderError;
    type Services = ();
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "Order";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &OrderEvent) -> Option<Self> {
        match event {
            OrderEvent::Placed { item, quantity } => Some(Self {
                item: item.clone(),
                quantity: *quantity,
                status: OrderStatus::Placed,
            }),
            OrderEvent::Filled | OrderEvent::Cancelled => None,
        }
    }

    fn evolve(entity: &Self, event: &OrderEvent) -> Result<Option<Self>, OrderError> {
        match event {
            OrderEvent::Placed { .. } => Ok(None),
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

    async fn initialize(
        command: OrderCommand,
        _services: &(),
    ) -> Result<Vec<OrderEvent>, OrderError> {
        match command {
            OrderCommand::Place { item, quantity } => {
                Ok(vec![OrderEvent::Placed { item, quantity }])
            }
            OrderCommand::Fill | OrderCommand::Cancel => Err(OrderError::NotPlaced),
        }
    }

    async fn transition(
        &self,
        command: OrderCommand,
        _services: &(),
    ) -> Result<Vec<OrderEvent>, OrderError> {
        match command {
            OrderCommand::Place { .. } => Err(OrderError::AlreadyPlaced),
            OrderCommand::Fill => match self.status {
                OrderStatus::Placed => Ok(vec![OrderEvent::Filled]),
                OrderStatus::Filled | OrderStatus::Cancelled => Err(OrderError::NotOpen),
            },
            OrderCommand::Cancel => match self.status {
                OrderStatus::Placed => Ok(vec![OrderEvent::Cancelled]),
                OrderStatus::Filled | OrderStatus::Cancelled => Err(OrderError::NotOpen),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Inventory {
    item: String,
    on_hand: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum InventoryEvent {
    Initialized { item: String, on_hand: u32 },
    Restocked { added: u32 },
    Consumed { taken: u32 },
}

impl DomainEvent for InventoryEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Initialized { .. } => "InventoryEvent::Initialized".to_string(),
            Self::Restocked { .. } => "InventoryEvent::Restocked".to_string(),
            Self::Consumed { .. } => "InventoryEvent::Consumed".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone)]
enum InventoryCommand {
    Initialize { item: String, on_hand: u32 },
    Restock { added: u32 },
    Consume { taken: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
enum InventoryError {
    #[error("inventory already initialized")]
    AlreadyInitialized,
    #[error("inventory has not been initialized")]
    NotInitialized,
    #[error("on-hand overflow")]
    Overflow,
    #[error("only {on_hand} units on hand, cannot consume {taken}")]
    Underflow { on_hand: u32, taken: u32 },
}

#[async_trait]
impl EventSourced for Inventory {
    type Id = String;
    type Event = InventoryEvent;
    type Command = InventoryCommand;
    type Error = InventoryError;
    type Services = ();
    type Materialized = Table;

    const AGGREGATE_TYPE: &'static str = "Inventory";
    const PROJECTION: Table = Table("inventory_view");
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &InventoryEvent) -> Option<Self> {
        match event {
            InventoryEvent::Initialized { item, on_hand } => Some(Self {
                item: item.clone(),
                on_hand: *on_hand,
            }),
            InventoryEvent::Restocked { .. } | InventoryEvent::Consumed { .. } => None,
        }
    }

    fn evolve(entity: &Self, event: &InventoryEvent) -> Result<Option<Self>, InventoryError> {
        match event {
            InventoryEvent::Initialized { .. } => Ok(None),
            InventoryEvent::Restocked { added } => entity
                .on_hand
                .checked_add(*added)
                .map(|on_hand| {
                    Some(Self {
                        on_hand,
                        ..entity.clone()
                    })
                })
                .ok_or(InventoryError::Overflow),
            InventoryEvent::Consumed { taken } => entity
                .on_hand
                .checked_sub(*taken)
                .map(|on_hand| {
                    Some(Self {
                        on_hand,
                        ..entity.clone()
                    })
                })
                .ok_or(InventoryError::Underflow {
                    on_hand: entity.on_hand,
                    taken: *taken,
                }),
        }
    }

    async fn initialize(
        command: InventoryCommand,
        _services: &(),
    ) -> Result<Vec<InventoryEvent>, InventoryError> {
        match command {
            InventoryCommand::Initialize { item, on_hand } => {
                Ok(vec![InventoryEvent::Initialized { item, on_hand }])
            }
            InventoryCommand::Restock { .. } | InventoryCommand::Consume { .. } => {
                Err(InventoryError::NotInitialized)
            }
        }
    }

    async fn transition(
        &self,
        command: InventoryCommand,
        _services: &(),
    ) -> Result<Vec<InventoryEvent>, InventoryError> {
        match command {
            InventoryCommand::Initialize { .. } => Err(InventoryError::AlreadyInitialized),
            InventoryCommand::Restock { added } => {
                self.on_hand
                    .checked_add(added)
                    .ok_or(InventoryError::Overflow)?;
                Ok(vec![InventoryEvent::Restocked { added }])
            }
            InventoryCommand::Consume { taken } => {
                if taken > self.on_hand {
                    return Err(InventoryError::Underflow {
                        on_hand: self.on_hand,
                        taken,
                    });
                }
                Ok(vec![InventoryEvent::Consumed { taken }])
            }
        }
    }
}

/// Multi-entity reactor. Counts noteworthy events from both streams. The
/// `deps!` macro generates the `Dependent` and `HasEntity` impls; the
/// `.on(...).on(...).exhaustive()` chain enforces compile-time
/// exhaustiveness over the entity list.
struct StockAlert {
    low_water: u32,
    fills: AtomicUsize,
    low_stock_alerts: AtomicUsize,
}

impl StockAlert {
    fn new(low_water: u32) -> Self {
        Self {
            low_water,
            fills: AtomicUsize::new(0),
            low_stock_alerts: AtomicUsize::new(0),
        }
    }
}

deps!(StockAlert, [Order, Inventory]);

#[async_trait]
impl Reactor for StockAlert {
    type Error = Never;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        event
            .on(|_id: String, event: OrderEvent| async move {
                if matches!(event, OrderEvent::Filled) {
                    self.fills.fetch_add(1, Ordering::SeqCst);
                }
            })
            .on(|_id: String, event: InventoryEvent| async move {
                let new_on_hand = match event {
                    InventoryEvent::Initialized { on_hand, .. } => on_hand,
                    InventoryEvent::Restocked { .. } | InventoryEvent::Consumed { .. } => {
                        return;
                    }
                };
                if new_on_hand <= self.low_water {
                    self.low_stock_alerts.fetch_add(1, Ordering::SeqCst);
                }
            })
            .exhaustive()
            .await;
        Ok(())
    }
}

/// Single-entity reactor. Demonstrates `OneOf::into_inner` for reactors
/// with exactly one dependency, and the "custom reactor alongside the
/// auto-projection" pattern: wired via `.with()` on the `Order` builder
/// even though `Order` is `Materialized = Nil` (the same shape would
/// also work next to a `Materialized = Table` auto-projection).
struct AuditLog {
    entries: Mutex<Vec<String>>,
}

impl AuditLog {
    fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }

    async fn entries(&self) -> Vec<String> {
        self.entries.lock().await.clone()
    }
}

deps!(AuditLog, [Order]);

#[async_trait]
impl Reactor for AuditLog {
    type Error = Never;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        let (id, event) = event.into_inner();
        let line = format!("{id}: {}", event.event_type());
        self.entries.lock().await.push(line);
        Ok(())
    }
}

async fn create_inventory_view(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS inventory_view ( \
             view_id TEXT PRIMARY KEY, \
             version BIGINT NOT NULL, \
             payload JSON NOT NULL \
         )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect(":memory:").await?;
    sqlx::migrate!("../../migrations").run(&pool).await?;
    create_inventory_view(&pool).await?;

    // One reactor instance, shared across both StoreBuilder calls.
    let stock_alert = Arc::new(StockAlert::new(2));
    let audit_log = Arc::new(AuditLog::new());

    // Order is Materialized = Nil; AuditLog runs as the only reactor.
    let orders = StoreBuilder::<Order>::new(pool.clone())
        .with(stock_alert.clone())
        .with(audit_log.clone())
        .build(())
        .await?;

    // Inventory is Materialized = Table; StockAlert runs alongside the
    // auto-wired projection.
    let (inventory, inventory_projection) = StoreBuilder::<Inventory>::new(pool.clone())
        .with(stock_alert.clone())
        .build(())
        .await?;

    // Drive the system.
    inventory
        .send(
            &"widgets".to_string(),
            InventoryCommand::Initialize {
                item: "widgets".to_string(),
                on_hand: 1,
            },
        )
        .await?;
    inventory
        .send(
            &"widgets".to_string(),
            InventoryCommand::Restock { added: 5 },
        )
        .await?;

    orders
        .send(
            &"order-1".to_string(),
            OrderCommand::Place {
                item: "widgets".to_string(),
                quantity: 3,
            },
        )
        .await?;
    orders
        .send(&"order-1".to_string(), OrderCommand::Fill)
        .await?;
    inventory
        .send(
            &"widgets".to_string(),
            InventoryCommand::Consume { taken: 3 },
        )
        .await?;

    orders
        .send(
            &"order-2".to_string(),
            OrderCommand::Place {
                item: "widgets".to_string(),
                quantity: 1,
            },
        )
        .await?;
    orders
        .send(&"order-2".to_string(), OrderCommand::Cancel)
        .await?;

    println!(
        "stock_alert.fills            = {}",
        stock_alert.fills.load(Ordering::SeqCst)
    );
    println!(
        "stock_alert.low_stock_alerts = {} (1 -- the initial 1 unit was at/under the threshold)",
        stock_alert.low_stock_alerts.load(Ordering::SeqCst)
    );

    let log = audit_log.entries().await;
    println!("audit_log.entries            = {} lines", log.len());
    for line in &log {
        println!("    {line}");
    }

    let widgets = inventory_projection
        .load(&"widgets".to_string())
        .await?
        .ok_or("widgets missing")?;
    println!(
        "inventory_projection         = {{ item: {:?}, on_hand: {} }}",
        widgets.item, widgets.on_hand
    );

    Ok(())
}

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use event_sorcery::{ReactorHarness, SpyReactor};

    use super::*;

    #[tokio::test]
    async fn spy_reactor_captures_dispatched_order_events() {
        let spy = SpyReactor::<Order>::new();

        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        let store = StoreBuilder::<Order>::new(pool)
            .with(Arc::new(spy.clone()))
            .build(())
            .await
            .unwrap();

        store
            .send(
                &"order-1".to_string(),
                OrderCommand::Place {
                    item: "widgets".to_string(),
                    quantity: 3,
                },
            )
            .await
            .unwrap();
        store
            .send(&"order-1".to_string(), OrderCommand::Fill)
            .await
            .unwrap();

        let captured = spy.events().await;
        let kinds: Vec<_> = captured
            .iter()
            .map(|(_, event)| event.event_type())
            .collect();
        assert_eq!(kinds, vec!["OrderEvent::Placed", "OrderEvent::Filled"]);
    }

    #[tokio::test]
    async fn reactor_harness_dispatches_to_multi_entity_reactor() {
        let alert = StockAlert::new(2);
        let harness = ReactorHarness::new(alert);

        // Inject from each entity in turn -- HasEntity impls generated by
        // deps! resolve the correct OneOf depth.
        harness
            .receive::<Order>("order-1".to_string(), OrderEvent::Filled)
            .await
            .unwrap();
        harness
            .receive::<Inventory>(
                "widgets".to_string(),
                InventoryEvent::Initialized {
                    item: "widgets".to_string(),
                    on_hand: 1,
                },
            )
            .await
            .unwrap();

        let alert = harness.inner();
        assert_eq!(alert.fills.load(Ordering::SeqCst), 1);
        assert_eq!(alert.low_stock_alerts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn shared_reactor_observes_events_across_two_stores() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();
        create_inventory_view(&pool).await.unwrap();

        let alert = Arc::new(StockAlert::new(2));

        let orders = StoreBuilder::<Order>::new(pool.clone())
            .with(alert.clone())
            .build(())
            .await
            .unwrap();
        let (inventory, _projection) = StoreBuilder::<Inventory>::new(pool.clone())
            .with(alert.clone())
            .build(())
            .await
            .unwrap();

        inventory
            .send(
                &"widgets".to_string(),
                InventoryCommand::Initialize {
                    item: "widgets".to_string(),
                    on_hand: 1,
                },
            )
            .await
            .unwrap();
        orders
            .send(
                &"order-1".to_string(),
                OrderCommand::Place {
                    item: "widgets".to_string(),
                    quantity: 1,
                },
            )
            .await
            .unwrap();
        orders
            .send(&"order-1".to_string(), OrderCommand::Fill)
            .await
            .unwrap();

        assert_eq!(alert.fills.load(Ordering::SeqCst), 1);
        assert_eq!(alert.low_stock_alerts.load(Ordering::SeqCst), 1);
    }
}
