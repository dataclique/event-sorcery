//! Basic `EventSourced` entity end-to-end against a real SQLite event store.
//!
//! Domain: a `BankAccount` aggregate with a typed `AccountId` newtype and a
//! `thiserror`-derived domain error. The aggregate has no materialized view
//! (`type Materialized = Nil`), opts into `CompactionPolicy::CompactAfterSnapshot`
//! so we can demonstrate event compaction, and uses `SNAPSHOT_SIZE = 1` so each
//! command writes a snapshot immediately.
//!
//! `main()` walks through:
//! - building a `Store` with `StoreBuilder`;
//! - sending commands via `Store::send` and reading state via `Store::load`;
//! - reading state without a `Store` via `load_entity`;
//! - sending a command without a `Store` via `send_command`;
//! - enumerating aggregates via `load_all_ids`, `count_aggregates`, and
//!   `load_ids_paginated`;
//! - reclaiming events with `compact_events` and pages with `incremental_vacuum`.
//!
//! Run with: `cargo run -p event-sorcery --example basic_entity`
//!
//! See `README.md` next to this file for design notes.

use std::error::Error;
use std::fmt::{self, Display};
use std::str::FromStr;

use async_trait::async_trait;
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use event_sorcery::{
    CompactionPolicy, EventSourced, Nil, StoreBuilder, compact_events, count_aggregates,
    incremental_vacuum, load_all_ids, load_entity, load_ids_paginated, send_command,
};

/// Strongly-typed account identifier. Demonstrates a non-trivial `Id`
/// (newtype around `u64`) with `Display` + `FromStr` for the `EventSourced`
/// boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct AccountId(u64);

impl Display for AccountId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl FromStr for AccountId {
    type Err = std::num::ParseIntError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse::<u64>().map(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BankAccount {
    holder: String,
    balance: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum BankAccountEvent {
    Opened { holder: String, opening: u64 },
    Deposited { amount: u64 },
    Withdrawn { amount: u64 },
}

impl DomainEvent for BankAccountEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Opened { .. } => "BankAccountEvent::Opened".to_string(),
            Self::Deposited { .. } => "BankAccountEvent::Deposited".to_string(),
            Self::Withdrawn { .. } => "BankAccountEvent::Withdrawn".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone)]
enum BankAccountCommand {
    Open { holder: String, opening: u64 },
    Deposit { amount: u64 },
    Withdraw { amount: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
enum BankAccountError {
    #[error("account already exists")]
    AlreadyOpen,
    #[error("account is not open")]
    NotOpen,
    #[error("balance overflow")]
    Overflow,
    #[error("insufficient funds: balance {balance}, withdrawal {amount}")]
    InsufficientFunds { balance: u64, amount: u64 },
}

#[async_trait]
impl EventSourced for BankAccount {
    type Id = AccountId;
    type Event = BankAccountEvent;
    type Command = BankAccountCommand;
    type Error = BankAccountError;
    type Services = ();
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "BankAccount";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 1;
    const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::CompactAfterSnapshot;
    const SNAPSHOT_SIZE: usize = 1;

    fn originate(event: &BankAccountEvent) -> Option<Self> {
        match event {
            BankAccountEvent::Opened { holder, opening } => Some(Self {
                holder: holder.clone(),
                balance: *opening,
            }),
            BankAccountEvent::Deposited { .. } | BankAccountEvent::Withdrawn { .. } => None,
        }
    }

    fn evolve(entity: &Self, event: &BankAccountEvent) -> Result<Option<Self>, BankAccountError> {
        match event {
            BankAccountEvent::Opened { .. } => Ok(None),
            BankAccountEvent::Deposited { amount } => entity
                .balance
                .checked_add(*amount)
                .map(|balance| {
                    Some(Self {
                        balance,
                        ..entity.clone()
                    })
                })
                .ok_or(BankAccountError::Overflow),
            BankAccountEvent::Withdrawn { amount } => entity
                .balance
                .checked_sub(*amount)
                .map(|balance| {
                    Some(Self {
                        balance,
                        ..entity.clone()
                    })
                })
                .ok_or(BankAccountError::InsufficientFunds {
                    balance: entity.balance,
                    amount: *amount,
                }),
        }
    }

    async fn initialize(
        command: BankAccountCommand,
        _services: &(),
    ) -> Result<Vec<BankAccountEvent>, BankAccountError> {
        match command {
            BankAccountCommand::Open { holder, opening } => {
                Ok(vec![BankAccountEvent::Opened { holder, opening }])
            }
            BankAccountCommand::Deposit { .. } | BankAccountCommand::Withdraw { .. } => {
                Err(BankAccountError::NotOpen)
            }
        }
    }

    async fn transition(
        &self,
        command: BankAccountCommand,
        _services: &(),
    ) -> Result<Vec<BankAccountEvent>, BankAccountError> {
        match command {
            BankAccountCommand::Open { .. } => Err(BankAccountError::AlreadyOpen),
            BankAccountCommand::Deposit { amount } => {
                self.balance
                    .checked_add(amount)
                    .ok_or(BankAccountError::Overflow)?;
                Ok(vec![BankAccountEvent::Deposited { amount }])
            }
            BankAccountCommand::Withdraw { amount } => {
                if amount > self.balance {
                    return Err(BankAccountError::InsufficientFunds {
                        balance: self.balance,
                        amount,
                    });
                }
                Ok(vec![BankAccountEvent::Withdrawn { amount }])
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = SqlitePool::connect(":memory:").await?;
    sqlx::migrate!("../../migrations").run(&pool).await?;

    let store = StoreBuilder::<BankAccount>::new(pool.clone())
        .build(())
        .await?;

    // Open three accounts and exercise the typed Store API.
    let alice = AccountId(1);
    let bob = AccountId(2);
    let carol = AccountId(3);

    store
        .send(
            &alice,
            BankAccountCommand::Open {
                holder: "Alice".to_string(),
                opening: 100,
            },
        )
        .await?;
    store
        .send(&alice, BankAccountCommand::Deposit { amount: 50 })
        .await?;
    store
        .send(&alice, BankAccountCommand::Withdraw { amount: 30 })
        .await?;

    store
        .send(
            &bob,
            BankAccountCommand::Open {
                holder: "Bob".to_string(),
                opening: 0,
            },
        )
        .await?;

    // send_command demonstrates the standalone command path -- useful for
    // CLI/migration code that doesn't hold a long-lived Store.
    send_command::<BankAccount>(
        &pool,
        &carol,
        BankAccountCommand::Open {
            holder: "Carol".to_string(),
            opening: 250,
        },
        (),
    )
    .await?;

    // Read Alice's state through the Store.
    let alice_via_store = store.load(&alice).await?.ok_or("alice missing")?;
    println!(
        "alice via Store::load        = balance {}",
        alice_via_store.balance
    );

    // Read Carol's state without a Store -- demonstrates load_entity.
    let carol_via_helper = load_entity::<BankAccount>(&pool, &carol)
        .await?
        .ok_or("carol missing")?;
    println!(
        "carol via load_entity        = balance {}",
        carol_via_helper.balance
    );

    // Enumerate aggregates -- mirrors how dashboards/cleanup loops scan the store.
    let total = count_aggregates::<BankAccount>(&pool).await?;
    let all_ids = load_all_ids::<BankAccount>(&pool).await?;
    let first_two = load_ids_paginated::<BankAccount>(&pool, 2, 0).await?;

    println!("count_aggregates             = {total}");
    println!("load_all_ids                 = {all_ids:?}");
    println!("load_ids_paginated(2, 0)     = {first_two:?}");

    // Reclaim events that are already covered by snapshots, then reclaim
    // freelist pages. CompactionPolicy::CompactAfterSnapshot means previous
    // events for live aggregates are now safe to delete.
    let deleted = compact_events::<BankAccount>(&pool).await?;
    incremental_vacuum(&pool, 100).await?;
    println!("compact_events deleted       = {deleted} events");

    Ok(())
}

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use event_sorcery::{LifecycleError, TestHarness, TestStore, replay};

    use super::*;

    #[test]
    fn replay_reconstructs_balance_from_events() {
        let account = replay::<BankAccount>(vec![
            BankAccountEvent::Opened {
                holder: "Alice".to_string(),
                opening: 100,
            },
            BankAccountEvent::Deposited { amount: 50 },
            BankAccountEvent::Withdrawn { amount: 30 },
        ])
        .unwrap()
        .unwrap();

        assert_eq!(account.holder, "Alice");
        assert_eq!(account.balance, 120);
    }

    #[test]
    fn replay_rejects_history_without_genesis_event() {
        let error =
            replay::<BankAccount>(vec![BankAccountEvent::Deposited { amount: 10 }]).unwrap_err();

        assert!(matches!(error, LifecycleError::EventCantOriginate { .. }));
    }

    #[tokio::test]
    async fn deposit_after_open_emits_deposited_event() {
        TestHarness::<BankAccount>::with(())
            .given(vec![BankAccountEvent::Opened {
                holder: "Alice".to_string(),
                opening: 0,
            }])
            .when(BankAccountCommand::Deposit { amount: 25 })
            .await
            .then_expect_events(&[BankAccountEvent::Deposited { amount: 25 }]);
    }

    #[tokio::test]
    async fn withdraw_more_than_balance_returns_domain_error() {
        let error = TestHarness::<BankAccount>::with(())
            .given(vec![BankAccountEvent::Opened {
                holder: "Alice".to_string(),
                opening: 10,
            }])
            .when(BankAccountCommand::Withdraw { amount: 50 })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(BankAccountError::InsufficientFunds {
                balance: 10,
                amount: 50,
            })
        ));
    }

    #[tokio::test]
    async fn test_store_round_trip_against_in_memory_store() {
        let store = TestStore::<BankAccount>::new(());
        let id = AccountId(1);

        store
            .send(
                &id,
                BankAccountCommand::Open {
                    holder: "Alice".to_string(),
                    opening: 100,
                },
            )
            .await
            .unwrap();
        store
            .send(&id, BankAccountCommand::Deposit { amount: 25 })
            .await
            .unwrap();

        let account = store.load(&id).await.unwrap().unwrap();
        assert_eq!(account.balance, 125);
    }

    /// `CompactAfterSnapshot` aggregates cannot have their snapshots
    /// auto-cleared on a `SCHEMA_VERSION` bump, because the snapshot is the
    /// only durable record of pre-compaction state. The reconciler refuses
    /// to clear and returns `CompactedSnapshotClear`, asking the operator
    /// to rebuild from an external source first.
    ///
    /// Retain-policy aggregates (the default) clear snapshots silently on
    /// version bumps -- the next replay rebuilds from the full event stream.
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct BankAccountV2 {
        holder: String,
        balance: u64,
    }

    #[async_trait]
    impl EventSourced for BankAccountV2 {
        type Id = AccountId;
        type Event = BankAccountEvent;
        type Command = BankAccountCommand;
        type Error = BankAccountError;
        type Services = ();
        type Materialized = Nil;

        const AGGREGATE_TYPE: &'static str = "BankAccount";
        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 2;
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::CompactAfterSnapshot;
        const SNAPSHOT_SIZE: usize = 1;

        fn originate(event: &BankAccountEvent) -> Option<Self> {
            BankAccount::originate(event).map(|account| Self {
                holder: account.holder,
                balance: account.balance,
            })
        }

        fn evolve(
            entity: &Self,
            event: &BankAccountEvent,
        ) -> Result<Option<Self>, BankAccountError> {
            let cloned = BankAccount {
                holder: entity.holder.clone(),
                balance: entity.balance,
            };
            BankAccount::evolve(&cloned, event).map(|maybe| {
                maybe.map(|account| Self {
                    holder: account.holder,
                    balance: account.balance,
                })
            })
        }

        async fn initialize(
            command: BankAccountCommand,
            services: &(),
        ) -> Result<Vec<BankAccountEvent>, BankAccountError> {
            BankAccount::initialize(command, services).await
        }

        async fn transition(
            &self,
            command: BankAccountCommand,
            services: &(),
        ) -> Result<Vec<BankAccountEvent>, BankAccountError> {
            let cloned = BankAccount {
                holder: self.holder.clone(),
                balance: self.balance,
            };
            BankAccount::transition(&cloned, command, services).await
        }
    }

    #[tokio::test]
    async fn schema_version_bump_on_compactable_aggregate_is_rejected() {
        use event_sorcery::ReconcileError;

        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("../../migrations").run(&pool).await.unwrap();

        // V1 startup: open an account so a snapshot is written.
        {
            let store = StoreBuilder::<BankAccount>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            store
                .send(
                    &AccountId(1),
                    BankAccountCommand::Open {
                        holder: "Alice".to_string(),
                        opening: 100,
                    },
                )
                .await
                .unwrap();
        }

        // V2 startup: same AGGREGATE_TYPE, bumped SCHEMA_VERSION. Because the
        // aggregate is compactable, the reconciler refuses to silently drop
        // snapshots and surfaces CompactedSnapshotClear. The operator must
        // rebuild from an external source before bumping the version.
        let result = StoreBuilder::<BankAccountV2>::new(pool.clone())
            .build(())
            .await;

        let error = match result {
            Ok(_) => panic!("expected reconciler to reject the schema bump"),
            Err(error) => error,
        };

        match error {
            ReconcileError::CompactedSnapshotClear {
                aggregate,
                old_version,
                new_version,
            } => {
                assert_eq!(aggregate, "BankAccount");
                assert_eq!(old_version, Some(1));
                assert_eq!(new_version, 2);
            }
            other => panic!("expected CompactedSnapshotClear, got: {other:?}"),
        }
    }
}
