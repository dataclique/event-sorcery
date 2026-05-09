# basic_entity

Smallest useful event-sourced entity wired through `event-sorcery`. A
`BankAccount` aggregate without a materialized view, with a typed `AccountId`, a
`thiserror`-derived domain error, and an opt-in compaction policy so we can
demonstrate the full lifecycle.

## Run

```bash
cargo run -p event-sorcery --example basic_entity
cargo nextest run -p event-sorcery --example basic_entity --features test-support
```

The first command runs `main()` end-to-end against an in-memory SQLite event
store. The second runs the example's `#[cfg(test)] mod tests` block covering
`replay`, `TestHarness`, `TestStore`, and schema reconciliation.

## What it covers

`main()` walks the canonical pattern for a non-projected aggregate:

| Capability                                     | Where in `main.rs`       |
| ---------------------------------------------- | ------------------------ |
| Implementing `EventSourced` (all four methods) | `impl EventSourced for…` |
| Typed `Id` with `Display` + `FromStr`          | `struct AccountId(u64)`  |
| Domain error with `thiserror`                  | `enum BankAccountError`  |
| `CompactionPolicy::CompactAfterSnapshot`       | trait constants          |
| `StoreBuilder::<…>::new(pool).build(())`       | start of `main`          |
| `Store::send`, `Store::load`                   | `alice` flow             |
| `send_command` (no `Store` needed)             | `carol` setup            |
| `load_entity` (no `Store` needed)              | `carol_via_helper`       |
| `count_aggregates`, `load_all_ids`             | enumeration block        |
| `load_ids_paginated`                           | enumeration block        |
| `compact_events`, `incremental_vacuum`         | maintenance block        |

The `#[cfg(test)] mod tests` block adds:

| Capability                                              | Test                                             |
| ------------------------------------------------------- | ------------------------------------------------ |
| `replay::<Entity>(events)`                              | `replay_*` cases                                 |
| `TestHarness::with(...).given(...).when(...)`           | `deposit_*`, `withdraw_*` cases                  |
| `TestStore::<Entity>::new(...)`                         | `test_store_round_trip_*`                        |
| `LifecycleError::EventCantOriginate` / `Apply` variants | the same cases (assertions)                      |
| `SCHEMA_VERSION` reconciliation safety rail             | `schema_version_bump_on_compactable_aggregate_*` |

## Why these choices

**`type Materialized = Nil`.** The aggregate has no materialized view. Reads go
through `Store::load` (replay from events) or `load_entity` for contexts that
don't hold a `Store`. Use `Materialized = Nil` when there is no read-side query
that benefits from a denormalized table — see the
[`projection`](../projection/README.md) example for the alternative.

**`CompactionPolicy::CompactAfterSnapshot`.** Lets `compact_events` delete
events covered by a snapshot. Suitable for high-frequency observational
aggregates (e.g., the `InventorySnapshot` pattern in `st0x.liquidity`).
**Financial-audit aggregates must keep the default `CompactionPolicy::Retain`**
— losing events makes audits impossible.

**`SNAPSHOT_SIZE = 1`.** With compaction enabled, every command writes a
snapshot, so the snapshot is the durable pre-compaction state. Retained
aggregates with low command counts can use a higher value (10–50) to reduce
write amplification.

**Schema-version safety rail.** Bumping `SCHEMA_VERSION` on a
`CompactAfterSnapshot` aggregate fails with `CompactedSnapshotClear` because
clearing the snapshot would erase the only durable record of pre-compaction
state. The example test asserts this so consumers know they need an external
rebuild path before bumping the version on a compactable aggregate.
Retain-policy aggregates clear snapshots silently on a version bump.
