# reactor

Multi-entity `Reactor` wired across two stores, plus a single-entity reactor
running alongside an auto-wired `Materialized = Table` projection. Together they
exercise every public capability around `deps!`, `OneOf`, and
`StoreBuilder::with`.

## Run

```bash
cargo run -p event-sorcery --example reactor
cargo nextest run -p event-sorcery --example reactor --features test-support
```

The first command runs `main()` end-to-end: an order/inventory flow where one
`StockAlert` reactor watches both event streams and an `AuditLog` reactor
records every `Order` event. The second runs the test module covering
`SpyReactor`, `ReactorHarness`, and a SQLite e2e flow.

## What it covers

| Capability                                            | Where in `main.rs`                      |
| ----------------------------------------------------- | --------------------------------------- |
| Two entities (`Materialized = Nil` and `= Table`)     | `Order` and `Inventory` impls           |
| `deps!(Reactor, [A, B])` (multi-entity)               | `deps!(StockAlert, [Order, Inventory])` |
| `deps!(Reactor, [A])` (single-entity)                 | `deps!(AuditLog, [Order])`              |
| `event.on(...).on(...).exhaustive().await`            | `StockAlert::react`                     |
| `OneOf::into_inner` for single-entity events          | `AuditLog::react`                       |
| Sharing one `Arc<R>` across multiple `StoreBuilder`s  | `stock_alert.clone()` on both builders  |
| Custom reactor wired alongside an auto-projection     | `StockAlert` on the `Inventory` builder |
| `StoreBuilder::with` on a `Materialized = Nil` entity | `AuditLog` on the `Order` builder       |

The `#[cfg(all(test, feature = "test-support"))] mod tests` block adds:

| Capability                                       | Test                                           |
| ------------------------------------------------ | ---------------------------------------------- |
| `SpyReactor` capturing live dispatch via `Store` | `spy_reactor_captures_dispatched_order_events` |
| `ReactorHarness::receive::<Entity>` (multi)      | `reactor_harness_dispatches_to_multi_entity_…` |
| Same `Arc<Reactor>` across two stores            | `shared_reactor_observes_events_across_two_…`  |

## Why these choices

**`deps!` macro.** Declares the entity dependency list once. The
statement-position form (`deps!(StockAlert, [Order, Inventory])`) generates both
the `Dependent` impl (the type-level entity list) and the `HasEntity` impls used
by `ReactorHarness::receive`. The reactor then computes its event union as
`OneOf<(OrderId, OrderEvent),
OneOf<(InventoryId, InventoryEvent), Never>>`
automatically.

**`.on(...).on(...).exhaustive()`.** Each `.on()` consumes one entity from the
type-level list. `.exhaustive()` requires the remaining tail to be `Never`, so
missing a handler is a compile error rather than a runtime fallthrough. For
single-entity reactors there is the dual `event.into_inner()` shorthand — see
`AuditLog::react`.

**Sharing reactors via `Arc`.** A multi-entity reactor depends on multiple
stores, so the same `Arc<R>` is wired into each entity's `StoreBuilder` via
`.with(reactor.clone())`. cqrs-es dispatches each event to every registered
reactor for that aggregate type; cloning the `Arc` does _not_ duplicate the
reactor's state.

**Custom reactor + auto-wired projection.** The `Materialized = Table`
auto-wiring slots a `Projection<Inventory>` reactor in automatically, but you
can still wire arbitrary additional reactors via `.with()`. Here `StockAlert`
runs alongside the `Inventory` projection. This is how production systems
compose: side-effects (alerts, logs, downstream event publishing) sit next to
the read-side view without the wiring code knowing about either.
