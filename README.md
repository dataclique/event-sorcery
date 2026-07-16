# Event Sorcery

Event-sourcing primitives in Rust. A thin, opinionated layer on top of
[`cqrs-es`](https://crates.io/crates/cqrs-es) plus a SQLite-backed event store.

## Crates

- **[`crates/sqlite-es`](crates/sqlite-es)** — SQLite implementation of
  `cqrs-es`'s event repository, view repository, and `CqrsFramework` glue.
  Standalone — usable wherever you'd plug in a `cqrs-es` backend.
- **[`crates/event-sorcery`](crates/event-sorcery)** — higher-level ergonomics
  on top of `sqlite-es`: the `EventSourced` trait, `Lifecycle` adapter, typed
  `Store`, projections, schema registry, reactor, and durable job dispatch
  enforced by the handler signature (dispatch/confirm/fail lifecycles with
  submit/reconcile retry safety and outcome feedback; see
  [ADR-0008](adrs/0008-entity-scoped-durable-operations.md) and
  [ADR-0009](adrs/0009-handlers-return-events-or-one-job-dispatch.md)).

`event-sorcery` is the recommended entry point. Use `sqlite-es` directly only if
you need lower-level control.

## Status

This repository is an experimental fork maintained by Data Clique Software
Design FZCO in the `dataclique` GitHub organization. It contains work unrelated
to ST0x, including the Haskell binding in 0.4.0 and the planned TypeScript
binding in 0.5.0.

The original Event Sorcery project remains owned and maintained by ST0x, with
contributions from Data Clique Software Design FZCO and other contributors.
Features developed in this fork do not imply ST0x ownership or adoption.
External users are welcome, but the API surface is still in flux.

## Examples

Runnable end-to-end examples live under [`examples/`](examples/) — one directory
per concept (`basic_entity`, `projection`, `reactor`), each with its own README
explaining what it covers. Run any of them with
`cargo run -p event-sorcery --example <name>`.

## Documentation

- [`docs/domain.md`](docs/domain.md) — domain terminology and naming
  conventions.
- [`docs/cqrs.md`](docs/cqrs.md) — event-sourcing patterns, the `EventSourced`
  trait, projections, typed jobs, schema registry.
- [`docs/event-sourced-job-backend-spec.md`](docs/event-sourced-job-backend-spec.md)
  — the durable-jobs backend: claim/ack/fence/lease, the worker, the concurrency
  rationale.
- [`docs/migrating-to-durable-jobs.md`](docs/migrating-to-durable-jobs.md) —
  moving an existing manual / apalis job system onto event-sorcery durable jobs:
  what's covered, the prerequisites, and the safe per-kind cutover.
- [`docs/sqlx.md`](docs/sqlx.md) — `SQLX_OFFLINE`, `query!` vs runtime queries,
  regenerating the query cache.
- [`docs/ttdd.md`](docs/ttdd.md) — type-driven TDD methodology used in this
  project.

## Development

```bash
nix develop          # rust toolchain + sqlx-cli + cargo-nextest
cargo check --workspace
cargo nextest run --workspace
```

See [AGENTS.md](AGENTS.md) for contribution conventions.

## License

MIT — see [LICENSE](LICENSE).
