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
- **[`crates/event-sorcery-ffi`](crates/event-sorcery-ffi)** — C ABI over the
  shared Rust engine, with a generated C header and static library for language
  bindings.

`event-sorcery` is the recommended entry point. Use `sqlite-es` directly only if
you need lower-level control.

`EventSourced` implementations must declare `SNAPSHOT_SIZE` and
`COMPACTION_POLICY`; `StandaloneJob` implementations must declare
`TERMINAL_FAILURE_MSG`. These settings have no inherited defaults. See
[implementing an entity](docs/cqrs.md#3-implement-eventsourced) for
configuration.

## Status

Data Clique Software Design FZCO maintains the open-source
[`dataclique/event-sorcery`](https://github.com/dataclique/event-sorcery)
repository. It includes the Haskell binding in 0.4.0 and the planned TypeScript
binding in 0.5.0.

The project began as a fork of the ST0x implementation. External users are
welcome, but the API surface is still in flux.

## Event retention

Aggregates can retain every event, compact snapshot-covered history, or keep a
trailing sequence window with `CompactionPolicy::RetainLast { events }`.
Compaction preserves unsnapshotted events and leaves streams without snapshots
untouched. Projected entities retain all events. See
[event compaction](docs/cqrs.md#event-compaction) for window semantics and
snapshot-recovery requirements.

## Examples

Runnable end-to-end examples live in [`examples/simple`](examples/simple/) and
[`examples/complex`](examples/complex/), each with its own README. Run one with
`cargo run --manifest-path examples/simple/Cargo.toml` (or the complex
manifest). Run both examples' formatting, linting, tests, and demos with
`nix run .#check-examples`.

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
nix develop --impure # devenv shell: Rust, GHC, Stack and development tools
cargo check --workspace
cargo nextest run --workspace
cargo test --locked --workspace --all-features --doc
```

The flake pins nixpkgs and the Rust overlay directly. Crane builds the Rust
workspace and the FFI engine used by the Haskell package. The devenv shell needs
`--impure` to resolve its working directory; standalone package and hook builds
remain pure:

```bash
nix build --no-link .#rust .#engine .#haskell
nix build --no-link .#checks.aarch64-darwin.formatting
```

Use your platform's system name for the hook check (`x86_64-linux` in CI).
Development-shell state lives under `.tmp/devenv/`.

### Local PostgreSQL

The flake pins Process Compose; devenv supplies the local PostgreSQL service.
For interactive database work, start the service with:

```bash
devenv up postgres
```

The service accepts private Unix-socket connections only. It creates the
`event_sorcery_test` database and role; that role can create isolated test
databases. PostgreSQL data lives under `.tmp/devenv/state/postgres/`.

See [AGENTS.md](AGENTS.md) for contribution conventions.

## License

MIT — see [LICENSE](LICENSE).
