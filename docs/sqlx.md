# SQLx

## Offline mode (`SQLX_OFFLINE=true`)

The nix dev shell sets `SQLX_OFFLINE=true` (in `flake.nix`), which tells sqlx
compile-time macros (`query!`, `query_scalar!`, etc.) to use cached metadata
from the `.sqlx/` directory instead of connecting to the database. This means:

- Regular `cargo check`/`cargo nextest run` typically don't need a running
  database -- they read from `.sqlx/` cache files checked into version control.
  Exception: test code using `query!` macros (see below).
- If you add or change a `query!` macro invocation, you must regenerate the
  cache before the change will compile under `SQLX_OFFLINE=true`.

## Regenerating the query cache

```bash
cargo sqlx prepare --workspace -- --all-targets
```

Then check the updated `.sqlx/` files into version control.

### Pitfall: `#[cfg(test)]` queries don't work with offline mode

`cargo sqlx prepare` does NOT collect queries from `#[cfg(test)]` code, even
with `-- --all-targets`. This is a known limitation -- the `--all-targets` flag
is supposed to compile test targets during preparation, but in practice the
test-only queries are silently skipped.

When you then run `cargo nextest run` (which enables `cfg(test)`), the compiler
sees the query macro, finds no cached metadata, and fails with:

```text
`SQLX_OFFLINE=true` but there is no cached data for this query
```

**The fix: use runtime query functions in test code.** Instead of the
compile-time macro `sqlx::query_scalar!("...")`, use the runtime function
`sqlx::query_scalar("...")`. The non-macro version doesn't need offline cache
entries. Since test code runs against a real in-memory database anyway,
compile-time query verification adds no value.

```rust
// BROKEN in offline mode -- macro needs cache entry that prepare won't generate
#[cfg(test)]
let count = sqlx::query_scalar!("SELECT COUNT(*) FROM my_table")
    .fetch_one(pool).await?;

// WORKS -- runtime query, no cache needed
#[cfg(test)]
let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM my_table")
    .fetch_one(pool).await?;
```

Note the type annotation on the `let` binding -- the runtime function doesn't
infer return types like the macro does.

## Cancellation and pooled transactions

Never pair a raw `BEGIN` statement on a `PoolConnection` with a later raw
`COMMIT` or `ROLLBACK`. An async future can be cancelled between those
statements. Dropping the future then returns a connection with an open
transaction to the pool, where the next borrower sees
`cannot start a
transaction within a transaction` and the retained writer lock
makes other connections see `SQLITE_BUSY`.

Use `Pool::begin` or `Pool::begin_with` and keep the returned SQLx `Transaction`
guard alive across the whole operation. Its drop path queues a rollback before
the pooled connection is reused. The durable-job claim uses
`begin_with("BEGIN IMMEDIATE")` because it needs the write lock at transaction
start.

## `SQLITE_BUSY` vs `SQLITE_BUSY_SNAPSHOT`

`sqlx-sqlite` surfaces both as the _extended_ result code via
`DatabaseError::code()`:

- `SQLITE_BUSY` = `"5"` -- ordinary lock wait. `busy_timeout` usually covers
  this by retrying internally within the configured window.
- `SQLITE_BUSY_SNAPSHOT` = `"517"` -- WAL write-write conflict detection.
  `busy_timeout` does **not** retry this at all; the transaction must be rolled
  back and re-begun on a fresh snapshot by the caller.

`crates/event-sorcery/src/reactor.rs`'s `is_retryable_sqlite_busy` classifies
both as retryable by walking the error's `source()` chain and downcasting to
`sqlx::Error`.

### Pitfall: the source-chain walk has a real, only partially-closable blind spot

`cqrs-es`'s own `PersistenceError::ConnectionError(Box<dyn Error>)` and
`AggregateError::DatabaseConnectionError(Box<dyn Error>)` are declared with
plain `#[error("{0}")]` and **no** `#[source]`/`#[from]` on the boxed field.
`thiserror` only wires `source()` for fields marked `#[source]`/`#[from]`/named
`source`, so a naive `.source()`-only walk stalls at the first
`PersistenceError`/`AggregateError` it hits and never reaches the `sqlx::Error`
sealed inside.

Two sub-cases, and they are not symmetric:

- `cqrs_es::persist::PersistenceError` is a **concrete, non-generic** public
  enum. Its boxed field is reachable without knowing any `Entity` type --
  `error.downcast_ref::<PersistenceError>()` works from fully generic code.
  `is_retryable_sqlite_busy` special-cases it: match `ConnectionError`/
  `UnknownError` and manually continue the walk into the boxed inner error as
  the next "source". This closes the gap for any reactor error chain that passes
  through `PersistenceError` directly (e.g. via `ProjectionError<Entity>` or
  `sqlite_event_repository.rs`'s conversions).
- `cqrs_es::AggregateError<T>` (what `Store::send`'s error type resolves to) is
  **generic over `T`**. Downcasting to a concrete
  `AggregateError<LifecycleError<Entity>>` requires naming `Entity`, which is
  unknowable at a generic classification site. This half of the gap is **not
  closable** without a new bound on `Reactor` or a downstream-supplied hook --
  both out of scope. A busy error originating inside a nested `Store::send()`
  call (cross-aggregate orchestration) stays unreachable and
  `is_retryable_sqlite_busy` fails closed ("not retryable") for it. This is the
  correct default: don't retry what can't be positively identified as a safe
  SQLite conflict.

### Pitfall: `#[error(transparent)]` breaks the walk differently than expected

`#[error(transparent)]` is the idiomatic thiserror pattern for "just forward a
foreign error", and it changes what `source()` returns in a way that could
defeat the walk above: thiserror generates `source()` to delegate to the
_wrapped field's own_ `source()`, not to return the field itself. So a variant
like `#[error(transparent)] Sqlx(#[from] sqlx::Error)` never surfaces the
wrapped `sqlx::Error` node to the walk directly -- it's skipped straight
through, and the walk instead lands on whatever `sqlx::Error::source()` itself
returns.

For the case that matters here
(`sqlx::Error::Database(#[source] Box<dyn
DatabaseError>)`), that is one further
hop in: `Box<dyn DatabaseError>` itself, _not_ the concrete
`sqlx::sqlite::SqliteError` sealed inside it. This was verified empirically
against a real provoked `SQLITE_BUSY`: the node reachable after the transparent
skip downcasts successfully as `Box<dyn
sqlx::error::DatabaseError>` (sqlx's own
`impl StdError for Box<dyn
DatabaseError> {}` is what makes this typecheck) but
fails to downcast as `SqliteError` -- the concrete sqlite type is not what the
trait-object coercion vtables on. `is_retryable_sqlite_busy` therefore downcasts
to `Box<dyn
DatabaseError>` (in addition to `sqlx::Error` for the
non-transparent shape) to close this gap for arbitrary downstream reactor error
types, without requiring those types to avoid `transparent` the way
`ProjectionError`'s own `Sqlx` and `Persistence` variants do (see above -- that
fix still stands for this crate's own error type, it just isn't the only way to
close the gap anymore).

Downstream reactor error types are free to use either shape:
`#[error(transparent)] Sqlx(#[from] sqlx::Error)` now works, and an explicit
`#[error("...: {0}")] Sqlx(#[source] sqlx::Error)` still works as before. A
`transparent` variant wrapping something other than `sqlx::Error` directly (or a
type that doesn't ultimately expose a `sqlx::Error`/`Box<dyn DatabaseError>`
node in its `source()` chain) is still an unclosable blind spot from this
function's perspective, same as the `AggregateError<T>` case above.
