# event-sorcery 0.4.0 system specification

This document specifies the 0.4.0 monorepo migration. It replaces the previous
`SPEC.md` in full. The key words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are
to be interpreted as described in RFC 2119.

Where the existing implementation, `docs/domain.md`, `docs/cqrs.md`, or the ADRs
conflict with this document, this document wins. Existing behavior that is not
changed here remains the compatibility authority and MUST be captured by tests
before it is moved.

## Status and scope

`event-sorcery` 0.4.0 is one existing Rust implementation exposed through two
typed language surfaces:

- the current native Rust API; and
- an idiomatic Haskell binding over a versioned C ABI.

The shared engine MUST be extracted from the implementation already present in
this repository. It MUST continue to build on:

- `cqrs-es` for aggregate execution, event persistence contracts, snapshots, and
  query dispatch;
- Apalis for native Rust worker hosting and task execution;
- `sqlite-es`, SQLx, and SQLite for the production persistence path; and
- the existing event-sourced job, dispatch, reactor, projection, schema, and
  delivery implementations.

This is an extraction and binding project. It is not permission to design a
second event store, queue, workflow engine, or set of storage semantics.

## Goals

- Make the existing Rust implementation the only production implementation of
  persistence and durable state transitions.
- Preserve the native Rust domain API while routing it through a reusable,
  erased engine facade.
- Provide Haskell users with the same domain capabilities through idiomatic
  Haskell types and a thin FFI adapter.
- Keep domain folds and handlers in the language where the domain is written.
- Make cross-language behavior testable against the same real SQLite engine.
- Keep the Nix development and release graph reproducible for both languages.

## Non-goals

The following are explicitly out of scope for 0.4.0:

- replacing `cqrs-es`, Apalis, `sqlite-es`, SQLx, or SQLite;
- replacing the current JSON persistence representation merely to support an
  FFI;
- introducing a parallel `JobRecord`, job kernel, queue schema, event schema,
  snapshot model, or checkpoint model;
- redesigning `ClaimId`, claim sequence routing, retries, leases, fencing, or
  dispatch settlement;
- adding a Postgres backend;
- adding a general broker, transport, or outbox framework;
- maintaining the standalone Haskell storage implementation; and
- making Haskell reproduce Rust storage or job decisions.

Any such change requires a separate specification and compatibility decision.

## Architectural rule

```text
       native Rust API                 Haskell API
EventSourced / Store / jobs      type families / Effect / jobs
           |                              |
           | direct Rust calls            | deterministic CBOR over C ABI
           v                              v
       +---------- erased engine facade ----------+
       | existing store and durable operations     |
       | existing cqrs-es repository adapters      |
       | existing job, dispatch, and query logic   |
       +--------------------------------------------+
                  |                    |
               cqrs-es              Apalis
                  |                    |
                  +-- sqlite-es/SQLx/SQLite
```

The erased facade is a reusable entry point into the current implementation. It
is not a new implementation beside it.

The native Rust API and the C ABI MUST reach the same writer path. A feature
that works through one surface MUST NOT have a separate persistence or durable
decision implementation through the other.

No database transaction may span the FFI. The Rust engine owns every atomic
storage operation. The engine never calls foreign code and the ABI contains no
callbacks or function pointers.

## Existing semantic authorities

The following code is the source from which the shared engine is extracted. Its
behavior MUST be characterized before structural changes are made.

### Aggregate execution and persistence

The existing `EventSourced`, `Store`, `cqrs_es::CqrsFramework`,
`PersistedEventStore`, and repository implementations define aggregate load,
command execution, optimistic concurrency, snapshots, and query dispatch.

The current SQLite event repository atomically persists the aggregate events,
snapshot changes, durable job intent, job events, and queue projection that
already belong to one command. Extraction MUST preserve that atomic boundary.

### Durable jobs and Apalis

The current `JobEvent`, `JobState`, `plan_claim`, `JobBackend`, `JobContext`,
lease renewal, acknowledgement, retry, defer, and dead-letter paths define the
job protocol.

The `job` aggregate event stream is the sole durable source of job lifecycle
truth. `job_queue` remains its rebuildable polling projection. A claim appends
the job event and advances that projection in one transaction. The
projection-only lease is operational coordination state and MUST NOT redefine
the event-sourced lifecycle.

`ClaimId` and the persisted claim sequence remain the fencing identity. The
current first-execution versus reconciliation routing remains based on the
existing claim sequence contract. The ABI MAY encode these values as an opaque
claim handle, but MUST NOT replace them with a newly invented token model.

Apalis remains the native Rust worker host. A foreign-language worker adapter
may poll and settle jobs through the facade, but it MUST invoke the same claim,
renewal, and settlement operations as the Apalis backend.

### Entity-scoped dispatch

The existing `DispatchedJob`, dispatch guard, sealed outcome types, fold,
duplicate settlement behavior, and submit/reconcile routing remain normative.
The Haskell surface MUST preserve the same invariants with sealed modules and
compile-checked job membership.

### Reactors, projections, schema, and delivery

Rust reactors remain the existing synchronous `cqrs-es` query adapters. They do
not have durable checkpoints. SQLite materialized views retain their existing
per-aggregate optimistic versions, catch-up, and rebuild behavior. The schema
registry remains the authority for projection invalidation, and dispatched-job
verdict delivery remains the existing worker orchestration.

Global journal offsets, durable reactor checkpoints, outboxes, and generic
delivery receipts do not exist in the current Rust implementation. They are not
part of the 0.4.0 binding extraction and MUST NOT be invented to satisfy an FFI
shape. Adding any of them requires its own specification and architectural
decision based on the native engine first.

## Responsibility boundary

| Concern                                            | Shared Rust engine | Language binding |
| -------------------------------------------------- | ------------------ | ---------------- |
| Event and snapshot persistence                     | MUST               | MUST NOT         |
| Optimistic concurrency                             | MUST               | MUST NOT         |
| Atomic event and job enqueue                       | MUST               | MUST NOT         |
| Job claim, lease, fencing, retry, and settlement   | MUST               | MUST NOT         |
| Schema state and materialized projection rows      | MUST               | MUST NOT         |
| Domain event and command types                     | MUST NOT           | MUST             |
| Domain payload codecs                              | MUST NOT           | MUST             |
| Pure aggregate folds and handlers                  | MUST NOT           | MUST             |
| Job `perform`, `submit`, and `reconcile` bodies    | MUST NOT           | MUST             |
| Language-native iteration and supervision adapters | MUST NOT           | MAY              |

Calling an engine operation and interpreting its typed result is binding work.
Recomputing whether a durable transition is legal is engine work and MUST NOT
appear in a binding.

## Domain protocol

Both language surfaces expose the same domain shape:

The SQLite claim transaction is owned by SQLx's transaction guard from
`BEGIN IMMEDIATE` through its explicit commit or rollback. Cancelling a claim
future drops that guard, which queues a rollback before the connection can be
reused by the pool. A cancelled worker must never return an open transaction to
the pool or retain SQLite's writer lock.

### Durable jobs

Domain event and job payloads are owned by the binding. The engine treats their
serialized representation as opaque application data while retaining the
existing internal metadata and persistence representation.

Bindings MAY perform pure replay validation and effect construction. They MUST
submit all persistence and durable decisions to the engine.

## Erased engine facade

The facade MUST be obtained by extracting and generalizing current operations.
It MUST NOT be implemented by copying them into a new module or crate.

The extraction MUST provide language-neutral values for:

- stream identity and expected version;
- serialized proposed and stored events;
- command commit requests, including the existing optional durable job intent;
- snapshots and schema versions;
- job identity, payload, claim handle, attempt, schedule, and settlement;
- existing materialized-view identities, optimistic versions, and payloads.

The exact Rust module and crate layout is not contractual. These invariants are:

- the existing native `Store` lowers through the facade;
- the C ABI lowers through the facade;
- repository writes remain implemented once;
- existing transaction boundaries remain intact;
- existing Rust public behavior remains covered by compatibility tests; and
- neither caller may bypass the facade for a production write.

Where `cqrs-es` requires typed aggregate values, the extraction SHOULD adapt the
existing serialized repository boundary instead of reproducing `cqrs-es`
behavior. Where the existing repository already operates on erased serialized
values, that repository contract SHOULD be reused directly.

## C ABI

The engine ships a C-compatible library and a cbindgen-generated header. The
header is a build artifact and MUST NOT be hand-maintained.

### Handles and buffers

The ABI uses an opaque, thread-safe store handle. Inputs remain caller-owned for
the duration of a call. Output buffers are engine-allocated and are freed
exactly once with the exported buffer-free function. Explicit close and the
binding finalizer MUST be safely idempotent together.

Starting explicit close or the binding finalizer linearizes the handle into
`closing`. Calls that acquired the handle before that transition MAY finish; new
calls MUST fail with `INVALID_STATE`. Close MUST wait for acquired calls to
drain before destroying the engine, and concurrent close/finalizer attempts MUST
join that drain and destroy the handle exactly once. A completed close leaves a
closed owner value that subsequent closes absorb without touching freed memory.

Every store call is synchronous and may block. Haskell imports for those calls
MUST use `foreign import capi safe`. Only constant-time functions such as ABI
version inspection and buffer release MAY use `unsafe` imports.

Every export MUST prevent a Rust panic from unwinding across the C boundary. A
panicked handle is poisoned and refuses subsequent data operations. Explicit
close and binding finalization remain callable and MUST follow the same
idempotent drain-and-destroy protocol for poisoned handles, destroying the
handle exactly once.

### Versioning

The ABI reports `(major << 16) | minor`. Each binding declares the minimum minor
version containing every export and encoding it uses. Before opening a store,
the binding MUST reject a mismatched major or a library minor below that
minimum. A library with the same major and a newer minor is compatible. Additive
exports increment minor; removals, changed signatures, or incompatible encodings
increment major.

### Encoding

Framework request and response values use deterministic CBOR at the ABI
boundary:

- definite-length values only;
- shortest-form integers;
- no floats in framework values;
- arrays for products and tagged sums; and
- an explicit format version for every top-level framework value.

ABI major version 0 has these normative resource limits:

| Resource                                        |  Limit |
| ----------------------------------------------- | -----: |
| Encoded request buffer                          | 16 MiB |
| Encoded response buffer, including error detail | 64 MiB |
| CBOR container nesting depth                    |     32 |
| One opaque domain payload                       |  1 MiB |
| Events in one commit                            |  1,024 |
| Items in one page or list response              |  4,096 |
| UTF-8 error-detail text                         |  4 KiB |

Lengths and counts MUST be checked before proportional allocation or durable
work whenever the encoded shape makes that possible. Exceeding any limit MUST
return `RESOURCE_LIMIT` with no partial write. A list-producing operation MUST
page or reject before crossing the response or item limit; truncation is not
allowed. Limits are measured over encoded bytes and include opaque payloads.

Domain payload bytes remain opaque. Deterministic CBOR is an interchange
encoding, not a mandate to migrate the existing SQLite JSON event and job
representation. The engine adapter performs any required conversion at the
boundary.

Hand-written codecs are preferred for framework values so each field and tag is
auditable. The shared conformance corpus pins their encoded bytes.

### Error identity

Errors cross the ABI as `[1, code: uint, detail]`. These numeric codes are
stable for ABI major version 0:

| Code | Name                  | Canonical detail                                   |
| ---: | --------------------- | -------------------------------------------------- |
|    1 | `MALFORMED_INPUT`     | redacted UTF-8 text                                |
|    2 | `OPTIMISTIC_CONFLICT` | `[aggregate_type, aggregate_id, expected, actual]` |
|    3 | `JOB_REFUSAL`         | `[job_id, reason]`                                 |
|    4 | `STORAGE_FAILURE`     | redacted UTF-8 text                                |
|    5 | `INVALID_STATE`       | redacted UTF-8 text                                |
|    6 | `RESOURCE_LIMIT`      | `[resource, observed, limit]`                      |
|    7 | `ABI_MISMATCH`        | `abi-version-detail`                               |
|  100 | `PANIC`               | `null`                                             |

```cddl
abi-version-detail = [expected_major: uint, minimum_minor: uint,
                      actual_major: uint, actual_minor: uint]
```

Text details MUST fit the encoding limit above and MUST be valid UTF-8. The
engine and bindings MUST preserve the code and canonical detail shape rather
than collapsing errors into exceptions or unstructured text. Error values and
logs MUST NOT contain opaque domain payloads, raw ABI buffers, serialized event
or job bodies, or excerpts derived from them. Decoder locations, stable metadata
identities, sizes, limits, and redacted causal categories MAY be reported.

### Required capabilities

The ABI exposes the existing engine capabilities needed by the binding:

- open, close, ABI version, and buffer release;
- load a stream and read its current version;
- atomically commit events with the existing optional job intent;
- resolve, store, and discard snapshots;
- poll, claim, renew, acknowledge, retry, defer, and dead-letter jobs;
- access existing materialized-view and schema operations only where their typed
  repository boundary can be erased without weakening table-name or
  optimistic-concurrency guarantees.

Names and signatures are finalized in the generated header and conformance
tests. They MUST model the current engine operations rather than speculative
storage models.

## Native Rust binding

The existing Rust `EventSourced`, `Store`, `Effect`, `DispatchedJob`, worker,
reactor, and projection surfaces remain the native binding.

0.4.0 MAY make explicitly documented breaking changes, but extraction alone is
not justification to break consumers. The implementation MUST first add
facade-level characterization tests, then route the native surface through the
facade while keeping its behavioral suite green.

The native worker remains Apalis-hosted. Its adapter consumes the same facade
claim and settlement operations exported to foreign bindings.

## Haskell binding

The Haskell package lives in this monorepo and is built against the generated
header and static library from the same Nix dependency graph.

### Retained domain surface

The useful typed surface from the former Haskell repository is retained and
adapted:

- `EventSourced` with injective type families;
- strongly typed stream keys;
- `Effect` with a type-level job list and `Member` constraint;
- fallible pure replay and command folds;
- sealed dispatch outcomes with explicit test-only constructors;
- `TestHarness` for pure domain tests; and
- one-shot protocol values implemented with `LinearTypes` where ownership
  matters, including commit requests and claimed-job handles.

### Removed framework duplication

The Haskell package MUST NOT contain production memory or SQLite backends. It
MUST NOT retain decision functions that reproduce Rust persistence, snapshot,
claim, fencing, retry, schema, or projection semantics. It also MUST NOT
introduce binding-owned checkpoint or receipt semantics that the native engine
does not have.

The sole production backend is the engine handle. Pure test helpers MAY fold
domain events, but MUST NOT pretend to be a production store.

### Runtime adapters

Conduit SHOULD provide streaming iteration for journal, reactor, projection, and
worker loops. It is an orchestration adapter: it sequences engine calls and runs
Haskell domain or job code. It MUST NOT become another queue or implement
durable transition rules.

A Haskell job body executes in Haskell because it is application code. Before
and after that body, the worker uses the engine's claim handle and settlement
operations. It does not infer claim legality or rewrite the engine's retry and
fencing rules.

Every potentially blocking FFI call releases a GHC capability. The store is
owned by a `ForeignPtr`, supports explicit close, and checks ABI compatibility
when opened.

## Build and repository contract

The repository is managed with Nix. The flake consumes the shared external
`github:dataclique/but.nix` library rather than vendoring a `but.nix` file. The
development shell includes the Rust toolchain, Stack, GHC 9.14.1 via the
Stack-compatible Nix toolchain workaround, cbindgen, Fourmolu, HLint, and the
native libraries required to link the engine.

`.envrc` uses nix-direnv. Git hooks are declared with `git-hooks.nix` and run
through `prek`. Hooks include fast formatting and static file checks only;
`cargo check` and Clippy are CI checks and MUST NOT be git hooks.

Fourmolu uses the repository configuration and an 80-column limit. Source code
MUST still be written with reasonable line lengths where a formatter cannot make
a good break.

## Verification

### Characterization before extraction

Before an existing operation is moved behind the facade, a test MUST exercise
its current public path and pin the behavior being preserved. At minimum this
covers:

- aggregate load, command execution, conflict, and snapshot behavior;
- atomic event plus durable-job persistence;
- job poll, claim, renewal, fencing, retry, defer, dead-letter, and ack;
- first-execution and reconcile routing;
- sealed and duplicate dispatch settlement;
- synchronous reactor dispatch and retry behavior;
- projection catch-up, rebuild, and optimistic updates; and
- schema reconciliation.

The same tests, or equivalent facade-level tests, MUST pass after extraction.

### Cross-binding conformance

The repository contains one deterministic-CBOR corpus consumed by Rust and
Haskell. Both bindings test their public behavior against a real in-memory
SQLite engine. There is deliberately no Haskell production mock backend.

Cross-binding tests MUST prove that equivalent Rust and Haskell domain
operations produce equivalent stored behavior and error identities through the
same engine.

### Benchmarks

Benchmarks cover at least aggregate load/replay, command commit, job
claim/settlement, and reactor or projection catch-up. Haskell benchmarks MUST
include enough repeated work to expose laziness, allocation, FFI-copying, and GC
regressions. Benchmark results are release evidence, not git hooks.

## Migration order

The migration proceeds as one stacked series of draft pull requests:

1. Correct this specification and the architectural record.
2. Add characterization tests for the existing Rust behavior.
3. Extract the erased facade from the existing implementation.
4. Route the existing native Rust API through the facade.
5. Add deterministic ABI codecs, the C ABI, and generated header.
6. Bring the Haskell typed surface into the monorepo and replace its production
   backends and decision machinery with the engine adapter.
7. Add cross-binding conformance tests and benchmarks.
8. Delete the superseded `event-sorcery-hs` GitHub repository after the full
   stack is published and green.

Each implementation pull request MUST remain reviewable and MUST not introduce a
temporary parallel engine.

## Release criteria

0.4.0 is releasable only when:

- the native Rust API and Haskell binding use the same extracted writer path;
- `cqrs-es`, Apalis, `sqlite-es`, SQLx, and SQLite remain the implementation
  foundations described here;
- no duplicate production backend or durable decision implementation remains in
  Haskell;
- no parallel Rust event store or job model was introduced;
- the generated header matches the linked library;
- ABI version and deterministic codec conformance pass in both languages;
- Rust and Haskell behavioral, integration, Nix, and benchmark suites pass; and
- all open pull-request feedback is addressed.
