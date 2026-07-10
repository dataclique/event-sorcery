# ADR-0008: Entity-scoped durable operations

## Status

Accepted.

Date: 2026-07-10.

## Context

ADR-0001/0006/0007 made jobs first-class: handlers stay pure and enqueue
transactionally, workers execute at-least-once, and the job's own lifecycle
(Enqueued/Claimed/Succeeded/RetryScheduled/Rescheduled/Dead) commits
exactly-once as events. What the framework does **not** model is the lifecycle
of the _business operation on the entity_ that the job exists to perform.

An audit of `st0x.liquidity` (the library's origin consumer) shows what that
omission costs. Every fallible external operation — broker order, on-chain tx,
CCTP bridge leg, transfer, wrap/unwrap, vault deposit — is hand-modeled as:

- a Pending/Submitting -> Submitted/InFlight -> Confirmed/Failed event triad on
  the entity, repeated per operation (`OffchainOrder`: ~75% of its events and
  code are this plumbing; `EquityRedemption`: ~80-90%; `UsdcRebalance`: 21 of 21
  event variants, one triad per bridge leg);
- outcome-feedback commands (`MarkAccepted`, `MarkPlacementFailed`,
  `ConfirmWithdrawal`, `FailBridging`, ...) that exist only to carry a job
  result back into the entity;
- a reload-and-state-guard idempotency check before every external call, so an
  at-least-once retry that already settled does not act twice;
- intent-before-call persistence (idempotency key, `from_block` chain head) so a
  crash between the call and the outcome commit is recoverable;
- a per-state resume dispatcher per entity (`resume_mint`,
  `continue_alpaca_to_base_from_*`, startup recovery) that re-drives whatever
  state the entity crashed in.

The skeleton is byte-for-byte identical across entities; only the external call
and the payload types differ. `st0x.liquidity`'s ADR-0014 is a case study of how
error-prone hand-rolling it is (stuck-pending races between `MarkAccepted` and
`MarkFailed`).

On the library side the weld is missing entirely: `Job::Output` is discarded by
the ack path (`AckService` maps `Done(_)` to `Succeeded` and drops the value),
so a consumer who needs the result must inject a `Store` handle into
`Job::Input` and send follow-up commands by hand inside `perform`. `perform`
also does not receive the job id, so the documented guidance to key
external-boundary idempotency on it cannot be followed through the API.

Two more consumers (moneymentum and an upcoming dataclique service) are about to
need this pattern. Without a library primitive, each will re-duplicate the
plumbing.

## Decision

Absorb the skeleton into event-sorcery as **entity-scoped durable operations**,
four composable additions:

1. **`Operation<J>` — a library-owned state machine embedded in entity state.**
   `Operation<J: Job>` models
   `Idle -> Requested -> Confirmed(J::Output) | Failed(J::Error)` (with
   library-defined markers for operator reconciliation), together with
   `OperationEvent<J>` and `OperationCommand<J>` wrapper types the consumer
   nests in their own event/command enums (one variant per operation, e.g.
   `Withdrawal(OperationEvent<InitiateWithdrawal>)`). The library provides
   `evolve`/`transition` for the embedded machine; requesting an operation
   enqueues the job in the same transaction that commits the `Requested` event
   (the existing pending-scope flush), so there is no intent/call crash window.
   The state guard — "ignore an outcome that already landed, refuse a second
   request while one is in flight" — lives in the library machine, not in
   consumer code. Payload the operation must remember across the round trip
   (idempotency key, chain head) is carried in `Requested`.

2. **Outcome feed-back — job results become commands on the originating
   entity.** A job that drives an operation declares its origin
   (`type Origin: EventSourced` plus `origin_id()`); the framework maps the
   `perform` result to the corresponding `OperationCommand` and delivers it to
   the origin entity **before** acking the job as succeeded. If delivery fails,
   the job is not acked and redelivers; duplicate delivery is absorbed by the
   state guard in (1). A job that dead-letters delivers `Failed(DeadReason)` the
   same way, so the entity never dangles in-flight. Entity-side resume
   dispatchers disappear: the job subsystem (claims, leases, retry, defer,
   startup poll) is already the re-driver, and the operation state machine is
   its durable ledger.

3. **Submit/reconcile split — a retry is routed to a different method.** This
   library is used for financial operations, where whether this is the first try
   or a follow-up changes what is safe to do: a follow-up must never blindly
   resubmit, because the dangerous window is "the call reached the external
   system but the outcome never committed". So an operation-driving job does not
   implement a single `perform`. The framework calls `submit(input, ctx)` only
   on the job's first claim; any later claim (retry after an error,
   lease-expired reclaim, crash mid-call) is routed to `reconcile(input, ctx)`,
   which must determine the fate of the earlier attempt — typically by querying
   the external system with the operation's idempotency key — and return a typed
   verdict: `Settled(outcome)` (the earlier attempt landed or definitively
   failed; feed it back), `NotSubmitted` (provably never reached the external
   system; the framework authorizes `submit` again), or `Indeterminate` (cannot
   tell yet; maps onto the existing `JobOutcome::Defer`, which correctly counts
   no attempt — polling for the fate of a submission is not a new business
   attempt). The claim counter that drives the routing is already exactly-once
   (claims are events on the job aggregate) and over-approximates submissions in
   the safe direction: it is impossible to be routed to `submit` while a prior
   submission might exist. Jobs without an `Origin` keep the plain `perform`.

4. **`JobContext` and stable idempotency keys.** Both `submit` and `reconcile`
   receive the job id and attempt number. External-boundary idempotency keys
   (broker `client_order_id`, transfer reference) must derive from the job id
   only — never the attempt — so every retry presents the same key and the
   external system dedupes (the `client_order_id_for_placement` reuse pattern,
   made structural). The terminal `Confirmed`/`Failed` feedback events carry the
   attempt count, so the entity's audit trail records how many tries the
   operation took without the entity tracking it live.

Multi-leg sequencing (bridge leg B starts when leg A confirms) deliberately
stays consumer code — that ordering _is_ the domain. Each leg becomes one
`Operation` field and one enqueue instead of ~40 lines of triad + weld + resume.

## Alternatives Considered

### Codegen only: a derive/macro that stamps out the triad per operation

- Pros: no runtime changes; smallest library surface; consumers keep full
  control of event shapes.
- Cons: generated enums are opaque to readers and greppers; the weld (outcome
  routing inside `perform`), the state guard, and resume all remain
  hand-written; the intent/call crash window remains the consumer's problem.
- Rejected because: it compresses the _typing_ of the plumbing but not the
  _responsibility_ for it — every correctness-critical piece (idempotency,
  feedback, recovery) stays duplicated per consumer.

### Full saga / process-manager framework

- Pros: would also absorb multi-leg sequencing (`UsdcRebalance`'s 7 legs as a
  declarative workflow); one abstraction for everything.
- Cons: large blast radius on a young jobs subsystem; hides the domain sequence
  inside framework config; the audit shows legs need bespoke policies
  (fail-closed vs fail-open, block-capture, timeout semantics) that a generic
  saga DSL would either forbid or reinvent as escape hatches.
- Rejected because: the expensive, identical part is the per-leg skeleton, not
  the sequencing. Absorb the skeleton first; a saga layer can compose
  `Operation`s later if a real need emerges.

### Outcome routing only (no entity-side `Operation` machine)

- Pros: minimal API delta (`Origin` + command mapping); fixes the discarded
  `Job::Output` and the hand-written weld.
- Cons: consumers keep hand-writing the event triads, state guards, and resume
  logic — which the audit shows is 75-95% of the boilerplate and the part
  ADR-0014-class bugs live in.
- Rejected because: it fixes the smaller half of the problem and leaves the
  error-prone half untouched.

### Track the attempt count inside the entity's `Operation` state machine

- Pros: the entity event log would show retries directly; no need to consult the
  job stream for attempt history.
- Cons: the job aggregate already records every claim/retry/reschedule
  exactly-once as events; a second durable counter on the entity can disagree
  with it, and contradictory duplicated state is exactly what this library's
  type-modeling rules forbid.
- Rejected because: the job stream is the operational ledger and the entity
  stream is the domain ledger. The entity gets the attempt count where it is
  domain-relevant — stamped on the terminal `Confirmed`/`Failed` events — and
  the per-attempt history stays on the job stream.

### Advisory attempt number only (expose `ctx.attempt`, keep one `perform`)

- Pros: smallest API change; consumers who care can branch on the attempt number
  themselves.
- Cons: nothing forces the check-before-resubmit discipline; a consumer who
  forgets the branch double-places a trade on the first lease expiry.
  `st0x.liquidity` ADR-0014 documents precisely this class of incident with
  hand-rolled guards.
- Rejected because: for money movement, first-try-vs-follow-up is a safety
  invariant, not a hint — it belongs in the method routing where the type system
  enforces it, not in a field the consumer may ignore.

### Status quo: keep the pattern in consumer code

- Pros: zero library work; maximum consumer freedom.
- Cons: three consumers (st0x.liquidity, moneymentum, the upcoming service) each
  hand-roll the same crash-window-sensitive machinery; the library's stated
  purpose (remove cqrs-es sharp edges that caused production bugs) argues the
  opposite.
- Rejected because: the pattern has already produced production incidents in the
  origin consumer, and duplication is about to triple.

## Consequences

- Consumer entities keep plumbing _variants_ in their enums, but each is one
  library-typed wrapper line instead of a hand-built triad; handlers shrink to
  domain decisions plus `Operation` delegation.
- Operation-driving jobs implement `submit`/`reconcile` instead of `perform`,
  and `perform`/`submit`/`reconcile` gain a `JobContext` parameter. This is a
  breaking change for existing `Job` impls (the two examples and any early
  consumers) — acceptable pre-1.0, and jobs without an origin can stay
  outcome-less via a default.
- Delivery-before-ack makes entity command execution part of the job success
  path; a persistently failing origin entity will hold its job in retry. That is
  the correct failure mode (fail-stop, visible), consistent with ADR-0007's
  fail-stop direction.
- The state guard subsumes the entity-driven part of ADR-0007 item 3 (dedup);
  items 3-6 (dedup keys for standalone enqueue, terminal-failure hook, cancel,
  prune) remain planned and compose with this design — terminal-failure hooks in
  particular become "deliver `Failed` to the origin".
- SPEC.md must gain a durable-operations section (and its stale
  `EventSourced::Services` write-path text must be replaced by the
  jobs/operations model) before implementation, per the repo's spec-first rule.
- Blast radius if wrong: the additions are additive to the event schema and
  opt-in per operation; existing hand-rolled entities keep working unchanged, so
  a reversal supersedes this ADR without a data migration.
