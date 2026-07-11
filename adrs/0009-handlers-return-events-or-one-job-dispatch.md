# ADR-0009: Handlers return events or one job dispatch -- durability enforced by the signature

## Status

Accepted. Revises the handler-facing surface of
[ADR-0001](0001-jobs-replace-services.md) (the `JobQueue` handler parameter) and
[ADR-0008](0008-entity-scoped-durable-operations.md) (which made the operation
machinery available but optional).

## Context

ADR-0008 shipped `Operation<J>`, submit/reconcile routing, and outcome feed-back
-- but as an **opt-in library next to the old interface**. The handler contract
is still:

```rust
async fn transition(&self, command, jobs: &JobQueue<Self::Jobs>)
    -> Result<Vec<Self::Event>, Self::Error>;
```

which permits exactly the bug class this whole effort exists to kill:

- `jobs.push(job)` is a side-channel with **no structural connection** to the
  returned events. A handler can enqueue `SendOrderConfirmation` and in the same
  breath emit `Placed` -- an accomplished-fact event -- when the effect has only
  been _queued_. The event log then asserts something the world has not done
  yet.
- Nothing forces a consumer to use `Operation` at all. The proof is empirical:
  after the entire ADR-0008 implementation landed, **both existing examples
  compiled unchanged**. A structural upgrade to durability semantics that no
  existing consumer is forced to acknowledge has not actually changed the
  contract.
- `JobQueue::push` outside a command scope silently drops the job (a warning
  log) -- a footgun that only exists because enqueue is a free-floating
  side-channel rather than part of the handler's return value.
- `OperationEvent::Confirmed` / `OperationCommand::Confirm` are public
  constructors: consumer code can fabricate a settled outcome without any job
  having run, so `Operation::Confirmed` does not actually _prove_ settlement.

The acceptance criterion for the fix, set explicitly: **existing consumers must
fail to compile.** If the examples do not have to change, the interface did not
change.

## Decision

Make the durability coupling the handler signature itself. A command is the
operation being performed; if it needs a worker, it kicks off a **job**. Command
handlers lose the `JobQueue` parameter and return a sum type: **either domain
events, or exactly one job dispatch.**

```rust
async fn initialize(command: Self::Command)
    -> Result<Decision<Self>, Self::Error>;
async fn transition(&self, command: Self::Command)
    -> Result<Decision<Self>, Self::Error>;

pub enum Decision<Entity: EventSourced> {
    /// Pure domain facts. No side effects were requested; nothing to enforce.
    Events(Vec<Entity::Event>),
    /// One job kicked off for a worker. The framework -- not the handler --
    /// emits the `Dispatched` event and enqueues the job in the same
    /// transaction. No other events can accompany it.
    Dispatch(JobDispatch<Entity>),
}
```

Naming: the entity-kicked job trait becomes THE `Job` trait (`Origin`,
`submit`/`reconcile`); the origin-less worker job (ADR-0007's standalone enqueue
path) is renamed `StandaloneJob` (`perform`). The embedded machine is
`DispatchedJob<J>` (`Idle | InFlight | Confirmed | Failed`), its events
`DispatchEvent<J>`, the delivered verdicts `DispatchOutcome<J>`, failures
`DispatchFailure`, guard refusals `DispatchRefused`.

1. **The intent IS the job.** `DispatchEvent::Dispatched` carries the job value
   itself (`Dispatched { job_id, job: J }`), so the full intent -- item,
   quantity, amount -- is durably recorded on the origin's stream and
   `originate`/`evolve` derive entity state from it. `Placed` stops being an
   emitted fact and becomes the fold of `Confirmation(Dispatched { job })`:
   expressible as intent, not completable by the handler.

2. **`JobDispatch<Entity>` is the only path to an enqueue from a handler.** It
   is obtained from the machine --
   `DispatchedJob::dispatch(job) -> Result<JobDispatch<Entity>, DispatchRefused>`
   -- which enforces the state guard (refuse while in flight) at construction
   time. Its constructor bounds require `J: Job<Origin = Entity>`,
   `Entity::Event: From<DispatchEvent<J>>`, and membership in `Entity::Jobs`, so
   the wiring the delivery path needs is proven at the dispatch site. `JobQueue`
   disappears from the public API along with the push-outside-scope footgun.

3. **Outcomes are sealed.** `DispatchEvent::{Confirmed, Failed}` and
   `DispatchOutcome` gain a private construction token: only the framework's
   delivery path can create them. `DispatchedJob::Confirmed` in entity state is
   thereby a _proof_ that the job settled -- not a claim any handler could have
   fabricated.

4. **One command, one dispatch.** `Decision::Dispatch` carries exactly one job.
   Multi-leg flows are chains: the delivery of one leg's outcome is a command,
   and its handler may dispatch the next leg. This is the st0x.liquidity audit's
   finding restated as a rule -- the sequencing is domain logic, each hop
   durable.

5. **Fire-and-forget from handlers is gone, not grandfathered.** A notification
   email is a job with `Output = ()` -- the entity's state honestly
   distinguishes dispatched-from-sent. Work with no origin entity at all
   (reactor sweeps, pollers, startup recovery) keeps the `StandaloneJob` +
   `JobRuntime::enqueue` path from ADR-0007, which never had a handler in the
   loop.

## Alternatives Considered

### Keep `JobQueue` and rely on convention/review to pair events with operations

- Pros: no breaking change; consumers migrate at their own pace.
- Cons: empirically failed already -- the examples compiled through the entire
  ADR-0008 landing without acknowledging it; the eager-fact bug remains
  expressible.
- Rejected because: the invariant is exactly the kind that must live in types,
  not review checklists. If the compiler does not force the migration, the
  contract did not change.

### `Decision::Dispatch { events: Vec<Event>, dispatch: JobDispatch }`

- Pros: lets a command record additional domain facts alongside the intent.
- Cons: reopens the hole -- the accompanying events are free-form, so a handler
  can emit the accomplished-fact event next to the enqueue again.
- Rejected because: the domain payload those events would carry rides the job
  inside `Dispatched` instead; if a genuine pure fact coincides with an intent,
  it is expressible as its own command or folded from `Dispatched`.

### Allow N job dispatches per command

- Pros: one command could fan out several effects.
- Cons: multiplies the guard/correlation surface; the audit found zero handler
  sites that need it (multi-leg flows are sequential chains).
- Rejected because: chains-of-single-dispatches express the real cases and keep
  "what did this command durably start" answerable with one job id.

## Consequences

- **Every consumer handler breaks** -- by design. Both examples must be
  rewritten: `Order::Placed` becomes the fold of a dispatched-confirmation
  intent that settles through delivery; the support ticket's `NotifyClosed`
  becomes an `Output = ()` job or moves to a standalone enqueue outside the
  handler.
- The ADR-0008 machinery is reshaped, not discarded: the embedded machine,
  submit/reconcile, `OriginPort` delivery all stay; the command-plus-queue
  transition form is replaced by `dispatch` (guarded construction) and a sealed
  settlement path.
- `Lifecycle::handle` takes over what `JobQueue`'s task-local scope did:
  `Decision::Dispatch` is turned into the wrapped `Dispatched` event plus the
  transactional enqueue inside the framework, where it cannot be forgotten or
  duplicated.
- Entities whose commands never touch the outside world return
  `Decision::Events` everywhere and pay one enum wrap per handler -- the full
  cost of the guarantee for everyone else.
- SPEC.md's write path, docs/cqrs.md, docs/domain.md, and the ADR-0008 sections
  describing handler-side `JobQueue` pushes must be updated with the new
  contract.
- Blast radius if wrong: the change is contract-shape, not storage-shape --
  event schemas gain the job payload inside `Requested` but the events table,
  job stream, and delivery semantics are unchanged, so a reversal is another
  signature migration, not a data migration.
