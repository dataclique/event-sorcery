# Migrating a manual / apalis job system to event-sorcery durable jobs

This guide is for a consumer that already runs background work through a manual
or apalis-backed job system (e.g. `st0x.liquidity`'s `conductor::job` over
`apalis-sqlite`) and wants to move to event-sorcery's cqrs-native durable jobs
(ADR-0006,
[`docs/event-sourced-job-backend-spec.md`](event-sourced-job-backend-spec.md)).

The examples use job kinds from the legacy `st0x.liquidity` workload.
event-sorcery provides execution, standalone enqueue and successful deferral; a
consumer must still satisfy its kind-specific migration requirements (§2) and
preserve in-flight work across the cutover (§3).

## 1. What event-sorcery already covers

If your job system uses these, event-sorcery is a clean (often stronger) drop-in
for the _execution_ side:

- **Durable at-least-once execution** — each job is its own event stream
  (`Enqueued`) + a `job_queue_projection` projection row, replacing apalis
  `SqliteStorage` rows.
- **Standalone durable enqueue** — `JobRuntime::enqueue` appends the job's
  `Enqueued` event and seeds its queue row in one transaction. Reactors,
  pollers, sibling jobs and startup recovery can use it without a command
  handler. It is not atomic with an earlier event or external operation that
  triggered the enqueue, so producers still need retry and idempotency rules.
- **Successful deferral** — `Ok(JobOutcome::Defer(delay))` reschedules the job
  without recording a failed attempt. Use it for polling and external waits
  rather than keeping a claim open across a long-running `perform`.
- **Retry with exponential backoff** — `max_attempts` + `Backoff` via
  `RetryScheduled`; `attempt` is bumped only by a recorded failure, so a crash
  is not a failed attempt (matches apalis's contract).
- **Serialized execution per kind** — `max_concurrency = 1` reproduces apalis
  `.concurrency(1)`; the `ConcurrencyLimit` layer gates the _claim_, so a job is
  claimed only when a slot is free.
- **Crash / orphan recovery** — the claim/lease/`renew` with a projection-only
  `lease_until` column re-claims a crashed worker's job once its lease expires.
  This **replaces `requeue_orphaned`** and sidesteps the apalis
  deterministic-worker-name heartbeat bug — but note it recovers after the lease
  expires (~`lease_duration`), not instantly at startup (§4).
- **Dead-letter with audit retention** — `JobState::Dead { reason }` rows are
  retained (`status = 'dead'`, never re-run), like apalis
  Failed-for-reconciliation.
- **Crash-loop protection** — `max_claims` dead-letters as `Abandoned` a job
  that keeps claiming but never records an outcome (apalis lacked this).
- **Execution timeout** — `tokio::time::timeout(execution_timeout)` per
  `perform`.
- **Configurable polling** — the default polling interval is 250ms, without
  apalis-style exponential idle backoff. Validate pickup and execution latency
  under the consumer's backlog, concurrency limits and database contention; the
  polling interval alone does not establish an SLO.

Not load-bearing either way: apalis-workflow DAG `Output` composition (liquidity
sets `Output = ()` everywhere) and the `FailureInjector` (test-only).

## 2. What event-sorcery does NOT cover yet (prerequisites)

Command-born work uses `Effect::Dispatch` and commits with the command's
`Dispatched` event. Work originating in reactors, polling loops, sibling jobs or
startup recovery uses `JobRuntime::enqueue`; self-rescheduling work can use
`JobOutcome::Defer`. Neither capability requires reshaping external producers
into artificial command handlers.

The remaining migration requirements depend on the job kind. Verify these before
enabling that kind on the new worker:

| Prerequisite                                                                                       | Why liquidity needs it                                                                                                                                                                                                                        |
| -------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Enqueue-time dedup** (optional `Job::dedup_key`) + **`has_in_flight` / pending-count query**     | Backfill overlap-skip, per-symbol recovery dedup, direction-independent transfer dedup. ES mints a fresh ULID per push with no unique key.                                                                                                    |
| **Per-kind terminal-failure (fail-stop) hook** fired on `Dead` (including `DeadReason::Abandoned`) | Liquidity crashes the process on a terminal trading/transfer failure and lets systemd restart clean. ES currently dead-letters and keeps idling. A kind that does _not_ install the hook is the best-effort tier — one mechanism covers both. |
| **Cancel-by-kind / cancel-by-dedup-key** (a terminal `Cancelled`)                                  | Discard stale pending work (rebalancing checks captured against pre-rebalance inventory; stale resume rows).                                                                                                                                  |
| **Lease/timeout sizing guidance** + steer long external waits to _defer_                           | Long waits need successful lease renewal and must fit within `execution_timeout`; prefer deferral to holding a claim across an external wait.                                                                                                 |
| **`prune_terminal_jobs`** (already an ADR-0006 follow-up)                                          | Replace apalis `spawn_finished_job_cleanup`; bound `job_queue_projection` growth while retaining `Dead` rows for audit.                                                                                                                       |

Standalone enqueue and deferral are available. Migration of kinds requiring
deduplication or fail-stop behavior must still wait until those requirements are
satisfied; enqueue availability alone does not establish migration safety.

## 3. The safe cutover (per kind)

Both backends can coexist on the **same SQLite file** during the migration
(apalis `Jobs` table + event-sorcery
`events`/`snapshots`/`job_queue_projection`). Run both migrators with
`set_ignore_missing(true)` against the shared `_sqlx_migrations`.

For each job kind, migrate one kind at a time, leaf-first, using a producer and
worker barrier:

1. Pause every producer of that kind, including reactors, pollers, job chains,
   self-reschedules and startup recovery. Preserve incoming work durably or
   ensure it can be reconstructed; pausing must not silently drop arrivals.
2. Keep the apalis worker draining its existing rows, with ES execution for that
   kind disabled. Wait for zero non-terminal apalis rows and no active apalis
   execution. Reconcile terminal failures with unresolved effects, including
   ambiguous external outcomes or unaccounted fills, before allowing the
   affected resource to resume. A row that cannot drain or whose effects cannot
   be reconciled blocks the cutover; do not delete it or assume safety.
3. Stop that kind's apalis worker, then switch its enqueue sites atomically. Use
   `JobRuntime::enqueue` for standalone producers and retain `Effect::Dispatch`
   for work that must commit with command events.
4. Enable its ES worker and replay buffered arrivals or run the required
   reconstruction. Verify that every captured item is represented, preserving
   its domain identity, before resuming producers. Unaccounted arrivals or
   unresolved old outcomes block completion. Retain old terminal rows for audit
   until the whole migration permits decommissioning apalis.

If arrivals cannot pause, they may be durably buffered in ES while the old
worker drains, but ES execution must remain disabled until the barrier is
satisfied. Concurrent execution on both stacks requires an independently
verified resource-scoped fence or overlap-safe domain semantics. Routing each
enqueue to one backend does not prevent an old job and a different new job from
operating on the same resource.

### Cutover order (liquidity)

Before enabling any supervised kind, wire and verify its required fail-stop
behavior, including `DeadReason::Abandoned`, and its end-to-end failure
assertions. `ResumeTokenizationAggregate` remains best-effort without that hook.
The following legacy kind names illustrate ordering, not a certification of a
consumer's current recovery behavior. Verify its dependency graph, record
ownership and reconstruction guarantees before migrating leaf-first, lowest-risk
first:

1. **Reconstructible work, such as `ResumeTokenizationAggregate`** — first
   verify that source events can reconstruct every outstanding item. Use this
   kind to validate standalone enqueue and verified replay of cutover-window
   arrivals end to end. Keep its best-effort failure policy explicit.
2. **Chained / idempotent trading queues** — `AccountForDexTrade`,
   `PollOrderStatus` (uses deferral), `ReconcileOrderFill`,
   `HandleOrderRejection`, `PlaceHedge`. Pollers (`BackfillRange`,
   `CheckPositions`) re-drive naturally.
3. **Trade accounting, such as `AccountForDexTrade`** — determine whether a
   queued row is the sole remaining record of an observed fill after its
   backfill checkpoint advances. If so, never remove the worker, recreate the
   database or drop `Jobs` before that fill is durably accounted for. A failed
   row is a hard STOP-and-reconcile, even if its queue status is terminal.
4. **Money-movement + self-rescheduling queues** — USDC/equity transfers,
   wrapped/unwrapped recovery, `CheckPositions`. Use deferral and verify the
   required deduplication and cancellation contracts before cutover.
5. **Decommission apalis** (final, irreversible) — only once every queue has
   zero non-terminal rows, no active execution and no unresolved terminal
   failures or ambiguous external outcomes. Retain reconciled rows and domain
   identities for audit; then remove the second pool, apalis migrations,
   `JobQueue`/`JobKind`/`FailureInjector`, and the finished-job cleanup.

## 4. Safety invariants (audit each migrated job against these)

- **Idempotency keys on domain identity, NOT `job_id`.** ES is at-least-once for
  _every_ `perform`. Retries and reclaims keep the same `job_id`; re-enqueue
  creates a new job identity. A `job_id` key can deduplicate attempts of that
  job, but cannot deduplicate a re-enqueued or cross-stack duplicate. For
  workloads keyed on `tx_hash`/`log_index`, `symbol` or transfer direction,
  preserve those domain guards. Any enqueue-time deduplication is _additive_
  defense, not a replacement for the event-reconstructed fail-closed
  `usdc_in_progress` / `equity_in_progress` guards (those are the authoritative
  re-burn defense for real money).
- **An ES-only in-flight check cannot fence the other stack.** Checking
  `job_queue_projection` does not see apalis `Jobs` rows. For a kind whose only
  overlap defense was `has_in_flight` (e.g. `BackfillRange`), use the pause and
  drain barrier above. An enqueuer flip alone does not establish that old and
  new work cannot overlap.
- **`max_concurrency = 1` does NOT halt after a dead-letter** the way apalis
  fail-stop does — the ES poll loop moves straight to the next pending
  candidate. The halt depends entirely on the fail-stop hook crashing the
  process _before_ the next poll claims. Make the hook synchronous/abort-fast,
  and treat the Dead-to-exit window as a real race for kinds that must not run
  with stale state.
- **Model multi-minute external waits as _defer_, not a long `perform`.**
  Healthy executions renew their leases. Failed renewal or lease loss can expose
  work to a reclaimer; domain idempotency must still prevent duplicate effects.
  An execution timeout records a failure and uses the retry budget before
  dead-lettering. Deferral returns quickly and re-arms without holding a claim
  across the wait.
- **Route `DeadReason::Abandoned` (claim-budget) through the fail-stop hook**
  for critical kinds, so a restart-heavy crash-loop escalates loudly instead of
  dying silently.
- **Single process owner during cutover.** ES startup/lease recovery assumes one
  fresh process; systemd must ensure the old process exits before the new starts
  (the ES claim CAS is strictly safer here than apalis's heartbeat).
- **`SeedVaultRegistry` runs inline at startup, not queued** — do not give it an
  ES worker that could double-run it.

## 5. Schema migration

A consumer owns its migrations. event-sorcery ships the canonical schema; copy
it into your migration directory (see `examples/simple/migrations/`):

- `…_init.sql` — the `events` + `snapshots` tables (the event-sourcing core).
- `…_job_queue.sql` — the legacy durable-job projection schema (generated
  `kind`/`status`/`run_at` columns + the projection-only `lease_until`).
- `…_rename_job_queue_projection.sql` — rename that table to
  `job_queue_projection`, preserving its rows and leases. Keep the earlier
  migration unchanged and apply the complete migration chain.

`StoreBuilder::build()` and `JobRuntime::build()` wire over an already-migrated
pool; neither migrates for you (so they compose with your own view migrations
instead of conflicting). Apply all migrations at startup before building either.
