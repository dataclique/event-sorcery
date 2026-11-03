# ADR-0011: Explicit public trait configuration

- Status: Proposed
- Date: 2026-11-03
- Issue: [#118](https://github.com/dataclique/event-sorcery/issues/118)

## Context

`EventSourced` supplies a snapshot interval of `10` and a `Retain` compaction
policy when an implementation omits those constants. `StandaloneJob` supplies
`"Job failed after retries"` as its terminal-failure message. An implementation
can therefore hide persistence and reporting choices behind inherited values.

ADR-0010 preserves the native Rust interface unless a separate, documented 0.4.0
compatibility decision changes it. Requiring explicit configuration is a
source-compatibility change for implementations that currently omit settings; it
does not require different persistence, worker, or ABI behavior.

## Decision

Require implementations to declare `EventSourced::SNAPSHOT_SIZE`,
`EventSourced::COMPACTION_POLICY`, and `StandaloneJob::TERMINAL_FAILURE_MSG`.
Remove their trait-level default expressions without changing their types.

Migrate existing implementations to their previously selected values. The
`Job`-to-`StandaloneJob` adapter declares its existing generic failure message
explicitly; the `Job` trait gains no new constant.

Test-only const-generic defaults remain permitted. State `Default`
implementations and the deliberate backend type defaults described by ADR-0005
are unchanged.

## Alternatives Considered

### Keep defaults and expand documentation

- Pros: No source migration for existing implementations.
- Cons: An implementation can still omit consequential choices.
- Rejected because: Documentation alone does not make configuration explicit in
  each implementation.

### Move configuration into a new runtime options surface

- Pros: Settings could be supplied when constructing a store or worker.
- Cons: Adds a second configuration model and changes existing ownership and
  call sites beyond the omitted constants.
- Rejected because: Required associated constants express the existing
  per-implementation choices without a new runtime abstraction.

## Consequences

- Rust consumers that omitted a constant must declare their chosen value.
- Existing snapshot intervals, retention policies, and messages stay unchanged
  during the repository migration.
- Compiler-contract tests must reject omission of each required constant and
  accept fully configured implementations.
- No event schema, snapshot format, C ABI signature, or encoding changes.
- This records an explicit compatibility change within the 0.4.0 migration; it
  does not authorize a release or a separate version bump.
