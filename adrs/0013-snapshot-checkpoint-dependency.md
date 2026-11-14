# ADR-0013: Explicit snapshot checkpoints through the CQRS dependency

- Status: Accepted
- Date: 2026-11-12
- Issue:
  [Snapshot checkpoint contract](https://github.com/dataclique/event-sorcery/issues/128)

## Context

A snapshot can cover a prefix of one committed event batch. With interval two,
three events from an empty stream produce a snapshot covering sequence two and
leave sequence three for replay. The snapshot version is a separate counter.
Recording the batch's final sequence skips the unsnapshotted suffix on reload.

Registry `cqrs-es` 0.5.0 calculates that prefix but passes repositories only the
aggregate identity, payload and snapshot version. A repository cannot recover
the missing checkpoint reliably from those values, especially with sparse or
compacted history.

## Decision

Consume the maintained [CQRS fork](https://github.com/0xgleb/cqrs) as a Git
package pinned to `d61e05290bc7b24af1f615bc828a74ae6fc22f25`. Preserve upstream
licensing and provenance; do not vendor or relabel its source.

The additive `persist_with_snapshot` hook carries the existing
`SerializedSnapshot` type, including `current_sequence` and `current_snapshot`.
The generic store forwards its calculated checkpoint without changing snapshot
cadence. The default adapter preserves the required legacy `persist` signature;
it does not give legacy repository implementations the missing checkpoint. The
event-sorcery SQLite and PostgreSQL repositories override the hook.

Cargo package identity includes its source. Applications whose external backends
depend on registry `cqrs-es` 0.5.0 must align the dependency graph with the same
pinned source, using a root-level Cargo patch or matching Git dependencies. A
matching version number alone is insufficient. The
[compatibility migration](../docs/cqrs.md#cqrs-dependency-for-external-backends)
defines the required manifest change. Backend method signatures remain intact.

This complements [ADR-0012](0012-postgresql-repository-persistence.md): the
PostgreSQL repository still owns its complete transaction and no fork of
`postgres-es` is required.

## Alternatives considered

### Keep registry 0.5.0 and infer coverage in each repository

- Pros: preserves the registry package identity without consumer overrides.
- Cons: the persistence tuple omits the producer's checkpoint; snapshot
  counters, batch ends and retained row counts do not supply it.
- Rejected because: inference can lose or duplicate events on replay.

### Change the required persistence signature

- Pros: every implementation would receive the complete snapshot metadata.
- Cons: all existing repository implementations would need source changes.
- Rejected because: a provided hook carries the metadata without removing the
  legacy entry point.

## Consequences

- Native snapshot persistence distinguishes covered sequence, batch end and
  snapshot version, preserving suffix replay and atomic job writes.
- Consumers must resolve one compatible CQRS package identity across framework
  and backend code; the simple example exercises a registry requirement with the
  root patch.
- Custom repositories need a snapshot-aware override to preserve explicit prefix
  coverage. Source alignment alone does not change their legacy writer.
- The maintained fork and its immutable revision become part of the build
  contract and require review when updated.
