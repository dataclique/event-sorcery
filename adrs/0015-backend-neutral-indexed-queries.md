# ADR-0015: Backend-neutral indexed-query contracts

- Status: Accepted
- Date: 2026-11-16
- Issue:
  [Native PostgreSQL stores](https://github.com/dataclique/event-sorcery/issues/122)

## Context

SQLite and PostgreSQL implement the same indexed materialized-view query
contract. Its predicate, comparison, value and ordering types currently belong
to `sqlite-es`, forcing PostgreSQL storage to depend directly on the SQLite
backend. Both adapters also translate the same typed predicates into SQL.

These are shared persistence contracts, not SQLite-specific behavior. SQLx
provides argument encoding and placeholders, but not this predicate language.
The CQRS dependency fork carries temporary upstream fixes; it is not a platform
for separately evolving query features.

## Decision

Introduce one focused neutral workspace crate, `indexed-query`, owning the
existing indexed-query contracts and their shared SQL lowering. Both storage
adapters consume it. Keep execution, connection pools, transactions and
storage-error adaptation in the existing repositories.

Preserve existing SQLite query import paths with reexports of the shared
implementations, not duplicate types or wrapper traits. Share predicate
validation, grouping, operators and identifier handling; retain native, fallible
driver argument encoding and placeholder generation. This is lowering an
existing typed query model, not introducing a parser or another backend
selection framework.

Keep the workspace's MIT licensing and applicable source notices. No query
capability is added to the CQRS fork. This complements
[ADR-0013](0013-snapshot-checkpoint-dependency.md): its snapshot-checkpoint
contract and pinned-source compatibility requirements remain unchanged.

## Alternatives considered

### Extend the CQRS fork

- Pros: places indexed querying beside the existing neutral `ViewRepository`
  contract without adding a workspace package.
- Cons: adds a separate feature-development responsibility to a temporary fix
  dependency, including compiler dependencies and cross-repository releases.
- Rejected because: the fork is limited to upstream fixes, not an independently
  evolving query platform.

### Reexport SQLite-owned contracts through event-sorcery

- Pros: removes the direct PostgreSQL manifest entry with little code movement.
- Cons: leaves SQLite owning the shared definitions and leaves SQL translation
  duplicated; a facade conceals the dependency instead of removing it.
- Rejected because: the shared contracts require an actual neutral owner.

## Consequences

- PostgreSQL no longer needs a direct SQLite dependency for indexed querying.
  This does not remove event-sorcery's existing transitive SQLite dependency.
- Both databases share the typed predicate grammar and validation while keeping
  their existing storage implementations, native arguments and database
  collation behavior.
- Existing query construction and SQLite import paths remain source-compatible;
  persisted event, snapshot and materialized-view formats do not change.
- The workspace gains one package and its associated version, documentation and
  verification responsibilities. Compiler regressions cover both drivers, and
  real database tests remain part of the ordinary workspace suite.
