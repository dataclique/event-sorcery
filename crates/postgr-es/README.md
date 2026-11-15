# postgr-es

PostgreSQL repositories and native persistence integration for `event-sorcery`.

Apply `MIGRATOR` to the application pool before constructing `PostgresBackend`.
Application materialized-view tables remain caller-owned. Native startup
reconciles the entity schema before exposing its store.

Real PostgreSQL tests use the `integration-tests` feature. Linux flake checks
provision an isolated database through the PostgreSQL test hook and enable all
features; Darwin flake tests exclude database-dependent cases. Pure validation
and query-construction tests also run without a database.
