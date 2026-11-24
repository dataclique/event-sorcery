//! SQLite implementation of event sourcing persistence for cqrs-es
//!
//! This crate provides SQLite-backed event store and view repositories for use
//! with the cqrs-es framework. It follows the same pattern as postgres-es and
//! mysql-es.
//!
//! # Features
//!
//! - **Event Store**: Persistent storage for domain events with snapshot support
//! - **View Repository**: Generic view persistence with optimistic locking
//! - **SQLite Backend**: Lightweight, serverless database for event sourcing
//! - **JSON Storage**: Events and views stored as JSON for flexibility
//!
//! # Example Usage
//!
//! ## Event Repository
//!
//! ```ignore
//! use sqlite_es::SqliteEventRepository;
//! use sqlx::SqlitePool;
//!
//! let pool = SqlitePool::connect("sqlite::memory:").await?;
//! let repo = SqliteEventRepository::new(pool);
//!
//! // Use with CqrsFramework
//! let cqrs = CqrsFramework::new(store, vec![], Services::default());
//! ```
//!
//! ## View Repository
//!
//! ```ignore
//! use sqlite_es::SqliteViewRepository;
//! use sqlx::SqlitePool;
//! use cqrs_es::persist::GenericQuery;
//!
//! let pool = SqlitePool::connect("sqlite::memory:").await?;
//! let view_repo = SqliteViewRepository::<MyView, MyAggregate>::new(
//!     pool,
//!     "my_view".to_string()
//! );
//!
//! // Use with GenericQuery processor
//! let query = GenericQuery::new(view_repo);
//! ```

mod aggregate_ids;
mod compaction;
mod contention;
mod cqrs;
mod event_page;
mod event_repository;
mod job_queue;
mod projection_queries;
mod projection_recovery;
mod schema_registry;
mod snapshot;
mod sql_query;
mod stream_version;
pub mod testing;
mod vacuum;
mod view_repository;

/// The canonical event-store schema (events, snapshots, job queue,
/// schema registry), embedded at compile time from this crate's
/// `migrations/` directory.
///
/// Run it against an application database with
/// `MIGRATOR.run(&pool)`. Consumers that manage their own migration
/// history instead copy the `.sql` files from this crate's
/// `migrations/` directory into their application's migration
/// directory -- all of them together, never a subset -- and never
/// edit them afterwards.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub use aggregate_ids::{count_aggregates, load_all_ids, load_ids_paginated};
pub use compaction::compact_events;
pub use contention::is_retryable_sqlite_busy;
pub use cqrs::{SqliteCqrs, sqlite_cqrs};
pub use cqrs_es::persist::ViewContext;
pub use event_page::{EventPageReadError, load_events_page, load_events_page_bounded};
pub use event_repository::{
    SqliteAggregateError, SqliteEventRepository, insert_serialized_events_batch, load_event_rows,
    replay_events,
};
pub use job_queue::{SqliteJobError, claim_job, enqueue_job, renew_job};
pub use projection_queries::{SqliteProjectionFilterError, filter_projection, projection_rows};
pub use projection_recovery::SqliteProjectionRecovery;
pub use schema_registry::load_schema_registry_payloads;
pub use snapshot::{
    StoreSnapshotError, clear_snapshots, discard_snapshot, load_snapshot, store_snapshot,
    write_snapshot,
};
pub use stream_version::committed_stream_version;
pub use vacuum::{incremental_vacuum, vacuum};
pub use view_repository::{
    Cmp, IndexedView, Order, Predicate, SqliteViewError, SqliteViewRepository, Term, Value,
};

#[cfg(test)]
mod tests {
    //! Durable terminal-job projection metadata survives schema migration.

    use serde_json::json;
    use sqlx::migrate::Migrator;
    use sqlx::sqlite::SqlitePoolOptions;

    use crate::MIGRATOR;

    #[tokio::test]
    async fn terminal_job_migration_preserves_rows_and_recomputes_generated_fields() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        let previous = Migrator::with_migrations(
            MIGRATOR
                .iter()
                .filter(|migration| migration.version < 20_261_115_095_705)
                .cloned()
                .collect(),
        );
        previous.run(&pool).await.unwrap();
        sqlx::query(
            "ALTER TABLE job_queue_projection ADD COLUMN operator_note TEXT DEFAULT 'retained'",
        )
        .execute(&pool)
        .await
        .unwrap();
        let jobs = [
            (
                "01ARZ3NDEKTSV4RRFFQ69G5FAW",
                json!({"Live": {"Done": {
                    "kind": "migration-probe"
                }}}),
                "done",
                "resolved by operator",
            ),
            (
                "01ARZ3NDEKTSV4RRFFQ69G5FAX",
                json!({"Live": {"Dead": {
                    "kind": "migration-probe", "reason": "Abandoned"
                }}}),
                "dead",
                "review abandonment",
            ),
        ];
        for (view_id, payload, _, note) in &jobs {
            sqlx::query(
                "INSERT INTO job_queue_projection \
                 (view_id, version, payload, lease_until, operator_note) VALUES (?1, 7, ?2, ?3, ?4)",
            )
            .bind(view_id)
            .bind(payload.to_string())
            .bind(1_700_000_030_000_i64)
            .bind(note)
            .execute(&pool)
            .await
            .unwrap();
        }

        MIGRATOR.run(&pool).await.unwrap();
        for (view_id, payload, expected_status, expected_note) in &jobs {
            let note: String = sqlx::query_scalar(
                "SELECT operator_note FROM job_queue_projection WHERE view_id = ?1",
            )
            .bind(view_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(note, *expected_note);
            let (version, stored, lease, kind, status, run_at): (
                i64,
                String,
                Option<i64>,
                Option<String>,
                Option<String>,
                Option<i64>,
            ) = sqlx::query_as(
                "SELECT version, payload, lease_until, kind, status, run_at \
                 FROM job_queue_projection WHERE view_id = ?1",
            )
            .bind(view_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(version, 7);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&stored).unwrap(),
                *payload
            );
            assert_eq!(lease, Some(1_700_000_030_000));
            assert_eq!(kind.as_deref(), Some("migration-probe"));
            assert_eq!(status.as_deref(), Some(*expected_status));
            assert_eq!(run_at, None);
        }
    }
}
