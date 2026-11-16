//! Real PostgreSQL coverage for the shared persistence contract.

#![cfg(feature = "integration-tests")]

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use sqlx::migrate::MigrateError;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use tokio::time::timeout;

use event_sorcery::{
    CompactionPolicy, DomainEvent, Effect, EventSourced, Never, Nil, PersistenceBackend,
    ReconcileError, StoreBuilder,
};
use postgr_es::{MIGRATOR, PostgresBackend};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Note<const VERSION: u64> {
    text: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum NoteEvent {
    Written { text: String },
}

impl DomainEvent for NoteEvent {
    fn event_type(&self) -> String {
        "NoteWritten".to_string()
    }

    fn event_version(&self) -> String {
        "1".to_string()
    }
}

#[async_trait]
impl<const VERSION: u64> EventSourced for Note<VERSION> {
    type Id = String;
    type Command = String;
    type Event = NoteEvent;
    type Error = Never;
    type Materialized = Nil;
    type Jobs = Nil;

    const AGGREGATE_TYPE: &'static str = "NativeNote";
    const SCHEMA_VERSION: u64 = VERSION;
    const SNAPSHOT_SIZE: usize = 2;
    const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;
    const PROJECTION: Nil = Nil;

    fn originate(event: &Self::Event) -> Option<Self> {
        let NoteEvent::Written { text } = event;
        Some(Self { text: text.clone() })
    }

    fn evolve(_entity: &Self, event: &Self::Event) -> Result<Option<Self>, Never> {
        Ok(Self::originate(event))
    }

    async fn initialize(text: String) -> Result<Effect<Self>, Never> {
        Ok(Effect::Events(vec![NoteEvent::Written { text }]))
    }

    async fn transition(&self, text: String) -> Result<Effect<Self>, Never> {
        Self::initialize(text).await
    }
}

#[tokio::test]
async fn unprojected_builder_preserves_unavailable_storage_error() {
    let pool = PgPool::connect_lazy("postgresql://localhost/unused").unwrap();
    pool.close().await;

    let result = StoreBuilder::<Note<1>, PostgresBackend>::with_backend(PostgresBackend { pool })
        .build()
        .await;
    let Err(error) = result else {
        panic!("closed storage must prevent startup");
    };

    assert!(matches!(
        error,
        ReconcileError::Sqlx(sqlx::Error::PoolClosed)
    ));
}

#[sqlx::test(migrations = false)]
async fn unprojected_builder_restarts_and_reconciles_with_one_connection(pool: PgPool) {
    MIGRATOR.run(&pool).await.unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let backend = PostgresBackend { pool: pool.clone() };
    let store = timeout(
        Duration::from_secs(10),
        StoreBuilder::<Note<1>, PostgresBackend>::with_backend(backend.clone()).build(),
    )
    .await
    .expect("startup must not acquire a second pool connection")
    .unwrap();
    let id = "note".to_string();
    store.send(&id, "first".to_string()).await.unwrap();
    store.send(&id, "second".to_string()).await.unwrap();
    assert_eq!(store.load(&id).await.unwrap().unwrap().text, "second");
    drop(store);

    let restarted = StoreBuilder::<Note<1>, PostgresBackend>::with_backend(backend.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(restarted.load(&id).await.unwrap().unwrap().text, "second");
    drop(restarted);
    let before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM snapshots WHERE aggregate_type = $1")
            .bind("NativeNote")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(before, 1);

    let upgraded = StoreBuilder::<Note<2>, PostgresBackend>::with_backend(backend)
        .build()
        .await
        .unwrap();
    assert_eq!(upgraded.load(&id).await.unwrap().unwrap().text, "second");
    let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM snapshots WHERE aggregate_type = $1")
        .bind("NativeNote")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(after, 0);
    let recorded: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM events \
         WHERE aggregate_type = 'SchemaRegistry' AND aggregate_id = 'schema' \
         ORDER BY sequence DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        recorded,
        json!({"VersionUpdated": {"name": "NativeNote", "version": 2}})
    );
}

#[sqlx::test(migrations = false)]
async fn failed_registry_registration_preserves_snapshot_and_allows_upgrade_retry(pool: PgPool) {
    MIGRATOR.run(&pool).await.unwrap();
    let backend = PostgresBackend { pool: pool.clone() };
    let store = StoreBuilder::<Note<1>, PostgresBackend>::with_backend(backend.clone())
        .build()
        .await
        .unwrap();
    let id = "note".to_string();
    store.send(&id, "first".to_string()).await.unwrap();
    store.send(&id, "second".to_string()).await.unwrap();
    drop(store);

    let snapshot_sql = "SELECT last_sequence, snapshot_version, payload FROM snapshots \
                        WHERE aggregate_type = 'NativeNote' AND aggregate_id = 'note'";
    let before: (i64, i64, serde_json::Value) =
        sqlx::query_as(snapshot_sql).fetch_one(&pool).await.unwrap();
    sqlx::query(
        "ALTER TABLE events ADD CONSTRAINT reject_registry_upgrade CHECK (\
         aggregate_type <> 'SchemaRegistry' OR \
         payload <> '{\"VersionUpdated\": {\"name\": \"NativeNote\", \"version\": 2}}'::JSONB)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let result = StoreBuilder::<Note<2>, PostgresBackend>::with_backend(backend.clone())
        .build()
        .await;
    let Err(error) = result else {
        panic!("registry rejection must prevent startup");
    };
    assert!(matches!(error, ReconcileError::Aggregate(_)));

    let after: (i64, i64, serde_json::Value) =
        sqlx::query_as(snapshot_sql).fetch_one(&pool).await.unwrap();
    assert_eq!(after, before);
    let registry_sql = "SELECT payload FROM events WHERE aggregate_type = 'SchemaRegistry' \
                        AND aggregate_id = 'schema' ORDER BY sequence DESC LIMIT 1";
    let recorded: serde_json::Value = sqlx::query_scalar(registry_sql)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        recorded,
        json!({"VersionUpdated": {"name": "NativeNote", "version": 1}})
    );

    sqlx::query("ALTER TABLE events DROP CONSTRAINT reject_registry_upgrade")
        .execute(&pool)
        .await
        .unwrap();
    let upgraded = StoreBuilder::<Note<2>, PostgresBackend>::with_backend(backend)
        .build()
        .await
        .unwrap();
    assert_eq!(upgraded.load(&id).await.unwrap().unwrap().text, "second");
    let recorded: serde_json::Value = sqlx::query_scalar(registry_sql)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        recorded,
        json!({"VersionUpdated": {"name": "NativeNote", "version": 2}})
    );
}

#[sqlx::test(migrations = false)]
async fn bootstrap_initializes_framework_storage(pool: PgPool) {
    let backend = PostgresBackend { pool: pool.clone() };
    backend.bootstrap_persistence().await.unwrap();
    backend.bootstrap_persistence().await.unwrap();

    let tables: Vec<String> = sqlx::query_scalar(
        r"
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename IN ('events', 'snapshots', 'job_queue_projection')
        ORDER BY tablename
        ",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    assert_eq!(tables, ["events", "job_queue_projection", "snapshots"]);
}

#[tokio::test]
async fn backend_bootstrap_preserves_an_unavailable_pool_error() {
    // A closed lazy pool never opens a database connection.
    let pool = PgPool::connect_lazy("postgresql://localhost/unused").unwrap();
    let backend = PostgresBackend { pool: pool.clone() };
    pool.close().await;

    let error = backend.bootstrap_persistence().await.unwrap_err();
    let sqlx::Error::Migrate(source) = error else {
        panic!("expected a migration error from the closed pool");
    };
    assert!(matches!(
        *source,
        MigrateError::Execute(sqlx::Error::PoolClosed)
    ));
}

#[sqlx::test(migrations = false)]
async fn bootstrap_failure_rolls_back_and_can_be_retried(pool: PgPool) {
    sqlx::query("CREATE TABLE snapshots (marker TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO snapshots (marker) VALUES ('keep')")
        .execute(&pool)
        .await
        .unwrap();

    let backend = PostgresBackend { pool: pool.clone() };
    let error = backend.bootstrap_persistence().await.unwrap_err();
    let sqlx::Error::Migrate(error) = error else {
        panic!("expected bootstrap to preserve its migration error");
    };
    let MigrateError::ExecuteMigration(source, version) = *error else {
        panic!("expected the bootstrap migration to fail");
    };
    assert_eq!(version, MIGRATOR.iter().next().unwrap().version);
    let sqlx::Error::Database(source) = source else {
        panic!("expected a database error for the conflicting table");
    };
    assert_eq!(source.code().as_deref(), Some("42P07"));

    let tables: Vec<String> = sqlx::query_scalar(
        r"
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename IN ('events', 'snapshots', 'job_queue_projection')
        ORDER BY tablename
        ",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(tables, ["snapshots"]);

    let marker: String = sqlx::query_scalar("SELECT marker FROM snapshots")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(marker, "keep");

    sqlx::query("DROP TABLE snapshots")
        .execute(&pool)
        .await
        .unwrap();
    let independent = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    timeout(
        Duration::from_secs(10),
        PostgresBackend { pool: independent }.bootstrap_persistence(),
    )
    .await
    .expect("retry must not wait for a migration lock leaked by the still-open original pool")
    .unwrap();

    let tables: i64 = sqlx::query_scalar(
        r"
        SELECT COUNT(*)
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename IN ('events', 'snapshots', 'job_queue_projection')
        ",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(tables, 3);
}
