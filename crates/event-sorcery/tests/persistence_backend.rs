//! External compatibility contracts for persistence and legacy worker backends.

use cqrs_es::persist::SerializedEvent;
use sqlx::SqlitePool;
use sqlx::migrate::MigrateError;
use sqlx::sqlite::SqlitePoolOptions;

use event_sorcery::{
    ClaimDecision, ClaimOutcome, ClaimRead, CompactionPolicy, EventBackend, LeaseRenewal,
    PersistenceBackend, SchemaRegistry, SqliteBackend, SqliteJobError, Store, StoreBuilder,
};

#[derive(Clone)]
struct LegacyBackend(SqliteBackend);

impl EventBackend for LegacyBackend {
    type EventRepo = <SqliteBackend as EventBackend>::EventRepo;
    type Error = SqliteJobError;

    fn event_repo(&self, policy: CompactionPolicy) -> Self::EventRepo {
        let Self(backend) = self;
        backend.event_repo(policy)
    }

    async fn migrate(&self) -> Result<(), Self::Error> {
        let Self(backend) = self;
        backend.migrate().await
    }

    async fn claim<Decide, Won>(
        &self,
        job_id: &str,
        decide: Decide,
    ) -> Result<ClaimOutcome<Won>, Self::Error>
    where
        Decide: FnOnce(Option<ClaimRead>) -> ClaimDecision<Won> + Send,
        Won: Send,
    {
        let Self(backend) = self;
        backend.claim(job_id, decide).await
    }

    async fn renew(
        &self,
        job_id: &str,
        claim_seq: i64,
        lease_until: i64,
    ) -> Result<LeaseRenewal, Self::Error> {
        let Self(backend) = self;
        backend.renew(job_id, claim_seq, lease_until).await
    }

    async fn enqueue(&self, event: SerializedEvent, payload: String) -> Result<(), Self::Error> {
        let Self(backend) = self;
        backend.enqueue(event, payload).await
    }
}

#[derive(Clone)]
struct NarrowBackend(SqliteBackend);

impl PersistenceBackend for NarrowBackend {
    type PersistenceRepo = <SqliteBackend as EventBackend>::EventRepo;
    type BootstrapError = SqliteJobError;

    fn persistence_repo(&self, policy: CompactionPolicy) -> Self::PersistenceRepo {
        let Self(backend) = self;
        backend.event_repo(policy)
    }

    async fn bootstrap_persistence(&self) -> Result<(), Self::BootstrapError> {
        let Self(backend) = self;
        backend.migrate().await
    }
}

async fn legacy_contract<Backend: EventBackend>(
    backend: &Backend,
    _store: Option<&Store<SchemaRegistry, Backend>>,
) -> Result<Backend::EventRepo, Backend::Error> {
    backend.migrate().await?;
    Ok(backend.event_repo(CompactionPolicy::Retain))
}

#[tokio::test]
async fn legacy_backends_gain_persistence_without_new_required_members() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(":memory:")
        .await
        .unwrap();
    let backend = LegacyBackend(SqliteBackend::new(pool.clone()));

    backend.bootstrap_persistence().await.unwrap();
    let _: <SqliteBackend as EventBackend>::EventRepo =
        legacy_contract(&backend, None).await.unwrap();
    let _: <SqliteBackend as EventBackend>::EventRepo =
        backend.persistence_repo(CompactionPolicy::Retain);

    let _store = StoreBuilder::<SchemaRegistry>::new(pool.clone())
        .build()
        .await
        .unwrap();
    let before: Vec<(String, i64, String)> =
        sqlx::query_as("SELECT aggregate_id, sequence, payload FROM events ORDER BY sequence")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(before.len(), 1);

    backend.bootstrap_persistence().await.unwrap();

    let after: Vec<(String, i64, String)> =
        sqlx::query_as("SELECT aggregate_id, sequence, payload FROM events ORDER BY sequence")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(after, before);
}

#[tokio::test]
async fn narrow_backend_types_do_not_require_worker_capabilities() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    let backend = NarrowBackend(SqliteBackend::new(pool.clone()));

    backend.bootstrap_persistence().await.unwrap();
    let _: <SqliteBackend as EventBackend>::EventRepo =
        backend.persistence_repo(CompactionPolicy::Retain);
    let _: Option<Store<SchemaRegistry, NarrowBackend>> = None;
    let _builder = StoreBuilder::<SchemaRegistry, NarrowBackend>::with_backend(backend);

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn bootstrap_preserves_the_legacy_backend_error() {
    let pool = SqlitePool::connect(":memory:").await.unwrap();
    let backend = LegacyBackend(SqliteBackend::new(pool.clone()));
    pool.close().await;

    let error = backend.bootstrap_persistence().await.unwrap_err();

    let SqliteJobError::Sql(sqlx::Error::Migrate(source)) = error else {
        panic!("expected the migration error from the legacy backend");
    };
    assert!(matches!(
        *source,
        MigrateError::Execute(sqlx::Error::PoolClosed)
    ));
}
