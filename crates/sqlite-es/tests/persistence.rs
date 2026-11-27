#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::AggregateError;
    use cqrs_es::persist::SerializedEvent;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use sqlx::{SqlitePool, migrate::MigrateError, sqlite::SqlitePoolOptions};
    use std::sync::{Arc, Mutex};

    use event_sorcery::{
        ClaimDecision, ClaimOutcome, ClaimRead, CommitRequest, CompactionPolicy, DomainEvent,
        Effect, EventBackend, EventSourced, JobId, LeaseRenewal, Never, Nil, PersistenceBackend,
        ReconcileError, SchemaRegistry, Store, StoreBuilder, StreamIdentity, buffer_pending_job,
        register_schema, with_pending_jobs,
    };
    use sqlite_es::{Engine, SqliteBackend, SqliteJobError};

    #[tokio::test]
    async fn event_backend_migrate_initializes_the_existing_sqlite_schema() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        let backend = SqliteBackend { pool };
        EventBackend::migrate(&backend).await.unwrap();

        let stream = StreamIdentity::new("backend-migration-test", "one");
        let event = SerializedEvent {
            aggregate_type: "backend-migration-test".to_string(),
            aggregate_id: "one".to_string(),
            sequence: 1,
            event_type: "Created".to_string(),
            event_version: "1.0".to_string(),
            payload: json!({}),
            metadata: json!({}),
        };
        let engine = Engine::new(backend.pool);
        engine
            .commit(CommitRequest::new(
                stream.clone(),
                std::slice::from_ref(&event),
            ))
            .await
            .unwrap();
        assert_eq!(engine.load_events(&stream, None).await.unwrap(), [event]);
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct SchemaProbe<const VERSION: u64, const COMPACT: bool> {
        value: u64,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    enum ProbeEvent {
        ValueWritten { value: u64 },
    }

    impl DomainEvent for ProbeEvent {
        fn event_type(&self) -> String {
            "ValueWritten".to_string()
        }

        fn event_version(&self) -> String {
            "1".to_string()
        }
    }

    #[async_trait]
    impl<const VERSION: u64, const COMPACT: bool> EventSourced for SchemaProbe<VERSION, COMPACT> {
        type Id = String;
        type Command = u64;
        type Event = ProbeEvent;
        type Error = Never;
        type Materialized = Nil;
        type Jobs = Nil;

        const AGGREGATE_TYPE: &'static str = "PersistenceProbe";
        const SCHEMA_VERSION: u64 = VERSION;
        const SNAPSHOT_SIZE: usize = 2;
        const COMPACTION_POLICY: CompactionPolicy = if COMPACT {
            CompactionPolicy::CompactAfterSnapshot
        } else {
            CompactionPolicy::Retain
        };
        const PROJECTION: Nil = Nil;

        fn originate(event: &Self::Event) -> Option<Self> {
            let ProbeEvent::ValueWritten { value } = event;
            Some(Self { value: *value })
        }

        fn evolve(_entity: &Self, event: &Self::Event) -> Result<Option<Self>, Never> {
            Ok(Self::originate(event))
        }

        async fn initialize(value: u64) -> Result<Effect<Self>, Never> {
            Ok(Effect::Events(vec![ProbeEvent::ValueWritten { value }]))
        }

        async fn transition(&self, value: u64) -> Result<Effect<Self>, Never> {
            Self::initialize(value).await
        }
    }

    #[derive(Clone)]
    struct LegacyBackend {
        backend: SqliteBackend,
        policies: Arc<Mutex<Vec<CompactionPolicy>>>,
    }

    impl EventBackend for LegacyBackend {
        type EventRepo = <SqliteBackend as EventBackend>::EventRepo;
        type Error = SqliteJobError;

        fn event_repo(&self, policy: CompactionPolicy) -> Self::EventRepo {
            self.policies.lock().unwrap().push(policy);
            self.backend.event_repo(policy)
        }

        async fn migrate(&self) -> Result<(), Self::Error> {
            self.backend.migrate().await
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
            self.backend.claim(job_id, decide).await
        }

        async fn renew(
            &self,
            job_id: &str,
            claim_seq: i64,
            lease_until: i64,
        ) -> Result<LeaseRenewal, Self::Error> {
            self.backend.renew(job_id, claim_seq, lease_until).await
        }

        async fn enqueue(
            &self,
            event: SerializedEvent,
            payload: String,
        ) -> Result<(), Self::Error> {
            self.backend.enqueue(event, payload).await
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
        let backend = LegacyBackend {
            backend: SqliteBackend { pool: pool.clone() },
            policies: Arc::new(Mutex::new(vec![])),
        };

        backend.bootstrap_persistence().await.unwrap();
        let _: <SqliteBackend as EventBackend>::EventRepo =
            legacy_contract(&backend, None).await.unwrap();
        let _: <SqliteBackend as EventBackend>::EventRepo =
            backend.persistence_repo(CompactionPolicy::Retain);

        let _store =
            StoreBuilder::<SchemaRegistry, _>::with_backend(SqliteBackend { pool: pool.clone() })
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
        let backend = NarrowBackend(SqliteBackend { pool: pool.clone() });

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
        let backend = LegacyBackend {
            backend: SqliteBackend { pool: pool.clone() },
            policies: Arc::new(Mutex::new(vec![])),
        };
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

    #[tokio::test]
    async fn legacy_repository_factory_receives_each_policy_unchanged() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        let backend = LegacyBackend {
            backend: SqliteBackend { pool },
            policies: Arc::new(Mutex::new(vec![])),
        };
        let policies = [
            CompactionPolicy::Retain,
            CompactionPolicy::CompactAfterSnapshot,
            CompactionPolicy::RetainLast { events: 17 },
        ];
        for policy in policies {
            let _repository = backend.persistence_repo(policy);
        }
        assert_eq!(*backend.policies.lock().unwrap(), policies);
    }

    #[tokio::test]
    async fn schema_registration_is_idempotent_and_records_each_upgrade_once() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        let backend = SqliteBackend { pool: pool.clone() };
        backend.bootstrap_persistence().await.unwrap();

        register_schema::<SchemaProbe<1, false>, _>(
            backend.persistence_repo(CompactionPolicy::Retain),
        )
        .await
        .unwrap();
        register_schema::<SchemaProbe<1, false>, _>(
            backend.persistence_repo(CompactionPolicy::Retain),
        )
        .await
        .unwrap();
        register_schema::<SchemaProbe<2, false>, _>(
            backend.persistence_repo(CompactionPolicy::Retain),
        )
        .await
        .unwrap();

        let recorded: Vec<(String, i64, String)> = sqlx::query_as(
            "SELECT aggregate_id, sequence, payload FROM events \
             WHERE aggregate_type = 'SchemaRegistry' ORDER BY sequence",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        let recorded: Vec<_> = recorded
            .into_iter()
            .map(|(identity, sequence, payload)| {
                (
                    identity,
                    sequence,
                    serde_json::from_str::<serde_json::Value>(&payload).unwrap(),
                )
            })
            .collect();
        assert_eq!(
            recorded,
            [
                (
                    "schema".to_string(),
                    1,
                    json!({"VersionUpdated": {"name": "PersistenceProbe", "version": 1}}),
                ),
                (
                    "schema".to_string(),
                    2,
                    json!({"VersionUpdated": {"name": "PersistenceProbe", "version": 2}}),
                ),
            ],
        );
    }

    #[tokio::test]
    async fn schema_registration_does_not_commit_the_callers_pending_jobs() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        let backend = SqliteBackend { pool: pool.clone() };
        backend.bootstrap_persistence().await.unwrap();
        let job_id = JobId::new();

        with_pending_jobs(async {
            buffer_pending_job(
                job_id,
                "registry-isolation-probe",
                json!({"probe": "caller-dispatch"}),
            )
            .unwrap();

            Box::pin(register_schema::<SchemaProbe<1, false>, _>(
                backend.persistence_repo(CompactionPolicy::Retain),
            ))
            .await
            .unwrap();

            let committed_jobs: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE aggregate_type = 'job'")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(committed_jobs, 0);
            let (request, _) = CommitRequest::prepare_repository("caller", &[], None).unwrap();
            let (_, _, _, _, remaining) = request.check().unwrap().into_parts();
            assert_eq!(remaining.len(), 1);
            assert_eq!(
                remaining[0].event().unwrap().aggregate_id,
                job_id.to_string()
            );
        })
        .await;
    }

    #[tokio::test]
    async fn schema_registration_preserves_the_storage_error_chain() {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        let repository =
            (SqliteBackend { pool: pool.clone() }).persistence_repo(CompactionPolicy::Retain);
        pool.close().await;

        let error = register_schema::<SchemaProbe<1, false>, _>(repository)
            .await
            .unwrap_err();
        let ReconcileError::Aggregate(AggregateError::DatabaseConnectionError(source)) = error
        else {
            panic!("expected the registry storage error, got {error:?}");
        };
        let source = source.downcast::<sqlx::Error>().unwrap();
        assert!(matches!(*source, sqlx::Error::PoolClosed));
    }
}
