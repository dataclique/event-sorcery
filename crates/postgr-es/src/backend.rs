//! Native PostgreSQL persistence capabilities over the shared repository.

use cqrs_es::Aggregate;
use cqrs_es::persist::{
    PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot,
};
use serde_json::Value;
use sqlx::PgPool;
use sqlx::migrate::MigrateError;

use event_sorcery::{
    CompactionPolicy, EventSourced, Nil, PersistenceBackend, ProjectedStoreBackend, Projection,
    ProjectionError, ProjectionRecoveryMode, ReconcileError, SchemaReconciliation, Table,
    UnmaterializedStoreBackend, guard_snapshot,
};

use crate::recovery::PostgresRecovery;
use crate::{MIGRATOR, PostgresEventRepository, PostgresViewBackend};

/// Native persistence capability over an application-provisioned pool.
#[derive(Clone)]
pub struct PostgresBackend {
    /// Application-provisioned PostgreSQL connection pool.
    pub pool: PgPool,
}

/// Policy-aware repository constructed by [`PostgresBackend`].
pub struct PostgresStoreRepository {
    repository: PostgresEventRepository,
    compaction_policy: CompactionPolicy,
}

impl PersistenceBackend for PostgresBackend {
    type PersistenceRepo = PostgresStoreRepository;
    type BootstrapError = sqlx::Error;

    fn persistence_repo(&self, policy: CompactionPolicy) -> Self::PersistenceRepo {
        PostgresStoreRepository {
            repository: PostgresEventRepository {
                pool: self.pool.clone(),
            },
            compaction_policy: policy,
        }
    }

    async fn bootstrap_persistence(&self) -> Result<(), Self::BootstrapError> {
        let mut connection = self.pool.acquire().await.map_err(MigrateError::from)?;
        connection.close_on_drop();
        MIGRATOR
            .run_direct(None, &mut *connection, false)
            .await
            .map_err(sqlx::Error::from)
    }
}

impl UnmaterializedStoreBackend for PostgresBackend {
    async fn reconcile_unmaterialized<Entity: EventSourced<Materialized = Nil>>(
        &self,
    ) -> Result<(), ReconcileError> {
        let (recovery, _) = PostgresRecovery::<Entity>::begin(&self.pool).await?;
        recovery.finish().await
    }
}

impl ProjectedStoreBackend for PostgresBackend {
    type ViewBackend = PostgresViewBackend;

    async fn reconcile_projected<Entity>(
        &self,
    ) -> Result<Projection<Entity, PostgresViewBackend>, ReconcileError>
    where
        Entity: EventSourced<Materialized = Table> + 'static,
    {
        use SchemaReconciliation::{Changed, Unchanged};

        match Entity::COMPACTION_POLICY {
            CompactionPolicy::Retain => {}
            CompactionPolicy::CompactAfterSnapshot | CompactionPolicy::RetainLast { .. } => {
                return Err(ReconcileError::ProjectionRequiresRetainedHistory {
                    aggregate_type: Entity::AGGREGATE_TYPE,
                });
            }
        }

        let (mut recovery, decision) = PostgresRecovery::<Entity>::begin(&self.pool).await?;
        let mode = match decision {
            Changed => ProjectionRecoveryMode::RebuildAll,
            Unchanged => ProjectionRecoveryMode::CatchUp,
        };
        recovery
            .recover_projection(&mode)
            .await
            .map_err(|error| match error {
                ProjectionError::Sqlx(source) => ReconcileError::Sqlx(source),
                ProjectionError::Persistence(source) => ReconcileError::Persistence(source),
                other => {
                    ReconcileError::Persistence(PersistenceError::UnknownError(Box::new(other)))
                }
            })?;
        recovery.finish().await?;
        Ok(Projection::with_backend(self.pool.clone()))
    }
}

impl PersistedEventRepository for PostgresStoreRepository {
    async fn get_events<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.repository
            .get_events::<AggregateType>(aggregate_id)
            .await
    }

    async fn get_last_events<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.repository
            .get_last_events::<AggregateType>(aggregate_id, last_sequence)
            .await
    }

    async fn get_snapshot<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        self.repository
            .get_snapshot::<AggregateType>(aggregate_id)
            .await?
            .map(|snapshot| guard_snapshot::<AggregateType>(snapshot, self.compaction_policy))
            .transpose()
    }

    async fn persist<AggregateType: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        self.repository
            .persist::<AggregateType>(events, snapshot_update)
            .await
    }

    async fn persist_with_snapshot<AggregateType: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<SerializedSnapshot>,
    ) -> Result<(), PersistenceError> {
        self.repository
            .persist_with_snapshot::<AggregateType>(events, snapshot_update)
            .await
    }

    async fn stream_events<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<ReplayStream, PersistenceError> {
        self.repository
            .stream_events::<AggregateType>(aggregate_id)
            .await
    }

    async fn stream_all_events<AggregateType: Aggregate>(
        &self,
    ) -> Result<ReplayStream, PersistenceError> {
        self.repository.stream_all_events::<AggregateType>().await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    //! Real PostgreSQL coverage for the shared persistence contract.

    use async_trait::async_trait;
    use cqrs_es::mem_store::MemStore;
    use cqrs_es::persist::{
        PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent,
        SerializedSnapshot,
    };
    use cqrs_es::{CqrsFramework, EventStore};
    use futures_util::{TryStreamExt, stream};
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use sqlx::PgPool;
    use sqlx::migrate::MigrateError;
    use sqlx::postgres::PgPoolOptions;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    use event_sorcery::{
        Column, CompactionPolicy, Cons, Dependent, DomainEvent, Effect, EntityList, EventSourced,
        Never, Nil, OneOf, PersistenceBackend, ProjectedStoreBackend, Projection, Reactor,
        ReactorContext, ReconcileError, Store, StoreBuilder, Table,
    };

    use crate::view_repository::tests::{RegisteredSchemaEvent, RegisteredSchemaView};
    use crate::{MIGRATOR, PostgresBackend, PostgresViewBackend};

    async fn command_events(aggregate_id: &str, versions: &[u64]) -> Vec<SerializedEvent> {
        let memory = MemStore::<RegisteredSchemaView>::default();
        let framework = CqrsFramework::new(memory.clone(), vec![], ());
        for version in versions {
            framework
                .execute(
                    aggregate_id,
                    RegisteredSchemaEvent::VersionUpdated {
                        name: aggregate_id.to_string(),
                        version: *version,
                    },
                )
                .await
                .unwrap();
        }
        memory
            .load_events(aggregate_id)
            .await
            .unwrap()
            .iter()
            .map(SerializedEvent::try_from)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    async fn replay_events(replay: ReplayStream) -> Vec<SerializedEvent> {
        stream::unfold(replay, |mut replay| async move {
            replay
                .next::<RegisteredSchemaView>(&[])
                .await
                .map(|result| {
                    let event = result.and_then(|event| SerializedEvent::try_from(&event));
                    (event, replay)
                })
        })
        .try_collect()
        .await
        .unwrap()
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    /// Command-driven entity shared by backend and recovery protocol tests.
    pub(crate) struct Note<const VERSION: u64, const COMPACT: bool = false> {
        text: String,
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    /// Event emitted through the shared note fixture's commands.
    pub(crate) enum NoteEvent {
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
    impl<const VERSION: u64, const COMPACT: bool> EventSourced for Note<VERSION, COMPACT> {
        type Id = String;
        type Command = String;
        type Event = NoteEvent;
        type Error = Never;
        type Materialized = Nil;
        type Jobs = Nil;

        const AGGREGATE_TYPE: &'static str = "NativeNote";
        const SCHEMA_VERSION: u64 = VERSION;
        const SNAPSHOT_SIZE: usize = 2;
        const COMPACTION_POLICY: CompactionPolicy = if COMPACT {
            CompactionPolicy::RetainLast { events: 1 }
        } else {
            CompactionPolicy::Retain
        };
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

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct ProjectedNote<const VERSION: u64, const COMPACT: bool = false> {
        text: String,
        history: Vec<String>,
    }

    #[async_trait]
    impl<const VERSION: u64, const COMPACT: bool> EventSourced for ProjectedNote<VERSION, COMPACT> {
        type Id = String;
        type Command = String;
        type Event = NoteEvent;
        type Error = Never;
        type Materialized = Table;
        type Jobs = Nil;

        const AGGREGATE_TYPE: &'static str = "ProjectedNote";
        const SCHEMA_VERSION: u64 = VERSION;
        const SNAPSHOT_SIZE: usize = 2;
        const COMPACTION_POLICY: CompactionPolicy = if COMPACT {
            CompactionPolicy::CompactAfterSnapshot
        } else {
            CompactionPolicy::Retain
        };
        const PROJECTION: Table = Table("note_projection");

        fn originate(event: &Self::Event) -> Option<Self> {
            let NoteEvent::Written { text } = event;
            Some(Self {
                text: text.clone(),
                history: vec![text.clone()],
            })
        }

        fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Never> {
            let NoteEvent::Written { text } = event;
            Ok(Some(Self {
                text: text.clone(),
                history: entity
                    .history
                    .iter()
                    .cloned()
                    .chain(std::iter::once(text.clone()))
                    .collect(),
            }))
        }

        async fn initialize(text: String) -> Result<Effect<Self>, Never> {
            Ok(Effect::Events(vec![NoteEvent::Written { text }]))
        }

        async fn transition(&self, text: String) -> Result<Effect<Self>, Never> {
            Self::initialize(text).await
        }
    }

    async fn projected_pool(pool: PgPool) -> PgPool {
        MIGRATOR.run(&pool).await.unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(pool.connect_options().as_ref().clone())
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE note_projection (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSONB NOT NULL,
                text_value TEXT GENERATED ALWAYS AS (payload->'Live'->>'text') STORED
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    #[sqlx::test(migrations = false)]
    async fn direct_projected_startup_refuses_a_compactable_entity(pool: PgPool) {
        let pool = projected_pool(pool).await;
        let backend = PostgresBackend { pool: pool.clone() };

        let result = backend
            .reconcile_projected::<ProjectedNote<1, true>>()
            .await;

        assert!(matches!(
            result,
            Err(ReconcileError::ProjectionRequiresRetainedHistory { .. })
        ));
        let registry: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'SchemaRegistry'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(registry, 0);
    }

    #[sqlx::test(migrations = false)]
    async fn projected_builder_queries_and_recovers_with_one_connection(pool: PgPool) {
        let pool = projected_pool(pool).await;
        let backend = PostgresBackend { pool: pool.clone() };
        let (store, projection) = timeout(
            Duration::from_secs(10),
            StoreBuilder::<ProjectedNote<1>, PostgresBackend>::with_backend(backend.clone())
                .build(),
        )
        .await
        .expect("projected startup must use its one connection")
        .unwrap();
        let id = "projected".to_string();
        store.send(&id, "first".to_string()).await.unwrap();
        store.send(&id, "second".to_string()).await.unwrap();
        assert_eq!(
            projection.load(&id).await.unwrap().unwrap().history,
            vec!["first", "second"]
        );
        assert_eq!(projection.load_all().await.unwrap().len(), 1);
        assert_eq!(
            projection
                .filter(Column("text_value"), &"second".to_string())
                .await
                .unwrap()
                .len(),
            1
        );
        drop((store, projection));
        sqlx::query("DELETE FROM note_projection")
            .execute(&pool)
            .await
            .unwrap();
        let (store, projection) = timeout(
            Duration::from_secs(10),
            StoreBuilder::<ProjectedNote<1>, PostgresBackend>::with_backend(backend).build(),
        )
        .await
        .expect("projected restart must not acquire a second connection")
        .unwrap();
        assert_eq!(
            projection.load(&id).await.unwrap().unwrap().history,
            vec!["first", "second"]
        );
        projection.rebuild(&id).await.unwrap();
        projection.rebuild_all().await.unwrap();
        assert_eq!(
            projection.load(&id).await.unwrap().unwrap().history,
            vec!["first", "second"]
        );
        assert_eq!(
            store.load(&id).await.unwrap().unwrap().history,
            vec!["first", "second"]
        );
    }

    struct PausedDelivery {
        armed: AtomicBool,
        persisted: Notify,
        resume: Notify,
    }

    impl Dependent for PausedDelivery {
        type Dependencies = Cons<ProjectedNote<1>, Nil>;
    }

    #[async_trait]
    impl Reactor for PausedDelivery {
        type Error = Never;

        async fn react(
            &self,
            event: <Self::Dependencies as EntityList>::Event,
        ) -> Result<(), Never> {
            let (_, NoteEvent::Written { text }) = event.into_inner();
            if self.armed.load(Ordering::SeqCst) && text == "second" {
                self.persisted.notify_one();
                self.resume.notified().await;
            }
            Ok(())
        }
    }

    #[sqlx::test(migrations = false)]
    async fn recovered_event_is_not_applied_again_by_its_pending_reactor_delivery(pool: PgPool) {
        let pool = projected_pool(pool).await;
        let backend = PostgresBackend { pool: pool.clone() };
        let gate = Arc::new(PausedDelivery {
            armed: AtomicBool::new(false),
            persisted: Notify::new(),
            resume: Notify::new(),
        });
        let (store, projection) =
            StoreBuilder::<ProjectedNote<1>, PostgresBackend>::with_backend(backend.clone())
                .with(gate.clone())
                .build()
                .await
                .unwrap();
        let id = "projected".to_string();
        store.send(&id, "first".to_string()).await.unwrap();
        gate.armed.store(true, Ordering::SeqCst);
        let writing_id = id.clone();
        let writing = tokio::spawn(async move {
            store.send(&writing_id, "second".to_string()).await.unwrap();
        });
        timeout(Duration::from_secs(10), gate.persisted.notified())
            .await
            .unwrap();
        let (_, recovered) = timeout(
            Duration::from_secs(10),
            StoreBuilder::<ProjectedNote<1>, PostgresBackend>::with_backend(backend).build(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            recovered.load(&id).await.unwrap().unwrap().history,
            vec!["first", "second"]
        );
        gate.resume.notify_one();
        timeout(Duration::from_secs(10), writing)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            projection.load(&id).await.unwrap().unwrap().history,
            vec!["first", "second"]
        );
        let version: i64 = sqlx::query_scalar("SELECT version FROM note_projection")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, 2);
    }

    struct PendingProjection {
        store: Arc<Store<ProjectedNote<1>, PostgresBackend>>,
        projection: Arc<Projection<ProjectedNote<1>, PostgresViewBackend>>,
        gate: Arc<PausedDelivery>,
        writing: tokio::task::JoinHandle<()>,
        event: NoteEvent,
        context: ReactorContext,
    }

    async fn pending_projection(pool: &PgPool) -> PendingProjection {
        let gate = Arc::new(PausedDelivery {
            armed: AtomicBool::new(false),
            persisted: Notify::new(),
            resume: Notify::new(),
        });
        let (store, projection) =
            StoreBuilder::<ProjectedNote<1>, PostgresBackend>::with_backend(PostgresBackend {
                pool: pool.clone(),
            })
            .with(gate.clone())
            .build()
            .await
            .unwrap();
        store
            .send(&"projected".to_string(), "first".to_string())
            .await
            .unwrap();
        gate.armed.store(true, Ordering::SeqCst);
        let writing_store = store.clone();
        let writing = tokio::spawn(async move {
            writing_store
                .send(&"projected".to_string(), "second".to_string())
                .await
                .unwrap();
        });
        timeout(Duration::from_secs(10), gate.persisted.notified())
            .await
            .unwrap();
        let (sequence, payload): (i64, serde_json::Value) = sqlx::query_as(
            "SELECT sequence, payload FROM events
             WHERE aggregate_type = 'ProjectedNote' AND aggregate_id = 'projected'
             ORDER BY sequence DESC LIMIT 1",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        PendingProjection {
            store,
            projection,
            gate,
            writing,
            event: serde_json::from_value(payload).unwrap(),
            context: ReactorContext {
                sequence: Some(usize::try_from(sequence).unwrap().try_into().unwrap()),
            },
        }
    }

    async fn wait_for_projection_locks(pool: &PgPool, mode: &str, expected: i64) {
        timeout(Duration::from_secs(10), async {
            loop {
                let waiting: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM pg_locks
                     WHERE relation = 'note_projection'::regclass AND NOT granted AND mode = $1
                       AND database = (
                           SELECT oid FROM pg_database WHERE datname = current_database()
                       )",
                )
                .bind(mode)
                .fetch_one(pool)
                .await
                .unwrap();

                if waiting == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[sqlx::test(migrations = false)]
    async fn overlapping_duplicate_deliveries_apply_the_persisted_event_once(pool: PgPool) {
        let control = projected_pool(pool).await;
        let pool = PgPoolOptions::new()
            .max_connections(3)
            .connect_with(control.connect_options().as_ref().clone())
            .await
            .unwrap();
        let pending = pending_projection(&pool).await;
        let mut blocker = control.begin().await.unwrap();
        sqlx::query("LOCK TABLE note_projection IN SHARE MODE")
            .execute(&mut *blocker)
            .await
            .unwrap();
        let deliveries: Vec<_> = (0..2)
            .map(|_| {
                let projection = pending.projection.clone();
                let event = pending.event.clone();
                let context = pending.context;
                tokio::spawn(async move {
                    projection
                        .react_with_context(OneOf::Here(("projected".to_string(), event)), context)
                        .await
                        .unwrap();
                })
            })
            .collect();
        wait_for_projection_locks(&pool, "RowExclusiveLock", 2).await;
        blocker.commit().await.unwrap();
        for delivery in deliveries {
            timeout(Duration::from_secs(10), delivery)
                .await
                .unwrap()
                .unwrap();
        }
        pending.gate.resume.notify_one();
        timeout(Duration::from_secs(10), pending.writing)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            pending
                .projection
                .load(&"projected".to_string())
                .await
                .unwrap()
                .unwrap()
                .history,
            vec!["first", "second"]
        );
        let version: i64 = sqlx::query_scalar("SELECT version FROM note_projection")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, 2);
    }

    #[sqlx::test(migrations = false)]
    async fn recovery_advancing_a_loaded_checkpoint_fences_the_retrying_delivery(pool: PgPool) {
        let control = projected_pool(pool).await;
        let pool = PgPoolOptions::new()
            .max_connections(3)
            .connect_with(control.connect_options().as_ref().clone())
            .await
            .unwrap();
        let pending = pending_projection(&pool).await;
        let mut blocker = control.begin().await.unwrap();
        sqlx::query("LOCK TABLE note_projection IN SHARE MODE")
            .execute(&mut *blocker)
            .await
            .unwrap();
        let projection = pending.projection.clone();
        let recovery = tokio::spawn(async move {
            projection.catch_up().await.unwrap();
        });
        wait_for_projection_locks(&pool, "ShareRowExclusiveLock", 1).await;
        let projection = pending.projection.clone();
        let event = pending.event.clone();
        let context = pending.context;
        let delivery = tokio::spawn(async move {
            projection
                .react_with_context(OneOf::Here(("projected".to_string(), event)), context)
                .await
                .unwrap();
        });
        wait_for_projection_locks(&pool, "RowExclusiveLock", 1).await;
        blocker.commit().await.unwrap();
        timeout(Duration::from_secs(10), recovery)
            .await
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(10), delivery)
            .await
            .unwrap()
            .unwrap();
        pending.gate.resume.notify_one();
        timeout(Duration::from_secs(10), pending.writing)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            pending
                .projection
                .load(&"projected".to_string())
                .await
                .unwrap()
                .unwrap()
                .history,
            vec!["first", "second"]
        );
        let version: i64 = sqlx::query_scalar("SELECT version FROM note_projection")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, 2);
    }

    #[sqlx::test(migrations = false)]
    async fn exhausted_gap_delivery_preserves_coverage_until_catch_up(pool: PgPool) {
        let pool = projected_pool(pool).await;
        let pending = pending_projection(&pool).await;
        pending
            .store
            .send(&"projected".to_string(), "third".to_string())
            .await
            .unwrap();
        assert_eq!(
            pending
                .projection
                .load(&"projected".to_string())
                .await
                .unwrap()
                .unwrap()
                .history,
            vec!["first"]
        );
        let version: i64 = sqlx::query_scalar("SELECT version FROM note_projection")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, 1);
        pending.gate.resume.notify_one();
        timeout(Duration::from_secs(10), pending.writing)
            .await
            .unwrap()
            .unwrap();
        pending.projection.catch_up().await.unwrap();
        assert_eq!(
            pending
                .projection
                .load(&"projected".to_string())
                .await
                .unwrap()
                .unwrap()
                .history,
            vec!["first", "second", "third"]
        );
        let version: i64 = sqlx::query_scalar("SELECT version FROM note_projection")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, 3);
    }

    async fn seed_projected(pool: &PgPool) {
        let (store, _) =
            StoreBuilder::<ProjectedNote<1>, PostgresBackend>::with_backend(PostgresBackend {
                pool: pool.clone(),
            })
            .build()
            .await
            .unwrap();
        let id = "projected".to_string();
        store.send(&id, "first".to_string()).await.unwrap();
        store.send(&id, "second".to_string()).await.unwrap();
    }

    #[derive(Debug, PartialEq)]
    struct ProjectedRecoveryState {
        view: (i64, serde_json::Value),
        snapshot: (i64, i64, serde_json::Value),
        registry: Vec<serde_json::Value>,
    }

    async fn projected_state(pool: &PgPool) -> ProjectedRecoveryState {
        let view = sqlx::query_as(
            "SELECT version, payload FROM note_projection WHERE view_id = 'projected'",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        let snapshot = sqlx::query_as(
            "SELECT last_sequence, snapshot_version, payload FROM snapshots
             WHERE aggregate_type = 'ProjectedNote' AND aggregate_id = 'projected'",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        let registry = sqlx::query_scalar(
            "SELECT payload FROM events WHERE aggregate_type = 'SchemaRegistry' ORDER BY sequence",
        )
        .fetch_all(pool)
        .await
        .unwrap();
        ProjectedRecoveryState {
            view,
            snapshot,
            registry,
        }
    }

    #[sqlx::test(migrations = false)]
    async fn projected_schema_change_rebuilds_current_incompatible_state(pool: PgPool) {
        let pool = projected_pool(pool).await;
        seed_projected(&pool).await;
        sqlx::query("UPDATE note_projection SET payload = '{\"old_format\":true}'::jsonb")
            .execute(&pool)
            .await
            .unwrap();
        let before = projected_state(&pool).await;
        assert_eq!(before.view.0, 2);
        let (_, projection) =
            StoreBuilder::<ProjectedNote<2>, PostgresBackend>::with_backend(PostgresBackend {
                pool: pool.clone(),
            })
            .build()
            .await
            .unwrap();
        let id = "projected".to_string();
        assert_eq!(
            projection.load(&id).await.unwrap().unwrap().history,
            vec!["first", "second"]
        );
        let checkpoint: i64 = sqlx::query_scalar("SELECT version FROM note_projection")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(checkpoint, 2);
        let snapshots: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM snapshots")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(snapshots, 0);
        let registry: Vec<serde_json::Value> = sqlx::query_scalar(
            "SELECT payload FROM events WHERE aggregate_type = 'SchemaRegistry' ORDER BY sequence",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            registry,
            vec![
                json!({"VersionUpdated": {"name": "ProjectedNote", "version": 1}}),
                json!({"VersionUpdated": {"name": "ProjectedNote", "version": 2}}),
            ]
        );
    }

    #[sqlx::test(migrations = false)]
    async fn projected_write_failure_rolls_back_snapshot_view_and_registry(pool: PgPool) {
        let pool = projected_pool(pool).await;
        seed_projected(&pool).await;
        let before = projected_state(&pool).await;
        sqlx::query(
            "ALTER TABLE note_projection ADD CONSTRAINT reject_rebuild CHECK (FALSE) NOT VALID",
        )
        .execute(&pool)
        .await
        .unwrap();
        let result =
            StoreBuilder::<ProjectedNote<2>, PostgresBackend>::with_backend(PostgresBackend {
                pool: pool.clone(),
            })
            .build()
            .await;
        let Err(ReconcileError::Sqlx(sqlx::Error::Database(source))) = result else {
            panic!("recovery write rejection must preserve its SQL failure");
        };
        assert_eq!(source.code().as_deref(), Some("23514"));
        assert_eq!(projected_state(&pool).await, before);
        sqlx::query("ALTER TABLE note_projection DROP CONSTRAINT reject_rebuild")
            .execute(&pool)
            .await
            .unwrap();
        let (_, projection) =
            StoreBuilder::<ProjectedNote<2>, PostgresBackend>::with_backend(PostgresBackend {
                pool,
            })
            .build()
            .await
            .unwrap();
        assert_eq!(
            projection
                .load(&"projected".to_string())
                .await
                .unwrap()
                .unwrap()
                .history,
            vec!["first", "second"]
        );
    }

    #[sqlx::test(migrations = false)]
    async fn registry_failure_after_projected_replay_rolls_back_every_derived_write(pool: PgPool) {
        let pool = projected_pool(pool).await;
        seed_projected(&pool).await;
        sqlx::query("UPDATE note_projection SET payload = '{\"old_format\":true}'::jsonb")
            .execute(&pool)
            .await
            .unwrap();
        let before = projected_state(&pool).await;
        sqlx::query(
            "ALTER TABLE events ADD CONSTRAINT reject_projected_registration CHECK (
                aggregate_type <> 'SchemaRegistry' OR sequence = 1
             )",
        )
        .execute(&pool)
        .await
        .unwrap();
        let result =
            StoreBuilder::<ProjectedNote<2>, PostgresBackend>::with_backend(PostgresBackend {
                pool: pool.clone(),
            })
            .build()
            .await;
        let Err(error) = result else {
            panic!("registry rejection must prevent projected startup");
        };
        assert!(matches!(error, ReconcileError::Aggregate(_)));
        assert_eq!(projected_state(&pool).await, before);
        sqlx::query("ALTER TABLE events DROP CONSTRAINT reject_projected_registration")
            .execute(&pool)
            .await
            .unwrap();
        let (_, projection) =
            StoreBuilder::<ProjectedNote<2>, PostgresBackend>::with_backend(PostgresBackend {
                pool,
            })
            .build()
            .await
            .unwrap();
        assert_eq!(
            projection
                .load(&"projected".to_string())
                .await
                .unwrap()
                .unwrap()
                .history,
            vec!["first", "second"]
        );
    }

    #[tokio::test]
    async fn unprojected_builder_preserves_unavailable_storage_error() {
        let pool = PgPool::connect_lazy("postgresql://localhost/unused").unwrap();
        pool.close().await;

        let result =
            StoreBuilder::<Note<1>, PostgresBackend>::with_backend(PostgresBackend { pool })
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
        let after: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM snapshots WHERE aggregate_type = $1")
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
    async fn retained_startup_refuses_a_snapshot_with_missing_covered_history(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(pool.connect_options().as_ref().clone())
            .await
            .unwrap();
        let backend = PostgresBackend { pool: pool.clone() };
        let store = StoreBuilder::<Note<1, true>, PostgresBackend>::with_backend(backend.clone())
            .build()
            .await
            .unwrap();
        let id = "compacted-note".to_string();
        store.send(&id, "first".to_string()).await.unwrap();
        store.send(&id, "second".to_string()).await.unwrap();
        drop(store);
        let snapshot_sql = "SELECT last_sequence, snapshot_version, payload FROM snapshots \
                            WHERE aggregate_type = 'NativeNote' AND aggregate_id = 'compacted-note'";
        let before: (i64, i64, serde_json::Value) =
            sqlx::query_as(snapshot_sql).fetch_one(&pool).await.unwrap();
        assert_eq!((before.0, before.1), (2, 1));
        sqlx::query("DELETE FROM events WHERE aggregate_type = 'NativeNote' AND sequence = 1")
            .execute(&pool)
            .await
            .unwrap();

        let result = timeout(
            Duration::from_secs(10),
            StoreBuilder::<Note<2>, PostgresBackend>::with_backend(backend).build(),
        )
        .await
        .expect("history validation must use the startup transaction's connection");
        let Err(error) = result else {
            panic!("incomplete history must not allow retained-schema startup");
        };
        assert!(matches!(
            error,
            ReconcileError::UnprovenSnapshotHistory {
                covered_sequence: 2,
                retained_events: 1,
                ..
            }
        ));
        let after: (i64, i64, serde_json::Value) =
            sqlx::query_as(snapshot_sql).fetch_one(&pool).await.unwrap();
        assert_eq!(after, before);
        let registered: serde_json::Value = sqlx::query_scalar(
            "SELECT payload FROM events WHERE aggregate_type = 'SchemaRegistry' \
             ORDER BY sequence DESC LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            registered,
            json!({"VersionUpdated": {"name": "NativeNote", "version": 1}})
        );
        let remaining: Vec<i64> = sqlx::query_scalar(
            "SELECT sequence FROM events WHERE aggregate_type = 'NativeNote' ORDER BY sequence",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(remaining, [2]);
    }

    #[sqlx::test(migrations = false)]
    async fn failed_registry_registration_preserves_snapshot_and_allows_upgrade_retry(
        pool: PgPool,
    ) {
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
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(pool.connect_options().as_ref().clone())
            .await
            .unwrap();
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

    #[tokio::test]
    async fn native_snapshot_reads_preserve_closed_pool_errors() {
        let pool = PgPool::connect_lazy("postgresql://localhost/unused").unwrap();
        let repository =
            PostgresBackend { pool: pool.clone() }.persistence_repo(CompactionPolicy::Retain);
        pool.close().await;

        let error = repository
            .get_snapshot::<RegisteredSchemaView>("alpha")
            .await
            .unwrap_err();
        let PersistenceError::ConnectionError(source) = error else {
            panic!("expected the snapshot storage error, got {error:?}");
        };
        assert!(matches!(
            source.downcast_ref::<sqlx::Error>().unwrap(),
            sqlx::Error::PoolClosed,
        ));
    }

    #[sqlx::test(migrations = false)]
    async fn native_repository_round_trips_legacy_snapshots_and_every_read_path(pool: PgPool) {
        let backend = PostgresBackend { pool };
        backend.bootstrap_persistence().await.unwrap();
        let repository = backend.persistence_repo(CompactionPolicy::Retain);
        let alpha = command_events("alpha", &[1, 2]).await;
        let beta = command_events("beta", &[1]).await;

        repository
            .persist::<RegisteredSchemaView>(
                &alpha,
                Some((
                    "alpha".to_string(),
                    json!({"name": "alpha", "schema_version": 2}),
                    1,
                )),
            )
            .await
            .unwrap();
        repository
            .persist::<RegisteredSchemaView>(&beta, None)
            .await
            .unwrap();

        assert_eq!(
            repository
                .get_events::<RegisteredSchemaView>("alpha")
                .await
                .unwrap(),
            alpha
        );
        assert_eq!(
            repository
                .get_events::<RegisteredSchemaView>("beta")
                .await
                .unwrap(),
            beta
        );
        assert_eq!(
            repository
                .get_last_events::<RegisteredSchemaView>("alpha", 1)
                .await
                .unwrap(),
            alpha[1..],
        );
        assert_eq!(
            repository
                .get_last_events::<RegisteredSchemaView>("beta", 1)
                .await
                .unwrap(),
            [],
        );
        assert_eq!(
            repository
                .get_snapshot::<RegisteredSchemaView>("alpha")
                .await
                .unwrap(),
            Some(SerializedSnapshot {
                aggregate_id: "alpha".to_string(),
                aggregate: json!({"name": "alpha", "schema_version": 2}),
                current_sequence: 2,
                current_snapshot: 1,
            }),
        );
        assert_eq!(
            repository
                .get_snapshot::<RegisteredSchemaView>("beta")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            replay_events(
                repository
                    .stream_events::<RegisteredSchemaView>("alpha")
                    .await
                    .unwrap()
            )
            .await,
            alpha,
        );
        assert_eq!(
            replay_events(
                repository
                    .stream_events::<RegisteredSchemaView>("beta")
                    .await
                    .unwrap()
            )
            .await,
            beta,
        );
        let replayed = replay_events(
            repository
                .stream_all_events::<RegisteredSchemaView>()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(replayed.len(), 3);
        let replayed: BTreeMap<_, _> = replayed
            .into_iter()
            .map(|event| ((event.aggregate_id.clone(), event.sequence), event))
            .collect();
        let expected: BTreeMap<_, _> = alpha
            .into_iter()
            .chain(beta)
            .map(|event| ((event.aggregate_id.clone(), event.sequence), event))
            .collect();
        assert_eq!(replayed, expected);
    }

    #[sqlx::test(migrations = false)]
    async fn native_covered_snapshot_keeps_its_prefix_and_replays_the_tail(pool: PgPool) {
        let backend = PostgresBackend { pool };
        backend.bootstrap_persistence().await.unwrap();
        let repository = backend.persistence_repo(CompactionPolicy::Retain);
        let events = command_events("covered", &[1, 2, 3]).await;

        repository
            .persist_with_snapshot::<RegisteredSchemaView>(
                &events,
                Some(SerializedSnapshot {
                    aggregate_id: "covered".to_string(),
                    aggregate: json!({"name": "covered", "schema_version": 2}),
                    current_sequence: events[1].sequence,
                    current_snapshot: 1,
                }),
            )
            .await
            .unwrap();

        assert_eq!(
            repository
                .get_events::<RegisteredSchemaView>("covered")
                .await
                .unwrap(),
            events,
        );
        assert_eq!(
            repository
                .get_snapshot::<RegisteredSchemaView>("covered")
                .await
                .unwrap(),
            Some(SerializedSnapshot {
                aggregate_id: "covered".to_string(),
                aggregate: json!({"name": "covered", "schema_version": 2}),
                current_sequence: 2,
                current_snapshot: 1,
            }),
        );
        assert_eq!(
            repository
                .get_last_events::<RegisteredSchemaView>("covered", 2)
                .await
                .unwrap(),
            events[2..],
        );
    }
}
