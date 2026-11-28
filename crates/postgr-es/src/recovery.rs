//! Transaction-scoped schema startup using the existing registry authority.

use cqrs_es::Aggregate;
use cqrs_es::persist::{
    PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot,
};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use std::marker::PhantomData;
use tokio::sync::Mutex;

use event_sorcery::{
    CommitRequest, EngineError, EventSourced, ReconcileError, SCHEMA_REGISTRY_ID,
    SchemaReconciliation, SchemaRegistry, StreamIdentity, reconcile_schema, register_schema,
    validate_expected_version, validate_snapshot_history,
};

use crate::PostgresEventRepository;
use crate::projection_recovery::snapshot_histories;

pub(crate) struct PostgresRecovery<Entity: EventSourced> {
    transaction: Transaction<'static, Postgres>,
    repository: PostgresEventRepository,
    _entity: PhantomData<fn() -> Entity>,
}

impl<Entity: EventSourced> PostgresRecovery<Entity> {
    pub(crate) async fn begin(
        pool: &PgPool,
    ) -> Result<(Self, SchemaReconciliation), ReconcileError> {
        let mut transaction = pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *transaction)
            .await?;
        // All entity registrations append to one registry stream. Transaction
        // locks release on rollback, including cancellation, without pool leakage.
        sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
            .bind(i32::from_be_bytes(*b"ESRC"))
            .bind(i32::from_be_bytes(*b"SCMA"))
            .execute(&mut *transaction)
            .await?;
        let repository = PostgresEventRepository { pool: pool.clone() };
        let events = repository
            .official()
            .get_serialized_events_in(
                &mut transaction,
                SchemaRegistry::AGGREGATE_TYPE,
                SCHEMA_REGISTRY_ID,
            )
            .await?;
        let payloads = events
            .into_iter()
            .map(|event| serde_json::to_string(&event.payload))
            .collect::<Result<Vec<_>, _>>()?;
        let reconciliation = reconcile_schema::<Entity>(payloads)?;

        if reconciliation.clears_snapshots(Entity::COMPACTION_POLICY) {
            for history in
                snapshot_histories(&mut transaction, Entity::AGGREGATE_TYPE, None).await?
            {
                validate_snapshot_history(
                    StreamIdentity::new(Entity::AGGREGATE_TYPE, history.aggregate_id),
                    history.covered_sequence,
                    history.retained_events,
                )?;
            }
            sqlx::query("DELETE FROM snapshots WHERE aggregate_type = $1")
                .bind(Entity::AGGREGATE_TYPE)
                .execute(&mut *transaction)
                .await?;
        }

        Ok((
            Self {
                transaction,
                repository,
                _entity: PhantomData,
            },
            reconciliation,
        ))
    }

    pub(crate) async fn recover_projection(
        &mut self,
        mode: &event_sorcery::ProjectionRecoveryMode,
    ) -> Result<(), event_sorcery::ProjectionError<Entity>>
    where
        Entity: EventSourced<Materialized = event_sorcery::Table>,
    {
        let mut storage = crate::projection_recovery::PostgresProjectionRecovery {
            connection: &mut self.transaction,
        };
        event_sorcery::recover_projection::<Entity>(&mut storage, mode).await
    }

    pub(crate) async fn finish(mut self) -> Result<(), ReconcileError> {
        {
            let repository = RegistryRepository {
                transaction: Mutex::new(&mut self.transaction),
                repository: &self.repository,
            };
            register_schema::<Entity, _>(repository).await?;
        }
        self.transaction.commit().await?;
        Ok(())
    }
}

struct RegistryRepository<'connection> {
    transaction: Mutex<&'connection mut Transaction<'static, Postgres>>,
    repository: &'connection PostgresEventRepository,
}

#[derive(Debug, thiserror::Error)]
enum RegistryStorageModeError {
    #[error("schema registry storage does not use snapshots")]
    Snapshot,
    #[error("schema registry startup uses transaction-bound event reads, not streaming")]
    Streaming,
}

impl PersistedEventRepository for RegistryRepository<'_> {
    async fn get_events<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let mut transaction = self.transaction.lock().await;
        self.repository
            .official()
            .get_events_in::<AggregateType>(&mut transaction, aggregate_id)
            .await
    }

    async fn get_last_events<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let mut transaction = self.transaction.lock().await;
        self.repository
            .official()
            .get_last_events_in::<AggregateType>(&mut transaction, aggregate_id, last_sequence)
            .await
    }

    async fn get_snapshot<AggregateType: Aggregate>(
        &self,
        _aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        Err(PersistenceError::UnknownError(Box::new(
            RegistryStorageModeError::Snapshot,
        )))
    }

    async fn persist<AggregateType: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        if snapshot_update.is_some() {
            return Err(PersistenceError::UnknownError(Box::new(
                RegistryStorageModeError::Snapshot,
            )));
        }
        let Some(first) = events.first() else {
            return Ok(());
        };
        let expected = first
            .sequence
            .checked_sub(1)
            .ok_or(EngineError::OptimisticLock)?;
        let request = CommitRequest::new(
            StreamIdentity::new(AggregateType::TYPE, &first.aggregate_id),
            events,
        )
        .at_expected_version(expected);
        let _ = request.prepare()?;

        let mut transaction = self.transaction.lock().await;
        let current: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(sequence), 0) FROM events \
             WHERE aggregate_type = $1 AND aggregate_id = $2",
        )
        .bind(AggregateType::TYPE)
        .bind(&first.aggregate_id)
        .fetch_one(&mut ***transaction)
        .await
        .map_err(EngineError::from)?;
        validate_expected_version(
            expected,
            usize::try_from(current).map_err(EngineError::from)?,
        )?;
        self.repository
            .official()
            .append_serialized_events_in(&mut transaction, AggregateType::TYPE, events)
            .await
            .map_err(PersistenceError::from)?;
        drop(transaction);
        Ok(())
    }

    async fn stream_events<AggregateType: Aggregate>(
        &self,
        _aggregate_id: &str,
    ) -> Result<ReplayStream, PersistenceError> {
        Err(PersistenceError::UnknownError(Box::new(
            RegistryStorageModeError::Streaming,
        )))
    }

    async fn stream_all_events<AggregateType: Aggregate>(
        &self,
    ) -> Result<ReplayStream, PersistenceError> {
        Err(PersistenceError::UnknownError(Box::new(
            RegistryStorageModeError::Streaming,
        )))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sqlx::postgres::PgPoolOptions;
    use std::future::pending;
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use event_sorcery::StoreBuilder;

    use super::*;
    use crate::backend::tests::Note;
    use crate::view_repository::tests::RegisteredSchemaView;
    use crate::{MIGRATOR, PostgresBackend};

    async fn seed_snapshot(pool: &PgPool) {
        let store = StoreBuilder::<Note<1>, PostgresBackend>::with_backend(PostgresBackend {
            pool: pool.clone(),
        })
        .build()
        .await
        .unwrap();
        let id = "recovery-note".to_string();
        store.send(&id, "first".to_string()).await.unwrap();
        store.send(&id, "second".to_string()).await.unwrap();
    }

    #[sqlx::test(migrations = false)]
    async fn dropping_recovery_restores_snapshot_and_releases_its_only_connection(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(pool.connect_options().as_ref().clone())
            .await
            .unwrap();
        seed_snapshot(&pool).await;
        let snapshot_sql = "SELECT last_sequence, snapshot_version, payload FROM snapshots \
                            WHERE aggregate_type = 'NativeNote' AND aggregate_id = 'recovery-note'";
        let before: (i64, i64, Value) =
            sqlx::query_as(snapshot_sql).fetch_one(&pool).await.unwrap();
        let (recovery, decision) = PostgresRecovery::<Note<2>>::begin(&pool).await.unwrap();
        assert_eq!(decision, SchemaReconciliation::Changed);
        drop(recovery);
        let after: (i64, i64, Value) = timeout(
            Duration::from_secs(10),
            sqlx::query_as(snapshot_sql).fetch_one(&pool),
        )
        .await
        .expect("dropping recovery must release the connection after rollback")
        .unwrap();
        assert_eq!(after, before);
        let recorded: Vec<Value> = sqlx::query_scalar(
            "SELECT payload FROM events WHERE aggregate_type = 'SchemaRegistry' ORDER BY sequence",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            recorded,
            vec![json!({"VersionUpdated": {"name": "NativeNote", "version": 1}})]
        );
        let (retry, decision) = timeout(
            Duration::from_secs(10),
            PostgresRecovery::<Note<2>>::begin(&pool),
        )
        .await
        .expect("rollback must release the schema lock")
        .unwrap();
        assert_eq!(decision, SchemaReconciliation::Changed);
        retry.finish().await.unwrap();
        let snapshots: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM snapshots")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(snapshots, 0);
    }

    #[sqlx::test(migrations = false)]
    async fn cancelling_recovery_preserves_history_and_allows_a_clean_retry(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(pool.connect_options().as_ref().clone())
            .await
            .unwrap();
        seed_snapshot(&pool).await;
        let (started, waiting) = oneshot::channel();
        let recovering_pool = pool.clone();
        let recovering = tokio::spawn(async move {
            let (recovery, decision) = PostgresRecovery::<Note<2>>::begin(&recovering_pool)
                .await
                .unwrap();
            assert_eq!(decision, SchemaReconciliation::Changed);
            started.send(()).unwrap();
            pending::<()>().await;
            drop(recovery);
        });
        timeout(Duration::from_secs(10), waiting)
            .await
            .unwrap()
            .unwrap();
        recovering.abort();
        assert!(recovering.await.unwrap_err().is_cancelled());
        let snapshots: i64 = timeout(
            Duration::from_secs(10),
            sqlx::query_scalar("SELECT COUNT(*) FROM snapshots").fetch_one(&pool),
        )
        .await
        .expect("cancelled recovery must roll back before pool reuse")
        .unwrap();
        assert_eq!(snapshots, 1);
        let events: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE aggregate_type = 'NativeNote'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(events, 2);
        let (retry, decision) = timeout(
            Duration::from_secs(10),
            PostgresRecovery::<Note<2>>::begin(&pool),
        )
        .await
        .expect("cancellation must release the registry lock")
        .unwrap();
        assert_eq!(decision, SchemaReconciliation::Changed);
        retry.finish().await.unwrap();
        let recorded: Vec<Value> = sqlx::query_scalar(
            "SELECT payload FROM events WHERE aggregate_type = 'SchemaRegistry' ORDER BY sequence",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            recorded,
            vec![
                json!({"VersionUpdated": {"name": "NativeNote", "version": 1}}),
                json!({"VersionUpdated": {"name": "NativeNote", "version": 2}}),
            ]
        );
    }

    #[sqlx::test(migrations = false)]
    async fn competing_startups_observe_the_committed_registry_version_once(pool: PgPool) {
        MIGRATOR.run(&pool).await.unwrap();
        seed_snapshot(&pool).await;
        let winner_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(pool.connect_options().as_ref().clone())
            .await
            .unwrap();
        let loser_pool = PgPoolOptions::new()
            .max_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(pool.connect_options().as_ref().clone())
            .await
            .unwrap();
        let losing_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&loser_pool)
            .await
            .unwrap();
        let history_sql = "SELECT sequence, payload FROM events
                           WHERE aggregate_type = 'NativeNote' ORDER BY sequence";
        let before: Vec<(i64, Value)> = sqlx::query_as(history_sql).fetch_all(&pool).await.unwrap();
        let (mut winner, decision) = PostgresRecovery::<Note<2>>::begin(&winner_pool)
            .await
            .unwrap();
        assert_eq!(decision, SchemaReconciliation::Changed);
        let winning_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *winner.transaction)
            .await
            .unwrap();
        let loser = tokio::spawn(async move {
            let (recovery, decision) = PostgresRecovery::<Note<2>>::begin(&loser_pool)
                .await
                .unwrap();
            recovery.finish().await.unwrap();
            decision
        });
        timeout(Duration::from_secs(10), async {
            loop {
                let blocking: Vec<i32> = sqlx::query_scalar("SELECT pg_blocking_pids($1)")
                    .bind(losing_pid)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                if blocking == vec![winning_pid] {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("independent startup must actually contend on the winner's registry lock");
        let registry_sql = "SELECT payload FROM events
                            WHERE aggregate_type = 'SchemaRegistry' ORDER BY sequence";
        let during: Vec<Value> = sqlx::query_scalar(registry_sql)
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(
            during,
            vec![json!({"VersionUpdated": {"name": "NativeNote", "version": 1}})]
        );
        let snapshots: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM snapshots")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(snapshots, 1);
        winner.finish().await.unwrap();
        let loser_decision = timeout(Duration::from_secs(10), loser)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loser_decision, SchemaReconciliation::Unchanged);
        let recorded: Vec<Value> = sqlx::query_scalar(registry_sql)
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(
            recorded,
            vec![
                json!({"VersionUpdated": {"name": "NativeNote", "version": 1}}),
                json!({"VersionUpdated": {"name": "NativeNote", "version": 2}}),
            ]
        );
        let after: Vec<(i64, Value)> = sqlx::query_as(history_sql).fetch_all(&pool).await.unwrap();
        assert_eq!(after, before);
        let snapshots: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM snapshots")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(snapshots, 0);
    }

    fn mode_error<Success>(result: Result<Success, PersistenceError>) -> RegistryStorageModeError {
        let Err(PersistenceError::UnknownError(source)) = result else {
            panic!("expected an explicit registry storage-mode error");
        };
        *source.downcast::<RegistryStorageModeError>().unwrap()
    }

    #[sqlx::test(migrations = false)]
    async fn registry_repository_enforces_event_only_storage(pool: PgPool) {
        let mut transaction = pool.begin().await.unwrap();
        let official = PostgresEventRepository { pool: pool.clone() };
        {
            let repository = RegistryRepository {
                transaction: Mutex::new(&mut transaction),
                repository: &official,
            };
            assert!(matches!(
                mode_error(
                    repository
                        .get_snapshot::<RegisteredSchemaView>(SCHEMA_REGISTRY_ID)
                        .await
                ),
                RegistryStorageModeError::Snapshot
            ));
            assert!(matches!(
                mode_error(
                    repository
                        .persist::<RegisteredSchemaView>(
                            &[],
                            Some((SCHEMA_REGISTRY_ID.to_string(), json!({}), 1)),
                        )
                        .await
                ),
                RegistryStorageModeError::Snapshot
            ));
            assert!(matches!(
                mode_error(
                    repository
                        .stream_events::<RegisteredSchemaView>(SCHEMA_REGISTRY_ID)
                        .await
                ),
                RegistryStorageModeError::Streaming
            ));
            assert!(matches!(
                mode_error(repository.stream_all_events::<RegisteredSchemaView>().await),
                RegistryStorageModeError::Streaming
            ));
            repository
                .persist::<RegisteredSchemaView>(&[], None)
                .await
                .unwrap();
        }
        transaction.rollback().await.unwrap();
    }
}
