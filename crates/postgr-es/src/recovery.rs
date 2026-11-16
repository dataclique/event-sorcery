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
            let histories: Vec<(String, i64, i64)> = sqlx::query_as(
                "SELECT s.aggregate_id, s.last_sequence, \
                 (SELECT COUNT(*) FROM events e \
                  WHERE e.aggregate_type = s.aggregate_type \
                    AND e.aggregate_id = s.aggregate_id \
                    AND e.sequence BETWEEN 1 AND s.last_sequence) \
                 FROM snapshots s WHERE s.aggregate_type = $1",
            )
            .bind(Entity::AGGREGATE_TYPE)
            .fetch_all(&mut *transaction)
            .await?;
            for (aggregate_id, covered_sequence, retained_events) in histories {
                validate_snapshot_history(
                    StreamIdentity::new(Entity::AGGREGATE_TYPE, aggregate_id),
                    covered_sequence,
                    retained_events,
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

    use super::*;
    use crate::view_repository::tests::RegisteredSchemaView;

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
