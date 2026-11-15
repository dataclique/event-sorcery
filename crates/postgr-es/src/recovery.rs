//! Transaction-scoped schema startup using the existing registry authority.

use cqrs_es::Aggregate;
use cqrs_es::persist::{
    PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot,
};
use serde_json::Value;
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use std::marker::PhantomData;
use tokio::sync::Mutex;

use event_sorcery::{
    CommitRequest, EngineError, EventSourced, ReconcileError, SCHEMA_REGISTRY_ID,
    SchemaReconciliation, SchemaRegistry, StreamIdentity, reconcile_schema, register_schema,
    validate_expected_version,
};

use crate::event_repository::{append_events, load_events};

pub(crate) struct PostgresRecovery<Entity: EventSourced> {
    transaction: Transaction<'static, Postgres>,
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
        let payloads = sqlx::query_scalar::<_, String>(
            "SELECT payload::TEXT FROM events \
             WHERE aggregate_type = $1 AND aggregate_id = $2 ORDER BY sequence",
        )
        .bind(SchemaRegistry::AGGREGATE_TYPE)
        .bind(SCHEMA_REGISTRY_ID)
        .fetch_all(&mut *transaction)
        .await?;
        let reconciliation = reconcile_schema::<Entity>(payloads)?;

        match reconciliation {
            SchemaReconciliation::Changed => {
                sqlx::query("DELETE FROM snapshots WHERE aggregate_type = $1")
                    .bind(Entity::AGGREGATE_TYPE)
                    .execute(&mut *transaction)
                    .await?;
            }
            SchemaReconciliation::Unchanged => {}
        }

        Ok((
            Self {
                transaction,
                _entity: PhantomData,
            },
            reconciliation,
        ))
    }

    pub(crate) async fn finish(mut self) -> Result<(), ReconcileError> {
        {
            let repository = RegistryRepository {
                connection: Mutex::new(&mut *self.transaction),
            };
            register_schema::<Entity, _>(repository).await?;
        }
        self.transaction.commit().await?;
        Ok(())
    }
}

struct RegistryRepository<'connection> {
    connection: Mutex<&'connection mut PgConnection>,
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
        let mut connection = self.connection.lock().await;
        Ok(load_events(&mut **connection, AggregateType::TYPE, aggregate_id, 0).await?)
    }

    async fn get_last_events<AggregateType: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let mut connection = self.connection.lock().await;
        Ok(load_events(
            &mut **connection,
            AggregateType::TYPE,
            aggregate_id,
            last_sequence,
        )
        .await?)
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

        let mut connection = self.connection.lock().await;
        let current: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(sequence), 0) FROM events \
             WHERE aggregate_type = $1 AND aggregate_id = $2",
        )
        .bind(AggregateType::TYPE)
        .bind(&first.aggregate_id)
        .fetch_one(&mut **connection)
        .await
        .map_err(EngineError::from)?;
        validate_expected_version(
            expected,
            usize::try_from(current).map_err(EngineError::from)?,
        )?;
        append_events(&mut connection, events).await?;
        drop(connection);
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
        {
            let repository = RegistryRepository {
                connection: Mutex::new(&mut *transaction),
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
