//! Backend-agnostic projection for loading entity state from
//! materialized views.
//!
//! Generic over an explicitly selected [`ViewBackend`](crate::ViewBackend).
//! Database-specific construction and recovery live in the backend integrations.

use async_trait::async_trait;
use cqrs_es::persist::{PersistenceError, ViewRepository};
use cqrs_es::{Aggregate, EventEnvelope, Query};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use tracing::{error, trace, warn};

use indexed_query::{IndexedView, Order, Predicate};

use crate::dependency::{Cons, Dependent, EntityList, Nil};
use crate::lifecycle::{Lifecycle, LifecycleError, Never};
use crate::reactor::{
    RETRY_BASE_DELAY_MS, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY_MS, Reactor, ReactorContext,
    retry_delays,
};
use crate::view_backend::{ViewBackend, ViewUpdateError};
use crate::{EventSourced, Table};

/// A column name for view table queries.
///
/// Used with [`Projection::filter`] to query materialized views
/// by generated column values. Column existence is validated
/// against the table schema at query time to catch stale
/// generated columns early.
#[derive(Debug, Clone)]
pub struct Column(pub &'static str);

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// Errors from [`Projection`] query operations.
#[derive(Debug, thiserror::Error)]
pub enum ProjectionError<Entity: EventSourced> {
    /// Replay requires an entity configured to retain its event history.
    #[error("projection recovery requires retained history for aggregate '{aggregate_type}'")]
    RequiresRetainedHistory {
        /// Entity type whose projection cannot be recovered from compactable history.
        aggregate_type: &'static str,
    },
    /// This operation requires a projection associated with a native view table.
    #[error("operation requires a projection backed by a native persistence adapter")]
    MissingViewTable,
    #[error("column '{column}' does not exist in table '{table}'")]
    ColumnNotFound { column: Column, table: String },
    #[error(
        "generated column '{column}' has all NULL values in \
         table '{table}' ({row_count} rows) - likely stale \
         JSON path in migration"
    )]
    StaleColumn {
        column: Column,
        table: String,
        row_count: i64,
    },
    #[error("serde failed for aggregate '{aggregate_id}': {source}")]
    Serde {
        aggregate_id: String,
        source: serde_json::Error,
    },
    #[error("projection query failed: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("view repository operation failed: {0}")]
    Persistence(#[from] PersistenceError),
    #[error(transparent)]
    Lifecycle(Box<LifecycleError<Entity>>),
}

impl<Entity: EventSourced> From<LifecycleError<Entity>> for ProjectionError<Entity> {
    fn from(error: LifecycleError<Entity>) -> Self {
        Self::Lifecycle(Box::new(error))
    }
}

/// Materialized view of an event-sourced entity.
///
/// Provides [`load`](Self::load) to retrieve a single entity by ID.
/// Native projections also provide [`load_all`](Self::load_all) and
/// [`filter`](Self::filter) for typed column queries.
/// The backend is explicit in named types: `Projection<Position, Backend>`.
/// Constructed through the selected native backend during wiring or directly
/// in CLI/test code.
pub struct Projection<Entity: EventSourced, Backend: ViewBackend> {
    repo: Arc<Backend::Repo<Lifecycle<Entity>, Lifecycle<Entity>>>,
    table: Option<Table>,
    _entity: PhantomData<Entity>,
}

impl<Entity, Backend> Projection<Entity, Backend>
where
    Entity: EventSourced<Materialized = Table>,
    Backend: crate::NativeProjectionBackend,
{
    /// Constructs a native projection without applying migrations or recording schema.
    pub fn with_backend(source: Backend::Source) -> Self {
        let table = Entity::PROJECTION;
        Self {
            repo: Arc::new(Backend::repository::<Lifecycle<Entity>, Lifecycle<Entity>>(
                source, table,
            )),
            table: Some(table),
            _entity: PhantomData,
        }
    }

    /// Reads all live entities through the native storage source.
    pub async fn load_all(&self) -> Result<Vec<(Entity::Id, Entity)>, ProjectionError<Entity>>
    where
        <Entity::Id as FromStr>::Err: Debug,
    {
        let (source, table) = self.native_backing()?;
        Ok(Self::parse_rows(Backend::rows(source, table).await?))
    }

    /// Validates a generated column and binds its domain value with the native driver.
    pub async fn filter<Value>(
        &self,
        column: Column,
        value: &Value,
    ) -> Result<Vec<(Entity::Id, Entity)>, ProjectionError<Entity>>
    where
        for<'query> &'query Value: sqlx::Encode<'query, Backend::Database>,
        Value: sqlx::Type<Backend::Database> + Send + Sync,
        <Entity::Id as FromStr>::Err: Debug,
    {
        let (source, table) = self.native_backing()?;
        let rows = Backend::filter::<Entity, Value>(source, table, column, value).await?;
        Ok(Self::parse_rows(rows))
    }

    /// Replays missing events through a captured per-stream checkpoint.
    pub async fn catch_up(&self) -> Result<(), ProjectionError<Entity>> {
        let (source, _) = self.native_backing()?;
        Backend::recover::<Entity>(source, &crate::ProjectionRecoveryMode::CatchUp).await
    }

    /// Rebuilds one materialized row from retained history.
    pub async fn rebuild(&self, id: &Entity::Id) -> Result<(), ProjectionError<Entity>> {
        let (source, _) = self.native_backing()?;
        Backend::recover::<Entity>(
            source,
            &crate::ProjectionRecoveryMode::RebuildOne {
                aggregate_id: id.to_string(),
            },
        )
        .await
    }

    /// Rebuilds all materialized rows from retained history.
    pub async fn rebuild_all(&self) -> Result<(), ProjectionError<Entity>> {
        let (source, _) = self.native_backing()?;
        Backend::recover::<Entity>(source, &crate::ProjectionRecoveryMode::RebuildAll).await
    }

    fn native_backing(&self) -> Result<(&Backend::Source, Table), ProjectionError<Entity>> {
        let table = self.table.ok_or(ProjectionError::MissingViewTable)?;
        Ok((Backend::source(self.repo.as_ref()), table))
    }

    fn parse_rows(rows: Vec<(String, String)>) -> Vec<(Entity::Id, Entity)>
    where
        <Entity::Id as FromStr>::Err: Debug,
    {
        rows.into_iter()
            .filter_map(|(view_id, payload)| {
                let id: Entity::Id = match view_id.parse() {
                    Ok(id) => id,
                    Err(error) => {
                        warn!(view_id, ?error, "Failed to parse view ID");
                        return None;
                    }
                };

                let lifecycle: Lifecycle<Entity> = match serde_json::from_str(&payload) {
                    Ok(lifecycle) => lifecycle,
                    Err(error) => {
                        warn!(%id, ?error, "Failed to deserialize view payload");
                        return None;
                    }
                };

                if let Lifecycle::Live(entity) = lifecycle {
                    Some((id, entity))
                } else {
                    warn!(%id, "Skipping non-live aggregate in view");
                    None
                }
            })
            .collect()
    }
}

impl<Entity: EventSourced, Backend: ViewBackend> Projection<Entity, Backend> {
    #[cfg(test)]
    pub(crate) fn new(repo: Arc<Backend::Repo<Lifecycle<Entity>, Lifecycle<Entity>>>) -> Self {
        Self {
            repo,
            table: None,
            _entity: PhantomData,
        }
    }

    /// Load a single entity by ID from the materialized view.
    ///
    /// Delegates to the underlying view repository, then unwraps
    /// the internal `Lifecycle` wrapper. Returns `None` if the
    /// entity doesn't exist or hasn't been initialized yet.
    pub async fn load(&self, id: &Entity::Id) -> Result<Option<Entity>, ProjectionError<Entity>> {
        let view_id = id.to_string();

        trace!(%view_id, "Loading view");

        match self.repo.load(&view_id).await {
            Ok(Some(lifecycle)) => Ok(lifecycle.into_result()?),
            Ok(None) => Ok(None),
            Err(error) => Err(error)?,
        }
    }

    /// Find the `view_id`s whose view columns satisfy `predicate`, ordered and
    /// capped at `limit`. Delegates to the view repository's indexed scan -- the
    /// one query the load-by-id repository cannot express (e.g. polling a job
    /// queue for runnable rows).
    pub async fn find(
        &self,
        predicate: &Predicate,
        order: Option<&Order>,
        limit: i64,
    ) -> Result<Vec<String>, ProjectionError<Entity>> {
        Ok(self.repo.find(predicate, order, limit).await?)
    }
}

impl<Entity: EventSourced, Backend: ViewBackend> Clone for Projection<Entity, Backend> {
    fn clone(&self) -> Self {
        Self {
            repo: Arc::clone(&self.repo),
            table: self.table,
            _entity: PhantomData,
        }
    }
}

impl<Entity, Backend> Dependent for Projection<Entity, Backend>
where
    Entity: EventSourced + 'static,
    Backend: ViewBackend,
{
    type Dependencies = Cons<Entity, Nil>;
}

enum SkippedDelivery {
    Covered,
    InvalidCheckpoint(i64),
    Gap(DeliveryGap),
}

struct DeliveryGap {
    covered: usize,
    sequence: NonZeroUsize,
}

struct GapWait(Duration);

impl GapWait {
    fn for_batch() -> Self {
        let schedule = retry_delays(RETRY_BASE_DELAY_MS, RETRY_MAX_DELAY_MS);
        Self(
            schedule
                .zip(0..RETRY_MAX_ATTEMPTS)
                .map(|(delay, _)| delay)
                .sum(),
        )
    }

    fn spend(&mut self, delay: Duration) -> bool {
        let Self(remaining) = self;
        let Some(after_sleep) = remaining.checked_sub(delay) else {
            return false;
        };
        *remaining = after_sleep;
        true
    }
}

pub(crate) struct ProjectionQuery<Entity: EventSourced, Backend: ViewBackend> {
    /// The builder's recovered native projection, shared with the caller.
    pub(crate) projection: Arc<Projection<Entity, Backend>>,
}

#[async_trait]
impl<Entity, Backend> Query<Lifecycle<Entity>> for ProjectionQuery<Entity, Backend>
where
    Entity: EventSourced<Materialized = Table> + 'static,
    Backend: crate::NativeProjectionBackend,
    <Entity::Id as FromStr>::Err: Debug,
{
    async fn dispatch(&self, aggregate_id: &str, events: &[EventEnvelope<Lifecycle<Entity>>]) {
        let Ok(id) = aggregate_id.parse::<Entity::Id>() else {
            warn!(
                aggregate_id,
                aggregate_type = Entity::AGGREGATE_TYPE,
                "Failed to parse aggregate ID in projection query"
            );
            return;
        };
        let view_id = id.to_string();
        let mut gap_wait = GapWait::for_batch();
        for envelope in events {
            let Some(sequence) = NonZeroUsize::new(envelope.sequence) else {
                error!(%view_id, sequence = envelope.sequence,
                    "Persisted event has an invalid zero sequence; refusing projection delivery");
                continue;
            };
            let gap = self
                .projection
                .deliver(
                    &view_id,
                    &envelope.payload,
                    self.projection.table.and(Some(sequence)),
                    &mut gap_wait,
                )
                .await;
            if let Some(DeliveryGap { covered, sequence }) = gap {
                if let Err(error) = self.projection.catch_up().await {
                    error!(%view_id, covered, sequence = sequence.get(), ?error,
                        "Projection catch-up failed after a delivery gap");
                }
                // Catch-up captures the durable events, including the rest of this batch.
                return;
            }
        }
    }
}

#[async_trait]
impl<Entity, Backend> Reactor for Projection<Entity, Backend>
where
    Entity: EventSourced + 'static,
    Backend: ViewBackend,
{
    type Error = Never;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        self.react_with_context(event, ReactorContext { sequence: None })
            .await
    }

    async fn react_with_context(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
        delivery: ReactorContext,
    ) -> Result<(), Self::Error> {
        let (id, event) = event.into_inner();
        let view_id = id.to_string();
        // Only native construction establishes sequence-based checkpoints.
        // Plain custom repositories retain their opaque optimistic versions.
        let sequence = self.table.and(delivery.sequence);
        if let Some(DeliveryGap { covered, sequence }) = self
            .deliver(&view_id, &event, sequence, &mut GapWait::for_batch())
            .await
        {
            error!(%view_id, covered, sequence = sequence.get(),
                "Projection delivery is out of order; leaving checkpoint for recovery");
        }
        Ok(())
    }
}

impl<Entity: EventSourced, Backend: ViewBackend> Projection<Entity, Backend> {
    async fn deliver(
        &self,
        view_id: &str,
        event: &Entity::Event,
        sequence: Option<NonZeroUsize>,
        gap_wait: &mut GapWait,
    ) -> Option<DeliveryGap> {
        // Retry with exponential backoff: 10ms, 20ms, 40ms, ... capped at 1s.
        // 10 retries gives ~4.3s of total retry budget, enough for any
        // realistic burst of concurrent writers on the same aggregate.
        let max_retries = RETRY_MAX_ATTEMPTS;
        let base_delay_ms = RETRY_BASE_DELAY_MS;
        let max_delay_ms = RETRY_MAX_DELAY_MS;

        let delays = retry_delays(base_delay_ms, max_delay_ms);
        for (attempt, delay) in (0..=max_retries).zip(delays) {
            let outcome = Backend::try_update(&self.repo, view_id, |stored, context| {
                if let Some(sequence) = sequence {
                    let Ok(covered) = usize::try_from(context.version) else {
                        return ControlFlow::Break(SkippedDelivery::InvalidCheckpoint(
                            context.version,
                        ));
                    };
                    if sequence.get() <= covered {
                        return ControlFlow::Break(SkippedDelivery::Covered);
                    }
                    if covered.checked_add(1) != Some(sequence.get()) {
                        return ControlFlow::Break(SkippedDelivery::Gap(DeliveryGap {
                            covered,
                            sequence,
                        }));
                    }
                }
                let mut lifecycle = stored.unwrap_or_default();
                lifecycle.apply(event.clone());
                ControlFlow::Continue(lifecycle)
            })
            .await;

            use ViewUpdateError::{Load, Write};
            match outcome {
                Ok(None | Some(SkippedDelivery::Covered)) => return None,
                Ok(Some(SkippedDelivery::InvalidCheckpoint(version))) => {
                    error!(%view_id, version,
                        "Invalid projection checkpoint; refusing event delivery");
                    return None;
                }
                Ok(Some(SkippedDelivery::Gap(gap))) => {
                    if attempt < max_retries && gap_wait.spend(delay) {
                        sleep(delay).await;
                        continue;
                    }
                    return Some(gap);
                }
                Err(Load(error)) if Backend::is_retryable_error(&error) => {
                    if attempt < max_retries {
                        warn!(
                            %view_id, attempt = attempt + 1, max_retries, ?delay,
                            "Storage contention loading view, retrying"
                        );
                        sleep(delay).await;
                        continue;
                    }

                    error!(
                        %view_id, max_retries,
                        "View load lost: Storage contention conflict persisted after all retries"
                    );
                    return None;
                }
                Err(Load(error)) => {
                    warn!(%view_id, ?error, "Failed to load view for update");
                    return None;
                }
                Err(Write(PersistenceError::OptimisticLockError)) if attempt < max_retries => {
                    warn!(
                        %view_id, attempt = attempt + 1, max_retries, ?delay,
                        "Optimistic lock conflict, retrying view update"
                    );
                    sleep(delay).await;
                }
                Err(Write(PersistenceError::OptimisticLockError)) => {
                    error!(
                        %view_id, max_retries,
                        "View update lost: optimistic lock conflict persisted after all retries"
                    );
                    return None;
                }
                Err(Write(error)) if Backend::is_retryable_error(&error) => {
                    if attempt < max_retries {
                        warn!(
                            %view_id, attempt = attempt + 1, max_retries, ?delay,
                            "Storage contention, retrying view update"
                        );
                        sleep(delay).await;
                    } else {
                        error!(
                            %view_id, max_retries,
                            "View update lost: Storage contention conflict persisted after all retries"
                        );
                        return None;
                    }
                }
                Err(Write(error)) => {
                    warn!(%view_id, ?error, "Failed to save view update");
                    return None;
                }
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use cqrs_es::DomainEvent;
    use cqrs_es::persist::{PersistenceError, ViewContext, ViewRepository};
    use serde::{Deserialize, Serialize};

    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    use super::*;
    use crate::Nil;
    use crate::lifecycle::Never;
    use crate::{CompactionPolicy, Effect, uneventful};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEntity {
        name: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEvent;

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> String {
            "TestEvent".to_string()
        }
        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[async_trait]
    impl EventSourced for TestEntity {
        type Id = String;
        type Error = Never;
        type Command = ();
        type Event = TestEvent;
        type Materialized = Nil;
        type Jobs = Nil;

        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;
        const SNAPSHOT_SIZE: usize = 10;
        const AGGREGATE_TYPE: &'static str = "TestEntity";
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;

        fn originate(_event: &TestEvent) -> Option<Self> {
            Some(Self {
                name: "test".to_string(),
            })
        }

        fn evolve(entity: &Self, _event: &TestEvent) -> Result<Option<Self>, Never> {
            Ok(Some(entity.clone()))
        }

        async fn initialize(_command: ()) -> Result<Effect<Self>, Never> {
            uneventful()
        }

        async fn transition(&self, _command: ()) -> Result<Effect<Self>, Never> {
            uneventful()
        }
    }

    /// In-memory view repository for testing Projection::load.
    struct InMemoryRepo<View, Agg>
    where
        View: cqrs_es::View<Agg>,
        Agg: cqrs_es::Aggregate,
    {
        views: RwLock<HashMap<String, View>>,
        _phantom: PhantomData<Agg>,
    }

    impl<View, Agg> InMemoryRepo<View, Agg>
    where
        View: cqrs_es::View<Agg> + Clone,
        Agg: cqrs_es::Aggregate,
    {
        fn with(entries: Vec<(&str, View)>) -> Self {
            let mut views = HashMap::new();
            for (aggregate_id, view) in entries {
                views.insert(aggregate_id.to_string(), view);
            }
            Self {
                views: RwLock::new(views),
                _phantom: PhantomData,
            }
        }
    }

    impl<View, Agg> ViewRepository<View, Agg> for InMemoryRepo<View, Agg>
    where
        View: cqrs_es::View<Agg> + Clone,
        Agg: cqrs_es::Aggregate,
    {
        async fn load(&self, aggregate_id: &str) -> Result<Option<View>, PersistenceError> {
            Ok(self.views.read().await.get(aggregate_id).cloned())
        }

        async fn load_with_context(
            &self,
            aggregate_id: &str,
        ) -> Result<Option<(View, ViewContext)>, PersistenceError> {
            let view = self.views.read().await.get(aggregate_id).cloned();
            Ok(view.map(|view| {
                let context = ViewContext::new(aggregate_id.to_string(), 0);
                (view, context)
            }))
        }

        async fn update_view(
            &self,
            view: View,
            context: ViewContext,
        ) -> Result<(), PersistenceError> {
            self.views
                .write()
                .await
                .insert(context.view_instance_id, view);
            Ok(())
        }
    }

    impl<View, Agg> IndexedView<View, Agg> for InMemoryRepo<View, Agg>
    where
        View: cqrs_es::View<Agg> + Clone,
        Agg: cqrs_es::Aggregate,
    {
        // The in-memory double has no indexed columns; `find` behavior is
        // covered against `SqliteViewRepository`. Tests here poll via `load`.
        async fn find(
            &self,
            _predicate: &Predicate,
            _order: Option<&Order>,
            _limit: i64,
        ) -> Result<Vec<String>, PersistenceError> {
            Ok(vec![])
        }
    }

    /// `ViewBackend` whose `Repo<V, A>` is `InMemoryRepo<V, A>`.
    struct InMemoryViewBackend;

    impl ViewBackend for InMemoryViewBackend {
        type Repo<View, Agg>
            = InMemoryRepo<View, Agg>
        where
            View: cqrs_es::View<Agg> + Clone + 'static,
            Agg: cqrs_es::Aggregate + 'static;
    }

    type TestInMemoryRepo = InMemoryRepo<Lifecycle<TestEntity>, Lifecycle<TestEntity>>;
    type TestProjection = Projection<TestEntity, InMemoryViewBackend>;

    use std::sync::atomic::{AtomicU32, Ordering};

    use crate::dependency::OneOf;
    use crate::reactor::Reactor;

    /// In-memory view repository that returns `OptimisticLockError`
    /// a configurable number of times before succeeding.
    struct ConflictingRepo<View, Agg>
    where
        View: cqrs_es::View<Agg>,
        Agg: cqrs_es::Aggregate,
    {
        views: RwLock<HashMap<String, (View, i64)>>,
        remaining_conflicts: AtomicU32,
        _phantom: PhantomData<Agg>,
    }

    impl<View, Agg> ConflictingRepo<View, Agg>
    where
        View: cqrs_es::View<Agg>,
        Agg: cqrs_es::Aggregate,
    {
        fn new(conflicts: u32) -> Self {
            Self {
                views: RwLock::new(HashMap::new()),
                remaining_conflicts: AtomicU32::new(conflicts),
                _phantom: PhantomData,
            }
        }

        fn with_view(self, aggregate_id: &str, view: View) -> Self {
            let mut views = self.views.into_inner();
            views.insert(aggregate_id.to_string(), (view, 1));
            Self {
                views: RwLock::new(views),
                ..self
            }
        }
    }

    impl<View, Agg> ViewRepository<View, Agg> for ConflictingRepo<View, Agg>
    where
        View: cqrs_es::View<Agg> + Clone,
        Agg: cqrs_es::Aggregate,
    {
        async fn load(&self, aggregate_id: &str) -> Result<Option<View>, PersistenceError> {
            Ok(self
                .views
                .read()
                .await
                .get(aggregate_id)
                .map(|(view, _version)| view.clone()))
        }

        async fn load_with_context(
            &self,
            aggregate_id: &str,
        ) -> Result<Option<(View, ViewContext)>, PersistenceError> {
            let guard = self.views.read().await;
            Ok(guard.get(aggregate_id).map(|(view, version)| {
                let context = ViewContext::new(aggregate_id.to_string(), *version);
                (view.clone(), context)
            }))
        }

        async fn update_view(
            &self,
            view: View,
            context: ViewContext,
        ) -> Result<(), PersistenceError> {
            let remaining = self.remaining_conflicts.load(Ordering::SeqCst);

            if remaining > 0 {
                self.remaining_conflicts
                    .store(remaining - 1, Ordering::SeqCst);
                return Err(PersistenceError::OptimisticLockError);
            }

            let new_version = context.version + 1;
            self.views
                .write()
                .await
                .insert(context.view_instance_id, (view, new_version));

            Ok(())
        }
    }

    impl<View, Agg> IndexedView<View, Agg> for ConflictingRepo<View, Agg>
    where
        View: cqrs_es::View<Agg> + Clone,
        Agg: cqrs_es::Aggregate,
    {
        async fn find(
            &self,
            _predicate: &Predicate,
            _order: Option<&Order>,
            _limit: i64,
        ) -> Result<Vec<String>, PersistenceError> {
            Ok(vec![])
        }
    }

    /// `ViewBackend` whose `Repo<V, A>` is `ConflictingRepo<V, A>`.
    struct ConflictingViewBackend;

    impl ViewBackend for ConflictingViewBackend {
        type Repo<View, Agg>
            = ConflictingRepo<View, Agg>
        where
            View: cqrs_es::View<Agg> + Clone + 'static,
            Agg: cqrs_es::Aggregate + 'static;
    }

    type TestConflictingRepo = ConflictingRepo<Lifecycle<TestEntity>, Lifecycle<TestEntity>>;
    type TestConflictingProjection = Projection<TestEntity, ConflictingViewBackend>;

    /// Minimal `sqlx::error::DatabaseError` impl so tests don't need real
    /// SQLite lock contention -- just a stand-in with a controllable
    /// extended result code. Mirrors `TestDatabaseError` in `reactor.rs`.
    #[derive(Debug)]
    struct TestBusyDatabaseError {
        code: String,
    }

    impl std::fmt::Display for TestBusyDatabaseError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "test database error (code {})", self.code)
        }
    }

    impl std::error::Error for TestBusyDatabaseError {}

    impl sqlx::error::DatabaseError for TestBusyDatabaseError {
        fn message(&self) -> &'static str {
            "test database error"
        }

        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            Some(std::borrow::Cow::Borrowed(&self.code))
        }

        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    fn busy_persistence_error() -> PersistenceError {
        let boxed_sqlx_error: Box<dyn std::error::Error + Send + Sync + 'static> =
            Box::new(sqlx::Error::Database(Box::new(TestBusyDatabaseError {
                code: "5".to_string(),
            })));
        PersistenceError::ConnectionError(boxed_sqlx_error)
    }

    /// In-memory view repository that returns a busy-classified
    /// `PersistenceError::ConnectionError` a configurable number of times
    /// before succeeding. Mirrors `ConflictingRepo`, but for the SQLite-busy
    /// retry path rather than the optimistic-lock retry path.
    struct BusyRepo<View, Agg>
    where
        View: cqrs_es::View<Agg>,
        Agg: cqrs_es::Aggregate,
    {
        views: RwLock<HashMap<String, (View, i64)>>,
        remaining_busy_failures: AtomicU32,
        remaining_load_busy_failures: AtomicU32,
        recoveries: AtomicU32,
        _phantom: PhantomData<Agg>,
    }

    impl<View, Agg> BusyRepo<View, Agg>
    where
        View: cqrs_es::View<Agg>,
        Agg: cqrs_es::Aggregate,
    {
        /// Fails `update_view` a configurable number of times before succeeding.
        fn new(busy_failures: u32) -> Self {
            Self {
                views: RwLock::new(HashMap::new()),
                remaining_busy_failures: AtomicU32::new(busy_failures),
                remaining_load_busy_failures: AtomicU32::new(0),
                recoveries: AtomicU32::new(0),
                _phantom: PhantomData,
            }
        }

        /// Fails `load_with_context` a configurable number of times before
        /// succeeding, instead of `update_view`. Exercises the load-side busy
        /// retry branch in `Projection::react`.
        fn new_with_load_failures(load_busy_failures: u32) -> Self {
            Self {
                views: RwLock::new(HashMap::new()),
                remaining_busy_failures: AtomicU32::new(0),
                remaining_load_busy_failures: AtomicU32::new(load_busy_failures),
                recoveries: AtomicU32::new(0),
                _phantom: PhantomData,
            }
        }

        fn with_view(self, aggregate_id: &str, view: View) -> Self {
            let mut views = self.views.into_inner();
            views.insert(aggregate_id.to_string(), (view, 1));
            Self {
                views: RwLock::new(views),
                ..self
            }
        }
    }

    impl<View, Agg> ViewRepository<View, Agg> for BusyRepo<View, Agg>
    where
        View: cqrs_es::View<Agg> + Clone,
        Agg: cqrs_es::Aggregate,
    {
        async fn load(&self, aggregate_id: &str) -> Result<Option<View>, PersistenceError> {
            Ok(self
                .views
                .read()
                .await
                .get(aggregate_id)
                .map(|(view, _version)| view.clone()))
        }

        async fn load_with_context(
            &self,
            aggregate_id: &str,
        ) -> Result<Option<(View, ViewContext)>, PersistenceError> {
            let remaining = self.remaining_load_busy_failures.load(Ordering::SeqCst);

            if remaining > 0 {
                self.remaining_load_busy_failures
                    .store(remaining - 1, Ordering::SeqCst);
                return Err(busy_persistence_error());
            }

            let guard = self.views.read().await;
            Ok(guard.get(aggregate_id).map(|(view, version)| {
                let context = ViewContext::new(aggregate_id.to_string(), *version);
                (view.clone(), context)
            }))
        }

        async fn update_view(
            &self,
            view: View,
            context: ViewContext,
        ) -> Result<(), PersistenceError> {
            let remaining = self.remaining_busy_failures.load(Ordering::SeqCst);

            if remaining > 0 {
                self.remaining_busy_failures
                    .store(remaining - 1, Ordering::SeqCst);
                return Err(busy_persistence_error());
            }

            let new_version = context.version + 1;
            self.views
                .write()
                .await
                .insert(context.view_instance_id, (view, new_version));

            Ok(())
        }
    }

    impl<View, Agg> IndexedView<View, Agg> for BusyRepo<View, Agg>
    where
        View: cqrs_es::View<Agg> + Clone,
        Agg: cqrs_es::Aggregate,
    {
        async fn find(
            &self,
            _predicate: &Predicate,
            _order: Option<&Order>,
            _limit: i64,
        ) -> Result<Vec<String>, PersistenceError> {
            Ok(vec![])
        }
    }

    /// `ViewBackend` whose `Repo<V, A>` is `BusyRepo<V, A>`.
    struct BusyViewBackend;

    impl ViewBackend for BusyViewBackend {
        type Repo<View, Agg>
            = BusyRepo<View, Agg>
        where
            View: cqrs_es::View<Agg> + Clone + 'static,
            Agg: cqrs_es::Aggregate + 'static;

        fn is_retryable_error(error: &(dyn std::error::Error + 'static)) -> bool {
            matches!(error.downcast_ref::<PersistenceError>(), Some(PersistenceError::ConnectionError(source)) if matches!(source.downcast_ref::<sqlx::Error>(), Some(sqlx::Error::Database(database)) if database.try_downcast_ref::<TestBusyDatabaseError>().is_some()))
        }
    }

    type TestBusyRepo = BusyRepo<Lifecycle<TestEntity>, Lifecycle<TestEntity>>;
    type TestBusyProjection = Projection<TestEntity, BusyViewBackend>;

    // `Counter` (defined below for the `catch_up` tests) has an `evolve` that
    // actually mutates its state, unlike `TestEntity`'s no-op `evolve`. The
    // busy-retry success tests use it so a silently dropped update (rather
    // than a genuinely retried-and-applied one) would show up as a failed
    // assertion instead of passing either way.
    type CounterBusyRepo = BusyRepo<Lifecycle<Counter>, Lifecycle<Counter>>;
    type CounterBusyProjection = Projection<Counter, BusyViewBackend>;

    #[tokio::test]
    async fn react_retries_on_optimistic_lock_conflict() {
        let entity = TestEntity {
            name: "original".to_string(),
        };
        let repo: TestConflictingRepo =
            ConflictingRepo::new(2).with_view("id-1", Lifecycle::Live(entity));
        let projection: TestConflictingProjection = Projection::new(Arc::new(repo));

        let event: OneOf<(String, TestEvent), Never> = OneOf::Here(("id-1".to_string(), TestEvent));

        projection.react(event).await.unwrap();

        // evolve clones the entity unchanged, but the update was persisted
        let result = projection.load(&"id-1".to_string()).await.unwrap();
        assert_eq!(
            result,
            Some(TestEntity {
                name: "original".to_string(),
            })
        );
    }

    #[tokio::test(start_paused = true)]
    async fn react_gives_up_after_max_retries() {
        // 11 conflicts exceeds the max of 10 retries (attempts 0..=10)
        let entity = TestEntity {
            name: "original".to_string(),
        };
        let repo: Arc<TestConflictingRepo> =
            Arc::new(ConflictingRepo::new(11).with_view("id-1", Lifecycle::Live(entity.clone())));
        let projection: TestConflictingProjection = Projection::new(Arc::clone(&repo));

        let event: OneOf<(String, TestEvent), Never> = OneOf::Here(("id-1".to_string(), TestEvent));

        // Should not panic -- react swallows the error
        projection.react(event).await.unwrap();

        // All 11 attempts were made (counter decremented from 11 to 0)
        assert_eq!(repo.remaining_conflicts.load(Ordering::SeqCst), 0);

        // View should still have the original entity (update never succeeded)
        let result = projection.load(&"id-1".to_string()).await.unwrap();
        assert_eq!(result, Some(entity));
    }

    #[tokio::test]
    async fn react_succeeds_without_conflict() {
        let entity = TestEntity {
            name: "original".to_string(),
        };
        let repo: TestConflictingRepo =
            ConflictingRepo::new(0).with_view("id-1", Lifecycle::Live(entity));
        let projection: TestConflictingProjection = Projection::new(Arc::new(repo));

        let event: OneOf<(String, TestEvent), Never> = OneOf::Here(("id-1".to_string(), TestEvent));

        projection.react(event).await.unwrap();

        let result = projection.load(&"id-1".to_string()).await.unwrap();
        assert_eq!(
            result,
            Some(TestEntity {
                name: "original".to_string(),
            })
        );
    }

    impl crate::NativeProjectionBackend for BusyViewBackend {
        type Source = AtomicU32;
        type Database = sqlx::Any;

        fn repository<ViewState, AggregateType>(
            recoveries: Self::Source,
            _table: Table,
        ) -> Self::Repo<ViewState, AggregateType>
        where
            ViewState: cqrs_es::View<AggregateType> + Clone + 'static,
            AggregateType: Aggregate + 'static,
        {
            BusyRepo {
                recoveries,
                ..BusyRepo::new(0)
            }
        }

        fn source<ViewState, AggregateType>(
            repository: &Self::Repo<ViewState, AggregateType>,
        ) -> &Self::Source
        where
            ViewState: cqrs_es::View<AggregateType> + Clone + 'static,
            AggregateType: Aggregate + 'static,
        {
            &repository.recoveries
        }

        async fn rows(
            _source: &Self::Source,
            _table: Table,
        ) -> Result<Vec<(String, String)>, sqlx::Error> {
            unreachable!("gap recovery does not scan materialized views")
        }

        async fn filter<Entity, Value>(
            _source: &Self::Source,
            _table: Table,
            _column: Column,
            _value: &Value,
        ) -> Result<Vec<(String, String)>, ProjectionError<Entity>>
        where
            Entity: EventSourced,
            for<'query> &'query Value: sqlx::Encode<'query, Self::Database>,
            Value: sqlx::Type<Self::Database> + Send + Sync,
        {
            unreachable!("gap recovery does not filter materialized views")
        }

        async fn recover<Entity>(
            recoveries: &Self::Source,
            mode: &crate::ProjectionRecoveryMode,
        ) -> Result<(), ProjectionError<Entity>>
        where
            Entity: EventSourced<Materialized = Table>,
        {
            assert!(matches!(mode, crate::ProjectionRecoveryMode::CatchUp));
            recoveries.fetch_add(1, Ordering::SeqCst);
            // A storage failure leaves the gap unresolved, so processing another
            // envelope would request catch-up again.
            Err(sqlx::Error::PoolClosed.into())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn native_dispatch_waits_one_budget_and_attempts_recovery_once() {
        let id = "id-1".to_string();
        let projection = Arc::new(CounterBusyProjection::with_backend(AtomicU32::new(0)));
        projection
            .repo
            .update_view(
                Lifecycle::Live(Counter { value: 0 }),
                ViewContext::new(id.clone(), 0),
            )
            .await
            .unwrap();
        let query = ProjectionQuery {
            projection: projection.clone(),
        };
        let batch = [3, 4].map(|sequence| EventEnvelope {
            aggregate_id: id.clone(),
            sequence,
            payload: CounterEvent::Incremented,
            metadata: HashMap::new(),
        });

        let started_at = tokio::time::Instant::now();
        query.dispatch(&id, &batch).await;

        assert_eq!(started_at.elapsed(), Duration::from_millis(4_270));
        assert_eq!(projection.repo.recoveries.load(Ordering::SeqCst), 1);
        let (_, context) = projection
            .repo
            .load_with_context(&id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(context.version, 1);
        assert_eq!(
            projection.load(&id).await.unwrap(),
            Some(Counter { value: 0 })
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_second_gap_cannot_restart_the_batch_wait() {
        let repo: Arc<CounterBusyRepo> =
            Arc::new(BusyRepo::new(0).with_view("id-1", Lifecycle::Live(Counter { value: 0 })));
        let projection: CounterBusyProjection = Projection::new(repo);
        let event = CounterEvent::Incremented;
        let mut wait = GapWait::for_batch();
        let initial_time = tokio::time::Instant::now();
        for sequence in [3, 4] {
            let gap = projection
                .deliver(
                    "id-1",
                    &event,
                    Some(sequence.try_into().unwrap()),
                    &mut wait,
                )
                .await
                .unwrap();
            assert_eq!(gap.covered, 1);
            assert_eq!(gap.sequence.get(), sequence);
            assert_eq!(initial_time.elapsed(), Duration::from_millis(4_270));
        }
        assert_eq!(
            projection.load(&"id-1".to_string()).await.unwrap(),
            Some(Counter { value: 0 })
        );
    }

    #[tokio::test(start_paused = true)]
    async fn custom_repository_keeps_opaque_versions_during_sequenced_delivery() {
        let repo: Arc<CounterBusyRepo> =
            Arc::new(BusyRepo::new(0).with_view("id-1", Lifecycle::Live(Counter { value: 0 })));
        let projection: CounterBusyProjection = Projection::new(repo);
        let event: OneOf<(String, CounterEvent), Never> =
            OneOf::Here(("id-1".to_string(), CounterEvent::Incremented));
        projection
            .react_with_context(
                event,
                ReactorContext {
                    sequence: Some(3_usize.try_into().unwrap()),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            projection.load(&"id-1".to_string()).await.unwrap(),
            Some(Counter { value: 1 })
        );
    }

    #[tokio::test]
    async fn react_retries_on_sqlite_busy_error_then_succeeds() {
        // Succeeds on the 3rd attempt, so total sleep is 10ms + 20ms -- fast.
        let repo: Arc<CounterBusyRepo> =
            Arc::new(BusyRepo::new(2).with_view("id-1", Lifecycle::Live(Counter { value: 0 })));
        let projection: CounterBusyProjection = Projection::new(Arc::clone(&repo));

        let event: OneOf<(String, CounterEvent), Never> =
            OneOf::Here(("id-1".to_string(), CounterEvent::Incremented));

        projection.react(event).await.unwrap();

        // Both busy failures were consumed by retries before the write succeeded.
        assert_eq!(repo.remaining_busy_failures.load(Ordering::SeqCst), 0);

        // Counter::evolve actually increments the value, so this only passes
        // if the retried write was truly applied -- a silently dropped update
        // (e.g. a missing/broken retry arm) would leave the view at 0.
        let result = projection.load(&"id-1".to_string()).await.unwrap();
        assert_eq!(result, Some(Counter { value: 1 }));
    }

    #[tokio::test]
    async fn react_retries_on_sqlite_busy_load_error_then_succeeds() {
        // Succeeds loading on the 3rd attempt, so total sleep is 10ms + 20ms.
        let repo: Arc<CounterBusyRepo> = Arc::new(
            BusyRepo::new_with_load_failures(2)
                .with_view("id-1", Lifecycle::Live(Counter { value: 0 })),
        );
        let projection: CounterBusyProjection = Projection::new(Arc::clone(&repo));

        let event: OneOf<(String, CounterEvent), Never> =
            OneOf::Here(("id-1".to_string(), CounterEvent::Incremented));

        projection.react(event).await.unwrap();

        // Both load-busy failures were consumed by retries before the load
        // succeeded.
        assert_eq!(repo.remaining_load_busy_failures.load(Ordering::SeqCst), 0);

        // Proves the retried load was followed by a real applied write, not a
        // silently dropped update.
        let result = projection.load(&"id-1".to_string()).await.unwrap();
        assert_eq!(result, Some(Counter { value: 1 }));
    }

    #[tokio::test(start_paused = true)]
    async fn react_gives_up_after_sqlite_busy_load_conflict_persists() {
        // 11 load-busy failures exceeds the max of 10 retries (attempts 0..=10)
        let repo: Arc<CounterBusyRepo> = Arc::new(
            BusyRepo::new_with_load_failures(11)
                .with_view("id-1", Lifecycle::Live(Counter { value: 0 })),
        );
        let projection: CounterBusyProjection = Projection::new(Arc::clone(&repo));

        let event: OneOf<(String, CounterEvent), Never> =
            OneOf::Here(("id-1".to_string(), CounterEvent::Incremented));

        // Should not panic -- react swallows the error
        projection.react(event).await.unwrap();

        // All 11 attempts were made (counter decremented from 11 to 0)
        assert_eq!(repo.remaining_load_busy_failures.load(Ordering::SeqCst), 0);

        // View should still have the original value (update never succeeded)
        let result = projection.load(&"id-1".to_string()).await.unwrap();
        assert_eq!(result, Some(Counter { value: 0 }));
    }

    #[tokio::test(start_paused = true)]
    async fn react_gives_up_after_sqlite_busy_conflict_persists() {
        // 11 busy failures exceeds the max of 10 retries (attempts 0..=10)
        let entity = TestEntity {
            name: "original".to_string(),
        };
        let repo: Arc<TestBusyRepo> =
            Arc::new(BusyRepo::new(11).with_view("id-1", Lifecycle::Live(entity.clone())));
        let projection: TestBusyProjection = Projection::new(Arc::clone(&repo));

        let event: OneOf<(String, TestEvent), Never> = OneOf::Here(("id-1".to_string(), TestEvent));

        // Should not panic -- react swallows the error
        projection.react(event).await.unwrap();

        // All 11 attempts were made (counter decremented from 11 to 0)
        assert_eq!(repo.remaining_busy_failures.load(Ordering::SeqCst), 0);

        // View should still have the original entity (update never succeeded)
        let result = projection.load(&"id-1".to_string()).await.unwrap();
        assert_eq!(result, Some(entity));
    }

    #[tokio::test]
    async fn load_live_entity_returns_some() {
        let entity = TestEntity {
            name: "alice".to_string(),
        };
        let repo: TestInMemoryRepo =
            InMemoryRepo::with(vec![("id-1", Lifecycle::Live(entity.clone()))]);
        let projection: TestProjection = Projection::new(Arc::new(repo));

        let result = projection.load(&"id-1".to_string()).await.unwrap();

        assert_eq!(result, Some(entity));
    }

    #[tokio::test]
    async fn load_missing_view_returns_none() {
        let repo: TestInMemoryRepo = InMemoryRepo::with(vec![]);
        let projection: TestProjection = Projection::new(Arc::new(repo));

        let result = projection.load(&"nonexistent".to_string()).await.unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn load_uninitialized_returns_none() {
        let repo: TestInMemoryRepo = InMemoryRepo::with(vec![("id-1", Lifecycle::Uninitialized)]);
        let projection: TestProjection = Projection::new(Arc::new(repo));

        let result = projection.load(&"id-1".to_string()).await.unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn load_failed_returns_error() {
        let error = LifecycleError::EventCantOriginate { event: TestEvent };
        let repo: TestInMemoryRepo = InMemoryRepo::with(vec![(
            "id-1",
            Lifecycle::Failed {
                error: error.clone(),
                last_valid_entity: None,
            },
        )]);
        let projection: TestProjection = Projection::new(Arc::new(repo));

        let result = projection.load(&"id-1".to_string()).await;

        assert!(matches!(
            result.unwrap_err(),
            ProjectionError::Lifecycle(boxed)
                if matches!(*boxed, LifecycleError::EventCantOriginate { .. })
        ));
    }

    // -- catch_up tests --

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Counter {
        value: i64,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum CounterEvent {
        Created { initial: i64 },
        Incremented,
    }

    impl DomainEvent for CounterEvent {
        fn event_type(&self) -> String {
            match self {
                Self::Created { .. } => "CounterEvent::Created".to_string(),
                Self::Incremented => "CounterEvent::Incremented".to_string(),
            }
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[async_trait]
    impl EventSourced for Counter {
        type Id = String;
        type Error = Never;
        type Command = ();
        type Event = CounterEvent;
        type Materialized = Table;
        type Jobs = Nil;

        const PROJECTION: Table = Table("counter_view");
        const SCHEMA_VERSION: u64 = 1;
        const SNAPSHOT_SIZE: usize = 10;
        const AGGREGATE_TYPE: &'static str = "Counter";
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;

        fn originate(event: &CounterEvent) -> Option<Self> {
            match event {
                CounterEvent::Created { initial } => Some(Self { value: *initial }),
                CounterEvent::Incremented => None,
            }
        }

        fn evolve(entity: &Self, event: &CounterEvent) -> Result<Option<Self>, Never> {
            match event {
                CounterEvent::Created { .. } => Ok(None),
                CounterEvent::Incremented => Ok(Some(Self {
                    value: entity.value + 1,
                })),
            }
        }

        async fn initialize(_command: ()) -> Result<Effect<Self>, Never> {
            uneventful()
        }

        async fn transition(&self, _command: ()) -> Result<Effect<Self>, Never> {
            uneventful()
        }
    }
}
