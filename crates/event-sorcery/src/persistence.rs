//! Persistence capabilities for native stores, independent of worker operation.

use std::future::Future;

use cqrs_es::persist::PersistedEventRepository;

use crate::{CompactionPolicy, EventBackend};

/// Storage required to execute and reload native aggregates.
///
/// Repositories must atomically persist command events, snapshots and dispatched
/// jobs. Bootstrap applies library-owned migrations; it does not reconcile an
/// entity's schema or start workers. Callers invoke bootstrap explicitly.
///
/// Existing [`crate::EventBackend`] implementations receive this capability
/// automatically. Backend authors implement either this narrower contract or
/// `EventBackend`, not both. Existing backend member names remain unambiguous:
///
/// ```
/// use event_sorcery::*;
///
/// fn repository<Backend: EventBackend>(backend: &Backend) -> Backend::EventRepo {
///     backend.event_repo(CompactionPolicy::Retain)
/// }
///
/// async fn migrate<Backend: EventBackend>(backend: &Backend) -> Result<(), Backend::Error> {
///     backend.migrate().await
/// }
/// ```
pub trait PersistenceBackend: Clone + Send + Sync + 'static {
    /// The repository used by the existing cqrs-es execution path.
    type PersistenceRepo: PersistedEventRepository + Send + Sync + 'static;

    /// Failure while applying library-owned storage migrations.
    type BootstrapError: std::error::Error + Send + Sync + 'static;

    /// Constructs a repository honoring the entity's retention policy.
    fn persistence_repo(&self, policy: CompactionPolicy) -> Self::PersistenceRepo;

    /// Applies library-owned migrations without changing existing data.
    fn bootstrap_persistence(
        &self,
    ) -> impl Future<Output = Result<(), Self::BootstrapError>> + Send;
}

impl<Backend: EventBackend> PersistenceBackend for Backend {
    type PersistenceRepo = <Backend as EventBackend>::EventRepo;
    type BootstrapError = <Backend as EventBackend>::Error;

    fn persistence_repo(&self, policy: CompactionPolicy) -> Self::PersistenceRepo {
        <Backend as EventBackend>::event_repo(self, policy)
    }

    async fn bootstrap_persistence(&self) -> Result<(), Self::BootstrapError> {
        <Backend as EventBackend>::migrate(self).await
    }
}
