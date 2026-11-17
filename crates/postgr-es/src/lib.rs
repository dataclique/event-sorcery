//! PostgreSQL repositories, migrations and native persistence adapters.

mod backend;
mod event_repository;
mod projection_recovery;
mod recovery;
mod view_repository;

pub use backend::{PostgresBackend, PostgresStoreRepository};
pub use event_repository::{MIGRATOR, PostgresEventRepository};
pub use indexed_query::ProjectionQueryError;
pub use view_repository::{PostgresViewBackend, PostgresViewRepository, ProjectionVersionError};
