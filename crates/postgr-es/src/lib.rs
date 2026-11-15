//! PostgreSQL repositories, migrations and native persistence adapters.

mod backend;
mod event_repository;
mod recovery;
mod view_repository;

pub use backend::{PostgresBackend, PostgresStoreRepository};
pub use event_repository::{MIGRATOR, PostgresEventRepository};
pub use view_repository::{
    PostgresViewBackend, PostgresViewRepository, ProjectionQueryError, ProjectionVersionError,
};
