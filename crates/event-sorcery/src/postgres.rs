//! PostgreSQL persistence through the shared CQRS repository contracts.

/// Framework-owned PostgreSQL schema migrations.
///
/// Applications apply these migrations to their PostgreSQL pool before using
/// framework persistence. Existing SQLite migrations remain in `sqlite-es`.
pub static POSTGRES_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/postgres");
