use sqlx::{Pool, Sqlite};

use crate::MIGRATOR;

/// Creates an in-memory SQLite database with migrations applied
///
/// # Errors
///
/// Returns an error if the database connection fails or migrations cannot be applied
pub async fn create_test_pool() -> Result<Pool<Sqlite>, sqlx::Error> {
    let pool = Pool::<Sqlite>::connect(":memory:").await?;
    MIGRATOR.run(&pool).await?;
    Ok(pool)
}
