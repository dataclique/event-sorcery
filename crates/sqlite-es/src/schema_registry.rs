//! Reads the framework's schema registry from SQLite event history.

use sqlx::SqlitePool;

/// Reads the schema-registry payloads in event order.
pub async fn load_schema_registry_payloads(pool: &SqlitePool) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar!(
        r#"
            SELECT payload AS "payload: String"
            FROM events
            WHERE aggregate_type = 'SchemaRegistry'
              AND aggregate_id = 'schema'
            ORDER BY sequence
            "#,
    )
    .fetch_all(pool)
    .await
}
