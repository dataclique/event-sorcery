//! SQLite contention classification.

use cqrs_es::persist::PersistenceError;
use sqlx::error::DatabaseError;

/// Classifies whether an error chain contains a transient SQLite busy error.
///
/// The `SQLITE_BUSY` extended-code family (`5` plain, `261` recovery, `517`
/// snapshot, `773` timeout) is safe to retry from an event-log perspective.
///
/// Walks the `source()` chain, downcasting each node to `sqlx::Error`. A
/// downstream reactor error using the idiomatic
/// `#[error(transparent)] Sqlx(#[from] sqlx::Error)` shape makes thiserror
/// delegate `source()` past the `sqlx::Error` node to the `Box<dyn
/// DatabaseError>` that `sqlx::Error::Database` exposes one hop further in, so
/// each node is also downcast to that boxed database error.
/// `cqrs_es::persist::PersistenceError`'s boxed inner error isn't wired as
/// `#[source]` by cqrs-es, so the walk special-cases it and continues
/// manually into the box. `cqrs_es::AggregateError<T>` has the same shape but
/// is generic over the aggregate's `Entity`, so it can't be downcast from
/// fully generic code -- a busy error sealed behind it is unreachable here
/// and this function fails closed (`false`) for it. See `docs/sqlx.md` for the
/// full writeup of these shapes.
pub fn is_retryable_sqlite_busy(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);

    while let Some(this_error) = current {
        if let Some(sqlx::Error::Database(database_error)) =
            this_error.downcast_ref::<sqlx::Error>()
            && is_busy_extended_code(database_error.code().as_deref())
        {
            return true;
        }

        // A `#[error(transparent)]` sqlx wrapper delegates `source()` past the
        // `sqlx::Error` node straight to this boxed database error, so classify
        // it here too rather than miss the whole idiomatic downstream shape.
        if let Some(database_error) = this_error.downcast_ref::<Box<dyn DatabaseError>>()
            && is_busy_extended_code(database_error.code().as_deref())
        {
            return true;
        }

        let persistence_boxed_source =
            this_error
                .downcast_ref::<PersistenceError>()
                .and_then(|persistence_error| match persistence_error {
                    PersistenceError::ConnectionError(inner)
                    | PersistenceError::UnknownError(inner) => {
                        Some(inner.as_ref() as &(dyn std::error::Error + 'static))
                    }
                    PersistenceError::DeserializationError(_)
                    | PersistenceError::OptimisticLockError => None,
                });

        current = persistence_boxed_source.or_else(|| this_error.source());
    }

    false
}

/// Whether a `DatabaseError::code()` value is in SQLite's `SQLITE_BUSY`
/// extended-code family.
///
/// sqlx reports the extended result code as a decimal string; the primary code
/// is the low byte (`extended & 0xFF`). `5` is `SQLITE_BUSY`, and every extended
/// code built on it (recovery `261`, snapshot `517`, timeout `773`) is the same
/// underlying lock conflict, so match the whole family rather than enumerating
/// each extended code by hand.
fn is_busy_extended_code(code: Option<&str>) -> bool {
    match code.map(str::parse::<i32>) {
        Some(Ok(extended_code)) => extended_code & 0xFF == 5,
        Some(Err(_)) | None => false,
    }
}
