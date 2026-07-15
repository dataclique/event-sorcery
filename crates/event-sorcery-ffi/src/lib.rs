use std::io::Cursor;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use event_sorcery::Engine;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};

const ABI_MAJOR: u32 = 0;
const ABI_MINOR: u32 = 1;
const ES_OK: i32 = 0;
const ES_ERR_DECODE: i32 = 1;
const ES_ERR_STORAGE: i32 = 2;
const ES_ERR_STATE: i32 = 3;
const ES_ERR_PANIC: i32 = 100;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct EsBuf {
    pub ptr: *mut u8,
    pub len: usize,
}

pub struct EsStore {
    _runtime: tokio::runtime::Runtime,
    _engine: Engine,
    poisoned: AtomicBool,
}

#[unsafe(no_mangle)]
pub const extern "C" fn es_abi_version() -> u32 {
    (ABI_MAJOR << 16) | ABI_MINOR
}

#[unsafe(no_mangle)]
/// Opens and migrates an engine store from deterministic-CBOR options.
///
/// # Safety
///
/// `options` must reference readable bytes for the duration of the call.
/// `out_store` must be writable, and `out_error` must be null or writable.
pub unsafe extern "C" fn es_open(
    options: EsBuf,
    out_store: *mut *mut EsStore,
    out_error: *mut EsBuf,
) -> i32 {
    ffi_call(None, out_error, || {
        if out_store.is_null() {
            return Err(AbiError::State("out_store is null".to_string()));
        }
        let options = decode_open_options(options)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(options.runtime_threads)
            .enable_all()
            .build()
            .map_err(|error| AbiError::State(error.to_string()))?;
        let connect = SqliteConnectOptions::from_str(&options.path)
            .map_err(|error| AbiError::Decode(error.to_string()))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(options.busy_timeout);
        let pool = runtime
            .block_on(
                SqlitePoolOptions::new()
                    .max_connections(options.pool_size)
                    .connect_with(connect),
            )
            .map_err(|error| AbiError::Storage(error.to_string()))?;
        let engine = Engine::new(pool);
        runtime
            .block_on(engine.migrate())
            .map_err(|error| AbiError::Storage(error.to_string()))?;
        let store = Box::new(EsStore {
            _runtime: runtime,
            _engine: engine,
            poisoned: AtomicBool::new(false),
        });
        unsafe { out_store.write(Box::into_raw(store)) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// Closes a store returned by [`es_open`]. A null pointer is a no-op.
///
/// # Safety
///
/// A non-null `store` must be an unclosed pointer returned by [`es_open`].
pub unsafe extern "C" fn es_close(store: *mut EsStore) -> i32 {
    if store.is_null() {
        return ES_OK;
    }
    match catch_unwind(AssertUnwindSafe(|| unsafe { drop(Box::from_raw(store)) })) {
        Ok(()) => ES_OK,
        Err(_) => ES_ERR_PANIC,
    }
}

#[unsafe(no_mangle)]
/// Releases an engine-owned output buffer. A null buffer is a no-op.
///
/// # Safety
///
/// A non-null buffer must have been returned by this library and not yet freed.
pub unsafe extern "C" fn es_buf_free(buffer: EsBuf) {
    if buffer.ptr.is_null() {
        return;
    }
    let slice = ptr::slice_from_raw_parts_mut(buffer.ptr, buffer.len);
    unsafe { drop(Box::from_raw(slice)) };
}

struct OpenOptions {
    path: String,
    busy_timeout: Duration,
    pool_size: u32,
    runtime_threads: usize,
}

enum AbiError {
    Decode(String),
    Storage(String),
    State(String),
    Panic,
}

fn decode_open_options(buffer: EsBuf) -> Result<OpenOptions, AbiError> {
    if buffer.ptr.is_null() {
        return Err(AbiError::Decode("options buffer is null".to_string()));
    }
    let bytes = unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len) };
    let (version, path, busy_timeout_ms, pool_size, runtime_threads): (
        u8,
        String,
        u64,
        u32,
        usize,
    ) = ciborium::from_reader(Cursor::new(bytes))
        .map_err(|error| AbiError::Decode(error.to_string()))?;
    if version != 1
        || pool_size == 0
        || runtime_threads == 0
        || (path == "sqlite::memory:" && pool_size != 1)
    {
        return Err(AbiError::Decode("invalid open options".to_string()));
    }
    Ok(OpenOptions {
        path,
        busy_timeout: Duration::from_millis(busy_timeout_ms),
        pool_size,
        runtime_threads,
    })
}

fn ffi_call(
    store: Option<&EsStore>,
    out_error: *mut EsBuf,
    call: impl FnOnce() -> Result<(), AbiError>,
) -> i32 {
    if store.is_some_and(|store| store.poisoned.load(Ordering::Acquire)) {
        return write_error(out_error, AbiError::State("store is poisoned".to_string()));
    }
    match catch_unwind(AssertUnwindSafe(call)) {
        Ok(Ok(())) => ES_OK,
        Ok(Err(error)) => write_error(out_error, error),
        Err(_) => {
            if let Some(store) = store {
                store.poisoned.store(true, Ordering::Release);
            }
            write_error(out_error, AbiError::Panic)
        }
    }
}

fn write_error(out_error: *mut EsBuf, error: AbiError) -> i32 {
    let (class, detail) = match error {
        AbiError::Decode(detail) => (ES_ERR_DECODE, detail),
        AbiError::Storage(detail) => (ES_ERR_STORAGE, detail),
        AbiError::State(detail) => (ES_ERR_STATE, detail),
        AbiError::Panic => (ES_ERR_PANIC, "engine panic".to_string()),
    };
    if !out_error.is_null() {
        let mut bytes = Vec::new();
        if ciborium::into_writer(&(1_u8, class, detail), &mut bytes).is_ok() {
            let boxed = bytes.into_boxed_slice();
            let buffer = EsBuf {
                len: boxed.len(),
                ptr: Box::into_raw(boxed).cast::<u8>(),
            };
            unsafe { out_error.write(buffer) };
        }
    }
    class
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_migrates_and_closes_an_in_memory_store() {
        let mut encoded = Vec::new();
        ciborium::into_writer(
            &(1_u8, "sqlite::memory:", 5_000_u64, 1_u32, 1_usize),
            &mut encoded,
        )
        .unwrap();
        let mut store = ptr::null_mut();
        let mut error = EsBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        let options = EsBuf {
            ptr: encoded.as_mut_ptr(),
            len: encoded.len(),
        };

        assert_eq!(
            unsafe { es_open(options, &raw mut store, &raw mut error) },
            ES_OK
        );
        assert!(!store.is_null());
        assert_eq!(unsafe { es_close(store) }, ES_OK);
        assert!(error.ptr.is_null());
    }
}
