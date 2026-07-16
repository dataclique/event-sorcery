use std::io::Cursor;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use event_sorcery::Engine;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};

const ABI_MAJOR: u32 = 0;
const ABI_MINOR: u32 = 1;
const ES_OK: i32 = 0;
const ES_ERR_DECODE: i32 = 1;
const ES_ERR_STORAGE: i32 = 4;
const ES_ERR_STATE: i32 = 5;
const ES_ERR_RESOURCE_LIMIT: i32 = 6;
const ES_ERR_PANIC: i32 = 100;
const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
static CLOSE_GATE: Mutex<()> = Mutex::new(());

#[repr(C)]
#[derive(Clone, Copy)]
pub struct EsBuf {
    pub ptr: *mut u8,
    pub len: usize,
}

pub struct EsStore {
    state: Mutex<StoreState>,
    closed: Condvar,
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
    options: *const EsBuf,
    out_store: *mut *mut EsStore,
    out_error: *mut EsBuf,
) -> i32 {
    if !out_store.is_null() {
        unsafe { out_store.write(ptr::null_mut()) };
    }
    ffi_call(None, out_error, || {
        if out_store.is_null() {
            return Err(AbiError::State("out_store is null"));
        }
        let options = decode_open_options(options)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(options.runtime_threads)
            .enable_all()
            .build()
            .map_err(|_| AbiError::State("runtime initialization failed"))?;
        let connect = SqliteConnectOptions::from_str(&options.path)
            .map_err(|_| AbiError::MalformedInput)?
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
            .map_err(|_| AbiError::Storage)?;
        let engine = Engine::new(pool);
        runtime
            .block_on(engine.migrate())
            .map_err(|_| AbiError::Storage)?;
        let store = Box::new(EsStore {
            state: Mutex::new(StoreState::Open(StoreInner {
                _runtime: runtime,
                _engine: engine,
            })),
            closed: Condvar::new(),
            poisoned: AtomicBool::new(false),
        });
        unsafe { out_store.write(Box::into_raw(store)) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// Closes a store returned by [`es_open`]. A null owner or null handle is a
/// no-op.
///
/// # Safety
///
/// A non-null owner must contain a pointer returned by [`es_open`]. Repeated
/// and concurrent calls with the same owner are safe.
pub unsafe extern "C" fn es_close(store: *mut *mut EsStore) -> i32 {
    let store = {
        let _close = CLOSE_GATE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(store) = (unsafe { store.as_mut() }) else {
            return ES_OK;
        };
        std::mem::replace(store, ptr::null_mut())
    };
    if store.is_null() {
        return ES_OK;
    }
    let store = unsafe { Box::from_raw(store) };
    match catch_unwind(AssertUnwindSafe(move || {
        let result = store.close();
        drop(store);
        result
    })) {
        Ok(Ok(())) => ES_OK,
        Ok(Err(error)) => error.code(),
        Err(_) => ES_ERR_PANIC,
    }
}

#[unsafe(no_mangle)]
/// Releases an engine-owned output buffer. A null owner or null buffer is a
/// no-op.
///
/// # Safety
///
/// A non-null owner must contain a buffer returned by this library.
pub unsafe extern "C" fn es_buf_free(buffer: *mut EsBuf) {
    let Some(buffer) = (unsafe { buffer.as_mut() }) else {
        return;
    };
    if buffer.ptr.is_null() {
        return;
    }
    let owned = std::mem::replace(
        buffer,
        EsBuf {
            ptr: ptr::null_mut(),
            len: 0,
        },
    );
    let slice = ptr::slice_from_raw_parts_mut(owned.ptr, owned.len);
    unsafe { drop(Box::from_raw(slice)) };
}

struct OpenOptions {
    path: String,
    busy_timeout: Duration,
    pool_size: u32,
    runtime_threads: usize,
}

struct StoreInner {
    _runtime: tokio::runtime::Runtime,
    _engine: Engine,
}

enum StoreState {
    Open(StoreInner),
    Closing,
    Closed,
}

#[derive(Clone, Copy)]
enum AbiError {
    MalformedInput,
    Storage,
    State(&'static str),
    ResourceLimit {
        resource: &'static str,
        observed: usize,
        limit: usize,
    },
    Panic,
}

impl EsStore {
    fn close(&self) -> Result<(), AbiError> {
        let inner = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            loop {
                match &*state {
                    StoreState::Open(_) => {
                        let StoreState::Open(inner) =
                            std::mem::replace(&mut *state, StoreState::Closing)
                        else {
                            unreachable!();
                        };
                        break inner;
                    }
                    StoreState::Closing => {
                        state = self
                            .closed
                            .wait(state)
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                    }
                    StoreState::Closed => return Ok(()),
                }
            }
        };

        let close_result = catch_unwind(AssertUnwindSafe(|| drop(inner)));
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = StoreState::Closed;
        drop(state);
        self.closed.notify_all();
        close_result.map_err(|_| AbiError::Panic)
    }
}

impl AbiError {
    const fn code(&self) -> i32 {
        match self {
            Self::MalformedInput => ES_ERR_DECODE,
            Self::Storage => ES_ERR_STORAGE,
            Self::State(_) => ES_ERR_STATE,
            Self::ResourceLimit { .. } => ES_ERR_RESOURCE_LIMIT,
            Self::Panic => ES_ERR_PANIC,
        }
    }
}

fn decode_open_options(buffer: *const EsBuf) -> Result<OpenOptions, AbiError> {
    let Some(buffer) = (unsafe { buffer.as_ref() }) else {
        return Err(AbiError::MalformedInput);
    };
    if buffer.ptr.is_null() {
        return Err(AbiError::MalformedInput);
    }
    if buffer.len > MAX_REQUEST_BYTES {
        return Err(AbiError::ResourceLimit {
            resource: "encoded_request_buffer",
            observed: buffer.len,
            limit: MAX_REQUEST_BYTES,
        });
    }
    let bytes = unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len) };
    let decoded: (u8, String, u64, u32, usize) =
        ciborium::from_reader(Cursor::new(bytes)).map_err(|_| AbiError::MalformedInput)?;
    let mut canonical = Vec::new();
    ciborium::into_writer(&decoded, &mut canonical).map_err(|_| AbiError::MalformedInput)?;
    if canonical != bytes {
        return Err(AbiError::MalformedInput);
    }
    let (version, path, busy_timeout_ms, pool_size, runtime_threads): (
        u8,
        String,
        u64,
        u32,
        usize,
    ) = decoded;
    if version != 1
        || pool_size == 0
        || runtime_threads == 0
        || (path == "sqlite::memory:" && pool_size != 1)
    {
        return Err(AbiError::MalformedInput);
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
    clear_buffer_output(out_error);
    if store.is_some_and(|store| store.poisoned.load(Ordering::Acquire)) {
        return write_error(out_error, AbiError::State("store is poisoned"));
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
    let class = error.code();
    let detail = match error {
        AbiError::MalformedInput => ciborium::Value::Text("malformed input".to_string()),
        AbiError::Storage => ciborium::Value::Text("storage failure".to_string()),
        AbiError::State(detail) => ciborium::Value::Text(detail.to_string()),
        AbiError::ResourceLimit {
            resource,
            observed,
            limit,
        } => ciborium::Value::Array(vec![
            ciborium::Value::Text(resource.to_string()),
            ciborium::Value::Integer(observed.into()),
            ciborium::Value::Integer(limit.into()),
        ]),
        AbiError::Panic => ciborium::Value::Null,
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

const fn clear_buffer_output(output: *mut EsBuf) {
    if !output.is_null() {
        unsafe {
            output.write(EsBuf {
                ptr: ptr::null_mut(),
                len: 0,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use ciborium::Value;

    use super::*;

    fn empty_buffer() -> EsBuf {
        EsBuf {
            ptr: ptr::null_mut(),
            len: 0,
        }
    }

    fn open_options() -> Vec<u8> {
        let mut encoded = Vec::new();
        ciborium::into_writer(
            &(1_u8, "sqlite::memory:", 5_000_u64, 1_u32, 1_usize),
            &mut encoded,
        )
        .unwrap();
        encoded
    }

    fn decode_error(buffer: EsBuf) -> (u8, i32, Value) {
        let bytes = unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len) };
        ciborium::from_reader(Cursor::new(bytes)).unwrap()
    }

    #[test]
    fn opens_migrates_and_closes_an_in_memory_store() {
        let mut encoded = open_options();
        let mut store = ptr::null_mut();
        let mut error = empty_buffer();
        let options = EsBuf {
            ptr: encoded.as_mut_ptr(),
            len: encoded.len(),
        };

        assert_eq!(
            unsafe { es_open(&raw const options, &raw mut store, &raw mut error) },
            ES_OK
        );
        assert!(!store.is_null());
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
        assert!(store.is_null());
        assert!(error.ptr.is_null());
    }

    #[test]
    fn open_clears_the_store_output_before_decoding() {
        let mut malformed = [0xff];
        let mut store = std::ptr::NonNull::<EsStore>::dangling().as_ptr();
        let mut error = empty_buffer();
        let options = EsBuf {
            ptr: malformed.as_mut_ptr(),
            len: malformed.len(),
        };

        let result = unsafe { es_open(&raw const options, &raw mut store, &raw mut error) };

        assert_eq!(result, ES_ERR_DECODE);
        assert!(store.is_null());
        unsafe { es_buf_free(&raw mut error) };
    }

    #[test]
    fn successful_open_clears_a_stale_error_output() {
        let mut encoded = open_options();
        let mut store = ptr::null_mut();
        let mut error = EsBuf {
            ptr: std::ptr::NonNull::<u8>::dangling().as_ptr(),
            len: 0,
        };
        let options = EsBuf {
            ptr: encoded.as_mut_ptr(),
            len: encoded.len(),
        };

        let result = unsafe { es_open(&raw const options, &raw mut store, &raw mut error) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);

        assert_eq!(result, ES_OK);
        assert!(error.ptr.is_null());
    }

    #[test]
    fn oversized_open_request_reports_the_resource_limit() {
        let mut oversized = vec![0_u8; MAX_REQUEST_BYTES + 1];
        let mut store = ptr::null_mut();
        let mut error = empty_buffer();
        let options = EsBuf {
            ptr: oversized.as_mut_ptr(),
            len: oversized.len(),
        };

        let result = unsafe { es_open(&raw const options, &raw mut store, &raw mut error) };
        let error_value = decode_error(error);

        assert_eq!(result, ES_ERR_RESOURCE_LIMIT);
        assert_eq!(
            error_value,
            (
                1,
                ES_ERR_RESOURCE_LIMIT,
                Value::Array(vec![
                    Value::Text("encoded_request_buffer".to_string()),
                    Value::Integer(oversized.len().into()),
                    Value::Integer(MAX_REQUEST_BYTES.into()),
                ]),
            )
        );
        assert!(store.is_null());
        unsafe { es_buf_free(&raw mut error) };
    }

    #[test]
    fn open_rejects_an_indefinite_length_cbor_product() {
        let mut encoded = open_options();
        assert_eq!(encoded[0], 0x85);
        encoded[0] = 0x9f;
        encoded.push(0xff);
        let mut store = ptr::null_mut();
        let mut error = empty_buffer();
        let options = EsBuf {
            ptr: encoded.as_mut_ptr(),
            len: encoded.len(),
        };

        let result = unsafe { es_open(&raw const options, &raw mut store, &raw mut error) };

        if !store.is_null() {
            assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
        }
        assert_eq!(result, ES_ERR_DECODE);
        unsafe { es_buf_free(&raw mut error) };
    }

    #[test]
    fn storage_errors_use_the_stable_code_and_redacted_detail() {
        let mut error = empty_buffer();

        let result = write_error(&raw mut error, AbiError::Storage);
        let error_value = decode_error(error);

        assert_eq!(result, ES_ERR_STORAGE);
        assert_eq!(
            error_value,
            (
                1,
                ES_ERR_STORAGE,
                Value::Text("storage failure".to_string())
            )
        );
        unsafe { es_buf_free(&raw mut error) };
    }

    #[test]
    fn panic_errors_have_a_null_detail() {
        let mut error = empty_buffer();

        let result = write_error(&raw mut error, AbiError::Panic);
        let error_value = decode_error(error);

        assert_eq!(result, ES_ERR_PANIC);
        assert_eq!(error_value, (1, ES_ERR_PANIC, Value::Null));
        unsafe { es_buf_free(&raw mut error) };
    }

    #[test]
    fn close_is_idempotent_for_the_same_owner() {
        let mut encoded = open_options();
        let mut store = ptr::null_mut();
        let mut error = empty_buffer();
        let options = EsBuf {
            ptr: encoded.as_mut_ptr(),
            len: encoded.len(),
        };
        let result = unsafe { es_open(&raw const options, &raw mut store, &raw mut error) };
        assert_eq!(result, ES_OK);

        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
        assert!(store.is_null());
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn concurrent_close_attempts_destroy_the_store_once() {
        let mut encoded = open_options();
        let mut store = ptr::null_mut();
        let mut error = empty_buffer();
        let options = EsBuf {
            ptr: encoded.as_mut_ptr(),
            len: encoded.len(),
        };
        let result = unsafe { es_open(&raw const options, &raw mut store, &raw mut error) };
        assert_eq!(result, ES_OK);

        let owner_address = (&raw mut store) as usize;
        let results = std::thread::scope(|scope| {
            let handles = std::array::from_fn::<_, 4, _>(|_| {
                scope.spawn(move || unsafe { es_close(owner_address as *mut *mut EsStore) })
            });
            handles.map(|handle| handle.join().unwrap())
        });

        assert_eq!(results, [ES_OK; 4]);
        assert!(store.is_null());
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn buffer_release_clears_the_owner_and_is_idempotent() {
        let mut buffer = empty_buffer();
        assert_eq!(
            write_error(&raw mut buffer, AbiError::State("test failure")),
            ES_ERR_STATE
        );
        assert!(!buffer.ptr.is_null());

        unsafe { es_buf_free(&raw mut buffer) };
        assert!(buffer.ptr.is_null());
        assert_eq!(buffer.len, 0);
        unsafe { es_buf_free(&raw mut buffer) };
    }
}
