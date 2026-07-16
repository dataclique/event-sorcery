//! Unsafe C ABI for the captive event-sorcery engine.
//!
//! Inputs remain caller-owned for each call. Outputs remain engine-owned until
//! released with [`es_buf_free`]. The owner cell initialized by [`es_open`] must
//! remain at a stable address; [`es_close`] linearizes concurrent closes through
//! that cell, waits for acquired calls, and destroys the engine exactly once.

use std::collections::HashMap;
use std::io::Cursor;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
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
const MAX_CBOR_DEPTH: usize = 32;
/// Maximum worker threads accepted from the fixed-width C ABI options product.
const MAX_RUNTIME_THREADS: u32 = 256;
static STORE_REGISTRY: OnceLock<Mutex<HashMap<usize, StoreEntry>>> = OnceLock::new();

#[repr(C)]
#[derive(Clone, Copy)]
pub struct EsBuf {
    pub ptr: *mut u8,
    pub len: usize,
}

pub struct EsStore {
    state: Mutex<StoreState>,
    state_changed: Condvar,
    inner: Mutex<Option<Arc<StoreInner>>>,
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
/// `out_store` must be a stable, unowned writable cell that remains at the same
/// address until close completes. `out_error` must be null or writable.
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
        let store = Arc::new(EsStore {
            state: Mutex::new(StoreState::Open { active_calls: 0 }),
            state_changed: Condvar::new(),
            inner: Mutex::new(Some(Arc::new(StoreInner {
                _runtime: runtime,
                _engine: engine,
            }))),
            poisoned: AtomicBool::new(false),
        });
        publish_store(out_store, store)
    })
}

#[unsafe(no_mangle)]
/// Closes a store returned by [`es_open`]. A null owner or null handle is a
/// no-op.
///
/// # Safety
///
/// A non-null owner must be the original cell passed to [`es_open`]. Its value
/// must not be copied to another owner or modified by the caller. Repeated and
/// concurrent calls with the same owner are safe.
pub unsafe extern "C" fn es_close(store: *mut *mut EsStore) -> i32 {
    if store.is_null() {
        return ES_OK;
    }
    let owner_address = store as usize;
    let (store, close_role) = match linearize_close(store) {
        Ok(Some(close)) => close,
        Ok(None) => return ES_OK,
        Err(error) => return error.code(),
    };
    let result =
        catch_unwind(AssertUnwindSafe(|| store.close(close_role))).unwrap_or(Err(AbiError::Panic));
    retire_store(owner_address, &store);
    result.map_or_else(|error| error.code(), |()| ES_OK)
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

struct StoreEntry {
    raw_store: usize,
    store: Arc<EsStore>,
}

#[cfg(test)]
struct StoreLease {
    store: Arc<EsStore>,
    _inner: Arc<StoreInner>,
}

enum StoreState {
    Open { active_calls: usize },
    Closing { active_calls: usize },
    Closed(Result<(), AbiError>),
}

#[derive(Clone, Copy)]
enum CloseRole {
    Destroy,
    Join,
}

fn store_registry() -> &'static Mutex<HashMap<usize, StoreEntry>> {
    STORE_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn publish_store(owner: *mut *mut EsStore, store: Arc<EsStore>) -> Result<(), AbiError> {
    let owner_address = owner as usize;
    let mut registry = store_registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if registry.contains_key(&owner_address) {
        return Err(AbiError::State("store owner is already open"));
    }
    let raw_store = Arc::into_raw(Arc::clone(&store)).cast_mut();
    registry.insert(
        owner_address,
        StoreEntry {
            raw_store: raw_store as usize,
            store,
        },
    );
    unsafe { owner.write(raw_store) };
    drop(registry);
    Ok(())
}

fn linearize_close(
    owner: *mut *mut EsStore,
) -> Result<Option<(Arc<EsStore>, CloseRole)>, AbiError> {
    let owner_address = owner as usize;
    let registry = store_registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(entry) = registry.get(&owner_address) else {
        unsafe { owner.write(ptr::null_mut()) };
        return Ok(None);
    };
    let raw_store = unsafe { owner.read() };
    if !raw_store.is_null() && raw_store as usize != entry.raw_store {
        return Err(AbiError::State("store owner does not match its handle"));
    }
    let store = Arc::clone(&entry.store);
    let mut state = store
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let role = match &*state {
        StoreState::Open { active_calls } => {
            *state = StoreState::Closing {
                active_calls: *active_calls,
            };
            CloseRole::Destroy
        }
        StoreState::Closing { .. } | StoreState::Closed(_) => CloseRole::Join,
    };
    unsafe { owner.write(ptr::null_mut()) };
    drop(state);
    store.state_changed.notify_all();
    drop(registry);
    Ok(Some((store, role)))
}

fn retire_store(owner_address: usize, store: &Arc<EsStore>) {
    let mut registry = store_registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let should_remove = registry
        .get(&owner_address)
        .is_some_and(|entry| Arc::ptr_eq(&entry.store, store));
    let entry = should_remove
        .then(|| registry.remove(&owner_address))
        .flatten();
    drop(registry);
    if let Some(entry) = entry {
        let raw_store: *const EsStore = std::ptr::with_exposed_provenance(entry.raw_store);
        unsafe { drop(Arc::from_raw(raw_store)) };
    }
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
    #[cfg(test)]
    fn acquire(owner: *mut *mut Self) -> Result<StoreLease, AbiError> {
        if owner.is_null() {
            return Err(AbiError::State("store owner is null"));
        }
        let owner_address = owner as usize;
        let registry = store_registry()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(entry) = registry.get(&owner_address) else {
            return Err(AbiError::State("store is closed"));
        };
        let raw_store = unsafe { owner.read() };
        if raw_store.is_null() || raw_store as usize != entry.raw_store {
            return Err(AbiError::State("store is closed"));
        }
        let store = Arc::clone(&entry.store);
        let mut state = store
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let StoreState::Open { active_calls } = &mut *state else {
            return Err(AbiError::State("store is closing"));
        };
        let inner = store
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .cloned()
            .ok_or(AbiError::State("store is closed"))?;
        *active_calls = active_calls
            .checked_add(1)
            .ok_or(AbiError::State("active call count overflow"))?;
        drop(state);
        drop(registry);
        Ok(StoreLease {
            store,
            _inner: inner,
        })
    }

    fn close(&self, role: CloseRole) -> Result<(), AbiError> {
        if matches!(role, CloseRole::Join) {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            loop {
                match &*state {
                    StoreState::Closed(result) => return *result,
                    StoreState::Open { .. } | StoreState::Closing { .. } => {
                        state = self
                            .state_changed
                            .wait(state)
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                    }
                }
            }
        }

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while matches!(&*state, StoreState::Closing { active_calls } if *active_calls != 0) {
            state = self
                .state_changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        drop(state);

        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let result = catch_unwind(AssertUnwindSafe(|| drop(inner))).map_err(|_| AbiError::Panic);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = StoreState::Closed(result);
        drop(state);
        self.state_changed.notify_all();
        result
    }
}

#[cfg(test)]
impl StoreLease {
    fn wait_until_closing(&self) {
        let mut state = self
            .store
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while matches!(&*state, StoreState::Open { .. }) {
            state = self
                .store
                .state_changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        drop(state);
    }
}

#[cfg(test)]
impl Drop for StoreLease {
    fn drop(&mut self) {
        let mut state = self
            .store
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let drained = match &mut *state {
            StoreState::Open { active_calls } | StoreState::Closing { active_calls } => {
                if let Some(remaining) = active_calls.checked_sub(1) {
                    *active_calls = remaining;
                    remaining == 0
                } else {
                    self.store.poisoned.store(true, Ordering::Release);
                    true
                }
            }
            StoreState::Closed(_) => {
                self.store.poisoned.store(true, Ordering::Release);
                true
            }
        };
        drop(state);
        if drained {
            self.store.state_changed.notify_all();
        }
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
    let decoded: (u8, String, u64, u32, u32) =
        ciborium::de::from_reader_with_recursion_limit(Cursor::new(bytes), MAX_CBOR_DEPTH)
            .map_err(|_| AbiError::MalformedInput)?;
    let mut canonical = Vec::new();
    ciborium::into_writer(&decoded, &mut canonical).map_err(|_| AbiError::MalformedInput)?;
    if canonical != bytes {
        return Err(AbiError::MalformedInput);
    }
    let (version, path, busy_timeout_ms, pool_size, runtime_threads): (u8, String, u64, u32, u32) =
        decoded;
    if version != 1
        || pool_size == 0
        || runtime_threads == 0
        || (path == "sqlite::memory:" && pool_size != 1)
    {
        return Err(AbiError::MalformedInput);
    }
    let runtime_threads = usize::try_from(runtime_threads).map_err(|_| AbiError::MalformedInput)?;
    let runtime_thread_limit =
        usize::try_from(MAX_RUNTIME_THREADS).map_err(|_| AbiError::MalformedInput)?;
    if runtime_threads > runtime_thread_limit {
        return Err(AbiError::ResourceLimit {
            resource: "runtime_threads",
            observed: runtime_threads,
            limit: runtime_thread_limit,
        });
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

    fn encoded_open_options(runtime_threads: u32) -> Vec<u8> {
        let mut encoded = Vec::new();
        ciborium::into_writer(
            &(1_u8, "sqlite::memory:", 5_000_u64, 1_u32, runtime_threads),
            &mut encoded,
        )
        .unwrap();
        encoded
    }

    fn open_options() -> Vec<u8> {
        encoded_open_options(1)
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
    fn open_options_accept_the_maximum_runtime_thread_count() {
        let mut encoded = encoded_open_options(MAX_RUNTIME_THREADS);
        let buffer = EsBuf {
            ptr: encoded.as_mut_ptr(),
            len: encoded.len(),
        };

        let Ok(options) = decode_open_options(&raw const buffer) else {
            panic!("maximum runtime thread count must be accepted");
        };

        assert_eq!(
            options.runtime_threads,
            usize::try_from(MAX_RUNTIME_THREADS).unwrap()
        );
    }

    #[test]
    fn open_options_reject_the_first_runtime_thread_count_above_the_limit() {
        let mut encoded = encoded_open_options(MAX_RUNTIME_THREADS + 1);
        let buffer = EsBuf {
            ptr: encoded.as_mut_ptr(),
            len: encoded.len(),
        };

        let result = decode_open_options(&raw const buffer);

        assert!(matches!(
            result,
            Err(AbiError::ResourceLimit {
                resource: "runtime_threads",
                observed,
                limit,
            }) if observed == usize::try_from(MAX_RUNTIME_THREADS + 1).unwrap()
                && limit == usize::try_from(MAX_RUNTIME_THREADS).unwrap()
        ));
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
    fn close_waits_for_calls_that_acquired_the_store() {
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
        let Ok(lease) = EsStore::acquire(&raw mut store) else {
            panic!("open store must be acquirable");
        };
        let owner_address = (&raw mut store) as usize;
        let (closed_tx, closed_rx) = std::sync::mpsc::channel();

        std::thread::scope(|scope| {
            scope.spawn(move || {
                let result = unsafe { es_close(owner_address as *mut *mut EsStore) };
                closed_tx.send(result).unwrap();
            });

            lease.wait_until_closing();
            assert!(matches!(
                closed_rx.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ));
            drop(lease);
            assert_eq!(
                closed_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
                ES_OK
            );
        });

        assert!(store.is_null());
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
