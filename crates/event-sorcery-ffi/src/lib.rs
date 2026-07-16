//! Stable C ABI over the shared event-sorcery engine.

use std::collections::HashMap;
use std::fmt;
use std::io::{self, Cursor, Write};
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

use cqrs_es::persist::SerializedEvent;
use event_sorcery::{
    CommitRequest, Engine, EngineError, LoadedEvent, LoadedPayload, StreamIdentity,
};
use serde::de::{DeserializeOwned, Error as DeserializeError, IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};

const ABI_MAJOR: u32 = 0;
const ABI_MINOR: u32 = 2;
const ES_OK: i32 = 0;
const ES_ERR_DECODE: i32 = 1;
const ES_ERR_CONFLICT: i32 = 2;
const ES_ERR_STORAGE: i32 = 4;
const ES_ERR_STATE: i32 = 5;
const ES_ERR_RESOURCE_LIMIT: i32 = 6;
const ES_ERR_PANIC: i32 = 100;
const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
/// Stored JSON can expand each opaque byte to four characters plus envelope
/// overhead before the exact CBOR response limit is applied.
const MAX_STORED_EVENT_PAGE_BYTES: usize = 4 * MAX_RESPONSE_BYTES + MAX_REQUEST_BYTES;
const MAX_CBOR_DEPTH: usize = 32;
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const MAX_COMMIT_EVENTS: usize = 1024;
const MAX_LIST_ITEMS: usize = 4096;
const MAX_ERROR_TEXT_BYTES: usize = 4 * 1024;
/// Maximum worker threads accepted from the fixed-width C ABI options product.
const MAX_RUNTIME_THREADS: u32 = 256;
static STORE_REGISTRY: OnceLock<Mutex<HashMap<usize, StoreEntry>>> = OnceLock::new();

type ProposedEventWire = (String, String, OpaqueBytes);
type CommitWire = (u8, String, String, u64, CommitEvents);
type StoredEventWire = (u64, String, String, OpaqueBytes);
#[cfg(test)]
type StoredEventsWire = (u8, Vec<StoredEventWire>);

#[derive(Debug, PartialEq, Eq)]
struct OpaqueBytes(Vec<u8>);

#[derive(Debug)]
struct CommitEvents {
    values: Vec<ProposedEventWire>,
    observed: usize,
}

impl Serialize for OpaqueBytes {
    fn serialize<SerializerType>(
        &self,
        serializer: SerializerType,
    ) -> Result<SerializerType::Ok, SerializerType::Error>
    where
        SerializerType: Serializer,
    {
        let Self(bytes) = self;
        serializer.serialize_bytes(bytes)
    }
}

impl Serialize for CommitEvents {
    fn serialize<SerializerType>(
        &self,
        serializer: SerializerType,
    ) -> Result<SerializerType::Ok, SerializerType::Error>
    where
        SerializerType: Serializer,
    {
        self.values.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CommitEvents {
    fn deserialize<DeserializerType>(
        deserializer: DeserializerType,
    ) -> Result<Self, DeserializerType::Error>
    where
        DeserializerType: Deserializer<'de>,
    {
        deserializer.deserialize_seq(CommitEventsVisitor)
    }
}

struct CommitEventsVisitor;

impl<'de> Visitor<'de> for CommitEventsVisitor {
    type Value = CommitEvents;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded sequence of proposed events")
    }

    fn visit_seq<Sequence>(self, mut sequence: Sequence) -> Result<Self::Value, Sequence::Error>
    where
        Sequence: SeqAccess<'de>,
    {
        let capacity = sequence.size_hint().unwrap_or(0).min(MAX_COMMIT_EVENTS);
        let mut values = Vec::with_capacity(capacity);
        while values.len() < MAX_COMMIT_EVENTS {
            let Some(event) = sequence.next_element()? else {
                return Ok(CommitEvents {
                    observed: values.len(),
                    values,
                });
            };
            values.push(event);
        }

        let mut observed = values.len();
        while sequence.next_element::<IgnoredAny>()?.is_some() {
            observed = observed
                .checked_add(1)
                .ok_or_else(|| Sequence::Error::custom("commit event count overflow"))?;
        }
        Ok(CommitEvents { values, observed })
    }
}

impl<'de> Deserialize<'de> for OpaqueBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_byte_buf(OpaqueBytesVisitor)
    }
}

struct OpaqueBytesVisitor;

impl Visitor<'_> for OpaqueBytesVisitor {
    type Value = OpaqueBytes;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a CBOR byte string")
    }

    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E> {
        Ok(OpaqueBytes(value.to_vec()))
    }

    fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E> {
        Ok(OpaqueBytes(value))
    }
}

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
/// `options` must reference a readable buffer for the duration of the call.
/// `out_store` must be a stable, unowned writable cell that remains at the same
/// address until close completes. `out_error` must be null or writable. The
/// options buffer may alias `out_error`; options are decoded before either
/// output is written.
pub unsafe extern "C" fn es_open(
    options: *const EsBuf,
    out_store: *mut *mut EsStore,
    out_error: *mut EsBuf,
) -> i32 {
    ffi_call(
        None,
        out_error,
        || {},
        || {
            let options = decode_open_options(options);
            if !out_store.is_null() {
                unsafe { out_store.write(ptr::null_mut()) };
            }
            if out_store.is_null() {
                return Err(AbiError::State("out_store is null"));
            }
            let options = options?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(options.runtime_threads)
                .enable_all()
                .build()
                .map_err(|error| AbiError::state_source("runtime initialization failed", error))?;
            let connect = SqliteConnectOptions::from_str(&options.path)
                .map_err(AbiError::decode)?
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
                .map_err(AbiError::storage)?;
            let engine = Engine::new(pool);
            runtime
                .block_on(engine.migrate())
                .map_err(AbiError::storage)?;
            let store = Arc::new(EsStore {
                state: Mutex::new(StoreState::Open { active_calls: 0 }),
                state_changed: Condvar::new(),
                inner: Mutex::new(Some(Arc::new(StoreInner { runtime, engine }))),
                poisoned: AtomicBool::new(false),
            });
            publish_store(out_store, store)
        },
    )
}

#[unsafe(no_mangle)]
/// Loads a stream through the shared engine.
///
/// # Safety
///
/// `store` must be the original owner cell passed to [`es_open`]. `request`
/// must reference a readable buffer. `out_events` must be writable, and
/// `out_error` must be null or writable. The request buffer may alias either
/// output, but `out_events` and `out_error` must be distinct.
pub unsafe extern "C" fn es_load_stream(
    store: *mut *mut EsStore,
    request: *const EsBuf,
    out_events: *mut EsBuf,
    out_error: *mut EsBuf,
) -> i32 {
    let lease = match EsStore::acquire(store) {
        Ok(lease) => lease,
        Err(error) => {
            clear_buffer_output(out_events);
            return write_error(out_error, error);
        }
    };
    ffi_call(
        Some(&lease.store),
        out_error,
        || clear_buffer_output(out_events),
        || {
            let request: Result<(u8, String, String, Option<u64>), AbiError> = decode(request);
            clear_buffer_output(out_events);
            if out_events.is_null() {
                return Err(AbiError::State("out_events is null"));
            }
            let (version, aggregate_type, aggregate_id, after) = request?;
            require_version(version)?;
            let after = after
                .map(usize::try_from)
                .transpose()
                .map_err(|_| AbiError::MalformedInput)?;
            let stream = StreamIdentity::new(aggregate_type, aggregate_id);
            let query_limit = NonZeroUsize::new(MAX_LIST_ITEMS + 1)
                .ok_or(AbiError::State("invalid list query limit"))?;
            let events = lease
                .inner
                .runtime
                .block_on(
                    lease.inner.engine.load_events_page_bounded(
                        &stream,
                        after,
                        query_limit,
                        NonZeroUsize::new(MAX_STORED_EVENT_PAGE_BYTES)
                            .ok_or(AbiError::State("invalid stored page byte limit"))?,
                    ),
                )
                .map_err(AbiError::from)?;
            if events.len() > MAX_LIST_ITEMS {
                return Err(AbiError::ResourceLimit {
                    resource: "list_items",
                    observed: events.len(),
                    limit: MAX_LIST_ITEMS,
                });
            }
            unsafe { out_events.write(encode_event_page(events)?) };
            Ok(())
        },
    )
}

#[unsafe(no_mangle)]
/// Reads the current stream version, where zero means no events.
///
/// # Safety
///
/// `store` must be the original owner cell passed to [`es_open`]. `request`
/// must reference a readable buffer. `out_version` must be writable, and
/// `out_error` must be null or writable. The request storage may alias either
/// output, but `out_version` and `out_error` must be distinct.
pub unsafe extern "C" fn es_current_version(
    store: *mut *mut EsStore,
    request: *const EsBuf,
    out_version: *mut u64,
    out_error: *mut EsBuf,
) -> i32 {
    let lease = match EsStore::acquire(store) {
        Ok(lease) => lease,
        Err(error) => {
            clear_version_output(out_version);
            return write_error(out_error, error);
        }
    };
    ffi_call(
        Some(&lease.store),
        out_error,
        || clear_version_output(out_version),
        || {
            let request: Result<(u8, String, String), AbiError> = decode(request);
            clear_version_output(out_version);
            if out_version.is_null() {
                return Err(AbiError::State("out_version is null"));
            }
            let (version, aggregate_type, aggregate_id) = request?;
            require_version(version)?;
            let stream = StreamIdentity::new(aggregate_type, aggregate_id);
            let version = lease
                .inner
                .runtime
                .block_on(lease.inner.engine.current_version(&stream))
                .map_err(AbiError::from)?;
            let version = u64::try_from(version).map_err(AbiError::storage)?;
            unsafe { out_version.write(version) };
            Ok(())
        },
    )
}

#[unsafe(no_mangle)]
/// Atomically appends events at the requested expected version.
///
/// # Safety
///
/// `store` must be the original owner cell passed to [`es_open`]. `request`
/// must reference a readable buffer. `out_error` must be null or writable. The
/// request buffer may alias `out_error`; it is decoded before the output is
/// written.
pub unsafe extern "C" fn es_commit(
    store: *mut *mut EsStore,
    request: *const EsBuf,
    out_error: *mut EsBuf,
) -> i32 {
    let lease = match EsStore::acquire(store) {
        Ok(lease) => lease,
        Err(error) => return write_error(out_error, error),
    };
    ffi_call(
        Some(&lease.store),
        out_error,
        || {},
        || {
            let (version, aggregate_type, aggregate_id, expected, events): CommitWire =
                decode_validated(request, |wire: &CommitWire| {
                    if wire.4.observed <= MAX_COMMIT_EVENTS {
                        return Ok(());
                    }
                    Err(AbiError::ResourceLimit {
                        resource: "commit_events",
                        observed: wire.4.observed,
                        limit: MAX_COMMIT_EVENTS,
                    })
                })?;
            require_version(version)?;
            require_error_text_limit(aggregate_type.len())?;
            require_error_text_limit(aggregate_id.len())?;
            let CommitEvents {
                values: events,
                observed: _,
            } = events;
            if events.is_empty() {
                return Err(AbiError::MalformedInput);
            }
            let expected_sequence =
                usize::try_from(expected).map_err(|_| AbiError::MalformedInput)?;
            expected_sequence
                .checked_add(events.len())
                .ok_or(AbiError::MalformedInput)?;
            let serialized = events
                .into_iter()
                .enumerate()
                .map(
                    |(index, (event_type, event_version, OpaqueBytes(payload)))| {
                        require_payload_limit(payload.len())?;
                        let sequence = expected_sequence
                            .checked_add(index)
                            .and_then(|sequence| sequence.checked_add(1))
                            .ok_or(AbiError::MalformedInput)?;
                        Ok(SerializedEvent {
                            aggregate_type: aggregate_type.clone(),
                            aggregate_id: aggregate_id.clone(),
                            sequence,
                            event_type,
                            event_version,
                            payload: serde_json::to_value(payload).map_err(AbiError::storage)?,
                            metadata: serde_json::Value::Object(serde_json::Map::new()),
                        })
                    },
                )
                .collect::<Result<Vec<_>, AbiError>>()?;
            let stream = StreamIdentity::new(aggregate_type.clone(), aggregate_id.clone());
            match lease.inner.runtime.block_on(
                lease
                    .inner
                    .engine
                    .commit(CommitRequest::new(stream.clone(), &serialized).with_opaque_payloads()),
            ) {
                Ok(()) => Ok(()),
                Err(EngineError::OptimisticLock) => {
                    let actual = lease
                        .inner
                        .runtime
                        .block_on(lease.inner.engine.current_version(&stream))
                        .map_err(AbiError::storage)?;
                    let actual = u64::try_from(actual).map_err(AbiError::storage)?;
                    Err(AbiError::Conflict {
                        aggregate_type,
                        aggregate_id,
                        expected,
                        actual,
                    })
                }
                Err(error) => Err(AbiError::storage(error)),
            }
        },
    )
}

#[unsafe(no_mangle)]
/// Closes a store returned by [`es_open`]. A null owner or handle is a no-op.
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
/// Releases an engine-owned output buffer. A null owner or buffer is a no-op.
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
    runtime: tokio::runtime::Runtime,
    engine: Engine,
}

struct StoreEntry {
    raw_store: usize,
    store: Arc<EsStore>,
}

struct StoreLease {
    store: Arc<EsStore>,
    inner: Arc<StoreInner>,
}

enum StoreState {
    Open { active_calls: usize },
    Closing { active_calls: usize },
    Closed(CloseOutcome),
}

#[derive(Clone, Copy)]
enum CloseRole {
    Destroy,
    Join,
}

#[derive(Clone, Copy)]
enum CloseOutcome {
    Ok,
    Panic,
}

#[derive(Debug, thiserror::Error)]
enum AbiError {
    #[error("malformed input")]
    MalformedInput,
    #[error("input decoding failed")]
    Decode(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("optimistic conflict")]
    Conflict {
        aggregate_type: String,
        aggregate_id: String,
        expected: u64,
        actual: u64,
    },
    #[error("storage operation failed")]
    Storage(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("{0}")]
    State(&'static str),
    #[error("{detail}")]
    StateSource {
        detail: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("resource {resource} observed {observed} above limit {limit}")]
    ResourceLimit {
        resource: &'static str,
        observed: usize,
        limit: usize,
    },
    #[error("panic crossed the ABI boundary")]
    Panic,
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

impl EsStore {
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
        Ok(StoreLease { store, inner })
    }

    fn close(&self, role: CloseRole) -> Result<(), AbiError> {
        if matches!(role, CloseRole::Join) {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            loop {
                match &*state {
                    StoreState::Closed(CloseOutcome::Ok) => return Ok(()),
                    StoreState::Closed(CloseOutcome::Panic) => return Err(AbiError::Panic),
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
        let outcome = if result.is_ok() {
            CloseOutcome::Ok
        } else {
            CloseOutcome::Panic
        };
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = StoreState::Closed(outcome);
        drop(state);
        self.state_changed.notify_all();
        result
    }
}

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

impl From<EngineError> for AbiError {
    fn from(error: EngineError) -> Self {
        match error {
            EngineError::EventPageTooLarge { observed, limit } => Self::ResourceLimit {
                resource: "stored_event_page",
                observed,
                limit,
            },
            other => Self::storage(other),
        }
    }
}

impl AbiError {
    fn decode(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Decode(Box::new(error))
    }

    fn storage(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Storage(Box::new(error))
    }

    fn state_source(
        detail: &'static str,
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::StateSource {
            detail,
            source: Box::new(error),
        }
    }

    const fn code(&self) -> i32 {
        match self {
            Self::MalformedInput | Self::Decode(_) => ES_ERR_DECODE,
            Self::Conflict { .. } => ES_ERR_CONFLICT,
            Self::Storage(_) => ES_ERR_STORAGE,
            Self::State(_) | Self::StateSource { .. } => ES_ERR_STATE,
            Self::ResourceLimit { .. } => ES_ERR_RESOURCE_LIMIT,
            Self::Panic => ES_ERR_PANIC,
        }
    }
}

fn decode<T: DeserializeOwned + Serialize>(buffer: *const EsBuf) -> Result<T, AbiError> {
    decode_validated(buffer, |_| Ok(()))
}

fn decode_validated<T: DeserializeOwned + Serialize>(
    buffer: *const EsBuf,
    validate: impl FnOnce(&T) -> Result<(), AbiError>,
) -> Result<T, AbiError> {
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
    let decoded =
        ciborium::de::from_reader_with_recursion_limit(Cursor::new(bytes), MAX_CBOR_DEPTH)
            .map_err(AbiError::decode)?;
    validate(&decoded)?;
    let mut canonical = Vec::new();
    ciborium::into_writer(&decoded, &mut canonical).map_err(AbiError::decode)?;
    if canonical != bytes {
        return Err(AbiError::MalformedInput);
    }
    Ok(decoded)
}

struct BoundedResponseWriter {
    bytes: Vec<u8>,
    overflow_observed: Option<usize>,
}

impl BoundedResponseWriter {
    const fn new() -> Self {
        Self {
            bytes: Vec::new(),
            overflow_observed: None,
        }
    }

    fn into_buffer(self) -> EsBuf {
        owned_buffer(self.bytes)
    }

    fn encoding_error(&self) -> AbiError {
        self.overflow_observed
            .map_or(AbiError::State("response encoding failed"), |observed| {
                AbiError::ResourceLimit {
                    resource: "encoded_response_buffer",
                    observed,
                    limit: MAX_RESPONSE_BYTES,
                }
            })
    }
}

impl Write for BoundedResponseWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let observed = self
            .bytes
            .len()
            .checked_add(buffer.len())
            .ok_or_else(|| io::Error::other("response size overflow"))?;
        if observed > MAX_RESPONSE_BYTES {
            self.overflow_observed = Some(observed);
            return Err(io::Error::other("response byte limit exceeded"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode_event_page(events: Vec<LoadedEvent>) -> Result<EsBuf, AbiError> {
    let mut writer = BoundedResponseWriter::new();
    writer
        .write_all(&[0x82, 0x01])
        .map_err(|_| writer.encoding_error())?;
    write_array_header(&mut writer, events.len())?;
    for event in events {
        let wire = event_to_wire(event)?;
        if ciborium::into_writer(&wire, &mut writer).is_err() {
            return Err(writer.encoding_error());
        }
    }
    Ok(writer.into_buffer())
}

fn write_array_header(writer: &mut BoundedResponseWriter, length: usize) -> Result<(), AbiError> {
    let encoded = match length {
        0..=23 => vec![0x80 | u8::try_from(length).map_err(AbiError::storage)?],
        24..=255 => vec![0x98, u8::try_from(length).map_err(AbiError::storage)?],
        256..=65_535 => {
            let length = u16::try_from(length).map_err(AbiError::storage)?;
            let mut encoded = vec![0x99];
            encoded.extend_from_slice(&length.to_be_bytes());
            encoded
        }
        _ => return Err(AbiError::State("event page length exceeds ABI limit")),
    };
    writer
        .write_all(&encoded)
        .map_err(|_| writer.encoding_error())
}

fn owned_buffer(bytes: Vec<u8>) -> EsBuf {
    let boxed = bytes.into_boxed_slice();
    EsBuf {
        len: boxed.len(),
        ptr: Box::into_raw(boxed).cast::<u8>(),
    }
}

const fn require_version(version: u8) -> Result<(), AbiError> {
    if version == 1 {
        Ok(())
    } else {
        Err(AbiError::MalformedInput)
    }
}

fn event_to_wire(event: LoadedEvent) -> Result<StoredEventWire, AbiError> {
    let payload = match event.payload {
        LoadedPayload::Json(payload) => serde_json::to_vec(&payload).map_err(AbiError::storage)?,
        LoadedPayload::OpaqueBytes(bytes) => bytes,
    };
    require_payload_limit(payload.len())?;
    Ok((
        u64::try_from(event.sequence).map_err(AbiError::storage)?,
        event.event_type,
        event.event_version,
        OpaqueBytes(payload),
    ))
}

const fn require_payload_limit(observed: usize) -> Result<(), AbiError> {
    if observed <= MAX_PAYLOAD_BYTES {
        return Ok(());
    }
    Err(AbiError::ResourceLimit {
        resource: "opaque_domain_payload",
        observed,
        limit: MAX_PAYLOAD_BYTES,
    })
}

const fn require_error_text_limit(observed: usize) -> Result<(), AbiError> {
    if observed <= MAX_ERROR_TEXT_BYTES {
        return Ok(());
    }
    Err(AbiError::ResourceLimit {
        resource: "error_detail_text",
        observed,
        limit: MAX_ERROR_TEXT_BYTES,
    })
}

fn decode_open_options(buffer: *const EsBuf) -> Result<OpenOptions, AbiError> {
    let (version, path, busy_timeout_ms, pool_size, runtime_threads): (u8, String, u64, u32, u32) =
        decode(buffer)?;
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
    initialize_result_output: impl Fn(),
    call: impl FnOnce() -> Result<(), AbiError>,
) -> i32 {
    if store.is_some_and(|store| store.poisoned.load(Ordering::Acquire)) {
        initialize_result_output();
        return write_error(out_error, AbiError::State("store is poisoned"));
    }
    match catch_unwind(AssertUnwindSafe(call)) {
        Ok(Ok(())) => {
            clear_buffer_output(out_error);
            ES_OK
        }
        Ok(Err(error)) => {
            initialize_result_output();
            write_error(out_error, error)
        }
        Err(_) => {
            initialize_result_output();
            if let Some(store) = store {
                store.poisoned.store(true, Ordering::Release);
            }
            write_error(out_error, AbiError::Panic)
        }
    }
}

fn write_error(out_error: *mut EsBuf, error: AbiError) -> i32 {
    clear_buffer_output(out_error);
    let class = error.code();
    let detail = match error {
        AbiError::MalformedInput | AbiError::Decode(_) => {
            ciborium::Value::Text("malformed input".to_string())
        }
        AbiError::Conflict {
            aggregate_type,
            aggregate_id,
            expected,
            actual,
        } => ciborium::Value::Array(vec![
            ciborium::Value::Text(aggregate_type),
            ciborium::Value::Text(aggregate_id),
            ciborium::Value::Integer(expected.into()),
            ciborium::Value::Integer(actual.into()),
        ]),
        AbiError::Storage(_) => ciborium::Value::Text("storage failure".to_string()),
        AbiError::State(detail) | AbiError::StateSource { detail, source: _ } => {
            ciborium::Value::Text(detail.to_string())
        }
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
        if ciborium::into_writer(&(1_u8, class, detail), &mut bytes).is_ok()
            && bytes.len() <= MAX_RESPONSE_BYTES
        {
            unsafe { out_error.write(owned_buffer(bytes)) };
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

const fn clear_version_output(output: *mut u64) {
    if !output.is_null() {
        unsafe { output.write(0) };
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

    fn encode_request(value: &impl Serialize) -> Vec<u8> {
        let mut encoded = Vec::new();
        ciborium::into_writer(value, &mut encoded).unwrap();
        encoded
    }

    fn caller_buffer(bytes: &mut [u8]) -> EsBuf {
        EsBuf {
            ptr: bytes.as_mut_ptr(),
            len: bytes.len(),
        }
    }

    fn open_store(store: &mut *mut EsStore) {
        let mut encoded = encode_request(&(1_u8, "sqlite::memory:", 5_000_u64, 1_u32, 1_u32));
        let mut error = empty_buffer();
        let options = caller_buffer(&mut encoded);

        assert_eq!(
            unsafe {
                es_open(
                    &raw const options,
                    std::ptr::from_mut(store),
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert!(!(*store).is_null());
        assert!(error.ptr.is_null());
    }

    fn decode_error(buffer: &EsBuf) -> (u8, i32, Value) {
        let bytes = unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len) };
        ciborium::from_reader(Cursor::new(bytes)).unwrap()
    }

    #[test]
    fn opens_migrates_and_closes_an_in_memory_store() {
        let mut store = ptr::null_mut();
        open_store(&mut store);

        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
        assert!(store.is_null());
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn open_options_accept_the_maximum_runtime_thread_count() {
        let mut encoded = encode_request(&(
            1_u8,
            "sqlite::memory:",
            5_000_u64,
            1_u32,
            MAX_RUNTIME_THREADS,
        ));
        let buffer = caller_buffer(&mut encoded);

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
        let mut encoded = encode_request(&(
            1_u8,
            "sqlite::memory:",
            5_000_u64,
            1_u32,
            MAX_RUNTIME_THREADS + 1,
        ));
        let buffer = caller_buffer(&mut encoded);

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
    fn open_options_reject_each_invalid_product() {
        for options in [
            (2_u8, "sqlite::memory:", 5_000_u64, 1_u32, 1_u32),
            (1_u8, "sqlite::memory:", 5_000_u64, 0_u32, 1_u32),
            (1_u8, "sqlite::memory:", 5_000_u64, 1_u32, 0_u32),
            (1_u8, "sqlite::memory:", 5_000_u64, 2_u32, 1_u32),
        ] {
            let mut encoded = encode_request(&options);
            let buffer = caller_buffer(&mut encoded);

            assert!(matches!(
                decode_open_options(&raw const buffer),
                Err(AbiError::MalformedInput)
            ));
        }
    }

    #[test]
    fn commits_and_loads_opaque_payloads_through_the_engine() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let mut error = empty_buffer();

        let payload = vec![0_u8, 1, 2, 255];
        let mut commit = encode_request(&(
            1_u8,
            "ffi-test",
            "one",
            0_u64,
            vec![("Created", "1.0", OpaqueBytes(payload.clone()))],
        ));
        let commit_buffer = caller_buffer(&mut commit);
        assert_eq!(
            unsafe { es_commit(&raw mut store, &raw const commit_buffer, &raw mut error,) },
            ES_OK
        );
        assert_eq!(
            unsafe { es_commit(&raw mut store, &raw const commit_buffer, &raw mut error,) },
            ES_ERR_CONFLICT
        );
        assert_eq!(
            decode_error(&error),
            (
                1,
                ES_ERR_CONFLICT,
                Value::Array(vec![
                    Value::Text("ffi-test".to_string()),
                    Value::Text("one".to_string()),
                    Value::Integer(0.into()),
                    Value::Integer(1.into()),
                ]),
            )
        );
        unsafe { es_buf_free(&raw mut error) };

        let mut version_request = encode_request(&(1_u8, "ffi-test", "one"));
        let version_buffer = caller_buffer(&mut version_request);
        let mut current_version = 0_u64;
        assert_eq!(
            unsafe {
                es_current_version(
                    &raw mut store,
                    &raw const version_buffer,
                    &raw mut current_version,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(current_version, 1);

        let mut load = encode_request(&(1_u8, "ffi-test", "one", Option::<u64>::None));
        let load_buffer = caller_buffer(&mut load);
        let mut output = empty_buffer();
        assert_eq!(
            unsafe {
                es_load_stream(
                    &raw mut store,
                    &raw const load_buffer,
                    &raw mut output,
                    &raw mut error,
                )
            },
            ES_OK
        );
        let bytes = unsafe { std::slice::from_raw_parts(output.ptr, output.len) };
        let response: StoredEventsWire = ciborium::from_reader(Cursor::new(bytes)).unwrap();
        assert_eq!(
            response,
            (
                1,
                vec![(1, "Created".into(), "1.0".into(), OpaqueBytes(payload))]
            )
        );

        unsafe { es_buf_free(&raw mut output) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn wire_contract_assigns_sequences_and_pages_after_an_exclusive_checkpoint() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let mut error = empty_buffer();
        let mut commit = vec![
            0x85, 0x01, 0x68, b'f', b'f', b'i', b'-', b't', b'e', b's', b't', 0x63, b'o', b'n',
            b'e', 0x00, 0x82, 0x83, 0x67, b'C', b'r', b'e', b'a', b't', b'e', b'd', 0x63, b'1',
            b'.', b'0', 0x44, 0x00, 0x01, 0x02, 0xff, 0x83, 0x67, b'R', b'e', b'n', b'a', b'm',
            b'e', b'd', 0x63, b'1', b'.', b'0', 0x41, 0xff,
        ];
        let commit_buffer = caller_buffer(&mut commit);

        assert_eq!(
            unsafe { es_commit(&raw mut store, &raw const commit_buffer, &raw mut error) },
            ES_OK
        );

        let mut load_after_first = vec![
            0x84, 0x01, 0x68, b'f', b'f', b'i', b'-', b't', b'e', b's', b't', 0x63, b'o', b'n',
            b'e', 0x01,
        ];
        let load_buffer = caller_buffer(&mut load_after_first);
        let mut output = empty_buffer();
        assert_eq!(
            unsafe {
                es_load_stream(
                    &raw mut store,
                    &raw const load_buffer,
                    &raw mut output,
                    &raw mut error,
                )
            },
            ES_OK
        );
        let response = unsafe { std::slice::from_raw_parts(output.ptr, output.len) };
        assert_eq!(
            response,
            [
                0x82, 0x01, 0x81, 0x84, 0x02, 0x67, b'R', b'e', b'n', b'a', b'm', b'e', b'd', 0x63,
                b'1', b'.', b'0', 0x41, 0xff,
            ]
        );

        unsafe { es_buf_free(&raw mut output) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn commit_rejects_sequence_overflow_without_persisting_events() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let mut error = empty_buffer();
        let mut request = encode_request(&(
            1_u8,
            "overflow-test",
            "one",
            u64::MAX,
            vec![("Created", "1.0", OpaqueBytes(Vec::new()))],
        ));
        let request_buffer = caller_buffer(&mut request);

        assert_eq!(
            unsafe { es_commit(&raw mut store, &raw const request_buffer, &raw mut error) },
            ES_ERR_DECODE
        );
        unsafe { es_buf_free(&raw mut error) };

        let mut version_request = encode_request(&(1_u8, "overflow-test", "one"));
        let version_buffer = caller_buffer(&mut version_request);
        let mut version = u64::MAX;
        assert_eq!(
            unsafe {
                es_current_version(
                    &raw mut store,
                    &raw const version_buffer,
                    &raw mut version,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(version, 0);
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn commit_rejects_an_expected_version_ahead_of_the_stream() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let mut error = empty_buffer();
        let mut request = encode_request(&(
            1_u8,
            "expected-version-test",
            "one",
            5_u64,
            vec![("Created", "1.0", OpaqueBytes(Vec::new()))],
        ));
        let request_buffer = caller_buffer(&mut request);

        assert_eq!(
            unsafe { es_commit(&raw mut store, &raw const request_buffer, &raw mut error) },
            ES_ERR_CONFLICT
        );
        assert_eq!(
            decode_error(&error),
            (
                1,
                ES_ERR_CONFLICT,
                Value::Array(vec![
                    Value::Text("expected-version-test".to_string()),
                    Value::Text("one".to_string()),
                    Value::Integer(5.into()),
                    Value::Integer(0.into()),
                ]),
            )
        );
        unsafe { es_buf_free(&raw mut error) };

        let mut version_request = encode_request(&(1_u8, "expected-version-test", "one"));
        let version_buffer = caller_buffer(&mut version_request);
        let mut version = u64::MAX;
        assert_eq!(
            unsafe {
                es_current_version(
                    &raw mut store,
                    &raw const version_buffer,
                    &raw mut version,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(version, 0);
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn request_deserialization_precedes_writes_to_aliased_outputs() {
        let mut options = encode_request(&(1_u8, "sqlite::memory:", 5_000_u64, 1_u32, 1_u32));
        let mut options_buffer = caller_buffer(&mut options);
        let mut store = ptr::null_mut();
        assert_eq!(
            unsafe {
                es_open(
                    &raw const options_buffer,
                    &raw mut store,
                    &raw mut options_buffer,
                )
            },
            ES_OK
        );
        assert!(options_buffer.ptr.is_null());

        let mut commit = encode_request(&(
            1_u8,
            "alias-test",
            "one",
            0_u64,
            vec![("Created", "1.0", OpaqueBytes(vec![1, 2, 3]))],
        ));
        let mut commit_buffer = caller_buffer(&mut commit);
        assert_eq!(
            unsafe {
                es_commit(
                    &raw mut store,
                    &raw const commit_buffer,
                    &raw mut commit_buffer,
                )
            },
            ES_OK
        );
        assert!(commit_buffer.ptr.is_null());

        let mut load = encode_request(&(1_u8, "alias-test", "one", Option::<u64>::None));
        let mut load_buffer = caller_buffer(&mut load);
        let mut error = empty_buffer();
        assert_eq!(
            unsafe {
                es_load_stream(
                    &raw mut store,
                    &raw const load_buffer,
                    &raw mut load_buffer,
                    &raw mut error,
                )
            },
            ES_OK
        );
        unsafe { es_buf_free(&raw mut load_buffer) };

        let mut version_request = encode_request(&(1_u8, "alias-test", "one"));
        let mut version_buffer = caller_buffer(&mut version_request);
        let mut version = 0;
        assert_eq!(
            unsafe {
                es_current_version(
                    &raw mut store,
                    &raw const version_buffer,
                    &raw mut version,
                    &raw mut version_buffer,
                )
            },
            ES_OK
        );
        assert_eq!(version, 1);
        assert!(version_buffer.ptr.is_null());
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn native_metadata_cannot_claim_the_opaque_payload_envelope() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let Ok(lease) = EsStore::acquire(&raw mut store) else {
            panic!("open store must be acquirable");
        };
        let stream = StreamIdentity::new("native-metadata-test", "one");
        let event = SerializedEvent {
            aggregate_type: "native-metadata-test".to_string(),
            aggregate_id: "one".to_string(),
            sequence: 1,
            event_type: "Created".to_string(),
            event_version: "1.0".to_string(),
            payload: serde_json::json!(["native"]),
            metadata: serde_json::json!({ "$event-sorcery-ffi": 1 }),
        };
        lease
            .inner
            .runtime
            .block_on(
                lease
                    .inner
                    .engine
                    .commit(CommitRequest::new(stream, std::slice::from_ref(&event))),
            )
            .unwrap();
        drop(lease);

        let mut request =
            encode_request(&(1_u8, "native-metadata-test", "one", Option::<u64>::None));
        let request_buffer = caller_buffer(&mut request);
        let mut output = empty_buffer();
        let mut error = empty_buffer();
        assert_eq!(
            unsafe {
                es_load_stream(
                    &raw mut store,
                    &raw const request_buffer,
                    &raw mut output,
                    &raw mut error,
                )
            },
            ES_OK
        );
        let bytes = unsafe { std::slice::from_raw_parts(output.ptr, output.len) };
        let response: Value = ciborium::from_reader(Cursor::new(bytes)).unwrap();
        assert_eq!(
            response,
            Value::Array(vec![
                Value::Integer(1.into()),
                Value::Array(vec![Value::Array(vec![
                    Value::Integer(1.into()),
                    Value::Text("Created".to_string()),
                    Value::Text("1.0".to_string()),
                    Value::Bytes(br#"["native"]"#.to_vec()),
                ])]),
            ])
        );

        unsafe { es_buf_free(&raw mut output) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn native_payload_cannot_claim_the_opaque_payload_envelope() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let Ok(lease) = EsStore::acquire(&raw mut store) else {
            panic!("open store must be acquirable");
        };
        let stream = StreamIdentity::new("native-payload-test", "one");
        let payload = serde_json::json!({
            "$event-sorcery-engine": {
                "version": 1,
                "payload": { "opaque_bytes": [1, 2, 3] },
            },
        });
        let expected_payload = serde_json::to_vec(&payload).unwrap();
        let event = SerializedEvent {
            aggregate_type: "native-payload-test".to_string(),
            aggregate_id: "one".to_string(),
            sequence: 1,
            event_type: "Created".to_string(),
            event_version: "1.0".to_string(),
            payload,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        };
        lease
            .inner
            .runtime
            .block_on(
                lease
                    .inner
                    .engine
                    .commit(CommitRequest::new(stream, std::slice::from_ref(&event))),
            )
            .unwrap();
        drop(lease);

        let mut request =
            encode_request(&(1_u8, "native-payload-test", "one", Option::<u64>::None));
        let request_buffer = caller_buffer(&mut request);
        let mut output = empty_buffer();
        let mut error = empty_buffer();
        assert_eq!(
            unsafe {
                es_load_stream(
                    &raw mut store,
                    &raw const request_buffer,
                    &raw mut output,
                    &raw mut error,
                )
            },
            ES_OK
        );
        let bytes = unsafe { std::slice::from_raw_parts(output.ptr, output.len) };
        let response: StoredEventsWire = ciborium::from_reader(Cursor::new(bytes)).unwrap();
        assert_eq!(
            response,
            (
                1,
                vec![(
                    1,
                    "Created".to_string(),
                    "1.0".to_string(),
                    OpaqueBytes(expected_payload),
                )],
            )
        );

        unsafe { es_buf_free(&raw mut output) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn open_clears_outputs_and_enforces_canonical_request_limits() {
        let mut oversized = vec![0_u8; MAX_REQUEST_BYTES + 1];
        let options = caller_buffer(&mut oversized);
        let mut store = std::ptr::NonNull::<EsStore>::dangling().as_ptr();
        let mut error = EsBuf {
            ptr: std::ptr::NonNull::<u8>::dangling().as_ptr(),
            len: 0,
        };

        assert_eq!(
            unsafe { es_open(&raw const options, &raw mut store, &raw mut error) },
            ES_ERR_RESOURCE_LIMIT
        );
        assert!(store.is_null());
        assert_eq!(
            decode_error(&error),
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
        unsafe { es_buf_free(&raw mut error) };

        let mut noncanonical = encode_request(&(1_u8, "sqlite::memory:", 5_000_u64, 1_u32, 1_u32));
        assert_eq!(noncanonical[0], 0x85);
        noncanonical[0] = 0x9f;
        noncanonical.push(0xff);
        let options = caller_buffer(&mut noncanonical);
        assert_eq!(
            unsafe { es_open(&raw const options, &raw mut store, &raw mut error) },
            ES_ERR_DECODE
        );
        assert!(store.is_null());
        unsafe { es_buf_free(&raw mut error) };
    }

    #[test]
    fn decoder_rejects_values_beyond_the_nesting_limit() {
        let nested = (0..=MAX_CBOR_DEPTH).fold(Value::Null, |value, _| Value::Array(vec![value]));
        let mut encoded = encode_request(&nested);
        let buffer = caller_buffer(&mut encoded);

        assert!(matches!(
            decode::<Value>(&raw const buffer),
            Err(AbiError::Decode(_))
        ));
    }

    #[test]
    fn commit_rejects_event_and_payload_limits_before_writing() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let mut error = empty_buffer();
        let events = (0..=MAX_COMMIT_EVENTS)
            .map(|_| ("Created", "1.0", OpaqueBytes(Vec::new())))
            .collect::<Vec<_>>();
        let mut request = encode_request(&(1_u8, "limit-test", "events", 0_u64, events));
        let decoded: CommitWire = ciborium::from_reader(Cursor::new(&request)).unwrap();
        assert_eq!(decoded.4.values.len(), MAX_COMMIT_EVENTS);
        assert_eq!(decoded.4.observed, MAX_COMMIT_EVENTS + 1);
        let request_buffer = caller_buffer(&mut request);

        assert_eq!(
            unsafe { es_commit(&raw mut store, &raw const request_buffer, &raw mut error,) },
            ES_ERR_RESOURCE_LIMIT
        );
        unsafe { es_buf_free(&raw mut error) };

        let oversized_payload = vec![0_u8; MAX_PAYLOAD_BYTES + 1];
        let mut request = encode_request(&(
            1_u8,
            "limit-test",
            "payload",
            0_u64,
            vec![("Created", "1.0", OpaqueBytes(oversized_payload))],
        ));
        let request_buffer = caller_buffer(&mut request);
        assert_eq!(
            unsafe { es_commit(&raw mut store, &raw const request_buffer, &raw mut error,) },
            ES_ERR_RESOURCE_LIMIT
        );
        unsafe { es_buf_free(&raw mut error) };

        let mut version_request = encode_request(&(1_u8, "limit-test", "events"));
        let version_buffer = caller_buffer(&mut version_request);
        let mut version = u64::MAX;
        assert_eq!(
            unsafe {
                es_current_version(
                    &raw mut store,
                    &raw const version_buffer,
                    &raw mut version,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(version, 0);
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn failed_calls_clear_stale_outputs() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let mut malformed = [0xff];
        let request = caller_buffer(&mut malformed);
        let mut output = EsBuf {
            ptr: std::ptr::NonNull::<u8>::dangling().as_ptr(),
            len: 0,
        };
        let mut version = u64::MAX;
        let mut error = empty_buffer();

        assert_eq!(
            unsafe {
                es_load_stream(
                    &raw mut store,
                    &raw const request,
                    &raw mut output,
                    &raw mut error,
                )
            },
            ES_ERR_DECODE
        );
        assert!(output.ptr.is_null());
        unsafe { es_buf_free(&raw mut error) };
        assert_eq!(
            unsafe {
                es_current_version(
                    &raw mut store,
                    &raw const request,
                    &raw mut version,
                    &raw mut error,
                )
            },
            ES_ERR_DECODE
        );
        assert_eq!(version, 0);
        unsafe { es_buf_free(&raw mut error) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);

        output = EsBuf {
            ptr: std::ptr::NonNull::<u8>::dangling().as_ptr(),
            len: 0,
        };
        version = u64::MAX;
        assert_eq!(
            unsafe {
                es_load_stream(
                    &raw mut store,
                    &raw const request,
                    &raw mut output,
                    &raw mut error,
                )
            },
            ES_ERR_STATE
        );
        assert!(output.ptr.is_null());
        unsafe { es_buf_free(&raw mut error) };
        assert_eq!(
            unsafe {
                es_current_version(
                    &raw mut store,
                    &raw const request,
                    &raw mut version,
                    &raw mut error,
                )
            },
            ES_ERR_STATE
        );
        assert_eq!(version, 0);
        unsafe { es_buf_free(&raw mut error) };

        open_store(&mut store);
        let Ok(lease) = EsStore::acquire(&raw mut store) else {
            panic!("open store must be acquirable");
        };
        lease.store.poisoned.store(true, Ordering::Release);
        drop(lease);
        output = EsBuf {
            ptr: std::ptr::NonNull::<u8>::dangling().as_ptr(),
            len: 0,
        };
        version = u64::MAX;
        assert_eq!(
            unsafe {
                es_load_stream(
                    &raw mut store,
                    &raw const request,
                    &raw mut output,
                    &raw mut error,
                )
            },
            ES_ERR_STATE
        );
        assert!(output.ptr.is_null());
        unsafe { es_buf_free(&raw mut error) };
        assert_eq!(
            unsafe {
                es_current_version(
                    &raw mut store,
                    &raw const request,
                    &raw mut version,
                    &raw mut error,
                )
            },
            ES_ERR_STATE
        );
        assert_eq!(version, 0);
        unsafe { es_buf_free(&raw mut error) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn stream_load_rejects_a_response_beyond_the_item_limit() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let Ok(lease) = EsStore::acquire(&raw mut store) else {
            panic!("open store must be acquirable");
        };
        let stream = StreamIdentity::new("list-limit-test", "one");
        let events = (1..=MAX_LIST_ITEMS + 1)
            .map(|sequence| SerializedEvent {
                aggregate_type: "list-limit-test".to_string(),
                aggregate_id: "one".to_string(),
                sequence,
                event_type: "Created".to_string(),
                event_version: "1.0".to_string(),
                payload: serde_json::Value::Array(Vec::new()),
                metadata: serde_json::json!({ "$event-sorcery-ffi": 1 }),
            })
            .collect::<Vec<_>>();
        lease
            .inner
            .runtime
            .block_on(
                lease
                    .inner
                    .engine
                    .commit(CommitRequest::new(stream, &events)),
            )
            .unwrap();
        drop(lease);

        let mut request = encode_request(&(1_u8, "list-limit-test", "one", Option::<u64>::None));
        let request_buffer = caller_buffer(&mut request);
        let mut output = empty_buffer();
        let mut error = empty_buffer();
        assert_eq!(
            unsafe {
                es_load_stream(
                    &raw mut store,
                    &raw const request_buffer,
                    &raw mut output,
                    &raw mut error,
                )
            },
            ES_ERR_RESOURCE_LIMIT
        );
        assert!(output.ptr.is_null());
        assert_eq!(
            decode_error(&error),
            (
                1,
                ES_ERR_RESOURCE_LIMIT,
                Value::Array(vec![
                    Value::Text("list_items".to_string()),
                    Value::Integer((MAX_LIST_ITEMS + 1).into()),
                    Value::Integer(MAX_LIST_ITEMS.into()),
                ]),
            )
        );
        unsafe { es_buf_free(&raw mut error) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn response_encoder_rejects_buffers_beyond_the_byte_limit() {
        let events = (1..=65)
            .map(|sequence| LoadedEvent {
                sequence,
                event_type: "Created".to_string(),
                event_version: "1.0".to_string(),
                payload: LoadedPayload::Json(serde_json::Value::String(
                    "x".repeat(MAX_PAYLOAD_BYTES - 2),
                )),
            })
            .collect::<Vec<_>>();

        let result = encode_event_page(events);

        assert!(matches!(
            result,
            Err(AbiError::ResourceLimit {
                resource: "encoded_response_buffer",
                observed,
                limit: MAX_RESPONSE_BYTES,
            }) if observed > MAX_RESPONSE_BYTES
        ));
    }

    #[test]
    fn errors_are_stable_and_redacted() {
        let mut error = empty_buffer();
        let storage_error = AbiError::storage(std::io::Error::other("sensitive storage detail"));
        assert_eq!(
            std::error::Error::source(&storage_error).map(std::string::ToString::to_string),
            Some("sensitive storage detail".to_string())
        );

        assert_eq!(write_error(&raw mut error, storage_error), ES_ERR_STORAGE);
        assert_eq!(
            decode_error(&error),
            (
                1,
                ES_ERR_STORAGE,
                Value::Text("storage failure".to_string()),
            )
        );
        unsafe { es_buf_free(&raw mut error) };
        assert_eq!(write_error(&raw mut error, AbiError::Panic), ES_ERR_PANIC);
        assert_eq!(decode_error(&error), (1, ES_ERR_PANIC, Value::Null));
        unsafe { es_buf_free(&raw mut error) };
    }

    #[test]
    fn close_waits_for_acquired_calls_and_concurrent_closers() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let Ok(lease) = EsStore::acquire(&raw mut store) else {
            panic!("open store must be acquirable");
        };
        let owner_address = (&raw mut store) as usize;
        let (closed_tx, closed_rx) = std::sync::mpsc::channel();

        std::thread::scope(|scope| {
            let first_tx = closed_tx.clone();
            scope.spawn(move || {
                let result = unsafe { es_close(owner_address as *mut *mut EsStore) };
                first_tx.send(result).unwrap();
            });
            lease.wait_until_closing();
            scope.spawn(move || {
                let result = unsafe { es_close(owner_address as *mut *mut EsStore) };
                closed_tx.send(result).unwrap();
            });

            assert!(matches!(
                closed_rx.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ));
            drop(lease);
            assert_eq!(
                closed_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
                ES_OK
            );
            assert_eq!(
                closed_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
                ES_OK
            );
        });

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

        unsafe { es_buf_free(&raw mut buffer) };
        assert!(buffer.ptr.is_null());
        assert_eq!(buffer.len, 0);
        unsafe { es_buf_free(&raw mut buffer) };
    }
}
