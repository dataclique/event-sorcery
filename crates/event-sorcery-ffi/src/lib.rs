use std::fmt;
use std::io::Cursor;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use cqrs_es::persist::SerializedEvent;
use event_sorcery::{CommitRequest, Engine, EngineError, StreamIdentity};
use serde::de::{DeserializeOwned, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};

const ABI_MAJOR: u32 = 0;
const ABI_MINOR: u32 = 2;
const ES_OK: i32 = 0;
const ES_ERR_DECODE: i32 = 1;
const ES_ERR_CONFLICT: i32 = 2;
const ES_ERR_STORAGE: i32 = 4;
const ES_ERR_STATE: i32 = 5;
const ES_ERR_PANIC: i32 = 100;

type ProposedEventWire = (String, String, OpaqueBytes);
type CommitWire = (u8, String, String, u64, Vec<ProposedEventWire>);
type StoredEventWire = (u64, String, String, OpaqueBytes);
type StoredEventsWire = (u8, Vec<StoredEventWire>);

#[derive(Debug, PartialEq, Eq)]
struct OpaqueBytes(Vec<u8>);

impl Serialize for OpaqueBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
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
    runtime: tokio::runtime::Runtime,
    engine: Engine,
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
            runtime,
            engine,
            poisoned: AtomicBool::new(false),
        });
        unsafe { out_store.write(Box::into_raw(store)) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// Loads a stream through the shared engine.
///
/// # Safety
///
/// `store` must be an open handle. `request` must reference readable bytes.
/// `out_events` must be writable, and `out_error` must be null or writable.
pub unsafe extern "C" fn es_load_stream(
    store: *mut EsStore,
    request: *const EsBuf,
    out_events: *mut EsBuf,
    out_error: *mut EsBuf,
) -> i32 {
    let Some(store) = (unsafe { store.as_ref() }) else {
        return write_error(out_error, AbiError::State("store is null".to_string()));
    };
    ffi_call(Some(store), out_error, || {
        if out_events.is_null() {
            return Err(AbiError::State("out_events is null".to_string()));
        }
        let (version, aggregate_type, aggregate_id, after): (u8, String, String, Option<u64>) =
            decode(request)?;
        require_version(version)?;
        let after = after
            .map(usize::try_from)
            .transpose()
            .map_err(|error| AbiError::Decode(error.to_string()))?;
        let stream = StreamIdentity::new(aggregate_type, aggregate_id);
        let events = store
            .runtime
            .block_on(store.engine.load_events(&stream, after))
            .map_err(AbiError::from)?;
        let events = events
            .into_iter()
            .map(event_to_wire)
            .collect::<Result<Vec<_>, _>>()?;
        let response: StoredEventsWire = (1, events);
        unsafe { out_events.write(encode(&response)?) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// Reads the current stream version, where zero means no events.
///
/// # Safety
///
/// `store` must be an open handle. `request` must reference readable bytes.
/// `out_version` must be writable, and `out_error` must be null or writable.
pub unsafe extern "C" fn es_current_version(
    store: *mut EsStore,
    request: *const EsBuf,
    out_version: *mut u64,
    out_error: *mut EsBuf,
) -> i32 {
    let Some(store) = (unsafe { store.as_ref() }) else {
        return write_error(out_error, AbiError::State("store is null".to_string()));
    };
    ffi_call(Some(store), out_error, || {
        if out_version.is_null() {
            return Err(AbiError::State("out_version is null".to_string()));
        }
        let (version, aggregate_type, aggregate_id): (u8, String, String) = decode(request)?;
        require_version(version)?;
        let stream = StreamIdentity::new(aggregate_type, aggregate_id);
        let events = store
            .runtime
            .block_on(store.engine.load_events(&stream, None))
            .map_err(AbiError::from)?;
        let version = events.last().map_or(Ok(0), |event| {
            u64::try_from(event.sequence).map_err(|error| AbiError::Storage(error.to_string()))
        })?;
        unsafe { out_version.write(version) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// Atomically appends events at the requested expected version.
///
/// # Safety
///
/// `store` must be an open handle. `request` must reference readable bytes.
/// `out_error` must be null or writable.
pub unsafe extern "C" fn es_commit(
    store: *mut EsStore,
    request: *const EsBuf,
    out_error: *mut EsBuf,
) -> i32 {
    let Some(store) = (unsafe { store.as_ref() }) else {
        return write_error(out_error, AbiError::State("store is null".to_string()));
    };
    ffi_call(Some(store), out_error, || {
        let (version, aggregate_type, aggregate_id, expected, events): CommitWire =
            decode(request)?;
        require_version(version)?;
        if events.is_empty() {
            return Err(AbiError::Decode("commit contains no events".to_string()));
        }
        let expected =
            usize::try_from(expected).map_err(|error| AbiError::Decode(error.to_string()))?;
        let serialized = events
            .into_iter()
            .enumerate()
            .map(|(index, (event_type, event_version, payload))| {
                let sequence = expected
                    .checked_add(index)
                    .and_then(|sequence| sequence.checked_add(1))
                    .ok_or_else(|| AbiError::Decode("event sequence overflow".to_string()))?;
                Ok(SerializedEvent {
                    aggregate_type: aggregate_type.clone(),
                    aggregate_id: aggregate_id.clone(),
                    sequence,
                    event_type,
                    event_version,
                    payload: serde_json::Value::Array(
                        payload.0.into_iter().map(Into::into).collect(),
                    ),
                    metadata: serde_json::json!({ "$event-sorcery-ffi": 1 }),
                })
            })
            .collect::<Result<Vec<_>, AbiError>>()?;
        let stream = StreamIdentity::new(aggregate_type, aggregate_id);
        store
            .runtime
            .block_on(store.engine.commit(CommitRequest::new(stream, &serialized)))
            .map_err(AbiError::from)
    })
}

#[unsafe(no_mangle)]
/// Closes a store returned by [`es_open`]. A null pointer or null handle is a no-op.
///
/// # Safety
///
/// A non-null handle must have been returned by [`es_open`].
pub unsafe extern "C" fn es_close(store: *mut *mut EsStore) -> i32 {
    let Some(store) = (unsafe { store.as_mut() }) else {
        return ES_OK;
    };
    if store.is_null() {
        return ES_OK;
    }
    let owned = std::mem::replace(store, ptr::null_mut());
    match catch_unwind(AssertUnwindSafe(|| unsafe { drop(Box::from_raw(owned)) })) {
        Ok(()) => ES_OK,
        Err(_) => ES_ERR_PANIC,
    }
}

#[unsafe(no_mangle)]
/// Releases an engine-owned output buffer. A null pointer or null buffer is a no-op.
///
/// # Safety
///
/// A non-null buffer must have been returned by this library.
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

enum AbiError {
    Decode(String),
    Conflict,
    Storage(String),
    State(String),
    Panic,
}

impl From<EngineError> for AbiError {
    fn from(error: EngineError) -> Self {
        match error {
            EngineError::OptimisticLock => Self::Conflict,
            other => Self::Storage(other.to_string()),
        }
    }
}

fn decode<T: DeserializeOwned>(buffer: *const EsBuf) -> Result<T, AbiError> {
    let Some(buffer) = (unsafe { buffer.as_ref() }) else {
        return Err(AbiError::Decode("input buffer is null".to_string()));
    };
    if buffer.ptr.is_null() {
        return Err(AbiError::Decode("input buffer is null".to_string()));
    }
    let bytes = unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len) };
    ciborium::from_reader(Cursor::new(bytes)).map_err(|error| AbiError::Decode(error.to_string()))
}

fn encode(value: &impl Serialize) -> Result<EsBuf, AbiError> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes).map_err(|error| AbiError::State(error.to_string()))?;
    Ok(owned_buffer(bytes))
}

fn owned_buffer(bytes: Vec<u8>) -> EsBuf {
    let boxed = bytes.into_boxed_slice();
    EsBuf {
        len: boxed.len(),
        ptr: Box::into_raw(boxed).cast::<u8>(),
    }
}

fn require_version(version: u8) -> Result<(), AbiError> {
    if version == 1 {
        Ok(())
    } else {
        Err(AbiError::Decode("unsupported format version".to_string()))
    }
}

fn event_to_wire(event: SerializedEvent) -> Result<StoredEventWire, AbiError> {
    let payload = if event.metadata.get("$event-sorcery-ffi") == Some(&serde_json::json!(1)) {
        let serde_json::Value::Array(bytes) = event.payload else {
            return Err(AbiError::Storage("invalid FFI event payload".to_string()));
        };
        bytes
            .into_iter()
            .map(|byte| {
                byte.as_u64()
                    .and_then(|byte| u8::try_from(byte).ok())
                    .ok_or_else(|| AbiError::Storage("invalid FFI event byte".to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        serde_json::to_vec(&event.payload).map_err(|error| AbiError::Storage(error.to_string()))?
    };
    Ok((
        u64::try_from(event.sequence).map_err(|error| AbiError::Storage(error.to_string()))?,
        event.event_type,
        event.event_version,
        OpaqueBytes(payload),
    ))
}

fn decode_open_options(buffer: *const EsBuf) -> Result<OpenOptions, AbiError> {
    let (version, path, busy_timeout_ms, pool_size, runtime_threads): (
        u8,
        String,
        u64,
        u32,
        usize,
    ) = decode(buffer)?;
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
        AbiError::Conflict => (ES_ERR_CONFLICT, "optimistic conflict".to_string()),
        AbiError::Storage(detail) => (ES_ERR_STORAGE, detail),
        AbiError::State(detail) => (ES_ERR_STATE, detail),
        AbiError::Panic => (ES_ERR_PANIC, "engine panic".to_string()),
    };
    if !out_error.is_null() {
        let mut bytes = Vec::new();
        if ciborium::into_writer(&(1_u8, class, detail), &mut bytes).is_ok() {
            unsafe { out_error.write(owned_buffer(bytes)) };
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
            unsafe { es_open(&raw const options, &raw mut store, &raw mut error) },
            ES_OK
        );
        assert!(!store.is_null());
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
        assert!(store.is_null());
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
        assert!(error.ptr.is_null());
    }

    #[test]
    fn commits_and_loads_opaque_payloads_through_the_engine() {
        let mut options = Vec::new();
        ciborium::into_writer(
            &(1_u8, "sqlite::memory:", 5_000_u64, 1_u32, 1_usize),
            &mut options,
        )
        .unwrap();
        let mut store = ptr::null_mut();
        let mut error = EsBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        let options = EsBuf {
            ptr: options.as_mut_ptr(),
            len: options.len(),
        };
        assert_eq!(
            unsafe { es_open(&raw const options, &raw mut store, &raw mut error,) },
            ES_OK
        );

        let payload = vec![0_u8, 1, 2, 255];
        let mut commit = Vec::new();
        ciborium::into_writer(
            &(
                1_u8,
                "ffi-test",
                "one",
                0_u64,
                vec![("Created", "1.0", OpaqueBytes(payload.clone()))],
            ),
            &mut commit,
        )
        .unwrap();
        let commit = EsBuf {
            ptr: commit.as_mut_ptr(),
            len: commit.len(),
        };
        assert_eq!(
            unsafe { es_commit(store, &raw const commit, &raw mut error) },
            ES_OK
        );
        assert_eq!(
            unsafe { es_commit(store, &raw const commit, &raw mut error) },
            ES_ERR_CONFLICT
        );
        unsafe { es_buf_free(&raw mut error) };
        assert!(error.ptr.is_null());
        assert_eq!(error.len, 0);
        unsafe { es_buf_free(&raw mut error) };

        let mut version_request = Vec::new();
        ciborium::into_writer(&(1_u8, "ffi-test", "one"), &mut version_request).unwrap();
        let version_request = EsBuf {
            ptr: version_request.as_mut_ptr(),
            len: version_request.len(),
        };
        let mut current_version = 0_u64;
        assert_eq!(
            unsafe {
                es_current_version(
                    store,
                    &raw const version_request,
                    &raw mut current_version,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(current_version, 1);

        let mut load = Vec::new();
        ciborium::into_writer(&(1_u8, "ffi-test", "one", Option::<u64>::None), &mut load).unwrap();
        let load = EsBuf {
            ptr: load.as_mut_ptr(),
            len: load.len(),
        };
        let mut output = EsBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        assert_eq!(
            unsafe { es_load_stream(store, &raw const load, &raw mut output, &raw mut error,) },
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
        assert!(output.ptr.is_null());
        assert_eq!(output.len, 0);
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
        assert!(store.is_null());
    }
}
