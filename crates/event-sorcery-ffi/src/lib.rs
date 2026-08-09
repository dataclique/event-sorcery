//! Stable C ABI over the shared event-sorcery engine.

use cqrs_es::persist::SerializedEvent;
use serde::de::{DeserializeOwned, Error as DeserializeError, IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use std::collections::HashMap;
use std::fmt;
use std::io::{self, Cursor, Write};
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use event_sorcery::{
    CommitRequest, DeadReason, Engine, EngineError, JobClaimHandle, JobClaimResult, JobId,
    JobLeaseResult, JobRuntime, JobRuntimeError, JobSeed, JobSettlementResult, JobStoreError,
    LoadedEvent, LoadedPayload, StreamIdentity,
};

const ABI_MAJOR: u32 = 0;
const ABI_MINOR: u32 = 3;
const ES_OK: i32 = 0;
const ES_ERR_DECODE: i32 = 1;
const ES_ERR_CONFLICT: i32 = 2;
const ES_ERR_JOB_REFUSAL: i32 = 3;
const ES_ERR_STORAGE: i32 = 4;
const ES_ERR_STATE: i32 = 5;
const ES_ERR_RESOURCE_LIMIT: i32 = 6;
const ES_ERR_PANIC: i32 = 100;
const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
/// CBOR framing per event that the engine's stored-byte accounting cannot see:
/// the item array header, the sequence integer, and the three field headers.
const EVENT_PAGE_FRAMING_BYTES: usize = 32;
/// Stored-byte budget for one loaded page.
///
/// Every field the response carries is also counted in the engine's stored
/// total, and the stored total additionally counts the stream identity and the
/// metadata the response omits. Reserving the framing allowance above therefore
/// guarantees that a page which fits this budget also encodes within
/// [`MAX_RESPONSE_BYTES`], so the engine's byte-boundary truncation - not the
/// response encoder - is what bounds a page.
const MAX_STORED_EVENT_PAGE_BYTES: usize =
    MAX_RESPONSE_BYTES - (MAX_LIST_ITEMS * EVENT_PAGE_FRAMING_BYTES) - EVENT_PAGE_FRAMING_BYTES;
const MAX_CBOR_DEPTH: usize = 32;
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const MAX_COMMIT_EVENTS: usize = 1024;
const MAX_LIST_ITEMS: usize = 4096;
const MAX_ACTIVE_CLAIMS: usize = MAX_LIST_ITEMS;
const MAX_ERROR_TEXT_BYTES: usize = 4 * 1024;
/// Bound on every caller-supplied identifier the ABI stores or matches on:
/// aggregate type and id, job id, job kind, and worker name.
const MAX_IDENTIFIER_BYTES: usize = 4 * 1024;
/// Maximum worker threads accepted from the fixed-width C ABI options product.
const MAX_RUNTIME_THREADS: u32 = 256;
/// Largest busy timeout the SQLite driver accepts.
///
/// `sqlx_sqlite` converts the configured duration with `i32::try_from(..)` and
/// panics above that bound on the connection's own worker thread, where the
/// unwind escapes [`ffi_call`]'s `catch_unwind` and surfaces to the caller as a
/// retryable storage failure. A timeout past the bound is refused up front.
const MAX_BUSY_TIMEOUT_MS: u32 = i32::MAX.unsigned_abs();
/// CBOR major type 4 (array), with the item count in the argument bits.
const CBOR_ARRAY_HEADER: u8 = 0x80;
/// [`CBOR_ARRAY_HEADER`] whose item count is the single byte that follows it.
const CBOR_ARRAY_HEADER_U8_COUNT: u8 = 0x98;
/// [`CBOR_ARRAY_HEADER`] whose item count is the big-endian `u16` following it.
const CBOR_ARRAY_HEADER_U16_COUNT: u8 = 0x99;
/// Format version carried as the first item of every framework response.
const RESPONSE_VERSION: u8 = 1;
static STORE_REGISTRY: OnceLock<Mutex<HashMap<usize, StoreEntry>>> = OnceLock::new();

/// One event a commit request proposes: `[event_type, event_version, payload]`.
#[derive(Debug, Deserialize, Serialize)]
struct ProposedEventWire(String, String, OpaqueBytes);

/// `[version, aggregate_type, aggregate_id, expected_version, events]`.
#[derive(Deserialize, Serialize)]
struct CommitWire(u8, String, String, u64, CommitEvents);

/// The durable job intent a commit carries: `[job_id, kind, payload, run_at_ms]`.
#[derive(Deserialize, Serialize)]
struct JobSeedWire(String, String, OpaqueBytes, i64);

/// [`CommitWire`] extended with the job intent committed in the same transaction.
#[derive(Deserialize, Serialize)]
struct CommitWithJobWire(u8, String, String, u64, CommitEvents, JobSeedWire);

/// One stored event in a page response: `[sequence, event_type, event_version,
/// payload]`.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredEventWire(u64, String, String, OpaqueBytes);

/// A whole page response: `[response_version, events]`.
#[cfg(test)]
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct StoredEventsWire(u8, Vec<StoredEventWire>);

/// A claim outcome: `[response_version, outcome_tag, token, attempt,
/// has_prior_execution, payload]`, where every option is present only for a won
/// claim.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct JobClaimWire(
    u8,
    u8,
    Option<OpaqueBytes>,
    Option<u32>,
    Option<u8>,
    Option<OpaqueBytes>,
);

/// `[version, claim_token]`.
#[derive(Deserialize, Serialize)]
struct JobAckWire(u8, OpaqueBytes);

/// `[version, claim_token, run_at_ms, error]`.
#[derive(Deserialize, Serialize)]
struct JobRetryWire(u8, OpaqueBytes, i64, String);

/// `[version, claim_token, run_at_ms]`.
#[derive(Deserialize, Serialize)]
struct JobDeferWire(u8, OpaqueBytes, i64);

/// `[version, claim_token, reason_tag, error]`.
#[derive(Deserialize, Serialize)]
struct JobDeadLetterWire(u8, OpaqueBytes, u8, String);

type ClaimToken = [u8; 16];

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
                // SAFETY: the caller's contract makes a non-null `out_store` a
                // writable cell for the duration of the call, and the options
                // buffer was already decoded, so a decode that aliases this
                // cell has finished reading before the write lands.
                unsafe { out_store.write(ptr::null_mut()) };
            }
            if out_store.is_null() {
                return Err(AbiError::State(StateFault::NullOutput(OutputSlot::Store)));
            }
            let options = options?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(options.runtime_threads)
                .enable_all()
                .build()
                .map_err(|error| {
                    AbiError::state_source(
                        StateFault::Lifecycle(StoreLifecycle::RuntimeInitialization),
                        error,
                    )
                })?;
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
            let engine = Engine::new(pool.clone());
            runtime
                .block_on(engine.migrate())
                .map_err(AbiError::storage)?;
            let jobs = runtime
                .block_on(JobRuntime::build(pool))
                .map_err(AbiError::storage)?;
            let store = Arc::new(EsStore {
                state: Mutex::new(StoreState::Open { active_calls: 0 }),
                state_changed: Condvar::new(),
                inner: Mutex::new(Some(Arc::new(StoreInner {
                    runtime,
                    engine,
                    jobs,
                    claims: Mutex::new(ClaimRegistry::default()),
                }))),
                poisoned: AtomicBool::new(false),
            });
            publish_store(out_store, store)
        },
    )
}

#[unsafe(no_mangle)]
/// Loads one page of a stream through the shared engine.
///
/// The page is bounded by [`MAX_LIST_ITEMS`] events and by the stored-byte
/// budget the engine truncates on, so a stream of any length is read by
/// repeating the call with `after` set to the last returned sequence. A page
/// shorter than the item bound is not by itself proof that the stream ended;
/// the stream ends at the first empty page.
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
                return Err(AbiError::State(StateFault::NullOutput(OutputSlot::Events)));
            }
            let (version, aggregate_type, aggregate_id, after) = request?;
            require_version(version)?;
            let after = after
                .map(usize::try_from)
                .transpose()
                .map_err(|_| AbiError::MalformedInput)?;
            let stream = StreamIdentity::new(aggregate_type, aggregate_id);
            let query_limit = NonZeroUsize::new(MAX_LIST_ITEMS)
                .ok_or(AbiError::State(StateFault::Page(PageFault::ListQueryLimit)))?;
            let events = lease
                .inner
                .runtime
                .block_on(lease.inner.engine.load_events_page_bounded(
                    &stream,
                    after,
                    query_limit,
                    NonZeroUsize::new(MAX_STORED_EVENT_PAGE_BYTES).ok_or(AbiError::State(
                        StateFault::Page(PageFault::StoredPageByteLimit),
                    ))?,
                ))
                .map_err(AbiError::from)?;
            // SAFETY: `out_events` was null-checked above and the caller's
            // contract makes it writable and distinct from `out_error`. The
            // buffer it receives is freshly leaked by `encode_event_page`, so
            // the cell's previous contents are not aliased by this write.
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
                return Err(AbiError::State(StateFault::NullOutput(OutputSlot::Version)));
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
            // SAFETY: `out_version` was null-checked above and the caller's
            // contract makes it a writable `u64` cell distinct from
            // `out_error`.
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
            let CommitWire(version, aggregate_type, aggregate_id, expected, events) =
                decode_validated(request, |CommitWire(_, _, _, _, events): &CommitWire| {
                    require_commit_event_count(events.observed)
                })?;
            let (stream, serialized) =
                commit_parts(version, aggregate_type, aggregate_id, expected, events)?;
            commit_result(&lease.inner, &stream, &serialized, expected, None)
        },
    )
}

#[unsafe(no_mangle)]
/// Atomically appends domain events and one durable job intent.
///
/// # Safety
///
/// `store` must be the original owner cell passed to [`es_open`]. `request`
/// must reference a readable buffer. `out_error` must be null or writable.
pub unsafe extern "C" fn es_commit_with_job(
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
            let CommitWithJobWire(version, aggregate_type, aggregate_id, expected, events, job) =
                decode_validated(
                    request,
                    |CommitWithJobWire(_, _, _, _, events, _): &CommitWithJobWire| {
                        require_commit_event_count(events.observed)
                    },
                )?;
            let (stream, serialized) =
                commit_parts(version, aggregate_type, aggregate_id, expected, events)?;
            let JobSeedWire(job_id, kind, OpaqueBytes(payload), run_at_ms) = job;
            require_identifier_limit(job_id.len())?;
            require_identifier_limit(kind.len())?;
            require_payload_limit(payload.len())?;
            let job_id = job_id.parse::<JobId>().map_err(AbiError::decode)?;
            let payload = serde_json::to_value(payload).map_err(AbiError::storage)?;
            commit_result(
                &lease.inner,
                &stream,
                &serialized,
                expected,
                Some(JobSeed::new(job_id, kind, payload, run_at_ms)),
            )
        },
    )
}

#[unsafe(no_mangle)]
/// Enqueues an erased payload through the retained event-sourced job runtime.
///
/// # Safety
///
/// `store` must be the original owner cell passed to [`es_open`]. `request`
/// must reference a readable buffer. `out_error` must be null or writable.
pub unsafe extern "C" fn es_job_enqueue(
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
            let (version, job_id, kind, OpaqueBytes(payload), run_at_ms): (
                u8,
                String,
                String,
                OpaqueBytes,
                i64,
            ) = decode(request)?;
            require_version(version)?;
            require_identifier_limit(job_id.len())?;
            require_identifier_limit(kind.len())?;
            require_payload_limit(payload.len())?;
            let job_id = job_id.parse::<JobId>().map_err(AbiError::decode)?;
            let payload = serde_json::to_value(payload).map_err(AbiError::storage)?;
            lease
                .inner
                .runtime
                .block_on(
                    lease
                        .inner
                        .jobs
                        .enqueue_job_payload(job_id, kind, payload, run_at_ms),
                )
                .map_err(|error| job_runtime_error(job_id, error))
        },
    )
}

#[unsafe(no_mangle)]
/// Polls the rebuildable job queue projection.
///
/// # Safety
///
/// `store` must be the original owner cell passed to [`es_open`]. `request`
/// must reference a readable buffer. `out_jobs` must be writable and
/// `out_error` must be null or writable. The request buffer may alias either
/// output, but `out_jobs` and `out_error` must be distinct.
pub unsafe extern "C" fn es_job_poll(
    store: *mut *mut EsStore,
    request: *const EsBuf,
    out_jobs: *mut EsBuf,
    out_error: *mut EsBuf,
) -> i32 {
    let lease = match EsStore::acquire(store) {
        Ok(lease) => lease,
        Err(error) => {
            clear_buffer_output(out_jobs);
            return write_error(out_error, error);
        }
    };
    ffi_call(
        Some(&lease.store),
        out_error,
        || clear_buffer_output(out_jobs),
        || {
            let decoded: (u8, String, i64, u32) = decode(request)?;
            clear_buffer_output(out_jobs);
            if out_jobs.is_null() {
                return Err(AbiError::State(StateFault::NullOutput(OutputSlot::Jobs)));
            }
            let (version, kind, now_ms, limit) = decoded;
            require_version(version)?;
            require_identifier_limit(kind.len())?;
            let observed = usize::try_from(limit).map_err(AbiError::storage)?;
            if observed > MAX_LIST_ITEMS {
                return Err(AbiError::ResourceLimit {
                    resource: "job_poll_limit",
                    observed,
                    limit: MAX_LIST_ITEMS,
                });
            }
            // A poll names no single job, so it has no identity to attribute a
            // refusal to. It also cannot produce one: the queue projection is
            // the only thing it consults, and a projection failure is
            // infrastructure.
            let jobs = lease
                .inner
                .runtime
                .block_on(lease.inner.jobs.poll_jobs(&kind, now_ms, limit))
                .map_err(AbiError::storage)?;
            // SAFETY: `out_jobs` was null-checked above and the caller's
            // contract makes it writable and distinct from `out_error`. The
            // buffer it receives is freshly leaked by `encode_response`.
            unsafe { out_jobs.write(encode_response(&(RESPONSE_VERSION, jobs))?) };
            Ok(())
        },
    )
}

#[unsafe(no_mangle)]
/// Claims one queue candidate and returns an opaque store-owned claim token.
///
/// # Safety
///
/// `store` must be the original owner cell passed to [`es_open`]. `request`
/// must reference a readable buffer. `out_claim` must be writable and
/// `out_error` must be null or writable. The request buffer may alias either
/// output, but `out_claim` and `out_error` must be distinct.
pub unsafe extern "C" fn es_job_claim(
    store: *mut *mut EsStore,
    request: *const EsBuf,
    out_claim: *mut EsBuf,
    out_error: *mut EsBuf,
) -> i32 {
    let lease = match EsStore::acquire(store) {
        Ok(lease) => lease,
        Err(error) => {
            clear_buffer_output(out_claim);
            return write_error(out_error, error);
        }
    };
    ffi_call(
        Some(&lease.store),
        out_error,
        || clear_buffer_output(out_claim),
        || {
            let decoded: (u8, String, String, i64, i64, u32) = decode(request)?;
            clear_buffer_output(out_claim);
            if out_claim.is_null() {
                return Err(AbiError::State(StateFault::NullOutput(OutputSlot::Claim)));
            }
            let (version, job_id, worker_name, now_ms, lease_ms, max_claims) = decoded;
            require_version(version)?;
            require_identifier_limit(job_id.len())?;
            require_identifier_limit(worker_name.len())?;
            if lease_ms <= 0 || max_claims == 0 {
                return Err(AbiError::MalformedInput);
            }
            let job_id = job_id.parse::<JobId>().map_err(AbiError::decode)?;
            let lease_until_ms =
                now_ms
                    .checked_add(lease_ms)
                    .ok_or(AbiError::State(StateFault::Overflow(
                        CounterOverflow::ClaimLeaseInstant,
                    )))?;
            // Minted before the durable claim: a CSPRNG failure afterwards
            // would leave the job claimed with no token in existence to settle
            // it, which is the state `dead_letter_unusable_claim` exists to
            // avoid.
            let token = claim_token()?;
            let reservation = lease.inner.prepare_claim(now_ms)?;
            let result = lease
                .inner
                .runtime
                .block_on(lease.inner.jobs.claim_job(
                    job_id,
                    &worker_name,
                    now_ms,
                    lease_ms,
                    max_claims,
                ))
                .map_err(|error| job_runtime_error(job_id, error))?;
            let wire = match result {
                JobClaimResult::Claimed(claimed) => {
                    let attempt = claimed.handle.attempt();
                    let has_prior_execution = u8::from(!claimed.handle.is_first_execution());
                    let payload = match claimed_payload_bytes(claimed.payload) {
                        Ok(payload) => payload,
                        Err(refusal) => {
                            return Err(dead_letter_unusable_claim(
                                &lease.inner,
                                claimed.handle,
                                refusal,
                            ));
                        }
                    };
                    reservation.issue(token, claimed.handle, lease_until_ms);
                    JobClaimWire(
                        RESPONSE_VERSION,
                        0,
                        Some(OpaqueBytes(token.to_vec())),
                        Some(attempt),
                        Some(has_prior_execution),
                        Some(OpaqueBytes(payload)),
                    )
                }
                JobClaimResult::Abandoned => {
                    lease.inner.retire_expired_claims(job_id, now_ms);
                    JobClaimWire(RESPONSE_VERSION, 1, None, None, None, None)
                }
                JobClaimResult::Contended => {
                    JobClaimWire(RESPONSE_VERSION, 2, None, None, None, None)
                }
                JobClaimResult::Skipped => {
                    lease.inner.retire_expired_claims(job_id, now_ms);
                    JobClaimWire(RESPONSE_VERSION, 3, None, None, None, None)
                }
            };
            // SAFETY: `out_claim` was null-checked above and the caller's
            // contract makes it writable and distinct from `out_error`. The
            // buffer it receives is freshly leaked by `encode_response`.
            unsafe { out_claim.write(encode_response(&wire)?) };
            Ok(())
        },
    )
}

#[unsafe(no_mangle)]
/// Extends the lease represented by an opaque claim token.
///
/// # Safety
///
/// `store` must be the original owner cell passed to [`es_open`]. `request`
/// must reference a readable buffer. `out_renewal` must be writable and
/// `out_error` must be null or writable. The request storage may alias either
/// output, but `out_renewal` and `out_error` must be distinct.
pub unsafe extern "C" fn es_job_renew(
    store: *mut *mut EsStore,
    request: *const EsBuf,
    out_renewal: *mut u8,
    out_error: *mut EsBuf,
) -> i32 {
    let lease = match EsStore::acquire(store) {
        Ok(lease) => lease,
        Err(error) => {
            clear_tag_output(out_renewal);
            return write_error(out_error, error);
        }
    };
    ffi_call(
        Some(&lease.store),
        out_error,
        || clear_tag_output(out_renewal),
        || {
            let decoded: (u8, OpaqueBytes, i64) = decode(request)?;
            clear_tag_output(out_renewal);
            if out_renewal.is_null() {
                return Err(AbiError::State(StateFault::NullOutput(OutputSlot::Renewal)));
            }
            let (version, OpaqueBytes(encoded), new_lease_until_ms) = decoded;
            require_version(version)?;
            let renewal = lease.inner.begin_renewal(&encoded)?;
            let handle = renewal.handle();
            let result = lease
                .inner
                .runtime
                .block_on(lease.inner.jobs.renew_job(&handle, new_lease_until_ms))
                .map_err(|error| job_runtime_error(handle.job_id(), error))?;
            let tag = match result {
                JobLeaseResult::Held => {
                    renewal.complete(new_lease_until_ms)?;
                    0
                }
                JobLeaseResult::Lost => {
                    renewal.retire();
                    1
                }
            };
            // SAFETY: `out_renewal` was null-checked above and the caller's
            // contract makes it a writable `u8` cell distinct from `out_error`.
            unsafe { out_renewal.write(tag) };
            Ok(())
        },
    )
}

#[unsafe(no_mangle)]
/// Records successful completion for an opaque claim token.
///
/// # Safety
///
/// `store` must be the original owner cell passed to [`es_open`]. `request`
/// must reference a readable buffer. `out_settlement` must be writable and
/// `out_error` must be null or writable. The request storage may alias either
/// output, but `out_settlement` and `out_error` must be distinct.
pub unsafe extern "C" fn es_job_ack(
    store: *mut *mut EsStore,
    request: *const EsBuf,
    out_settlement: *mut u8,
    out_error: *mut EsBuf,
) -> i32 {
    settle_claim_with::<JobAckWire>(
        store,
        request,
        out_settlement,
        out_error,
        |JobAckWire(version, _)| require_version(*version),
        |JobAckWire(_, OpaqueBytes(encoded))| decode_claim_token(encoded),
        |inner, claim, _| {
            let job_id = claim.job_id();
            inner
                .runtime
                .block_on(inner.jobs.acknowledge_job(claim))
                .map_err(|error| job_runtime_error(job_id, error))
        },
    )
}

#[unsafe(no_mangle)]
/// Records a failed attempt and schedules another execution.
///
/// # Safety
///
/// `store` must be the original owner cell passed to [`es_open`]. `request`
/// must reference a readable buffer. `out_settlement` must be writable and
/// `out_error` must be null or writable. The request storage may alias either
/// output, but `out_settlement` and `out_error` must be distinct.
pub unsafe extern "C" fn es_job_retry(
    store: *mut *mut EsStore,
    request: *const EsBuf,
    out_settlement: *mut u8,
    out_error: *mut EsBuf,
) -> i32 {
    settle_claim_with::<JobRetryWire>(
        store,
        request,
        out_settlement,
        out_error,
        |JobRetryWire(version, _, _, error)| {
            require_version(*version).and_then(|()| require_error_text_limit(error.len()))
        },
        |JobRetryWire(_, OpaqueBytes(encoded), _, _)| decode_claim_token(encoded),
        |inner, claim, JobRetryWire(_, _, run_at_ms, error)| {
            let job_id = claim.job_id();
            inner
                .runtime
                .block_on(inner.jobs.retry_job(claim, run_at_ms, error))
                .map_err(|failure| job_runtime_error(job_id, failure))
        },
    )
}

#[unsafe(no_mangle)]
/// Reschedules a claim without advancing its attempt counter.
///
/// # Safety
///
/// `store` must be the original owner cell passed to [`es_open`]. `request`
/// must reference a readable buffer. `out_settlement` must be writable and
/// `out_error` must be null or writable. The request storage may alias either
/// output, but `out_settlement` and `out_error` must be distinct.
pub unsafe extern "C" fn es_job_defer(
    store: *mut *mut EsStore,
    request: *const EsBuf,
    out_settlement: *mut u8,
    out_error: *mut EsBuf,
) -> i32 {
    settle_claim_with::<JobDeferWire>(
        store,
        request,
        out_settlement,
        out_error,
        |JobDeferWire(version, _, _)| require_version(*version),
        |JobDeferWire(_, OpaqueBytes(encoded), _)| decode_claim_token(encoded),
        |inner, claim, JobDeferWire(_, _, run_at_ms)| {
            let job_id = claim.job_id();
            inner
                .runtime
                .block_on(inner.jobs.defer_job(claim, run_at_ms))
                .map_err(|error| job_runtime_error(job_id, error))
        },
    )
}

#[unsafe(no_mangle)]
/// Records terminal failure for an opaque claim token.
///
/// # Safety
///
/// `store` must be the original owner cell passed to [`es_open`]. `request`
/// must reference a readable buffer. `out_settlement` must be writable and
/// `out_error` must be null or writable. The request storage may alias either
/// output, but `out_settlement` and `out_error` must be distinct.
pub unsafe extern "C" fn es_job_dead_letter(
    store: *mut *mut EsStore,
    request: *const EsBuf,
    out_settlement: *mut u8,
    out_error: *mut EsBuf,
) -> i32 {
    settle_claim_with::<JobDeadLetterWire>(
        store,
        request,
        out_settlement,
        out_error,
        |JobDeadLetterWire(version, _, reason, error)| {
            require_version(*version)
                .and_then(|()| require_error_text_limit(error.len()))
                .and_then(|()| dead_reason(*reason).map(drop))
        },
        |JobDeadLetterWire(_, OpaqueBytes(encoded), _, _)| decode_claim_token(encoded),
        |inner, claim, JobDeadLetterWire(_, _, reason, error)| {
            let reason = dead_reason(reason)?;
            let job_id = claim.job_id();
            inner
                .runtime
                .block_on(inner.jobs.dead_letter_job(claim, reason, error))
                .map_err(|failure| job_runtime_error(job_id, failure))
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
    // SAFETY: the caller's contract makes a non-null `buffer` a live, aligned
    // `EsBuf` it owns, and this call has exclusive use of it for its duration.
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
    // SAFETY: every buffer this library hands out comes from `owned_buffer`,
    // which leaks exactly one `Box<[u8]>` of `len` bytes, so reconstructing the
    // box reclaims that allocation with its original layout. The owner cell was
    // nulled above, so a repeated call cannot reach this line for the same
    // buffer.
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
    jobs: JobRuntime,
    claims: Mutex<ClaimRegistry>,
}

impl StoreInner {
    /// Reserves one of the store's active-claim slots for an in-flight claim.
    ///
    /// A lapsed local lease is not evidence that a claim was lost - only the
    /// durable job decides that - so retained tokens are never dropped merely
    /// because `now_ms` moved past them. Expired tokens are evicted only to
    /// make room, oldest first, when the registry is otherwise at capacity.
    fn prepare_claim(&self, now_ms: i64) -> Result<ClaimReservation<'_>, AbiError> {
        let mut registry = lock_ignoring_poison(&self.claims);
        let observed = registry
            .retained
            .len()
            .checked_add(registry.reservations)
            .and_then(|used| used.checked_add(1))
            .ok_or(AbiError::State(StateFault::Overflow(
                CounterOverflow::ActiveClaims,
            )))?;
        if observed > MAX_ACTIVE_CLAIMS && registry.evict_oldest_expired(now_ms).is_none() {
            return Err(AbiError::ResourceLimit {
                resource: "active_claims",
                observed,
                limit: MAX_ACTIVE_CLAIMS,
            });
        }
        registry.reservations += 1;
        drop(registry);
        Ok(ClaimReservation {
            inner: self,
            state: ClaimOperationState::Pending,
        })
    }

    fn begin_renewal(&self, encoded: &[u8]) -> Result<ClaimRenewal<'_>, AbiError> {
        let token = decode_claim_token(encoded)?;
        let mut registry = lock_ignoring_poison(&self.claims);
        let retained = registry
            .retained
            .get_mut(&token)
            .filter(|claim| claim.state == ClaimState::Available)
            .ok_or(AbiError::State(StateFault::Claim(
                ClaimFault::InvalidHandle,
            )))?;
        retained.state = ClaimState::Renewing;
        let handle = retained.handle.clone();
        drop(registry);
        Ok(ClaimRenewal {
            inner: self,
            token,
            handle,
            state: ClaimOperationState::Pending,
        })
    }

    fn begin_settlement(&self, token: ClaimToken) -> Result<ClaimSettlement<'_>, AbiError> {
        let mut registry = lock_ignoring_poison(&self.claims);
        let retained = registry
            .retained
            .get_mut(&token)
            .filter(|claim| claim.state == ClaimState::Available)
            .ok_or(AbiError::State(StateFault::Claim(
                ClaimFault::InvalidHandle,
            )))?;
        retained.state = ClaimState::Settling;
        let handle = retained.handle.clone();
        drop(registry);
        Ok(ClaimSettlement {
            inner: self,
            token,
            handle,
            state: ClaimOperationState::Pending,
        })
    }

    /// Drops this job's lapsed tokens once the durable queue has shown that
    /// nothing is executing under them, which a re-claim answering `Abandoned`
    /// or `Skipped` establishes.
    fn retire_expired_claims(&self, job_id: JobId, now_ms: i64) {
        lock_ignoring_poison(&self.claims)
            .retained
            .retain(|_, claim| claim.handle.job_id() != job_id || !claim.is_expired(now_ms));
    }

    fn retire_claim(&self, token: ClaimToken) {
        lock_ignoring_poison(&self.claims).retained.remove(&token);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ClaimState {
    Available,
    Renewing,
    Settling,
}

#[derive(Default)]
struct ClaimRegistry {
    retained: HashMap<ClaimToken, RetainedClaim>,
    reservations: usize,
}

impl ClaimRegistry {
    /// Frees the longest-lapsed retained slot, reporting which token went.
    ///
    /// Returns [`None`] when nothing has lapsed, which leaves the caller to
    /// report the capacity limit rather than evicting a live claim.
    fn evict_oldest_expired(&mut self, now_ms: i64) -> Option<ClaimToken> {
        let token = self
            .retained
            .iter()
            .filter(|(_, retained)| retained.is_expired(now_ms))
            .min_by_key(|(_, retained)| retained.lease_until_ms)
            .map(|(token, _)| *token)?;
        self.retained.remove(&token).map(|_| token)
    }

    /// Saturates instead of asserting: this runs from [`ClaimReservation`]'s
    /// destructor, where a panic during an unwind would abort the host process
    /// past the ABI's `catch_unwind` boundary.
    fn release_reservation(&mut self) {
        self.reservations = self.reservations.saturating_sub(1);
    }
}

struct RetainedClaim {
    handle: JobClaimHandle,
    lease_until_ms: i64,
    state: ClaimState,
}

impl RetainedClaim {
    fn is_expired(&self, now_ms: i64) -> bool {
        self.state == ClaimState::Available && self.lease_until_ms < now_ms
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ClaimOperationState {
    Pending,
    Completed,
}

struct ClaimReservation<'store> {
    inner: &'store StoreInner,
    state: ClaimOperationState,
}

impl ClaimReservation<'_> {
    /// Retains `token` as the job's only settleable claim and consumes the
    /// reservation.
    ///
    /// Infallible by construction: the token is minted before the durable claim
    /// is taken, so nothing between winning a claim and handing out its token
    /// can fail and strand the claim.
    fn issue(mut self, token: ClaimToken, handle: JobClaimHandle, lease_until_ms: i64) {
        let job_id = handle.job_id();
        let mut registry = lock_ignoring_poison(&self.inner.claims);
        registry.retained.retain(|_, existing| {
            existing.state != ClaimState::Available || existing.handle.job_id() != job_id
        });
        registry.retained.insert(
            token,
            RetainedClaim {
                handle,
                lease_until_ms,
                state: ClaimState::Available,
            },
        );
        registry.release_reservation();
        drop(registry);
        self.state = ClaimOperationState::Completed;
    }
}

/// Mints the opaque handle that fences one claim's settlement.
///
/// The token is the only thing standing between a caller and settling a claim
/// it does not own, so it comes from the operating system's CSPRNG rather than
/// from any in-process sequence a holder of one token could extrapolate.
fn claim_token() -> Result<ClaimToken, AbiError> {
    let mut token = [0_u8; 16];
    getrandom::fill(&mut token).map_err(|error| {
        AbiError::state_source(StateFault::Claim(ClaimFault::TokenGeneration), error)
    })?;
    Ok(token)
}

impl Drop for ClaimReservation<'_> {
    fn drop(&mut self) {
        if self.state == ClaimOperationState::Completed {
            return;
        }
        lock_ignoring_poison(&self.inner.claims).release_reservation();
    }
}

struct ClaimRenewal<'store> {
    inner: &'store StoreInner,
    token: ClaimToken,
    handle: JobClaimHandle,
    state: ClaimOperationState,
}

impl ClaimRenewal<'_> {
    fn handle(&self) -> JobClaimHandle {
        self.handle.clone()
    }

    fn complete(mut self, lease_until_ms: i64) -> Result<(), AbiError> {
        let mut registry = lock_ignoring_poison(&self.inner.claims);
        let Some(retained) = registry
            .retained
            .get_mut(&self.token)
            .filter(|retained| retained.state == ClaimState::Renewing)
        else {
            drop(registry);
            return Err(AbiError::State(StateFault::Claim(
                ClaimFault::RenewalNotRetained,
            )));
        };
        retained.lease_until_ms = lease_until_ms;
        retained.state = ClaimState::Available;
        drop(registry);
        self.state = ClaimOperationState::Completed;
        Ok(())
    }

    fn retire(mut self) {
        self.inner.retire_claim(self.token);
        self.state = ClaimOperationState::Completed;
    }
}

impl Drop for ClaimRenewal<'_> {
    fn drop(&mut self) {
        if self.state == ClaimOperationState::Completed {
            return;
        }
        if let Some(claim) = lock_ignoring_poison(&self.inner.claims)
            .retained
            .get_mut(&self.token)
            .filter(|claim| claim.state == ClaimState::Renewing)
        {
            claim.state = ClaimState::Available;
        }
    }
}

struct ClaimSettlement<'store> {
    inner: &'store StoreInner,
    token: ClaimToken,
    handle: JobClaimHandle,
    state: ClaimOperationState,
}

impl ClaimSettlement<'_> {
    fn handle(&self) -> JobClaimHandle {
        self.handle.clone()
    }

    fn complete(mut self) {
        self.inner.retire_claim(self.token);
        self.state = ClaimOperationState::Completed;
    }
}

impl Drop for ClaimSettlement<'_> {
    fn drop(&mut self) {
        if self.state == ClaimOperationState::Completed {
            return;
        }
        if let Some(claim) = lock_ignoring_poison(&self.inner.claims)
            .retained
            .get_mut(&self.token)
            .filter(|claim| claim.state == ClaimState::Settling)
        {
            claim.state = ClaimState::Available;
        }
    }
}

fn decode_claim_token(encoded: &[u8]) -> Result<ClaimToken, AbiError> {
    ClaimToken::try_from(encoded).map_err(|_| AbiError::MalformedInput)
}

fn settle_claim_with<Decoded: DeserializeOwned + Serialize>(
    store: *mut *mut EsStore,
    request: *const EsBuf,
    out_settlement: *mut u8,
    out_error: *mut EsBuf,
    validate: impl FnOnce(&Decoded) -> Result<(), AbiError>,
    claim_token: impl FnOnce(&Decoded) -> Result<ClaimToken, AbiError>,
    operation: impl FnOnce(
        &StoreInner,
        JobClaimHandle,
        Decoded,
    ) -> Result<JobSettlementResult, AbiError>,
) -> i32 {
    let lease = match EsStore::acquire(store) {
        Ok(lease) => lease,
        Err(error) => {
            clear_tag_output(out_settlement);
            return write_error(out_error, error);
        }
    };
    ffi_call(
        Some(&lease.store),
        out_error,
        || clear_tag_output(out_settlement),
        || {
            let decoded: Decoded = decode(request)?;
            clear_tag_output(out_settlement);
            if out_settlement.is_null() {
                return Err(AbiError::State(StateFault::NullOutput(
                    OutputSlot::Settlement,
                )));
            }
            validate(&decoded)?;
            let token = claim_token(&decoded)?;
            let settlement = lease.inner.begin_settlement(token)?;
            let result = operation(&lease.inner, settlement.handle(), decoded)?;
            settlement.complete();
            // SAFETY: `out_settlement` was null-checked above and the caller's
            // contract makes it a writable `u8` cell distinct from `out_error`.
            unsafe { out_settlement.write(settlement_tag(result)) };
            Ok(())
        },
    )
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
    #[error("job {job_id} refused the request: {refusal}")]
    JobRefusal { job_id: JobId, refusal: JobRefusal },
    #[error("storage operation failed")]
    Storage(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("{0}")]
    State(StateFault),
    #[error("{fault}")]
    StateSource {
        fault: StateFault,
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

/// Why one job refused the request made of it, behind [`ES_ERR_JOB_REFUSAL`].
///
/// SPEC pins the `JOB_REFUSAL` detail to `[job_id, reason]`, so the rendered
/// text of each leaf is the discriminant a binding matches on. Both the set of
/// leaves and their rendered text are part of the ABI: add a leaf rather than
/// rewording an existing one.
///
/// A refusal is terminal for the job it names: unlike [`ES_ERR_STORAGE`], no
/// amount of retrying the same request can turn it into a success.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
enum JobRefusal {
    #[error("job attempt counter exhausted")]
    AttemptExhausted,
    #[error("invalid job claim policy")]
    InvalidClaimPolicy,
    #[error("job aggregate refused the settlement")]
    SettlementRefused,
    #[error("claimed job payload is not opaque bytes")]
    PayloadNotOpaqueBytes,
    #[error("claimed job payload contains an invalid byte")]
    PayloadByteOutOfRange,
    #[error("claimed job payload exceeds the ABI payload limit")]
    PayloadAboveAbiLimit,
}

/// The failure classes behind [`ES_ERR_STATE`].
///
/// SPEC pins the `INVALID_STATE` detail to redacted UTF-8 text, so the rendered
/// text of each leaf is the discriminant a binding matches on. Both the set of
/// leaves and their rendered text are part of the ABI: add a leaf rather than
/// rewording an existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
enum StateFault {
    #[error("{0}")]
    NullOutput(OutputSlot),
    #[error("{0}")]
    Lifecycle(StoreLifecycle),
    #[error("{0}")]
    Overflow(CounterOverflow),
    #[error("{0}")]
    Claim(ClaimFault),
    #[error("{0}")]
    Page(PageFault),
}

/// The caller-owned output cell a call could not write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
enum OutputSlot {
    #[error("out_store is null")]
    Store,
    #[error("out_events is null")]
    Events,
    #[error("out_version is null")]
    Version,
    #[error("out_jobs is null")]
    Jobs,
    #[error("out_claim is null")]
    Claim,
    #[error("out_renewal is null")]
    Renewal,
    #[error("out_settlement is null")]
    Settlement,
}

/// A store handle that cannot serve the requested operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
enum StoreLifecycle {
    #[error("store owner is null")]
    OwnerNull,
    #[error("store is closed")]
    Closed,
    #[error("store is closing")]
    Closing,
    #[error("store is poisoned")]
    Poisoned,
    #[error("store owner does not match its handle")]
    OwnerMismatch,
    #[error("store owner is already open")]
    OwnerAlreadyOpen,
    #[error("runtime initialization failed")]
    RuntimeInitialization,
}

/// An in-process counter that cannot admit another participant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
enum CounterOverflow {
    #[error("active call count overflow")]
    ActiveCalls,
    #[error("active claim capacity overflowed")]
    ActiveClaims,
    #[error("claim lease instant overflowed")]
    ClaimLeaseInstant,
}

/// A claim token that cannot be honoured, or a claim that cannot be handed out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
enum ClaimFault {
    #[error("claim handle is invalid")]
    InvalidHandle,
    #[error("renewing claim is no longer retained")]
    RenewalNotRetained,
    #[error("claim token generation failed")]
    TokenGeneration,
}

/// An event page that cannot be framed within the ABI's fixed bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
enum PageFault {
    #[error("invalid list query limit")]
    ListQueryLimit,
    #[error("invalid stored page byte limit")]
    StoredPageByteLimit,
    #[error("event page length exceeds ABI limit")]
    LengthAboveAbiLimit,
    #[error("response encoding failed")]
    ResponseEncodingFailed,
}

fn store_registry() -> &'static Mutex<HashMap<usize, StoreEntry>> {
    STORE_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Takes `mutex`, recovering the guarded value if a panic poisoned it.
///
/// Poisoning is not a failure class this ABI reports: [`ffi_call`] already
/// converted the panic that poisoned the mutex into [`ES_ERR_PANIC`] and
/// poisoned the store, so raising a second panic here would abort the host
/// process instead of returning a code. Every lock in this crate goes through
/// this function so the policy holds at every site.
fn lock_ignoring_poison<Guarded>(mutex: &Mutex<Guarded>) -> MutexGuard<'_, Guarded> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn publish_store(owner: *mut *mut EsStore, store: Arc<EsStore>) -> Result<(), AbiError> {
    let owner_address = owner as usize;
    let mut registry = lock_ignoring_poison(store_registry());
    if registry.contains_key(&owner_address) {
        return Err(AbiError::State(StateFault::Lifecycle(
            StoreLifecycle::OwnerAlreadyOpen,
        )));
    }
    let raw_store = Arc::into_raw(Arc::clone(&store)).cast_mut();
    registry.insert(
        owner_address,
        StoreEntry {
            raw_store: raw_store as usize,
            store,
        },
    );
    // SAFETY: `owner` is the caller's store cell, which [`es_open`]'s contract
    // makes writable and stable until close completes. The registry entry
    // published above holds the matching strong count, so the address written
    // here stays valid until `retire_store` reclaims it.
    unsafe { owner.write(raw_store) };
    drop(registry);
    Ok(())
}

fn linearize_close(
    owner: *mut *mut EsStore,
) -> Result<Option<(Arc<EsStore>, CloseRole)>, AbiError> {
    let owner_address = owner as usize;
    let registry = lock_ignoring_poison(store_registry());
    let Some(entry) = registry.get(&owner_address) else {
        // SAFETY: `es_close` checked `owner` for null, and its contract makes a
        // non-null owner the original writable cell. No entry means the store
        // was already retired, so nulling the cell cannot orphan a handle.
        unsafe { owner.write(ptr::null_mut()) };
        return Ok(None);
    };
    // SAFETY: as above, `owner` is a readable cell the caller owns; the
    // registry lock is held, so the entry it is compared against cannot be
    // retired concurrently.
    let raw_store = unsafe { owner.read() };
    if !raw_store.is_null() && raw_store as usize != entry.raw_store {
        return Err(AbiError::State(StateFault::Lifecycle(
            StoreLifecycle::OwnerMismatch,
        )));
    }
    let store = Arc::clone(&entry.store);
    let mut state = lock_ignoring_poison(&store.state);
    let role = match &*state {
        StoreState::Open { active_calls } => {
            *state = StoreState::Closing {
                active_calls: *active_calls,
            };
            CloseRole::Destroy
        }
        StoreState::Closing { .. } | StoreState::Closed(_) => CloseRole::Join,
    };
    // SAFETY: as above, `owner` is the caller's writable cell. Nulling it under
    // the registry lock is what makes a concurrent `acquire` observe a closed
    // store instead of racing this close.
    unsafe { owner.write(ptr::null_mut()) };
    drop(state);
    store.state_changed.notify_all();
    drop(registry);
    Ok(Some((store, role)))
}

fn retire_store(owner_address: usize, store: &Arc<EsStore>) {
    let mut registry = lock_ignoring_poison(store_registry());
    let should_remove = registry
        .get(&owner_address)
        .is_some_and(|entry| Arc::ptr_eq(&entry.store, store));
    let entry = should_remove
        .then(|| registry.remove(&owner_address))
        .flatten();
    drop(registry);
    if let Some(entry) = entry {
        let raw_store: *const EsStore = std::ptr::with_exposed_provenance(entry.raw_store);
        // SAFETY: `entry.raw_store` is the address `publish_store` exposed from
        // the single strong count it leaked with `Arc::into_raw`, so the
        // provenance round trip reconstructs that same allocation. The entry
        // was removed from the registry under the lock above and the removal
        // was conditional on this being the same store, so exactly one caller
        // reaches this line for any published entry.
        unsafe { drop(Arc::from_raw(raw_store)) };
    }
}

impl EsStore {
    fn acquire(owner: *mut *mut Self) -> Result<StoreLease, AbiError> {
        if owner.is_null() {
            return Err(AbiError::State(StateFault::Lifecycle(
                StoreLifecycle::OwnerNull,
            )));
        }
        let owner_address = owner as usize;
        let registry = lock_ignoring_poison(store_registry());
        let Some(entry) = registry.get(&owner_address) else {
            return Err(AbiError::State(StateFault::Lifecycle(
                StoreLifecycle::Closed,
            )));
        };
        // SAFETY: `owner` was null-checked above, and every entry point's
        // contract makes a non-null owner the original readable cell from
        // `es_open`. The registry lock is held, so the address read here is
        // compared against an entry no concurrent close can retire.
        let raw_store = unsafe { owner.read() };
        if raw_store.is_null() || raw_store as usize != entry.raw_store {
            return Err(AbiError::State(StateFault::Lifecycle(
                StoreLifecycle::Closed,
            )));
        }
        let store = Arc::clone(&entry.store);
        let mut state = lock_ignoring_poison(&store.state);
        let StoreState::Open { active_calls } = &mut *state else {
            return Err(AbiError::State(StateFault::Lifecycle(
                StoreLifecycle::Closing,
            )));
        };
        let inner = lock_ignoring_poison(&store.inner)
            .as_ref()
            .cloned()
            .ok_or(AbiError::State(StateFault::Lifecycle(
                StoreLifecycle::Closed,
            )))?;
        *active_calls =
            active_calls
                .checked_add(1)
                .ok_or(AbiError::State(StateFault::Overflow(
                    CounterOverflow::ActiveCalls,
                )))?;
        drop(state);
        drop(registry);
        Ok(StoreLease { store, inner })
    }

    fn close(&self, role: CloseRole) -> Result<(), AbiError> {
        if matches!(role, CloseRole::Join) {
            let mut state = lock_ignoring_poison(&self.state);
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

        let mut state = lock_ignoring_poison(&self.state);
        while matches!(&*state, StoreState::Closing { active_calls } if *active_calls != 0) {
            state = self
                .state_changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        drop(state);

        let inner = lock_ignoring_poison(&self.inner).take();
        let result = catch_unwind(AssertUnwindSafe(|| drop(inner))).map_err(|_| AbiError::Panic);
        let outcome = if result.is_ok() {
            CloseOutcome::Ok
        } else {
            CloseOutcome::Panic
        };
        let mut state = lock_ignoring_poison(&self.state);
        *state = StoreState::Closed(outcome);
        drop(state);
        self.state_changed.notify_all();
        result
    }
}

impl Drop for StoreLease {
    fn drop(&mut self) {
        let mut state = lock_ignoring_poison(&self.store.state);
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
        let mut state = lock_ignoring_poison(&self.store.state);
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
            // A batch that does not follow the stream it targets is a caller
            // fault that no retry can resolve, unlike the conflict a stale
            // writer sees.
            caller_fault @ (EngineError::NonContiguousSequences { .. }
            | EngineError::EmptySnapshotUpdate
            | EngineError::StreamIdentityMismatch { .. }) => Self::decode(caller_fault),
            // The same permanently unfixable instant `es_job_enqueue` reports
            // as malformed input: which entry point the job intent arrived
            // through must not change how the caller is told to react.
            EngineError::JobFlush(JobStoreError::InvalidInstant(_)) => Self::MalformedInput,
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
        fault: StateFault,
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::StateSource {
            fault,
            source: Box::new(error),
        }
    }

    const fn code(&self) -> i32 {
        match self {
            Self::MalformedInput | Self::Decode(_) => ES_ERR_DECODE,
            Self::Conflict { .. } => ES_ERR_CONFLICT,
            Self::JobRefusal { .. } => ES_ERR_JOB_REFUSAL,
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
    // SAFETY: every entry point's contract makes a non-null `buffer` reference
    // a live, aligned `EsBuf` that stays readable for the duration of the call.
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
    // SAFETY: `buffer.ptr` was null-checked and `buffer.len` was bounded by
    // `MAX_REQUEST_BYTES` above, and the calling entry point's contract makes
    // the pointer reference that many readable bytes for the duration of the
    // call. Nothing here reads past `len` or retains the slice.
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
        self.overflow_observed.map_or(
            AbiError::State(StateFault::Page(PageFault::ResponseEncodingFailed)),
            |observed| AbiError::ResourceLimit {
                resource: "encoded_response_buffer",
                observed,
                limit: MAX_RESPONSE_BYTES,
            },
        )
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

/// Streams a page as `[response_version, events]` without materializing the
/// whole array first, so a page at the response byte limit is refused while it
/// is being written rather than after it has been built twice.
fn encode_event_page(events: Vec<LoadedEvent>) -> Result<EsBuf, AbiError> {
    let mut writer = BoundedResponseWriter::new();
    // `array(2)` followed by the version: a CBOR unsigned integer below 24 is
    // its own shortest-form encoding, so the version byte needs no header.
    writer
        .write_all(&[CBOR_ARRAY_HEADER | 2, RESPONSE_VERSION])
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

fn encode_response(value: &impl Serialize) -> Result<EsBuf, AbiError> {
    let mut writer = BoundedResponseWriter::new();
    if ciborium::into_writer(value, &mut writer).is_err() {
        return Err(writer.encoding_error());
    }
    Ok(writer.into_buffer())
}

/// Renders a claimed job's stored payload as the opaque bytes the ABI returns.
///
/// Payloads written outside this ABI need not have the opaque-bytes shape, so
/// this is a real failure path on a claim that already succeeded durably. The
/// shape of a stored payload never changes, so every failure here is the job
/// refusing this ABI for good; see [`dead_letter_unusable_claim`] for how the
/// claim is settled.
fn claimed_payload_bytes(payload: serde_json::Value) -> Result<Vec<u8>, JobRefusal> {
    let serde_json::Value::Array(bytes) = payload else {
        return Err(JobRefusal::PayloadNotOpaqueBytes);
    };
    let bytes = bytes
        .into_iter()
        .map(|byte| {
            byte.as_u64()
                .and_then(|byte| u8::try_from(byte).ok())
                .ok_or(JobRefusal::PayloadByteOutOfRange)
        })
        .collect::<Result<Vec<u8>, JobRefusal>>()?;
    if bytes.len() > MAX_PAYLOAD_BYTES {
        return Err(JobRefusal::PayloadAboveAbiLimit);
    }
    Ok(bytes)
}

/// Settles a durably-won claim the caller can never be handed.
///
/// The claim is committed before the payload can be inspected, so failing
/// without compensation would leave the job claimed with no token in existence
/// to settle it. Deferring it back would re-arm a claim that can only fail the
/// same way -- and a defer resets the claim budget, so the abandon path could
/// never catch the loop either. The stored payload is what the ABI cannot read,
/// which is exactly [`DeadReason::Undecodable`], so the job is settled
/// terminally and the caller is told the job refused the claim rather than that
/// the ABI is in a bad state. A failure to settle replaces the refusal, because
/// that is the condition the caller has to act on.
///
/// The recorded reason is one of [`JobRefusal`]'s fixed leaves, so it carries no
/// caller data and is far inside the ABI's error-text bound.
fn dead_letter_unusable_claim(
    inner: &StoreInner,
    handle: JobClaimHandle,
    refusal: JobRefusal,
) -> AbiError {
    let job_id = handle.job_id();
    let settled = inner.runtime.block_on(inner.jobs.dead_letter_job(
        handle,
        DeadReason::Undecodable,
        refusal.to_string(),
    ));
    match settled {
        Ok(JobSettlementResult::Applied | JobSettlementResult::Fenced) => {
            AbiError::JobRefusal { job_id, refusal }
        }
        Err(error) => job_runtime_error(job_id, error),
    }
}

/// Separates the three retry answers a job-runtime fault can carry.
///
/// Only the infrastructure variants become `STORAGE_FAILURE`, the one class a
/// binding is expected to retry. An out-of-range instant is a caller-supplied
/// value that can never become valid, so it is malformed input. The rest are
/// `job_id`'s own refusals: the request was well formed and the infrastructure
/// is healthy, but this job cannot serve it, so the caller must stop settling
/// and surface the job instead of retrying it.
fn job_runtime_error<BackendError>(job_id: JobId, error: JobRuntimeError<BackendError>) -> AbiError
where
    BackendError: std::error::Error + Send + Sync + 'static,
{
    match error {
        JobRuntimeError::InvalidInstant(_) => AbiError::MalformedInput,
        JobRuntimeError::AttemptExhausted => AbiError::JobRefusal {
            job_id,
            refusal: JobRefusal::AttemptExhausted,
        },
        JobRuntimeError::InvalidClaimPolicy => AbiError::JobRefusal {
            job_id,
            refusal: JobRefusal::InvalidClaimPolicy,
        },
        JobRuntimeError::SettlementRefused {
            job_id: refused,
            source: _,
        } => AbiError::JobRefusal {
            job_id: refused,
            refusal: JobRefusal::SettlementRefused,
        },
        infrastructure @ (JobRuntimeError::Backend(_)
        | JobRuntimeError::Projection(_)
        | JobRuntimeError::Aggregate(_)) => AbiError::storage(infrastructure),
    }
}

const fn settlement_tag(result: JobSettlementResult) -> u8 {
    match result {
        JobSettlementResult::Applied => 0,
        JobSettlementResult::Fenced => 1,
    }
}

const fn dead_reason(tag: u8) -> Result<DeadReason, AbiError> {
    match tag {
        0 => Ok(DeadReason::RetriesExhausted),
        1 => Ok(DeadReason::Rejected),
        2 => Ok(DeadReason::Undecodable),
        3 => Ok(DeadReason::Abandoned),
        _ => Err(AbiError::MalformedInput),
    }
}

/// Writes the deterministic-CBOR array header for `length` items.
///
/// Shortest-form only, matching what `ciborium` emits for the items that
/// follow: counts under 24 ride in the header's argument bits, then a one-byte
/// count, then a two-byte one. Beyond that the page is past the ABI's item
/// bound and is refused rather than framed.
fn write_array_header(writer: &mut BoundedResponseWriter, length: usize) -> Result<(), AbiError> {
    let encoded = match length {
        0..=23 => vec![CBOR_ARRAY_HEADER | u8::try_from(length).map_err(AbiError::storage)?],
        24..=255 => vec![
            CBOR_ARRAY_HEADER_U8_COUNT,
            u8::try_from(length).map_err(AbiError::storage)?,
        ],
        256..=65_535 => {
            let length = u16::try_from(length).map_err(AbiError::storage)?;
            let mut encoded = vec![CBOR_ARRAY_HEADER_U16_COUNT];
            encoded.extend_from_slice(&length.to_be_bytes());
            encoded
        }
        _ => {
            return Err(AbiError::State(StateFault::Page(
                PageFault::LengthAboveAbiLimit,
            )));
        }
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
    Ok(StoredEventWire(
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

/// Bounds an identifier, reported under its own resource name.
///
/// The identifier and error-text limits happen to coincide, but the
/// `RESOURCE_LIMIT` detail names which of the two a request exceeded. A caller
/// whose aggregate id was too long, and is told its error text was, goes
/// looking in the wrong place.
const fn require_identifier_limit(observed: usize) -> Result<(), AbiError> {
    if observed <= MAX_IDENTIFIER_BYTES {
        return Ok(());
    }
    Err(AbiError::ResourceLimit {
        resource: "identifier_text",
        observed,
        limit: MAX_IDENTIFIER_BYTES,
    })
}

const fn require_commit_event_count(observed: usize) -> Result<(), AbiError> {
    if observed <= MAX_COMMIT_EVENTS {
        return Ok(());
    }
    Err(AbiError::ResourceLimit {
        resource: "commit_events",
        observed,
        limit: MAX_COMMIT_EVENTS,
    })
}

fn commit_parts(
    version: u8,
    aggregate_type: String,
    aggregate_id: String,
    expected: u64,
    events: CommitEvents,
) -> Result<(StreamIdentity, Vec<SerializedEvent>), AbiError> {
    require_version(version)?;
    require_identifier_limit(aggregate_type.len())?;
    require_identifier_limit(aggregate_id.len())?;
    require_commit_event_count(events.observed)?;
    if events.values.is_empty() {
        return Err(AbiError::MalformedInput);
    }
    let expected_sequence = usize::try_from(expected).map_err(|_| AbiError::MalformedInput)?;
    expected_sequence
        .checked_add(events.values.len())
        .ok_or(AbiError::MalformedInput)?;
    let serialized = events
        .values
        .into_iter()
        .enumerate()
        .map(
            |(index, ProposedEventWire(event_type, event_version, OpaqueBytes(payload)))| {
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
    Ok((
        StreamIdentity::new(aggregate_type, aggregate_id),
        serialized,
    ))
}

fn commit_result(
    inner: &StoreInner,
    stream: &StreamIdentity,
    serialized: &[SerializedEvent],
    expected: u64,
    job: Option<JobSeed>,
) -> Result<(), AbiError> {
    let Some(first_event) = serialized.first() else {
        return Err(AbiError::MalformedInput);
    };
    let aggregate_type = first_event.aggregate_type.clone();
    let aggregate_id = first_event.aggregate_id.clone();
    // `Engine::commit` treats only a stored version *ahead* of the batch as a
    // conflict, so a compacted native stream stays writable (ADR-0003). The
    // engine's oracle counts the sequences a snapshot covers, so a compacted
    // stream reports its real version here and a current writer is not refused.
    // `expected` is caller-supplied, so a durable version *behind* it is a
    // caller fault that would open a permanent sequence gap the engine's own
    // check cannot see. Losing this race can only miss a rejection the engine
    // then makes itself.
    let observed = inner
        .runtime
        .block_on(inner.engine.current_version(stream))
        .map_err(AbiError::storage)?;
    let observed = u64::try_from(observed).map_err(AbiError::storage)?;
    if observed < expected {
        return Err(AbiError::Conflict {
            aggregate_type,
            aggregate_id,
            expected,
            actual: observed,
        });
    }
    let base = CommitRequest::new(stream.clone(), serialized).with_opaque_payloads();
    let request = if let Some(job) = job {
        base.with_job(job)
    } else {
        base
    };
    match inner.runtime.block_on(inner.engine.commit(request)) {
        Ok(()) => Ok(()),
        // Both versions come from the write transaction that detected the
        // conflict, so the `actual` reported to the caller is the version that
        // caused it rather than whatever a later read would have found.
        Err(EngineError::OptimisticLock {
            expected_version,
            actual_version,
        }) => Err(AbiError::Conflict {
            aggregate_type,
            aggregate_id,
            expected: u64::try_from(expected_version).map_err(AbiError::storage)?,
            actual: u64::try_from(actual_version).map_err(AbiError::storage)?,
        }),
        Err(error) => Err(AbiError::from(error)),
    }
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
    let busy_timeout_limit = u64::from(MAX_BUSY_TIMEOUT_MS);
    if busy_timeout_ms > busy_timeout_limit {
        return Err(AbiError::ResourceLimit {
            resource: "busy_timeout_ms",
            observed: usize::try_from(busy_timeout_ms).map_err(|_| AbiError::MalformedInput)?,
            limit: usize::try_from(busy_timeout_limit).map_err(|_| AbiError::MalformedInput)?,
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
        return write_error(
            out_error,
            AbiError::State(StateFault::Lifecycle(StoreLifecycle::Poisoned)),
        );
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
        AbiError::JobRefusal { job_id, refusal } => ciborium::Value::Array(vec![
            ciborium::Value::Text(job_id.to_string()),
            ciborium::Value::Text(refusal.to_string()),
        ]),
        AbiError::Storage(_) => ciborium::Value::Text("storage failure".to_string()),
        AbiError::State(fault) | AbiError::StateSource { fault, source: _ } => {
            ciborium::Value::Text(fault.to_string())
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
        if ciborium::into_writer(&(RESPONSE_VERSION, class, detail), &mut bytes).is_ok()
            && bytes.len() <= MAX_RESPONSE_BYTES
        {
            // SAFETY: `out_error` was null-checked above, and every entry
            // point's contract makes a non-null `out_error` writable and
            // distinct from the call's other output. It was cleared at the top
            // of this function, so nothing owned is overwritten here.
            unsafe { out_error.write(owned_buffer(bytes)) };
        }
    }
    class
}

const fn clear_buffer_output(output: *mut EsBuf) {
    if !output.is_null() {
        // SAFETY: null was excluded above, and every entry point's contract
        // makes a non-null output cell writable for the duration of the call.
        // Writing a null buffer cannot leak: the cell is caller-owned and any
        // buffer this library previously put there was handed to the caller.
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
        // SAFETY: null was excluded above, and the contract of the entry point
        // that owns this cell makes a non-null `out_version` writable.
        unsafe { output.write(0) };
    }
}

const fn clear_tag_output(output: *mut u8) {
    if !output.is_null() {
        // SAFETY: null was excluded above, and the contract of the entry point
        // that owns this cell makes a non-null tag output writable.
        unsafe { output.write(0) };
    }
}

#[cfg(test)]
mod tests {
    use ciborium::Value;

    use event_sorcery::SnapshotUpdate;

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

    /// Opens the same store wiring [`es_open`] builds, plus the pool behind it
    /// so a test can act as a native peer sharing the database.
    fn open_store_inner() -> (StoreInner, sqlx::SqlitePool) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let connect = SqliteConnectOptions::from_str("sqlite::memory:")
            .unwrap()
            .create_if_missing(true);
        let pool = runtime
            .block_on(
                SqlitePoolOptions::new()
                    .max_connections(1)
                    .connect_with(connect),
            )
            .unwrap();
        let engine = Engine::new(pool.clone());
        runtime.block_on(engine.migrate()).unwrap();
        let jobs = runtime.block_on(JobRuntime::build(pool.clone())).unwrap();

        (
            StoreInner {
                runtime,
                engine,
                jobs,
                claims: Mutex::new(ClaimRegistry::default()),
            },
            pool,
        )
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

    fn decode_output<T: DeserializeOwned>(buffer: &EsBuf) -> T {
        let bytes = unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len) };
        ciborium::from_reader(Cursor::new(bytes)).unwrap()
    }

    fn load_page(
        store: &mut *mut EsStore,
        aggregate_type: &str,
        aggregate_id: &str,
        after: Option<u64>,
    ) -> Vec<StoredEventWire> {
        let mut encoded = encode_request(&(1_u8, aggregate_type, aggregate_id, after));
        let request = caller_buffer(&mut encoded);
        let mut output = empty_buffer();
        let mut error = empty_buffer();
        assert_eq!(
            unsafe { es_load_stream(store, &raw const request, &raw mut output, &raw mut error,) },
            ES_OK
        );
        assert!(error.ptr.is_null());
        let StoredEventsWire(1, events) = decode_output::<StoredEventsWire>(&output) else {
            panic!("event page must carry the response version");
        };
        unsafe { es_buf_free(&raw mut output) };
        events
    }

    fn enqueue_test_job(store: &mut *mut EsStore, job_id: &str, run_at_ms: i64) {
        let mut encoded = encode_request(&(
            1_u8,
            job_id,
            "ffi-test",
            OpaqueBytes(vec![0, 1, 255]),
            run_at_ms,
        ));
        let request = caller_buffer(&mut encoded);
        let mut error = empty_buffer();
        assert_eq!(
            unsafe { es_job_enqueue(store, &raw const request, &raw mut error) },
            ES_OK
        );
        assert!(error.ptr.is_null());
    }

    fn poll_test_jobs(store: &mut *mut EsStore, now_ms: i64) -> Vec<String> {
        let mut encoded = encode_request(&(1_u8, "ffi-test", now_ms, 10_u32));
        let request = caller_buffer(&mut encoded);
        let mut jobs = empty_buffer();
        let mut error = empty_buffer();
        assert_eq!(
            unsafe { es_job_poll(store, &raw const request, &raw mut jobs, &raw mut error) },
            ES_OK
        );
        assert!(error.ptr.is_null());
        let (1, runnable) = decode_output::<(u8, Vec<String>)>(&jobs) else {
            panic!("a job page must carry the response version");
        };
        unsafe { es_buf_free(&raw mut jobs) };
        runnable
    }

    fn claim_test_job(
        store: &mut *mut EsStore,
        job_id: &str,
        now_ms: i64,
        expected_attempt: u32,
        expected_execution: u8,
    ) -> Vec<u8> {
        let mut encoded = encode_request(&(1_u8, job_id, "ffi-worker", now_ms, 30_000_i64, 50_u32));
        let request = caller_buffer(&mut encoded);
        let mut claim = empty_buffer();
        let mut error = empty_buffer();
        assert_eq!(
            unsafe { es_job_claim(store, &raw const request, &raw mut claim, &raw mut error) },
            ES_OK
        );
        let JobClaimWire(
            1,
            0,
            Some(OpaqueBytes(token)),
            Some(attempt),
            Some(execution),
            Some(payload),
        ) = decode_output::<JobClaimWire>(&claim)
        else {
            panic!("expected a won claim");
        };
        assert_eq!(attempt, expected_attempt);
        assert_eq!(execution, expected_execution);
        assert_eq!(payload, OpaqueBytes(vec![0, 1, 255]));
        assert_eq!(token.len(), 16);
        unsafe { es_buf_free(&raw mut claim) };
        token
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
            StoredEventsWire(
                1,
                vec![StoredEventWire(
                    1,
                    "Created".into(),
                    "1.0".into(),
                    OpaqueBytes(payload)
                )]
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
    fn commit_succeeds_at_the_expected_version_a_snapshot_covers_after_compaction() {
        let (inner, pool) = open_store_inner();
        let stream = StreamIdentity::new("ffi-compaction-test", "one");
        let seeded = (1..=2)
            .map(|sequence| SerializedEvent {
                aggregate_type: "ffi-compaction-test".to_string(),
                aggregate_id: "one".to_string(),
                sequence,
                event_type: "Created".to_string(),
                event_version: "1.0".to_string(),
                payload: serde_json::json!(["native"]),
                metadata: serde_json::Value::Object(serde_json::Map::new()),
            })
            .collect::<Vec<_>>();
        inner
            .runtime
            .block_on(
                inner.engine.commit(
                    CommitRequest::new(stream.clone(), &seeded)
                        .with_snapshot(SnapshotUpdate::new(serde_json::json!({ "seeded": 2 }), 1))
                        .unwrap(),
                ),
            )
            .unwrap();

        // A native peer sharing this database compacts the snapshot-covered
        // prefix away. The ABI caller still holds the aggregate's real version,
        // so its next commit is current, not stale.
        inner
            .runtime
            .block_on(
                sqlx::query(
                    "DELETE FROM events \
                     WHERE aggregate_type = ?1 \
                       AND aggregate_id = ?2 \
                       AND sequence <= (SELECT last_sequence FROM snapshots \
                                        WHERE aggregate_type = ?1 AND aggregate_id = ?2)",
                )
                .bind("ffi-compaction-test")
                .bind("one")
                .execute(&pool),
            )
            .unwrap();

        let appended = [SerializedEvent {
            aggregate_type: "ffi-compaction-test".to_string(),
            aggregate_id: "one".to_string(),
            sequence: 3,
            event_type: "Renamed".to_string(),
            event_version: "1.0".to_string(),
            payload: serde_json::to_value(OpaqueBytes(vec![7, 8])).unwrap(),
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        }];
        commit_result(&inner, &stream, &appended, 2, None).unwrap();

        let loaded = inner
            .runtime
            .block_on(inner.engine.load_events_page_bounded(
                &stream,
                None,
                NonZeroUsize::new(MAX_LIST_ITEMS).unwrap(),
                NonZeroUsize::new(MAX_STORED_EVENT_PAGE_BYTES).unwrap(),
            ))
            .unwrap();
        assert_eq!(
            loaded,
            vec![LoadedEvent {
                sequence: 3,
                event_type: "Renamed".to_string(),
                event_version: "1.0".to_string(),
                payload: LoadedPayload::OpaqueBytes(vec![7, 8]),
            }]
        );
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
            StoredEventsWire(
                1,
                vec![StoredEventWire(
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
        let CommitWire(_, _, _, _, decoded_events) =
            ciborium::from_reader::<CommitWire, _>(Cursor::new(&request)).unwrap();
        assert_eq!(decoded_events.values.len(), MAX_COMMIT_EVENTS);
        assert_eq!(decoded_events.observed, MAX_COMMIT_EVENTS + 1);
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
    fn stream_longer_than_one_page_is_read_by_walking_the_cursor() {
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

        let first_page = load_page(&mut store, "list-limit-test", "one", None);
        assert_eq!(first_page.len(), MAX_LIST_ITEMS);
        assert_eq!(first_page.first().map(|event| event.0), Some(1));
        assert_eq!(
            first_page.last().map(|event| event.0),
            Some(u64::try_from(MAX_LIST_ITEMS).unwrap())
        );

        let second_page = load_page(
            &mut store,
            "list-limit-test",
            "one",
            first_page.last().map(|event| event.0),
        );
        assert_eq!(
            second_page.iter().map(|event| event.0).collect::<Vec<_>>(),
            vec![u64::try_from(MAX_LIST_ITEMS + 1).unwrap()]
        );

        let exhausted = load_page(
            &mut store,
            "list-limit-test",
            "one",
            second_page.last().map(|event| event.0),
        );
        assert_eq!(exhausted, Vec::new());
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
            write_error(
                &raw mut buffer,
                AbiError::State(StateFault::Lifecycle(StoreLifecycle::Closed))
            ),
            ES_ERR_STATE
        );

        unsafe { es_buf_free(&raw mut buffer) };
        assert!(buffer.ptr.is_null());
        assert_eq!(buffer.len, 0);
        unsafe { es_buf_free(&raw mut buffer) };
    }

    #[test]
    fn job_claim_tokens_support_the_lifecycle_and_reject_tampering_and_replay() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let job_id = JobId::new().to_string();
        enqueue_test_job(&mut store, &job_id, 1_000);

        let token = claim_test_job(&mut store, &job_id, 1_000, 0, 0);
        let mut renew_encoded = encode_request(&(1_u8, OpaqueBytes(token.clone()), 60_000_i64));
        let renew = caller_buffer(&mut renew_encoded);
        let mut renewal = u8::MAX;
        let mut error = empty_buffer();
        assert_eq!(
            unsafe {
                es_job_renew(
                    &raw mut store,
                    &raw const renew,
                    &raw mut renewal,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(renewal, 0);

        let mut tampered = token.clone();
        tampered[0] ^= 1;
        let mut tampered_encoded = encode_request(&(1_u8, OpaqueBytes(tampered)));
        let tampered_request = caller_buffer(&mut tampered_encoded);
        let mut settlement = u8::MAX;
        assert_eq!(
            unsafe {
                es_job_ack(
                    &raw mut store,
                    &raw const tampered_request,
                    &raw mut settlement,
                    &raw mut error,
                )
            },
            ES_ERR_STATE
        );
        assert_eq!(settlement, 0);
        unsafe { es_buf_free(&raw mut error) };

        let mut ack_encoded = encode_request(&(1_u8, OpaqueBytes(token)));
        let ack = caller_buffer(&mut ack_encoded);
        assert_eq!(
            unsafe {
                es_job_ack(
                    &raw mut store,
                    &raw const ack,
                    &raw mut settlement,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(settlement, 0);
        assert_eq!(
            unsafe {
                es_job_ack(
                    &raw mut store,
                    &raw const ack,
                    &raw mut settlement,
                    &raw mut error,
                )
            },
            ES_ERR_STATE
        );
        unsafe { es_buf_free(&raw mut error) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn renewal_excludes_replay_and_restores_the_token_when_interrupted() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let job_id = JobId::new().to_string();
        enqueue_test_job(&mut store, &job_id, 1_000);
        let token = claim_test_job(&mut store, &job_id, 1_000, 0, 0);

        let lease = EsStore::acquire(&raw mut store).unwrap();
        let renewal = lease.inner.begin_renewal(&token).unwrap();
        let pruning_reservation = lease.inner.prepare_claim(100_000).unwrap();
        assert!(matches!(
            lease.inner.begin_renewal(&token),
            Err(AbiError::State(StateFault::Claim(
                ClaimFault::InvalidHandle
            )))
        ));
        drop(pruning_reservation);
        drop(renewal);
        drop(lease);

        let mut ack_encoded = encode_request(&(1_u8, OpaqueBytes(token)));
        let ack = caller_buffer(&mut ack_encoded);
        let mut settlement = u8::MAX;
        let mut error = empty_buffer();
        assert_eq!(
            unsafe {
                es_job_ack(
                    &raw mut store,
                    &raw const ack,
                    &raw mut settlement,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(settlement, 0);
        assert!(error.ptr.is_null());
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn concurrent_job_ack_consumes_exactly_one_claim_token() {
        const CALLERS: usize = 16;

        let mut store = ptr::null_mut();
        open_store(&mut store);
        let job_id = JobId::new().to_string();
        enqueue_test_job(&mut store, &job_id, 1_000);
        let token = claim_test_job(&mut store, &job_id, 1_000, 0, 0);
        let owner_address = (&raw mut store) as usize;
        let barrier = Arc::new(std::sync::Barrier::new(CALLERS));

        let results = std::thread::scope(|scope| {
            (0..CALLERS)
                .map(|_| {
                    let barrier = Arc::clone(&barrier);
                    let token = token.clone();
                    scope.spawn(move || {
                        let mut encoded = encode_request(&(1_u8, OpaqueBytes(token)));
                        let request = caller_buffer(&mut encoded);
                        let mut settlement = u8::MAX;
                        let mut error = empty_buffer();
                        barrier.wait();
                        let result = unsafe {
                            es_job_ack(
                                owner_address as *mut *mut EsStore,
                                &raw const request,
                                &raw mut settlement,
                                &raw mut error,
                            )
                        };
                        unsafe { es_buf_free(&raw mut error) };
                        (result, settlement)
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });

        assert_eq!(
            results
                .iter()
                .filter(|(result, settlement)| *result == ES_OK && *settlement == 0)
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|(result, settlement)| *result == ES_ERR_STATE && *settlement == 0)
                .count(),
            CALLERS - 1
        );
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn concurrent_claims_reserve_the_last_active_claim_slot_once() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let job_id = JobId::new().to_string();
        enqueue_test_job(&mut store, &job_id, 1_000);
        let token = ClaimToken::try_from(claim_test_job(&mut store, &job_id, 1_000, 0, 0)).unwrap();
        let lease = EsStore::acquire(&raw mut store).unwrap();
        let retained = {
            let claims = lock_ignoring_poison(&lease.inner.claims);
            claims.retained.get(&token).unwrap().handle.clone()
        };
        {
            let mut claims = lock_ignoring_poison(&lease.inner.claims);
            (1..(MAX_ACTIVE_CLAIMS - 1)).for_each(|index| {
                let mut fabricated_token = [0_u8; 16];
                fabricated_token[..8].copy_from_slice(&index.to_be_bytes());
                claims.retained.insert(
                    fabricated_token,
                    RetainedClaim {
                        handle: retained.clone(),
                        lease_until_ms: 31_000,
                        state: ClaimState::Available,
                    },
                );
            });
            assert_eq!(claims.retained.len(), MAX_ACTIVE_CLAIMS - 1);
            drop(claims);
        }

        let ready = Arc::new(std::sync::Barrier::new(2));
        let reserved = Arc::new(std::sync::Barrier::new(2));
        let results = std::thread::scope(|scope| {
            let first_ready = Arc::clone(&ready);
            let first_reserved = Arc::clone(&reserved);
            let first_inner = &lease.inner;
            let first = scope.spawn(move || {
                first_ready.wait();
                let reservation = first_inner.prepare_claim(1_000);
                first_reserved.wait();
                reservation.is_ok()
            });
            let second_inner = &lease.inner;
            let second = scope.spawn(move || {
                ready.wait();
                let reservation = second_inner.prepare_claim(1_000);
                reserved.wait();
                reservation.is_ok()
            });
            [first.join().unwrap(), second.join().unwrap()]
        });

        assert_eq!(results.into_iter().filter(|reserved| *reserved).count(), 1);
        drop(lease);
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn abandoned_job_claim_retires_the_expired_token() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let job_id = JobId::new().to_string();
        enqueue_test_job(&mut store, &job_id, 1_000);
        let token = claim_test_job(&mut store, &job_id, 1_000, 0, 0);

        let mut reclaim_encoded =
            encode_request(&(1_u8, job_id, "ffi-worker", 31_001_i64, 30_000_i64, 1_u32));
        let reclaim_request = caller_buffer(&mut reclaim_encoded);
        let mut reclaim = empty_buffer();
        let mut error = empty_buffer();
        assert_eq!(
            unsafe {
                es_job_claim(
                    &raw mut store,
                    &raw const reclaim_request,
                    &raw mut reclaim,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(
            decode_output::<JobClaimWire>(&reclaim),
            JobClaimWire(1, 1, None, None, None, None)
        );
        unsafe { es_buf_free(&raw mut reclaim) };

        let mut ack_encoded = encode_request(&(1_u8, OpaqueBytes(token)));
        let ack_request = caller_buffer(&mut ack_encoded);
        let mut settlement = u8::MAX;
        assert_eq!(
            unsafe {
                es_job_ack(
                    &raw mut store,
                    &raw const ack_request,
                    &raw mut settlement,
                    &raw mut error,
                )
            },
            ES_ERR_STATE
        );
        assert_eq!(settlement, 0);
        unsafe { es_buf_free(&raw mut error) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn failed_job_settlement_restores_the_claim_token() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let job_id = JobId::new().to_string();
        enqueue_test_job(&mut store, &job_id, 1_000);
        let token = claim_test_job(&mut store, &job_id, 1_000, 0, 0);
        let mut retry_encoded = encode_request(&(
            1_u8,
            OpaqueBytes(token.clone()),
            i64::MAX,
            "invalid instant",
        ));
        let retry_request = caller_buffer(&mut retry_encoded);
        let mut settlement = u8::MAX;
        let mut error = empty_buffer();

        assert_eq!(
            unsafe {
                es_job_retry(
                    &raw mut store,
                    &raw const retry_request,
                    &raw mut settlement,
                    &raw mut error,
                )
            },
            ES_ERR_DECODE
        );
        assert_eq!(settlement, 0);
        assert_eq!(
            decode_error(&error),
            (1, ES_ERR_DECODE, Value::Text("malformed input".to_string()),)
        );
        unsafe { es_buf_free(&raw mut error) };

        let mut ack_encoded = encode_request(&(1_u8, OpaqueBytes(token)));
        let ack_request = caller_buffer(&mut ack_encoded);
        assert_eq!(
            unsafe {
                es_job_ack(
                    &raw mut store,
                    &raw const ack_request,
                    &raw mut settlement,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(settlement, 0);
        assert!(error.ptr.is_null());
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn commit_with_job_is_atomic_and_invalid_job_instants_do_not_persist() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let job_id = JobId::new().to_string();
        let mut commit_encoded = encode_request(&(
            1_u8,
            "ffi-domain",
            "one",
            0_u64,
            vec![("Created", "1.0", OpaqueBytes(vec![7]))],
            (job_id.clone(), "ffi-test", OpaqueBytes(vec![42]), 1_000_i64),
        ));
        let commit = caller_buffer(&mut commit_encoded);
        let mut error = empty_buffer();
        assert_eq!(
            unsafe { es_commit_with_job(&raw mut store, &raw const commit, &raw mut error) },
            ES_OK
        );

        let mut poll_encoded = encode_request(&(1_u8, "ffi-test", 1_000_i64, 10_u32));
        let poll = caller_buffer(&mut poll_encoded);
        let mut jobs = empty_buffer();
        assert_eq!(
            unsafe {
                es_job_poll(
                    &raw mut store,
                    &raw const poll,
                    &raw mut jobs,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(decode_output::<(u8, Vec<String>)>(&jobs), (1, vec![job_id]));
        unsafe { es_buf_free(&raw mut jobs) };

        let invalid_job_id = JobId::new().to_string();
        let mut invalid_encoded = encode_request(&(
            1_u8,
            invalid_job_id,
            "ffi-test",
            OpaqueBytes(vec![1]),
            i64::MAX,
        ));
        let invalid = caller_buffer(&mut invalid_encoded);
        assert_eq!(
            unsafe { es_job_enqueue(&raw mut store, &raw const invalid, &raw mut error) },
            ES_ERR_DECODE
        );
        assert_eq!(
            decode_error(&error),
            (1, ES_ERR_DECODE, Value::Text("malformed input".to_string()),)
        );
        unsafe { es_buf_free(&raw mut error) };

        let rollback_job_id = JobId::new().to_string();
        let rollback_job_kind = "ffi-rollback";
        let mut rollback_encoded = encode_request(&(
            1_u8,
            "ffi-domain",
            "rollback",
            0_u64,
            vec![("Created", "1.0", OpaqueBytes(vec![9]))],
            (
                rollback_job_id,
                rollback_job_kind,
                OpaqueBytes(vec![9]),
                i64::MAX,
            ),
        ));
        let rollback = caller_buffer(&mut rollback_encoded);
        assert_eq!(
            unsafe { es_commit_with_job(&raw mut store, &raw const rollback, &raw mut error) },
            ES_ERR_DECODE,
            "an out-of-range job instant is the same caller fault on either entry point"
        );
        assert_eq!(
            decode_error(&error),
            (1, ES_ERR_DECODE, Value::Text("malformed input".to_string()),)
        );
        unsafe { es_buf_free(&raw mut error) };
        let mut version_encoded = encode_request(&(1_u8, "ffi-domain", "rollback"));
        let version_request = caller_buffer(&mut version_encoded);
        let mut version = u64::MAX;
        assert_eq!(
            unsafe {
                es_current_version(
                    &raw mut store,
                    &raw const version_request,
                    &raw mut version,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(version, 0);
        let mut rollback_poll_encoded =
            encode_request(&(1_u8, rollback_job_kind, i64::MAX, 10_u32));
        let rollback_poll = caller_buffer(&mut rollback_poll_encoded);
        let mut rollback_jobs = empty_buffer();
        assert_eq!(
            unsafe {
                es_job_poll(
                    &raw mut store,
                    &raw const rollback_poll,
                    &raw mut rollback_jobs,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(
            decode_output::<(u8, Vec<String>)>(&rollback_jobs),
            (1, Vec::new())
        );
        unsafe { es_buf_free(&raw mut rollback_jobs) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn retry_defer_and_dead_letter_consume_each_claim_once() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let job_id = JobId::new().to_string();
        enqueue_test_job(&mut store, &job_id, 1_000);
        let mut settlement = u8::MAX;
        let mut error = empty_buffer();

        let first = claim_test_job(&mut store, &job_id, 1_000, 0, 0);
        let mut retry_encoded = encode_request(&(1_u8, OpaqueBytes(first), 2_000_i64, "transient"));
        let retry = caller_buffer(&mut retry_encoded);
        assert_eq!(
            unsafe {
                es_job_retry(
                    &raw mut store,
                    &raw const retry,
                    &raw mut settlement,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(settlement, 0);
        assert_eq!(
            unsafe {
                es_job_retry(
                    &raw mut store,
                    &raw const retry,
                    &raw mut settlement,
                    &raw mut error,
                )
            },
            ES_ERR_STATE
        );
        assert_eq!(settlement, 0);
        unsafe { es_buf_free(&raw mut error) };

        let second = claim_test_job(&mut store, &job_id, 2_000, 1, 1);
        let mut defer_encoded = encode_request(&(1_u8, OpaqueBytes(second), 3_000_i64));
        let defer = caller_buffer(&mut defer_encoded);
        assert_eq!(
            unsafe {
                es_job_defer(
                    &raw mut store,
                    &raw const defer,
                    &raw mut settlement,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(settlement, 0);
        assert_eq!(
            unsafe {
                es_job_defer(
                    &raw mut store,
                    &raw const defer,
                    &raw mut settlement,
                    &raw mut error,
                )
            },
            ES_ERR_STATE
        );
        assert_eq!(settlement, 0);
        unsafe { es_buf_free(&raw mut error) };

        let third = claim_test_job(&mut store, &job_id, 3_000, 1, 1);
        let mut dead_encoded = encode_request(&(1_u8, OpaqueBytes(third), 1_u8, "terminal"));
        let dead = caller_buffer(&mut dead_encoded);
        assert_eq!(
            unsafe {
                es_job_dead_letter(
                    &raw mut store,
                    &raw const dead,
                    &raw mut settlement,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(settlement, 0);
        assert_eq!(
            unsafe {
                es_job_dead_letter(
                    &raw mut store,
                    &raw const dead,
                    &raw mut settlement,
                    &raw mut error,
                )
            },
            ES_ERR_STATE
        );
        assert_eq!(settlement, 0);
        unsafe { es_buf_free(&raw mut error) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn claim_tokens_outlive_their_local_lease_until_the_durable_job_settles() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let executing_job = JobId::new().to_string();
        enqueue_test_job(&mut store, &executing_job, 1_000);
        let token = claim_test_job(&mut store, &executing_job, 1_000, 0, 0);

        let unrelated_job = JobId::new().to_string();
        enqueue_test_job(&mut store, &unrelated_job, 1_000);
        let unrelated_token = claim_test_job(&mut store, &unrelated_job, 100_000, 0, 0);
        assert_ne!(unrelated_token, token);

        let mut ack_encoded = encode_request(&(1_u8, OpaqueBytes(token)));
        let ack = caller_buffer(&mut ack_encoded);
        let mut settlement = u8::MAX;
        let mut error = empty_buffer();
        assert_eq!(
            unsafe {
                es_job_ack(
                    &raw mut store,
                    &raw const ack,
                    &raw mut settlement,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(settlement, 0);
        assert!(error.ptr.is_null());
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn claim_capacity_evicts_the_longest_lapsed_token_before_refusing() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let job_id = JobId::new().to_string();
        enqueue_test_job(&mut store, &job_id, 1_000);
        let live = ClaimToken::try_from(claim_test_job(&mut store, &job_id, 1_000, 0, 0)).unwrap();
        let lease = EsStore::acquire(&raw mut store).unwrap();
        let handle = {
            let claims = lock_ignoring_poison(&lease.inner.claims);
            claims.retained.get(&live).unwrap().handle.clone()
        };
        let mut lapsed = [0_u8; 16];
        lapsed[0] = 1;
        let mut newer_lapsed = [0_u8; 16];
        newer_lapsed[0] = 2;
        {
            let mut claims = lock_ignoring_poison(&lease.inner.claims);
            claims.retained.insert(
                lapsed,
                RetainedClaim {
                    handle: handle.clone(),
                    lease_until_ms: 10,
                    state: ClaimState::Available,
                },
            );
            claims.retained.insert(
                newer_lapsed,
                RetainedClaim {
                    handle: handle.clone(),
                    lease_until_ms: 20,
                    state: ClaimState::Available,
                },
            );
            (3..MAX_ACTIVE_CLAIMS).for_each(|index| {
                let mut token = [0_u8; 16];
                token[..8].copy_from_slice(&index.to_be_bytes());
                claims.retained.insert(
                    token,
                    RetainedClaim {
                        handle: handle.clone(),
                        lease_until_ms: 31_000,
                        state: ClaimState::Available,
                    },
                );
            });
            assert_eq!(claims.retained.len(), MAX_ACTIVE_CLAIMS);
            drop(claims);
        }

        let reservation = lease.inner.prepare_claim(1_000).unwrap();
        {
            let claims = lock_ignoring_poison(&lease.inner.claims);
            assert!(
                !claims.retained.contains_key(&lapsed),
                "the longest-lapsed token frees the slot"
            );
            assert!(
                claims.retained.contains_key(&newer_lapsed),
                "only one token is evicted per reservation"
            );
            assert!(
                claims.retained.contains_key(&live),
                "a live claim is never evicted to make room"
            );
            drop(claims);
        }
        drop(reservation);
        drop(lease);
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn a_claim_whose_payload_cannot_be_returned_is_dead_lettered_as_undecodable() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let job_id = JobId::new().to_string();

        // A structured payload is what the native enqueue surface writes, and a
        // mixed deployment lets a foreign worker poll the same kind. The shape
        // is a property of the stored job, so re-arming it would loop forever.
        let lease = EsStore::acquire(&raw mut store).unwrap();
        lease
            .inner
            .runtime
            .block_on(lease.inner.jobs.enqueue_job_payload(
                job_id.parse::<JobId>().unwrap(),
                "ffi-test".to_string(),
                serde_json::json!({ "recipient": "ffi", "attempts": 3 }),
                1_000,
            ))
            .unwrap();
        drop(lease);

        assert_eq!(poll_test_jobs(&mut store, 1_000), vec![job_id.clone()]);

        let mut claim_encoded = encode_request(&(
            1_u8,
            job_id.as_str(),
            "ffi-worker",
            1_000_i64,
            30_000_i64,
            50_u32,
        ));
        let claim_request = caller_buffer(&mut claim_encoded);
        let mut claim = empty_buffer();
        let mut error = empty_buffer();
        assert_eq!(
            unsafe {
                es_job_claim(
                    &raw mut store,
                    &raw const claim_request,
                    &raw mut claim,
                    &raw mut error,
                )
            },
            ES_ERR_JOB_REFUSAL
        );
        assert!(claim.ptr.is_null());
        assert_eq!(
            decode_error(&error),
            (
                1,
                ES_ERR_JOB_REFUSAL,
                Value::Array(vec![
                    Value::Text(job_id.clone()),
                    Value::Text("claimed job payload is not opaque bytes".to_string()),
                ]),
            )
        );
        unsafe { es_buf_free(&raw mut error) };

        let events = load_page(&mut store, "job", &job_id, None);
        let StoredEventWire(_, event_type, _, OpaqueBytes(payload)) =
            events.last().expect("the job stream must record the claim");
        assert_eq!(event_type, "JobDead");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(payload).unwrap()["Dead"]["reason"],
            serde_json::json!("Undecodable")
        );

        assert_eq!(poll_test_jobs(&mut store, 100_000), Vec::<String>::new());
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn claim_requests_reject_an_unusable_lease_or_claim_budget_before_claiming() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let job_id = JobId::new().to_string();
        enqueue_test_job(&mut store, &job_id, 1_000);

        for (lease_ms, max_claims) in [(0_i64, 50_u32), (-1, 50), (30_000, 0)] {
            let mut encoded = encode_request(&(
                1_u8,
                job_id.as_str(),
                "ffi-worker",
                1_000_i64,
                lease_ms,
                max_claims,
            ));
            let request = caller_buffer(&mut encoded);
            let mut claim = empty_buffer();
            let mut error = empty_buffer();
            assert_eq!(
                unsafe {
                    es_job_claim(
                        &raw mut store,
                        &raw const request,
                        &raw mut claim,
                        &raw mut error,
                    )
                },
                ES_ERR_DECODE
            );
            assert!(claim.ptr.is_null());
            unsafe { es_buf_free(&raw mut error) };
        }

        let token = claim_test_job(&mut store, &job_id, 1_000, 0, 0);
        assert_eq!(token.len(), 16);
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn open_options_bound_the_busy_timeout_at_what_the_driver_accepts() {
        let mut accepted = encode_request(&(
            1_u8,
            "sqlite::memory:",
            u64::from(MAX_BUSY_TIMEOUT_MS),
            1_u32,
            1_u32,
        ));
        let accepted_buffer = caller_buffer(&mut accepted);

        let Ok(options) = decode_open_options(&raw const accepted_buffer) else {
            panic!("the largest timeout the driver accepts must open a store");
        };

        assert_eq!(
            options.busy_timeout,
            Duration::from_millis(u64::from(MAX_BUSY_TIMEOUT_MS))
        );

        let mut refused = encode_request(&(
            1_u8,
            "sqlite::memory:",
            u64::from(MAX_BUSY_TIMEOUT_MS) + 1,
            1_u32,
            1_u32,
        ));
        let refused_buffer = caller_buffer(&mut refused);

        assert!(matches!(
            decode_open_options(&raw const refused_buffer),
            Err(AbiError::ResourceLimit {
                resource: "busy_timeout_ms",
                observed,
                limit,
            }) if observed == usize::try_from(u64::from(MAX_BUSY_TIMEOUT_MS) + 1).unwrap()
                && limit == usize::try_from(MAX_BUSY_TIMEOUT_MS).unwrap()
        ));
    }

    #[test]
    fn resource_limits_name_identifiers_and_error_text_apart() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let job_id = JobId::new().to_string();
        enqueue_test_job(&mut store, &job_id, 1_000);
        let token = claim_test_job(&mut store, &job_id, 1_000, 0, 0);
        let mut error = empty_buffer();

        let mut oversized_kind = encode_request(&(
            1_u8,
            "x".repeat(MAX_IDENTIFIER_BYTES + 1),
            1_000_i64,
            10_u32,
        ));
        let poll = caller_buffer(&mut oversized_kind);
        let mut jobs = empty_buffer();
        assert_eq!(
            unsafe {
                es_job_poll(
                    &raw mut store,
                    &raw const poll,
                    &raw mut jobs,
                    &raw mut error,
                )
            },
            ES_ERR_RESOURCE_LIMIT
        );
        assert_eq!(
            decode_error(&error),
            (
                1,
                ES_ERR_RESOURCE_LIMIT,
                Value::Array(vec![
                    Value::Text("identifier_text".to_string()),
                    Value::Integer((MAX_IDENTIFIER_BYTES + 1).into()),
                    Value::Integer(MAX_IDENTIFIER_BYTES.into()),
                ]),
            )
        );
        unsafe { es_buf_free(&raw mut error) };

        let mut oversized_error = encode_request(&(
            1_u8,
            OpaqueBytes(token),
            2_000_i64,
            "x".repeat(MAX_ERROR_TEXT_BYTES + 1),
        ));
        let retry = caller_buffer(&mut oversized_error);
        let mut settlement = u8::MAX;
        assert_eq!(
            unsafe {
                es_job_retry(
                    &raw mut store,
                    &raw const retry,
                    &raw mut settlement,
                    &raw mut error,
                )
            },
            ES_ERR_RESOURCE_LIMIT
        );
        assert_eq!(
            decode_error(&error),
            (
                1,
                ES_ERR_RESOURCE_LIMIT,
                Value::Array(vec![
                    Value::Text("error_detail_text".to_string()),
                    Value::Integer((MAX_ERROR_TEXT_BYTES + 1).into()),
                    Value::Integer(MAX_ERROR_TEXT_BYTES.into()),
                ]),
            )
        );
        unsafe { es_buf_free(&raw mut error) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn an_unrepresentable_lease_instant_is_reported_as_a_job_refusal() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let job_id = JobId::new().to_string();
        enqueue_test_job(&mut store, &job_id, 1_000);

        // Within `i64` but outside chrono's range, so the ABI's own overflow
        // guard passes and the job itself is what refuses the claim.
        let mut encoded = encode_request(&(
            1_u8,
            job_id.as_str(),
            "ffi-worker",
            i64::MAX - 1,
            1_i64,
            50_u32,
        ));
        let request = caller_buffer(&mut encoded);
        let mut claim = empty_buffer();
        let mut error = empty_buffer();

        assert_eq!(
            unsafe {
                es_job_claim(
                    &raw mut store,
                    &raw const request,
                    &raw mut claim,
                    &raw mut error,
                )
            },
            ES_ERR_JOB_REFUSAL
        );
        assert!(claim.ptr.is_null());
        assert_eq!(
            decode_error(&error),
            (
                1,
                ES_ERR_JOB_REFUSAL,
                Value::Array(vec![
                    Value::Text(job_id),
                    Value::Text("invalid job claim policy".to_string()),
                ]),
            )
        );
        unsafe { es_buf_free(&raw mut error) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn a_re_claimed_job_reports_a_lost_renewal_and_a_fenced_settlement() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let job_id = JobId::new().to_string();
        enqueue_test_job(&mut store, &job_id, 1_000);
        let first = ClaimToken::try_from(claim_test_job(&mut store, &job_id, 1_000, 0, 0)).unwrap();
        let lease = EsStore::acquire(&raw mut store).unwrap();
        let stale = lock_ignoring_poison(&lease.inner.claims)
            .retained
            .get(&first)
            .unwrap()
            .handle
            .clone();
        drop(lease);

        // The first lease lapses at 31_000, so a re-claim wins the job and
        // fences the handle the first claim was executing under.
        let mut reclaim_encoded = encode_request(&(
            1_u8,
            job_id.as_str(),
            "ffi-worker",
            31_001_i64,
            30_000_i64,
            50_u32,
        ));
        let reclaim_request = caller_buffer(&mut reclaim_encoded);
        let mut reclaim = empty_buffer();
        let mut error = empty_buffer();
        assert_eq!(
            unsafe {
                es_job_claim(
                    &raw mut store,
                    &raw const reclaim_request,
                    &raw mut reclaim,
                    &raw mut error,
                )
            },
            ES_OK
        );
        let JobClaimWire(1, 0, Some(_), Some(_), Some(_), Some(_)) =
            decode_output::<JobClaimWire>(&reclaim)
        else {
            panic!("the lapsed job must be re-claimable");
        };
        unsafe { es_buf_free(&raw mut reclaim) };

        // The re-claim dropped the first token locally, so the stale handle is
        // reinstated under a token of its own to drive it through the ABI.
        let stale_token = [0xAA_u8; 16];
        let lease = EsStore::acquire(&raw mut store).unwrap();
        lock_ignoring_poison(&lease.inner.claims).retained.insert(
            stale_token,
            RetainedClaim {
                handle: stale.clone(),
                lease_until_ms: 61_000,
                state: ClaimState::Available,
            },
        );
        drop(lease);

        let mut renew_encoded =
            encode_request(&(1_u8, OpaqueBytes(stale_token.to_vec()), 61_000_i64));
        let renew = caller_buffer(&mut renew_encoded);
        let mut renewal = u8::MAX;
        assert_eq!(
            unsafe {
                es_job_renew(
                    &raw mut store,
                    &raw const renew,
                    &raw mut renewal,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(renewal, 1, "the lease belongs to the re-claimer");
        assert!(error.ptr.is_null());

        let lease = EsStore::acquire(&raw mut store).unwrap();
        assert!(
            !lock_ignoring_poison(&lease.inner.claims)
                .retained
                .contains_key(&stale_token),
            "a lost renewal retires the token it was renewing"
        );
        lock_ignoring_poison(&lease.inner.claims).retained.insert(
            stale_token,
            RetainedClaim {
                handle: stale,
                lease_until_ms: 61_000,
                state: ClaimState::Available,
            },
        );
        drop(lease);

        let mut ack_encoded = encode_request(&(1_u8, OpaqueBytes(stale_token.to_vec())));
        let ack = caller_buffer(&mut ack_encoded);
        let mut settlement = u8::MAX;
        assert_eq!(
            unsafe {
                es_job_ack(
                    &raw mut store,
                    &raw const ack,
                    &raw mut settlement,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(settlement, 1, "the re-claimer owns the outcome");
        assert!(error.ptr.is_null());
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn claim_tokens_do_not_repeat() {
        let tokens = (0..1_024)
            .map(|_| claim_token().unwrap())
            .collect::<std::collections::HashSet<_>>();

        assert_eq!(tokens.len(), 1_024);
    }

    #[test]
    fn job_poll_rejects_limits_above_the_response_item_bound() {
        let mut store = ptr::null_mut();
        open_store(&mut store);
        let limit = u32::try_from(MAX_LIST_ITEMS).unwrap() + 1;
        let mut poll_encoded = encode_request(&(1_u8, "ffi-test", 1_000_i64, limit));
        let poll = caller_buffer(&mut poll_encoded);
        let mut jobs = empty_buffer();
        let mut error = empty_buffer();

        assert_eq!(
            unsafe {
                es_job_poll(
                    &raw mut store,
                    &raw const poll,
                    &raw mut jobs,
                    &raw mut error,
                )
            },
            ES_ERR_RESOURCE_LIMIT
        );
        assert!(jobs.ptr.is_null());
        unsafe { es_buf_free(&raw mut error) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }
}
