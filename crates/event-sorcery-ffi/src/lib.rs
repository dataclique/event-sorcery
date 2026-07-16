use std::fmt;
use std::io::Cursor;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use cqrs_es::AggregateError;
use cqrs_es::persist::SerializedEvent;
use event_sorcery::{
    CommitRequest, CompactionPolicy, DeadReason, Engine, EngineError, JobClaimHandle,
    JobClaimResult, JobId, JobLeaseResult, JobRuntime, JobSeed, JobSettlementResult,
    ReconcileError, SchemaReconciliation, SchemaTarget, SnapshotWrite, StreamIdentity,
};
use serde::de::{DeserializeOwned, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};

const ABI_MAJOR: u32 = 0;
const ABI_MINOR: u32 = 5;
const ES_OK: i32 = 0;
const ES_ERR_DECODE: i32 = 1;
const ES_ERR_CONFLICT: i32 = 2;
const ES_ERR_STORAGE: i32 = 4;
const ES_ERR_STATE: i32 = 5;
const ES_ERR_PANIC: i32 = 100;

type ProposedEventWire = (String, String, OpaqueBytes);
type CommitWire = (u8, String, String, u64, Vec<ProposedEventWire>);
type CommitWithJobWire = (
    u8,
    String,
    String,
    u64,
    Vec<ProposedEventWire>,
    (String, String, OpaqueBytes, i64),
);
type StoredEventWire = (u64, String, String, OpaqueBytes);
type StoredEventsWire = (u8, Vec<StoredEventWire>);
type StoredSnapshotWire = (u8, Option<(u64, u64, OpaqueBytes)>);
type JobClaimWire = (
    u8,
    u8,
    Option<OpaqueBytes>,
    Option<u32>,
    Option<u8>,
    Option<OpaqueBytes>,
);

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
    jobs: JobRuntime,
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
        let engine = Engine::new(pool.clone());
        runtime
            .block_on(engine.migrate())
            .map_err(|error| AbiError::Storage(error.to_string()))?;
        let jobs = runtime
            .block_on(JobRuntime::build(pool))
            .map_err(|error| AbiError::Storage(error.to_string()))?;
        let store = Box::new(EsStore {
            runtime,
            engine,
            jobs,
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
        let (stream, serialized) =
            commit_parts(version, aggregate_type, aggregate_id, expected, events)?;
        store
            .runtime
            .block_on(store.engine.commit(CommitRequest::new(stream, &serialized)))
            .map_err(AbiError::from)
    })
}

#[unsafe(no_mangle)]
/// Atomically appends domain events and one durable job intent.
///
/// # Safety
///
/// `store` must be open. `request` must reference readable bytes, and
/// `out_error` must be null or writable.
pub unsafe extern "C" fn es_commit_with_job(
    store: *mut EsStore,
    request: *const EsBuf,
    out_error: *mut EsBuf,
) -> i32 {
    let Some(store) = (unsafe { store.as_ref() }) else {
        return write_error(out_error, AbiError::State("store is null".to_string()));
    };
    ffi_call(Some(store), out_error, || {
        let (version, aggregate_type, aggregate_id, expected, events, job): CommitWithJobWire =
            decode(request)?;
        let (stream, serialized) =
            commit_parts(version, aggregate_type, aggregate_id, expected, events)?;
        let (job_id, kind, payload, run_at_ms) = job;
        let job_id = job_id
            .parse::<JobId>()
            .map_err(|error| AbiError::Decode(error.to_string()))?;
        let payload = serde_json::Value::Array(payload.0.into_iter().map(Into::into).collect());
        let request = CommitRequest::new(stream, &serialized)
            .with_job(JobSeed::new(job_id, kind, payload, run_at_ms));
        store
            .runtime
            .block_on(store.engine.commit(request))
            .map_err(AbiError::from)
    })
}

#[unsafe(no_mangle)]
/// Loads an opaque snapshot through the shared engine.
///
/// # Safety
///
/// `store` must be open. `request` must reference readable bytes.
/// `out_snapshot` must be writable, and `out_error` must be null or writable.
pub unsafe extern "C" fn es_snapshot_load(
    store: *mut EsStore,
    request: *const EsBuf,
    out_snapshot: *mut EsBuf,
    out_error: *mut EsBuf,
) -> i32 {
    let Some(store) = (unsafe { store.as_ref() }) else {
        return write_error(out_error, AbiError::State("store is null".to_string()));
    };
    ffi_call(Some(store), out_error, || {
        if out_snapshot.is_null() {
            return Err(AbiError::State("out_snapshot is null".to_string()));
        }
        let (version, aggregate_type, aggregate_id): (u8, String, String) = decode(request)?;
        require_version(version)?;
        let stream = StreamIdentity::new(aggregate_type, aggregate_id);
        let snapshot = store
            .runtime
            .block_on(store.engine.load_snapshot(&stream))
            .map_err(AbiError::from)?
            .map(|snapshot| -> Result<(u64, u64, OpaqueBytes), AbiError> {
                Ok((
                    u64::try_from(snapshot.current_sequence)
                        .map_err(|error| AbiError::Storage(error.to_string()))?,
                    u64::try_from(snapshot.current_snapshot)
                        .map_err(|error| AbiError::Storage(error.to_string()))?,
                    OpaqueBytes(opaque_json_bytes(snapshot.aggregate, "snapshot")?),
                ))
            })
            .transpose()?;
        let response: StoredSnapshotWire = (1, snapshot);
        unsafe { out_snapshot.write(encode(&response)?) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// Stores an opaque snapshot through the shared engine.
///
/// # Safety
///
/// `store` must be open. `request` must reference readable bytes.
/// `out_version` must be writable, and `out_error` must be null or writable.
pub unsafe extern "C" fn es_snapshot_store(
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
        let (version, aggregate_type, aggregate_id, sequence, payload): (
            u8,
            String,
            String,
            u64,
            OpaqueBytes,
        ) = decode(request)?;
        require_version(version)?;
        let sequence =
            usize::try_from(sequence).map_err(|error| AbiError::Decode(error.to_string()))?;
        let aggregate = serde_json::Value::Array(payload.0.into_iter().map(Into::into).collect());
        let stream = StreamIdentity::new(aggregate_type, aggregate_id);
        let snapshot_version = store
            .runtime
            .block_on(
                store
                    .engine
                    .store_snapshot(&stream, SnapshotWrite::new(aggregate, sequence)),
            )
            .map_err(AbiError::from)?;
        let snapshot_version = u64::try_from(snapshot_version)
            .map_err(|error| AbiError::Storage(error.to_string()))?;
        unsafe { out_version.write(snapshot_version) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// Discards an opaque snapshot through the shared engine.
///
/// # Safety
///
/// `store` must be open. `request` must reference readable bytes, and
/// `out_error` must be null or writable.
pub unsafe extern "C" fn es_snapshot_discard(
    store: *mut EsStore,
    request: *const EsBuf,
    out_error: *mut EsBuf,
) -> i32 {
    let Some(store) = (unsafe { store.as_ref() }) else {
        return write_error(out_error, AbiError::State("store is null".to_string()));
    };
    ffi_call(Some(store), out_error, || {
        let (version, aggregate_type, aggregate_id): (u8, String, String) = decode(request)?;
        require_version(version)?;
        let stream = StreamIdentity::new(aggregate_type, aggregate_id);
        store
            .runtime
            .block_on(store.engine.discard_snapshot(&stream))
            .map_err(AbiError::from)
    })
}

#[unsafe(no_mangle)]
/// Reconciles aggregate schema metadata through the shared engine.
///
/// # Safety
///
/// `store` must be open. `request` must reference readable bytes.
/// `out_reconciliation` must be writable, and `out_error` must be null or
/// writable.
pub unsafe extern "C" fn es_schema_reconcile(
    store: *mut EsStore,
    request: *const EsBuf,
    out_reconciliation: *mut u8,
    out_error: *mut EsBuf,
) -> i32 {
    let Some(store) = (unsafe { store.as_ref() }) else {
        return write_error(out_error, AbiError::State("store is null".to_string()));
    };
    ffi_call(Some(store), out_error, || {
        if out_reconciliation.is_null() {
            return Err(AbiError::State("out_reconciliation is null".to_string()));
        }
        let target = decode_schema_target(request)?;
        let reconciliation = store
            .runtime
            .block_on(store.engine.reconcile_schema(&target))
            .map_err(AbiError::from)?;
        let reconciliation = match reconciliation {
            SchemaReconciliation::Changed => 0,
            SchemaReconciliation::Unchanged => 1,
        };
        unsafe { out_reconciliation.write(reconciliation) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// Records an aggregate schema version after recovery completes.
///
/// # Safety
///
/// `store` must be open. `request` must reference readable bytes, and
/// `out_error` must be null or writable.
pub unsafe extern "C" fn es_schema_record(
    store: *mut EsStore,
    request: *const EsBuf,
    out_error: *mut EsBuf,
) -> i32 {
    let Some(store) = (unsafe { store.as_ref() }) else {
        return write_error(out_error, AbiError::State("store is null".to_string()));
    };
    ffi_call(Some(store), out_error, || {
        let target = decode_schema_target(request)?;
        store
            .runtime
            .block_on(store.engine.record_schema(&target))
            .map_err(AbiError::from)
    })
}

#[unsafe(no_mangle)]
/// Enqueues an erased job payload through the retained event-sourced job runtime.
///
/// # Safety
///
/// `store` must be open. `request` must reference readable bytes, and
/// `out_error` must be null or writable.
pub unsafe extern "C" fn es_job_enqueue(
    store: *mut EsStore,
    request: *const EsBuf,
    out_error: *mut EsBuf,
) -> i32 {
    let Some(store) = (unsafe { store.as_ref() }) else {
        return write_error(out_error, AbiError::State("store is null".to_string()));
    };
    ffi_call(Some(store), out_error, || {
        let (version, job_id, kind, payload, run_at_ms): (u8, String, String, OpaqueBytes, i64) =
            decode(request)?;
        require_version(version)?;
        let job_id = job_id
            .parse::<JobId>()
            .map_err(|error| AbiError::Decode(error.to_string()))?;
        let payload = serde_json::Value::Array(payload.0.into_iter().map(Into::into).collect());
        store
            .runtime
            .block_on(
                store
                    .jobs
                    .enqueue_job_payload(job_id, kind, payload, run_at_ms),
            )
            .map_err(job_runtime_error)
    })
}

#[unsafe(no_mangle)]
/// Polls the existing rebuildable job queue projection.
///
/// # Safety
///
/// `store` must be open. `request` must reference readable bytes.
/// `out_jobs` must be writable, and `out_error` must be null or writable.
pub unsafe extern "C" fn es_job_poll(
    store: *mut EsStore,
    request: *const EsBuf,
    out_jobs: *mut EsBuf,
    out_error: *mut EsBuf,
) -> i32 {
    let Some(store) = (unsafe { store.as_ref() }) else {
        return write_error(out_error, AbiError::State("store is null".to_string()));
    };
    ffi_call(Some(store), out_error, || {
        if out_jobs.is_null() {
            return Err(AbiError::State("out_jobs is null".to_string()));
        }
        let (version, kind, now_ms, limit): (u8, String, i64, u32) = decode(request)?;
        require_version(version)?;
        let jobs = store
            .runtime
            .block_on(store.jobs.poll_jobs(&kind, now_ms, limit))
            .map_err(job_runtime_error)?;
        unsafe { out_jobs.write(encode(&(1_u8, jobs))?) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// Claims one job through the canonical event-sourced claim planner.
///
/// # Safety
///
/// `store` must be open. `request` must reference readable bytes.
/// `out_claim` must be writable, and `out_error` must be null or writable.
pub unsafe extern "C" fn es_job_claim(
    store: *mut EsStore,
    request: *const EsBuf,
    out_claim: *mut EsBuf,
    out_error: *mut EsBuf,
) -> i32 {
    let Some(store) = (unsafe { store.as_ref() }) else {
        return write_error(out_error, AbiError::State("store is null".to_string()));
    };
    ffi_call(Some(store), out_error, || {
        if out_claim.is_null() {
            return Err(AbiError::State("out_claim is null".to_string()));
        }
        let (version, job_id, worker, now_ms, lease_ms, max_claims): (
            u8,
            String,
            String,
            i64,
            i64,
            u32,
        ) = decode(request)?;
        require_version(version)?;
        if lease_ms <= 0 || max_claims == 0 || now_ms.checked_add(lease_ms).is_none() {
            return Err(AbiError::Decode("invalid claim policy".to_string()));
        }
        let claim = store
            .runtime
            .block_on(
                store
                    .jobs
                    .claim_job(&job_id, &worker, now_ms, lease_ms, max_claims),
            )
            .map_err(job_runtime_error)?;
        let response: JobClaimWire = match claim {
            JobClaimResult::Claimed(claim) => {
                let attempt = claim.handle.attempt();
                let route = u8::from(!claim.handle.is_first_execution());
                (
                    1,
                    0,
                    Some(OpaqueBytes(encode_bytes(&claim.handle)?)),
                    Some(attempt),
                    Some(route),
                    Some(OpaqueBytes(opaque_json_bytes(claim.payload, "job")?)),
                )
            }
            JobClaimResult::Abandoned => (1, 1, None, None, None, None),
            JobClaimResult::Contended => (1, 2, None, None, None, None),
            JobClaimResult::Skipped => (1, 3, None, None, None, None),
        };
        unsafe { out_claim.write(encode(&response)?) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// Extends a won claim's projection-only lease.
///
/// # Safety
///
/// `store` must be open. `request` must reference readable bytes.
/// `out_renewal` must be writable, and `out_error` must be null or writable.
pub unsafe extern "C" fn es_job_renew(
    store: *mut EsStore,
    request: *const EsBuf,
    out_renewal: *mut u8,
    out_error: *mut EsBuf,
) -> i32 {
    let Some(store) = (unsafe { store.as_ref() }) else {
        return write_error(out_error, AbiError::State("store is null".to_string()));
    };
    ffi_call(Some(store), out_error, || {
        if out_renewal.is_null() {
            return Err(AbiError::State("out_renewal is null".to_string()));
        }
        let (version, handle, new_lease_until_ms): (u8, OpaqueBytes, i64) = decode(request)?;
        require_version(version)?;
        let handle = decode_bytes::<JobClaimHandle>(&handle.0)?;
        let renewal = store
            .runtime
            .block_on(store.jobs.renew_job(&handle, new_lease_until_ms))
            .map_err(job_runtime_error)?;
        let renewal = match renewal {
            JobLeaseResult::Held => 0,
            JobLeaseResult::Lost => 1,
        };
        unsafe { out_renewal.write(renewal) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// Acknowledges a won claim through the fenced cqrs-es job aggregate.
///
/// # Safety
///
/// `store` must be open. `request` must reference readable bytes.
/// `out_settlement` must be writable, and `out_error` must be null or writable.
pub unsafe extern "C" fn es_job_ack(
    store: *mut EsStore,
    request: *const EsBuf,
    out_settlement: *mut u8,
    out_error: *mut EsBuf,
) -> i32 {
    let Some(store) = (unsafe { store.as_ref() }) else {
        return write_error(out_error, AbiError::State("store is null".to_string()));
    };
    ffi_call(Some(store), out_error, || {
        if out_settlement.is_null() {
            return Err(AbiError::State("out_settlement is null".to_string()));
        }
        let (version, handle): (u8, OpaqueBytes) = decode(request)?;
        require_version(version)?;
        let handle = decode_bytes::<JobClaimHandle>(&handle.0)?;
        let settlement = store
            .runtime
            .block_on(store.jobs.acknowledge_job(handle))
            .map_err(job_runtime_error)?;
        unsafe { out_settlement.write(settlement_tag(settlement)) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// Records a failed attempt and schedules the claimed job to retry.
///
/// # Safety
///
/// `store` must be open. `request` must reference readable bytes.
/// `out_settlement` must be writable, and `out_error` must be null or writable.
pub unsafe extern "C" fn es_job_retry(
    store: *mut EsStore,
    request: *const EsBuf,
    out_settlement: *mut u8,
    out_error: *mut EsBuf,
) -> i32 {
    let Some(store) = (unsafe { store.as_ref() }) else {
        return write_error(out_error, AbiError::State("store is null".to_string()));
    };
    ffi_call(Some(store), out_error, || {
        if out_settlement.is_null() {
            return Err(AbiError::State("out_settlement is null".to_string()));
        }
        let (version, handle, run_at_ms, error): (u8, OpaqueBytes, i64, String) = decode(request)?;
        require_version(version)?;
        let handle = decode_bytes::<JobClaimHandle>(&handle.0)?;
        let settlement = store
            .runtime
            .block_on(store.jobs.retry_job(handle, run_at_ms, error))
            .map_err(job_runtime_error)?;
        unsafe { out_settlement.write(settlement_tag(settlement)) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// Productively defers the claimed job without recording a failed attempt.
///
/// # Safety
///
/// `store` must be open. `request` must reference readable bytes.
/// `out_settlement` must be writable, and `out_error` must be null or writable.
pub unsafe extern "C" fn es_job_defer(
    store: *mut EsStore,
    request: *const EsBuf,
    out_settlement: *mut u8,
    out_error: *mut EsBuf,
) -> i32 {
    let Some(store) = (unsafe { store.as_ref() }) else {
        return write_error(out_error, AbiError::State("store is null".to_string()));
    };
    ffi_call(Some(store), out_error, || {
        if out_settlement.is_null() {
            return Err(AbiError::State("out_settlement is null".to_string()));
        }
        let (version, handle, run_at_ms): (u8, OpaqueBytes, i64) = decode(request)?;
        require_version(version)?;
        let handle = decode_bytes::<JobClaimHandle>(&handle.0)?;
        let settlement = store
            .runtime
            .block_on(store.jobs.defer_job(handle, run_at_ms))
            .map_err(job_runtime_error)?;
        unsafe { out_settlement.write(settlement_tag(settlement)) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
/// Dead-letters the claimed job with one of the existing terminal reasons.
///
/// # Safety
///
/// `store` must be open. `request` must reference readable bytes.
/// `out_settlement` must be writable, and `out_error` must be null or writable.
pub unsafe extern "C" fn es_job_dead_letter(
    store: *mut EsStore,
    request: *const EsBuf,
    out_settlement: *mut u8,
    out_error: *mut EsBuf,
) -> i32 {
    let Some(store) = (unsafe { store.as_ref() }) else {
        return write_error(out_error, AbiError::State("store is null".to_string()));
    };
    ffi_call(Some(store), out_error, || {
        if out_settlement.is_null() {
            return Err(AbiError::State("out_settlement is null".to_string()));
        }
        let (version, handle, reason, error): (u8, OpaqueBytes, u8, String) = decode(request)?;
        require_version(version)?;
        let handle = decode_bytes::<JobClaimHandle>(&handle.0)?;
        let reason = dead_reason(reason)?;
        let settlement = store
            .runtime
            .block_on(store.jobs.dead_letter_job(handle, reason, error))
            .map_err(job_runtime_error)?;
        unsafe { out_settlement.write(settlement_tag(settlement)) };
        Ok(())
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

#[derive(Debug)]
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
            EngineError::Schema(ReconcileError::Aggregate(AggregateError::AggregateConflict)) => {
                Self::Conflict
            }
            EngineError::Schema(source @ ReconcileError::CompactedSnapshotClear { .. }) => {
                Self::State(source.to_string())
            }
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

fn decode_schema_target(buffer: *const EsBuf) -> Result<SchemaTarget, AbiError> {
    let (version, aggregate_type, schema_version, compaction): (u8, String, u64, u8) =
        decode(buffer)?;
    require_version(version)?;
    let compaction = match compaction {
        0 => CompactionPolicy::Retain,
        1 => CompactionPolicy::CompactAfterSnapshot,
        _ => return Err(AbiError::Decode("invalid compaction policy".to_string())),
    };

    Ok(SchemaTarget::new(
        aggregate_type,
        schema_version,
        compaction,
    ))
}

fn encode(value: &impl Serialize) -> Result<EsBuf, AbiError> {
    Ok(owned_buffer(encode_bytes(value)?))
}

fn encode_bytes(value: &impl Serialize) -> Result<Vec<u8>, AbiError> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes).map_err(|error| AbiError::State(error.to_string()))?;
    Ok(bytes)
}

fn decode_bytes<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, AbiError> {
    ciborium::from_reader(Cursor::new(bytes)).map_err(|error| AbiError::Decode(error.to_string()))
}

fn opaque_json_bytes(payload: serde_json::Value, resource: &str) -> Result<Vec<u8>, AbiError> {
    let serde_json::Value::Array(bytes) = payload else {
        return Err(AbiError::Storage(format!(
            "foreign {resource} payload is not opaque bytes"
        )));
    };
    bytes
        .into_iter()
        .map(|byte| {
            byte.as_u64()
                .and_then(|byte| u8::try_from(byte).ok())
                .ok_or_else(|| {
                    AbiError::Storage(format!("invalid foreign {resource} payload byte"))
                })
        })
        .collect()
}

fn job_runtime_error(error: impl fmt::Display) -> AbiError {
    AbiError::Storage(error.to_string())
}

const fn settlement_tag(result: JobSettlementResult) -> u8 {
    match result {
        JobSettlementResult::Applied => 0,
        JobSettlementResult::Fenced => 1,
    }
}

fn dead_reason(tag: u8) -> Result<DeadReason, AbiError> {
    match tag {
        0 => Ok(DeadReason::RetriesExhausted),
        1 => Ok(DeadReason::Rejected),
        2 => Ok(DeadReason::Undecodable),
        3 => Ok(DeadReason::Abandoned),
        _ => Err(AbiError::Decode("invalid dead-letter reason".to_string())),
    }
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

fn commit_parts(
    version: u8,
    aggregate_type: String,
    aggregate_id: String,
    expected: u64,
    events: Vec<ProposedEventWire>,
) -> Result<(StreamIdentity, Vec<SerializedEvent>), AbiError> {
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
                payload: serde_json::Value::Array(payload.0.into_iter().map(Into::into).collect()),
                metadata: serde_json::json!({ "$event-sorcery-ffi": 1 }),
            })
        })
        .collect::<Result<Vec<_>, AbiError>>()?;
    Ok((
        StreamIdentity::new(aggregate_type, aggregate_id),
        serialized,
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

    const ENCODING_VECTORS: &str = include_str!("../../../conformance/encoding-v1.vectors");

    fn conformance_vector(name: &str) -> Vec<u8> {
        let mut bytes = Vec::new();

        for encoded in ENCODING_VECTORS.lines() {
            let mut fields = encoded.split_ascii_whitespace();
            if fields.next() == Some(name) {
                bytes
                    .extend(fields.map(|byte| byte.parse::<u8>().expect("valid conformance byte")));
            }
        }

        assert!(!bytes.is_empty(), "missing conformance vector: {name}");
        bytes
    }

    fn encode_conformance(value: &impl Serialize) -> Vec<u8> {
        encode_bytes(value).expect("conformance value must encode")
    }

    fn input(value: &impl Serialize) -> (Vec<u8>, EsBuf) {
        let mut bytes = Vec::new();
        ciborium::into_writer(value, &mut bytes).unwrap();
        let buffer = EsBuf {
            ptr: bytes.as_mut_ptr(),
            len: bytes.len(),
        };
        (bytes, buffer)
    }

    fn open_test_store(error: &mut EsBuf) -> *mut EsStore {
        let (_bytes, options) = input(&(1_u8, "sqlite::memory:", 5_000_u64, 1_u32, 1_usize));
        let mut store = ptr::null_mut();
        assert_eq!(
            unsafe { es_open(&raw const options, &raw mut store, error) },
            ES_OK
        );
        store
    }

    fn enqueue_test_job(store: *mut EsStore, job_id: &str, error: &mut EsBuf) {
        let (_bytes, request) = input(&(
            1_u8,
            job_id,
            "haskell-test",
            OpaqueBytes(vec![42]),
            1_000_i64,
        ));
        assert_eq!(
            unsafe { es_job_enqueue(store, &raw const request, error) },
            ES_OK
        );
    }

    fn claim_test_job(
        store: *mut EsStore,
        job_id: &str,
        now_ms: i64,
        error: &mut EsBuf,
    ) -> OpaqueBytes {
        let (_bytes, request) =
            input(&(1_u8, job_id, "haskell-worker", now_ms, 30_000_i64, 50_u32));
        let mut output = EsBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        assert_eq!(
            unsafe { es_job_claim(store, &raw const request, &raw mut output, error) },
            ES_OK
        );
        let bytes = unsafe { std::slice::from_raw_parts(output.ptr, output.len) };
        let claimed: JobClaimWire = ciborium::from_reader(Cursor::new(bytes)).unwrap();
        unsafe { es_buf_free(&raw mut output) };
        let (1, 0, Some(handle), ..) = claimed else {
            panic!("expected a won claim");
        };
        handle
    }

    #[test]
    fn rust_codec_matches_the_shared_encoding_corpus() {
        let stream = ("account", "one");
        let proposed = ("Created", "1.0", OpaqueBytes(vec![0, 1]));
        let stored = (1_u64, "Created", "1.0", OpaqueBytes(vec![0, 1]));

        assert_eq!(
            encode_conformance(&(1_u8, "sqlite::memory:", 5_000_u64, 1_u32, 1_usize)),
            conformance_vector("open-options")
        );
        assert_eq!(
            encode_conformance(&(1_u8, stream.0, stream.1, Option::<u64>::None)),
            conformance_vector("load-stream")
        );
        assert_eq!(
            encode_conformance(&(1_u8, stream.0, stream.1)),
            conformance_vector("current-version")
        );
        assert_eq!(
            encode_conformance(&(1_u8, stream.0, stream.1, 0_u64, vec![proposed])),
            conformance_vector("commit")
        );
        assert_eq!(
            encode_conformance(&(1_u8, vec![stored])),
            conformance_vector("stored-events")
        );
        assert_eq!(
            encode_conformance(&(1_u8, ES_ERR_CONFLICT, "optimistic conflict")),
            conformance_vector("conflict-error")
        );
    }

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

    #[test]
    fn snapshot_exports_round_trip_opaque_payloads() {
        let mut error = EsBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        let mut store = open_test_store(&mut error);
        let (_commit_bytes, commit) = input(&(
            1_u8,
            "ffi-snapshot-test",
            "one",
            0_u64,
            vec![("Created", "1.0", OpaqueBytes(vec![1]))],
        ));
        assert_eq!(
            unsafe { es_commit(store, &raw const commit, &raw mut error) },
            ES_OK
        );

        let payload = vec![0_u8, 1, 2, 255];
        let (_store_bytes, store_request) = input(&(
            1_u8,
            "ffi-snapshot-test",
            "one",
            1_u64,
            OpaqueBytes(payload.clone()),
        ));
        let mut snapshot_version = 0_u64;
        assert_eq!(
            unsafe {
                es_snapshot_store(
                    store,
                    &raw const store_request,
                    &raw mut snapshot_version,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(snapshot_version, 1);

        let (_load_bytes, load_request) = input(&(1_u8, "ffi-snapshot-test", "one"));
        let mut output = EsBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        assert_eq!(
            unsafe {
                es_snapshot_load(
                    store,
                    &raw const load_request,
                    &raw mut output,
                    &raw mut error,
                )
            },
            ES_OK
        );
        let bytes = unsafe { std::slice::from_raw_parts(output.ptr, output.len) };
        let snapshot: StoredSnapshotWire = ciborium::from_reader(Cursor::new(bytes)).unwrap();
        assert_eq!(snapshot, (1, Some((1, 1, OpaqueBytes(payload)))));
        unsafe { es_buf_free(&raw mut output) };

        assert_eq!(
            unsafe { es_snapshot_discard(store, &raw const load_request, &raw mut error) },
            ES_OK
        );
        assert_eq!(
            unsafe {
                es_snapshot_load(
                    store,
                    &raw const load_request,
                    &raw mut output,
                    &raw mut error,
                )
            },
            ES_OK
        );
        let bytes = unsafe { std::slice::from_raw_parts(output.ptr, output.len) };
        let snapshot: StoredSnapshotWire = ciborium::from_reader(Cursor::new(bytes)).unwrap();
        assert_eq!(snapshot, (1, None));

        unsafe { es_buf_free(&raw mut output) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn job_exports_use_the_existing_event_sourced_runtime() {
        let mut options = Vec::new();
        ciborium::into_writer(
            &(1_u8, "sqlite::memory:", 5_000_u64, 1_u32, 1_usize),
            &mut options,
        )
        .unwrap();
        let options = EsBuf {
            ptr: options.as_mut_ptr(),
            len: options.len(),
        };
        let mut store = ptr::null_mut();
        let mut error = EsBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        assert_eq!(
            unsafe { es_open(&raw const options, &raw mut store, &raw mut error) },
            ES_OK
        );

        let job_id = event_sorcery::JobId::new().to_string();
        let payload = vec![0_u8, 1, 255];
        let mut enqueue = Vec::new();
        ciborium::into_writer(
            &(
                1_u8,
                job_id.clone(),
                "haskell-test",
                OpaqueBytes(payload.clone()),
                1_000_i64,
            ),
            &mut enqueue,
        )
        .unwrap();
        let enqueue = EsBuf {
            ptr: enqueue.as_mut_ptr(),
            len: enqueue.len(),
        };
        assert_eq!(
            unsafe { es_job_enqueue(store, &raw const enqueue, &raw mut error) },
            ES_OK
        );

        let mut poll = Vec::new();
        ciborium::into_writer(&(1_u8, "haskell-test", 1_000_i64, 10_u32), &mut poll).unwrap();
        let poll = EsBuf {
            ptr: poll.as_mut_ptr(),
            len: poll.len(),
        };
        let mut output = EsBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        assert_eq!(
            unsafe { es_job_poll(store, &raw const poll, &raw mut output, &raw mut error,) },
            ES_OK
        );
        let bytes = unsafe { std::slice::from_raw_parts(output.ptr, output.len) };
        let polled: (u8, Vec<String>) = ciborium::from_reader(Cursor::new(bytes)).unwrap();
        assert_eq!(polled, (1, vec![job_id.clone()]));
        unsafe { es_buf_free(&raw mut output) };

        let mut claim = Vec::new();
        ciborium::into_writer(
            &(
                1_u8,
                job_id,
                "haskell-worker",
                1_000_i64,
                30_000_i64,
                50_u32,
            ),
            &mut claim,
        )
        .unwrap();
        let claim = EsBuf {
            ptr: claim.as_mut_ptr(),
            len: claim.len(),
        };
        assert_eq!(
            unsafe { es_job_claim(store, &raw const claim, &raw mut output, &raw mut error,) },
            ES_OK
        );
        let bytes = unsafe { std::slice::from_raw_parts(output.ptr, output.len) };
        let claimed: JobClaimWire = ciborium::from_reader(Cursor::new(bytes)).unwrap();
        let (1, 0, Some(handle), Some(0), Some(0), Some(claimed_payload)) = claimed else {
            panic!("expected a first-execution claim");
        };
        assert_eq!(claimed_payload.0, payload);
        unsafe { es_buf_free(&raw mut output) };

        let mut renew = Vec::new();
        ciborium::into_writer(&(1_u8, &handle, 60_000_i64), &mut renew).unwrap();
        let renew = EsBuf {
            ptr: renew.as_mut_ptr(),
            len: renew.len(),
        };
        let mut renewal = 1_u8;
        assert_eq!(
            unsafe { es_job_renew(store, &raw const renew, &raw mut renewal, &raw mut error,) },
            ES_OK
        );
        assert_eq!(renewal, 0);

        let mut ack = Vec::new();
        ciborium::into_writer(&(1_u8, handle), &mut ack).unwrap();
        let ack = EsBuf {
            ptr: ack.as_mut_ptr(),
            len: ack.len(),
        };
        let mut settlement = 1_u8;
        assert_eq!(
            unsafe { es_job_ack(store, &raw const ack, &raw mut settlement, &raw mut error,) },
            ES_OK
        );
        assert_eq!(settlement, 0);
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn job_settlement_exports_append_existing_job_events() {
        let mut error = EsBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        let mut store = open_test_store(&mut error);
        let job_id = event_sorcery::JobId::new().to_string();
        enqueue_test_job(store, &job_id, &mut error);

        let first = claim_test_job(store, &job_id, 1_000, &mut error);
        let (_bytes, retry) = input(&(1_u8, first, 2_000_i64, "transient"));
        let mut settlement = 1_u8;
        assert_eq!(
            unsafe { es_job_retry(store, &raw const retry, &raw mut settlement, &raw mut error,) },
            ES_OK
        );
        assert_eq!(settlement, 0);

        let second = claim_test_job(store, &job_id, 2_000, &mut error);
        let (_bytes, defer) = input(&(1_u8, second, 3_000_i64));
        assert_eq!(
            unsafe { es_job_defer(store, &raw const defer, &raw mut settlement, &raw mut error,) },
            ES_OK
        );
        assert_eq!(settlement, 0);

        let third = claim_test_job(store, &job_id, 3_000, &mut error);
        let (_bytes, dead) = input(&(1_u8, third, 1_u8, "terminal"));
        assert_eq!(
            unsafe {
                es_job_dead_letter(store, &raw const dead, &raw mut settlement, &raw mut error)
            },
            ES_OK
        );
        assert_eq!(settlement, 0);
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn commit_with_job_keeps_domain_event_and_job_intent_atomic() {
        let mut error = EsBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        let mut store = open_test_store(&mut error);
        let job_id = event_sorcery::JobId::new().to_string();
        let (_bytes, request) = input(&(
            1_u8,
            "ffi-domain",
            "one",
            0_u64,
            vec![("Created", "1.0", OpaqueBytes(vec![7]))],
            (
                job_id.clone(),
                "haskell-test",
                OpaqueBytes(vec![42]),
                1_000_i64,
            ),
        ));

        assert_eq!(
            unsafe { es_commit_with_job(store, &raw const request, &raw mut error) },
            ES_OK
        );

        let (_bytes, version_request) = input(&(1_u8, "ffi-domain", "one"));
        let mut version = 0_u64;
        assert_eq!(
            unsafe {
                es_current_version(
                    store,
                    &raw const version_request,
                    &raw mut version,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(version, 1);

        let (_bytes, poll) = input(&(1_u8, "haskell-test", 1_000_i64, 10_u32));
        let mut jobs = EsBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        assert_eq!(
            unsafe { es_job_poll(store, &raw const poll, &raw mut jobs, &raw mut error) },
            ES_OK
        );
        let bytes = unsafe { std::slice::from_raw_parts(jobs.ptr, jobs.len) };
        let polled: (u8, Vec<String>) = ciborium::from_reader(Cursor::new(bytes)).unwrap();
        assert_eq!(polled, (1, vec![job_id]));
        unsafe { es_buf_free(&raw mut jobs) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn schema_exports_preserve_the_existing_recovery_protocol() {
        let mut error = EsBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        let mut store = open_test_store(&mut error);
        let (_bytes, request) = input(&(1_u8, "ffi-schema", 1_u64, 0_u8));
        let mut reconciliation = u8::MAX;

        assert_eq!(
            unsafe {
                es_schema_reconcile(
                    store,
                    &raw const request,
                    &raw mut reconciliation,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(reconciliation, 0);

        assert_eq!(
            unsafe { es_schema_record(store, &raw const request, &raw mut error) },
            ES_OK
        );

        reconciliation = u8::MAX;
        assert_eq!(
            unsafe {
                es_schema_reconcile(
                    store,
                    &raw const request,
                    &raw mut reconciliation,
                    &raw mut error,
                )
            },
            ES_OK
        );
        assert_eq!(reconciliation, 1);
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }

    #[test]
    fn schema_exports_reject_unknown_compaction_policy() {
        let mut error = EsBuf {
            ptr: ptr::null_mut(),
            len: 0,
        };
        let mut store = open_test_store(&mut error);
        let (_bytes, request) = input(&(1_u8, "ffi-schema", 1_u64, 2_u8));

        assert_eq!(
            unsafe { es_schema_record(store, &raw const request, &raw mut error) },
            ES_ERR_DECODE
        );

        unsafe { es_buf_free(&raw mut error) };
        assert_eq!(unsafe { es_close(&raw mut store) }, ES_OK);
    }
}
