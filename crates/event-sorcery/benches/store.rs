//! End-to-end write/read-path benchmarks over a real in-memory SQLite store:
//! pure-event commits, dispatch commits (event + transactional enqueue),
//! entity loads, and standalone enqueues.
//!
//! Run with: `cargo bench --features test-support --bench store`

use std::fmt::Debug;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use criterion::{Criterion, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::runtime::Runtime;

use event_sorcery::{
    DispatchEvent, DispatchOutcome, DispatchRefused, DispatchedJob, Effect, EventSourced, Job,
    JobContext, JobFailure, JobOutcome, JobRuntime, Label, Never, Nil, Reconciliation,
    StandaloneJob, Store, StoreBuilder, fx, jobs,
};

fn ok<Value, Error: Debug>(result: Result<Value, Error>) -> Value {
    result.unwrap_or_else(|error| panic!("bench setup failed: {error:?}"))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BenchJob {
    origin: u64,
    amount: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct BenchEntity {
    notes: u64,
    work: DispatchedJob<BenchJob>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum BenchEvent {
    Noted,
    Dispatch(DispatchEvent<BenchJob>),
}

impl From<DispatchEvent<BenchJob>> for BenchEvent {
    fn from(event: DispatchEvent<BenchJob>) -> Self {
        Self::Dispatch(event)
    }
}

#[derive(Debug, Clone)]
enum BenchCommand {
    Note,
    Kick,
    Settle(DispatchOutcome<BenchJob>),
}

impl From<DispatchOutcome<BenchJob>> for BenchCommand {
    fn from(outcome: DispatchOutcome<BenchJob>) -> Self {
        Self::Settle(outcome)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
enum BenchError {
    #[error(transparent)]
    Refused(#[from] DispatchRefused),
}

impl cqrs_es::DomainEvent for BenchEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Noted => "BenchEvent::Noted".to_string(),
            Self::Dispatch(_) => "BenchEvent::Dispatch".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[async_trait::async_trait]
impl EventSourced for BenchEntity {
    type Id = u64;
    type Error = BenchError;
    type Command = BenchCommand;
    type Event = BenchEvent;
    type Materialized = Nil;
    type Jobs = jobs![BenchJob];

    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 1;
    const AGGREGATE_TYPE: &'static str = "BenchEntity";

    fn originate(event: &BenchEvent) -> Option<Self> {
        use BenchEvent::{Dispatch, Noted};

        match event {
            Noted => Some(Self {
                notes: 1,
                work: DispatchedJob::Idle,
            }),

            Dispatch(dispatch_event) => {
                let work = DispatchedJob::originate(dispatch_event).ok()?;
                Some(Self { notes: 0, work })
            }
        }
    }

    fn evolve(entity: &Self, event: &BenchEvent) -> Result<Option<Self>, BenchError> {
        use BenchEvent::{Dispatch, Noted};

        match event {
            Noted => Ok(Some(Self {
                notes: entity.notes + 1,
                work: entity.work.clone(),
            })),

            Dispatch(dispatch_event) => {
                Ok(entity.work.evolve(dispatch_event).ok().map(|work| Self {
                    notes: entity.notes,
                    work,
                }))
            }
        }
    }

    async fn initialize(command: BenchCommand) -> Result<Effect<Self>, BenchError> {
        use BenchCommand::{Kick, Note, Settle};

        match command {
            Note => fx(BenchEvent::Noted),

            Kick => fx(BenchJob {
                origin: 0,
                amount: 100,
            }),

            Settle(_) => fx(BenchError::Refused(DispatchRefused::OutcomeMismatch)),
        }
    }

    async fn transition(&self, command: BenchCommand) -> Result<Effect<Self>, BenchError> {
        use BenchCommand::{Kick, Note, Settle};

        match command {
            Note => fx(BenchEvent::Noted),

            Kick => fx(self.work.dispatch(BenchJob {
                origin: 0,
                amount: 100,
            })?),

            Settle(outcome) => fx(self.work.settle(outcome)?),
        }
    }
}

impl Job for BenchJob {
    type Input = ();
    type Output = u64;
    type Error = Never;
    type Origin = BenchEntity;

    const WORKER_NAME: &'static str = "bench-job";
    const KIND: &'static str = "bench-job";

    fn label(&self) -> Label {
        Label::new("bench-job")
    }

    fn origin_id(&self) -> u64 {
        self.origin
    }

    async fn submit(
        &self,
        _ctx: &JobContext,
        _input: &(),
    ) -> Result<JobOutcome<u64>, JobFailure<Never>> {
        Ok(JobOutcome::Done(self.amount))
    }

    async fn reconcile(
        &self,
        _ctx: &JobContext,
        _input: &(),
    ) -> Result<Reconciliation<u64>, JobFailure<Never>> {
        Ok(Reconciliation::Settled(self.amount))
    }
}

/// Origin-less job for the ADR-0007 standalone enqueue path.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchSweep;

impl StandaloneJob for BenchSweep {
    type Input = ();
    type Output = ();
    type Error = Never;

    const WORKER_NAME: &'static str = "bench-sweep";
    const KIND: &'static str = "bench-sweep";

    fn label(&self) -> Label {
        Label::new("bench-sweep")
    }

    async fn perform(
        &self,
        _ctx: &JobContext,
        _input: &(),
    ) -> Result<JobOutcome<()>, JobFailure<Never>> {
        Ok(JobOutcome::Done(()))
    }
}

struct Fixture {
    store: Arc<Store<BenchEntity>>,
    jobs: JobRuntime,
    next_id: AtomicU64,
}

async fn fixture() -> Fixture {
    let pool = ok(SqlitePool::connect(":memory:").await);
    ok(sqlite_es::MIGRATOR.run(&pool).await);
    let store = ok(StoreBuilder::<BenchEntity>::new(pool.clone()).build().await);
    let jobs = ok(JobRuntime::build(pool).await);
    Fixture {
        store,
        jobs,
        next_id: AtomicU64::new(1),
    }
}

fn bench_store_paths(criterion: &mut Criterion) {
    let runtime = ok(Runtime::new());
    let shared = runtime.block_on(fixture());

    let mut group = criterion.benchmark_group("store");

    group.bench_function("send_pure_event", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            let id = shared.next_id.fetch_add(1, Ordering::Relaxed);
            ok(shared.store.send(&id, BenchCommand::Note).await);
        });
    });

    group.bench_function("send_dispatch_with_enqueue_flush", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            let id = shared.next_id.fetch_add(1, Ordering::Relaxed);
            ok(shared.store.send(&id, BenchCommand::Kick).await);
        });
    });

    group.bench_function("standalone_enqueue", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            ok(shared.jobs.enqueue(BenchSweep).await);
        });
    });

    let loaded_id = runtime.block_on(async {
        let id = shared.next_id.fetch_add(1, Ordering::Relaxed);
        for _ in 0..10 {
            ok(shared.store.send(&id, BenchCommand::Note).await);
        }
        id
    });
    group.bench_function("load_entity_10_events", |bencher| {
        bencher.to_async(&runtime).iter(|| async {
            ok(shared.store.load(black_box(&loaded_id)).await);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_store_paths);
criterion_main!(benches);
