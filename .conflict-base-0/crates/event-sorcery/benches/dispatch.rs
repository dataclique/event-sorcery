//! Micro-benchmarks for the pure dispatch state machine: the guard, the fold,
//! settling, and the serde round trip every snapshot/view write pays.
//!
//! Run with: `cargo bench --features test-support --bench dispatch`

use std::fmt::Debug;
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};

use event_sorcery::{
    DispatchEvent, DispatchOutcome, DispatchRefused, DispatchedJob, Effect, EventSourced, Job,
    JobContext, JobFailure, JobId, JobOutcome, Label, Never, Nil, Reconciliation, Settled, fx,
    jobs,
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
    work: DispatchedJob<BenchJob>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum BenchEvent {
    Dispatch(DispatchEvent<BenchJob>),
}

impl From<DispatchEvent<BenchJob>> for BenchEvent {
    fn from(event: DispatchEvent<BenchJob>) -> Self {
        Self::Dispatch(event)
    }
}

#[derive(Debug, Clone)]
enum BenchCommand {
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
        "BenchEvent".to_string()
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
        let BenchEvent::Dispatch(dispatch_event) = event;
        let work = DispatchedJob::originate(dispatch_event).ok()?;
        Some(Self { work })
    }

    fn evolve(entity: &Self, event: &BenchEvent) -> Result<Option<Self>, BenchError> {
        let BenchEvent::Dispatch(dispatch_event) = event;
        Ok(entity
            .work
            .evolve(dispatch_event)
            .ok()
            .map(|work| Self { work }))
    }

    async fn initialize(command: BenchCommand) -> Result<Effect<Self>, BenchError> {
        let BenchCommand::Settle(_) = command;
        fx(BenchError::Refused(DispatchRefused::OutcomeMismatch))
    }

    async fn transition(&self, command: BenchCommand) -> Result<Effect<Self>, BenchError> {
        let BenchCommand::Settle(outcome) = command;
        fx(self.work.settle(outcome)?)
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

fn job() -> BenchJob {
    BenchJob {
        origin: 7,
        amount: 100,
    }
}

fn bench_dispatch_machine(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("dispatch-machine");

    group.bench_function("dispatch_guarded_kickoff", |bencher| {
        let idle = DispatchedJob::<BenchJob>::Idle;
        bencher.iter(|| ok(idle.dispatch(black_box(job()))));
    });

    group.bench_function("evolve_dispatched", |bencher| {
        let idle = DispatchedJob::<BenchJob>::Idle;
        let event = DispatchEvent::Dispatched {
            job_id: JobId::new(),
            job: job(),
        };
        bencher.iter(|| ok(idle.evolve(black_box(&event))));
    });

    group.bench_function("settle_confirmed", |bencher| {
        let job_id = JobId::new();
        let in_flight = DispatchedJob::<BenchJob>::InFlight { job_id };
        bencher.iter(|| {
            ok(
                in_flight.settle(black_box(DispatchOutcome::simulated_confirmed(
                    job_id, 100, 1,
                ))),
            )
        });
    });

    group.bench_function("state_serde_roundtrip", |bencher| {
        let job_id = JobId::new();
        let confirmed = DispatchedJob::<BenchJob>::Confirmed(Settled::simulated(job_id, 100, 1));
        bencher.iter(|| {
            let encoded = ok(serde_json::to_string(black_box(&confirmed)));
            let decoded: DispatchedJob<BenchJob> = ok(serde_json::from_str(&encoded));
            debug_assert_eq!(decoded, confirmed);
            decoded
        });
    });

    group.finish();
}

criterion_group!(benches, bench_dispatch_machine);
criterion_main!(benches);
