//! Dispatch and settlement through the SQLite worker and origin store.

#[cfg(test)]
mod tests {
    use apalis_core::worker::event::Event;
    use serde::{Deserialize, Serialize};
    use std::{sync::Arc, time::Duration};
    use tokio::{sync::Notify, time::timeout};

    use event_sorcery::*;
    use sqlite_es::{SqliteBackend, testing::create_test_pool};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct PlaceOrder {
        origin: u64,
        amount: u32,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
    #[error("order rejected: {reason}")]
    struct OrderRejected {
        reason: String,
    }

    /// Reference embedded-dispatch entity: one `DispatchedJob<PlaceOrder>`
    /// field, events/commands/errors delegating to the machine.
    #[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
    struct Desk {
        order: DispatchedJob<PlaceOrder>,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum DeskEvent {
        Dispatch(DispatchEvent<PlaceOrder>),
    }

    impl From<DispatchEvent<PlaceOrder>> for DeskEvent {
        fn from(event: DispatchEvent<PlaceOrder>) -> Self {
            Self::Dispatch(event)
        }
    }

    #[derive(Debug, Clone)]
    enum DeskCommand {
        Place(PlaceOrder),
        Settle(DispatchOutcome<PlaceOrder>),
    }

    impl From<DispatchOutcome<PlaceOrder>> for DeskCommand {
        fn from(outcome: DispatchOutcome<PlaceOrder>) -> Self {
            Self::Settle(outcome)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
    enum DeskError {
        #[error(transparent)]
        Refused(#[from] DispatchRefused),
    }

    impl cqrs_es::DomainEvent for DeskEvent {
        fn event_type(&self) -> String {
            "DeskEvent".to_string()
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[async_trait::async_trait]
    impl EventSourced for Desk {
        type Id = u64;
        type Error = DeskError;
        type Command = DeskCommand;
        type Event = DeskEvent;
        type Materialized = Nil;
        type Jobs = jobs![PlaceOrder];

        const PROJECTION: Nil = Nil;
        const SCHEMA_VERSION: u64 = 1;
        const SNAPSHOT_SIZE: usize = 10;
        const AGGREGATE_TYPE: &'static str = "Desk";
        const COMPACTION_POLICY: CompactionPolicy = CompactionPolicy::Retain;

        fn originate(event: &DeskEvent) -> Option<Self> {
            let DeskEvent::Dispatch(dispatch_event) = event;
            let order = DispatchedJob::originate(dispatch_event).ok()?;
            Some(Self { order })
        }

        fn evolve(entity: &Self, event: &DeskEvent) -> Result<Option<Self>, DeskError> {
            let DeskEvent::Dispatch(dispatch_event) = event;
            Ok(entity
                .order
                .evolve(dispatch_event)
                .ok()
                .map(|order| Self { order }))
        }

        async fn initialize(command: DeskCommand) -> Result<Effect<Self>, DeskError> {
            match command {
                DeskCommand::Place(job) => fx(job),
                DeskCommand::Settle(_) => fx(DeskError::Refused(DispatchRefused::OutcomeMismatch)),
            }
        }

        async fn transition(&self, command: DeskCommand) -> Result<Effect<Self>, DeskError> {
            match command {
                DeskCommand::Place(job) => fx(self.order.dispatch(job)?),
                DeskCommand::Settle(outcome) => fx(self.order.settle(outcome)?),
            }
        }
    }

    impl Job for PlaceOrder {
        type Input = ();
        type Output = String;
        type Error = OrderRejected;
        type Origin = Desk;

        const WORKER_NAME: &'static str = "place-order";
        const KIND: &'static str = "place-order";

        fn label(&self) -> Label {
            Label::new(format!("place-order:{}", self.amount))
        }

        fn origin_id(&self) -> u64 {
            self.origin
        }

        async fn submit(
            &self,
            _ctx: &JobContext,
            _input: &(),
        ) -> Result<JobOutcome<String>, JobFailure<OrderRejected>> {
            Ok(JobOutcome::Done("filled".to_string()))
        }

        async fn reconcile(
            &self,
            _ctx: &JobContext,
            _input: &(),
        ) -> Result<Reconciliation<String>, JobFailure<OrderRejected>> {
            Ok(Reconciliation::Settled("filled".to_string()))
        }
    }

    fn place() -> PlaceOrder {
        PlaceOrder {
            origin: 7,
            amount: 100,
        }
    }

    fn confirmed(job_id: JobId) -> DispatchOutcome<PlaceOrder> {
        DispatchOutcome::simulated_confirmed(job_id, "filled".to_string(), 1)
    }

    #[tokio::test]
    async fn dispatched_job_settles_through_a_real_store() {
        let pool = create_test_pool().await.unwrap();
        let backend = SqliteBackend { pool: pool.clone() };
        let desk_store = StoreBuilder::<Desk, _>::with_backend(backend.clone())
            .build()
            .await
            .unwrap();

        desk_store
            .send(&7, DeskCommand::Place(place()))
            .await
            .unwrap();
        let desk = desk_store.load(&7).await.unwrap().unwrap();
        let DispatchedJob::InFlight { job_id } = desk.order else {
            panic!("expected the job in flight, got {:?}", desk.order);
        };

        // The guard refuses an overlapping dispatch through the full stack.
        let refused = desk_store
            .send(&7, DeskCommand::Place(place()))
            .await
            .unwrap_err();
        assert!(matches!(
            refused,
            AggregateError::UserError(LifecycleError::Apply(DeskError::Refused(
                DispatchRefused::InFlight
            )))
        ));

        let runtime = JobRuntime::build(backend).await.unwrap();
        let finished = Arc::new(Notify::new());
        let acknowledged = Arc::clone(&finished);
        let monitor = build_supervised_worker!(
            runtime, JobWorkerConfig::default(), Clock::system(),
            {PlaceOrder => JobInput::<PlaceOrder>::new((), desk_store.clone())}
        )
        .on_event(move |_context, event| {
            if matches!(event, Event::Success(_)) {
                acknowledged.notify_one();
            }
        });
        timeout(
            Duration::from_secs(5),
            monitor.run_with_signal(async move {
                finished.notified().await;
                Ok::<(), std::io::Error>(())
            }),
        )
        .await
        .unwrap()
        .unwrap();

        let event_types: Vec<String> = sqlx::query_scalar(
            "SELECT event_type
             FROM events
             WHERE aggregate_type = 'job' AND aggregate_id = ?
             ORDER BY sequence",
        )
        .bind(job_id.to_string())
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(event_types, ["JobEnqueued", "JobClaimed", "JobSucceeded"]);

        let settled = desk_store.load(&7).await.unwrap().unwrap();
        let DispatchedJob::Confirmed(settled_order) = &settled.order else {
            panic!("expected Confirmed, got {:?}", settled.order);
        };
        assert_eq!(settled_order.job_id(), job_id);
        assert_eq!(settled_order.output(), "filled");
        assert_eq!(settled_order.attempts(), 1);

        // An at-least-once redelivery of the same verdict is absorbed.
        desk_store
            .send(&7, DeskCommand::Settle(confirmed(job_id)))
            .await
            .unwrap();
        let after = desk_store.load(&7).await.unwrap().unwrap();
        assert_eq!(after.order, settled.order);
    }
}
