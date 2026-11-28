//! Durable worker behavior over the native SQLite backend.

#[cfg(test)]
mod tests {
    use apalis_core::{
        backend::{Backend as ApalisBackend, BackendConfig},
        error::BoxDynError,
        task::{Task, data::MissingDataError, from_request::FromRequest},
        worker::{context::WorkerContext, event::Event},
    };
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};
    use sqlx::SqlitePool;
    use std::{
        cell::Cell,
        convert::Infallible,
        future::{Future, poll_fn, ready},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU32, Ordering},
        },
        task::Poll,
        time::Duration,
    };
    use tokio::{sync::Notify, time::timeout};
    use tower_layer::Layer;
    use tower_service::Service;

    use event_sorcery::*;
    use sqlite_es::{SqliteBackend, insert_serialized_events_batch, testing::create_test_pool};

    #[derive(Serialize, Deserialize)]
    struct TestJob {
        n: u32,
    }

    impl StandaloneJob for TestJob {
        type Input = ();
        type Output = ();
        type Error = TestPerformError;

        const WORKER_NAME: &'static str = "test-worker";
        const KIND: &'static str = "test-job";
        const TERMINAL_FAILURE_MSG: &'static str = "Job failed after retries";

        fn label(&self) -> Label {
            Label::new(format!("test:{}", self.n))
        }

        fn perform(
            &self,
            _ctx: &JobContext,
            _input: &(),
        ) -> impl Future<Output = Result<JobOutcome<()>, JobFailure<Self::Error>>> + Send {
            ready(Ok(JobOutcome::Done(())))
        }
    }

    #[derive(Serialize, Deserialize)]
    struct WorkerProbe {
        value: Cell<u32>,
    }

    impl StandaloneJob for WorkerProbe {
        type Input = tokio::sync::mpsc::UnboundedSender<(JobId, u32)>;
        type Output = Cell<u32>;
        type Error = Infallible;

        const WORKER_NAME: &'static str = "worker-probe";
        const KIND: &'static str = "worker-probe";
        const TERMINAL_FAILURE_MSG: &'static str = "Worker probe failed";

        fn label(&self) -> Label {
            Label::new("worker-probe")
        }

        fn perform(
            &self,
            context: &JobContext,
            input: &Self::Input,
        ) -> impl Future<Output = Result<JobOutcome<Cell<u32>>, JobFailure<Infallible>>> + Send
        {
            let value = self.value.get();
            poll_fn(move |_| {
                input.send((context.job_id(), value)).unwrap();
                Poll::Ready(Ok(JobOutcome::Done(Cell::new(value))))
            })
        }
    }

    fn from_millis(millis: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(millis).expect("valid timestamp")
    }

    async fn enqueue(runtime: &JobRuntime<SqliteBackend>, run_at_ms: i64) -> JobId {
        let id = JobId::new();
        runtime
            .enqueue_job_payload(
                id,
                TestJob::KIND.to_owned(),
                serde_json::json!({"n": 1}),
                run_at_ms,
            )
            .await
            .unwrap();
        id
    }

    async fn event_types(pool: &SqlitePool, job_id: &JobId) -> Vec<String> {
        sqlx::query_scalar::<_, String>(
            "SELECT event_type FROM events WHERE aggregate_type = 'job' AND aggregate_id = ?1 \
             ORDER BY sequence",
        )
        .bind(job_id.to_string())
        .fetch_all(pool)
        .await
        .unwrap()
    }

    async fn queue_status(pool: &SqlitePool, job_id: &JobId) -> Option<String> {
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT status FROM job_queue_projection WHERE view_id = ?1",
        )
        .bind(job_id.to_string())
        .fetch_optional(pool)
        .await
        .unwrap()
        .flatten()
    }

    #[tokio::test]
    async fn invalid_backoff_is_rejected_before_claiming() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1_000).await;
        let config = JobWorkerConfig {
            backoff: Backoff {
                factor: f32::NAN,
                ..Backoff::default()
            },
            ..JobWorkerConfig::default()
        };
        let mut worker = EventStoreBackend::<TestJob, _>::new(
            runtime,
            "invalid-backoff",
            config,
            Clock::from_fn(|| from_millis(1_000)),
        );
        let context = WorkerContext::new("invalid-backoff");
        let result = poll_fn(|cx| worker.poll_next(cx, &context)).await.unwrap();
        let Err(error) = result else {
            panic!("invalid backoff must be rejected before claiming");
        };
        assert!(matches!(error, JobScheduleError::InvalidFactor { .. }));
        assert_eq!(event_types(&pool, &job_id).await, ["JobEnqueued"]);
        assert_eq!(
            queue_status(&pool, &job_id).await.as_deref(),
            Some("pending")
        );
    }

    #[tokio::test]
    async fn runtime_facade_polls_claims_renews_and_acknowledges_the_job_stream() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1_000).await;

        let candidates = runtime.poll_jobs(TestJob::KIND, 1_000, 10).await.unwrap();
        assert_eq!(candidates, vec![job_id.to_string()]);

        let JobClaimResult::Claimed(claim) = runtime
            .claim_job(job_id, "ffi-worker", 1_000, 30_000, 50)
            .await
            .unwrap()
        else {
            panic!("expected the facade to win the claim");
        };
        assert_eq!(claim.handle.job_id(), job_id);
        assert_eq!(claim.handle.attempt(), 0);
        assert!(claim.handle.is_first_execution());
        assert_eq!(claim.payload, serde_json::json!({ "n": 1 }));

        assert_eq!(
            runtime.renew_job(&claim.handle, 60_000).await.unwrap(),
            JobLeaseResult::Held
        );
        assert_eq!(
            runtime.acknowledge_job(claim.handle).await.unwrap(),
            JobSettlementResult::Applied
        );
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed", "JobSucceeded"]
        );
    }

    #[tokio::test]
    async fn runtime_facade_enqueues_an_erased_payload_through_the_job_stream() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = JobId::new();

        runtime
            .enqueue_job_payload(
                job_id,
                TestJob::KIND.to_string(),
                serde_json::json!([0, 1, 255]),
                1_000,
            )
            .await
            .unwrap();

        assert_eq!(event_types(&pool, &job_id).await, ["JobEnqueued"]);
        assert_eq!(
            runtime.poll_jobs(TestJob::KIND, 1_000, 10).await.unwrap(),
            [job_id.to_string()]
        );
    }

    #[tokio::test]
    async fn runtime_facade_rejects_an_out_of_range_enqueue_instant() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = JobId::new();

        let error = runtime
            .enqueue_job_payload(
                job_id,
                TestJob::KIND.to_string(),
                serde_json::json!({ "n": 1 }),
                i64::MAX,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, JobRuntimeError::InvalidInstant(i64::MAX)));
        assert_eq!(event_types(&pool, &job_id).await, Vec::<String>::new());
        assert_eq!(
            runtime
                .poll_jobs(TestJob::KIND, i64::MAX, 10)
                .await
                .unwrap(),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn runtime_facade_rejects_a_lease_instant_outside_chronos_range() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1_000).await;

        // The sum stays inside i64 but far outside chrono's range, so only the
        // representability check catches it.
        let error = runtime
            .claim_job(job_id, "ffi-worker", i64::MAX - 1, 1, 50)
            .await
            .unwrap_err();

        assert!(matches!(error, JobRuntimeError::InvalidClaimPolicy));
        assert_eq!(event_types(&pool, &job_id).await, ["JobEnqueued"]);
        // The candidate is untouched and still runnable, which is exactly why
        // reporting this as an ordinary skip would strand the caller.
        assert_eq!(
            runtime.poll_jobs(TestJob::KIND, 1_000, 10).await.unwrap(),
            [job_id.to_string()]
        );
    }

    #[tokio::test]
    async fn runtime_facade_retries_defers_and_dead_letters_through_job_commands() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1_000).await;

        let JobClaimResult::Claimed(first) = runtime
            .claim_job(job_id, "ffi-worker", 1_000, 30_000, 50)
            .await
            .unwrap()
        else {
            panic!("expected first claim");
        };
        assert_eq!(
            runtime
                .retry_job(first.handle, 2_000, "transient".to_string())
                .await
                .unwrap(),
            JobSettlementResult::Applied
        );

        let JobClaimResult::Claimed(second) = runtime
            .claim_job(job_id, "ffi-worker", 2_000, 30_000, 50)
            .await
            .unwrap()
        else {
            panic!("expected retry claim");
        };
        assert_eq!(second.handle.attempt(), 1);
        assert_eq!(
            runtime.defer_job(second.handle, 3_000).await.unwrap(),
            JobSettlementResult::Applied
        );

        let JobClaimResult::Claimed(third) = runtime
            .claim_job(job_id, "ffi-worker", 3_000, 30_000, 50)
            .await
            .unwrap()
        else {
            panic!("expected deferred claim");
        };
        assert_eq!(third.handle.attempt(), 1);
        assert_eq!(
            runtime
                .dead_letter_job(third.handle, DeadReason::Rejected, "terminal".to_string())
                .await
                .unwrap(),
            JobSettlementResult::Applied
        );

        assert_eq!(
            event_types(&pool, &job_id).await,
            [
                "JobEnqueued",
                "JobClaimed",
                "JobRetryScheduled",
                "JobClaimed",
                "JobRescheduled",
                "JobClaimed",
                "JobDead",
            ]
        );
    }

    #[tokio::test]
    async fn facade_settlement_is_fenced_when_a_reclaimer_holds_the_job() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1_000).await;

        let outcome = runtime
            .claim_job(job_id, "w1", 1_000, 30_000, 50)
            .await
            .unwrap();
        let JobClaimResult::Claimed(stale) = outcome else {
            panic!("expected the first claim to win");
        };
        let JobClaimResult::Claimed(_) = runtime
            .claim_job(job_id, "w2", 1_000 + 31_000, 30_000, 50)
            .await
            .unwrap()
        else {
            panic!("expected the expired lease to be re-claimable");
        };

        assert_eq!(
            runtime.acknowledge_job(stale.handle).await.unwrap(),
            JobSettlementResult::Fenced
        );
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed", "JobClaimed"]
        );
    }

    #[tokio::test]
    async fn the_native_worker_adapter_claims_through_the_shared_facade() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1_000).await;
        let mut worker = EventStoreBackend::<TestJob, _>::new(
            runtime,
            "native-worker",
            JobWorkerConfig::default(),
            Clock::from_fn(|| from_millis(1_000)),
        );
        let context = WorkerContext::new("native-worker");
        let task =
            futures_util::future::poll_fn(|cx| ApalisBackend::poll_next(&mut worker, cx, &context))
                .await
                .expect("worker remains live")
                .unwrap();
        let claimed = JobContext::from_request(&task).await.unwrap();

        assert_eq!(claimed.job_id(), job_id);
        assert_eq!(claimed.attempt(), 0);
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed"]
        );
    }

    #[tokio::test]
    async fn native_worker_runs_a_send_only_job_and_durably_acknowledges_success() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = JobId::new();
        runtime
            .enqueue_job_payload(
                job_id,
                WorkerProbe::KIND.to_string(),
                serde_json::json!({"value": 7}),
                1_000,
            )
            .await
            .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<(JobId, u32)>();
        let worker_errors = Arc::new(Mutex::new(Vec::new()));
        let captured_errors = worker_errors.clone();
        let task_finished = Arc::new(Notify::new());
        let event_finished = task_finished.clone();
        let monitor = build_supervised_worker!(
            runtime,
            JobWorkerConfig::default(),
            Clock::from_fn(|| from_millis(1_000)),
            { WorkerProbe => sender }
        )
        .on_event(move |_context, event| {
            if matches!(event, Event::Success(_)) {
                event_finished.notify_one();
            }
            if let Some(error) = event.as_error() {
                captured_errors.lock().unwrap().push(error.to_string());
            }
        });
        let signal = async move {
            task_finished.notified().await;
            Ok::<(), std::io::Error>(())
        };

        let completion = timeout(Duration::from_secs(5), monitor.run_with_signal(signal)).await;

        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed", "JobSucceeded"],
            "worker completion: {completion:?}; worker errors: {:?}",
            worker_errors.lock().unwrap()
        );
        completion
            .expect("worker must execute and finish its durable acknowledgement")
            .unwrap();
        assert_eq!(receiver.try_recv().unwrap(), (job_id, 7));
        assert_eq!(queue_status(&pool, &job_id).await.as_deref(), Some("done"));
    }

    #[tokio::test]
    async fn native_worker_remains_live_after_an_empty_scan() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = JobId::new();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<(JobId, u32)>();
        let empty_scan = Arc::new(Notify::new());
        let poll_completed = empty_scan.clone();
        let poll_started = AtomicBool::new(false);
        let clock = Clock::from_fn(move || {
            // The next tick begins only after the first empty scan and idle delay.
            if poll_started.swap(true, Ordering::SeqCst) {
                poll_completed.notify_one();
            }
            from_millis(1_000)
        });
        let task_finished = Arc::new(Notify::new());
        let event_finished = task_finished.clone();
        let monitor = build_supervised_worker!(
            runtime.clone(),
            JobWorkerConfig::default(),
            clock,
            { WorkerProbe => sender }
        )
        .on_event(move |_context, event| {
            if matches!(event, Event::Success(_)) {
                event_finished.notify_one();
            }
        });
        let signal = async move {
            task_finished.notified().await;
            Ok::<(), std::io::Error>(())
        };
        let enqueue_after_empty_scan = async {
            empty_scan.notified().await;
            runtime
                .enqueue_job_payload(
                    job_id,
                    WorkerProbe::KIND.to_string(),
                    serde_json::json!({"value": 9}),
                    1_000,
                )
                .await
                .unwrap();
        };

        let (completed, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(monitor.run_with_signal(signal), enqueue_after_empty_scan)
        })
        .await
        .expect("an empty scan must not terminate the worker stream");

        completed.unwrap();
        assert_eq!(receiver.try_recv().unwrap(), (job_id, 9));
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed", "JobSucceeded"]
        );
        assert_eq!(queue_status(&pool, &job_id).await.as_deref(), Some("done"));
    }

    #[tokio::test]
    async fn closing_the_native_backend_prevents_further_claims() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1_000).await;
        let mut worker = EventStoreBackend::<TestJob, _>::new(
            runtime,
            "native-worker",
            JobWorkerConfig::default(),
            Clock::from_fn(|| from_millis(1_000)),
        );
        let context = WorkerContext::new("native-worker");

        futures_util::future::poll_fn(|cx| worker.poll_close(cx, &context))
            .await
            .unwrap();
        let next =
            futures_util::future::poll_fn(|cx| ApalisBackend::poll_next(&mut worker, cx, &context))
                .await;

        assert_eq!(next.map(|task| task.unwrap().args.n), None);
        assert_eq!(event_types(&pool, &job_id).await, ["JobEnqueued"]);
    }

    #[tokio::test]
    async fn send_only_worker_extractors_preserve_missing_data_errors() {
        let task = Task::new(WorkerProbe {
            value: Cell::new(1),
        });
        let Err(context_error) = JobContext::from_request(&task).await else {
            panic!("missing claim context must be rejected");
        };
        assert_eq!(
            context_error,
            MissingDataError::NotFound(std::any::type_name::<JobContext>().to_string())
        );

        let Err(input_error) = event_sorcery::__worker::JobInput::<()>::from_request(&task).await
        else {
            panic!("missing worker input must be rejected");
        };
        assert_eq!(
            input_error,
            MissingDataError::NotFound(std::any::type_name::<Arc<()>>().to_string())
        );
    }

    #[tokio::test]
    async fn build_supervised_worker_wires_a_monitor() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool }).await.unwrap();
        // Build (but do not run) a supervised monitor: this type-checks the whole
        // worker wiring -- EventStoreBackend + its claim/ack middleware +
        // WorkerBuilder + run_job + Monitor::register's bounds all line up.
        let _monitor = build_supervised_worker!(
            runtime,
            JobWorkerConfig::default(),
            Clock::system(),
            { TestJob => () }
        );
    }

    #[tokio::test]
    async fn claim_appends_claimed_and_marks_the_row() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1000).await;

        let outcome = runtime
            .claim_job(job_id, "w1", 1000, 30_000, 50)
            .await
            .unwrap();

        assert!(matches!(
            outcome,
            JobClaimResult::Claimed(ref job) if job.handle.is_first_execution()
        ));
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed"]
        );
        assert_eq!(
            queue_status(&pool, &job_id).await.as_deref(),
            Some("claimed")
        );
    }

    #[tokio::test]
    async fn runtime_facade_reports_a_refused_settlement_as_terminal() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1_000).await;

        let JobClaimResult::Claimed(claimed) = runtime
            .claim_job(job_id, "ffi-worker", 1_000, 30_000, 50)
            .await
            .unwrap()
        else {
            panic!("expected the facade to win the claim");
        };

        // A stray `Enqueued` on an already-claimed stream is an event the job
        // entity cannot evolve, so replay lands the lifecycle in `Failed` and
        // the aggregate refuses every later command.
        let mut stray = JobSeed::new(job_id, TestJob::KIND, serde_json::json!({"n": 1}), 1_000)
            .event()
            .unwrap();
        stray.sequence = 3;
        let mut connection = pool.acquire().await.unwrap();
        insert_serialized_events_batch(&mut connection, "events", std::slice::from_ref(&stray))
            .await
            .unwrap();
        drop(connection);

        let error = runtime.acknowledge_job(claimed.handle).await.unwrap_err();

        assert!(matches!(
            error,
            JobRuntimeError::SettlementRefused { job_id: refused, .. } if refused == job_id
        ));
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed", "JobEnqueued"]
        );
    }

    #[tokio::test]
    async fn standalone_enqueue_seeds_a_claimable_pending_job() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();

        let job_id = runtime.enqueue(TestJob { n: 7 }).await.unwrap();

        assert_eq!(event_types(&pool, &job_id).await, ["JobEnqueued"]);
        assert_eq!(
            queue_status(&pool, &job_id).await.as_deref(),
            Some("pending")
        );

        // A standalone-enqueued job is claimable like any handler-pushed one.

        let now_ms = Utc::now().timestamp_millis() + 1_000;
        let outcome = runtime
            .claim_job(job_id, "w1", now_ms, 30_000, 50)
            .await
            .unwrap();
        assert!(matches!(outcome, JobClaimResult::Claimed(_)));
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed"]
        );
    }

    #[tokio::test]
    async fn job_queue_rebuilds_from_the_job_event_stream() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = runtime.enqueue(TestJob { n: 7 }).await.unwrap();

        sqlx::query(
            r"
            DELETE FROM job_queue_projection
            WHERE view_id = ?1
            ",
        )
        .bind(job_id.to_string())
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(queue_status(&pool, &job_id).await, None);

        let _rebuilt = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();

        assert_eq!(event_types(&pool, &job_id).await, ["JobEnqueued"]);
        assert_eq!(
            queue_status(&pool, &job_id).await.as_deref(),
            Some("pending")
        );
    }

    #[tokio::test]
    async fn standalone_enqueue_with_delay_is_not_runnable_until_run_at() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();

        let job_id = runtime
            .enqueue_with_delay(TestJob { n: 1 }, std::time::Duration::from_hours(1))
            .await
            .unwrap();

        assert_eq!(
            queue_status(&pool, &job_id).await.as_deref(),
            Some("pending")
        );

        // run_at is an hour out, so a poll at "now" finds nothing runnable.
        let now_ms = Utc::now().timestamp_millis();
        let found = runtime.poll_jobs(TestJob::KIND, now_ms, 16).await.unwrap();
        assert!(
            found.is_empty(),
            "a delayed standalone enqueue must not be runnable before its run_at"
        );
    }

    #[tokio::test]
    async fn enqueue_rejects_a_reused_job_id() {
        let pool = create_test_pool().await.unwrap();

        let backend = SqliteBackend { pool: pool.clone() };
        let id = JobId::new();
        let seed = JobSeed::new(id, TestJob::KIND, serde_json::json!({"n": 1}), 1_000);
        let payload = seed.queue_payload().unwrap();

        backend
            .enqueue(seed.event().unwrap(), payload.clone())
            .await
            .unwrap();
        let error = backend
            .enqueue(seed.event().unwrap(), payload)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            sqlite_es::SqliteJobError::DuplicateEnqueue { job_id } if job_id == id.to_string()
        ));
    }

    #[tokio::test]
    async fn second_sequential_claim_is_not_runnable() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1000).await;

        let first = runtime
            .claim_job(job_id, "w1", 1000, 30_000, 50)
            .await
            .unwrap();
        let second = runtime
            .claim_job(job_id, "w2", 1000, 30_000, 50)
            .await
            .unwrap();

        assert!(matches!(first, JobClaimResult::Claimed(_)));
        assert!(matches!(second, JobClaimResult::Skipped));
        let claimed = event_types(&pool, &job_id)
            .await
            .iter()
            .filter(|event| *event == "JobClaimed")
            .count();
        assert_eq!(claimed, 1);
    }

    #[tokio::test]
    async fn claimed_job_is_not_runnable_before_its_lease_expires() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1000).await;
        runtime
            .claim_job(job_id, "w1", 1000, 30_000, 50)
            .await
            .unwrap();

        let found = runtime
            .poll_jobs(TestJob::KIND, 1000 + 5_000, 16)
            .await
            .unwrap();
        assert_eq!(found, Vec::<String>::new());
    }

    #[tokio::test]
    async fn expired_lease_is_reclaimable_without_counting_an_attempt() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1000).await;
        runtime
            .claim_job(job_id, "w1", 1000, 30_000, 50)
            .await
            .unwrap();

        let later = 1000 + 31_000;
        let outcome = runtime
            .claim_job(job_id, "w2", later, 30_000, 50)
            .await
            .unwrap();

        assert!(matches!(
            outcome,
            JobClaimResult::Claimed(ref job) if job.handle.attempt() == 0 && !job.handle.is_first_execution()
        ));
    }

    #[derive(Debug, thiserror::Error)]
    #[error("external system said no")]
    struct TestPerformError;

    async fn poll_task(runtime: &JobRuntime<SqliteBackend>, now_ms: i64) -> Task<TestJob> {
        let mut worker = EventStoreBackend::<TestJob, _>::new(
            runtime.clone(),
            "poll-test",
            JobWorkerConfig::default(),
            Clock::from_fn(move || from_millis(now_ms)),
        );
        let context = WorkerContext::new("poll-test");
        timeout(
            Duration::from_secs(2),
            poll_fn(|cx| worker.poll_next(cx, &context)),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    }

    async fn process_task(
        runtime: &JobRuntime<SqliteBackend>,
        task: Task<TestJob>,
        config: JobWorkerConfig,
        clock: Clock,
        handler: impl Fn(TestJob) -> Result<JobOutcome<()>, JobFailure<TestPerformError>>
        + Clone
        + Send
        + Sync
        + 'static,
    ) -> Result<JobOutcome<()>, BoxDynError> {
        let mut worker =
            EventStoreBackend::<TestJob, _>::new(runtime.clone(), "ack-test", config, clock);
        let mut context = WorkerContext::new("ack-test");
        let layer = worker.middleware(&mut context);
        let mut service = layer.layer(apalis_core::task::task_fn::task_fn(move |job: TestJob| {
            ready(handler(job))
        }));
        poll_fn(|cx| service.poll_ready(cx)).await?;
        service.call(task).await
    }

    #[tokio::test]
    async fn retry_timestamp_overflow_preserves_the_claim() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1_000).await;
        let task = poll_task(&runtime, 1_000).await;

        let error = process_task(
            &runtime,
            task,
            JobWorkerConfig::default(),
            Clock::from_fn(|| DateTime::<Utc>::MAX_UTC),
            |_| Err(JobFailure::Transient(TestPerformError)),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error.downcast_ref::<JobScheduleError>(),
            Some(JobScheduleError::TimestampOverflow)
        ));
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed"]
        );
        assert_eq!(
            queue_status(&pool, &job_id).await.as_deref(),
            Some("claimed")
        );
    }

    #[tokio::test]
    async fn unrepresentable_deferral_does_not_schedule_a_fallback() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1_000).await;
        let task = poll_task(&runtime, 1_000).await;

        let error = process_task(
            &runtime,
            task,
            JobWorkerConfig::default(),
            Clock::from_fn(|| from_millis(1_000)),
            |_| Ok(JobOutcome::Defer(Duration::MAX)),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error.downcast_ref::<JobScheduleError>(),
            Some(JobScheduleError::DelayOutOfRange(_))
        ));
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed"]
        );
        assert_eq!(
            queue_status(&pool, &job_id).await.as_deref(),
            Some("claimed")
        );
    }

    #[tokio::test]
    async fn schedule_failure_returns_typed_error_and_stops_renewal() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1_000).await;
        let task = poll_task(&runtime, 1_000).await;
        let performed = Arc::new(AtomicU32::new(0));
        let observed = Arc::clone(&performed);
        let clock_reads = Arc::new(AtomicU32::new(0));
        let observed_clock = Arc::clone(&clock_reads);
        let config = JobWorkerConfig {
            renew_interval: Duration::from_millis(10),
            ..JobWorkerConfig::default()
        };
        let clock = Clock::from_fn(move || {
            observed_clock.fetch_add(1, Ordering::SeqCst);
            DateTime::<Utc>::MAX_UTC
        });

        tokio::time::pause();
        let error = process_task(&runtime, task, config, clock, move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Err(JobFailure::Transient(TestPerformError))
        })
        .await
        .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<JobScheduleError>(),
            Some(JobScheduleError::TimestampOverflow)
        ));
        assert_eq!(performed.load(Ordering::SeqCst), 1);
        assert_eq!(clock_reads.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(clock_reads.load(Ordering::SeqCst), 1);
        tokio::time::resume();
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed"]
        );
    }

    #[tokio::test]
    async fn perform_receives_the_claimed_jobs_identity() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1_000).await;
        let first = poll_task(&runtime, 1_000).await;

        let context = JobContext::from_request(&first).await.unwrap();
        assert_eq!(context.job_id(), job_id);
        assert_eq!(context.attempt(), 0);
        let error = process_task(
            &runtime,
            first,
            JobWorkerConfig::default(),
            Clock::from_fn(|| from_millis(1_000)),
            |_| Err(JobFailure::Transient(TestPerformError)),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error.downcast_ref(),
            Some(JobFailure::Transient(TestPerformError))
        ));
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed", "JobRetryScheduled"]
        );

        // The failure occurred at 1,000 ms and the first retry waits 1,000 ms.
        assert_eq!(
            runtime.poll_jobs(TestJob::KIND, 1_999, 16).await.unwrap(),
            [] as [String; 0]
        );
        assert_eq!(
            runtime.poll_jobs(TestJob::KIND, 2_000, 16).await.unwrap(),
            [job_id.to_string()]
        );
        let retried = poll_task(&runtime, 2_000).await;
        let context = JobContext::from_request(&retried).await.unwrap();
        assert_eq!(context.job_id(), job_id);
        assert_eq!(context.attempt(), 1);
    }

    #[tokio::test]
    async fn terminal_failure_dead_letters_without_retries() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1_000).await;
        let task = poll_task(&runtime, 1_000).await;

        let error = process_task(
            &runtime,
            task,
            JobWorkerConfig::default(),
            Clock::from_fn(|| from_millis(1_000)),
            |_| Err(JobFailure::Terminal(TestPerformError)),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error.downcast_ref(),
            Some(JobFailure::Terminal(TestPerformError))
        ));
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed", "JobDead"]
        );
        assert_eq!(queue_status(&pool, &job_id).await.as_deref(), Some("dead"));
        let payload: String =
            sqlx::query_scalar("SELECT payload FROM job_queue_projection WHERE view_id = ?1")
                .bind(job_id.to_string())
                .fetch_one(&pool)
                .await
                .unwrap();
        let state: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(state["Live"]["Dead"]["reason"], "Rejected");
    }

    #[tokio::test]
    async fn deferred_job_reschedules_to_pending_and_resets_the_claim_budget() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1_000).await;
        let task = poll_task(&runtime, 1_000).await;

        let outcome = process_task(
            &runtime,
            task,
            JobWorkerConfig::default(),
            Clock::from_fn(|| from_millis(1_000)),
            |_| Ok(JobOutcome::Defer(Duration::from_hours(1))),
        )
        .await
        .unwrap();
        assert!(matches!(outcome, JobOutcome::Defer(delay) if delay == Duration::from_hours(1)));
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed", "JobRescheduled"]
        );
        assert_eq!(
            queue_status(&pool, &job_id).await.as_deref(),
            Some("pending")
        );
        let payload: String =
            sqlx::query_scalar("SELECT payload FROM job_queue_projection WHERE view_id = ?1")
                .bind(job_id.to_string())
                .fetch_one(&pool)
                .await
                .unwrap();
        let state: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(state["Live"]["Pending"]["claims"], 0);
        assert_eq!(
            runtime.poll_jobs(TestJob::KIND, 1_000, 16).await.unwrap(),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn ack_retry_then_kill_via_commands() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1_000).await;
        let JobClaimResult::Claimed(first) = runtime
            .claim_job(job_id, "w1", 1_000, 30_000, 50)
            .await
            .unwrap()
        else {
            panic!("expected claim");
        };
        assert_eq!(
            runtime
                .retry_job(first.handle, 2_000, "boom".to_owned())
                .await
                .unwrap(),
            JobSettlementResult::Applied
        );
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed", "JobRetryScheduled"]
        );
        assert_eq!(
            queue_status(&pool, &job_id).await.as_deref(),
            Some("pending")
        );

        let JobClaimResult::Claimed(second) = runtime
            .claim_job(job_id, "w1", 2_000, 30_000, 50)
            .await
            .unwrap()
        else {
            panic!("expected re-claim");
        };
        assert_eq!(
            runtime
                .dead_letter_job(
                    second.handle,
                    DeadReason::RetriesExhausted,
                    "boom".to_owned()
                )
                .await
                .unwrap(),
            JobSettlementResult::Applied
        );
        assert_eq!(
            event_types(&pool, &job_id).await,
            [
                "JobEnqueued",
                "JobClaimed",
                "JobRetryScheduled",
                "JobClaimed",
                "JobDead"
            ]
        );
        assert_eq!(queue_status(&pool, &job_id).await.as_deref(), Some("dead"));
    }

    #[tokio::test]
    async fn ack_is_fenced_when_a_reclaimer_holds_the_job() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1_000).await;
        let stale_task = poll_task(&runtime, 1_000).await;
        let JobClaimResult::Claimed(_) = runtime
            .claim_job(job_id, "w2", 1_000 + 31_000, 30_000, 50)
            .await
            .unwrap()
        else {
            panic!("expected re-claim");
        };

        let outcome = process_task(
            &runtime,
            stale_task,
            JobWorkerConfig::default(),
            Clock::from_fn(|| from_millis(32_000)),
            |_| Ok(JobOutcome::Done(())),
        )
        .await
        .unwrap();
        assert!(matches!(outcome, JobOutcome::Done(())));
        assert_eq!(
            event_types(&pool, &job_id).await,
            ["JobEnqueued", "JobClaimed", "JobClaimed"]
        );
        assert_eq!(
            queue_status(&pool, &job_id).await.as_deref(),
            Some("claimed")
        );
    }

    #[tokio::test]
    async fn a_task_without_claim_context_cannot_invoke_the_handler() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool }).await.unwrap();
        let invoked = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&invoked);
        let error = process_task(
            &runtime,
            Task::new(TestJob { n: 1 }),
            JobWorkerConfig::default(),
            Clock::system(),
            move |_| {
                observed.store(true, Ordering::SeqCst);
                Ok(JobOutcome::Done(()))
            },
        )
        .await
        .unwrap_err();

        assert!(
            matches!(error.downcast_ref::<MissingDataError>(), Some(MissingDataError::NotFound(name)) if name == std::any::type_name::<JobContext>())
        );
        assert!(!invoked.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn claim_budget_dead_letters_a_crash_loop() {
        let pool = create_test_pool().await.unwrap();
        let runtime = JobRuntime::build(SqliteBackend { pool: pool.clone() })
            .await
            .unwrap();
        let job_id = enqueue(&runtime, 1000).await;

        // max_claims = 2: claim twice (advancing past each lease), the third
        // claim exhausts the budget and dead-letters as abandoned.
        let mut now = 1000;
        for _ in 0..2 {
            let outcome = runtime
                .claim_job(job_id, "w1", now, 30_000, 2)
                .await
                .unwrap();
            assert!(matches!(outcome, JobClaimResult::Claimed(_)));
            now += 31_000;
        }

        let outcome = runtime
            .claim_job(job_id, "w1", now, 30_000, 2)
            .await
            .unwrap();
        assert!(matches!(outcome, JobClaimResult::Abandoned));
        assert_eq!(queue_status(&pool, &job_id).await.as_deref(), Some("dead"));
        assert!(
            event_types(&pool, &job_id)
                .await
                .contains(&"JobDead".to_string())
        );
    }
}
