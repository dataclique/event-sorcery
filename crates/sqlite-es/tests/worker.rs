//! Durable worker behavior over the native SQLite backend.

#[cfg(test)]
mod tests {
    use apalis_core::{
        backend::Backend as ApalisBackend,
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
            atomic::{AtomicBool, Ordering},
        },
        task::Poll,
        time::Duration,
    };
    use tokio::{sync::Notify, time::timeout};

    use event_sorcery::*;
    use sqlite_es::{SqliteBackend, testing::create_test_pool};

    #[derive(Serialize, Deserialize)]
    struct TestJob {
        n: u32,
    }

    impl StandaloneJob for TestJob {
        type Input = ();
        type Output = ();
        type Error = std::convert::Infallible;

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
        ) -> impl Future<Output = Result<JobOutcome<()>, JobFailure<std::convert::Infallible>>> + Send
        {
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
}
