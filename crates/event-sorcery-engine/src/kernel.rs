use crate::model::{
    AttemptCount, ClaimBudget, DeadReason, Decision, ExpectedVersion, Instant, JobPayload,
    JobRecord, LeaseToken, LeaseWindow, Mutation,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendConflict {
    expected: ExpectedVersion,
    actual: ExpectedVersion,
}

impl AppendConflict {
    pub const fn expected(self) -> ExpectedVersion {
        self.expected
    }

    pub const fn actual(self) -> ExpectedVersion {
        self.actual
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnqueueVerdict {
    Enqueued,
    AlreadyEnqueued,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobError {
    NotFound,
    PayloadMismatch,
    NotRunnable {
        run_at: Instant,
    },
    LeaseUnavailable {
        expires: Instant,
    },
    NotLeased,
    LeaseLost {
        requested: LeaseToken,
        held: LeaseToken,
    },
    AlreadyCompleted,
    AlreadyDeadLettered(DeadReason),
    LeaseTokenExhausted,
    AttemptCountExhausted,
    ClaimCountExhausted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClaimVerdict {
    Claimed {
        token: LeaseToken,
        attempts: AttemptCount,
        payload: JobPayload,
    },
    Abandon,
}

pub fn append_check(
    expected: ExpectedVersion,
    actual: ExpectedVersion,
) -> Result<(), AppendConflict> {
    if expected == actual {
        Ok(())
    } else {
        Err(AppendConflict { expected, actual })
    }
}

pub fn enqueue(
    payload: JobPayload,
    run_at: Option<crate::model::Instant>,
    current: Option<JobRecord>,
) -> Result<Decision<EnqueueVerdict, JobRecord>, JobError> {
    match current {
        None => Ok(Decision::new(
            EnqueueVerdict::Enqueued,
            crate::model::Mutation::Changed(JobRecord::enqueued(payload, run_at)),
        )),
        Some(record) if record.payload() == &payload => Ok(Decision::new(
            EnqueueVerdict::AlreadyEnqueued,
            crate::model::Mutation::Unchanged(record),
        )),
        Some(_) => Err(JobError::PayloadMismatch),
    }
}

pub fn claim(
    window: LeaseWindow,
    budget: ClaimBudget,
    current: Option<JobRecord>,
) -> Result<Decision<ClaimVerdict, JobRecord>, JobError> {
    let Some(record) = current else {
        return Err(JobError::NotFound);
    };

    match record.status() {
        crate::model::JobStatus::Completed => return Err(JobError::AlreadyCompleted),
        crate::model::JobStatus::DeadLettered(reason) => {
            return Err(JobError::AlreadyDeadLettered(reason));
        }
        crate::model::JobStatus::Scheduled(run_at) if window.start() < run_at => {
            return Err(JobError::NotRunnable { run_at });
        }
        crate::model::JobStatus::Leased(expires) if window.start() < expires => {
            return Err(JobError::LeaseUnavailable { expires });
        }
        crate::model::JobStatus::Ready
        | crate::model::JobStatus::Scheduled(_)
        | crate::model::JobStatus::Leased(_) => {}
    }

    let claims = record.claims().get();
    let Some(next_claims) = claims.checked_add(1) else {
        return Err(JobError::ClaimCountExhausted);
    };
    if next_claims > budget.get() {
        let abandoned = rebuild(
            &record,
            crate::model::JobStatus::DeadLettered(DeadReason::Abandoned),
            record.token(),
            record.attempts(),
            crate::model::ClaimCount::new(next_claims),
        );
        return Ok(Decision::new(
            ClaimVerdict::Abandon,
            Mutation::Changed(abandoned),
        ));
    }

    let Some(next_token) = record.token().get().checked_add(1) else {
        return Err(JobError::LeaseTokenExhausted);
    };
    let token = LeaseToken::new(next_token);
    let claimed = rebuild(
        &record,
        crate::model::JobStatus::Leased(window.end()),
        token,
        record.attempts(),
        crate::model::ClaimCount::new(next_claims),
    );
    Ok(Decision::new(
        ClaimVerdict::Claimed {
            token,
            attempts: record.attempts(),
            payload: record.payload().clone(),
        },
        Mutation::Changed(claimed),
    ))
}

pub fn acknowledge(
    token: LeaseToken,
    current: Option<JobRecord>,
) -> Result<Mutation<JobRecord>, JobError> {
    let Some(record) = current else {
        return Err(JobError::NotFound);
    };
    check_token(token, &record)?;

    match record.status() {
        crate::model::JobStatus::Ready | crate::model::JobStatus::Scheduled(_) => {
            Err(JobError::NotLeased)
        }
        crate::model::JobStatus::Leased(_) => {
            let completed = rebuild(
                &record,
                crate::model::JobStatus::Completed,
                record.token(),
                record.attempts(),
                record.claims(),
            );
            Ok(Mutation::Changed(completed))
        }
        crate::model::JobStatus::Completed => Ok(Mutation::Unchanged(record)),
        crate::model::JobStatus::DeadLettered(reason) => Err(JobError::AlreadyDeadLettered(reason)),
    }
}

pub fn retry(
    token: LeaseToken,
    run_at: Instant,
    current: Option<JobRecord>,
) -> Result<Decision<AttemptCount, JobRecord>, JobError> {
    let record = leased_record(token, current)?;
    let attempts = increment_attempts(record.attempts())?;
    let scheduled = rebuild(
        &record,
        crate::model::JobStatus::Scheduled(run_at),
        record.token(),
        attempts,
        record.claims(),
    );
    Ok(Decision::new(attempts, Mutation::Changed(scheduled)))
}

pub fn defer(
    token: LeaseToken,
    run_at: Instant,
    current: Option<JobRecord>,
) -> Result<Mutation<JobRecord>, JobError> {
    let record = leased_record(token, current)?;
    let scheduled = rebuild(
        &record,
        crate::model::JobStatus::Scheduled(run_at),
        record.token(),
        record.attempts(),
        crate::model::ClaimCount::ZERO,
    );
    Ok(Mutation::Changed(scheduled))
}

pub fn dead_letter(
    token: LeaseToken,
    reason: DeadReason,
    current: Option<JobRecord>,
) -> Result<Mutation<JobRecord>, JobError> {
    if let Some(record) = current.as_ref()
        && record.token() == token
        && record.status() == crate::model::JobStatus::DeadLettered(reason)
    {
        return Ok(Mutation::Unchanged(record.clone()));
    }

    let record = leased_record(token, current)?;
    let dead = rebuild(
        &record,
        crate::model::JobStatus::DeadLettered(reason),
        record.token(),
        record.attempts(),
        record.claims(),
    );
    Ok(Mutation::Changed(dead))
}

pub fn exhaust(
    token: LeaseToken,
    current: Option<JobRecord>,
) -> Result<Decision<AttemptCount, JobRecord>, JobError> {
    let record = leased_record(token, current)?;
    let attempts = increment_attempts(record.attempts())?;
    let dead = rebuild(
        &record,
        crate::model::JobStatus::DeadLettered(DeadReason::RetriesExhausted),
        record.token(),
        attempts,
        record.claims(),
    );
    Ok(Decision::new(attempts, Mutation::Changed(dead)))
}

fn leased_record(token: LeaseToken, current: Option<JobRecord>) -> Result<JobRecord, JobError> {
    let Some(record) = current else {
        return Err(JobError::NotFound);
    };
    check_token(token, &record)?;
    match record.status() {
        crate::model::JobStatus::Ready | crate::model::JobStatus::Scheduled(_) => {
            Err(JobError::NotLeased)
        }
        crate::model::JobStatus::Completed => Err(JobError::AlreadyCompleted),
        crate::model::JobStatus::DeadLettered(reason) => Err(JobError::AlreadyDeadLettered(reason)),
        crate::model::JobStatus::Leased(_) => Ok(record),
    }
}

fn check_token(requested: LeaseToken, record: &JobRecord) -> Result<(), JobError> {
    let held = record.token();
    if requested == held {
        Ok(())
    } else {
        Err(JobError::LeaseLost { requested, held })
    }
}

fn increment_attempts(attempts: AttemptCount) -> Result<AttemptCount, JobError> {
    attempts
        .get()
        .checked_add(1)
        .map(AttemptCount::new)
        .ok_or(JobError::AttemptCountExhausted)
}

fn rebuild(
    record: &JobRecord,
    status: crate::model::JobStatus,
    token: LeaseToken,
    attempts: AttemptCount,
    claims: crate::model::ClaimCount,
) -> JobRecord {
    JobRecord::new(record.payload().clone(), status, token, attempts, claims)
}

#[cfg(test)]
mod tests {
    use super::{
        ClaimVerdict, EnqueueVerdict, JobError, acknowledge, append_check, claim, dead_letter,
        defer, enqueue, exhaust, retry,
    };
    use crate::model::{
        AttemptCount, ClaimBudget, ClaimCount, DeadReason, Decision, ExpectedVersion, Instant,
        InvalidLeaseWindow, JobPayload, JobRecord, JobStatus, LeaseToken, LeaseWindow, Mutation,
        StreamVersion,
    };

    #[test]
    fn append_check_accepts_only_the_current_version() {
        let version = ExpectedVersion::At(StreamVersion::new(7));

        assert_eq!(append_check(version, version), Ok(()));
        assert_eq!(
            append_check(ExpectedVersion::NoStream, version),
            Err(super::AppendConflict {
                expected: ExpectedVersion::NoStream,
                actual: version,
            })
        );
    }

    #[test]
    fn enqueue_creates_the_requested_initial_state() {
        let ready_payload = JobPayload::new([1, 2, 3]);
        let scheduled_payload = JobPayload::new([4, 5, 6]);
        let run_at = Instant::from_millis(1_000);

        let ready = enqueue(ready_payload.clone(), None, None);
        let scheduled = enqueue(scheduled_payload.clone(), Some(run_at), None);

        assert_eq!(
            ready,
            Ok(crate::model::Decision::new(
                EnqueueVerdict::Enqueued,
                Mutation::Changed(JobRecord::enqueued(ready_payload, None)),
            ))
        );
        assert_eq!(
            scheduled,
            Ok(crate::model::Decision::new(
                EnqueueVerdict::Enqueued,
                Mutation::Changed(JobRecord::enqueued(scheduled_payload, Some(run_at),)),
            ))
        );
    }

    #[test]
    fn enqueue_absorbs_an_identical_payload_and_rejects_a_different_one() {
        let payload = JobPayload::new([1, 2, 3]);
        let record = JobRecord::enqueued(payload.clone(), None);

        let duplicate = enqueue(
            payload,
            Some(Instant::from_millis(9_000)),
            Some(record.clone()),
        );
        let mismatch = enqueue(JobPayload::new([9]), None, Some(record.clone()));

        assert_eq!(
            duplicate,
            Ok(crate::model::Decision::new(
                EnqueueVerdict::AlreadyEnqueued,
                Mutation::Unchanged(record),
            ))
        );
        assert_eq!(mismatch, Err(JobError::PayloadMismatch));
    }

    #[test]
    fn lease_window_requires_a_strictly_increasing_interval() {
        let instant = Instant::from_millis(100);

        assert_eq!(LeaseWindow::new(instant, instant), Err(InvalidLeaseWindow));
        assert_eq!(
            LeaseWindow::new(instant, Instant::from_millis(99)),
            Err(InvalidLeaseWindow)
        );
        assert!(LeaseWindow::new(instant, Instant::from_millis(101)).is_ok());
    }

    #[test]
    fn claim_enforces_runnability_and_terminal_state_before_counters() {
        let window = valid_window(100, 200);
        let budget = ClaimBudget::new(10);

        assert_eq!(claim(window, budget, None), Err(JobError::NotFound));
        assert_eq!(
            claim(
                window,
                budget,
                Some(record(JobStatus::Completed, u64::MAX, 0, u64::MAX,)),
            ),
            Err(JobError::AlreadyCompleted)
        );
        assert_eq!(
            claim(
                window,
                budget,
                Some(record(
                    JobStatus::Scheduled(Instant::from_millis(101)),
                    0,
                    0,
                    0,
                )),
            ),
            Err(JobError::NotRunnable {
                run_at: Instant::from_millis(101),
            })
        );
        assert_eq!(
            claim(
                window,
                budget,
                Some(record(
                    JobStatus::Leased(Instant::from_millis(101)),
                    0,
                    0,
                    0,
                )),
            ),
            Err(JobError::LeaseUnavailable {
                expires: Instant::from_millis(101),
            })
        );
    }

    #[test]
    fn claim_abandons_over_budget_before_checking_token_exhaustion() {
        let window = valid_window(100, 200);
        let current = record(JobStatus::Ready, u64::MAX, 2, 3);
        let expected = JobRecord::new(
            current.payload().clone(),
            JobStatus::DeadLettered(DeadReason::Abandoned),
            LeaseToken::new(u64::MAX),
            AttemptCount::new(2),
            ClaimCount::new(4),
        );

        assert_eq!(
            claim(window, ClaimBudget::new(3), Some(current)),
            Ok(Decision::new(
                ClaimVerdict::Abandon,
                Mutation::Changed(expected),
            ))
        );
        assert_eq!(
            claim(
                window,
                ClaimBudget::new(4),
                Some(record(JobStatus::Ready, u64::MAX, 2, 3)),
            ),
            Err(JobError::LeaseTokenExhausted)
        );
    }

    #[test]
    fn claim_increments_only_the_token_and_claim_count() {
        let window = valid_window(100, 200);
        let current = record(JobStatus::Ready, 4, 2, 3);
        let payload = current.payload().clone();
        let claimed = JobRecord::new(
            payload.clone(),
            JobStatus::Leased(window.end()),
            LeaseToken::new(5),
            AttemptCount::new(2),
            ClaimCount::new(4),
        );

        assert_eq!(
            claim(window, ClaimBudget::new(4), Some(current)),
            Ok(Decision::new(
                ClaimVerdict::Claimed {
                    token: LeaseToken::new(5),
                    attempts: AttemptCount::new(2),
                    payload,
                },
                Mutation::Changed(claimed),
            ))
        );
    }

    #[test]
    fn acknowledge_is_fenced_and_idempotent_after_completion() {
        let token = LeaseToken::new(3);
        let leased = record(
            JobStatus::Leased(Instant::from_millis(200)),
            token.get(),
            2,
            4,
        );
        let completed = record(JobStatus::Completed, token.get(), 2, 4);

        assert_eq!(
            acknowledge(LeaseToken::new(2), Some(leased.clone())),
            Err(JobError::LeaseLost {
                requested: LeaseToken::new(2),
                held: token,
            })
        );
        assert_eq!(
            acknowledge(token, Some(leased)),
            Ok(Mutation::Changed(completed.clone()))
        );
        assert_eq!(
            acknowledge(token, Some(completed.clone())),
            Ok(Mutation::Unchanged(completed))
        );
    }

    #[test]
    fn retry_counts_a_failure_while_defer_resets_only_claims() {
        let token = LeaseToken::new(3);
        let run_at = Instant::from_millis(500);
        let leased = record(
            JobStatus::Leased(Instant::from_millis(200)),
            token.get(),
            2,
            4,
        );

        assert_eq!(
            retry(token, run_at, Some(leased.clone())),
            Ok(Decision::new(
                AttemptCount::new(3),
                Mutation::Changed(record(JobStatus::Scheduled(run_at), 3, 3, 4)),
            ))
        );
        assert_eq!(
            defer(token, run_at, Some(leased)),
            Ok(Mutation::Changed(record(
                JobStatus::Scheduled(run_at),
                3,
                2,
                0,
            )))
        );
        assert_eq!(
            retry(
                token,
                run_at,
                Some(record(
                    JobStatus::Leased(Instant::from_millis(200)),
                    3,
                    u64::MAX,
                    4,
                )),
            ),
            Err(JobError::AttemptCountExhausted)
        );
    }

    #[test]
    fn dead_letter_is_reason_idempotent_and_exhaust_records_the_failure() {
        let token = LeaseToken::new(3);
        let leased = record(
            JobStatus::Leased(Instant::from_millis(200)),
            token.get(),
            2,
            4,
        );
        let rejected = record(
            JobStatus::DeadLettered(DeadReason::Rejected),
            token.get(),
            2,
            4,
        );

        assert_eq!(
            dead_letter(token, DeadReason::Rejected, Some(leased.clone())),
            Ok(Mutation::Changed(rejected.clone()))
        );
        assert_eq!(
            dead_letter(token, DeadReason::Rejected, Some(rejected.clone())),
            Ok(Mutation::Unchanged(rejected))
        );
        assert_eq!(
            exhaust(token, Some(leased)),
            Ok(Decision::new(
                AttemptCount::new(3),
                Mutation::Changed(record(
                    JobStatus::DeadLettered(DeadReason::RetriesExhausted),
                    3,
                    3,
                    4,
                )),
            ))
        );
    }

    fn valid_window(start: u64, end: u64) -> LeaseWindow {
        let result = LeaseWindow::new(Instant::from_millis(start), Instant::from_millis(end));
        let Ok(window) = result else {
            panic!("test lease window must be valid")
        };
        window
    }

    fn record(status: JobStatus, token: u64, attempts: u64, claims: u64) -> JobRecord {
        JobRecord::new(
            JobPayload::new([1, 2, 3]),
            status,
            LeaseToken::new(token),
            AttemptCount::new(attempts),
            ClaimCount::new(claims),
        )
    }
}
