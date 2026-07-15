#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpectedVersion {
    NoStream,
    At(StreamVersion),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamVersion(u64);

impl StreamVersion {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Instant(u64);

impl Instant {
    pub const fn from_millis(value: u64) -> Self {
        Self(value)
    }

    pub const fn as_millis(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaseWindow {
    start: Instant,
    end: Instant,
}

impl LeaseWindow {
    pub fn new(start: Instant, end: Instant) -> Result<Self, InvalidLeaseWindow> {
        if start < end {
            Ok(Self { start, end })
        } else {
            Err(InvalidLeaseWindow)
        }
    }

    pub const fn start(self) -> Instant {
        self.start
    }

    pub const fn end(self) -> Instant {
        self.end
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidLeaseWindow;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClaimBudget(u64);

impl ClaimBudget {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobPayload(Vec<u8>);

impl JobPayload {
    pub fn new(value: impl Into<Vec<u8>>) -> Self {
        Self(value.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaseToken(u64);

impl LeaseToken {
    pub const INITIAL: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptCount(u64);

impl AttemptCount {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClaimCount(u64);

impl ClaimCount {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeadReason {
    RetriesExhausted,
    Rejected,
    Undecodable,
    Abandoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobStatus {
    Ready,
    Scheduled(Instant),
    Leased(Instant),
    Completed,
    DeadLettered(DeadReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobRecord {
    payload: JobPayload,
    status: JobStatus,
    token: LeaseToken,
    attempts: AttemptCount,
    claims: ClaimCount,
}

impl JobRecord {
    pub const fn new(
        payload: JobPayload,
        status: JobStatus,
        token: LeaseToken,
        attempts: AttemptCount,
        claims: ClaimCount,
    ) -> Self {
        Self {
            payload,
            status,
            token,
            attempts,
            claims,
        }
    }

    pub fn enqueued(payload: JobPayload, run_at: Option<Instant>) -> Self {
        let status = run_at.map_or(JobStatus::Ready, JobStatus::Scheduled);
        Self {
            payload,
            status,
            token: LeaseToken::INITIAL,
            attempts: AttemptCount::ZERO,
            claims: ClaimCount::ZERO,
        }
    }

    pub const fn payload(&self) -> &JobPayload {
        &self.payload
    }

    pub const fn status(&self) -> JobStatus {
        self.status
    }

    pub const fn token(&self) -> LeaseToken {
        self.token
    }

    pub const fn attempts(&self) -> AttemptCount {
        self.attempts
    }

    pub const fn claims(&self) -> ClaimCount {
        self.claims
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation<T> {
    Changed(T),
    Unchanged(T),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Decision<V, T> {
    verdict: V,
    mutation: Mutation<T>,
}

impl<V, T> Decision<V, T> {
    pub const fn new(verdict: V, mutation: Mutation<T>) -> Self {
        Self { verdict, mutation }
    }

    pub const fn verdict(&self) -> &V {
        &self.verdict
    }

    pub const fn mutation(&self) -> &Mutation<T> {
        &self.mutation
    }

    pub fn into_parts(self) -> (V, Mutation<T>) {
        (self.verdict, self.mutation)
    }
}
