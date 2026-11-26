//! Event reactor trait for multi-entity event handling.
//!
//! [`Reactor`] defines a multi-entity event handler whose event
//! type is computed from its dependency list. Reactors handle
//! events via the [`.on()`](crate::OneOf::on) /
//! [`.exhaustive()`](crate::Fold::exhaustive) chain, which
//! guarantees at compile time that every entity is handled.
//!
//! Dependency lists are declared via [`deps!`] in the
//! [`dependency`](crate::dependency) module.

use async_trait::async_trait;
use backon::{BackoffBuilder, ExponentialBuilder};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use tracing::warn;

use crate::dependency::{Dependent, EntityList};

/// Persistence provenance attached to framework-driven reactor delivery.
#[derive(Clone, Copy, Debug)]
pub struct ReactorContext {
    /// Positive aggregate-local event sequence from the persisted envelope.
    /// Absent only for the existing direct, unsequenced `react` entry point.
    pub sequence: Option<NonZeroUsize>,
}

/// Event reactor with exhaustive compile-time checked handling.
///
/// The event type is computed from [`Dependent::Dependencies`]
/// -- no manual enum definition or `From` impls needed. Use the
/// [`.on()`](crate::OneOf::on) /
/// [`.exhaustive()`](crate::Fold::exhaustive) chain in the
/// `react` implementation to handle each entity.
///
/// Each `.on()` handler returns a future, which is boxed
/// internally for type erasure. Call `.exhaustive().await` to
/// run the matched handler.
///
/// ```ignore
/// deps!(RebalancingTrigger, [Position, TokenizedEquityMint]);
///
/// #[async_trait]
/// impl Reactor for RebalancingTrigger {
///     type Error = TriggerError;
///
///     async fn react(
///         &self,
///         event: <Self::Dependencies as EntityList>::Event,
///     ) -> Result<(), Self::Error> {
///         event
///             .on(|symbol, event| async move {
///                 self.on_position(symbol, event).await
///             })
///             .on(|id, event| async move {
///                 self.on_mint(id, event).await
///             })
///             .exhaustive()
///             .await;
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Reactor: Dependent {
    /// Error type for reactor failures.
    type Error: std::error::Error + Send + Sync;

    /// Handle a single event from any supported entity.
    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error>;

    /// Handles a persisted envelope while preserving existing reactor implementations.
    ///
    /// The default delegates to `react`; materialized views use the sequence to
    /// avoid reapplying events already incorporated by startup recovery.
    async fn react_with_context(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
        _context: ReactorContext,
    ) -> Result<(), Self::Error> {
        self.react(event).await
    }
}

/// Enables sharing a reactor via `Arc`.
#[async_trait]
impl<R: Reactor> Reactor for Arc<R> {
    type Error = R::Error;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        R::react(self, event).await
    }

    async fn react_with_context(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
        context: ReactorContext,
    ) -> Result<(), Self::Error> {
        R::react_with_context(self, event, context).await
    }
}

/// Default retry schedule: 10ms base delay, doubling, capped at 1s, 10 retries.
///
/// That is a ~4.3s total retry budget. Shared between `Projection::react`'s own
/// retry loop and the adapter's call to [`retry_with_backoff`], so the two
/// stay in sync without sharing the loop itself.
pub const RETRY_MAX_ATTEMPTS: u32 = 10;
pub const RETRY_BASE_DELAY_MS: u64 = 10;
pub const RETRY_MAX_DELAY_MS: u64 = 1000;

/// Builds the shared capped, non-jittered exponential delivery schedule.
///
/// Callers own their retry budget. A zero initial delay remains zero.
pub(crate) fn retry_delays(base_delay_ms: u64, max_delay_ms: u64) -> backon::ExponentialBackoff {
    let minimum = Duration::from_millis(base_delay_ms.min(max_delay_ms));
    let maximum = Duration::from_millis(max_delay_ms);
    ExponentialBuilder::default()
        .with_min_delay(minimum)
        .with_max_delay(maximum)
        .with_factor(2.0)
        .without_max_times()
        .build()
}

/// Retries `make_attempt` with exponential backoff while `should_retry`
/// returns `true` for the error, up to `max_attempts` retries (so
/// `max_attempts + 1` total calls in the worst case).
///
/// The schedule is an explicit parameter rather than baked-in constants so
/// callers exercising the exhaustion/backoff mechanics in tests can use a
/// tiny synthetic schedule instead of paying the real multi-second
/// production budget. Production callers pass [`RETRY_MAX_ATTEMPTS`],
/// [`RETRY_BASE_DELAY_MS`], and [`RETRY_MAX_DELAY_MS`].
pub async fn retry_with_backoff<Output, Error, MakeAttempt, Attempt>(
    max_attempts: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
    mut make_attempt: MakeAttempt,
    should_retry: impl Fn(&Error) -> bool,
) -> Result<Output, Error>
where
    MakeAttempt: FnMut() -> Attempt,
    Attempt: Future<Output = Result<Output, Error>>,
{
    let mut attempt = 0u32;
    let mut delays = retry_delays(base_delay_ms, max_delay_ms);

    loop {
        match make_attempt().await {
            Ok(output) => return Ok(output),
            Err(error) if attempt < max_attempts && should_retry(&error) => {
                let Some(delay) = delays.next() else {
                    warn!(attempt, "Retry delay schedule exhausted");
                    return Err(error);
                };
                warn!(
                    attempt = attempt + 1,
                    max_attempts,
                    ?delay,
                    "Retrying after transient error"
                );
                sleep(delay).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    enum SyntheticError {
        #[error("retryable")]
        Retryable,
        #[error("permanent")]
        Permanent,
    }

    fn is_synthetic_retryable(error: &SyntheticError) -> bool {
        matches!(error, SyntheticError::Retryable)
    }

    /// Tiny synthetic schedule (sub-millisecond) so exhaustion tests don't
    /// pay the real ~4.3s production budget -- see the DRY decision in the
    /// module doc comment on why the schedule is a parameter, not baked-in
    /// consts, on this helper.
    const TEST_MAX_ATTEMPTS: u32 = 3;
    const TEST_BASE_DELAY_MS: u64 = 1;
    const TEST_MAX_DELAY_MS: u64 = 5;

    #[tokio::test]
    async fn retry_with_backoff_succeeds_on_first_attempt() {
        let call_count = AtomicU32::new(0);

        let result = retry_with_backoff(
            TEST_MAX_ATTEMPTS,
            TEST_BASE_DELAY_MS,
            TEST_MAX_DELAY_MS,
            || {
                call_count.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, SyntheticError>(42) }
            },
            is_synthetic_retryable,
        )
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_with_backoff_retries_then_succeeds() {
        let call_count = AtomicU32::new(0);

        let result = retry_with_backoff(
            TEST_MAX_ATTEMPTS,
            TEST_BASE_DELAY_MS,
            TEST_MAX_DELAY_MS,
            || {
                let attempt = call_count.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt < 2 {
                        Err(SyntheticError::Retryable)
                    } else {
                        Ok(42)
                    }
                }
            },
            is_synthetic_retryable,
        )
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_with_backoff_stops_immediately_on_non_retryable_error() {
        let call_count = AtomicU32::new(0);

        let result: Result<u32, SyntheticError> = retry_with_backoff(
            TEST_MAX_ATTEMPTS,
            TEST_BASE_DELAY_MS,
            TEST_MAX_DELAY_MS,
            || {
                call_count.fetch_add(1, Ordering::SeqCst);
                async { Err(SyntheticError::Permanent) }
            },
            is_synthetic_retryable,
        )
        .await;

        assert_eq!(result.unwrap_err(), SyntheticError::Permanent);
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_with_backoff_exhausts_budget_and_returns_err() {
        let call_count = AtomicU32::new(0);

        let result: Result<u32, SyntheticError> = retry_with_backoff(
            TEST_MAX_ATTEMPTS,
            TEST_BASE_DELAY_MS,
            TEST_MAX_DELAY_MS,
            || {
                call_count.fetch_add(1, Ordering::SeqCst);
                async { Err(SyntheticError::Retryable) }
            },
            is_synthetic_retryable,
        )
        .await;

        assert_eq!(result.unwrap_err(), SyntheticError::Retryable);
        assert_eq!(call_count.load(Ordering::SeqCst), TEST_MAX_ATTEMPTS + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn zero_base_retry_delay_stays_zero_across_large_budgets() {
        let started = tokio::time::Instant::now();
        let call_count = AtomicU32::new(0);
        let result: Result<u32, SyntheticError> = retry_with_backoff(
            70,
            0,
            1,
            || {
                call_count.fetch_add(1, Ordering::SeqCst);
                async { Err(SyntheticError::Retryable) }
            },
            is_synthetic_retryable,
        )
        .await;

        assert_eq!(result.unwrap_err(), SyntheticError::Retryable);
        assert_eq!(call_count.load(Ordering::SeqCst), 71);
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    /// Large retry budgets retain the configured cap without arithmetic overflow.
    #[tokio::test]
    async fn retry_with_backoff_caps_delay_without_overflow_at_high_attempt_counts() {
        const HIGH_MAX_ATTEMPTS: u32 = 70;
        let call_count = AtomicU32::new(0);

        let result: Result<u32, SyntheticError> = retry_with_backoff(
            HIGH_MAX_ATTEMPTS,
            1,
            1,
            || {
                call_count.fetch_add(1, Ordering::SeqCst);
                async { Err(SyntheticError::Retryable) }
            },
            is_synthetic_retryable,
        )
        .await;

        assert_eq!(result.unwrap_err(), SyntheticError::Retryable);
        assert_eq!(call_count.load(Ordering::SeqCst), HIGH_MAX_ATTEMPTS + 1);
    }
}
