-- | Validated worker configuration and durable execution results.
module Event.Sorcery.Job.Worker.Definition (
  AttemptLimit (AttemptLimit),
  JobRunError (..),
  JobRunResult (..),
  JobWorker (..),
  RenewalSchedule (..),
  jobWorker,
  mkAttemptLimit,
  renewalSchedule,
  renewingJobWorker,
) where

import Data.Maybe (Maybe (..))
import Data.Word (Word32)
import Event.Sorcery.Engine (EngineError, Store)
import Event.Sorcery.Job (
  ClaimBudget,
  JobDecodeError,
  JobId,
  JobInstant,
  LeaseDuration,
  WorkerId,
 )
import Event.Sorcery.Job.Execution (
  DurableJob (JobInput),
  JobAttempt,
 )
import Prelude (Eq, IO, Show, otherwise, (==))


-- | Positive maximum number of durably recorded execution attempts.
newtype AttemptLimit = AttemptLimit Word32
  deriving stock (Eq, Show)


-- | Complete policy and engine context required to run one job kind.
data JobWorker job = JobWorker
  { store :: Store
  , workerId :: WorkerId
  , leaseDuration :: LeaseDuration
  , claimBudget :: ClaimBudget
  , attemptLimit :: AttemptLimit
  , retryAt :: JobAttempt -> JobInstant
  , input :: JobInput job
  , leaseRenewal :: Maybe RenewalSchedule
  }


-- | Injected timing operations used to renew long-running claims.
data RenewalSchedule = RenewalSchedule
  { waitBeforeRenewal :: IO ()
  , renewalDeadline :: IO JobInstant
  }


-- | Exhaustive durable outcome of one claim-and-execute cycle.
data JobRunResult output failure
  = JobSucceeded output
  | JobDeferredUntil JobInstant
  | JobRetryScheduled JobAttempt JobInstant failure
  | JobRejected failure
  | JobRetriesExhausted JobAttempt failure
  | JobRunAbandoned
  | JobRunContended
  | JobRunSkipped
  | JobRunFenced
  deriving stock (Eq, Show)


-- | Failure that prevented a worker cycle from reaching a durable outcome.
data JobRunError
  = JobRunEngineFailed EngineError
  | JobRunDecodeFailed JobId JobDecodeError
  deriving stock (Eq, Show)


-- | Validates that an attempt limit is non-zero.
mkAttemptLimit :: Word32 -> Maybe AttemptLimit
mkAttemptLimit value
  | value == 0 = Nothing
  | otherwise = Just (AttemptLimit value)


-- | Builds a worker without lease renewal.
jobWorker
  :: Store
  -> WorkerId
  -> LeaseDuration
  -> ClaimBudget
  -> AttemptLimit
  -> (JobAttempt -> JobInstant)
  -> JobInput job
  -> JobWorker job
jobWorker store workerId leaseDuration claimBudget attemptLimit retryAt input =
  JobWorker
    { store
    , workerId
    , leaseDuration
    , claimBudget
    , attemptLimit
    , retryAt
    , input
    , leaseRenewal = Nothing
    }


-- | Builds a renewal schedule from injectable waiting and clock operations.
renewalSchedule :: IO () -> IO JobInstant -> RenewalSchedule
renewalSchedule = RenewalSchedule


-- | Enables periodic lease renewal for a worker.
renewingJobWorker :: JobWorker job -> RenewalSchedule -> JobWorker job
renewingJobWorker worker schedule = worker {leaseRenewal = Just schedule}
