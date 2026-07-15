module EventSorcery.Job.Worker.Definition (
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
import EventSorcery.Engine (EngineError, Store)
import EventSorcery.Job (
  ClaimBudget,
  JobDecodeError,
  JobId,
  JobInstant,
  LeaseDuration,
  WorkerId,
 )
import EventSorcery.Job.Execution (
  DurableJob (JobInput),
  JobAttempt,
 )
import Prelude (Eq, IO, Show, otherwise, (==))


newtype AttemptLimit = AttemptLimit Word32
  deriving stock (Eq, Show)


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


data RenewalSchedule = RenewalSchedule
  { waitBeforeRenewal :: IO ()
  , renewalDeadline :: IO JobInstant
  }


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


data JobRunError
  = JobRunEngineFailed EngineError
  | JobRunDecodeFailed JobId JobDecodeError
  deriving stock (Eq, Show)


mkAttemptLimit :: Word32 -> Maybe AttemptLimit
mkAttemptLimit value
  | value == 0 = Nothing
  | otherwise = Just (AttemptLimit value)


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


renewalSchedule :: IO () -> IO JobInstant -> RenewalSchedule
renewalSchedule = RenewalSchedule


renewingJobWorker :: JobWorker job -> RenewalSchedule -> JobWorker job
renewingJobWorker worker schedule = worker {leaseRenewal = Just schedule}
