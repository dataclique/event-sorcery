module EventSorcery.Job.Worker.Definition (
  AttemptLimit (AttemptLimit),
  JobRunError (..),
  JobRunResult (..),
  JobWorker (..),
  jobWorker,
  mkAttemptLimit,
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
import Prelude (Eq, Show, otherwise, (==))


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
jobWorker = JobWorker
