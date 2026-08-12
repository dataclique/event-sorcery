module EventSorcery.Job.Worker (
  AttemptLimit,
  JobRunError (..),
  JobRunResult (..),
  JobWorker,
  jobWorker,
  mkAttemptLimit,
  runJobOnce,
) where

import Data.Maybe (Maybe (..))
import Data.Unrestricted.Linear (Ur (Ur))
import Data.Word (Word32)
import EventSorcery.Engine (EngineError, Store)
import EventSorcery.Job (
  ClaimBudget,
  ClaimedJob,
  DeadReason (..),
  Job (JobError, JobOutput, decodeJob),
  JobClaimDetails (..),
  JobClaimResult (..),
  JobDecodeError (..),
  JobId,
  JobInstant,
  JobSettlement (..),
  JobSettlementToken,
  LeaseDuration,
  WorkerId,
  acknowledgeJob,
  claimJob,
  deadLetterJob,
  deferJob,
  retryJob,
  settlementToken,
 )
import EventSorcery.Job.Execution (
  DurableJob (JobInput, renderJobError),
  JobAttempt (JobAttempt),
  JobContext (JobContext),
  JobFailure (..),
  JobOutcome (..),
  executeDurableJob,
 )
import Prelude (
  Bool,
  Either (..),
  Eq,
  IO,
  Show,
  maxBound,
  otherwise,
  pure,
  (+),
  (==),
  (>=),
 )


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


runJobOnce
  :: forall job
   . DurableJob job
  => JobWorker job
  -> JobId
  -> JobInstant
  -> IO (Either JobRunError (JobRunResult (JobOutput job) (JobError job)))
runJobOnce runtime identifier now = do
  claimed <-
    claimJob
      runtime.store
      identifier
      runtime.workerId
      now
      runtime.leaseDuration
      runtime.claimBudget
      releaseClaim

  case claimed of
    Left failure -> pure (Left (JobRunEngineFailed failure))
    Right (JobClaimed (details, token)) ->
      case decodeJob @job details.payload of
        Left failure -> rejectUndecodable runtime identifier token failure
        Right job -> do
          let attempt = JobAttempt details.attempt
              context = JobContext identifier attempt
          executed <- executeDurableJob context details.route runtime.input job
          persistExecution runtime job token attempt executed
    Right JobAbandoned -> pure (Right JobRunAbandoned)
    Right JobContended -> pure (Right JobRunContended)
    Right JobSkipped -> pure (Right JobRunSkipped)


rejectUndecodable
  :: JobWorker job
  -> JobId
  -> JobSettlementToken
  -> JobDecodeError
  -> IO (Either JobRunError (JobRunResult output failure))
rejectUndecodable runtime identifier token failure@(JobDecodeError cause) = do
  settled <- deadLetterJob runtime.store token Undecodable cause

  pure case settled of
    Left engineError -> Left (JobRunEngineFailed engineError)
    Right SettlementApplied -> Left (JobRunDecodeFailed identifier failure)
    Right SettlementFenced -> Right JobRunFenced


persistExecution
  :: DurableJob job
  => JobWorker job
  -> job
  -> JobSettlementToken
  -> JobAttempt
  -> Either (JobFailure (JobError job)) (JobOutcome (JobOutput job))
  -> IO (Either JobRunError (JobRunResult (JobOutput job) (JobError job)))
persistExecution runtime job token attempt executed = case executed of
  Right (JobDone output) -> do
    settled <- acknowledgeJob runtime.store token
    pure (settlementResult (JobSucceeded output) settled)
  Right (JobDeferred runAt) -> do
    settled <- deferJob runtime.store token runAt
    pure (settlementResult (JobDeferredUntil runAt) settled)
  Left (TerminalFailure failure) -> do
    settled <-
      deadLetterJob
        runtime.store
        token
        Rejected
        (renderJobError job failure)
    pure (settlementResult (JobRejected failure) settled)
  Left (TransientFailure failure) ->
    persistTransientFailure runtime job token attempt failure


persistTransientFailure
  :: DurableJob job
  => JobWorker job
  -> job
  -> JobSettlementToken
  -> JobAttempt
  -> JobError job
  -> IO (Either JobRunError (JobRunResult (JobOutput job) (JobError job)))
persistTransientFailure runtime job token attempt failure = do
  let next = nextAttempt attempt
      failureText = renderJobError job failure

  if retryBudgetExhausted runtime.attemptLimit next
    then do
      settled <-
        deadLetterJob
          runtime.store
          token
          RetriesExhausted
          failureText
      pure (settlementResult (JobRetriesExhausted next failure) settled)
    else do
      let runAt = runtime.retryAt next
      settled <- retryJob runtime.store token runAt failureText
      pure (settlementResult (JobRetryScheduled next runAt failure) settled)


settlementResult
  :: JobRunResult output failure
  -> Either EngineError JobSettlement
  -> Either JobRunError (JobRunResult output failure)
settlementResult result settled = case settled of
  Left engineError -> Left (JobRunEngineFailed engineError)
  Right SettlementApplied -> Right result
  Right SettlementFenced -> Right JobRunFenced


nextAttempt :: JobAttempt -> JobAttempt
nextAttempt attempt@(JobAttempt current)
  | current == maxBound = attempt
  | otherwise = JobAttempt (current + 1)


retryBudgetExhausted :: AttemptLimit -> JobAttempt -> Bool
retryBudgetExhausted (AttemptLimit limit) (JobAttempt attempt) =
  attempt >= limit


releaseClaim
  :: JobClaimDetails
  -> ClaimedJob
  %1 -> Ur (JobClaimDetails, JobSettlementToken)
releaseClaim details won =
  case settlementToken won of
    Ur token -> Ur (details, token)
