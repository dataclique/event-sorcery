module EventSorcery.Job.Worker.Internal (
  SettlementStrategy (..),
  VerdictDelivery (..),
  genericSettlement,
  runJobOnceWith,
) where

import Data.Unrestricted.Linear (Ur (Ur))
import EventSorcery.Engine (EngineError)
import EventSorcery.Job (
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
  acknowledgeJob,
  claimJob,
  deadLetterJob,
  deferJob,
  retryJob,
  settlementToken,
 )
import EventSorcery.Job.Execution (
  DurableJob (renderJobError),
  JobAttempt (JobAttempt),
  JobContext (JobContext),
  JobFailure (..),
  JobOutcome (..),
  executeDurableJob,
 )
import EventSorcery.Job.Worker.Definition (
  AttemptLimit (AttemptLimit),
  JobRunError (..),
  JobRunResult (..),
  JobWorker (..),
 )
import Prelude (
  Bool,
  Either (..),
  IO,
  maxBound,
  otherwise,
  pure,
  (+),
  (==),
  (>=),
 )


data VerdictDelivery
  = VerdictAccepted
  | VerdictDeferred JobInstant


data SettlementStrategy job = SettlementStrategy
  { beforeSuccess
      :: JobId
      -> job
      -> JobAttempt
      -> JobOutput job
      -> IO VerdictDelivery
  , beforeRejection
      :: JobId
      -> job
      -> JobAttempt
      -> JobError job
      -> IO VerdictDelivery
  , beforeExhaustion
      :: JobId
      -> job
      -> JobAttempt
      -> JobError job
      -> IO ()
  }


genericSettlement :: SettlementStrategy job
genericSettlement =
  SettlementStrategy
    { beforeSuccess = \_ _ _ _ -> pure VerdictAccepted
    , beforeRejection = \_ _ _ _ -> pure VerdictAccepted
    , beforeExhaustion = \_ _ _ _ -> pure ()
    }


runJobOnceWith
  :: forall job
   . DurableJob job
  => SettlementStrategy job
  -> JobWorker job
  -> JobId
  -> JobInstant
  -> IO (Either JobRunError (JobRunResult (JobOutput job) (JobError job)))
runJobOnceWith settlement runtime identifier now = do
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
          persistExecution
            settlement
            runtime
            identifier
            job
            token
            attempt
            executed
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
  => SettlementStrategy job
  -> JobWorker job
  -> JobId
  -> job
  -> JobSettlementToken
  -> JobAttempt
  -> Either (JobFailure (JobError job)) (JobOutcome (JobOutput job))
  -> IO (Either JobRunError (JobRunResult (JobOutput job) (JobError job)))
persistExecution settlement runtime identifier job token attempt executed =
  case executed of
    Right (JobDone output) -> do
      delivery <-
        settlement.beforeSuccess identifier job (nextAttempt attempt) output
      persistSuccess runtime token output delivery
    Right (JobDeferred runAt) -> do
      settled <- deferJob runtime.store token runAt
      pure (settlementResult (JobDeferredUntil runAt) settled)
    Left (TerminalFailure failure) -> do
      delivery <-
        settlement.beforeRejection identifier job (nextAttempt attempt) failure
      persistRejection runtime job token failure delivery
    Left (TransientFailure failure) ->
      persistTransientFailure
        settlement
        runtime
        identifier
        job
        token
        attempt
        failure


persistSuccess
  :: JobWorker job
  -> JobSettlementToken
  -> output
  -> VerdictDelivery
  -> IO (Either JobRunError (JobRunResult output failure))
persistSuccess runtime token output delivery = case delivery of
  VerdictAccepted -> do
    settled <- acknowledgeJob runtime.store token
    pure (settlementResult (JobSucceeded output) settled)
  VerdictDeferred runAt -> do
    settled <- deferJob runtime.store token runAt
    pure (settlementResult (JobDeferredUntil runAt) settled)


persistRejection
  :: DurableJob job
  => JobWorker job
  -> job
  -> JobSettlementToken
  -> JobError job
  -> VerdictDelivery
  -> IO (Either JobRunError (JobRunResult output (JobError job)))
persistRejection runtime job token failure delivery = case delivery of
  VerdictAccepted -> do
    settled <-
      deadLetterJob
        runtime.store
        token
        Rejected
        (renderJobError job failure)
    pure (settlementResult (JobRejected failure) settled)
  VerdictDeferred runAt -> do
    settled <- deferJob runtime.store token runAt
    pure (settlementResult (JobDeferredUntil runAt) settled)


persistTransientFailure
  :: DurableJob job
  => SettlementStrategy job
  -> JobWorker job
  -> JobId
  -> job
  -> JobSettlementToken
  -> JobAttempt
  -> JobError job
  -> IO (Either JobRunError (JobRunResult (JobOutput job) (JobError job)))
persistTransientFailure settlement runtime identifier job token attempt failure = do
  let next = nextAttempt attempt
      failureText = renderJobError job failure

  if retryBudgetExhausted runtime.attemptLimit next
    then do
      settlement.beforeExhaustion identifier job next failure
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
