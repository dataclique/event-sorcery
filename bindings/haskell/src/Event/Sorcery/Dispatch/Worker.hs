module Event.Sorcery.Dispatch.Worker (
  DeliveryPolicy,
  DispatchWorker,
  OriginDeliveryError (..),
  OriginJob (..),
  OriginPort,
  deliveryPolicy,
  dispatchWorker,
  originPort,
  runDispatchJobOnce,
  storeOriginPort,
) where

import Data.Word (Word32)
import Event.Sorcery.Aggregate (
  Dispatches (injectDispatchOutcome),
  EventSourced,
 )
import Event.Sorcery.Dispatch (DispatchFailure (..), DispatchOutcome)
import Event.Sorcery.Dispatch.Internal qualified as Dispatch
import Event.Sorcery.Job (
  DeadReason (RetriesExhausted),
  Job (JobError, JobOutput),
  JobId,
  JobInstant,
 )
import Event.Sorcery.Job.Execution (
  DurableJob (renderJobError),
  JobAttempt (JobAttempt),
 )
import Event.Sorcery.Job.Worker (
  JobRunError,
  JobRunResult,
  JobWorker,
 )
import Event.Sorcery.Job.Worker.Internal (
  SettlementStrategy (..),
  VerdictDelivery (..),
  runJobOnceWith,
 )
import Event.Sorcery.Store (
  Store,
  StoreError (..),
  executeCommand,
 )
import Event.Sorcery.Stream (StreamKey)
import Prelude (Either (..), IO, pure)


class
  ( DurableJob job
  , EventSourced (Origin job)
  , Dispatches (Origin job) job
  ) =>
  OriginJob job
  where
  type Origin job
  originKey :: job -> StreamKey (Origin job)


data OriginDeliveryError failure
  = OriginUnavailable failure
  | OriginRefused failure


newtype OriginPort job failure
  = OriginPort
      (job -> DispatchOutcome job -> IO (Either (OriginDeliveryError failure) ()))


data DeliveryPolicy failure
  = DeliveryPolicy
      (JobInstant -> failure -> JobInstant)
      (JobInstant -> failure -> JobInstant)
      (JobId -> failure -> IO ())


data DispatchWorker job failure
  = DispatchWorker
      (JobWorker job)
      (OriginPort job failure)
      (DeliveryPolicy failure)


originPort
  :: (job -> DispatchOutcome job -> IO (Either (OriginDeliveryError failure) ()))
  -> OriginPort job failure
originPort = OriginPort


deliveryPolicy
  :: (JobInstant -> failure -> JobInstant)
  -> (JobInstant -> failure -> JobInstant)
  -> (JobId -> failure -> IO ())
  -> DeliveryPolicy failure
deliveryPolicy = DeliveryPolicy


dispatchWorker
  :: JobWorker job
  -> OriginPort job failure
  -> DeliveryPolicy failure
  -> DispatchWorker job failure
dispatchWorker = DispatchWorker


storeOriginPort
  :: OriginJob job
  => Store (Origin job)
  -> OriginPort job (StoreError (Origin job))
storeOriginPort store = OriginPort \job outcome -> do
  delivered <-
    executeCommand
      store
      (originKey job)
      (injectDispatchOutcome outcome)

  pure case delivered of
    Left failure@(StoreCommandRejected _) -> Left (OriginRefused failure)
    Left failure@(StoreDecisionRejected _) -> Left (OriginRefused failure)
    Left failure -> Left (OriginUnavailable failure)
    Right _ -> Right ()


runDispatchJobOnce
  :: forall job failure
   . OriginJob job
  => DispatchWorker job failure
  -> JobId
  -> JobInstant
  -> IO (Either JobRunError (JobRunResult (JobOutput job) (JobError job)))
runDispatchJobOnce (DispatchWorker worker port policy) identifier now =
  runJobOnceWith (dispatchSettlement port policy now) worker identifier now


dispatchSettlement
  :: OriginJob job
  => OriginPort job failure
  -> DeliveryPolicy failure
  -> JobInstant
  -> SettlementStrategy job
dispatchSettlement port policy now =
  SettlementStrategy
    { beforeSuccess = \identifier job attempt output ->
        deliverVerdict
          policy
          now
          port
          job
          (Dispatch.confirmedOutcome identifier output (attemptCount attempt))
    , beforeRejection = \identifier job attempt failure ->
        deliverVerdict
          policy
          now
          port
          job
          ( Dispatch.failedOutcome
              identifier
              (Rejected failure)
              (attemptCount attempt)
          )
    , beforeExhaustion = \identifier job attempt failure -> do
        delivered <-
          deliver
            port
            job
            ( Dispatch.failedOutcome
                identifier
                ( DeadLettered
                    RetriesExhausted
                    (renderJobError job failure)
                )
                (attemptCount attempt)
            )

        case delivered of
          Left (OriginUnavailable deliveryFailure) ->
            report policy identifier deliveryFailure
          Left (OriginRefused deliveryFailure) ->
            report policy identifier deliveryFailure
          Right () -> pure ()
    }


deliverVerdict
  :: DeliveryPolicy failure
  -> JobInstant
  -> OriginPort job failure
  -> job
  -> DispatchOutcome job
  -> IO VerdictDelivery
deliverVerdict policy now port job outcome = do
  delivered <- deliver port job outcome

  pure case delivered of
    Left (OriginUnavailable failure) ->
      VerdictDeferred (retryUnavailable policy now failure)
    Left (OriginRefused failure) ->
      VerdictDeferred (retryRefused policy now failure)
    Right () -> VerdictAccepted


deliver
  :: OriginPort job failure
  -> job
  -> DispatchOutcome job
  -> IO (Either (OriginDeliveryError failure) ())
deliver (OriginPort send) = send


retryUnavailable
  :: DeliveryPolicy failure
  -> JobInstant
  -> failure
  -> JobInstant
retryUnavailable (DeliveryPolicy schedule _ _) = schedule


retryRefused
  :: DeliveryPolicy failure
  -> JobInstant
  -> failure
  -> JobInstant
retryRefused (DeliveryPolicy _ schedule _) = schedule


report :: DeliveryPolicy failure -> JobId -> failure -> IO ()
report (DeliveryPolicy _ _ send) = send


attemptCount :: JobAttempt -> Word32
attemptCount (JobAttempt attempt) = attempt
