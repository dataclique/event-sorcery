module Main (main) where

import Data.ByteString qualified as ByteString
import Data.Text (Text)
import EventSorcery.Dispatch (
  DispatchEvent (..),
  DispatchFailure (..),
  DispatchRefused (..),
  DispatchReplay (..),
  DispatchedJob (..),
  dispatchFailure,
  dispatchJob,
  evolveDispatch,
  guardDispatch,
  originateDispatch,
  settleDispatch,
  settledAttempts,
  settledFailureAttempts,
  settledFailureJobId,
  settledJobId,
  settledOutput,
 )
import EventSorcery.Dispatch.TestSupport (
  confirmedOutcome,
  failedOutcome,
 )
import EventSorcery.Job (
  DeadReason (RetriesExhausted),
  Job (..),
  JobId,
  mkJobId,
 )
import Prelude (
  Either (..),
  Eq,
  IO,
  Maybe (..),
  Show,
  error,
  pure,
  show,
  (&&),
  (<$>),
  (==),
 )


data ChargeCard = ChargeCard
  deriving stock (Eq, Show)


data Receipt = Receipt
  deriving stock (Eq, Show)


data ChargeError = CardDeclined
  deriving stock (Eq, Show)


instance Job ChargeCard where
  type JobType ChargeCard = "charge-card"
  type JobOutput ChargeCard = Receipt
  type JobError ChargeCard = ChargeError


  encodeJob _ = ByteString.empty
  decodeJob _ = Right ChargeCard


main :: IO ()
main = do
  let first = requireJobId "job-one"
      second = requireJobId "job-two"
      dispatched = Dispatched first ChargeCard
      confirmed = confirmedOutcome @ChargeCard first Receipt 2
      failed =
        failedOutcome @ChargeCard
          first
          (DeadLettered RetriesExhausted "gateway timeout")
          3

  case originateDispatch dispatched of
    Left failure -> error (show failure)
    Right inFlight -> do
      let guardedIdle = dispatchJob <$> guardDispatch Idle ChargeCard
          refusedOverlap = dispatchJob <$> guardDispatch inFlight ChargeCard
          confirmedEvents = settleDispatch inFlight confirmed
          failedEvents = settleDispatch inFlight failed
          wrongOutcome =
            settleDispatch
              inFlight
              (confirmedOutcome @ChargeCard second Receipt 1)
          wrongFailure =
            settleDispatch
              inFlight
              (failedOutcome @ChargeCard second (Rejected CardDeclined) 1)

      case (confirmedEvents, failedEvents) of
        (Right [ConfirmedEvent settled], Right [FailedEvent rejected]) -> do
          let confirmedState = evolveDispatch inFlight (ConfirmedEvent settled)
              failedState = evolveDispatch inFlight (FailedEvent rejected)
              duplicate = case confirmedState of
                Left _ -> Nothing
                Right state -> Just (settleDispatch state confirmed)
              refusedAfterConfirmation = case confirmedState of
                Left _ -> Nothing
                Right state ->
                  Just (dispatchJob <$> guardDispatch state ChargeCard)
              contradictoryVerdict = case confirmedState of
                Left _ -> Nothing
                Right state -> Just (settleDispatch state failed)
              retryAfterFailure = case failedState of
                Left _ -> Nothing
                Right state -> Just (guardDispatch state ChargeCard)
              duplicateFailure = case failedState of
                Left _ -> Nothing
                Right state -> Just (settleDispatch state failed)
              invalidReplay =
                evolveDispatch (Idle @ChargeCard) (ConfirmedEvent settled)
              overlappingReplay = evolveDispatch inFlight dispatched

          if guardedIdle == Right ChargeCard
            && refusedOverlap == Left DispatchInFlight
            && wrongOutcome == Left DispatchOutcomeMismatch
            && wrongFailure == Left DispatchOutcomeMismatch
            && settledJobId settled == first
            && settledOutput settled == Receipt
            && settledAttempts settled == 2
            && settledFailureJobId rejected == first
            && dispatchFailure rejected
              == DeadLettered RetriesExhausted "gateway timeout"
            && settledFailureAttempts rejected == 3
            && duplicate == Just (Right [])
            && refusedAfterConfirmation
              == Just (Left DispatchAlreadyConfirmed)
            && contradictoryVerdict
              == Just (Left DispatchOutcomeMismatch)
            && ((dispatchJob <$>) <$> retryAfterFailure)
              == Just (Right ChargeCard)
            && duplicateFailure == Just (Right [])
            && invalidReplay == Left DispatchReplay
            && overlappingReplay == Left DispatchReplay
            then pure ()
            else error "dispatch state machine violated its native contract"
        _ -> error "dispatch settlement did not produce sealed events"


requireJobId :: Text -> JobId
requireJobId value = case mkJobId value of
  Just identifier -> identifier
  Nothing -> error "test job id must be non-empty"
