module DispatchSpec (spec) where

import Data.ByteString qualified as ByteString
import Data.Text (Text)
import Event.Sorcery.Dispatch (
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
import Event.Sorcery.Dispatch.TestSupport (
  confirmedOutcome,
  failedOutcome,
 )
import Event.Sorcery.Job (
  DeadReason (RetriesExhausted),
  Job (..),
  JobId,
  mkJobId,
 )
import Test.Hspec (Spec, expectationFailure, it, shouldBe)
import Prelude (
  Either (..),
  Eq,
  Maybe (..),
  Show,
  error,
  show,
  ($),
  (<$>),
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


spec :: Spec
spec = it "preserves the dispatch state machine" $ do
  let first = requireJobId "01ARZ3NDEKTSV4RRFFQ69G5FAV"
      second = requireJobId "01ARZ3NDEKTSV4RRFFQ69G5FAW"
      dispatched = Dispatched first ChargeCard
      confirmed = confirmedOutcome @ChargeCard first Receipt 2
      failed =
        failedOutcome @ChargeCard
          first
          (DeadLettered RetriesExhausted "gateway timeout")
          3

  case originateDispatch dispatched of
    Left failure -> expectationFailure (show failure)
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

          guardedIdle `shouldBe` Right ChargeCard
          refusedOverlap `shouldBe` Left DispatchInFlight
          wrongOutcome `shouldBe` Left DispatchOutcomeMismatch
          wrongFailure `shouldBe` Left DispatchOutcomeMismatch
          settledJobId settled `shouldBe` first
          settledOutput settled `shouldBe` Receipt
          settledAttempts settled `shouldBe` 2
          settledFailureJobId rejected `shouldBe` first
          dispatchFailure rejected
            `shouldBe` DeadLettered RetriesExhausted "gateway timeout"
          settledFailureAttempts rejected `shouldBe` 3
          duplicate `shouldBe` Just (Right [])
          refusedAfterConfirmation
            `shouldBe` Just (Left DispatchAlreadyConfirmed)
          contradictoryVerdict `shouldBe` Just (Left DispatchOutcomeMismatch)
          ((dispatchJob <$>) <$> retryAfterFailure)
            `shouldBe` Just (Right ChargeCard)
          duplicateFailure `shouldBe` Just (Right [])
          invalidReplay `shouldBe` Left DispatchReplay
          overlappingReplay `shouldBe` Left DispatchReplay
        _ ->
          expectationFailure
            "dispatch settlement did not produce sealed events"


requireJobId :: Text -> JobId
requireJobId value = case mkJobId value of
  Just identifier -> identifier
  Nothing -> error "test job id must be non-empty"
