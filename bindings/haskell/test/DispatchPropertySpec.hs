module DispatchPropertySpec (spec) where

import Data.ByteString qualified as ByteString
import Data.Text (Text)
import Data.Word (Word32)
import Event.Sorcery.Dispatch (
  DispatchEvent (..),
  DispatchFailure (Rejected),
  DispatchRefused (DispatchOutcomeMismatch),
  DispatchReplay (DispatchReplay),
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
import Event.Sorcery.Job (Job (..), JobId, mkJobId)
import Test.Hspec (Spec, describe)
import Test.Hspec.QuickCheck (prop)
import Prelude (
  Bool (False),
  Either (..),
  Eq,
  Maybe (..),
  Show,
  error,
  (&&),
  (<$>),
  (==),
 )


spec :: Spec
spec = describe "dispatch protocol properties" do
  prop "settles matching confirmations and absorbs redelivery" do
    matchingConfirmationIsAbsorbed
  prop "settles matching failures and permits a fresh dispatch" do
    matchingFailurePermitsRetry
  prop "rejects confirmations for another job" do
    mismatchedConfirmationIsRejected
  prop "rejects replay events for another job" do
    mismatchedReplayIsRejected


matchingConfirmationIsAbsorbed :: Word32 -> Word32 -> Bool
matchingConfirmationIsAbsorbed output attempts =
  case originateDispatch (Dispatched firstJobId ProtocolJob) of
    Left _ -> False
    Right inFlight ->
      let outcome = confirmedOutcome @ProtocolJob firstJobId output attempts
       in case settleDispatch inFlight outcome of
            Right [event@(ConfirmedEvent settled)] ->
              evolveDispatch inFlight event == Right (Confirmed settled)
                && settleDispatch (Confirmed settled) outcome == Right []
                && settledJobId settled == firstJobId
                && settledOutput settled == output
                && settledAttempts settled == attempts
            _ -> False


matchingFailurePermitsRetry :: Word32 -> Word32 -> Bool
matchingFailurePermitsRetry failure attempts =
  case originateDispatch (Dispatched firstJobId ProtocolJob) of
    Left _ -> False
    Right inFlight ->
      let outcome =
            failedOutcome @ProtocolJob firstJobId (Rejected failure) attempts
       in case settleDispatch inFlight outcome of
            Right [event@(FailedEvent settled)] ->
              evolveDispatch inFlight event == Right (Failed settled)
                && settleDispatch (Failed settled) outcome == Right []
                && (dispatchJob <$> guardDispatch (Failed settled) ProtocolJob)
                  == Right ProtocolJob
                && settledFailureJobId settled == firstJobId
                && dispatchFailure settled == Rejected failure
                && settledFailureAttempts settled == attempts
            _ -> False


mismatchedConfirmationIsRejected :: Word32 -> Word32 -> Bool
mismatchedConfirmationIsRejected output attempts =
  case originateDispatch (Dispatched firstJobId ProtocolJob) of
    Left _ -> False
    Right inFlight ->
      settleDispatch
        inFlight
        (confirmedOutcome @ProtocolJob secondJobId output attempts)
        == Left DispatchOutcomeMismatch


mismatchedReplayIsRejected :: Word32 -> Word32 -> Bool
mismatchedReplayIsRejected output attempts =
  case originateDispatch (Dispatched secondJobId ProtocolJob) of
    Left _ -> False
    Right secondInFlight ->
      case settleDispatch
        secondInFlight
        (confirmedOutcome @ProtocolJob secondJobId output attempts) of
        Right [event] ->
          evolveDispatch (InFlight firstJobId) event == Left DispatchReplay
        _ -> False


data ProtocolJob = ProtocolJob
  deriving stock (Eq, Show)


instance Job ProtocolJob where
  type JobType ProtocolJob = "protocol-job"
  type JobOutput ProtocolJob = Word32
  type JobError ProtocolJob = Word32


  encodeJob _ = ByteString.empty
  decodeJob _ = Right ProtocolJob


firstJobId :: JobId
firstJobId = requireJobId "01ARZ3NDEKTSV4RRFFQ69G5FAV"


secondJobId :: JobId
secondJobId = requireJobId "01ARZ3NDEKTSV4RRFFQ69G5FAW"


requireJobId :: Text -> JobId
requireJobId encoded = case mkJobId encoded of
  Just identifier -> identifier
  Nothing -> error "property fixture contains an invalid job identifier"
