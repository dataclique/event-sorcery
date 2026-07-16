module DomainSpec (spec) where

import Data.ByteString qualified as ByteString
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.Text (Text)
import Event.Sorcery.Aggregate (
  DecodeCause (DecodeCause),
  Dispatches (..),
  Effect (..),
  EventSourced (..),
  EventVersion (EventVersion),
  SchemaVersion (SchemaVersion),
  dispatchIntent,
  dispatchJobId,
 )
import Event.Sorcery.Dispatch (DispatchOutcome, kickoff)
import Event.Sorcery.Job (
  Job (..),
  JobId,
  jobIdText,
  jobType,
  mkJobId,
 )
import Event.Sorcery.Stream (
  ActualSequence (ActualSequence),
  ExpectedSequence (ExpectedSequence),
  MetadataMismatch (EventTypeMismatch, EventVersionMismatch),
  ReplayError (..),
  StoredEvent (..),
  StreamIdentity (StreamIdentity),
  StreamPosition (StreamPosition),
  StreamVersion (StreamVersion),
  replay,
  resume,
  streamKey,
  streamKeyIdentity,
 )
import Test.Hspec (Spec, it)
import Prelude (
  Either (..),
  Eq,
  IO,
  Int,
  Maybe (..),
  Show,
  error,
  maxBound,
  otherwise,
  pure,
  (&&),
  (==),
 )


spec :: Spec
spec = it "preserves the typed domain invariants" do
  identifier <-
    case mkJobId testJobIdText of
      Just value -> pure value
      Nothing -> error "valid job identifier was rejected"

  case mkJobId "" of
    Nothing -> pure ()
    Just _ -> error "empty job identifier was accepted"

  let intent = dispatchIntent identifier SendWelcomeEmail

  if dispatchJobId intent == identifier && jobIdText identifier == testJobIdText
    then pure ()
    else error "dispatch intent did not preserve the job identity"

  if jobType @SendWelcomeEmail == "send-welcome-email"
    then pure ()
    else error "job type was not reflected from its type-level symbol"

  case transition (Account 1) SendWelcome of
    Right (Dispatch _) -> pure ()
    _ -> error "declared job membership did not produce a dispatch effect"

  exerciseReplay


exerciseReplay :: IO ()
exerciseReplay = do
  if streamKeyIdentity key == StreamIdentity "account" "account-1"
    then pure ()
    else error "stream key did not preserve the typed aggregate identity"

  case replay key [opened] of
    Right (Just (Account 1)) -> pure ()
    _ -> error "valid stream did not replay"

  case replay key [opened {sequence = 2}] of
    Left
      ( EventSequenceMismatch
          (ExpectedSequence (StreamPosition 1))
          (ActualSequence (StreamPosition 2))
        ) -> pure ()
    _ -> error "stream gap was not rejected"

  case replay key [opened {eventType = "renamed"}] of
    Left
      ( EventMetadataMismatch
          (StreamPosition 1)
          (EventTypeMismatch "account-opened" "renamed")
        ) -> pure ()
    _ -> error "event metadata mismatch was not rejected"

  case replay key [opened {eventVersion = "2"}] of
    Left
      ( EventMetadataMismatch
          (StreamPosition 1)
          (EventVersionMismatch (EventVersion "1") (EventVersion "2"))
        ) -> pure ()
    _ -> error "event version mismatch was not rejected"

  case replay key [opened {payload = ByteString.pack [2]}] of
    Left
      ( EventDecodeFailed
          (StreamPosition 1)
          (DecodeCause "invalid account event")
        ) -> pure ()
    _ -> error "event decode failure was not retained"

  case replay key [welcome] of
    Left (EventApplicationFailed (StreamPosition 1) AccountError) -> pure ()
    _ -> error "event application failure was not retained"

  case resume key (StreamVersion 1) (Account 1) [welcome {sequence = 2}] of
    Right (Account 1) -> pure ()
    _ -> error "valid snapshot continuation did not replay"

  case resume key (StreamVersion maxBound) (Account 1) [] of
    Left (EventSequenceOverflow (StreamPosition position))
      | position == maxBound -> pure ()
    _ -> error "stream sequence overflow was not rejected"
  where
    key = streamKey @Account AccountId
    opened = StoredEvent 1 "account-opened" "1" ByteString.empty
    welcome =
      StoredEvent
        1
        "welcome-requested"
        "1"
        (ByteString.pack [1])


newtype Account = Account Int
  deriving stock (Eq, Show)


data AccountId = AccountId


data AccountCommand
  = OpenAccount
  | SendWelcome
  | SettleWelcome (DispatchOutcome SendWelcomeEmail)


data AccountEvent
  = AccountOpened
  | WelcomeRequested JobId


data AccountError = AccountError
  deriving stock (Eq, Show)


data SendWelcomeEmail = SendWelcomeEmail
  deriving stock (Eq, Show)


instance Job SendWelcomeEmail where
  type JobType SendWelcomeEmail = "send-welcome-email"


  encodeJob _ = ByteString.empty
  decodeJob _ = Right SendWelcomeEmail


instance Dispatches Account SendWelcomeEmail where
  injectDispatchIntent intent = WelcomeRequested (dispatchJobId intent)
  injectDispatchOutcome = SettleWelcome


instance EventSourced Account where
  type EntityId Account = AccountId
  type Command Account = AccountCommand
  type Event Account = AccountEvent
  type CommandError Account = AccountError
  type ApplyError Account = AccountError
  type Jobs Account = '[SendWelcomeEmail]


  aggregateType _ = "account"
  encodeEntityId AccountId = "account-1"
  eventType AccountOpened = "account-opened"
  eventType (WelcomeRequested _) = "welcome-requested"
  eventVersion _ = EventVersion "1"
  schemaVersion _ = SchemaVersion 1
  encodeEvent _ = ByteString.empty
  decodeEvent bytes
    | bytes == ByteString.empty = Right AccountOpened
    | bytes == ByteString.pack [1] = Right (WelcomeRequested testJobId)
    | otherwise = Left (DecodeCause "invalid account event")
  encodeSnapshot _ = ByteString.empty
  decodeSnapshot _ = Right (Account 1)
  originate AccountOpened = Right (Account 1)
  originate (WelcomeRequested _) = Left AccountError
  evolve account AccountOpened = Right account
  evolve (Account count) (WelcomeRequested _) = Right (Account count)
  initialize OpenAccount = Right (Events (AccountOpened :| []))
  initialize _ = Left AccountError
  transition _ OpenAccount = Left AccountError
  transition _ SendWelcome = Right (Dispatch (kickoff SendWelcomeEmail))
  transition _ (SettleWelcome _) = Left AccountError


testJobId :: JobId
testJobId = case mkJobId testJobIdText of
  Just identifier -> identifier
  Nothing -> error "valid test job identifier was rejected"


testJobIdText :: Text
testJobIdText = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
