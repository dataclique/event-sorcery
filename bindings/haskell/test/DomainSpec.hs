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
  AggregateId (AggregateId),
  AggregateType (AggregateType),
  EventType (EventType),
  ExpectedSequence (ExpectedSequence),
  MetadataMismatch (EventTypeMismatch, EventVersionMismatch),
  ReplayError (..),
  StoredEvent (..),
  StreamIdentity (StreamIdentity),
  StreamKey,
  StreamPosition (StreamPosition),
  StreamVersion (StreamVersion),
  replay,
  resume,
  streamKey,
  streamKeyIdentity,
 )
import Test.Hspec (Spec, describe, expectationFailure, it, shouldBe)
import Prelude (
  Either (..),
  Eq,
  Int,
  Maybe (..),
  Show,
  error,
  fmap,
  maxBound,
  otherwise,
  pure,
  ($),
  (==),
 )


spec :: Spec
spec = do
  describe "typed job dispatch" $ do
    it "accepts a ULID-shaped job identifier" $
      fmap jobIdText (mkJobId testJobIdText) `shouldBe` Just testJobIdText

    it "rejects an empty job identifier" $
      mkJobId "" `shouldBe` Nothing

    it "preserves the job identity a dispatch intent carries" $
      dispatchJobId (dispatchIntent testJobId SendWelcomeEmail)
        `shouldBe` testJobId

    it "reflects the job type from its type-level symbol" $
      jobType @SendWelcomeEmail `shouldBe` "send-welcome-email"

    it "produces a dispatch effect for a declared job membership" $
      case transition (Account 1) SendWelcome of
        Right (Dispatch _) -> pure ()
        _ ->
          expectationFailure
            "declared job membership did not produce a dispatch effect"

  describe "stream replay" $ do
    it "preserves the typed aggregate identity in the stream key" $
      streamKeyIdentity accountKey
        `shouldBe` StreamIdentity
          (AggregateType "account")
          (AggregateId "account-1")

    it "replays a valid stream" $
      replay accountKey [openedEvent] `shouldBe` Right (Just (Account 1))

    it "rejects a stream gap" $
      replay accountKey [openedEvent {sequence = 2}]
        `shouldBe` Left
          ( EventSequenceMismatch
              (ExpectedSequence (StreamPosition 1))
              (ActualSequence (StreamPosition 2))
          )

    it "rejects an event whose metadata disagrees with the entity" $
      replay accountKey [openedEvent {eventType = EventType "renamed"}]
        `shouldBe` Left
          ( EventMetadataMismatch
              (StreamPosition 1)
              (EventTypeMismatch "account-opened" "renamed")
          )

    it "rejects an event whose version disagrees with the entity" $
      replay accountKey [openedEvent {eventVersion = EventVersion "2"}]
        `shouldBe` Left
          ( EventMetadataMismatch
              (StreamPosition 1)
              (EventVersionMismatch (EventVersion "1") (EventVersion "2"))
          )

    it "retains the cause an event decode failed with" $
      replay accountKey [openedEvent {payload = ByteString.pack [2]}]
        `shouldBe` Left
          ( EventDecodeFailed
              (StreamPosition 1)
              (DecodeCause "invalid account event")
          )

    it "retains the error an event application failed with" $
      replay accountKey [welcomeEvent]
        `shouldBe` Left
          (EventApplicationFailed (StreamPosition 1) AccountError)

    it "replays a snapshot continuation" $
      resume
        accountKey
        (StreamVersion 1)
        (Account 1)
        [welcomeEvent {sequence = 2}]
        `shouldBe` Right (Account 1)

    it "rejects a stream sequence overflow" $
      resume accountKey (StreamVersion maxBound) (Account 1) []
        `shouldBe` Left (EventSequenceOverflow (StreamPosition maxBound))


accountKey :: StreamKey Account
accountKey = streamKey @Account AccountId


openedEvent :: StoredEvent
openedEvent =
  StoredEvent
    1
    (EventType "account-opened")
    (EventVersion "1")
    ByteString.empty


welcomeEvent :: StoredEvent
welcomeEvent =
  StoredEvent
    1
    (EventType "welcome-requested")
    (EventVersion "1")
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
