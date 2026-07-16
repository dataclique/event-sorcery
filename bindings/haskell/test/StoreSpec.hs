module Main (main) where

import Data.ByteString qualified as ByteString
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.Text.Encoding qualified as Text
import Event.Sorcery.Aggregate (
  DecodeCause (DecodeCause),
  Dispatches (..),
  Effect (..),
  EventSourced (..),
  EventVersion (EventVersion),
  SchemaVersion (SchemaVersion),
  dispatchJobId,
 )
import Event.Sorcery.Dispatch (DispatchOutcome, kickoff)
import Event.Sorcery.Engine (
  OpenOptions (OpenOptions),
  closeStore,
  openStore,
 )
import Event.Sorcery.Job (
  Job (..),
  JobDecodeError (JobDecodeError),
  JobId,
  JobInstant (JobInstant),
  JobKind (JobKind),
  PollLimit (PollLimit),
  jobIdText,
  mkJobId,
  pollJobs,
 )
import Event.Sorcery.Snapshot qualified as Snapshot
import Event.Sorcery.Store (
  StoreError (..),
  executeCommand,
  loadEntity,
  mkStore,
  snapshotEntity,
 )
import Event.Sorcery.Stream (streamKey, streamKeyIdentity)
import Prelude (
  Either (..),
  Eq,
  IO,
  Int,
  Maybe (..),
  Show,
  error,
  fromIntegral,
  otherwise,
  pure,
  (&&),
  (+),
  (==),
 )


main :: IO ()
main = do
  opened <- openStore (OpenOptions "sqlite::memory:" 5000 1 1)

  case opened of
    Left _ -> error "failed to open the shared engine"
    Right engine -> do
      let store = mkStore engine (pure jobIdentifier)
          key = streamKey @Account AccountId

      initially <- loadEntity store key
      emptySnapshot <- snapshotEntity store key
      openedAccount <- executeCommand store key (OpenAccount 10)
      deposited <- executeCommand store key (Deposit 5)
      snapshotted <- snapshotEntity store key
      afterSnapshotDeposit <- executeCommand store key (Deposit 7)
      reloaded <- loadEntity store key
      invalidEvent <- executeCommand store key EmitInvalidEvent
      afterInvalidEvent <- loadEntity store key
      dispatched <- executeCommand store key SendWelcome
      jobs <- pollJobs engine kind (JobInstant 1) (PollLimit 10)
      rejected <- executeCommand store key (OpenAccount 20)
      afterRejection <- loadEntity store key
      corrupted <-
        Snapshot.storeSnapshot
          engine
          ( Snapshot.snapshotWrite
              (streamKeyIdentity key)
              4
              (ByteString.pack [255, 255])
          )
      corruptedLoad <- loadEntity store key
      discarded <- Snapshot.discardSnapshot engine (streamKeyIdentity key)
      recovered <- loadEntity store key

      if initially
        == Right Nothing
        && emptySnapshot
          == Right Nothing
        && openedAccount
          == Right (Account 10)
        && deposited
          == Right (Account 15)
        && snapshotted
          == Right (Just (Account 15))
        && afterSnapshotDeposit
          == Right (Account 22)
        && reloaded
          == Right (Just (Account 22))
        && invalidEvent
          == Left (StoreDecisionRejected AccountAlreadyOpened)
        && afterInvalidEvent
          == Right (Just (Account 22))
        && dispatched
          == Right (Account 22)
        && jobs
          == Right [jobIdentifier]
        && rejected
          == Left (StoreCommandRejected AlreadyOpen)
        && afterRejection
          == Right (Just (Account 22))
        && corrupted
          == Right (Snapshot.SnapshotVersion 2)
        && corruptedLoad
          == Left
            ( StoreSnapshotDecodeFailed
                4
                (DecodeCause "invalid account snapshot")
            )
        && discarded
          == Right ()
        && recovered
          == Right (Just (Account 22))
        then pure ()
        else error "typed command execution did not preserve store invariants"

      closed <- closeStore engine

      if closed == Right ()
        then pure ()
        else error "failed to close the shared engine"


newtype Account = Account Int
  deriving stock (Eq, Show)


data AccountId = AccountId


data AccountCommand
  = OpenAccount Int
  | Deposit Int
  | EmitInvalidEvent
  | SendWelcome
  | SettleWelcome (DispatchOutcome SendWelcomeEmail)


data AccountEvent
  = AccountOpened Int
  | FundsDeposited Int
  | WelcomeRequested JobId


data AccountCommandError = AlreadyOpen
  deriving stock (Eq, Show)


data AccountApplyError
  = AccountAlreadyOpened
  | DepositBeforeOpen
  deriving stock (Eq, Show)


data SendWelcomeEmail = SendWelcomeEmail
  deriving stock (Eq, Show)


instance Job SendWelcomeEmail where
  type JobType SendWelcomeEmail = "send-welcome-email"


  encodeJob _ = ByteString.empty
  decodeJob bytes
    | bytes == ByteString.empty = Right SendWelcomeEmail
    | otherwise = Left (JobDecodeError "invalid welcome job")


instance Dispatches Account SendWelcomeEmail where
  injectDispatchIntent intent = WelcomeRequested (dispatchJobId intent)
  injectDispatchOutcome = SettleWelcome


instance EventSourced Account where
  type EntityId Account = AccountId
  type Command Account = AccountCommand
  type Event Account = AccountEvent
  type CommandError Account = AccountCommandError
  type ApplyError Account = AccountApplyError
  type Jobs Account = '[SendWelcomeEmail]


  aggregateType _ = "account"
  encodeEntityId AccountId = "account-1"
  eventType (AccountOpened _) = "account-opened"
  eventType (FundsDeposited _) = "funds-deposited"
  eventType (WelcomeRequested _) = "welcome-requested"
  eventVersion _ = EventVersion "1"
  schemaVersion _ = SchemaVersion 1
  encodeEvent (AccountOpened amount) =
    ByteString.pack [0, fromIntegral amount]
  encodeEvent (FundsDeposited amount) =
    ByteString.pack [1, fromIntegral amount]
  encodeEvent (WelcomeRequested identifier) =
    ByteString.cons 2 (Text.encodeUtf8 (jobIdText identifier))
  decodeEvent bytes = case ByteString.uncons bytes of
    Just (0, amount) -> decodeAmount AccountOpened amount
    Just (1, amount) -> decodeAmount FundsDeposited amount
    Just (2, encodedIdentifier) -> decodeWelcome encodedIdentifier
    _ -> Left invalidEventEncoding
  encodeSnapshot (Account balance) = ByteString.singleton (fromIntegral balance)
  decodeSnapshot bytes = case ByteString.unpack bytes of
    [balance] -> Right (Account (fromIntegral balance))
    _ -> Left (DecodeCause "invalid account snapshot")
  originate (AccountOpened amount) = Right (Account amount)
  originate _ = Left DepositBeforeOpen
  evolve _ (AccountOpened _) = Left AccountAlreadyOpened
  evolve (Account balance) (FundsDeposited amount) =
    Right (Account (balance + amount))
  evolve account (WelcomeRequested _) = Right account
  initialize (OpenAccount amount) = Right (Events (AccountOpened amount :| []))
  initialize _ = Left AlreadyOpen
  transition _ (OpenAccount _) = Left AlreadyOpen
  transition _ (Deposit amount) = Right (Events (FundsDeposited amount :| []))
  transition _ EmitInvalidEvent = Right (Events (AccountOpened 99 :| []))
  transition _ SendWelcome = Right (Dispatch (kickoff SendWelcomeEmail))
  transition _ (SettleWelcome _) = Left AlreadyOpen


decodeAmount
  :: (Int -> AccountEvent)
  -> ByteString.ByteString
  -> Either DecodeCause AccountEvent
decodeAmount constructor bytes = case ByteString.unpack bytes of
  [amount] -> Right (constructor (fromIntegral amount))
  _ -> Left invalidEventEncoding


decodeWelcome :: ByteString.ByteString -> Either DecodeCause AccountEvent
decodeWelcome bytes = case Text.decodeUtf8' bytes of
  Left _ -> Left invalidEventEncoding
  Right encodedIdentifier -> case mkJobId encodedIdentifier of
    Just identifier -> Right (WelcomeRequested identifier)
    Nothing -> Left invalidEventEncoding


invalidEventEncoding :: DecodeCause
invalidEventEncoding = DecodeCause "invalid account event"


jobIdentifier :: JobId
jobIdentifier = case mkJobId "01ARZ3NDEKTSV4RRFFQ69G5FAV" of
  Just identifier -> identifier
  Nothing -> error "valid test job identifier was rejected"


kind :: JobKind
kind = JobKind "send-welcome-email"
