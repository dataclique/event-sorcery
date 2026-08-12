module Main (main) where

import Data.ByteString qualified as ByteString
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.Text.Encoding qualified as Text
import EventSorcery.Aggregate (
  DecodeCause (DecodeCause),
  Dispatches (injectDispatchIntent),
  Effect (..),
  EventSourced (..),
  EventVersion (EventVersion),
  SchemaVersion (SchemaVersion),
  dispatchJobId,
 )
import EventSorcery.Engine (
  OpenOptions (OpenOptions),
  closeStore,
  openStore,
 )
import EventSorcery.Job (
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
import EventSorcery.Store (
  StoreError (..),
  executeCommand,
  loadEntity,
  mkStore,
 )
import EventSorcery.Stream (streamKey)
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
      openedAccount <- executeCommand store key (OpenAccount 10)
      deposited <- executeCommand store key (Deposit 5)
      reloaded <- loadEntity store key
      invalidEvent <- executeCommand store key EmitInvalidEvent
      afterInvalidEvent <- loadEntity store key
      dispatched <- executeCommand store key SendWelcome
      jobs <- pollJobs engine kind (JobInstant 1) (PollLimit 10)
      rejected <- executeCommand store key (OpenAccount 20)
      afterRejection <- loadEntity store key

      if initially
        == Right Nothing
        && openedAccount
          == Right (Account 10)
        && deposited
          == Right (Account 15)
        && reloaded
          == Right (Just (Account 15))
        && invalidEvent
          == Left (StoreDecisionRejected AccountAlreadyOpened)
        && afterInvalidEvent
          == Right (Just (Account 15))
        && dispatched
          == Right (Account 15)
        && jobs
          == Right [jobIdentifier]
        && rejected
          == Left (StoreCommandRejected AlreadyOpen)
        && afterRejection
          == Right (Just (Account 15))
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
  encodeSnapshot _ = ByteString.empty
  decodeSnapshot _ = Left (DecodeCause "snapshots are not used in this test")
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
  transition _ SendWelcome = Right (Dispatch SendWelcomeEmail)


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
