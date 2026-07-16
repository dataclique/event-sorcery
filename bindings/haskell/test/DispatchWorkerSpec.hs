module DispatchWorkerSpec (spec) where

import Data.ByteString qualified as ByteString
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.Text (Text)
import Event.Sorcery.Aggregate (
  DecodeCause (DecodeCause),
  Dispatches (..),
  Effect (..),
  EventSourced (..),
  EventVersion (EventVersion),
  SchemaVersion (SchemaVersion),
  dispatchIntentJob,
  dispatchJobId,
 )
import Event.Sorcery.Dispatch (
  DispatchEvent (..),
  DispatchOutcome,
  DispatchRefused,
  DispatchReplay,
  DispatchedJob (..),
  evolveDispatch,
  guardDispatch,
  settleDispatch,
  settledOutput,
 )
import Event.Sorcery.Dispatch.TestSupport (confirmedOutcome)
import Event.Sorcery.Dispatch.Worker (
  DeliveryPolicy,
  OriginJob (..),
  deliveryPolicy,
  dispatchWorker,
  runDispatchJobOnce,
  storeOriginPort,
 )
import Event.Sorcery.Engine (
  OpenOptions (OpenOptions),
  Store,
  closeStore,
  openStore,
 )
import Event.Sorcery.Job (
  ClaimBudget (ClaimBudget),
  Job (..),
  JobDecodeError (JobDecodeError),
  JobId,
  JobInstant (JobInstant),
  LeaseDuration (LeaseDuration),
  WorkerId (WorkerId),
  mkJobId,
 )
import Event.Sorcery.Job.Execution (
  DurableJob (..),
  JobAttempt,
  JobOutcome (JobDone),
  Reconciliation (Reconciled),
 )
import Event.Sorcery.Job.Worker (
  AttemptLimit,
  JobRunResult (..),
  JobWorker,
  jobWorker,
  mkAttemptLimit,
 )
import Event.Sorcery.Store (
  executeCommand,
  loadEntity,
  mkStore,
 )
import Event.Sorcery.Stream (StreamKey, streamKey)
import Test.Hspec (Spec, it)
import Prelude (
  Bool (False, True),
  Either (Left, Right),
  Eq,
  IO,
  Maybe (Just, Nothing),
  Show,
  String,
  error,
  fmap,
  otherwise,
  pure,
  (&&),
  (<>),
  (==),
 )


newtype Account = Account (DispatchedJob ChargeCard)


data AccountId = AccountId


data AccountCommand
  = OpenAccount
  | StartCharge
  | SettleCharge (DispatchOutcome ChargeCard)


data AccountEvent
  = AccountOpened
  | ChargeChanged (DispatchEvent ChargeCard)


data AccountError
  = AccountAlreadyOpen
  | AccountNotOpen
  | ChargeRefused DispatchRefused
  deriving stock (Eq, Show)


data AccountReplayError
  = AccountReplayFailed DispatchReplay
  | AccountOpenedTwice
  deriving stock (Eq, Show)


data ChargeCard = ChargeCard
  deriving stock (Eq, Show)


instance Job ChargeCard where
  type JobType ChargeCard = "charge-card"
  type JobOutput ChargeCard = Text
  type JobError ChargeCard = Text


  encodeJob _ = ByteString.empty
  decodeJob bytes
    | bytes == ByteString.empty = Right ChargeCard
    | otherwise = Left (JobDecodeError "invalid charge job")


instance DurableJob ChargeCard where
  type JobInput ChargeCard = IORef [Text]


  renderJobError _ failure = failure


  submit _ calls _ = do
    modifyIORef' calls (<> ["submit"])
    pure (Right (JobDone "charged"))


  reconcile _ calls _ = do
    modifyIORef' calls (<> ["reconcile"])
    pure (Right (Reconciled "charged"))


instance OriginJob ChargeCard where
  type Origin ChargeCard = Account
  originKey _ = accountKey


instance Dispatches Account ChargeCard where
  injectDispatchIntent intent =
    ChargeChanged
      (Dispatched (dispatchJobId intent) (dispatchIntentJob intent))
  injectDispatchOutcome = SettleCharge


instance EventSourced Account where
  type EntityId Account = AccountId
  type Command Account = AccountCommand
  type Event Account = AccountEvent
  type CommandError Account = AccountError
  type ApplyError Account = AccountReplayError
  type Jobs Account = '[ChargeCard]


  aggregateType _ = "account"
  encodeEntityId AccountId = "account-1"
  eventType AccountOpened = "account-opened"
  eventType (ChargeChanged (Dispatched _ _)) = "charge-dispatched"
  eventType (ChargeChanged (ConfirmedEvent _)) = "charge-confirmed"
  eventType (ChargeChanged (FailedEvent _)) = "charge-failed"
  eventVersion _ = EventVersion "1"
  schemaVersion _ = SchemaVersion 1
  encodeEvent AccountOpened = ByteString.singleton 0
  encodeEvent (ChargeChanged (Dispatched _ _)) = ByteString.singleton 1
  encodeEvent (ChargeChanged (ConfirmedEvent _)) = ByteString.singleton 2
  encodeEvent (ChargeChanged (FailedEvent _)) = ByteString.singleton 3
  decodeEvent bytes = case ByteString.unpack bytes of
    [0] -> Right AccountOpened
    [1] -> Right (ChargeChanged (Dispatched jobIdentifier ChargeCard))
    [2] -> Right confirmedEvent
    _ -> Left (DecodeCause "invalid account event")
  encodeSnapshot _ = ByteString.empty
  decodeSnapshot _ = Right (Account Idle)
  originate AccountOpened = Right (Account Idle)
  originate _ = Left AccountOpenedTwice
  evolve _ AccountOpened = Left AccountOpenedTwice
  evolve (Account state) (ChargeChanged event) =
    case evolveDispatch state event of
      Left failure -> Left (AccountReplayFailed failure)
      Right next -> Right (Account next)
  initialize OpenAccount = Right (Events (AccountOpened :| []))
  initialize _ = Left AccountNotOpen
  transition _ OpenAccount = Left AccountAlreadyOpen
  transition (Account state) StartCharge =
    case guardDispatch state ChargeCard of
      Left failure -> Left (ChargeRefused failure)
      Right request -> Right (Dispatch request)
  transition (Account state) (SettleCharge outcome) =
    case settleDispatch state outcome of
      Left failure -> Left (ChargeRefused failure)
      Right [] -> Right Unchanged
      Right (event : remaining) ->
        Right (Events (ChargeChanged event :| fmap ChargeChanged remaining))


spec :: Spec
spec = it "delivers sealed dispatch verdicts" do
  opened <- openStore (OpenOptions "sqlite::memory:" 5000 1 1)

  case opened of
    Left _ -> error "failed to open the shared engine"
    Right engine -> do
      let originStore = mkStore engine (pure jobIdentifier)
      calls <- newIORef []

      openedAccount <- executeCommand originStore accountKey OpenAccount
      dispatched <- executeCommand originStore accountKey StartCharge
      result <-
        runDispatchJobOnce
          ( dispatchWorker
              (runner engine calls)
              (storeOriginPort originStore)
              policy
          )
          jobIdentifier
          now
      settled <- loadEntity originStore accountKey
      recorded <- readIORef calls

      expect "account did not open" (fmap isIdle openedAccount == Right True)
      expect
        "charge was not dispatched"
        (fmap isInFlight dispatched == Right True)
      expect
        "verdict was not delivered before the job acknowledged"
        ( result == Right (JobSucceeded "charged")
            && fmap (fmap settledOutputOf) settled == Right (Just "charged")
            && recorded == ["submit"]
        )

      closed <- closeStore engine
      expect "failed to close the shared engine" (closed == Right ())


runner
  :: Store
  -> IORef [Text]
  -> JobWorker ChargeCard
runner engine =
  jobWorker
    engine
    (WorkerId "dispatch-worker")
    (LeaseDuration 30_000)
    (ClaimBudget 50)
    attemptLimit
    retrySchedule


policy :: DeliveryPolicy failure
policy = deliveryPolicy deliveryRetryAt deliveryRetryAt (\_ _ -> pure ())


deliveryRetryAt :: JobInstant -> failure -> JobInstant
deliveryRetryAt _ _ = later


retrySchedule :: JobAttempt -> JobInstant
retrySchedule _ = later


attemptLimit :: AttemptLimit
attemptLimit = case mkAttemptLimit 3 of
  Just limit -> limit
  Nothing -> error "valid attempt limit was rejected"


confirmedEvent :: AccountEvent
confirmedEvent =
  case settleDispatch
    (InFlight jobIdentifier)
    (confirmedOutcome jobIdentifier "charged" 1) of
    Right [event] -> ChargeChanged event
    _ -> error "test verdict did not settle"


isIdle :: Account -> Bool
isIdle (Account Idle) = True
isIdle _ = False


isInFlight :: Account -> Bool
isInFlight (Account (InFlight _)) = True
isInFlight _ = False


settledOutputOf :: Account -> Text
settledOutputOf (Account (Confirmed settled)) = settledOutput settled
settledOutputOf _ = error "account charge was not confirmed"


accountKey :: StreamKey Account
accountKey = streamKey @Account AccountId


jobIdentifier :: JobId
jobIdentifier = case mkJobId "01ARZ3NDEKTSV4RRFFQ69G5FCA" of
  Just identifier -> identifier
  Nothing -> error "valid test job identifier was rejected"


now :: JobInstant
now = JobInstant 1_000


later :: JobInstant
later = JobInstant 90_000


expect :: String -> Bool -> IO ()
expect _ True = pure ()
expect message False = error message
