module JobWorkerSpec (spec) where

import Control.Concurrent.Async (async, wait)
import Control.Concurrent.MVar (
  MVar,
  newEmptyMVar,
  newMVar,
  putMVar,
  takeMVar,
 )
import Data.ByteString qualified as ByteString
import Data.IORef (
  IORef,
  atomicModifyIORef',
  modifyIORef',
  newIORef,
  readIORef,
 )
import Data.Text (Text)
import Data.Word (Word32)
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
  JobKind (JobKind),
  JobSeed (JobSeed),
  LeaseDuration (LeaseDuration),
  WorkerId (WorkerId),
  enqueueJob,
  mkJobId,
 )
import Event.Sorcery.Job.Execution (
  DurableJob (..),
  JobAttempt (JobAttempt),
  JobFailure (TerminalFailure, TransientFailure),
  JobOutcome (JobDone),
  Reconciliation (Reconciled),
 )
import Event.Sorcery.Job.Worker (
  AttemptLimit,
  JobRunError (JobRunDecodeFailed),
  JobRunResult (..),
  JobWorker,
  jobWorker,
  mkAttemptLimit,
  renewalSchedule,
  renewingJobWorker,
  runJobOnce,
 )
import Test.Hspec (Spec, expectationFailure, it, shouldBe)
import Prelude (
  Either (Left, Right),
  IO,
  Maybe (Just, Nothing),
  error,
  pure,
  ($),
  (+),
  (<>),
  (==),
 )


data ProbeJob
  = Succeeds
  | FailsTransiently
  | FailsTerminally


data RenewingProbeJob = WaitsForRelease


instance Job ProbeJob where
  type JobType ProbeJob = "worker-probe"
  type JobOutput ProbeJob = Text
  type JobError ProbeJob = Text


  encodeJob job = ByteString.singleton case job of
    Succeeds -> 0
    FailsTransiently -> 1
    FailsTerminally -> 2


  decodeJob bytes = case ByteString.unpack bytes of
    [0] -> Right Succeeds
    [1] -> Right FailsTransiently
    [2] -> Right FailsTerminally
    _ -> Left (JobDecodeError "invalid probe job")


instance Job RenewingProbeJob where
  type JobType RenewingProbeJob = "renewing-worker-probe"
  type JobOutput RenewingProbeJob = Text
  type JobError RenewingProbeJob = Text


  encodeJob WaitsForRelease = ByteString.singleton 0


  decodeJob bytes = case ByteString.unpack bytes of
    [0] -> Right WaitsForRelease
    _ -> Left (JobDecodeError "invalid renewing probe job")


instance DurableJob ProbeJob where
  type JobInput ProbeJob = IORef [Text]


  renderJobError _ failure = failure


  submit _ calls job = do
    modifyIORef' calls (<> ["submit"])

    pure case job of
      Succeeds -> Right (JobDone "submitted")
      FailsTransiently -> Left (TransientFailure "unavailable")
      FailsTerminally -> Left (TerminalFailure "rejected")


  reconcile _ calls _ = do
    modifyIORef' calls (<> ["reconcile"])
    pure (Right (Reconciled "reconciled"))


instance DurableJob RenewingProbeJob where
  type JobInput RenewingProbeJob = MVar ()


  renderJobError _ failure = failure


  submit _ release WaitsForRelease = do
    takeMVar release
    pure (Right (JobDone "renewed"))


  reconcile _ _ WaitsForRelease =
    pure (Right (Reconciled "reconciled"))


spec :: Spec
spec = it "settles and renews durable job executions" $ do
  opened <- openStore (OpenOptions "sqlite::memory:" 5000 1 1)

  case opened of
    Left _ -> expectationFailure "failed to open the shared engine"
    Right store -> do
      successfulJobIsAcknowledged store
      transientFailureRetriesThenReconciles store
      terminalFailureIsDeadLettered store
      exhaustedFailureIsDeadLettered store
      undecodableJobIsDeadLettered store
      longExecutionRenewsItsLease store

      closed <- closeStore store
      closed `shouldBe` Right ()


successfulJobIsAcknowledged :: Store -> IO ()
successfulJobIsAcknowledged store = do
  calls <- newIORef []
  identifier <- enqueue store "01ARZ3NDEKTSV4RRFFQ69G5FB0" Succeeds
  result <- runJobOnce (runner store calls attemptLimit) identifier now
  repeated <- runJobOnce (runner store calls attemptLimit) identifier later
  recorded <- readIORef calls

  result `shouldBe` Right (JobSucceeded "submitted")
  repeated `shouldBe` Right JobRunSkipped
  recorded `shouldBe` ["submit"]


transientFailureRetriesThenReconciles :: Store -> IO ()
transientFailureRetriesThenReconciles store = do
  calls <- newIORef []
  identifier <- enqueue store "01ARZ3NDEKTSV4RRFFQ69G5FB1" FailsTransiently
  first <- runJobOnce (runner store calls attemptLimit) identifier now
  second <- runJobOnce (runner store calls attemptLimit) identifier later
  recorded <- readIORef calls

  first
    `shouldBe` Right (JobRetryScheduled (JobAttempt 1) later "unavailable")
  second `shouldBe` Right (JobSucceeded "reconciled")
  recorded `shouldBe` ["submit", "reconcile"]


terminalFailureIsDeadLettered :: Store -> IO ()
terminalFailureIsDeadLettered store = do
  calls <- newIORef []
  identifier <- enqueue store "01ARZ3NDEKTSV4RRFFQ69G5FB2" FailsTerminally
  result <- runJobOnce (runner store calls attemptLimit) identifier now
  repeated <- runJobOnce (runner store calls attemptLimit) identifier later

  result `shouldBe` Right (JobRejected "rejected")
  repeated `shouldBe` Right JobRunSkipped


exhaustedFailureIsDeadLettered :: Store -> IO ()
exhaustedFailureIsDeadLettered store = do
  calls <- newIORef []
  identifier <- enqueue store "01ARZ3NDEKTSV4RRFFQ69G5FB3" FailsTransiently
  result <- runJobOnce (runner store calls singleAttempt) identifier now
  repeated <- runJobOnce (runner store calls singleAttempt) identifier later

  result `shouldBe` Right (JobRetriesExhausted (JobAttempt 1) "unavailable")
  repeated `shouldBe` Right JobRunSkipped


undecodableJobIsDeadLettered :: Store -> IO ()
undecodableJobIsDeadLettered store = do
  calls <- newIORef []
  let identifier = validJobId "01ARZ3NDEKTSV4RRFFQ69G5FB4"
      seed = JobSeed identifier kind (ByteString.singleton 255) now
  enqueued <- enqueueJob store seed
  result <- runJobOnce (runner store calls attemptLimit) identifier now
  repeated <- runJobOnce (runner store calls attemptLimit) identifier later

  enqueued `shouldBe` Right ()
  result
    `shouldBe` Left
      (JobRunDecodeFailed identifier (JobDecodeError "invalid probe job"))
  repeated `shouldBe` Right JobRunSkipped


longExecutionRenewsItsLease :: Store -> IO ()
longExecutionRenewsItsLease store = do
  releaseExecution <- newEmptyMVar
  allowRenewal <- newEmptyMVar
  renewalCompleted <- newEmptyMVar
  stopRenewal <- newEmptyMVar
  competingRelease <- newMVar ()
  waitCount <- newIORef 0
  identifier <- enqueueRenewing store
  let schedule =
        renewalSchedule
          (renewalWait waitCount allowRenewal renewalCompleted stopRenewal)
          (pure renewedUntil)
      renewingRunner =
        renewingJobWorker
          (renewingProbeRunner store releaseExecution)
          schedule
  running <- async (runJobOnce renewingRunner identifier now)

  putMVar allowRenewal ()
  takeMVar renewalCompleted
  competing <-
    runJobOnce
      (renewingProbeRunner store competingRelease)
      identifier
      afterOriginalLease

  putMVar releaseExecution ()
  completed <- wait running

  competing `shouldBe` Right JobRunSkipped
  completed `shouldBe` Right (JobSucceeded "renewed")


renewalWait
  :: IORef Word32
  -> MVar ()
  -> MVar ()
  -> MVar ()
  -> IO ()
renewalWait waitCount allowRenewal renewalCompleted stopRenewal = do
  invocation <- atomicModifyIORef' waitCount (\count -> (count + 1, count))

  if invocation == 0
    then takeMVar allowRenewal
    else do
      putMVar renewalCompleted ()
      takeMVar stopRenewal


runner :: Store -> IORef [Text] -> AttemptLimit -> JobWorker ProbeJob
runner store calls limit =
  jobWorker
    store
    (WorkerId "haskell-worker")
    (LeaseDuration 30_000)
    (ClaimBudget 50)
    limit
    retrySchedule
    calls


renewingProbeRunner :: Store -> MVar () -> JobWorker RenewingProbeJob
renewingProbeRunner store =
  jobWorker
    store
    (WorkerId "renewing-haskell-worker")
    (LeaseDuration 30_000)
    (ClaimBudget 50)
    attemptLimit
    retrySchedule


enqueue :: Store -> Text -> ProbeJob -> IO JobId
enqueue store rawIdentifier job = do
  let identifier = validJobId rawIdentifier
  result <- enqueueJob store (JobSeed identifier kind (encodeJob job) now)

  case result of
    Right () -> pure identifier
    Left _ -> error "failed to enqueue test job"


enqueueRenewing :: Store -> IO JobId
enqueueRenewing store = do
  let identifier = validJobId "01ARZ3NDEKTSV4RRFFQ69G5FB5"
      seed =
        JobSeed
          identifier
          (JobKind "renewing-worker-probe")
          (encodeJob WaitsForRelease)
          now
  result <- enqueueJob store seed

  case result of
    Right () -> pure identifier
    Left _ -> error "failed to enqueue renewing test job"


retrySchedule :: JobAttempt -> JobInstant
retrySchedule _ = later


attemptLimit :: AttemptLimit
attemptLimit = validAttemptLimit 3


singleAttempt :: AttemptLimit
singleAttempt = validAttemptLimit 1


validAttemptLimit :: Word32 -> AttemptLimit
validAttemptLimit value = case mkAttemptLimit value of
  Just limit -> limit
  Nothing -> error "valid attempt limit was rejected"


validJobId :: Text -> JobId
validJobId value = case mkJobId value of
  Just identifier -> identifier
  Nothing -> error "valid test job identifier was rejected"


kind :: JobKind
kind = JobKind "worker-probe"


now :: JobInstant
now = JobInstant 1_000


later :: JobInstant
later = JobInstant 90_000


afterOriginalLease :: JobInstant
afterOriginalLease = JobInstant 40_000


renewedUntil :: JobInstant
renewedUntil = JobInstant 90_000
