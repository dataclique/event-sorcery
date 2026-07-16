module Main (main) where

import Conduit (runConduit, sinkList, (.|))
import Control.Monad.Trans.Except (runExceptT)
import Data.ByteString qualified as ByteString
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.Maybe (Maybe (..))
import Data.Text (Text)
import Data.Unrestricted.Linear (Ur (Ur))
import EventSorcery.Engine (
  EngineError,
  OpenOptions (OpenOptions),
  Store,
  closeStore,
  openStore,
 )
import EventSorcery.Job (
  ClaimBudget (ClaimBudget),
  ClaimedJob,
  DeadReason (Rejected),
  JobClaimDetails (..),
  JobClaimResult (JobClaimed),
  JobExecutionRoute (ReconcileExecution, SubmitExecution),
  JobId,
  JobInstant (JobInstant),
  JobKind (JobKind),
  JobLeaseResult (LeaseHeld),
  JobSeed (JobSeed),
  JobSettlement (SettlementApplied),
  JobSettlementToken,
  LeaseDuration (LeaseDuration),
  PollLimit (PollLimit),
  WorkerId (WorkerId),
  acknowledgeJob,
  claimJob,
  commitWithJob,
  deadLetterJob,
  deferJob,
  enqueueJob,
  jobIdText,
  mkJobId,
  pollJobs,
  renewJob,
  retryJob,
  settlementToken,
  streamRunnableJobs,
 )
import EventSorcery.Stream (
  ProposedEvent (ProposedEvent),
  StreamIdentity (StreamIdentity),
  currentVersion,
 )
import Prelude (Either (Left, Right), IO, String, error, pure, (&&), (==))


main :: IO ()
main = do
  jobIdValidation

  opened <- openStore (OpenOptions "sqlite::memory:" 5000 1 1)

  case opened of
    Left _ -> error "failed to open the shared engine"
    Right store -> do
      exerciseAtomicCommit store
      exerciseRetry store
      exerciseDeferral store
      exerciseDeadLetter store

      closed <- closeStore store

      if closed == Right ()
        then pure ()
        else error "failed to close the shared engine"


jobIdValidation :: IO ()
jobIdValidation =
  case ( mkJobId valid
       , mkJobId "not-a-ulid"
       , mkJobId "80000000000000000000000000"
       ) of
    (Just identifier, Nothing, Nothing)
      | jobIdText identifier == valid -> pure ()
    _ -> error "Haskell JobId validation diverged from the Rust engine"
  where
    valid = "01ARZ3NDEKTSV4RRFFQ69G5FAV"


exerciseAtomicCommit :: Store -> IO ()
exerciseAtomicCommit store = do
  committed <-
    commitWithJob
      store
      stream
      0
      (ProposedEvent "opened" "1" payload :| [])
      seed
  expectUnit "atomic event-and-job commit failed" committed

  version <- currentVersion store stream
  streamed <-
    runExceptT
      ( runConduit
          (streamRunnableJobs store kind now (PollLimit 10) .| sinkList)
      )

  if version == Right 1 && streamed == Right [identifier]
    then pure ()
    else error "atomic commit was not visible through both features"

  (details, token) <- claimToken store identifier now
  renewed <- renewJob store details.reference (JobInstant 60_000)
  settled <- acknowledgeJob store token

  if details.attempt == 0
    && details.route == SubmitExecution
    && details.payload == payload
    && renewed == Right LeaseHeld
    && settled == Right SettlementApplied
    then pure ()
    else error "claimed job metadata or settlement was incorrect"
  where
    identifier = validJobId "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    stream = StreamIdentity "haskell-account" "account-1"
    seed = JobSeed identifier kind payload now


exerciseRetry :: Store -> IO ()
exerciseRetry store = do
  enqueued <- enqueueJob store seed
  expectUnit "retry job enqueue failed" enqueued

  (_, firstToken) <- claimToken store identifier now
  retried <- retryJob store firstToken later "transient failure"

  beforeRetry <- pollJobs store kind now (PollLimit 10)
  atRetry <- pollJobs store kind later (PollLimit 10)

  if retried == Right SettlementApplied
    && beforeRetry == Right []
    && atRetry == Right [identifier]
    then pure ()
    else error "retry scheduling was incorrect"

  (details, secondToken) <- claimToken store identifier later
  settled <- acknowledgeJob store secondToken

  if details.attempt == 1
    && details.route == ReconcileExecution
    && settled == Right SettlementApplied
    then pure ()
    else error "retried claim metadata was incorrect"
  where
    identifier = validJobId "01ARZ3NDEKTSV4RRFFQ69G5FAW"
    seed = JobSeed identifier kind payload now


exerciseDeferral :: Store -> IO ()
exerciseDeferral store = do
  enqueued <- enqueueJob store seed
  expectUnit "deferred job enqueue failed" enqueued

  (_, firstToken) <- claimToken store identifier now
  deferred <- deferJob store firstToken later
  beforeDeferral <- pollJobs store kind now (PollLimit 10)

  if deferred == Right SettlementApplied && beforeDeferral == Right []
    then pure ()
    else error "deferral scheduling was incorrect"

  (_, secondToken) <- claimToken store identifier later
  acknowledged <- acknowledgeJob store secondToken
  expectSettlement "deferred job acknowledgement failed" acknowledged
  where
    identifier = validJobId "01ARZ3NDEKTSV4RRFFQ69G5FAX"
    seed = JobSeed identifier kind payload now


exerciseDeadLetter :: Store -> IO ()
exerciseDeadLetter store = do
  enqueued <- enqueueJob store seed
  expectUnit "dead-letter job enqueue failed" enqueued

  (_, token) <- claimToken store identifier now
  dead <- deadLetterJob store token Rejected "rejected by dependency"
  runnable <- pollJobs store kind later (PollLimit 10)

  if dead == Right SettlementApplied && runnable == Right []
    then pure ()
    else error "dead-lettered job remained runnable"
  where
    identifier = validJobId "01ARZ3NDEKTSV4RRFFQ69G5FAY"
    seed = JobSeed identifier kind payload now


claimToken
  :: Store
  -> JobId
  -> JobInstant
  -> IO (JobClaimDetails, JobSettlementToken)
claimToken store identifier claimedAt = do
  claimed <-
    claimJob
      store
      identifier
      worker
      claimedAt
      (LeaseDuration 30_000)
      (ClaimBudget 50)
      releaseClaim

  case claimed of
    Right (JobClaimed result) -> pure result
    _ -> error "failed to claim the runnable job"


expectUnit :: String -> Either EngineError () -> IO ()
expectUnit _ (Right ()) = pure ()
expectUnit message (Left _) = error message


expectSettlement :: String -> Either EngineError JobSettlement -> IO ()
expectSettlement _ (Right SettlementApplied) = pure ()
expectSettlement message _ = error message


kind :: JobKind
kind = JobKind "haskell-test"


worker :: WorkerId
worker = WorkerId "haskell-worker"


now :: JobInstant
now = JobInstant 1_000


later :: JobInstant
later = JobInstant 90_000


payload :: ByteString.ByteString
payload = ByteString.pack [0, 1, 255]


validJobId :: Text -> JobId
validJobId value = case mkJobId value of
  Just identifier -> identifier
  Nothing -> error "valid test job identifier was rejected"


releaseClaim
  :: JobClaimDetails
  -> ClaimedJob
  %1 -> Ur (JobClaimDetails, JobSettlementToken)
releaseClaim details won =
  case settlementToken won of
    Ur token -> Ur (details, token)
