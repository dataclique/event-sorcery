module Main (main) where

import Conduit (runConduit, sinkList, (.|))
import Control.Exception (finally)
import Control.Monad.Trans.Except (runExceptT)
import Data.ByteString (ByteString)
import Data.ByteString qualified as ByteString
import Data.List.NonEmpty (NonEmpty (..))
import Data.Maybe (Maybe (Just, Nothing))
import Data.Unrestricted.Linear (Ur (Ur))
import EventSorcery.Engine (OpenOptions (..), closeStore, openStore)
import EventSorcery.Job (
  AggregateId (..),
  AggregateType (..),
  ClaimBudget (..),
  ClaimedJob,
  ConflictDetail (..),
  DeadReason (Rejected),
  EngineError (..),
  JobClaimDetails (..),
  JobClaimResult (JobClaimed),
  JobExecutionRoute (ReconcileExecution, SubmitExecution),
  JobId,
  JobInstant (..),
  JobKind (..),
  JobLeaseResult (LeaseHeld),
  JobSeed (..),
  JobSettlement (SettlementApplied),
  JobSettlementToken,
  LeaseDuration (..),
  PollLimit (..),
  Store,
  WorkerId (..),
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
  EventType (..),
  EventVersion (..),
  ProposedEvent (..),
  StreamIdentity (..),
  commit,
  currentVersion,
 )
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.HUnit (assertFailure, testCase, (@?=))
import Prelude (
  Either (..),
  IO,
  Show (show),
  error,
  pure,
  ($),
  (<>),
  (>>=),
 )


main :: IO ()
main = defaultMain tests


tests :: TestTree
tests =
  testGroup
    "durable jobs"
    [ testCase "accepts only ULID-shaped job identifiers" $
        case ( mkJobId "01ARZ3NDEKTSV4RRFFQ69G5FAV"
             , mkJobId "not-a-ulid"
             , mkJobId "80000000000000000000000000"
             ) of
          (Just accepted, Nothing, Nothing) ->
            jobIdText accepted @?= "01ARZ3NDEKTSV4RRFFQ69G5FAV"
          (_, _, _) ->
            assertFailure
              "Haskell JobId validation diverged from the Rust engine"
    , testCase "commits events and the job they dispatch together" $
        withStore $ \store -> do
          commitWithJob store stream 0 (proposed :| []) seed
            >>= (@?= Right ())
          currentVersion store stream >>= (@?= Right 1)
          pollJobs store kind now pollLimit >>= (@?= Right [identifier])
    , testCase "leaves no job behind when the commit conflicts" $
        withStore $ \store -> do
          commit store stream 0 (proposed :| []) >>= (@?= Right ())
          conflicted <- commitWithJob store stream 0 (proposed :| []) seed
          conflicted
            @?= Left
              (OptimisticConflict (ConflictDetail aggregateType aggregateId 0 1))
          currentVersion store stream >>= (@?= Right 1)
          pollJobs store kind now pollLimit >>= (@?= Right [])
    , testCase "claims a runnable job, renews it and acknowledges it" $
        withStore $ \store -> do
          enqueueJob store seed >>= (@?= Right ())
          (details, token) <- claimFixture store now
          details.attempt @?= 0
          details.route @?= SubmitExecution
          details.payload @?= payload
          renewJob store details.reference (JobInstant 60_000)
            >>= (@?= Right LeaseHeld)
          acknowledgeJob store token >>= (@?= Right SettlementApplied)
          pollJobs store kind later pollLimit >>= (@?= Right [])
    , testCase "reschedules a retried job as a reconciliation" $
        withStore $ \store -> do
          enqueueJob store seed >>= (@?= Right ())
          (_, firstToken) <- claimFixture store now
          retryJob store firstToken later "transient failure"
            >>= (@?= Right SettlementApplied)
          pollJobs store kind now pollLimit >>= (@?= Right [])
          pollJobs store kind later pollLimit >>= (@?= Right [identifier])
          (details, secondToken) <- claimFixture store later
          details.attempt @?= 1
          details.route @?= ReconcileExecution
          acknowledgeJob store secondToken >>= (@?= Right SettlementApplied)
    , testCase "holds a deferred job back until its next instant" $
        withStore $ \store -> do
          enqueueJob store seed >>= (@?= Right ())
          (_, firstToken) <- claimFixture store now
          deferJob store firstToken later >>= (@?= Right SettlementApplied)
          pollJobs store kind now pollLimit >>= (@?= Right [])
          pollJobs store kind later pollLimit >>= (@?= Right [identifier])
          (_, secondToken) <- claimFixture store later
          acknowledgeJob store secondToken >>= (@?= Right SettlementApplied)
    , testCase "takes a dead-lettered job out of the queue" $
        withStore $ \store -> do
          enqueueJob store seed >>= (@?= Right ())
          (_, token) <- claimFixture store now
          deadLetterJob store token Rejected "rejected by dependency"
            >>= (@?= Right SettlementApplied)
          pollJobs store kind later pollLimit >>= (@?= Right [])
    , testCase "retires the claim that a later claim superseded" $
        withStore $ \store -> do
          enqueueJob store seed >>= (@?= Right ())
          (stale, staleToken) <- claimFixture store now
          (_, currentToken) <- claimFixture store later
          renewJob store stale.reference (JobInstant 120_000)
            >>= (@?= Left (InvalidState "claim handle is invalid"))
          acknowledgeJob store staleToken
            >>= (@?= Left (InvalidState "claim handle is invalid"))
          acknowledgeJob store currentToken >>= (@?= Right SettlementApplied)
    , testCase "streams the runnable jobs one poll answers with" $
        withStore $ \store -> do
          enqueueJob store seed >>= (@?= Right ())
          streamed <-
            runExceptT
              ( runConduit
                  (streamRunnableJobs store kind now pollLimit .| sinkList)
              )
          streamed @?= Right [identifier]
    ]


withStore :: (Store -> IO ()) -> IO ()
withStore action = do
  opened <- openStore options
  case opened of
    Left engineError ->
      assertFailure ("failed to open the shared engine: " <> show engineError)
    Right store ->
      action store `finally` do
        closed <- closeStore store
        case closed of
          Left engineError ->
            assertFailure
              ("failed to close the shared engine: " <> show engineError)
          Right () -> pure ()


-- | Wins the fixture job's claim and hands both halves of it back.
--
-- The claim is consumed inside the call, so what a test keeps is the metadata
-- the engine reported and the one token that settles it.
claimFixture
  :: Store
  -> JobInstant
  -> IO (JobClaimDetails, JobSettlementToken)
claimFixture store claimedAt = do
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
    outcome -> assertFailure ("expected a won claim, got " <> show outcome)


releaseClaim
  :: JobClaimDetails
  -> ClaimedJob
  %1 -> Ur (JobClaimDetails, JobSettlementToken)
releaseClaim details won =
  case settlementToken won of
    Ur token -> Ur (details, token)


options :: OpenOptions
options = OpenOptions "sqlite::memory:" 5000 1 1


aggregateType :: AggregateType
aggregateType = AggregateType "haskell-account"


aggregateId :: AggregateId
aggregateId = AggregateId "account-1"


stream :: StreamIdentity
stream = StreamIdentity aggregateType aggregateId


proposed :: ProposedEvent
proposed = ProposedEvent (EventType "Opened") (EventVersion "1.0") payload


identifier :: JobId
identifier = case mkJobId "01ARZ3NDEKTSV4RRFFQ69G5FAV" of
  Just fixture -> fixture
  Nothing -> error "the fixture job identifier was rejected"


kind :: JobKind
kind = JobKind "haskell-test"


worker :: WorkerId
worker = WorkerId "haskell-worker"


seed :: JobSeed
seed = JobSeed identifier kind payload now


-- | The instant every fixture job is enqueued to run at.
now :: JobInstant
now = JobInstant 1_000


-- | An instant past the lease every claim in these tests takes.
later :: JobInstant
later = JobInstant 90_000


pollLimit :: PollLimit
pollLimit = PollLimit 10


payload :: ByteString
payload = ByteString.pack [0, 1, 255]
