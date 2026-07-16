module Main (main) where

import Control.DeepSeq (NFData (rnf))
import Criterion.Main (
  Benchmark,
  bench,
  bgroup,
  defaultMain,
  envWithCleanup,
  whnfIO,
 )
import Data.ByteString qualified as ByteString
import Data.IORef (IORef, atomicModifyIORef', newIORef)
import Data.List (foldl')
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.Maybe (Maybe (..))
import Data.Text qualified as Text
import Data.Unrestricted.Linear (Ur)
import Data.Word (Word64)
import Event.Sorcery.Engine (
  EngineError,
  OpenOptions (OpenOptions),
  Store,
  closeStore,
  openStore,
 )
import Event.Sorcery.Job (
  ClaimBudget (ClaimBudget),
  ClaimedJob,
  JobClaimDetails,
  JobClaimResult (JobClaimed),
  JobId,
  JobInstant (JobInstant),
  JobKind (JobKind),
  JobSeed (JobSeed),
  JobSettlement (SettlementApplied),
  JobSettlementToken,
  LeaseDuration (LeaseDuration),
  WorkerId (WorkerId),
  acknowledgeJob,
  claimJob,
  enqueueJob,
  mkJobId,
  settlementToken,
 )
import Event.Sorcery.Stream (
  ProposedEvent (ProposedEvent),
  StoredEvent (StoredEvent),
  StreamIdentity (StreamIdentity),
  commit,
  loadStream,
 )
import Prelude (
  Either (..),
  IO,
  Int,
  String,
  error,
  fromIntegral,
  pure,
  replicate,
  seq,
  show,
  ($),
  (+),
  (<>),
 )


data BenchmarkEnvironment = BenchmarkEnvironment
  { store :: Store
  , nextStream :: IORef Int
  , nextJob :: IORef Int
  }


instance NFData BenchmarkEnvironment where
  rnf (BenchmarkEnvironment store nextStream nextJob) =
    store `seq` nextStream `seq` nextJob `seq` ()


main :: IO ()
main =
  defaultMain
    [ envWithCleanup
        openBenchmarkEnvironment
        closeBenchmarkEnvironment
        benchmarks
    ]


benchmarks :: BenchmarkEnvironment -> Benchmark
benchmarks environment =
  bgroup
    "engine-backed Haskell binding"
    [ bench
        "load and force 1,000 stored events"
        (whnfIO (loadAndForceEvents environment))
    , bench
        "commit one event to a fresh stream"
        (whnfIO (commitFreshStream environment))
    , bench
        "enqueue, claim, and acknowledge one job"
        (whnfIO (runFreshJob environment))
    ]


openBenchmarkEnvironment :: IO BenchmarkEnvironment
openBenchmarkEnvironment = do
  opened <- openStore (OpenOptions "sqlite::memory:" 5_000 1 1)

  store <- expectRight "failed to open benchmark store" opened
  preloadReplayStream store
  nextStream <- newIORef 0
  nextJob <- newIORef 0

  pure BenchmarkEnvironment {store, nextStream, nextJob}


closeBenchmarkEnvironment :: BenchmarkEnvironment -> IO ()
closeBenchmarkEnvironment environment = do
  closed <- closeStore environment.store
  expectRight "failed to close benchmark store" closed


preloadReplayStream :: Store -> IO ()
preloadReplayStream store = do
  committed <-
    commit
      store
      replayStream
      0
      (benchmarkEvent :| replicate 999 benchmarkEvent)
  expectRight "failed to preload replay benchmark" committed


loadAndForceEvents :: BenchmarkEnvironment -> IO Word64
loadAndForceEvents environment = do
  loaded <- loadStream environment.store replayStream Nothing
  events <- expectRight "failed to load replay benchmark stream" loaded

  pure (foldl' forceStoredEvent 0 events)


forceStoredEvent :: Word64 -> StoredEvent -> Word64
forceStoredEvent
  total
  (StoredEvent sequence eventType eventVersion payload) =
    total
      + sequence
      + fromIntegral (Text.length eventType)
      + fromIntegral (Text.length eventVersion)
      + fromIntegral (ByteString.length payload)


commitFreshStream :: BenchmarkEnvironment -> IO Int
commitFreshStream environment = do
  identifier <- nextIdentifier environment.nextStream
  let stream =
        StreamIdentity
          "benchmark-commit"
          ("stream-" <> Text.pack (show identifier))

  committed <- commit environment.store stream 0 (benchmarkEvent :| [])
  expectRight "failed to commit benchmark event" committed

  pure identifier


runFreshJob :: BenchmarkEnvironment -> IO Int
runFreshJob environment = do
  identifier <- nextIdentifier environment.nextJob
  jobId <-
    expectJust
      "generated an invalid benchmark job identifier"
      (mkJobId (Text.justifyRight 26 '0' (Text.pack (show identifier))))
  enqueued <-
    enqueueJob
      environment.store
      (JobSeed jobId benchmarkJobKind benchmarkPayload benchmarkNow)
  expectRight "failed to enqueue benchmark job" enqueued

  token <- claimBenchmarkJob environment.store jobId
  settled <- acknowledgeJob environment.store token

  case settled of
    Right SettlementApplied -> pure identifier
    _ -> error "failed to acknowledge benchmark job"


claimBenchmarkJob :: Store -> JobId -> IO JobSettlementToken
claimBenchmarkJob store jobId = do
  claimed <-
    claimJob
      store
      jobId
      benchmarkWorker
      benchmarkNow
      (LeaseDuration 30_000)
      (ClaimBudget 50)
      releaseClaim

  case claimed of
    Right (JobClaimed token) -> pure token
    _ -> error "failed to claim benchmark job"


releaseClaim
  :: JobClaimDetails
  -> ClaimedJob
  %1 -> Ur JobSettlementToken
releaseClaim _ = settlementToken


nextIdentifier :: IORef Int -> IO Int
nextIdentifier counter =
  atomicModifyIORef' counter $ \current ->
    let next = current + 1
     in (next, next)


expectRight :: String -> Either EngineError value -> IO value
expectRight _ (Right value) = pure value
expectRight message (Left _) = error message


expectJust :: String -> Maybe value -> IO value
expectJust _ (Just value) = pure value
expectJust message Nothing = error message


replayStream :: StreamIdentity
replayStream = StreamIdentity "benchmark-replay" "fixed-stream"


benchmarkEvent :: ProposedEvent
benchmarkEvent =
  ProposedEvent
    "balance-adjusted"
    "1"
    (ByteString.replicate 256 42)


benchmarkJobKind :: JobKind
benchmarkJobKind = JobKind "benchmark-job"


benchmarkWorker :: WorkerId
benchmarkWorker = WorkerId "benchmark-worker"


benchmarkNow :: JobInstant
benchmarkNow = JobInstant 1_000


benchmarkPayload :: ByteString.ByteString
benchmarkPayload = ByteString.replicate 256 42
