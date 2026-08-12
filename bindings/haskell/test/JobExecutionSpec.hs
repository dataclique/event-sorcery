module JobExecutionSpec (spec) where

import Data.ByteString qualified as ByteString
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Data.Text (Text)
import Event.Sorcery.Job (
  Job (..),
  JobExecutionRoute (ReconcileExecution, SubmitExecution),
  JobId,
  JobInstant (JobInstant),
  mkJobId,
 )
import Event.Sorcery.Job.Execution (
  DurableJob (..),
  JobAttempt (JobAttempt),
  JobContext (JobContext),
  JobFailure (TerminalFailure, TransientFailure),
  JobOutcome (JobDeferred, JobDone),
  Reconciliation (Indeterminate, NotSubmitted, Reconciled),
  executeDurableJob,
 )
import Test.Hspec (Spec, describe, it, shouldBe)
import Prelude (
  Either (Left, Right),
  IO,
  Maybe (Just, Nothing),
  error,
  pure,
  ($),
  (<>),
 )


data ProbeJob
  = SubmitImmediately
  | ReconcileAsSettled
  | ReconcileAsMissing
  | ReconcileLater
  | SubmitTransientlyFails
  | ReconcileTerminallyFails


instance Job ProbeJob where
  type JobType ProbeJob = "probe"
  type JobOutput ProbeJob = Text
  type JobError ProbeJob = Text


  encodeJob _ = ByteString.empty
  decodeJob _ = Right SubmitImmediately


instance DurableJob ProbeJob where
  type JobInput ProbeJob = IORef [Text]


  renderJobError _ failure = failure


  submit _ calls job = do
    modifyIORef' calls (<> ["submit"])

    pure case job of
      SubmitTransientlyFails -> Left (TransientFailure "unavailable")
      _ -> Right (JobDone "submitted")


  reconcile _ calls job = do
    modifyIORef' calls (<> ["reconcile"])

    pure case job of
      ReconcileAsSettled -> Right (Reconciled "reconciled")
      ReconcileAsMissing -> Right NotSubmitted
      ReconcileLater -> Right (Indeterminate later)
      ReconcileTerminallyFails -> Left (TerminalFailure "rejected")
      _ -> Right NotSubmitted


spec :: Spec
spec = describe "durable job execution" $ do
  it "routes a first execution to submit" firstExecutionRunsSubmit
  it "accepts a reconciled later execution" laterExecutionRunsReconcile
  it
    "resubmits once reconciliation proves the job missing"
    missingSubmissionAuthorizesSubmit
  it
    "defers an indeterminate reconciliation without resubmitting"
    indeterminateReconciliationDefers
  it
    "keeps the retry classification a submit failure carries"
    submitFailureKeepsItsClassification
  it
    "keeps the terminal classification a reconciliation carries"
    reconcileFailureKeepsItsClassification


firstExecutionRunsSubmit :: IO ()
firstExecutionRunsSubmit = do
  calls <- newIORef []
  outcome <- executeDurableJob context SubmitExecution calls SubmitImmediately
  recorded <- readIORef calls

  outcome `shouldBe` Right (JobDone "submitted")
  recorded `shouldBe` ["submit"]


laterExecutionRunsReconcile :: IO ()
laterExecutionRunsReconcile = do
  calls <- newIORef []
  outcome <-
    executeDurableJob context ReconcileExecution calls ReconcileAsSettled
  recorded <- readIORef calls

  outcome `shouldBe` Right (JobDone "reconciled")
  recorded `shouldBe` ["reconcile"]


missingSubmissionAuthorizesSubmit :: IO ()
missingSubmissionAuthorizesSubmit = do
  calls <- newIORef []
  outcome <-
    executeDurableJob context ReconcileExecution calls ReconcileAsMissing
  recorded <- readIORef calls

  outcome `shouldBe` Right (JobDone "submitted")
  recorded `shouldBe` ["reconcile", "submit"]


indeterminateReconciliationDefers :: IO ()
indeterminateReconciliationDefers = do
  calls <- newIORef []
  outcome <- executeDurableJob context ReconcileExecution calls ReconcileLater
  recorded <- readIORef calls

  outcome `shouldBe` Right (JobDeferred later)
  recorded `shouldBe` ["reconcile"]


submitFailureKeepsItsClassification :: IO ()
submitFailureKeepsItsClassification = do
  calls <- newIORef []
  outcome <-
    executeDurableJob context SubmitExecution calls SubmitTransientlyFails
  recorded <- readIORef calls

  outcome `shouldBe` Left (TransientFailure "unavailable")
  recorded `shouldBe` ["submit"]


reconcileFailureKeepsItsClassification :: IO ()
reconcileFailureKeepsItsClassification = do
  calls <- newIORef []
  outcome <-
    executeDurableJob context ReconcileExecution calls ReconcileTerminallyFails
  recorded <- readIORef calls

  outcome `shouldBe` Left (TerminalFailure "rejected")
  recorded `shouldBe` ["reconcile"]


context :: JobContext
context =
  JobContext
    (validJobId "01ARZ3NDEKTSV4RRFFQ69G5FAZ")
    (JobAttempt 0)


later :: JobInstant
later = JobInstant 60_000


validJobId :: Text -> JobId
validJobId value = case mkJobId value of
  Just identifier -> identifier
  Nothing -> error "valid test job identifier was rejected"
