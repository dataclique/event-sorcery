module Main (main) where

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
import Prelude (
  Bool (False, True),
  Either (Left, Right),
  IO,
  Maybe (Just, Nothing),
  String,
  error,
  pure,
  (&&),
  (<>),
  (==),
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


main :: IO ()
main = do
  firstExecutionRunsSubmit
  laterExecutionRunsReconcile
  missingSubmissionAuthorizesSubmit
  indeterminateReconciliationDefers
  submitFailureKeepsItsClassification
  reconcileFailureKeepsItsClassification


firstExecutionRunsSubmit :: IO ()
firstExecutionRunsSubmit = do
  calls <- newIORef []
  outcome <- executeDurableJob context SubmitExecution calls SubmitImmediately
  recorded <- readIORef calls

  expect
    "first execution did not route exclusively to submit"
    (outcome == Right (JobDone "submitted") && recorded == ["submit"])


laterExecutionRunsReconcile :: IO ()
laterExecutionRunsReconcile = do
  calls <- newIORef []
  outcome <- executeDurableJob context ReconcileExecution calls ReconcileAsSettled
  recorded <- readIORef calls

  expect
    "later execution did not accept the reconciled result"
    (outcome == Right (JobDone "reconciled") && recorded == ["reconcile"])


missingSubmissionAuthorizesSubmit :: IO ()
missingSubmissionAuthorizesSubmit = do
  calls <- newIORef []
  outcome <- executeDurableJob context ReconcileExecution calls ReconcileAsMissing
  recorded <- readIORef calls

  expect
    "a proven missing submission did not reconcile before resubmitting"
    ( outcome == Right (JobDone "submitted")
        && recorded == ["reconcile", "submit"]
    )


indeterminateReconciliationDefers :: IO ()
indeterminateReconciliationDefers = do
  calls <- newIORef []
  outcome <- executeDurableJob context ReconcileExecution calls ReconcileLater
  recorded <- readIORef calls

  expect
    "an indeterminate reconciliation did not defer without resubmitting"
    (outcome == Right (JobDeferred later) && recorded == ["reconcile"])


submitFailureKeepsItsClassification :: IO ()
submitFailureKeepsItsClassification = do
  calls <- newIORef []
  outcome <-
    executeDurableJob context SubmitExecution calls SubmitTransientlyFails
  recorded <- readIORef calls

  expect
    "submit failure lost its retry classification"
    ( outcome == Left (TransientFailure "unavailable")
        && recorded == ["submit"]
    )


reconcileFailureKeepsItsClassification :: IO ()
reconcileFailureKeepsItsClassification = do
  calls <- newIORef []
  outcome <-
    executeDurableJob context ReconcileExecution calls ReconcileTerminallyFails
  recorded <- readIORef calls

  expect
    "reconciliation failure lost its terminal classification"
    ( outcome == Left (TerminalFailure "rejected")
        && recorded == ["reconcile"]
    )


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


expect :: String -> Bool -> IO ()
expect _ True = pure ()
expect message False = error message
