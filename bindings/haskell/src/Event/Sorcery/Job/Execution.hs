module Event.Sorcery.Job.Execution (
  DurableJob (..),
  JobAttempt (..),
  JobContext (..),
  JobFailure (..),
  JobOutcome (..),
  Reconciliation (..),
  executeDurableJob,
) where

import Data.Kind (Type)
import Data.Text (Text)
import Data.Word (Word32)
import Event.Sorcery.Job (
  Job (JobError, JobOutput),
  JobExecutionRoute (ReconcileExecution, SubmitExecution),
  JobId,
  JobInstant,
 )
import Prelude (Either (Left, Right), Eq, IO, Show, pure)


newtype JobAttempt = JobAttempt Word32
  deriving stock (Eq, Show)


data JobContext = JobContext
  { jobId :: JobId
  , attempt :: JobAttempt
  }
  deriving stock (Eq, Show)


data JobOutcome output
  = JobDone output
  | JobDeferred JobInstant
  deriving stock (Eq, Show)


data JobFailure failure
  = TransientFailure failure
  | TerminalFailure failure
  deriving stock (Eq, Show)


data Reconciliation output
  = Reconciled output
  | NotSubmitted
  | Indeterminate JobInstant
  deriving stock (Eq, Show)


class Job job => DurableJob job where
  type JobInput job :: Type


  renderJobError :: job -> JobError job -> Text


  submit
    :: JobContext
    -> JobInput job
    -> job
    -> IO (Either (JobFailure (JobError job)) (JobOutcome (JobOutput job)))


  reconcile
    :: JobContext
    -> JobInput job
    -> job
    -> IO (Either (JobFailure (JobError job)) (Reconciliation (JobOutput job)))


executeDurableJob
  :: DurableJob job
  => JobContext
  -> JobExecutionRoute
  -> JobInput job
  -> job
  -> IO (Either (JobFailure (JobError job)) (JobOutcome (JobOutput job)))
executeDurableJob context route input job = case route of
  SubmitExecution -> submit context input job
  ReconcileExecution -> do
    result <- reconcile context input job

    case result of
      Left failure -> pure (Left failure)
      Right (Reconciled output) -> pure (Right (JobDone output))
      Right NotSubmitted -> submit context input job
      Right (Indeterminate runAt) -> pure (Right (JobDeferred runAt))
