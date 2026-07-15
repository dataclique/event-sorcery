module EventSorcery.Job.Worker (
  AttemptLimit,
  JobRunError (..),
  JobRunResult (..),
  JobWorker,
  RenewalSchedule,
  jobWorker,
  mkAttemptLimit,
  renewalSchedule,
  renewingJobWorker,
  runJobOnce,
) where

import EventSorcery.Job (Job (JobError, JobOutput), JobId, JobInstant)
import EventSorcery.Job.Execution (DurableJob)
import EventSorcery.Job.Worker.Definition (
  AttemptLimit,
  JobRunError (..),
  JobRunResult (..),
  JobWorker,
  RenewalSchedule,
  jobWorker,
  mkAttemptLimit,
  renewalSchedule,
  renewingJobWorker,
 )
import EventSorcery.Job.Worker.Internal (
  genericSettlement,
  runJobOnceWith,
 )
import Prelude (Either, IO)


runJobOnce
  :: forall job
   . DurableJob job
  => JobWorker job
  -> JobId
  -> JobInstant
  -> IO (Either JobRunError (JobRunResult (JobOutput job) (JobError job)))
runJobOnce = runJobOnceWith genericSettlement
