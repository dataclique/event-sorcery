module Event.Sorcery.Job.Worker (
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

import Event.Sorcery.Job (Job (JobError, JobOutput), JobId, JobInstant)
import Event.Sorcery.Job.Execution (DurableJob)
import Event.Sorcery.Job.Worker.Definition (
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
import Event.Sorcery.Job.Worker.Internal (
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
