module EventSorcery.Dispatch.TestSupport (
  confirmedOutcome,
  failedOutcome,
) where

import Data.Word (Word32)
import EventSorcery.Dispatch.Internal (
  DispatchFailure,
  DispatchOutcome,
 )
import EventSorcery.Dispatch.Internal qualified as Internal
import EventSorcery.Job.Definition (
  Job (JobError, JobOutput),
  JobId,
 )


confirmedOutcome
  :: JobId
  -> JobOutput job
  -> Word32
  -> DispatchOutcome job
confirmedOutcome = Internal.confirmedOutcome


failedOutcome
  :: JobId
  -> DispatchFailure (JobError job)
  -> Word32
  -> DispatchOutcome job
failedOutcome = Internal.failedOutcome
