-- | Explicit test-only constructors for otherwise sealed dispatch verdicts.
module Event.Sorcery.Dispatch.TestSupport (
  confirmedOutcome,
  failedOutcome,
) where

import Data.Word (Word32)
import Event.Sorcery.Dispatch.Internal (
  DispatchFailure,
  DispatchOutcome,
 )
import Event.Sorcery.Dispatch.Internal qualified as Internal
import Event.Sorcery.Job.Definition (
  Job (JobError, JobOutput),
  JobId,
 )


-- | Fabricates a successful verdict for domain tests.
confirmedOutcome
  :: JobId
  -> JobOutput job
  -> Word32
  -> DispatchOutcome job
confirmedOutcome = Internal.confirmedOutcome


-- | Fabricates a failed verdict for domain tests.
failedOutcome
  :: JobId
  -> DispatchFailure (JobError job)
  -> Word32
  -> DispatchOutcome job
failedOutcome = Internal.failedOutcome
