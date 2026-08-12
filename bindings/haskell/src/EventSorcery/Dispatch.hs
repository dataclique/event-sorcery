module EventSorcery.Dispatch (
  DispatchEvent (..),
  DispatchFailure (..),
  DispatchOutcome,
  DispatchRefused (..),
  DispatchReplay (..),
  DispatchedJob (..),
  JobDispatch,
  Settled,
  SettledFailure,
  dispatchFailure,
  dispatchJob,
  evolveDispatch,
  guardDispatch,
  kickoff,
  originateDispatch,
  settleDispatch,
  settledAttempts,
  settledFailureAttempts,
  settledFailureJobId,
  settledJobId,
  settledOutput,
) where

import Data.Word (Word32)
import EventSorcery.Dispatch.Internal (
  DispatchEvent (..),
  DispatchFailure (..),
  DispatchOutcome,
  DispatchRefused (..),
  DispatchReplay (..),
  DispatchedJob (..),
  JobDispatch (JobDispatch),
  Settled (Settled),
  SettledFailure (SettledFailure),
  evolveDispatch,
  guardDispatch,
  kickoff,
  originateDispatch,
  settleDispatch,
 )
import EventSorcery.Job.Definition (JobId)


dispatchJob :: JobDispatch job -> job
dispatchJob (JobDispatch job) = job


settledJobId :: Settled output -> JobId
settledJobId (Settled identifier _ _) = identifier


settledOutput :: Settled output -> output
settledOutput (Settled _ output _) = output


settledAttempts :: Settled output -> Word32
settledAttempts (Settled _ _ attempts) = attempts


settledFailureJobId :: SettledFailure failure -> JobId
settledFailureJobId (SettledFailure identifier _ _) = identifier


dispatchFailure :: SettledFailure failure -> failure
dispatchFailure (SettledFailure _ failure _) = failure


settledFailureAttempts :: SettledFailure failure -> Word32
settledFailureAttempts (SettledFailure _ _ attempts) = attempts
