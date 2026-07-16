-- | Entity-scoped durable dispatch state and sealed verdict accessors.
module Event.Sorcery.Dispatch (
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
import Event.Sorcery.Dispatch.Internal (
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
import Event.Sorcery.Job.Definition (JobId)


-- | Returns the job requested by a command effect.
dispatchJob :: JobDispatch job -> job
dispatchJob (JobDispatch job) = job


-- | Returns the dispatch identifier from a successful verdict.
settledJobId :: Settled output -> JobId
settledJobId (Settled identifier _ _) = identifier


-- | Returns the output from a successful verdict.
settledOutput :: Settled output -> output
settledOutput (Settled _ output _) = output


-- | Returns the attempt count recorded by a successful verdict.
settledAttempts :: Settled output -> Word32
settledAttempts (Settled _ _ attempts) = attempts


-- | Returns the dispatch identifier from a failed verdict.
settledFailureJobId :: SettledFailure failure -> JobId
settledFailureJobId (SettledFailure identifier _ _) = identifier


-- | Returns the reason carried by a failed verdict.
dispatchFailure :: SettledFailure failure -> failure
dispatchFailure (SettledFailure _ failure _) = failure


-- | Returns the attempt count recorded by a failed verdict.
settledFailureAttempts :: SettledFailure failure -> Word32
settledFailureAttempts (SettledFailure _ _ attempts) = attempts
