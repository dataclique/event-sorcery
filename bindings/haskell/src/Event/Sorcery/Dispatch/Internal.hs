-- | State transitions for entity-scoped durable dispatches.
module Event.Sorcery.Dispatch.Internal (
  DispatchEvent (..),
  DispatchFailure (..),
  DispatchOutcome (..),
  DispatchRefused (..),
  DispatchReplay (..),
  DispatchedJob (..),
  JobDispatch (..),
  Settled (..),
  SettledFailure (..),
  confirmedOutcome,
  evolveDispatch,
  failedOutcome,
  guardDispatch,
  kickoff,
  originateDispatch,
  settleDispatch,
) where

import Data.Text (Text)
import Data.Word (Word32)
import Event.Sorcery.Job.Definition (
  DeadReason,
  Job (JobError, JobOutput),
  JobId,
 )
import Prelude (Either (..), Eq, Show, (==))


-- | Successful job output correlated with its dispatch and attempt count.
data Settled output = Settled JobId output Word32
  deriving stock (Eq, Show)


-- | Terminal job failure correlated with its dispatch and attempt count.
data SettledFailure failure = SettledFailure JobId failure Word32
  deriving stock (Eq, Show)


-- | A domain rejection or an engine dead-letter verdict.
data DispatchFailure failure
  = Rejected failure
  | DeadLettered DeadReason Text
  deriving stock (Eq, Show)


-- | Lifecycle embedded in an origin entity for one dispatch slot.
data DispatchedJob job
  = Idle
  | InFlight JobId
  | Confirmed (Settled (JobOutput job))
  | Failed (SettledFailure (DispatchFailure (JobError job)))


deriving stock instance
  (Eq (JobOutput job), Eq (JobError job)) => Eq (DispatchedJob job)


deriving stock instance
  (Show (JobOutput job), Show (JobError job)) => Show (DispatchedJob job)


-- | Events that evolve an entity's dispatch lifecycle.
data DispatchEvent job
  = Dispatched JobId job
  | ConfirmedEvent (Settled (JobOutput job))
  | FailedEvent (SettledFailure (DispatchFailure (JobError job)))


deriving stock instance
  (Eq job, Eq (JobOutput job), Eq (JobError job)) => Eq (DispatchEvent job)


deriving stock instance
  (Show job, Show (JobOutput job), Show (JobError job))
  => Show (DispatchEvent job)


-- | A command effect requesting exactly one durable job dispatch.
newtype JobDispatch job = JobDispatch job


-- | Sealed worker verdict delivered back to an origin entity.
data DispatchOutcome job
  = ConfirmedOutcome (Settled (JobOutput job))
  | FailedOutcome (SettledFailure (DispatchFailure (JobError job)))


-- | Reasons a fresh dispatch or delivered outcome is invalid.
data DispatchRefused
  = DispatchInFlight
  | DispatchAlreadyConfirmed
  | DispatchOutcomeMismatch
  deriving stock (Eq, Show)


-- | A dispatch event cannot follow the current lifecycle state.
data DispatchReplay = DispatchReplay
  deriving stock (Eq, Show)


-- | Creates a dispatch request for a command effect.
kickoff :: job -> JobDispatch job
kickoff = JobDispatch


-- | Refuses overlapping or already-confirmed dispatches.
guardDispatch
  :: DispatchedJob job
  -> job
  -> Either DispatchRefused (JobDispatch job)
guardDispatch state job = case state of
  Idle -> Right (JobDispatch job)
  Failed _ -> Right (JobDispatch job)
  InFlight _ -> Left DispatchInFlight
  Confirmed _ -> Left DispatchAlreadyConfirmed


-- | Correlates a sealed verdict and absorbs matching redelivery.
settleDispatch
  :: DispatchedJob job
  -> DispatchOutcome job
  -> Either DispatchRefused [DispatchEvent job]
settleDispatch state outcome = case (state, outcome) of
  (InFlight identifier, ConfirmedOutcome settled@(Settled jobId _ _))
    | identifier == jobId -> Right [ConfirmedEvent settled]
  (InFlight identifier, FailedOutcome settled@(SettledFailure jobId _ _))
    | identifier == jobId -> Right [FailedEvent settled]
  (Confirmed (Settled identifier _ _), ConfirmedOutcome (Settled jobId _ _))
    | identifier == jobId -> Right []
  ( Failed (SettledFailure identifier _ _)
    , FailedOutcome (SettledFailure jobId _ _)
    )
      | identifier == jobId -> Right []
  _ -> Left DispatchOutcomeMismatch


-- | Starts a dispatch lifecycle from its first event.
originateDispatch
  :: DispatchEvent job
  -> Either DispatchReplay (DispatchedJob job)
originateDispatch = evolveDispatch Idle


-- | Applies one dispatch event to an existing lifecycle.
evolveDispatch
  :: DispatchedJob job
  -> DispatchEvent job
  -> Either DispatchReplay (DispatchedJob job)
evolveDispatch state event = case (state, event) of
  (Idle, Dispatched identifier _) -> Right (InFlight identifier)
  (Failed _, Dispatched identifier _) -> Right (InFlight identifier)
  (InFlight identifier, ConfirmedEvent settled@(Settled jobId _ _))
    | identifier == jobId -> Right (Confirmed settled)
  (InFlight identifier, FailedEvent settled@(SettledFailure jobId _ _))
    | identifier == jobId -> Right (Failed settled)
  _ -> Left DispatchReplay


-- | Constructs a sealed successful worker verdict.
confirmedOutcome
  :: JobId
  -> JobOutput job
  -> Word32
  -> DispatchOutcome job
confirmedOutcome identifier output attempts =
  ConfirmedOutcome (Settled identifier output attempts)


-- | Constructs a sealed terminal worker verdict.
failedOutcome
  :: JobId
  -> DispatchFailure (JobError job)
  -> Word32
  -> DispatchOutcome job
failedOutcome identifier failure attempts =
  FailedOutcome (SettledFailure identifier failure attempts)
