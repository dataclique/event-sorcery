module EventSorcery.Store (
  Store,
  StoreError (..),
  executeCommand,
  loadEntity,
  mkStore,
  snapshotEntity,
) where

import Control.Monad (foldM)
import Data.Bifunctor (first)
import Data.Foldable (foldl')
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.List.NonEmpty qualified as NonEmpty
import Data.Unrestricted.Linear (Ur (Ur))
import Data.Word (Word64)
import EventSorcery.Aggregate (Effect (..), EventSourced, EventVersion (..))
import EventSorcery.Aggregate qualified as Aggregate
import EventSorcery.Engine (EngineError)
import EventSorcery.Engine qualified as Engine
import EventSorcery.Job (
  Job (encodeJob),
  JobId,
  JobInstant (JobInstant),
  JobKind (JobKind),
  JobSeed (JobSeed),
  commitWithJob,
  jobType,
 )
import EventSorcery.Snapshot (StoredSnapshot (StoredSnapshot))
import EventSorcery.Snapshot qualified as Snapshot
import EventSorcery.Stream (
  ProposedEvent (ProposedEvent),
  ReplayError,
  StoredEvent (StoredEvent),
  StreamIdentity,
  StreamKey,
  StreamVersion (StreamVersion),
  commit,
  loadStream,
  replay,
  resume,
  streamKeyIdentity,
 )
import Prelude (
  Either (..),
  Eq,
  IO,
  Maybe (..),
  Show,
  fmap,
  fst,
  pure,
  (<$),
 )


data Store entity = Store Engine.Store (IO JobId)


data StoreError entity
  = StoreEngineFailed EngineError
  | StoreReplayFailed (ReplayError entity)
  | StoreCommandRejected (Aggregate.CommandError entity)
  | StoreDecisionRejected (Aggregate.ApplyError entity)
  | StoreSnapshotDecodeFailed Word64 Aggregate.DecodeCause


deriving stock instance
  ( Eq (Aggregate.ApplyError entity)
  , Eq (Aggregate.CommandError entity)
  )
  => Eq (StoreError entity)


deriving stock instance
  ( Show (Aggregate.ApplyError entity)
  , Show (Aggregate.CommandError entity)
  )
  => Show (StoreError entity)


data PreparedCommand entity where
  PreparedCommand
    :: Ur (CommitPlan entity)
    %1 -> PreparedCommand entity


data CommitPlan entity
  = PreparedEvents entity Word64 (NonEmpty ProposedEvent)
  | PreparedDispatch entity Word64 (NonEmpty ProposedEvent) JobSeed


mkStore :: Engine.Store -> IO JobId -> Store entity
mkStore = Store


snapshotEntity
  :: forall entity
   . EventSourced entity
  => Store entity
  -> StreamKey entity
  -> IO (Either (StoreError entity) (Maybe entity))
snapshotEntity store@(Store engine _) key = do
  loaded <- loadCurrent store key

  case loaded of
    Left failure -> pure (Left failure)
    Right (Nothing, _) -> pure (Right Nothing)
    Right (Just entity, sequence) -> do
      stored <-
        Snapshot.storeSnapshot
          engine
          ( Snapshot.snapshotWrite
              (streamKeyIdentity key)
              sequence
              (Aggregate.encodeSnapshot entity)
          )

      pure case stored of
        Left failure -> Left (StoreEngineFailed failure)
        Right _ -> Right (Just entity)


loadEntity
  :: forall entity
   . EventSourced entity
  => Store entity
  -> StreamKey entity
  -> IO (Either (StoreError entity) (Maybe entity))
loadEntity store key = fmap (fmap fst) (loadCurrent store key)


executeCommand
  :: forall entity
   . EventSourced entity
  => Store entity
  -> StreamKey entity
  -> Aggregate.Command entity
  -> IO (Either (StoreError entity) entity)
executeCommand store@(Store engine _) key command = do
  loaded <- loadCurrent store key

  case loaded of
    Left failure -> pure (Left failure)
    Right (current, expected) ->
      case decide current of
        Left failure -> pure (Left (StoreCommandRejected failure))
        Right effect -> do
          prepared <- prepareEffect store expected current effect

          case prepared of
            Left failure -> pure (Left failure)
            Right preparedCommand ->
              commitPrepared
                engine
                (streamKeyIdentity key)
                preparedCommand
  where
    decide Nothing = Aggregate.initialize command
    decide (Just entity) = Aggregate.transition entity command


loadCurrent
  :: EventSourced entity
  => Store entity
  -> StreamKey entity
  -> IO (Either (StoreError entity) (Maybe entity, Word64))
loadCurrent (Store engine _) key = do
  loadedSnapshot <- Snapshot.loadSnapshot engine (streamKeyIdentity key)

  case loadedSnapshot of
    Left failure -> pure (Left (StoreEngineFailed failure))
    Right Nothing -> replayFullStream engine key
    Right (Just snapshot) -> resumeSnapshot engine key snapshot


replayFullStream
  :: EventSourced entity
  => Engine.Store
  -> StreamKey entity
  -> IO (Either (StoreError entity) (Maybe entity, Word64))
replayFullStream engine key = do
  loaded <- loadStream engine (streamKeyIdentity key) Nothing

  pure do
    events <- first StoreEngineFailed loaded
    entity <- first StoreReplayFailed (replay key events)

    pure (entity, latestSequence events)


resumeSnapshot
  :: EventSourced entity
  => Engine.Store
  -> StreamKey entity
  -> StoredSnapshot
  -> IO (Either (StoreError entity) (Maybe entity, Word64))
resumeSnapshot engine key (StoredSnapshot sequence _ payload) =
  case Aggregate.decodeSnapshot payload of
    Left failure ->
      pure (Left (StoreSnapshotDecodeFailed sequence failure))
    Right entity -> do
      loaded <- loadStream engine (streamKeyIdentity key) (Just sequence)

      pure do
        events <- first StoreEngineFailed loaded
        resumed <-
          first
            StoreReplayFailed
            (resume key (StreamVersion sequence) entity events)

        pure (Just resumed, latestSequenceAfter sequence events)


prepareEffect
  :: EventSourced entity
  => Store entity
  -> Word64
  -> Maybe entity
  -> Effect entity
  -> IO (Either (StoreError entity) (PreparedCommand entity))
prepareEffect _ expected current (Events events) =
  pure do
    next <- first StoreDecisionRejected (applyEvents current events)

    pure
      ( PreparedCommand
          (Ur (PreparedEvents next expected (encodeEvents events)))
      )
prepareEffect (Store _ nextJobId) expected current (Dispatch job) = do
  identifier <- nextJobId
  let intent = Aggregate.injectDispatchIntent (Aggregate.dispatchIntent identifier job)

  pure do
    next <-
      first
        StoreDecisionRejected
        (applyEvents current (intent :| []))
    let seed =
          JobSeed
            identifier
            (jobKind job)
            (encodeJob job)
            (JobInstant 0)

    pure
      ( PreparedCommand
          ( Ur
              ( PreparedDispatch
                  next
                  expected
                  (encodeEvents (intent :| []))
                  seed
              )
          )
      )


commitPrepared
  :: Engine.Store
  -> StreamIdentity
  -> PreparedCommand entity
  %1 -> IO (Either (StoreError entity) entity)
commitPrepared engine identity (PreparedCommand (Ur plan)) =
  case plan of
    PreparedEvents next expected events -> do
      committed <- commit engine identity expected events

      pure (next <$ first StoreEngineFailed committed)
    PreparedDispatch next expected events seed -> do
      committed <- commitWithJob engine identity expected events seed

      pure (next <$ first StoreEngineFailed committed)


applyEvents
  :: EventSourced entity
  => Maybe entity
  -> NonEmpty (Aggregate.Event entity)
  -> Either (Aggregate.ApplyError entity) entity
applyEvents Nothing (firstEvent :| remaining) = do
  initial <- Aggregate.originate firstEvent
  foldM Aggregate.evolve initial remaining
applyEvents (Just entity) events =
  foldM Aggregate.evolve entity events


encodeEvents
  :: EventSourced entity
  => NonEmpty (Aggregate.Event entity)
  -> NonEmpty ProposedEvent
encodeEvents = NonEmpty.map encodeEvent


encodeEvent
  :: EventSourced entity
  => Aggregate.Event entity
  -> ProposedEvent
encodeEvent event =
  let EventVersion version = Aggregate.eventVersion event
   in ProposedEvent
        (Aggregate.eventType event)
        version
        (Aggregate.encodeEvent event)


jobKind :: forall job. Job job => job -> JobKind
jobKind _ = JobKind (jobType @job)


latestSequence :: [StoredEvent] -> Word64
latestSequence = latestSequenceAfter 0


latestSequenceAfter :: Word64 -> [StoredEvent] -> Word64
latestSequenceAfter = foldl' useSequence
  where
    useSequence _ (StoredEvent sequence _ _ _) = sequence
