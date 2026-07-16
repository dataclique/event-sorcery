{-# LANGUAGE FunctionalDependencies #-}

-- | Typed aggregate definitions and pure command effects.
module Event.Sorcery.Aggregate (
  CompactionPolicy (..),
  DecodeCause (..),
  DispatchIntent,
  Dispatches (..),
  Effect (..),
  EventSourced (..),
  EventVersion (..),
  Member,
  SchemaVersion (..),
  dispatchIntent,
  dispatchIntentJob,
  dispatchJobId,
) where

import Data.ByteString (ByteString)
import Data.Kind (Type)
import Data.List.NonEmpty (NonEmpty)
import Data.Proxy (Proxy)
import Data.Text (Text)
import Data.Type.Equality (type (~))
import Data.Word (Word16)
import Event.Sorcery.Dispatch (DispatchOutcome, JobDispatch)
import Event.Sorcery.Job.Definition (
  Job (..),
  JobId,
 )
import Prelude (Bool (..), Either, Eq, Ord, Show)


-- | Stable version declared by an event codec.
newtype EventVersion = EventVersion Text
  deriving stock (Eq, Ord, Show)


-- | Version used to reconcile derived aggregate state.
newtype SchemaVersion = SchemaVersion Word16
  deriving stock (Eq, Ord, Show)


-- | Whether snapshots may replace the retained event history.
data CompactionPolicy
  = Retain
  | CompactAfterSnapshot
  deriving stock (Eq, Show)


-- | Domain-event or snapshot decoding failure.
newtype DecodeCause = DecodeCause Text
  deriving stock (Eq, Show)


-- | A job paired with the identifier minted for its durable dispatch.
data DispatchIntent job = DispatchIntent JobId job


-- | Injects a declared job's intent and sealed outcome into its origin entity.
class Dispatches entity job | job -> entity where
  injectDispatchIntent :: DispatchIntent job -> Event entity
  injectDispatchOutcome :: DispatchOutcome job -> Command entity


-- | Compile-time evidence that @item@ belongs to @items@.
type Member item items = Elem item items ~ 'True


type family Elem (item :: Type) (items :: [Type]) :: Bool where
  Elem item '[] = 'False
  Elem item (item ': items) = 'True
  Elem item (other ': items) = Elem item items


-- | The complete result of a pure command decision.
data Effect entity where
  Events :: NonEmpty (Event entity) -> Effect entity
  Unchanged :: Effect entity
  Dispatch
    :: (Job job, Member job (Jobs entity), Dispatches entity job)
    => JobDispatch job
    -> Effect entity


-- | Domain contract for folding events and deciding commands.
class EventSourced entity where
  type EntityId entity = (identifier :: Type) | identifier -> entity
  type Command entity :: Type
  type Event entity = (event :: Type) | event -> entity
  type CommandError entity :: Type
  type ApplyError entity :: Type
  type Jobs entity :: [Type]


  aggregateType :: Proxy entity -> Text
  encodeEntityId :: EntityId entity -> Text
  eventType :: Event entity -> Text
  eventVersion :: Event entity -> EventVersion
  schemaVersion :: Proxy entity -> SchemaVersion
  compactionPolicy :: Proxy entity -> CompactionPolicy
  compactionPolicy _ = Retain
  encodeEvent :: Event entity -> ByteString
  decodeEvent :: ByteString -> Either DecodeCause (Event entity)
  encodeSnapshot :: entity -> ByteString
  decodeSnapshot :: ByteString -> Either DecodeCause entity
  originate :: Event entity -> Either (ApplyError entity) entity
  evolve :: entity -> Event entity -> Either (ApplyError entity) entity
  initialize :: Command entity -> Either (CommandError entity) (Effect entity)
  transition
    :: entity
    -> Command entity
    -> Either (CommandError entity) (Effect entity)


-- | Associates a freshly minted identifier with a job value.
dispatchIntent :: JobId -> job -> DispatchIntent job
dispatchIntent = DispatchIntent


-- | Returns the durable identifier carried by a dispatch intent.
dispatchJobId :: DispatchIntent job -> JobId
dispatchJobId (DispatchIntent identifier _) = identifier


-- | Returns the job carried by a dispatch intent.
dispatchIntentJob :: DispatchIntent job -> job
dispatchIntentJob (DispatchIntent _ job) = job
