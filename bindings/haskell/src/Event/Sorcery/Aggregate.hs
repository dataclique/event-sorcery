{-# LANGUAGE FunctionalDependencies #-}

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


newtype EventVersion = EventVersion Text
  deriving stock (Eq, Ord, Show)


newtype SchemaVersion = SchemaVersion Word16
  deriving stock (Eq, Ord, Show)


data CompactionPolicy
  = Retain
  | CompactAfterSnapshot
  deriving stock (Eq, Show)


newtype DecodeCause = DecodeCause Text
  deriving stock (Eq, Show)


data DispatchIntent job = DispatchIntent JobId job


class Dispatches entity job | job -> entity where
  injectDispatchIntent :: DispatchIntent job -> Event entity
  injectDispatchOutcome :: DispatchOutcome job -> Command entity


type Member item items = Elem item items ~ 'True


type family Elem (item :: Type) (items :: [Type]) :: Bool where
  Elem item '[] = 'False
  Elem item (item ': items) = 'True
  Elem item (other ': items) = Elem item items


data Effect entity where
  Events :: NonEmpty (Event entity) -> Effect entity
  Unchanged :: Effect entity
  Dispatch
    :: (Job job, Member job (Jobs entity), Dispatches entity job)
    => JobDispatch job
    -> Effect entity


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


dispatchIntent :: JobId -> job -> DispatchIntent job
dispatchIntent = DispatchIntent


dispatchJobId :: DispatchIntent job -> JobId
dispatchJobId (DispatchIntent identifier _) = identifier


dispatchIntentJob :: DispatchIntent job -> job
dispatchIntentJob (DispatchIntent _ job) = job
