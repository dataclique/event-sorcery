module EventSorcery.Engine.Protocol (
  AggregateId (..),
  AggregateType (..),
  AbiVersionDetail (..),
  ConflictDetail (..),
  EngineError (..),
  EventType (..),
  EventVersion (..),
  OpenOptions (..),
  ProposedEvent (..),
  ResourceLimitDetail (..),
  StoredEvent (..),
  StreamIdentity (..),
) where

import Data.ByteString (ByteString)
import Data.Text (Text)
import Data.Word (Word32, Word64)
import Prelude (Eq, Show)


newtype AggregateType = AggregateType Text
  deriving newtype (Eq, Show)


newtype AggregateId = AggregateId Text
  deriving newtype (Eq, Show)


newtype EventType = EventType Text
  deriving newtype (Eq, Show)


newtype EventVersion = EventVersion Text
  deriving newtype (Eq, Show)


data OpenOptions = OpenOptions
  { path :: Text
  , busyTimeoutMilliseconds :: Word64
  , poolSize :: Word32
  , runtimeThreads :: Word32
  }
  deriving stock (Eq, Show)


data ConflictDetail = ConflictDetail
  { aggregateType :: AggregateType
  , aggregateId :: AggregateId
  , expectedVersion :: Word64
  , actualVersion :: Word64
  }
  deriving stock (Eq, Show)


data ResourceLimitDetail = ResourceLimitDetail
  { resource :: Text
  , observed :: Word64
  , limit :: Word64
  }
  deriving stock (Eq, Show)


data AbiVersionDetail = AbiVersionDetail
  { expectedMajor :: Word32
  , minimumMinor :: Word32
  , actualMajor :: Word32
  , actualMinor :: Word32
  }
  deriving stock (Eq, Show)


data EngineError
  = MalformedInput
  | OptimisticConflict ConflictDetail
  | StorageFailure Text
  | InvalidState Text
  | ResourceLimitExceeded ResourceLimitDetail
  | AbiVersionMismatch AbiVersionDetail
  | EnginePanic
  | UnknownEngineError Word32
  | BindingProtocolError Text
  deriving stock (Eq, Show)


data StreamIdentity = StreamIdentity
  { aggregateType :: AggregateType
  , aggregateId :: AggregateId
  }
  deriving stock (Eq, Show)


data ProposedEvent = ProposedEvent
  { eventType :: EventType
  , eventVersion :: EventVersion
  , payload :: ByteString
  }
  deriving stock (Eq, Show)


data StoredEvent = StoredEvent
  { sequence :: Word64
  , eventType :: EventType
  , eventVersion :: EventVersion
  , payload :: ByteString
  }
  deriving stock (Eq, Show)
