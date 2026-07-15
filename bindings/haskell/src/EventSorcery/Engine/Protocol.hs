module EventSorcery.Engine.Protocol (
  AggregateId (..),
  AggregateType (..),
  EngineError (..),
  ErrorClass (..),
  EventType (..),
  EventVersion (..),
  OpenOptions (..),
  ProposedEvent (..),
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


data ErrorClass
  = DecodeError
  | ConflictError
  | JobError
  | StorageError
  | StateError
  | AbiMismatch
  | PanicError
  | UnknownError Word32
  deriving stock (Eq, Show)


data EngineError
  = EngineError ErrorClass Text
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
