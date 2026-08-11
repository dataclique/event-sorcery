module EventSorcery.Engine.Protocol (
  AggregateId (..),
  AggregateType (..),
  AbiVersionDetail (..),
  BindingFault (..),
  ConflictDetail (..),
  EngineError (..),
  EngineErrorDecodeDetail (..),
  EventType (..),
  EventVersion (..),
  OpenOptions (..),
  PageAdvanceDetail (..),
  ProposedEvent (..),
  ResourceLimitDetail (..),
  StoredEvent (..),
  StreamIdentity (..),
) where

import Data.ByteString (ByteString)
import Data.Text (Text)
import Data.Word (Word32, Word64)
import Foreign.C.Types (CSize)
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
  -- ^ A SQLite connection URL, not a filesystem path.
  --
  -- The engine hands this to sqlx's SqliteConnectOptions URL parser: a
  -- leading "sqlite:" scheme is stripped, everything after the first
  -- question mark is read as query parameters and an unrecognised key
  -- fails the open, and the remainder is percent-decoded. A filesystem
  -- path holding a question mark, hash or percent sign therefore names a
  -- different database than it reads like. The literal "sqlite::memory:"
  -- opens a private in-memory database.
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


data PageAdvanceDetail = PageAdvanceDetail
  { cursor :: Word64
  , lastSequence :: Word64
  }
  deriving stock (Eq, Show)


data EngineErrorDecodeDetail = EngineErrorDecodeDetail
  { status :: Word32
  , cause :: Text
  }
  deriving stock (Eq, Show)


-- | A failure this binding detects rather than one the engine reported.
--
-- The engine's own errors arrive as a numeric class plus a decoded detail.
-- These are raised on the Haskell side of the ABI, so each keeps the values
-- that identify it instead of collapsing into prose a consumer could only
-- substring-match. The two remaining text payloads are cborg's own failure
-- description, which has no structure to preserve.
data BindingFault
  = NullOutputBufferWithLength CSize
  | OutputExceedsPlatformSize CSize
  | PageDidNotAdvance PageAdvanceDetail
  | UndecodableEngineError EngineErrorDecodeDetail
  | UndecodableResponse Text
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
  | BindingProtocolError BindingFault
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
