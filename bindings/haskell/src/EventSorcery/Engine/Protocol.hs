module EventSorcery.Engine.Protocol (
  OpenOptions (..),
  ProposedEvent (..),
  StoredEvent (..),
  StreamIdentity (..),
) where

import Data.ByteString (ByteString)
import Data.Text (Text)
import Data.Word (Word32, Word64)
import Prelude (Eq, Show)


data OpenOptions = OpenOptions
  { path :: Text
  , busyTimeoutMilliseconds :: Word64
  , poolSize :: Word32
  , runtimeThreads :: Word32
  }
  deriving stock (Eq, Show)


data StreamIdentity = StreamIdentity
  { aggregateType :: Text
  , aggregateId :: Text
  }
  deriving stock (Eq, Show)


data ProposedEvent = ProposedEvent
  { eventType :: Text
  , eventVersion :: Text
  , payload :: ByteString
  }
  deriving stock (Eq, Show)


data StoredEvent = StoredEvent
  { sequence :: Word64
  , eventType :: Text
  , eventVersion :: Text
  , payload :: ByteString
  }
  deriving stock (Eq, Show)
