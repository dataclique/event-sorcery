-- | The stream feature: stream identity, the events it carries, and its paging.
--
-- Everything a consumer needs to read and append to a stream is here, down to
-- the wire codecs the engine and this binding agree on. The engine handle and
-- the errors every call can answer with belong to "EventSorcery.Engine" and
-- are re-exported so a stream handler needs this one import.
module EventSorcery.Stream (
  AbiVersionDetail (..),
  AggregateId (..),
  AggregateType (..),
  BindingFault (..),
  ConflictDetail (..),
  EngineError (..),
  EngineErrorDecodeDetail (..),
  EventType (..),
  EventVersion (..),
  PageAdvanceDetail (..),
  ProposedEvent (..),
  ResourceLimitDetail (..),
  Store,
  StoredEvent (..),
  StreamIdentity (..),
  commit,
  currentVersion,
  decodeStoredEvents,
  encodeCommit,
  encodeCurrentVersion,
  encodeLoadStream,
  loadStream,
  loadStreamPage,
  nextCursor,
) where

import Codec.CBOR.Decoding (
  Decoder,
  decodeBytes,
  decodeListLen,
  decodeString,
  decodeWord,
  decodeWord64,
 )
import Codec.CBOR.Encoding (
  Encoding,
  encodeBytes,
  encodeListLen,
  encodeNull,
  encodeString,
  encodeWord,
  encodeWord64,
 )
import Codec.CBOR.Read (deserialiseFromBytes)
import Codec.CBOR.Write (toStrictByteString)
import Control.Monad (replicateM)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as LazyByteString
import Data.Foldable (foldMap)
import Data.List.NonEmpty (NonEmpty)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Maybe (maybe)
import Data.Text (Text)
import Data.Word (Word64)
import EventSorcery.Engine.Internal (
  AbiVersionDetail (..),
  AggregateId (..),
  AggregateType (..),
  BindingFault (..),
  ConflictDetail (..),
  EngineError (..),
  EngineErrorDecodeDetail (..),
  PageAdvanceDetail (..),
  ResourceLimitDetail (..),
  Store,
  callWithOutput,
  callWithoutOutput,
  decodeResponse,
  expectListLength,
  withInputBuffer,
  withOpenStore,
 )
import EventSorcery.Engine.Internal.FFI (
  esCommit,
  esCurrentVersion,
  esLoadStream,
 )
import Foreign.Marshal.Alloc (alloca)
import Foreign.Storable (peek, poke)
import Prelude (
  Either (..),
  Eq ((==)),
  IO,
  Maybe (Just, Nothing),
  Show (show),
  String,
  fail,
  fromIntegral,
  id,
  length,
  otherwise,
  pure,
  ($),
  (.),
  (<$>),
  (<>),
  (>),
  (>>=),
 )


newtype EventType = EventType Text
  deriving newtype (Eq, Show)


newtype EventVersion = EventVersion Text
  deriving newtype (Eq, Show)


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


-- | Reads every event after the cursor, walking as many pages as it takes.
--
-- The engine bounds one page by an event count and by a byte budget, so a
-- short page is not by itself the end of the stream; the stream ends at the
-- first empty page. Callers that want to handle one page at a time -- to keep
-- a long stream out of memory, say -- use 'loadStreamPage' instead.
loadStream
  :: Store
  -> StreamIdentity
  -> Maybe Word64
  -> IO (Either EngineError [StoredEvent])
loadStream store stream = walkPages store stream id


-- | Reads a single engine page of a stream, starting after the given sequence.
loadStreamPage
  :: Store
  -> StreamIdentity
  -> Maybe Word64
  -> IO (Either EngineError [StoredEvent])
loadStreamPage store stream after =
  withOpenStore store $ \handle ->
    withInputBuffer (encodeLoadStream stream after) $ \request -> do
      response <- callWithOutput (esLoadStream handle request)
      pure (response >>= decodeResponse decodeStoredEvents)


currentVersion :: Store -> StreamIdentity -> IO (Either EngineError Word64)
currentVersion store stream =
  withOpenStore store $ \handle ->
    withInputBuffer (encodeCurrentVersion stream) $ \request ->
      alloca $ \outVersion -> do
        poke outVersion 0
        result <- callWithoutOutput (esCurrentVersion handle request outVersion)
        case result of
          Left engineError -> pure (Left engineError)
          Right () -> Right <$> peek outVersion


commit
  :: Store
  -> StreamIdentity
  -> Word64
  -> NonEmpty ProposedEvent
  -> IO (Either EngineError ())
commit store stream expected events =
  withOpenStore store $ \handle ->
    withInputBuffer
      (encodeCommit stream expected (NonEmpty.toList events))
      (callWithoutOutput . esCommit handle)


-- | Reports where the next page starts, refusing a page that cannot advance.
--
-- A page whose last sequence does not move past the cursor would make the walk
-- request the same page forever, so it is reported as a protocol failure.
nextCursor :: Maybe Word64 -> NonEmpty StoredEvent -> Either EngineError Word64
nextCursor after page =
  case after of
    Nothing -> Right advanced
    Just cursor
      | advanced > cursor -> Right advanced
      | otherwise ->
          Left
            ( BindingProtocolError
                (PageDidNotAdvance (PageAdvanceDetail cursor advanced))
            )
  where
    lastEvent = NonEmpty.last page
    advanced = lastEvent.sequence


encodeLoadStream :: StreamIdentity -> Maybe Word64 -> ByteString
encodeLoadStream stream after =
  toStrictByteString $
    encodeListLen 4
      <> encodeWord 1
      <> encodeAggregateType stream.aggregateType
      <> encodeAggregateId stream.aggregateId
      <> maybe encodeNull encodeWord64 after


encodeCurrentVersion :: StreamIdentity -> ByteString
encodeCurrentVersion stream =
  toStrictByteString $
    encodeListLen 3
      <> encodeWord 1
      <> encodeAggregateType stream.aggregateType
      <> encodeAggregateId stream.aggregateId


encodeCommit :: StreamIdentity -> Word64 -> [ProposedEvent] -> ByteString
encodeCommit stream expected events =
  toStrictByteString $
    encodeListLen 5
      <> encodeWord 1
      <> encodeAggregateType stream.aggregateType
      <> encodeAggregateId stream.aggregateId
      <> encodeWord64 expected
      <> encodeListLen (fromIntegral (length events))
      <> foldMap encodeProposedEvent events


decodeStoredEvents :: ByteString -> Either String [StoredEvent]
decodeStoredEvents bytes =
  case deserialiseFromBytes decodeStoredEventsWire (LazyByteString.fromStrict bytes) of
    Left failure -> Left (show failure)
    Right (remaining, events)
      | LazyByteString.null remaining -> Right events
      | otherwise -> Left "trailing bytes after stored events"


walkPages
  :: Store
  -> StreamIdentity
  -> ([StoredEvent] -> [StoredEvent])
  -> Maybe Word64
  -> IO (Either EngineError [StoredEvent])
walkPages store stream loaded after = do
  page <- loadStreamPage store stream after
  case page of
    Left engineError -> pure (Left engineError)
    Right events -> case NonEmpty.nonEmpty events of
      Nothing -> pure (Right (loaded []))
      Just eventPage -> case nextCursor after eventPage of
        Left engineError -> pure (Left engineError)
        Right cursor -> walkPages store stream (loaded . (events <>)) (Just cursor)


encodeProposedEvent :: ProposedEvent -> Encoding
encodeProposedEvent event =
  encodeListLen 3
    <> encodeEventType event.eventType
    <> encodeEventVersion event.eventVersion
    <> encodeBytes event.payload


encodeAggregateType :: AggregateType -> Encoding
encodeAggregateType (AggregateType value) = encodeString value


encodeAggregateId :: AggregateId -> Encoding
encodeAggregateId (AggregateId value) = encodeString value


encodeEventType :: EventType -> Encoding
encodeEventType (EventType value) = encodeString value


encodeEventVersion :: EventVersion -> Encoding
encodeEventVersion (EventVersion value) = encodeString value


decodeStoredEventsWire :: Decoder s [StoredEvent]
decodeStoredEventsWire = do
  expectListLength 2
  version <- decodeWord
  if version == 1
    then do
      count <- decodeListLen
      replicateM count decodeStoredEvent
    else fail "unsupported stored-events format version"


decodeStoredEvent :: Decoder s StoredEvent
decodeStoredEvent = do
  expectListLength 4
  sequence <- decodeWord64
  eventType <- EventType <$> decodeString
  eventVersion <- EventVersion <$> decodeString
  payload <- decodeBytes
  pure StoredEvent {sequence, eventType, eventVersion, payload}
