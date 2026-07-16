module EventSorcery.Stream (
  AggregateId (..),
  AggregateType (..),
  EventType (..),
  EventVersion (..),
  ProposedEvent (..),
  StoredEvent (..),
  StreamIdentity (..),
  commit,
  currentVersion,
  decodeStoredEvents,
  encodeCommit,
  encodeCurrentVersion,
  encodeLoadStream,
  encodeProposedEvent,
  loadStream,
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
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as LazyByteString
import Data.Foldable (foldMap)
import Data.List.NonEmpty (NonEmpty)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Maybe (maybe)
import Data.Text qualified as Text
import Data.Word (Word64)
import EventSorcery.Engine.Internal (
  Store,
  callWithOutput,
  callWithoutOutput,
  withInputBuffer,
  withOpenStore,
 )
import EventSorcery.Engine.Internal.FFI (
  esCommit,
  esCurrentVersion,
  esLoadStream,
 )
import EventSorcery.Engine.Protocol (
  AggregateId (..),
  AggregateType (..),
  EngineError (BindingProtocolError),
  EventType (..),
  EventVersion (..),
  ProposedEvent (..),
  StoredEvent (..),
  StreamIdentity (..),
 )
import Foreign.Marshal.Alloc (alloca)
import Foreign.Storable (peek, poke)
import Prelude (
  Either (..),
  IO,
  Int,
  Maybe,
  String,
  fail,
  fromIntegral,
  length,
  otherwise,
  pure,
  show,
  ($),
  (.),
  (<$>),
  (<>),
  (==),
  (>>=),
 )


loadStream
  :: Store
  -> StreamIdentity
  -> Maybe Word64
  -> IO (Either EngineError [StoredEvent])
loadStream store stream after =
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


encodeProposedEvent :: ProposedEvent -> Encoding
encodeProposedEvent event =
  encodeListLen 3
    <> encodeEventType event.eventType
    <> encodeEventVersion event.eventVersion
    <> encodeBytes event.payload


decodeStoredEvents :: ByteString -> Either String [StoredEvent]
decodeStoredEvents bytes =
  case deserialiseFromBytes decodeStoredEventsWire (LazyByteString.fromStrict bytes) of
    Left failure -> Left (show failure)
    Right (remaining, events)
      | LazyByteString.null remaining -> Right events
      | otherwise -> Left "trailing bytes after stored events"


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


encodeAggregateType :: AggregateType -> Encoding
encodeAggregateType (AggregateType value) = encodeString value


encodeAggregateId :: AggregateId -> Encoding
encodeAggregateId (AggregateId value) = encodeString value


encodeEventType :: EventType -> Encoding
encodeEventType (EventType value) = encodeString value


encodeEventVersion :: EventVersion -> Encoding
encodeEventVersion (EventVersion value) = encodeString value


expectListLength :: Int -> Decoder s ()
expectListLength expected = do
  actual <- decodeListLen
  if actual == expected
    then pure ()
    else fail "unexpected CBOR list length"


decodeResponse
  :: (ByteString -> Either String value)
  -> ByteString
  -> Either EngineError value
decodeResponse decoder =
  first (BindingProtocolError . Text.pack) . decoder
