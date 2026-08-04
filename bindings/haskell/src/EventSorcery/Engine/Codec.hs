module EventSorcery.Engine.Codec (
  decodeEngineError,
  decodeStoredEvents,
  encodeCommit,
  encodeCurrentVersion,
  encodeLoadStream,
  encodeOpenOptions,
) where

import Codec.CBOR.Decoding (
  Decoder,
  decodeBytes,
  decodeListLen,
  decodeNull,
  decodeString,
  decodeWord,
  decodeWord32,
  decodeWord64,
 )
import Codec.CBOR.Encoding (
  Encoding,
  encodeBytes,
  encodeListLen,
  encodeNull,
  encodeString,
  encodeWord,
  encodeWord32,
  encodeWord64,
 )
import Codec.CBOR.Read (deserialiseFromBytes)
import Codec.CBOR.Write (toStrictByteString)
import Control.Monad (replicateM)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as LazyByteString
import Data.Foldable (foldMap)
import Data.Maybe (maybe)
import Data.Word (Word32, Word64)
import EventSorcery.Engine.Protocol (
  AggregateId (..),
  AggregateType (..),
  ConflictDetail (..),
  EngineError (..),
  EventType (..),
  EventVersion (..),
  OpenOptions (..),
  ProposedEvent (..),
  ResourceLimitDetail (..),
  StoredEvent (..),
  StreamIdentity (..),
 )
import Prelude (
  Either (..),
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
  (<$>),
  (<>),
  (==),
 )


encodeOpenOptions :: OpenOptions -> ByteString
encodeOpenOptions options =
  toStrictByteString $
    encodeListLen 5
      <> encodeWord 1
      <> encodeString options.path
      <> encodeWord64 options.busyTimeoutMilliseconds
      <> encodeWord32 options.poolSize
      <> encodeWord32 options.runtimeThreads


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


decodeEngineError :: Word32 -> ByteString -> Either String EngineError
decodeEngineError expectedCode bytes =
  case deserialiseFromBytes
    (decodeEngineErrorWire expectedCode)
    (LazyByteString.fromStrict bytes) of
    Left failure -> Left (show failure)
    Right (remaining, engineError)
      | LazyByteString.null remaining -> Right engineError
      | otherwise -> Left "trailing bytes after engine error"


decodeEngineErrorWire :: Word32 -> Decoder s EngineError
decodeEngineErrorWire expectedCode = do
  expectListLength 3
  version <- decodeWord
  if version == 1
    then do
      encodedCode <- decodeWord32
      if encodedCode == expectedCode
        then decodeEngineErrorDetail encodedCode
        else fail "engine status does not match encoded error code"
    else fail "unsupported engine-error format version"


decodeEngineErrorDetail :: Word32 -> Decoder s EngineError
decodeEngineErrorDetail 1 = do
  _ <- decodeString
  pure MalformedInput
decodeEngineErrorDetail 2 = do
  expectListLength 4
  aggregateType <- AggregateType <$> decodeString
  aggregateId <- AggregateId <$> decodeString
  expectedVersion <- decodeWord64
  actualVersion <- decodeWord64
  pure
    ( OptimisticConflict
        (ConflictDetail {aggregateType, aggregateId, expectedVersion, actualVersion})
    )
decodeEngineErrorDetail 4 = StorageFailure <$> decodeString
decodeEngineErrorDetail 5 = InvalidState <$> decodeString
decodeEngineErrorDetail 6 = do
  expectListLength 3
  resource <- decodeString
  observed <- decodeWord64
  limit <- decodeWord64
  pure (ResourceLimitExceeded (ResourceLimitDetail {resource, observed, limit}))
decodeEngineErrorDetail 100 = do
  decodeNull
  pure EnginePanic
decodeEngineErrorDetail value =
  fail ("unsupported engine error code " <> show value)


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
