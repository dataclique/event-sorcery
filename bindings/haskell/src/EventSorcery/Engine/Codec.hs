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
  EngineError (..),
  ErrorClass (..),
  EventType (..),
  EventVersion (..),
  OpenOptions (..),
  ProposedEvent (..),
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


decodeEngineError :: ByteString -> Either String EngineError
decodeEngineError bytes =
  case deserialiseFromBytes decodeEngineErrorWire (LazyByteString.fromStrict bytes) of
    Left failure -> Left (show failure)
    Right (remaining, engineError)
      | LazyByteString.null remaining -> Right engineError
      | otherwise -> Left "trailing bytes after engine error"


decodeEngineErrorWire :: Decoder s EngineError
decodeEngineErrorWire = do
  expectListLength 3
  version <- decodeWord
  if version == 1
    then do
      errorClass <- decodeErrorClass <$> decodeWord32
      EngineError errorClass <$> decodeString
    else fail "unsupported engine-error format version"


decodeErrorClass :: Word32 -> ErrorClass
decodeErrorClass 1 = DecodeError
decodeErrorClass 2 = ConflictError
decodeErrorClass 3 = JobError
decodeErrorClass 4 = StorageError
decodeErrorClass 5 = StateError
decodeErrorClass 6 = AbiMismatch
decodeErrorClass 100 = PanicError
decodeErrorClass value = UnknownError value


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
