module EventSorcery.Engine.Codec (
  decodeStoredEvents,
  encodeCommit,
  encodeLoadStream,
  encodeOpenOptions,
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
import Data.Word (Word64)
import EventSorcery.Engine.Protocol (
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
      <> encodeString stream.aggregateType
      <> encodeString stream.aggregateId
      <> maybe encodeNull encodeWord64 after


encodeCommit :: StreamIdentity -> Word64 -> [ProposedEvent] -> ByteString
encodeCommit stream expected events =
  toStrictByteString $
    encodeListLen 5
      <> encodeWord 1
      <> encodeString stream.aggregateType
      <> encodeString stream.aggregateId
      <> encodeWord64 expected
      <> encodeListLen (fromIntegral (length events))
      <> foldMap encodeProposedEvent events


encodeProposedEvent :: ProposedEvent -> Encoding
encodeProposedEvent event =
  encodeListLen 3
    <> encodeString event.eventType
    <> encodeString event.eventVersion
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
  eventType <- decodeString
  eventVersion <- decodeString
  payload <- decodeBytes
  pure StoredEvent {sequence, eventType, eventVersion, payload}


expectListLength :: Int -> Decoder s ()
expectListLength expected = do
  actual <- decodeListLen
  if actual == expected
    then pure ()
    else fail "unexpected CBOR list length"
