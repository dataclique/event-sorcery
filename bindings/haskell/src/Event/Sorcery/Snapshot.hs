-- | Linear snapshot writes and engine-backed snapshot persistence.
module Event.Sorcery.Snapshot (
  SnapshotVersion (..),
  SnapshotWrite,
  StoredSnapshot (..),
  discardSnapshot,
  loadSnapshot,
  snapshotWrite,
  storeSnapshot,
) where

import Codec.CBOR.Decoding (
  Decoder,
  TokenType (TypeNull),
  decodeBytes,
  decodeListLen,
  decodeNull,
  decodeWord,
  decodeWord64,
  peekTokenType,
 )
import Codec.CBOR.Encoding (
  encodeBytes,
  encodeListLen,
  encodeString,
  encodeWord,
  encodeWord64,
 )
import Codec.CBOR.Read (deserialiseFromBytes)
import Codec.CBOR.Write (toStrictByteString)
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as LazyByteString
import Data.Text qualified as Text
import Data.Unrestricted.Linear (Ur (Ur))
import Data.Word (Word64)
import Event.Sorcery.Engine (EngineError (BindingProtocolError))
import Event.Sorcery.Engine qualified as Engine
import Event.Sorcery.Engine.Internal (
  callWithOutput,
  callWithoutOutput,
  withInputBuffer,
  withOpenStore,
 )
import Event.Sorcery.Engine.Internal.FFI (
  esSnapshotDiscard,
  esSnapshotLoad,
  esSnapshotStore,
 )
import Event.Sorcery.Stream (StreamIdentity (StreamIdentity))
import Foreign.Marshal.Alloc (alloca)
import Foreign.Storable (peek, poke)
import Prelude (
  Either (..),
  Eq,
  IO,
  Int,
  Maybe (..),
  Show,
  String,
  fail,
  otherwise,
  pure,
  show,
  ($),
  (.),
  (<$),
  (<$>),
  (<>),
  (==),
  (>>=),
 )


-- | Monotonic version returned after a snapshot write.
newtype SnapshotVersion = SnapshotVersion Word64
  deriving stock (Eq, Show)


-- | Stored stream sequence, snapshot version, and opaque entity payload.
data StoredSnapshot = StoredSnapshot Word64 SnapshotVersion ByteString
  deriving stock (Eq, Show)


-- | One-shot snapshot write request.
data SnapshotWrite where
  SnapshotWrite
    :: Ur (StreamIdentity, Word64, ByteString)
    %1 -> SnapshotWrite


-- | Prepares a snapshot request that must be consumed exactly once.
snapshotWrite :: StreamIdentity -> Word64 -> ByteString -> SnapshotWrite
snapshotWrite identity sequence payload =
  SnapshotWrite (Ur (identity, sequence, payload))


-- | Loads the latest snapshot for a stream when one exists.
loadSnapshot
  :: Engine.Store
  -> StreamIdentity
  -> IO (Either EngineError (Maybe StoredSnapshot))
loadSnapshot store identity =
  withOpenStore store $ \handle ->
    withInputBuffer (encodeIdentity identity) $ \request -> do
      response <- callWithOutput (esSnapshotLoad handle request)
      pure (response >>= decodeResponse decodeStoredSnapshot)


-- | Consumes and atomically persists a prepared snapshot request.
storeSnapshot
  :: Engine.Store
  -> SnapshotWrite
  %1 -> IO (Either EngineError SnapshotVersion)
storeSnapshot store (SnapshotWrite (Ur (identity, sequence, payload))) =
  withOpenStore store $ \handle ->
    withInputBuffer (encodeWrite identity sequence payload) $ \request ->
      alloca $ \outVersion -> do
        poke outVersion 0
        stored <- callWithoutOutput (esSnapshotStore handle request outVersion)
        case stored of
          Left failure -> pure (Left failure)
          Right () -> Right . SnapshotVersion <$> peek outVersion


-- | Discards the current snapshot while retaining the event stream.
discardSnapshot
  :: Engine.Store
  -> StreamIdentity
  -> IO (Either EngineError ())
discardSnapshot store identity =
  withOpenStore store $ \handle ->
    withInputBuffer
      (encodeIdentity identity)
      (callWithoutOutput . esSnapshotDiscard handle)


encodeIdentity :: StreamIdentity -> ByteString
encodeIdentity (StreamIdentity aggregateType aggregateId) =
  toStrictByteString $
    encodeListLen 3
      <> encodeWord 1
      <> encodeString aggregateType
      <> encodeString aggregateId


encodeWrite :: StreamIdentity -> Word64 -> ByteString -> ByteString
encodeWrite (StreamIdentity aggregateType aggregateId) sequence payload =
  toStrictByteString $
    encodeListLen 5
      <> encodeWord 1
      <> encodeString aggregateType
      <> encodeString aggregateId
      <> encodeWord64 sequence
      <> encodeBytes payload


decodeStoredSnapshot :: ByteString -> Either String (Maybe StoredSnapshot)
decodeStoredSnapshot bytes =
  case deserialiseFromBytes
    decodeStoredSnapshotWire
    (LazyByteString.fromStrict bytes) of
    Left failure -> Left (show failure)
    Right (remaining, snapshot)
      | LazyByteString.null remaining -> Right snapshot
      | otherwise -> Left "trailing bytes after stored snapshot"


decodeStoredSnapshotWire :: Decoder s (Maybe StoredSnapshot)
decodeStoredSnapshotWire = do
  expectListLength 2
  expectFormatVersion
  token <- peekTokenType

  case token of
    TypeNull -> Nothing <$ decodeNull
    _ -> do
      expectListLength 3
      sequence <- decodeWord64
      version <- SnapshotVersion <$> decodeWord64
      Just . StoredSnapshot sequence version <$> decodeBytes


expectFormatVersion :: Decoder s ()
expectFormatVersion = do
  version <- decodeWord

  if version == 1
    then pure ()
    else fail "unsupported snapshot format version"


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
