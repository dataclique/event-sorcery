module Event.Sorcery.Stream (
  ActualSequence (..),
  ExpectedSequence (..),
  MetadataMismatch (..),
  ProposedEvent (..),
  ReplayError (..),
  StoredEvent (..),
  StreamIdentity (..),
  StreamKey,
  StreamPosition (..),
  StreamVersion (..),
  commit,
  currentVersion,
  decodeStoredEvents,
  encodeCommit,
  encodeCurrentVersion,
  encodeLoadStream,
  encodeProposedEvent,
  loadStream,
  replay,
  resume,
  streamKey,
  streamKeyIdentity,
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
import Control.Monad (foldM, replicateM)
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as LazyByteString
import Data.Foldable (foldMap)
import Data.List.NonEmpty (NonEmpty)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Maybe (fromMaybe, maybe)
import Data.Proxy (Proxy (Proxy))
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Word (Word64)
import Event.Sorcery.Aggregate (
  DecodeCause,
  EventSourced,
  EventVersion (..),
 )
import Event.Sorcery.Aggregate qualified as Aggregate
import Event.Sorcery.Engine.Internal (
  EngineError (BindingProtocolError),
  Store,
  callWithOutput,
  callWithoutOutput,
  withInputBuffer,
  withOpenStore,
 )
import Event.Sorcery.Engine.Internal.FFI (
  esCommit,
  esCurrentVersion,
  esLoadStream,
 )
import Foreign.Marshal.Alloc (alloca)
import Foreign.Storable (peek, poke)
import Prelude (
  Either (..),
  Eq,
  IO,
  Int,
  Maybe (..),
  Ord,
  Show,
  String,
  fail,
  fromIntegral,
  fst,
  length,
  maxBound,
  otherwise,
  pure,
  show,
  ($),
  (+),
  (.),
  (/=),
  (<$>),
  (<>),
  (==),
  (>>=),
 )


data StreamIdentity = StreamIdentity
  { aggregateType :: Text
  , aggregateId :: Text
  }
  deriving stock (Eq, Show)


newtype StreamVersion = StreamVersion Word64
  deriving stock (Eq, Ord, Show)


newtype StreamPosition = StreamPosition Word64
  deriving stock (Eq, Ord, Show)


newtype ExpectedSequence = ExpectedSequence StreamPosition
  deriving stock (Eq, Show)


newtype ActualSequence = ActualSequence StreamPosition
  deriving stock (Eq, Show)


newtype StreamKey entity = StreamKey StreamIdentity
  deriving stock (Eq, Show)


data MetadataMismatch
  = EventTypeMismatch Text Text
  | EventVersionMismatch EventVersion EventVersion
  deriving stock (Eq, Show)


data ReplayError entity
  = EventDecodeFailed StreamPosition DecodeCause
  | EventMetadataMismatch StreamPosition MetadataMismatch
  | EventSequenceMismatch ExpectedSequence ActualSequence
  | EventSequenceOverflow StreamPosition
  | EventApplicationFailed StreamPosition (Aggregate.ApplyError entity)


deriving stock instance
  Eq (Aggregate.ApplyError entity) => Eq (ReplayError entity)


deriving stock instance
  Show (Aggregate.ApplyError entity) => Show (ReplayError entity)


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


streamKey
  :: forall entity
   . EventSourced entity
  => Aggregate.EntityId entity
  -> StreamKey entity
streamKey identifier =
  StreamKey
    StreamIdentity
      { aggregateType = Aggregate.aggregateType (Proxy @entity)
      , aggregateId = Aggregate.encodeEntityId identifier
      }


streamKeyIdentity :: StreamKey entity -> StreamIdentity
streamKeyIdentity (StreamKey identity) = identity


replay
  :: forall entity
   . EventSourced entity
  => StreamKey entity
  -> [StoredEvent]
  -> Either (ReplayError entity) (Maybe entity)
replay _ events =
  fst <$> foldM replayEvent (Nothing, StreamPosition 1) events


resume
  :: forall entity
   . EventSourced entity
  => StreamKey entity
  -> StreamVersion
  -> entity
  -> [StoredEvent]
  -> Either (ReplayError entity) entity
resume _ (StreamVersion version) entity events = do
  initialPosition <- nextPosition (StreamPosition version)
  (result, _) <- foldM replayEvent (Just entity, initialPosition) events

  pure (fromMaybe entity result)


replayEvent
  :: forall entity
   . EventSourced entity
  => (Maybe entity, StreamPosition)
  -> StoredEvent
  -> Either (ReplayError entity) (Maybe entity, StreamPosition)
replayEvent (currentState, expectedPosition) stored = do
  let storedPosition = StreamPosition stored.sequence

  validateSequence expectedPosition storedPosition
  event <-
    first
      (EventDecodeFailed storedPosition)
      (Aggregate.decodeEvent @entity stored.payload)
  validateEventMetadata storedPosition stored event
  nextState <-
    first (EventApplicationFailed storedPosition) case currentState of
      Nothing -> Aggregate.originate event
      Just current -> Aggregate.evolve current event
  followingPosition <- nextPosition expectedPosition

  pure (Just nextState, followingPosition)


validateSequence
  :: StreamPosition
  -> StreamPosition
  -> Either (ReplayError entity) ()
validateSequence expected actual
  | actual == expected = Right ()
  | otherwise =
      Left
        ( EventSequenceMismatch
            (ExpectedSequence expected)
            (ActualSequence actual)
        )


validateEventMetadata
  :: EventSourced entity
  => StreamPosition
  -> StoredEvent
  -> Aggregate.Event entity
  -> Either (ReplayError entity) ()
validateEventMetadata position stored event
  | stored.eventType /= expectedType =
      mismatch (EventTypeMismatch expectedType stored.eventType)
  | EventVersion stored.eventVersion /= expectedVersion =
      mismatch
        ( EventVersionMismatch
            expectedVersion
            (EventVersion stored.eventVersion)
        )
  | otherwise = Right ()
  where
    expectedType = Aggregate.eventType event
    expectedVersion = Aggregate.eventVersion event
    mismatch = Left . EventMetadataMismatch position


nextPosition
  :: StreamPosition
  -> Either (ReplayError entity) StreamPosition
nextPosition position@(StreamPosition sequence)
  | sequence == maxBound = Left (EventSequenceOverflow position)
  | otherwise = Right (StreamPosition (sequence + 1))


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
      <> encodeString stream.aggregateType
      <> encodeString stream.aggregateId
      <> maybe encodeNull encodeWord64 after


encodeCurrentVersion :: StreamIdentity -> ByteString
encodeCurrentVersion stream =
  toStrictByteString $
    encodeListLen 3
      <> encodeWord 1
      <> encodeString stream.aggregateType
      <> encodeString stream.aggregateId


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


decodeStoredEvents :: ByteString -> Either String [StoredEvent]
decodeStoredEvents bytes =
  case deserialiseFromBytes
    decodeStoredEventsWire
    (LazyByteString.fromStrict bytes) of
    Left failure -> Left (show failure)
    Right (remaining, events)
      | LazyByteString.null remaining -> Right events
      | otherwise -> Left "trailing bytes after stored events"


encodeProposedEvent :: ProposedEvent -> Encoding
encodeProposedEvent event =
  encodeListLen 3
    <> encodeString event.eventType
    <> encodeString event.eventVersion
    <> encodeBytes event.payload


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


decodeResponse
  :: (ByteString -> Either String value)
  -> ByteString
  -> Either EngineError value
decodeResponse decoder =
  first (BindingProtocolError . Text.pack) . decoder
