-- | The stream feature: stream identity, the events it carries, and its paging.
--
-- Everything a consumer needs to read and append to a stream is here, down to
-- the wire codecs the engine and this binding agree on. The engine handle and
-- the errors every call can answer with belong to "EventSorcery.Engine" and
-- are re-exported so a stream handler needs this one import.
module EventSorcery.Stream (
  AbiVersionDetail (..),
  ActualSequence (..),
  AggregateId (..),
  AggregateType (..),
  BindingFault (..),
  ConflictDetail (..),
  EngineError (..),
  EngineErrorDecodeDetail (..),
  EventType (..),
  EventVersion (..),
  ExpectedSequence (..),
  MetadataMismatch (..),
  PageAdvanceDetail (..),
  ProposedEvent (..),
  ReplayError (..),
  ResourceLimitDetail (..),
  Store,
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
  encodeStreamIdentity,
  loadStream,
  loadStreamPage,
  nextCursor,
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
import Data.Word (Word64)
import EventSorcery.Aggregate (
  DecodeCause,
  EventSourced,
  EventVersion (..),
 )
import EventSorcery.Aggregate qualified as Aggregate
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
  Ord,
  Show (show),
  String,
  fail,
  fromIntegral,
  fst,
  id,
  length,
  maxBound,
  otherwise,
  pure,
  ($),
  (+),
  (.),
  (/=),
  (<$>),
  (<>),
  (>),
  (>>=),
 )


newtype EventType = EventType Text
  deriving newtype (Eq, Show)


data StreamIdentity = StreamIdentity
  { aggregateType :: AggregateType
  , aggregateId :: AggregateId
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


-- | Names the stream an entity of a given identity keeps its events in.
--
-- The pair of names is taken from the entity's own declaration rather than
-- spelled by the caller, so a handler cannot address the wrong stream for the
-- entity it is holding.
streamKey
  :: forall entity
   . EventSourced entity
  => Aggregate.EntityId entity
  -> StreamKey entity
streamKey identifier =
  StreamKey
    StreamIdentity
      { aggregateType = AggregateType (Aggregate.aggregateType (Proxy @entity))
      , aggregateId = AggregateId (Aggregate.encodeEntityId identifier)
      }


-- | Reads back the untyped stream identity a key stands for.
streamKeyIdentity :: StreamKey entity -> StreamIdentity
streamKeyIdentity (StreamKey identity) = identity


-- | Rebuilds an entity from its whole stream, or nothing when the stream is
-- empty.
--
-- The events have to start at the stream's first sequence, follow on from one
-- another, and carry the type and version the entity declares for them. A
-- stream that fails any of those is answered with a 'ReplayError' rather than
-- with a differently shaped entity.
replay
  :: forall entity
   . EventSourced entity
  => StreamKey entity
  -> [StoredEvent]
  -> Either (ReplayError entity) (Maybe entity)
replay _ events =
  fst <$> foldM replayEvent (Nothing, StreamPosition 1) events


-- | Carries an entity read at a version forward over the events after it.
--
-- The first event has to be the one that follows the version the entity was
-- read at, so a snapshot cannot silently skip a committed event.
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
      <> encodeStreamIdentity stream
      <> maybe encodeNull encodeWord64 after


encodeCurrentVersion :: StreamIdentity -> ByteString
encodeCurrentVersion stream =
  toStrictByteString $
    encodeListLen 3
      <> encodeWord 1
      <> encodeStreamIdentity stream


encodeCommit :: StreamIdentity -> Word64 -> [ProposedEvent] -> ByteString
encodeCommit stream expected events =
  toStrictByteString $
    encodeListLen 5
      <> encodeWord 1
      <> encodeStreamIdentity stream
      <> encodeWord64 expected
      <> encodeListLen (fromIntegral (length events))
      <> foldMap encodeProposedEvent events


-- | Writes the two names an operation addresses a stream by.
--
-- Every operation that names a stream spells the pair the same way, and a
-- commit that carries a job intent is one of them, so the pair is encoded in
-- one place rather than at each call.
encodeStreamIdentity :: StreamIdentity -> Encoding
encodeStreamIdentity stream =
  encodeAggregateType stream.aggregateType
    <> encodeAggregateId stream.aggregateId


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
  | storedType /= expectedType =
      mismatch (EventTypeMismatch expectedType storedType)
  | stored.eventVersion /= expectedVersion =
      mismatch (EventVersionMismatch expectedVersion stored.eventVersion)
  | otherwise = Right ()
  where
    EventType storedType = stored.eventType
    expectedType = Aggregate.eventType event
    expectedVersion = Aggregate.eventVersion event
    mismatch = Left . EventMetadataMismatch position


nextPosition
  :: StreamPosition
  -> Either (ReplayError entity) StreamPosition
nextPosition position@(StreamPosition sequence)
  | sequence == maxBound = Left (EventSequenceOverflow position)
  | otherwise = Right (StreamPosition (sequence + 1))


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
