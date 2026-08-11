module EventSorcery.Engine (
  Store,
  abiVersion,
  checkAbiVersion,
  closeStore,
  commit,
  currentVersion,
  loadStream,
  loadStreamPage,
  minimumAbiMinor,
  openStore,
  supportedAbiMajor,
) where

import Control.Concurrent.MVar (MVar, newMVar, withMVar)
import Control.Exception (finally, onException)
import Data.Bifunctor (first)
import Data.Bits (shiftR, (.&.))
import Data.ByteString (ByteString)
import Data.ByteString qualified as ByteString
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import Data.List.NonEmpty (NonEmpty)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Text qualified as Text
import Data.Word (Word32, Word64)
import EventSorcery.Engine.Acquisition (
  StoreAcquisition (..),
  acquireStore,
 )
import EventSorcery.Engine.Codec (
  decodeCloseStatus,
  decodeEngineError,
  decodeStoredEvents,
  encodeCommit,
  encodeCurrentVersion,
  encodeLoadStream,
  encodeOpenOptions,
 )
import EventSorcery.Engine.Internal.FFI (
  EsBuf (..),
  EsStore,
  esAbiVersion,
  esBufFree,
  esClose,
  esCommit,
  esCurrentVersion,
  esLoadStream,
  esOpen,
 )
import EventSorcery.Engine.Internal.Paging (nextCursor)
import EventSorcery.Engine.Protocol (
  AbiVersionDetail (..),
  BindingFault (..),
  EngineError (..),
  EngineErrorDecodeDetail (..),
  OpenOptions,
  ProposedEvent,
  StoredEvent (..),
  StreamIdentity,
 )
import Foreign.C.Types (CInt, CSize)
import Foreign.Concurrent qualified as Foreign
import Foreign.ForeignPtr (ForeignPtr, withForeignPtr)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Marshal.Alloc qualified as Alloc
import Foreign.Ptr (Ptr, castPtr, nullPtr)
import Foreign.Storable (peek, poke)
import Prelude (
  Bounded (maxBound),
  Either (..),
  Eq ((==)),
  IO,
  Int,
  Maybe (Just, Nothing),
  String,
  fromIntegral,
  id,
  pure,
  ($),
  (&&),
  (.),
  (<$>),
  (<>),
  (>),
  (>=),
  (>>=),
 )


data Store = Store (ForeignPtr (Ptr EsStore)) (MVar ())


abiVersion :: IO Word32
abiVersion = esAbiVersion


-- | The ABI major version this binding speaks.
supportedAbiMajor :: Word32
supportedAbiMajor = 0


-- | The oldest ABI minor version whose calls this binding can make.
minimumAbiMinor :: Word32
minimumAbiMinor = 2


-- | Decides whether a packed engine ABI version can serve this binding.
--
-- Minor versions are additive, so any engine at or above the floor is
-- accepted and a newer engine does not need a new binding.
checkAbiVersion :: Word32 -> Either EngineError ()
checkAbiVersion version =
  if actualMajor == supportedAbiMajor && actualMinor >= minimumAbiMinor
    then Right ()
    else
      Left
        ( AbiVersionMismatch
            ( AbiVersionDetail
                supportedAbiMajor
                minimumAbiMinor
                actualMajor
                actualMinor
            )
        )
  where
    actualMajor = version `shiftR` 16
    actualMinor = version .&. 0xffff


openStore :: OpenOptions -> IO (Either EngineError Store)
openStore options = do
  version <- abiVersion
  case checkAbiVersion version of
    Right () -> openCompatibleStore options
    Left engineError -> pure (Left engineError)


closeStore :: Store -> IO (Either EngineError ())
closeStore (Store owner gate) =
  withMVar gate $ \() ->
    withForeignPtr owner $ \cell -> do
      status <- esClose cell
      pure (decodeCloseStatus (fromIntegral status))


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


openCompatibleStore :: OpenOptions -> IO (Either EngineError Store)
openCompatibleStore options =
  acquireStore
    StoreAcquisition
      { allocate = allocateStoreCell
      , open = \cell ->
          withInputBuffer (encodeOpenOptions options) $ \request ->
            callWithoutOutput (esOpen request cell)
      , close = closeStoreCell
      , free = Alloc.free
      , createGate = newMVar ()
      , createOwner = Foreign.newForeignPtr
      , assemble = Store
      }


allocateStoreCell :: IO (Ptr (Ptr EsStore))
allocateStoreCell = do
  cell <- Alloc.malloc
  poke cell nullPtr `onException` Alloc.free cell
  pure cell


closeStoreCell :: Ptr (Ptr EsStore) -> IO ()
closeStoreCell cell = do
  _ <- esClose cell
  pure ()


-- | Hands the owner cell to an engine call for as long as the call runs.
--
-- The gate serializes the calls made on one store, so at most one of them is
-- in flight at a time. Holding the cell through 'withForeignPtr' keeps the
-- finalizer that frees it from running underneath the call, and the null
-- check answers a closed store without a foreign call; the engine validates
-- the cell against its own registry regardless.
withOpenStore
  :: Store
  -> (Ptr (Ptr EsStore) -> IO (Either EngineError value))
  -> IO (Either EngineError value)
withOpenStore (Store owner gate) action =
  withMVar gate $ \() ->
    withForeignPtr owner $ \cell -> do
      handle <- peek cell
      if handle == nullPtr
        then pure (Left (InvalidState "store is closed"))
        else action cell


-- | Lends an encoded request to an engine call without copying it.
--
-- The ABI borrows the request: every entry point decodes out of the buffer
-- before it returns and retains nothing, so the bytes only have to outlive the
-- call, which the continuation guarantees. Copying them -- as
-- useAsCStringLen does, to append a terminator the engine never reads --
-- would double peak memory on a commit carrying megabytes of payload.
withInputBuffer :: ByteString -> (Ptr EsBuf -> IO value) -> IO value
withInputBuffer bytes action =
  unsafeUseAsCStringLen bytes $ \(pointer, length) ->
    alloca $ \buffer -> do
      poke buffer (EsBuf (castPtr pointer) (fromIntegral length))
      action buffer


callWithoutOutput
  :: (Ptr EsBuf -> IO CInt)
  -> IO (Either EngineError ())
callWithoutOutput call =
  withErrorBuffer $ \errorBuffer -> do
    status <- call errorBuffer
    if status == 0
      then pure (Right ())
      else Left <$> readEngineError status errorBuffer


callWithOutput
  :: (Ptr EsBuf -> Ptr EsBuf -> IO CInt)
  -> IO (Either EngineError ByteString)
callWithOutput call =
  alloca $ \output -> do
    poke output emptyBuffer
    let useOutput =
          withErrorBuffer $ \errorBuffer -> do
            status <- call output errorBuffer
            if status == 0
              then readOwnedBuffer output
              else Left <$> readEngineError status errorBuffer
    useOutput `finally` esBufFree output


withErrorBuffer :: (Ptr EsBuf -> IO value) -> IO value
withErrorBuffer action =
  alloca $ \buffer -> do
    poke buffer emptyBuffer
    action buffer `finally` esBufFree buffer


readOwnedBuffer :: Ptr EsBuf -> IO (Either EngineError ByteString)
readOwnedBuffer buffer = do
  EsBuf pointer length <- peek buffer
  if pointer == nullPtr
    then
      if length == 0
        then pure (Right ByteString.empty)
        else
          pure
            (Left (BindingProtocolError (NullOutputBufferWithLength length)))
    else
      if length > fromIntegral (maxBound :: Int)
        then
          pure
            (Left (BindingProtocolError (OutputExceedsPlatformSize length)))
        else
          Right
            <$> ByteString.packCStringLen
              (castPtr pointer, fromIntegral length)


readEngineError :: CInt -> Ptr EsBuf -> IO EngineError
readEngineError status buffer = do
  bytes <- readOwnedBuffer buffer
  pure case bytes of
    Left protocolError -> protocolError
    Right encoded ->
      case decodeEngineError code encoded of
        Right engineError -> engineError
        Left cause ->
          BindingProtocolError
            ( UndecodableEngineError
                (EngineErrorDecodeDetail code (Text.pack cause))
            )
  where
    code :: Word32
    code = fromIntegral status


decodeResponse
  :: (ByteString -> Either String value)
  -> ByteString
  -> Either EngineError value
decodeResponse decoder =
  first (BindingProtocolError . UndecodableResponse . Text.pack) . decoder


emptyBuffer :: EsBuf
emptyBuffer = EsBuf nullPtr (0 :: CSize)
