module EventSorcery.Engine (
  Store,
  abiVersion,
  closeStore,
  commit,
  currentVersion,
  loadStream,
  openStore,
) where

import Control.Concurrent.MVar (MVar, newMVar, withMVar)
import Control.Exception (finally)
import Data.Bifunctor (first)
import Data.Bits (shiftR)
import Data.ByteString (ByteString)
import Data.ByteString qualified as ByteString
import Data.List.NonEmpty (NonEmpty)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Text qualified as Text
import Data.Word (Word32, Word64)
import EventSorcery.Engine.Codec (
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
import EventSorcery.Engine.Types (
  EngineError (..),
  ErrorClass (..),
  OpenOptions,
  ProposedEvent,
  StoredEvent,
  StreamIdentity,
 )
import Foreign.C.Types (CInt, CSize)
import Foreign.Concurrent qualified as Foreign
import Foreign.ForeignPtr (ForeignPtr, withForeignPtr)
import Foreign.Marshal.Alloc (alloca, free, malloc)
import Foreign.Ptr (Ptr, castPtr, nullPtr)
import Foreign.Storable (peek, poke)
import Prelude (
  Bounded (maxBound),
  Either (..),
  Eq ((==)),
  IO,
  Int,
  Maybe,
  Show (show),
  String,
  fromIntegral,
  pure,
  ($),
  (.),
  (<$>),
  (<>),
  (>),
  (>>=),
 )


data Store = Store (ForeignPtr (Ptr EsStore)) (MVar ())


abiVersion :: IO Word32
abiVersion = esAbiVersion


openStore :: OpenOptions -> IO (Either EngineError Store)
openStore options = do
  version <- abiVersion
  if version `shiftR` 16 == supportedAbiMajor
    then openCompatibleStore options
    else
      pure . Left $
        EngineError
          AbiMismatch
          ("unsupported engine ABI version " <> Text.pack (show version))


closeStore :: Store -> IO (Either EngineError ())
closeStore (Store owner gate) =
  withMVar gate $ \() ->
    withForeignPtr owner $ \cell -> do
      status <- esClose cell
      pure (statusWithoutDetail status)


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


supportedAbiMajor :: Word32
supportedAbiMajor = 0


openCompatibleStore :: OpenOptions -> IO (Either EngineError Store)
openCompatibleStore options = do
  cell <- malloc
  poke cell nullPtr
  opened <-
    withInputBuffer (encodeOpenOptions options) $ \request ->
      callWithoutOutput (esOpen request cell)
  case opened of
    Left engineError -> do
      free cell
      pure (Left engineError)
    Right () -> do
      gate <- newMVar ()
      owner <-
        Foreign.newForeignPtr cell do
          _ <- esClose cell
          free cell
      pure (Right (Store owner gate))


withOpenStore
  :: Store
  -> (Ptr EsStore -> IO (Either EngineError value))
  -> IO (Either EngineError value)
withOpenStore (Store owner gate) action =
  withMVar gate $ \() ->
    withForeignPtr owner $ \cell -> do
      handle <- peek cell
      if handle == nullPtr
        then pure (Left (EngineError StateError "store is closed"))
        else action handle


withInputBuffer :: ByteString -> (Ptr EsBuf -> IO value) -> IO value
withInputBuffer bytes action =
  ByteString.useAsCStringLen bytes $ \(pointer, length) ->
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
        else pure (Left (BindingProtocolError "null output buffer with nonzero length"))
    else
      if length > fromIntegral (maxBound :: Int)
        then
          pure (Left (BindingProtocolError "engine output exceeds Haskell size limits"))
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
      case decodeEngineError encoded of
        Right engineError -> engineError
        Left cause ->
          BindingProtocolError
            ( "cannot decode engine error for status "
                <> Text.pack (show status)
                <> ": "
                <> Text.pack cause
            )


decodeResponse
  :: (ByteString -> Either String value)
  -> ByteString
  -> Either EngineError value
decodeResponse decoder =
  first (BindingProtocolError . Text.pack) . decoder


statusWithoutDetail :: CInt -> Either EngineError ()
statusWithoutDetail 0 = Right ()
statusWithoutDetail 100 = Left (EngineError PanicError "engine panic")
statusWithoutDetail status =
  Left
    ( EngineError
        (UnknownError (fromIntegral status))
        "engine returned an unknown status"
    )


emptyBuffer :: EsBuf
emptyBuffer = EsBuf nullPtr (0 :: CSize)
