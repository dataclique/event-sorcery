module EventSorcery.Engine.Internal (
  Store,
  abiVersion,
  callWithOutput,
  callWithoutOutput,
  closeStore,
  decodeEngineError,
  encodeOpenOptions,
  openStore,
  supportsAbiVersion,
  withInputBuffer,
  withOpenStore,
) where

import Codec.CBOR.Decoding (
  Decoder,
  decodeListLen,
  decodeNull,
  decodeString,
  decodeWord,
  decodeWord32,
  decodeWord64,
 )
import Codec.CBOR.Encoding (
  encodeListLen,
  encodeString,
  encodeWord,
  encodeWord32,
  encodeWord64,
 )
import Codec.CBOR.Read (deserialiseFromBytes)
import Codec.CBOR.Write (toStrictByteString)
import Control.Concurrent.MVar (MVar, newMVar, withMVar)
import Control.Exception (finally, mask, onException)
import Data.Bits (shiftR, (.&.))
import Data.ByteString (ByteString)
import Data.ByteString qualified as ByteString
import Data.ByteString.Lazy qualified as LazyByteString
import Data.Text qualified as Text
import Data.Word (Word32)
import EventSorcery.Engine.Internal.FFI (
  EsBuf (..),
  EsStore,
  esAbiVersion,
  esBufFree,
  esClose,
  esOpen,
 )
import EventSorcery.Engine.Protocol (
  AbiVersionDetail (..),
  AggregateId (..),
  AggregateType (..),
  ConflictDetail (..),
  EngineError (..),
  OpenOptions (..),
  ResourceLimitDetail (..),
 )
import Foreign.C.Types (CInt, CSize)
import Foreign.Concurrent qualified as Foreign
import Foreign.ForeignPtr (ForeignPtr, withForeignPtr)
import Foreign.Marshal.Alloc (alloca, free, malloc)
import Foreign.Ptr (Ptr, castPtr, nullPtr)
import Foreign.Storable (peek, poke)
import Prelude (
  Bool,
  Bounded (maxBound),
  Either (..),
  Eq ((==)),
  IO,
  Int,
  Show (show),
  String,
  fail,
  fromIntegral,
  otherwise,
  pure,
  ($),
  (&&),
  (<$>),
  (<>),
  (>),
  (>=),
 )


data Store = Store (ForeignPtr (Ptr EsStore)) (MVar ())


abiVersion :: IO Word32
abiVersion = esAbiVersion


supportsAbiVersion :: Word32 -> Bool
supportsAbiVersion version =
  actualMajor == supportedAbiMajor && actualMinor >= minimumAbiMinor
  where
    actualMajor = version `shiftR` 16
    actualMinor = version .&. abiMinorMask


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
      pure (statusWithoutDetail status)


encodeOpenOptions :: OpenOptions -> ByteString
encodeOpenOptions options =
  toStrictByteString $
    encodeListLen 5
      <> encodeWord 1
      <> encodeString options.path
      <> encodeWord64 options.busyTimeoutMilliseconds
      <> encodeWord32 options.poolSize
      <> encodeWord32 options.runtimeThreads


decodeEngineError :: Word32 -> ByteString -> Either String EngineError
decodeEngineError expectedCode bytes =
  case deserialiseFromBytes
    (decodeEngineErrorWire expectedCode)
    (LazyByteString.fromStrict bytes) of
    Left failure -> Left (show failure)
    Right (remaining, engineError)
      | LazyByteString.null remaining -> Right engineError
      | otherwise -> Left "trailing bytes after engine error"


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


supportedAbiMajor :: Word32
supportedAbiMajor = 0


minimumAbiMinor :: Word32
minimumAbiMinor = 3


abiMinorMask :: Word32
abiMinorMask = 0xffff


checkAbiVersion :: Word32 -> Either EngineError ()
checkAbiVersion version =
  if supportsAbiVersion version
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
    actualMinor = version .&. abiMinorMask


openCompatibleStore :: OpenOptions -> IO (Either EngineError Store)
openCompatibleStore options = mask $ \restore -> do
  cell <- malloc
  poke cell nullPtr
  let release = do
        _ <- esClose cell
        free cell
  opened <-
    restore
      ( withInputBuffer (encodeOpenOptions options) $ \request ->
          callWithoutOutput (esOpen request cell)
      )
      `onException` release
  case opened of
    Left engineError -> do
      release
      pure (Left engineError)
    Right () -> do
      gate <- newMVar () `onException` release
      owner <-
        Foreign.newForeignPtr cell release `onException` release
      pure (Right (Store owner gate))


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


expectListLength :: Int -> Decoder s ()
expectListLength expected = do
  actual <- decodeListLen
  if actual == expected
    then pure ()
    else fail "unexpected CBOR list length"


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
      case decodeEngineError (fromIntegral status) encoded of
        Right engineError -> engineError
        Left cause ->
          BindingProtocolError
            ( "cannot decode engine error for status "
                <> Text.pack (show status)
                <> ": "
                <> Text.pack cause
            )


statusWithoutDetail :: CInt -> Either EngineError ()
statusWithoutDetail 0 = Right ()
statusWithoutDetail 100 = Left EnginePanic
statusWithoutDetail status =
  Left (UnknownEngineError (fromIntegral status))


emptyBuffer :: EsBuf
emptyBuffer = EsBuf nullPtr (0 :: CSize)
