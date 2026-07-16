module Event.Sorcery.Engine.Internal (
  EngineError (..),
  ErrorClass (..),
  OpenOptions (..),
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
  decodeString,
  decodeWord,
  decodeWord32,
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
import Control.Exception (finally)
import Data.Bits (shiftR, (.&.))
import Data.ByteString (ByteString)
import Data.ByteString qualified as ByteString
import Data.ByteString.Lazy qualified as LazyByteString
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Word (Word32, Word64)
import Event.Sorcery.Engine.Internal.FFI (
  EsBuf (..),
  EsStore,
  esAbiVersion,
  esBufFree,
  esClose,
  esOpen,
 )
import Foreign.C.Types (CInt, CSize)
import Foreign.Concurrent qualified as Foreign
import Foreign.ForeignPtr (ForeignPtr, withForeignPtr)
import Foreign.Marshal.Alloc (alloca, free, malloc)
import Foreign.Ptr (Ptr, castPtr, nullPtr)
import Foreign.Storable (peek, poke)
import Prelude (
  Bool (..),
  Bounded (maxBound),
  Either (..),
  Eq,
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
  (.),
  (<$>),
  (<>),
  (==),
  (>),
  (>=),
 )


data OpenOptions = OpenOptions
  { path :: Text
  , busyTimeoutMilliseconds :: Word64
  , poolSize :: Word32
  , runtimeThreads :: Word64
  }
  deriving stock (Eq, Show)


data ErrorClass
  = DecodeError
  | ConflictError
  | JobError
  | StorageError
  | StateError
  | AbiMismatch
  | PanicError
  | UnknownError Word32
  deriving stock (Eq, Show)


data EngineError
  = EngineError ErrorClass Text
  | BindingProtocolError Text
  deriving stock (Eq, Show)


data Store = Store (ForeignPtr (Ptr EsStore)) (MVar ())


abiVersion :: IO Word32
abiVersion = esAbiVersion


supportsAbiVersion :: Word32 -> Bool
supportsAbiVersion version =
  version `shiftR` 16 == supportedAbiMajor
    && version .&. abiMinorMask >= minimumSupportedAbiMinor


openStore :: OpenOptions -> IO (Either EngineError Store)
openStore options = do
  version <- abiVersion

  if supportsAbiVersion version
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


encodeOpenOptions :: OpenOptions -> ByteString
encodeOpenOptions options =
  toStrictByteString $
    encodeListLen 5
      <> encodeWord 1
      <> encodeString options.path
      <> encodeWord64 options.busyTimeoutMilliseconds
      <> encodeWord32 options.poolSize
      <> encodeWord64 options.runtimeThreads


decodeEngineError :: ByteString -> Either String EngineError
decodeEngineError bytes =
  case deserialiseFromBytes
    decodeEngineErrorWire
    (LazyByteString.fromStrict bytes) of
    Left failure -> Left (show failure)
    Right (remaining, engineError)
      | LazyByteString.null remaining -> Right engineError
      | otherwise -> Left "trailing bytes after engine error"


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


supportedAbiMajor :: Word32
supportedAbiMajor = 0


minimumSupportedAbiMinor :: Word32
minimumSupportedAbiMinor = 4


abiMinorMask :: Word32
abiMinorMask = 0xffff


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
        else
          pure
            ( Left
                (BindingProtocolError "null output buffer with nonzero length")
            )
    else
      if length > fromIntegral (maxBound :: Int)
        then
          pure
            ( Left
                ( BindingProtocolError
                    "engine output exceeds Haskell size limits"
                )
            )
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
