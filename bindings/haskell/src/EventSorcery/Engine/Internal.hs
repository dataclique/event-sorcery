-- | The engine feature: the store handle, its lifecycle, and the ABI it speaks.
--
-- "EventSorcery.Engine" re-exports the consumer-facing half of this module.
-- The call marshalling and the errors every call can answer with stay here so
-- the stream feature can issue engine calls without republishing the ABI
-- plumbing to consumers.
module EventSorcery.Engine.Internal (
  AbiVersionDetail (..),
  AggregateId (..),
  AggregateType (..),
  BindingFault (..),
  ConflictDetail (..),
  EngineError (..),
  EngineErrorDecodeDetail (..),
  OpenOptions (..),
  PageAdvanceDetail (..),
  ResourceLimitDetail (..),
  Store,
  abiVersion,
  callWithOutput,
  callWithoutOutput,
  checkAbiVersion,
  closeStore,
  decodeCloseStatus,
  decodeEngineError,
  decodeResponse,
  encodeOpenOptions,
  expectListLength,
  minimumAbiMinor,
  openStore,
  supportedAbiMajor,
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
import Codec.CBOR.Term (decodeTerm)
import Codec.CBOR.Write (toStrictByteString)
import Control.Concurrent.MVar (MVar, newMVar, withMVar)
import Control.Exception (finally, onException)
import Data.Bifunctor (first)
import Data.Bits (shiftR, (.&.))
import Data.ByteString (ByteString)
import Data.ByteString qualified as ByteString
import Data.ByteString.Lazy qualified as LazyByteString
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Word (Word32, Word64)
import EventSorcery.Engine.Acquisition (
  StoreAcquisition (..),
  acquireStore,
 )
import EventSorcery.Engine.Internal.FFI (
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
  (>),
  (>=),
 )


data Store = Store (ForeignPtr (Ptr EsStore)) (MVar ())


data OpenOptions = OpenOptions
  { path :: Text
  -- ^ A SQLite connection URL, not a filesystem path.
  --
  -- The engine hands this to sqlx's SqliteConnectOptions URL parser: a
  -- leading "sqlite:" scheme is stripped, everything after the first
  -- question mark is read as query parameters and an unrecognised key
  -- fails the open, and the remainder is percent-decoded. A filesystem
  -- path holding a question mark, hash or percent sign therefore names a
  -- different database than it reads like. The literal "sqlite::memory:"
  -- opens a private in-memory database.
  , busyTimeoutMilliseconds :: Word64
  , poolSize :: Word32
  , runtimeThreads :: Word32
  }
  deriving stock (Eq, Show)


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


newtype AggregateType = AggregateType Text
  deriving newtype (Eq, Show)


newtype AggregateId = AggregateId Text
  deriving newtype (Eq, Show)


data ConflictDetail = ConflictDetail
  { aggregateType :: AggregateType
  , aggregateId :: AggregateId
  , expectedVersion :: Word64
  , actualVersion :: Word64
  }
  deriving stock (Eq, Show)


data ResourceLimitDetail = ResourceLimitDetail
  { resource :: Text
  , observed :: Word64
  , limit :: Word64
  }
  deriving stock (Eq, Show)


data AbiVersionDetail = AbiVersionDetail
  { expectedMajor :: Word32
  , minimumMinor :: Word32
  , actualMajor :: Word32
  , actualMinor :: Word32
  }
  deriving stock (Eq, Show)


data PageAdvanceDetail = PageAdvanceDetail
  { cursor :: Word64
  , lastSequence :: Word64
  }
  deriving stock (Eq, Show)


data EngineErrorDecodeDetail = EngineErrorDecodeDetail
  { status :: Word32
  , cause :: Text
  }
  deriving stock (Eq, Show)


-- | A failure this binding detects rather than one the engine reported.
--
-- The engine's own errors arrive as a numeric class plus a decoded detail.
-- These are raised on the Haskell side of the ABI, so each keeps the values
-- that identify it instead of collapsing into prose a consumer could only
-- substring-match. The two remaining text payloads are cborg's own failure
-- description, which has no structure to preserve.
data BindingFault
  = NullOutputBufferWithLength CSize
  | OutputExceedsPlatformSize CSize
  | PageDidNotAdvance PageAdvanceDetail
  | UndecodableEngineError EngineErrorDecodeDetail
  | UndecodableResponse Text
  deriving stock (Eq, Show)


data EngineError
  = MalformedInput
  | OptimisticConflict ConflictDetail
  | StorageFailure Text
  | InvalidState Text
  | ResourceLimitExceeded ResourceLimitDetail
  | AbiVersionMismatch AbiVersionDetail
  | EnginePanic
  | UnknownEngineError Word32
  | BindingProtocolError BindingFault
  deriving stock (Eq, Show)


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


-- | Interprets the status of a call that carries no error buffer.
--
-- Only @es_close@ answers this way, so the numeric code is the whole error and
-- the invalid-state text names the one condition the engine reports there.
decodeCloseStatus :: Word32 -> Either EngineError ()
decodeCloseStatus 0 = Right ()
decodeCloseStatus 5 = Left (InvalidState "store close rejected the owner handle")
decodeCloseStatus 100 = Left EnginePanic
decodeCloseStatus status = Left (UnknownEngineError status)


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


decodeResponse
  :: (ByteString -> Either String value)
  -> ByteString
  -> Either EngineError value
decodeResponse decoder =
  first (BindingProtocolError . UndecodableResponse . Text.pack) . decoder


-- | Rejects a CBOR product whose arity is not the one the ABI specifies.
--
-- Products are encoded as definite-length arrays, so the arity is the first
-- thing a decoder can disagree with the engine about.
expectListLength :: Int -> Decoder s ()
expectListLength expected = do
  actual <- decodeListLen
  if actual == expected
    then pure ()
    else fail "unexpected CBOR list length"


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

-- The engine may be newer than this binding, so a code it does not model keeps
-- its numeric identity instead of collapsing into a binding protocol failure.
-- The clauses above are exactly the classes the engine can return today; an
-- ABI-version mismatch is not among them, because the binding raises that one
-- itself and there is no encoded detail shape to agree on.
decodeEngineErrorDetail code = do
  _ <- decodeTerm
  pure (UnknownEngineError code)


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


emptyBuffer :: EsBuf
emptyBuffer = EsBuf nullPtr (0 :: CSize)
