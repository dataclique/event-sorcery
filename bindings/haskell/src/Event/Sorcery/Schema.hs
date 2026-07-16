-- | Aggregate schema reconciliation through the shared engine.
module Event.Sorcery.Schema (
  SchemaReconciliation (..),
  reconcileSchema,
  recordSchema,
) where

import Codec.CBOR.Encoding (
  encodeListLen,
  encodeString,
  encodeWord,
  encodeWord64,
 )
import Codec.CBOR.Write (toStrictByteString)
import Data.ByteString (ByteString)
import Data.Proxy (Proxy)
import Data.Text qualified as Text
import Data.Word (Word64, Word8)
import Event.Sorcery.Aggregate (
  CompactionPolicy (CompactAfterSnapshot, Retain),
  EventSourced (aggregateType, compactionPolicy, schemaVersion),
  SchemaVersion (SchemaVersion),
 )
import Event.Sorcery.Engine (EngineError (BindingProtocolError), Store)
import Event.Sorcery.Engine.Internal (
  callWithoutOutput,
  withInputBuffer,
  withOpenStore,
 )
import Event.Sorcery.Engine.Internal.FFI (
  esSchemaReconcile,
  esSchemaRecord,
 )
import Foreign.Marshal.Alloc (alloca)
import Foreign.Storable (peek, poke)
import Prelude (
  Either (Left, Right),
  Eq,
  IO,
  Show,
  Word,
  fromIntegral,
  pure,
  show,
  ($),
  (.),
  (<$>),
  (<>),
 )


-- | Whether startup reconciliation invalidated derived aggregate state.
data SchemaReconciliation
  = Changed
  | Unchanged
  deriving stock (Eq, Show)


-- | Reconciles an aggregate definition before loading its streams.
reconcileSchema
  :: EventSourced entity
  => Store
  -> Proxy entity
  -> IO (Either EngineError SchemaReconciliation)
reconcileSchema store entity =
  withOpenStore store $ \handle ->
    withInputBuffer (encodeSchemaTarget entity) $ \request ->
      alloca $ \outReconciliation -> do
        poke outReconciliation 0
        reconciled <-
          callWithoutOutput
            (esSchemaReconcile handle request outReconciliation)

        case reconciled of
          Left failure -> pure (Left failure)
          Right () -> decodeReconciliation <$> peek outReconciliation


-- | Records a successfully recovered aggregate schema version.
recordSchema
  :: EventSourced entity
  => Store
  -> Proxy entity
  -> IO (Either EngineError ())
recordSchema store entity =
  withOpenStore store $ \handle ->
    withInputBuffer
      (encodeSchemaTarget entity)
      (callWithoutOutput . esSchemaRecord handle)


encodeSchemaTarget :: EventSourced entity => Proxy entity -> ByteString
encodeSchemaTarget entity =
  toStrictByteString $
    encodeListLen 4
      <> encodeWord 1
      <> encodeString (aggregateType entity)
      <> encodeWord64 (schemaVersionWord64 (schemaVersion entity))
      <> encodeWord (compactionTag (compactionPolicy entity))


schemaVersionWord64 :: SchemaVersion -> Word64
schemaVersionWord64 (SchemaVersion version) = fromIntegral version


compactionTag :: CompactionPolicy -> Word
compactionTag Retain = 0
compactionTag CompactAfterSnapshot = 1


decodeReconciliation :: Word8 -> Either EngineError SchemaReconciliation
decodeReconciliation 0 = Right Changed
decodeReconciliation 1 = Right Unchanged
decodeReconciliation tag =
  Left . BindingProtocolError $
    "unexpected schema reconciliation tag " <> Text.pack (show tag)
