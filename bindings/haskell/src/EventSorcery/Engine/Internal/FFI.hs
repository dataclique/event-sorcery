module EventSorcery.Engine.Internal.FFI (
  EsBuf (..),
  EsStore,
  esAbiVersion,
  esBufFree,
  esClose,
  esCommit,
  esCommitWithJob,
  esCurrentVersion,
  esJobAcknowledge,
  esJobClaim,
  esJobDeadLetter,
  esJobDefer,
  esJobEnqueue,
  esJobPoll,
  esJobRenew,
  esJobRetry,
  esLoadStream,
  esOpen,
) where

import Data.Word (Word32, Word64, Word8)
import Foreign.C.Types (CInt (..), CSize)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (
  Storable (..),
  peekByteOff,
  pokeByteOff,
 )
import Prelude (IO, Int, max, (+), (<$>), (<*>))


data EsBuf = EsBuf
  { pointer :: Ptr Word8
  , length :: CSize
  }


data EsStore


instance Storable EsBuf where
  sizeOf _ = pointerSize + lengthSize
  alignment _ = max pointerAlignment lengthAlignment
  peek buffer =
    EsBuf
      <$> peekByteOff buffer 0
      <*> peekByteOff buffer pointerSize
  poke buffer value = do
    pokeByteOff buffer 0 value.pointer
    pokeByteOff buffer pointerSize value.length


pointerSize :: Int
pointerSize = sizeOf (nullPtr :: Ptr Word8)


pointerAlignment :: Int
pointerAlignment = alignment (nullPtr :: Ptr Word8)


lengthSize :: Int
lengthSize = sizeOf (0 :: CSize)


lengthAlignment :: Int
lengthAlignment = alignment (0 :: CSize)


foreign import capi unsafe "event_sorcery.h es_abi_version"
  esAbiVersion :: IO Word32


foreign import capi safe "event_sorcery.h es_hs_open"
  esOpen :: Ptr EsBuf -> Ptr (Ptr EsStore) -> Ptr EsBuf -> IO CInt


foreign import capi safe "event_sorcery.h es_load_stream"
  esLoadStream :: Ptr EsStore -> Ptr EsBuf -> Ptr EsBuf -> Ptr EsBuf -> IO CInt


foreign import capi safe "event_sorcery.h es_current_version"
  esCurrentVersion
    :: Ptr EsStore -> Ptr EsBuf -> Ptr Word64 -> Ptr EsBuf -> IO CInt


foreign import capi safe "event_sorcery.h es_commit"
  esCommit :: Ptr EsStore -> Ptr EsBuf -> Ptr EsBuf -> IO CInt


foreign import capi safe "event_sorcery.h es_commit_with_job"
  esCommitWithJob :: Ptr EsStore -> Ptr EsBuf -> Ptr EsBuf -> IO CInt


foreign import capi safe "event_sorcery.h es_job_enqueue"
  esJobEnqueue :: Ptr EsStore -> Ptr EsBuf -> Ptr EsBuf -> IO CInt


foreign import capi safe "event_sorcery.h es_job_poll"
  esJobPoll :: Ptr EsStore -> Ptr EsBuf -> Ptr EsBuf -> Ptr EsBuf -> IO CInt


foreign import capi safe "event_sorcery.h es_job_claim"
  esJobClaim :: Ptr EsStore -> Ptr EsBuf -> Ptr EsBuf -> Ptr EsBuf -> IO CInt


foreign import capi safe "event_sorcery.h es_job_renew"
  esJobRenew :: Ptr EsStore -> Ptr EsBuf -> Ptr Word8 -> Ptr EsBuf -> IO CInt


foreign import capi safe "event_sorcery.h es_job_ack"
  esJobAcknowledge
    :: Ptr EsStore -> Ptr EsBuf -> Ptr Word8 -> Ptr EsBuf -> IO CInt


foreign import capi safe "event_sorcery.h es_job_retry"
  esJobRetry :: Ptr EsStore -> Ptr EsBuf -> Ptr Word8 -> Ptr EsBuf -> IO CInt


foreign import capi safe "event_sorcery.h es_job_defer"
  esJobDefer :: Ptr EsStore -> Ptr EsBuf -> Ptr Word8 -> Ptr EsBuf -> IO CInt


foreign import capi safe "event_sorcery.h es_job_dead_letter"
  esJobDeadLetter
    :: Ptr EsStore
    -> Ptr EsBuf
    -> Ptr Word8
    -> Ptr EsBuf
    -> IO CInt


foreign import capi safe "event_sorcery.h es_hs_close"
  esClose :: Ptr (Ptr EsStore) -> IO CInt


foreign import capi unsafe "event_sorcery.h es_buf_free"
  esBufFree :: Ptr EsBuf -> IO ()
