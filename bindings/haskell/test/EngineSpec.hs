module Main (main) where

import Control.Concurrent (forkFinally)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar, takeMVar)
import Control.Exception (SomeException, displayException, finally)
import Data.Bits (shiftL, shiftR)
import Data.ByteString qualified as ByteString
import Data.Foldable (traverse_)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Traversable (traverse)
import Data.Word (Word64, Word8)
import EventSorcery.Engine (
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
 )
import EventSorcery.Engine.AcquisitionSpec qualified as AcquisitionSpec
import EventSorcery.Engine.Internal.FFI (EsBuf (..))
import EventSorcery.Engine.Protocol (
  AbiVersionDetail (..),
  AggregateId (..),
  AggregateType (..),
  ConflictDetail (..),
  EngineError (..),
  EventType (..),
  EventVersion (..),
  OpenOptions (..),
  ProposedEvent (..),
  StoredEvent (..),
  StreamIdentity (..),
 )
import Foreign.C.Types (CSize)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr, nullPtr, plusPtr)
import Foreign.Storable (alignment, peekByteOff, poke, sizeOf)
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.HUnit (assertFailure, testCase, (@?=))
import Prelude (
  Either (..),
  Functor (fmap),
  IO,
  Int,
  Maybe (Just, Nothing),
  Show (show),
  const,
  fromIntegral,
  map,
  min,
  otherwise,
  pure,
  replicate,
  ($),
  (*),
  (+),
  (-),
  (.),
  (<>),
  (>=),
  (>>),
  (>>=),
 )


main :: IO ()
main = defaultMain tests


tests :: TestTree
tests =
  testGroup
    "shared engine FFI"
    [ AcquisitionSpec.tests
    , testCase "reports an ABI this binding supports" $ do
        version <- abiVersion
        version `shiftR` 16 @?= supportedAbiMajor
        checkAbiVersion version @?= Right ()
    , testCase "rejects an engine whose ABI major differs" $
        checkAbiVersion (1 `shiftL` 16)
          @?= Left
            ( AbiVersionMismatch
                (AbiVersionDetail supportedAbiMajor minimumAbiMinor 1 0)
            )
    , testCase "rejects an engine below the minimum ABI minor" $
        checkAbiVersion 1
          @?= Left
            ( AbiVersionMismatch
                (AbiVersionDetail supportedAbiMajor minimumAbiMinor 0 1)
            )
    , testCase "commits and loads opaque event bytes" $
        withStore $ \store -> do
          commitFixture store
          currentVersion store stream >>= (@?= Right 1)
          loadStream store stream Nothing >>= (@?= Right [stored])
    , testCase "loads only the events after a cursor" $
        withStore $ \store -> do
          commitFixture store
          commit store stream 1 (amended :| []) >>= (@?= Right ())
          loadStream store stream (Just 1) >>= (@?= Right [storedAmended])
    , testCase "walks a stream longer than one engine page" $
        withStore $ \store -> do
          commitFiller store pageOverflowLength
          firstPage <- loadStreamPage store stream Nothing
          fmap (map (.sequence)) firstPage @?= Right [1 .. enginePageLimit]
          walked <- loadStream store stream Nothing
          fmap (map (.sequence)) walked @?= Right [1 .. pageOverflowLength]
    , testCase "preserves optimistic conflict identity and versions" $
        withStore $ \store -> do
          commitFixture store
          conflict <- commit store stream 0 (proposed :| [])
          conflict
            @?= Left
              ( OptimisticConflict
                  (ConflictDetail aggregateType aggregateId 0 1)
              )
    , testCase "serves reads issued from concurrent threads" $
        withStore $ \store -> do
          commitFixture store
          concurrentReads store
    , testCase "closes idempotently and rejects later operations" $
        withStore $ \store -> do
          closeStore store >>= (@?= Right ())
          loadStream store stream Nothing
            >>= (@?= Left (InvalidState "store is closed"))
          closeStore store >>= (@?= Right ())
    , testCase "lays EsBuf out the way the C header declares it" esBufLayout
    ]


withStore :: (Store -> IO ()) -> IO ()
withStore action = do
  opened <- openStore options
  case opened of
    Left engineError -> assertFailure ("failed to open the shared engine: " <> show engineError)
    Right store ->
      action store `finally` do
        closed <- closeStore store
        case closed of
          Left engineError ->
            assertFailure ("failed to close the shared engine: " <> show engineError)
          Right () -> pure ()


commitFixture :: Store -> IO ()
commitFixture store =
  commit store stream 0 (proposed :| []) >>= (@?= Right ())


-- | Appends @total@ filler events, respecting the engine's per-commit bound.
commitFiller :: Store -> Word64 -> IO ()
commitFiller store total = go 0
  where
    go committed
      | committed >= total = pure ()
      | otherwise = do
          let batch = min maxCommitEvents (total - committed)
          commit
            store
            stream
            committed
            (proposed :| replicate (fromIntegral batch - 1) proposed)
            >>= (@?= Right ())
          go (committed + batch)


-- | Holds every reader at a gate so their engine calls are in flight together.
--
-- What this establishes is that overlapping calls on one store all come back
-- with the committed stream: no deadlock, no crash, no lease or registry
-- corruption. It does not establish that the engine serves them in parallel --
-- the shared 'options' open one pooled connection on one runtime thread, so
-- the SQL work cannot overlap even in principle. Proving parallelism needs a
-- file-backed store opened with a wider pool.
concurrentReads :: Store -> IO ()
concurrentReads store = do
  gate <- newEmptyMVar
  slots <- traverse (const newEmptyMVar) concurrentReaders
  traverse_
    (forkFinally (readMVar gate >> loadStream store stream Nothing) . putMVar)
    slots
  putMVar gate ()
  outcomes <- traverse takeMVar slots
  traverse_ assertLoadedFixture outcomes


assertLoadedFixture
  :: Either SomeException (Either EngineError [StoredEvent]) -> IO ()
assertLoadedFixture (Left failure) =
  assertFailure ("a concurrent read raised " <> displayException failure)
assertLoadedFixture (Right loaded) = loaded @?= Right [stored]


-- | Pins the struct the engine and the binding exchange every buffer through.
esBufLayout :: IO ()
esBufLayout = do
  sizeOf (EsBuf nullPtr 0) @?= 2 * pointerWidth
  alignment (EsBuf nullPtr 0) @?= alignment (nullPtr :: Ptr Word8)
  sizeOf (0 :: CSize) @?= pointerWidth
  alloca $ \buffer -> do
    poke buffer (EsBuf lengthFixturePointer 7)
    peekByteOff buffer 0 >>= (@?= lengthFixturePointer)
    peekByteOff buffer pointerWidth >>= (@?= (7 :: CSize))


pointerWidth :: Int
pointerWidth = sizeOf (nullPtr :: Ptr Word8)


lengthFixturePointer :: Ptr Word8
lengthFixturePointer = nullPtr `plusPtr` 24


concurrentReaders :: [Int]
concurrentReaders = [1 .. 8]


-- | The engine bounds one loaded page at this many events.
enginePageLimit :: Word64
enginePageLimit = 4096


pageOverflowLength :: Word64
pageOverflowLength = enginePageLimit + 1


-- | The engine bounds one commit at this many events.
maxCommitEvents :: Word64
maxCommitEvents = 1024


options :: OpenOptions
options = OpenOptions "sqlite::memory:" 5000 1 1


aggregateType :: AggregateType
aggregateType = AggregateType "account"


aggregateId :: AggregateId
aggregateId = AggregateId "one"


stream :: StreamIdentity
stream = StreamIdentity aggregateType aggregateId


proposed :: ProposedEvent
proposed =
  ProposedEvent
    (EventType "Created")
    (EventVersion "1.0")
    (ByteString.pack [0, 1])


amended :: ProposedEvent
amended =
  ProposedEvent
    (EventType "Renamed")
    (EventVersion "1.0")
    (ByteString.pack [2, 3])


stored :: StoredEvent
stored =
  StoredEvent
    1
    (EventType "Created")
    (EventVersion "1.0")
    (ByteString.pack [0, 1])


storedAmended :: StoredEvent
storedAmended =
  StoredEvent
    2
    (EventType "Renamed")
    (EventVersion "1.0")
    (ByteString.pack [2, 3])
