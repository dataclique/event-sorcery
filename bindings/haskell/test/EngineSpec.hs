module Main (main) where

import Control.Exception (finally)
import Data.ByteString qualified as ByteString
import Data.List.NonEmpty (NonEmpty (..))
import EventSorcery.Engine (
  Store,
  abiVersion,
  closeStore,
  commit,
  currentVersion,
  loadStream,
  openStore,
 )
import EventSorcery.Engine.AcquisitionSpec qualified as AcquisitionSpec
import EventSorcery.Engine.Protocol (
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
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.HUnit (assertFailure, testCase, (@?=))
import Prelude (
  Either (..),
  IO,
  Maybe (Nothing),
  Show (show),
  pure,
  ($),
  (<>),
  (>>=),
 )


main :: IO ()
main = defaultMain tests


tests :: TestTree
tests =
  testGroup
    "shared engine FFI"
    [ AcquisitionSpec.tests
    , testCase "reports ABI 0.2" $
        abiVersion >>= (@?= 2)
    , testCase "commits and loads opaque event bytes" $
        withStore $ \store -> do
          commitFixture store
          currentVersion store stream >>= (@?= Right 1)
          loadStream store stream Nothing >>= (@?= Right [stored])
    , testCase "preserves optimistic conflict identity and versions" $
        withStore $ \store -> do
          commitFixture store
          conflict <- commit store stream 0 (proposed :| [])
          conflict
            @?= Left
              ( OptimisticConflict
                  (ConflictDetail aggregateType aggregateId 0 1)
              )
    , testCase "closes idempotently and rejects later operations" $
        withStore $ \store -> do
          closeStore store >>= (@?= Right ())
          loadStream store stream Nothing
            >>= (@?= Left (InvalidState "store is closed"))
          closeStore store >>= (@?= Right ())
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


stored :: StoredEvent
stored =
  StoredEvent
    1
    (EventType "Created")
    (EventVersion "1.0")
    (ByteString.pack [0, 1])
