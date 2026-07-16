module EventSorcery.Engine.AcquisitionSpec (tests) where

import Control.Exception (IOException, try)
import Control.Monad (join)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import EventSorcery.Engine.Acquisition (StoreAcquisition (..), acquireStore)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (assertFailure, testCase, (@?=))
import Prelude (
  Either (..),
  IO,
  String,
  ioError,
  pure,
  userError,
  ($),
  (<>),
  (>>),
  (>>=),
 )


tests :: TestTree
tests =
  testGroup
    "store acquisition"
    [ testCase "releases a rejected opening in close-then-free order" $ do
        trace <- newIORef []

        result <- acquireStore (fixture trace (pure (Left "rejected")) pureOwner)

        result @?= Left "rejected"
        readIORef trace >>= (@?= ["allocate", "open", "close", "free"])
    , testCase "releases when opening raises" $ do
        trace <- newIORef []

        result <-
          try @IOException
            ( acquireStore
                (fixture trace (ioError (userError "open failed")) pureOwner)
            )

        assertRaised result
        readIORef trace >>= (@?= ["allocate", "open", "close", "free"])
    , testCase "releases when gate creation raises" $ do
        trace <- newIORef []
        let acquisition =
              (fixture trace (pure (Right ())) pureOwner)
                { createGate =
                    record trace "gate" >> ioError (userError "gate failed")
                }

        result <- try @IOException (acquireStore acquisition)

        assertRaised result
        readIORef trace
          >>= (@?= ["allocate", "open", "gate", "close", "free"])
    , testCase "releases when owner creation raises" $ do
        trace <- newIORef []
        let failingOwner _ = ioError (userError "owner failed")

        result <-
          try @IOException
            (acquireStore (fixture trace (pure (Right ())) failingOwner))

        assertRaised result
        readIORef trace
          >>= (@?= ["allocate", "open", "gate", "owner", "close", "free"])
    , testCase "transfers release ownership after acquisition" $ do
        trace <- newIORef []
        finalizer <- newIORef (pure ())
        let captureOwner release = writeIORef finalizer release >> pure "owner"

        result <- acquireStore (fixture trace (pure (Right ())) captureOwner)

        result @?= Right "ownergate"
        readIORef trace >>= (@?= ["allocate", "open", "gate", "owner"])

        join (readIORef finalizer)
        readIORef trace
          >>= (@?= ["allocate", "open", "gate", "owner", "close", "free"])
    ]


fixture
  :: IORef [String]
  -> IO (Either String ())
  -> (IO () -> IO String)
  -> StoreAcquisition () String String String String
fixture trace openAction ownerAction =
  StoreAcquisition
    { allocate = record trace "allocate"
    , open = \() -> record trace "open" >> openAction
    , close = \() -> record trace "close"
    , free = \() -> record trace "free"
    , createGate = record trace "gate" >> pure "gate"
    , createOwner = \() release ->
        record trace "owner" >> ownerAction release
    , assemble = (<>)
    }


pureOwner :: IO () -> IO String
pureOwner _ = pure "owner"


record :: IORef [String] -> String -> IO ()
record trace event = modifyIORef' trace (<> [event])


assertRaised :: Either IOException value -> IO ()
assertRaised result =
  case result of
    Left _ -> pure ()
    Right _ -> assertFailure "expected an IOException"
