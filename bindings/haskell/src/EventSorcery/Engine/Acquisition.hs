module EventSorcery.Engine.Acquisition (
  StoreAcquisition (..),
  acquireStore,
) where

import Control.Exception (finally, mask, onException)
import Prelude (Either (..), IO, pure)


data StoreAcquisition cell gate owner failure store = StoreAcquisition
  { allocate :: IO cell
  , open :: cell -> IO (Either failure ())
  , close :: cell -> IO ()
  , free :: cell -> IO ()
  , createGate :: IO gate
  , createOwner :: cell -> IO () -> IO owner
  , assemble :: owner -> gate -> store
  }


acquireStore
  :: StoreAcquisition cell gate owner failure store
  -> IO (Either failure store)
acquireStore acquisition = mask \restore -> do
  cell <- acquisition.allocate

  let release =
        acquisition.close cell
          `finally` acquisition.free cell

  opened <-
    restore (acquisition.open cell)
      `onException` release

  case opened of
    Left failure -> do
      release
      pure (Left failure)
    Right () -> do
      gate <- acquisition.createGate `onException` release
      owner <-
        acquisition.createOwner cell release
          `onException` release

      pure (Right (acquisition.assemble owner gate))
