module Main (main) where

import Data.ByteString qualified as ByteString
import Data.List.NonEmpty (NonEmpty (..))
import EventSorcery.Engine (
  abiVersion,
  closeStore,
  commit,
  currentVersion,
  loadStream,
  openStore,
 )
import EventSorcery.Engine.Types (
  EngineError (EngineError),
  ErrorClass (ConflictError, StateError),
  OpenOptions (..),
  ProposedEvent (..),
  StoredEvent (..),
  StreamIdentity (..),
 )
import Prelude (
  Either (..),
  IO,
  Maybe (Nothing),
  error,
  pure,
  show,
  (&&),
  (<>),
  (==),
 )


main :: IO ()
main = do
  version <- abiVersion
  if version == 2
    then exerciseStore
    else error "unexpected engine ABI version"


exerciseStore :: IO ()
exerciseStore = do
  opened <- openStore (OpenOptions "sqlite::memory:" 5000 1 1)
  case opened of
    Left _ -> error "failed to open the shared engine"
    Right store -> do
      committed <- commit store stream 0 (proposed :| [])
      case committed of
        Left _ -> error "failed to commit through the shared engine"
        Right () -> pure ()
      version <- currentVersion store stream
      if version == Right 1
        then pure ()
        else error "engine did not report the committed stream version"
      conflict <- commit store stream 0 (proposed :| [])
      case conflict of
        Left (EngineError ConflictError "optimistic conflict") -> pure ()
        _ -> error "Haskell did not preserve optimistic-conflict identity"
      loaded <- loadStream store stream Nothing
      if loaded == Right [stored]
        then pure ()
        else error ("Haskell loaded an unexpected event: " <> show loaded)
      firstClose <- closeStore store
      afterClose <- loadStream store stream Nothing
      secondClose <- closeStore store
      if firstClose == Right ()
        && afterClose == Left (EngineError StateError "store is closed")
        && secondClose == Right ()
        then pure ()
        else error "engine close was not idempotent"
  where
    stream = StreamIdentity "account" "one"
    proposed = ProposedEvent "Created" "1.0" (ByteString.pack [0, 1])
    stored = StoredEvent 1 "Created" "1.0" (ByteString.pack [0, 1])
