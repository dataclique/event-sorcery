module SchemaSpec (spec) where

import Data.Proxy (Proxy (Proxy))
import Event.Sorcery.Aggregate (
  CompactionPolicy (CompactAfterSnapshot),
  EventSourced (..),
  SchemaVersion (SchemaVersion),
 )
import Event.Sorcery.Engine (
  EngineError (EngineError),
  ErrorClass (StateError),
  OpenOptions (OpenOptions),
  Store,
  closeStore,
  openStore,
 )
import Event.Sorcery.Schema (
  SchemaReconciliation (Changed, Unchanged),
  reconcileSchema,
  recordSchema,
 )
import Test.Hspec (Spec, it, shouldReturn)
import Prelude (Either (Left, Right), IO, error, pure, ($))


data Account


data AccountV2


data AccountId


data AccountV2Id


data AccountCommand


data AccountEvent


data AccountV2Event


data AccountCommandError


data AccountApplyError


instance EventSourced Account where
  type EntityId Account = AccountId
  type Command Account = AccountCommand
  type Event Account = AccountEvent
  type CommandError Account = AccountCommandError
  type ApplyError Account = AccountApplyError
  type Jobs Account = '[]


  aggregateType _ = "account"
  encodeEntityId _ = "account-id"
  eventType _ = "event"
  eventVersion _ = error "unused event version"
  schemaVersion _ = SchemaVersion 1
  encodeEvent _ = error "unused event encoder"
  decodeEvent _ = error "unused event decoder"
  encodeSnapshot _ = error "unused snapshot encoder"
  decodeSnapshot _ = error "unused snapshot decoder"
  originate _ = error "unused origin fold"
  evolve _ _ = error "unused evolution fold"
  initialize _ = error "unused initialization handler"
  transition _ _ = error "unused transition handler"


instance EventSourced AccountV2 where
  type EntityId AccountV2 = AccountV2Id
  type Command AccountV2 = AccountCommand
  type Event AccountV2 = AccountV2Event
  type CommandError AccountV2 = AccountCommandError
  type ApplyError AccountV2 = AccountApplyError
  type Jobs AccountV2 = '[]


  aggregateType _ = "account"
  encodeEntityId _ = "account-id"
  eventType _ = "event"
  eventVersion _ = error "unused event version"
  schemaVersion _ = SchemaVersion 2
  compactionPolicy _ = CompactAfterSnapshot
  encodeEvent _ = error "unused event encoder"
  decodeEvent _ = error "unused event decoder"
  encodeSnapshot _ = error "unused snapshot encoder"
  decodeSnapshot _ = error "unused snapshot decoder"
  originate _ = error "unused origin fold"
  evolve _ _ = error "unused evolution fold"
  initialize _ = error "unused initialization handler"
  transition _ _ = error "unused transition handler"


spec :: Spec
spec = it "reconciles aggregate schemas through the shared engine" do
  withStore $ \store -> do
    reconcileSchema store (Proxy @Account) `shouldReturn` Right Changed
    recordSchema store (Proxy @Account) `shouldReturn` Right ()
    reconcileSchema store (Proxy @Account) `shouldReturn` Right Unchanged

    compactedChange <- reconcileSchema store (Proxy @AccountV2)
    case compactedChange of
      Left (EngineError StateError _) -> pure ()
      _ -> error "compacted schema change was not refused"


withStore :: (Store -> IO ()) -> IO ()
withStore action = do
  opened <- openStore (OpenOptions "sqlite::memory:" 5000 1 1)

  case opened of
    Left _ -> error "failed to open the shared engine"
    Right store -> do
      action store
      closeStore store `shouldReturn` Right ()
