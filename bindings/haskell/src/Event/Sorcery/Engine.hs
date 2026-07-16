-- | Safe ownership and error handling for the shared Rust engine.
module Event.Sorcery.Engine (
  EngineError (..),
  ErrorClass (..),
  OpenOptions (..),
  Store,
  abiVersion,
  closeStore,
  decodeEngineError,
  encodeOpenOptions,
  openStore,
  supportsAbiVersion,
) where

import Event.Sorcery.Engine.Internal (
  EngineError (..),
  ErrorClass (..),
  OpenOptions (..),
  Store,
  abiVersion,
  closeStore,
  decodeEngineError,
  encodeOpenOptions,
  openStore,
  supportsAbiVersion,
 )

