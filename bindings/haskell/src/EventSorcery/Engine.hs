module EventSorcery.Engine (
  AbiVersionDetail (..),
  ConflictDetail (..),
  EngineError (..),
  OpenOptions (..),
  ResourceLimitDetail (..),
  Store,
  abiVersion,
  closeStore,
  decodeEngineError,
  encodeOpenOptions,
  openStore,
  supportsAbiVersion,
) where

import EventSorcery.Engine.Internal (
  Store,
  abiVersion,
  closeStore,
  decodeEngineError,
  encodeOpenOptions,
  openStore,
  supportsAbiVersion,
 )
import EventSorcery.Engine.Protocol (
  AbiVersionDetail (..),
  ConflictDetail (..),
  EngineError (..),
  OpenOptions (..),
  ResourceLimitDetail (..),
 )

