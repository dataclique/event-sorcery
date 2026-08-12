-- | The engine feature: opening a store, closing it, and the ABI it speaks.
--
-- The lifecycle plumbing and the call marshalling stay in
-- "Event.Sorcery.Engine.Internal"; this module is the consumer-facing half of
-- the feature, including the error vocabulary every engine call answers with.
module Event.Sorcery.Engine (
  AbiVersionDetail (..),
  AggregateId (..),
  AggregateType (..),
  BindingFault (..),
  ConflictDetail (..),
  EngineError (..),
  EngineErrorDecodeDetail (..),
  JobId (..),
  JobRefusal (..),
  JobRefusalDetail (..),
  OpenOptions (..),
  PageAdvanceDetail (..),
  ResourceLimitDetail (..),
  Store,
  abiVersion,
  checkAbiVersion,
  closeStore,
  decodeCloseStatus,
  decodeEngineError,
  encodeOpenOptions,
  minimumAbiMinor,
  openStore,
  supportedAbiMajor,
) where

import Event.Sorcery.Engine.Internal (
  AbiVersionDetail (..),
  AggregateId (..),
  AggregateType (..),
  BindingFault (..),
  ConflictDetail (..),
  EngineError (..),
  EngineErrorDecodeDetail (..),
  JobId (..),
  JobRefusal (..),
  JobRefusalDetail (..),
  OpenOptions (..),
  PageAdvanceDetail (..),
  ResourceLimitDetail (..),
  Store,
  abiVersion,
  checkAbiVersion,
  closeStore,
  decodeCloseStatus,
  decodeEngineError,
  encodeOpenOptions,
  minimumAbiMinor,
  openStore,
  supportedAbiMajor,
 )

