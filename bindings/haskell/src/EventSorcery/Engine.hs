-- | The engine feature: opening a store, closing it, and the ABI it speaks.
--
-- The lifecycle plumbing and the call marshalling stay in
-- "EventSorcery.Engine.Internal"; this module is the consumer-facing half of
-- the feature, including the error vocabulary every engine call answers with.
module EventSorcery.Engine (
  AbiVersionDetail (..),
  AggregateId (..),
  AggregateType (..),
  BindingFault (..),
  ConflictDetail (..),
  EngineError (..),
  EngineErrorDecodeDetail (..),
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

import EventSorcery.Engine.Internal (
  AbiVersionDetail (..),
  AggregateId (..),
  AggregateType (..),
  BindingFault (..),
  ConflictDetail (..),
  EngineError (..),
  EngineErrorDecodeDetail (..),
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
