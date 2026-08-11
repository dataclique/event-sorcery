-- | The cursor arithmetic that walks a stream one engine page at a time.
--
-- Exposed so the guard against a page that cannot advance can be exercised
-- without a running engine; consumers walk streams through
-- "EventSorcery.Engine" instead.
module EventSorcery.Engine.Internal.Paging (
  nextCursor,
) where

import Data.List.NonEmpty (NonEmpty)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Word (Word64)
import EventSorcery.Engine.Protocol (
  BindingFault (PageDidNotAdvance),
  EngineError (BindingProtocolError),
  PageAdvanceDetail (..),
  StoredEvent (..),
 )
import Prelude (
  Either (..),
  Maybe (Just, Nothing),
  otherwise,
  (>),
 )


-- | Reports where the next page starts, refusing a page that cannot advance.
--
-- A page whose last sequence does not move past the cursor would make the walk
-- request the same page forever, so it is reported as a protocol failure.
nextCursor :: Maybe Word64 -> NonEmpty StoredEvent -> Either EngineError Word64
nextCursor after page =
  case after of
    Nothing -> Right advanced
    Just cursor
      | advanced > cursor -> Right advanced
      | otherwise ->
          Left
            ( BindingProtocolError
                (PageDidNotAdvance (PageAdvanceDetail cursor advanced))
            )
  where
    lastEvent = NonEmpty.last page
    advanced = lastEvent.sequence
