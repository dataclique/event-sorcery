module Main (main) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as ByteString
import Data.Either (Either (Left, Right))
import Data.List (isInfixOf)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Word (Word64)
import EventSorcery.Engine.Codec (
  decodeCloseStatus,
  decodeEngineError,
  decodeStoredEvents,
  encodeCommit,
  encodeCurrentVersion,
  encodeLoadStream,
  encodeOpenOptions,
 )
import EventSorcery.Engine.Internal.Paging (nextCursor)
import EventSorcery.Engine.Protocol (
  AggregateId (..),
  AggregateType (..),
  BindingFault (..),
  ConflictDetail (..),
  EngineError (..),
  EventType (..),
  EventVersion (..),
  OpenOptions (..),
  PageAdvanceDetail (..),
  ProposedEvent (..),
  ResourceLimitDetail (..),
  StoredEvent (..),
  StreamIdentity (..),
 )
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.HUnit (Assertion, assertBool, assertFailure, testCase, (@?=))
import Prelude (IO, Maybe (..), String, ($), (<>))


main :: IO ()
main = defaultMain tests


tests :: TestTree
tests =
  testGroup
    "engine codecs"
    [ testGroup
        "encoding"
        [ testCase "open options" $
            encodeOpenOptions options @?= expectedOpen
        , testCase "load stream without a cursor" $
            encodeLoadStream stream Nothing @?= expectedLoadWithoutCursor
        , testCase "load stream after a cursor" $
            encodeLoadStream stream (Just 256) @?= expectedLoadAfterCursor
        , testCase "current version" $
            encodeCurrentVersion stream @?= expectedCurrentVersion
        , testCase "commit" $
            encodeCommit stream 0 [proposed] @?= expectedCommit
        ]
    , testGroup
        "decoding"
        [ testCase "stored event" $
            decodeStoredEvents stored @?= Right [expectedStored]
        , testCase "malformed-input detail" $
            decodeEngineError 1 malformedInput @?= Right MalformedInput
        , testCase "storage-failure detail" $
            decodeEngineError 4 storageFailure
              @?= Right (StorageFailure "storage failure")
        , testCase "invalid-state detail" $
            decodeEngineError 5 invalidState
              @?= Right (InvalidState "store is closed")
        , testCase "keeps the numeric identity of an unmodelled code" $
            decodeEngineError 3 unmodelledCode @?= Right (UnknownEngineError 3)
        , testCase "conflict detail" $
            decodeEngineError 2 conflict
              @?= Right
                ( OptimisticConflict
                    ( ConflictDetail
                        (AggregateType "account")
                        (AggregateId "one")
                        0
                        1
                    )
                )
        , testCase "resource-limit detail" $
            decodeEngineError 6 resourceLimit
              @?= Right
                ( ResourceLimitExceeded
                    (ResourceLimitDetail "payload" 65 64)
                )
        , testCase "panic detail" $
            decodeEngineError 100 enginePanic @?= Right EnginePanic
        , testCase "rejects disagreement between status and encoded code" $
            assertDecodeFailure
              "engine status does not match encoded error code"
              (decodeEngineError 4 conflict)
        , testCase "rejects trailing bytes" $
            decodeStoredEvents (stored <> ByteString.singleton 0)
              @?= Left "trailing bytes after stored events"
        , testCase "rejects unsupported versions" $
            assertDecodeFailure
              "unsupported stored-events format version"
              (decodeStoredEvents unsupportedVersion)
        , testCase "enforces top-level arity" $
            assertDecodeFailure
              "unexpected CBOR list length"
              (decodeStoredEvents wrongTopLevelArity)
        , testCase "enforces stored-event arity" $
            assertDecodeFailure
              "unexpected CBOR list length"
              (decodeStoredEvents wrongEventArity)
        , testCase "requires byte-string payloads" $
            assertDecodeFailure "expected bytes" (decodeStoredEvents arrayPayload)
        ]
    , testGroup
        "stream paging"
        [ testCase "starts a fresh walk at the last sequence of the page" $
            nextCursor Nothing (pageEndingAt 4096) @?= Right 4096
        , testCase "advances to the last sequence the page reached" $
            nextCursor (Just 4096) (pageEndingAt 4097) @?= Right 4097
        , testCase "refuses a page that ends on the cursor" $
            nextCursor (Just 4096) (pageEndingAt 4096)
              @?= Left
                ( BindingProtocolError
                    (PageDidNotAdvance (PageAdvanceDetail 4096 4096))
                )
        , testCase "refuses a page that ends behind the cursor" $
            nextCursor (Just 4096) (pageEndingAt 4095)
              @?= Left
                ( BindingProtocolError
                    (PageDidNotAdvance (PageAdvanceDetail 4096 4095))
                )
        ]
    , testGroup
        "detail-free close statuses"
        [ testCase "success" $
            decodeCloseStatus 0 @?= Right ()
        , testCase "rejected owner handle" $
            decodeCloseStatus 5
              @?= Left (InvalidState "store close rejected the owner handle")
        , testCase "panic" $
            decodeCloseStatus 100 @?= Left EnginePanic
        , testCase "unmodelled status" $
            decodeCloseStatus 42 @?= Left (UnknownEngineError 42)
        ]
    ]


-- | Pins a negative case to the rule it was written for.
--
-- The decoders report failures as text, and a fixture that is both malformed
-- and truncated fails for either reason, so only the codec's own message shows
-- that the intended check is still the one doing the rejecting.
assertDecodeFailure :: String -> Either String value -> Assertion
assertDecodeFailure expected outcome =
  case outcome of
    Left message ->
      assertBool
        ("expected a failure mentioning " <> expected <> ", got " <> message)
        (expected `isInfixOf` message)
    Right _ -> assertFailure ("expected a failure mentioning " <> expected)


options :: OpenOptions
options = OpenOptions "sqlite::memory:" 5000 1 256


stream :: StreamIdentity
stream = StreamIdentity (AggregateType "account") (AggregateId "one")


proposed :: ProposedEvent
proposed =
  ProposedEvent
    (EventType "Created")
    (EventVersion "1.0")
    (ByteString.pack [0, 1])


-- | A two-event page, so the cursor has to come from the last event.
pageEndingAt :: Word64 -> NonEmpty StoredEvent
pageEndingAt lastSequence = pageEvent 1 :| [pageEvent lastSequence]


pageEvent :: Word64 -> StoredEvent
pageEvent sequenceNumber =
  StoredEvent
    sequenceNumber
    (EventType "Created")
    (EventVersion "1.0")
    ByteString.empty


expectedStored :: StoredEvent
expectedStored =
  StoredEvent
    1
    (EventType "Created")
    (EventVersion "1.0")
    (ByteString.pack [0, 1])


expectedOpen :: ByteString
expectedOpen =
  ByteString.pack
    [ 133 -- array(5)
    , 1 -- format version 1
    , 111 -- text(15)
    , 115
    , 113
    , 108
    , 105
    , 116
    , 101
    , 58
    , 58
    , 109
    , 101
    , 109
    , 111
    , 114
    , 121
    , 58 -- sqlite::memory:
    , 25
    , 19
    , 136 -- uint16(5000)
    , 1 -- pool size 1
    , 25
    , 1
    , 0 -- uint16(256) runtime threads
    ]


expectedLoadWithoutCursor :: ByteString
expectedLoadWithoutCursor =
  ByteString.pack
    [ 132 -- array(4)
    , 1 -- format version 1
    , 103
    , 97
    , 99
    , 99
    , 111
    , 117
    , 110
    , 116 -- text(7) account
    , 99
    , 111
    , 110
    , 101 -- text(3) one
    , 246 -- null cursor
    ]


expectedLoadAfterCursor :: ByteString
expectedLoadAfterCursor =
  ByteString.pack
    [ 132 -- array(4)
    , 1 -- format version 1
    , 103
    , 97
    , 99
    , 99
    , 111
    , 117
    , 110
    , 116 -- text(7) account
    , 99
    , 111
    , 110
    , 101 -- text(3) one
    , 25
    , 1
    , 0 -- uint16(256) cursor
    ]


expectedCurrentVersion :: ByteString
expectedCurrentVersion =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 103
    , 97
    , 99
    , 99
    , 111
    , 117
    , 110
    , 116 -- text(7) account
    , 99
    , 111
    , 110
    , 101 -- text(3) one
    ]


expectedCommit :: ByteString
expectedCommit =
  ByteString.pack
    [ 133 -- array(5)
    , 1 -- format version 1
    , 103
    , 97
    , 99
    , 99
    , 111
    , 117
    , 110
    , 116 -- text(7) account
    , 99
    , 111
    , 110
    , 101 -- text(3) one
    , 0 -- expected version 0
    , 129 -- array(1) proposed event
    , 131 -- array(3) event product
    , 103
    , 67
    , 114
    , 101
    , 97
    , 116
    , 101
    , 100 -- text(7) Created
    , 99
    , 49
    , 46
    , 48 -- text(3) 1.0
    , 66
    , 0
    , 1 -- bytes(2)
    ]


stored :: ByteString
stored =
  ByteString.pack
    [ 130 -- array(2)
    , 1 -- format version 1
    , 129 -- array(1) stored event
    , 132 -- array(4) event product
    , 1 -- sequence 1
    , 103
    , 67
    , 114
    , 101
    , 97
    , 116
    , 101
    , 100 -- text(7) Created
    , 99
    , 49
    , 46
    , 48 -- text(3) 1.0
    , 66
    , 0
    , 1 -- bytes(2)
    ]


unsupportedVersion :: ByteString
unsupportedVersion =
  ByteString.pack
    [ 130 -- array(2)
    , 2 -- unsupported format version 2
    , 128 -- array(0) stored events
    ]


wrongTopLevelArity :: ByteString
wrongTopLevelArity =
  ByteString.pack
    [ 129 -- array(1), expected array(2)
    , 1 -- format version 1
    ]


wrongEventArity :: ByteString
wrongEventArity =
  ByteString.pack
    [ 130 -- array(2)
    , 1 -- format version 1
    , 129 -- array(1) stored event
    , 131 -- array(3), expected array(4)
    , 1 -- sequence 1
    , 103
    , 67
    , 114
    , 101
    , 97
    , 116
    , 101
    , 100 -- text(7) Created
    , 99
    , 49
    , 46
    , 48 -- text(3) 1.0
    ]


arrayPayload :: ByteString
arrayPayload =
  ByteString.pack
    [ 130 -- array(2)
    , 1 -- format version 1
    , 129 -- array(1) stored event
    , 132 -- array(4) event product
    , 1 -- sequence 1
    , 103
    , 67
    , 114
    , 101
    , 97
    , 116
    , 101
    , 100 -- text(7) Created
    , 99
    , 49
    , 46
    , 48 -- text(3) 1.0
    , 130
    , 0
    , 1 -- array(2), not bytes(2)
    ]


conflict :: ByteString
conflict =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 2 -- conflict error
    , 132 -- array(4) conflict detail
    , 103
    , 97
    , 99
    , 99
    , 111
    , 117
    , 110
    , 116 -- text(7) account
    , 99
    , 111
    , 110
    , 101 -- text(3) one
    , 0 -- expected version 0
    , 1 -- actual version 1
    ]


malformedInput :: ByteString
malformedInput =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 1 -- malformed-input error
    , 111
    , 109
    , 97
    , 108
    , 102
    , 111
    , 114
    , 109
    , 101
    , 100
    , 32
    , 105
    , 110
    , 112
    , 117
    , 116 -- text(15) malformed input
    ]


storageFailure :: ByteString
storageFailure =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 4 -- storage-failure error
    , 111
    , 115
    , 116
    , 111
    , 114
    , 97
    , 103
    , 101
    , 32
    , 102
    , 97
    , 105
    , 108
    , 117
    , 114
    , 101 -- text(15) storage failure
    ]


invalidState :: ByteString
invalidState =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 5 -- invalid-state error
    , 111
    , 115
    , 116
    , 111
    , 114
    , 101
    , 32
    , 105
    , 115
    , 32
    , 99
    , 108
    , 111
    , 115
    , 101
    , 100 -- text(15) store is closed
    ]


-- | An error class the engine does not define, carrying an arbitrary detail.
--
-- The catch-all has to survive a detail whose shape the binding has never
-- seen, so the fixture pairs an undefined class with a product the decoder
-- has no clause for.
unmodelledCode :: ByteString
unmodelledCode =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 3 -- an error class the engine does not define
    , 130 -- array(2) detail of an unknown shape
    , 98
    , 105
    , 100 -- text(2) id
    , 100
    , 108
    , 97
    , 116
    , 101 -- text(4) late
    ]


resourceLimit :: ByteString
resourceLimit =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 6 -- resource-limit error
    , 131 -- array(3) resource detail
    , 103
    , 112
    , 97
    , 121
    , 108
    , 111
    , 97
    , 100 -- text(7) payload
    , 24
    , 65 -- observed 65
    , 24
    , 64 -- limit 64
    ]


enginePanic :: ByteString
enginePanic =
  ByteString.pack
    [ 131 -- array(3)
    , 1 -- format version 1
    , 24
    , 100 -- panic error
    , 246 -- null detail
    ]
