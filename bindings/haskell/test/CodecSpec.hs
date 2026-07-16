module Main (main) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as ByteString
import Data.Either (Either (Right), isLeft)
import EventSorcery.Engine (
  ConflictDetail (..),
  EngineError (..),
  OpenOptions (..),
  ResourceLimitDetail (..),
  decodeEngineError,
  encodeOpenOptions,
 )
import EventSorcery.Stream (
  AggregateId (..),
  AggregateType (..),
  EventType (..),
  EventVersion (..),
  ProposedEvent (..),
  StoredEvent (..),
  StreamIdentity (..),
  decodeStoredEvents,
  encodeCommit,
  encodeCurrentVersion,
  encodeLoadStream,
 )
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))
import Prelude (IO, Maybe (..), ($), (<>))


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
            assertBool
              "status mismatch must fail"
              (isLeft (decodeEngineError 4 conflict))
        , testCase "rejects trailing bytes" $
            assertBool
              "trailing byte must fail"
              (isLeft (decodeStoredEvents (stored <> ByteString.singleton 0)))
        , testCase "rejects unsupported versions" $
            assertBool
              "unsupported version must fail"
              (isLeft (decodeStoredEvents unsupportedVersion))
        , testCase "enforces top-level arity" $
            assertBool
              "wrong top-level arity must fail"
              (isLeft (decodeStoredEvents wrongTopLevelArity))
        , testCase "enforces stored-event arity" $
            assertBool
              "wrong stored-event arity must fail"
              (isLeft (decodeStoredEvents wrongEventArity))
        , testCase "requires byte-string payloads" $
            assertBool
              "array payload must fail"
              (isLeft (decodeStoredEvents arrayPayload))
        ]
    ]


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
